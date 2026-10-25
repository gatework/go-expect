package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestExpectCanceledInsideHandlerCannotReturnSuccess(t *testing.T) {
	s, p := pair(t)
	go p.Write([]byte("prompt"))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := s.Expect(ctx, []Case{{Pattern: Literal("prompt"), Handler: func(context.Context, *Session, Result) (Action, error) { cancel(); return Return, nil }}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled handler returned %v", err)
	}
}
func TestExpectBusyObservers(t *testing.T) {
	busy := 0
	s, p := pair(t, WithObserver(func(e Event) {
		if e.Error == "busy" {
			busy++
		}
	}))
	go p.Write([]byte("prompt"))
	_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(ctx context.Context, s *Session, r Result) (Action, error) {
		_, e := s.Expect(ctx, []Case{{Pattern: Literal("other")}})
		if !errors.Is(e, ErrBusy) {
			t.Error(e)
		}
		return Return, nil
	}}})
	if err != nil || busy != 1 {
		t.Fatalf("%v observed=%d", err, busy)
	}
}
func TestSessionPartialWriteCancellationReportsProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		consumed := make(chan string, 1)
		go func() { buf := make([]byte, 2); io.ReadFull(p, buf); consumed <- string(buf) }()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		n, err := s.Send(ctx, "yes\r")
		var we *WriteError
		if n != 2 || !errors.As(err, &we) || we.Written != 2 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("n=%d err=%v", n, err)
		}
		if <-consumed != "ye" {
			t.Fatal("wrong prefix")
		}
	})
}
func TestSessionConcurrentExpectAndCloseFromHandler(t *testing.T) {
	s, p := pair(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go p.Write([]byte("go"))
	go func() {
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("go"), Handler: func(context.Context, *Session, Result) (Action, error) {
			close(entered)
			<-release
			return Return, s.Close()
		}}})
		done <- err
	}()
	<-entered
	if _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("go")}}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestSessionWaitingSendCancelKeepsActiveWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		first := make(chan error, 1)
		go func() { _, err := s.Send(t.Context(), "first"); first <- err }()
		synctest.Wait()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if n, err := s.Send(ctx, "second"); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%d %v", n, err)
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(p, buf); err != nil || string(buf) != "first" {
			t.Fatalf("%s %v", buf, err)
		}
		if err := <-first; err != nil {
			t.Fatal(err)
		}
	})
}
func TestSessionSinkFailureStopsBeforeReply(t *testing.T) {
	failure := errors.New("sink broke")
	s, p := pair(t, WithOutput(failingWriter{failure}))
	go p.Write([]byte("confirm?"))
	calls := 0
	_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("confirm?"), Handler: func(context.Context, *Session, Result) (Action, error) { calls++; return Return, nil }}})
	if !errors.Is(err, failure) || calls != 0 {
		t.Fatalf("%v calls=%d", err, calls)
	}
	if _, err = s.Send(t.Context(), "yes"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }
func TestSessionLogsAndObserversDoNotLeak(t *testing.T) {
	var out bytes.Buffer
	s, p := pair(t, WithLogger(log.New(&out, "", 0)), WithObserver(func(e Event) { out.WriteString(e.Error) }))
	go p.Write([]byte("secret"))
	waitText(t, s, "secret")
	if strings.Contains(out.String(), "secret") {
		t.Fatal("payload in diagnostics")
	}
}
func TestSessionWriteTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := pair(t, WithWriteTimeout(time.Second))
		n, err := s.Send(t.Context(), "blocked")
		if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%d %v", n, err)
		}
	})
}
func TestOptionalCaptureAndSplitUTF8(t *testing.T) {
	s, p := pair(t)
	go func() {
		for _, b := range []byte("中!") {
			p.Write([]byte{b})
		}
		p.Close()
	}()
	r, err := s.Expect(t.Context(), []Case{{Pattern: mustPattern(t, `(中)(x)?()!`)}})
	if err != nil || r.Match != "中!" || len(r.Captures) != 3 || r.Captures[0].Text != "中" || r.Captures[1].Matched || !r.Captures[2].Matched {
		t.Fatalf("%v match=%q captures=%v", err, r.Match, r.Captures)
	}
}

func TestExpectReturnAfterRelativeDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		go p.Write([]byte("prompt"))
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(context.Context, *Session, Result) (Action, error) {
			time.Sleep(2 * time.Second)
			return Return, nil
		}}}, WithTimeout(time.Second))
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("expired handler returned %v", err)
		}
	})
}

func TestExpectReturnAfterIdleDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		go p.Write([]byte("prompt"))
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(context.Context, *Session, Result) (Action, error) {
			time.Sleep(2 * time.Second)
			return Return, nil
		}}}, WithIdleTimeout(time.Second))
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("expired handler returned %v", err)
		}
	})
}
func TestSessionDefaultTimeoutOverride(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t, WithDefaultTimeout(time.Second))
		if _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("x")}}); !errors.Is(err, ErrTimeout) {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); time.Sleep(1500 * time.Millisecond); p.Write([]byte("x")) }()
		if _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("x")}}, WithTimeout(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		<-done
	})
}
