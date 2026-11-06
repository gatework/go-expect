// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestSendCancellationBeforeWriteCompletion(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		transport := &cancelingWriter{cancel: cancel}
		s, err := NewSession(transport)
		if err != nil {
			t.Fatal(err)
		}
		n, err := s.Send(ctx, "yes")
		closed := s.isClosed()
		s.Close()
		cancel()
		var we *WriteError
		if n != 3 || !errors.As(err, &we) || !errors.Is(err, context.Canceled) || !closed {
			t.Fatalf("cancellation before completion lost: n=%d err=%v closed=%v", n, err, closed)
		}
	}
}

type cancelingWriter struct{ cancel context.CancelFunc }

func (w *cancelingWriter) Write(p []byte) (int, error) { w.cancel(); return len(p), nil }

func TestSendCloseBeforeWriteCompletion(t *testing.T) {
	for i := 0; i < 100; i++ {
		transport := &cancelingWriter{}
		s, err := NewSession(transport)
		if err != nil {
			t.Fatal(err)
		}
		transport.cancel = func() {
			go s.Close()
			<-s.done
		}
		n, err := s.Send(t.Context(), "yes")
		s.Close()
		var we *WriteError
		if n != 3 || !errors.As(err, &we) || !errors.Is(err, ErrClosed) {
			t.Fatalf("close before completion lost: n=%d err=%v", n, err)
		}
	}
}

type nilTransport struct{}

func (*nilTransport) Write(p []byte) (int, error) { return len(p), nil }

func TestCanceledWriteValidatesProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		defer b.Close()
		s, err := NewSession(&invalidCanceledWrite{Conn: a})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		n, err := s.Send(ctx, "yes")
		var we *WriteError
		if n != 0 || !errors.As(err, &we) || we.Written != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("invalid progress escaped: n=%d err=%v", n, err)
		}
		if !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("transport cause lost during cancellation")
		}
	})
}

func TestClosedWriteValidatesProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		defer b.Close()
		s, err := NewSession(&invalidCanceledWrite{Conn: a})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		done := make(chan writeResult, 1)
		go func() { n, err := s.Send(t.Context(), "yes"); done <- writeResult{n, err} }()
		synctest.Wait()
		s.Close()
		r := <-done
		var we *WriteError
		if r.n != 0 || !errors.As(r.err, &we) || we.Written != 0 || !errors.Is(r.err, io.ErrClosedPipe) {
			t.Fatalf("invalid progress escaped on close: n=%d err=%v", r.n, r.err)
		}
	})
}

type invalidCanceledWrite struct{ net.Conn }

func (w *invalidCanceledWrite) Write(p []byte) (int, error) {
	_, err := w.Conn.Write(p)
	return len(p) + 1, err
}

func (*chunkStream) Write(p []byte) (int, error) { return len(p), nil }

type admissionTransport struct {
	closed chan struct{}
	once   sync.Once
	writes atomic.Int32
}

func (p *admissionTransport) Write(b []byte) (int, error) { p.writes.Add(1); return len(b), nil }

func TestCanceledBeforeWriteEntry(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "once", true: "all"}[all], func(t *testing.T) {
			p := &admissionTransport{closed: make(chan struct{})}
			s, err := NewSession(p, WithWriteTimeout(0))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			s.lifeMu.Lock()
			go func() {
				var err error
				if all {
					_, err = s.SendAll(ctx, "command")
				} else {
					_, err = s.Send(ctx, "command")
				}
				done <- err
			}()
			deadline := time.Now().Add(time.Second)
			for len(s.writeGate) == 0 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			admitted := len(s.writeGate) > 0
			cancel()
			s.lifeMu.Unlock()
			if !admitted {
				t.Fatal("admission timed out")
			}
			err = <-done
			var we *WriteError
			if !errors.Is(err, context.Canceled) || errors.As(err, &we) || p.writes.Load() != 0 || s.isClosed() {
				t.Fatalf("err=%v writes=%d closed=%v", err, p.writes.Load(), s.isClosed())
			}
			if _, err = s.Send(t.Context(), "next"); err != nil {
				t.Fatal(err)
			}
		})
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

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestSessionWriteTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _ := pair(t, WithWriteTimeout(time.Second))
		n, err := s.Send(t.Context(), "blocked")
		if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%d %v", n, err)
		}
	})
}

func (s *tinyProgressStream) Write(p []byte) (int, error) { return min(1, len(p)), nil }

func TestSendAllDoesNotAllocateEachUnsentSuffix(t *testing.T) {
	text := string(make([]byte, 4096))
	allocations := testing.AllocsPerRun(3, func() {
		s, err := NewSession(&tinyProgressStream{done: make(chan struct{})})
		if err != nil {
			t.Fatal(err)
		}
		n, err := s.SendAll(t.Context(), text)
		s.Close()
		if err != nil || n != len(text) {
			t.Fatalf("written=%d err=%v", n, err)
		}
	})
	if allocations > 100 {
		t.Fatalf("partial writes allocate each suffix: %.0f allocations", allocations)
	}
}

func (s *failingDuplex) Write([]byte) (int, error) { return 1, s.writeErr }

func TestSendAllQuietPeerQueuedPromptVisible(t *testing.T) {
	a, b := net.Pipe()
	s, e := NewSession(a)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	sent := make(chan error, 1)
	go func() { _, e := s.SendAll(ctx, "request"); sent <- e }()
	if _, e = io.WriteString(b, "ready!"); e != nil {
		t.Fatal(e)
	}
	waitQueuedBytes(t, s, len("ready!"))
	r, e := s.Expect(ctx, []Case{{Pattern: Literal("ready!")}}, WithTimeout(100*time.Millisecond))
	if e != nil || r.Match != "ready!" {
		t.Fatalf("queued prompt hidden during write: match=%q err=%v", r.Match, e)
	}
	select {
	case e := <-sent:
		t.Fatalf("write completed before peer read: %v", e)
	default:
	}
	got := make([]byte, len("request"))
	if _, e = io.ReadFull(b, got); e != nil {
		t.Fatal(e)
	}
	if string(got) != "request" {
		t.Fatalf("request=%q", got)
	}
	if e = <-sent; e != nil {
		t.Fatal(e)
	}
}

type quietEOFTransport struct {
	input      *strings.Reader
	allowWrite chan struct{}
	closed     chan struct{}
	once       sync.Once
}

func (p *quietEOFTransport) Write(data []byte) (int, error) {
	select {
	case <-p.allowWrite:
		return len(data), nil
	case <-p.closed:
		return 0, io.ErrClosedPipe
	}
}

func TestSendAllQuietPeerQueuedEOFVisible(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		limit       int
	}{{"tail", "tail", 1024}, {"empty", "", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			p := &quietEOFTransport{input: strings.NewReader(tc.input), allowWrite: make(chan struct{}), closed: make(chan struct{})}
			s, e := NewSession(p, WithBufferLimit(tc.limit))
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			sent := make(chan error, 1)
			go func() { _, e := s.SendAll(ctx, "request"); sent <- e }()
			if tc.input != "" {
				waitQueuedBytes(t, s, len(tc.input))
			}
			r, e := s.Expect(ctx, []Case{{Pattern: EOF()}}, WithTimeout(100*time.Millisecond))
			if e != nil || r.Before != tc.input {
				t.Fatalf("tail=%q err=%v", r.Before, e)
			}
			select {
			case e := <-sent:
				t.Fatalf("write completed before permission: %v", e)
			default:
			}
			close(p.allowWrite)
			if e = <-sent; e != nil {
				t.Fatal(e)
			}
		})
	}
}

type countedFailureWriter struct {
	calls int
	err   error
}

func (w *countedFailureWriter) Write([]byte) (int, error) {
	w.calls++
	return 0, w.err
}

func TestSendOutputPanicCleanup(t *testing.T) {
	t.Run("synchronous sink panic releases lifecycle", func(t *testing.T) {
		s, p := pair(t, WithSendOutput(matchOutputFunc(func([]byte) (int, error) { panic("private sink") })))
		go func() { _, _ = io.ReadFull(p, make([]byte, 1)) }()
		func() {
			defer func() {
				if recover() != "private sink" {
					t.Error("panic was not propagated")
				}
			}()
			_, _ = s.Send(t.Context(), "x")
		}()
		done := make(chan struct{})
		go func() { _ = s.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Close blocked after panic")
		}
	})
	t.Run("asynchronous sink panic preserves progress", func(t *testing.T) {
		stream := &quietEOFTransport{input: strings.NewReader(""), allowWrite: make(chan struct{}), closed: make(chan struct{})}
		close(stream.allowWrite)
		s, err := NewSession(stream, WithSendOutput(matchOutputFunc(func([]byte) (int, error) { panic("private sink") })))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		n, err := s.Send(t.Context(), "x")
		var writeErr *WriteError
		var callback *CallbackError
		if n != 1 || !errors.As(err, &writeErr) || writeErr.Written != 1 || !errors.As(err, &callback) || callback.Kind != CallbackPanicked || !s.isClosed() {
			t.Fatalf("n=%d err=%v", n, err)
		}
	})
}
