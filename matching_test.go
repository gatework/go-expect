package expect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func mustPattern(t *testing.T, text string) Pattern {
	t.Helper()
	p, e := Compile(text)
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func TestExpectContinueAndCaptures(t *testing.T) {
	s, p := pair(t)
	go func() {
		p.Write([]byte("Password:"))
		b := make([]byte, 3)
		io.ReadFull(p, b)
		if string(b) == "pw\r" {
			p.Write([]byte("code=42;ready>tail"))
		}
		p.Close()
	}()
	calls := 0
	r, err := s.Expect(t.Context(), []Case{
		{Pattern: Literal("Password:"), Handler: func(ctx context.Context, s *Session, r Result) (Action, error) {
			calls++
			_, err := s.Send(ctx, "pw\r")
			return ContinueWithoutReset, err
		}},
		{Pattern: mustPattern(t, `code=(\d+);`), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
			if r.Captures[0].Text != "42" || !r.Captures[0].Matched {
				t.Error("capture")
			}
			calls++
			return Continue, nil
		}},
		{Pattern: Literal("ready>")},
	})
	if err != nil || r.CaseIndex != 2 || calls != 2 {
		t.Fatalf("%v index=%d calls=%d", err, r.CaseIndex, calls)
	}
	waitText(t, s, "tail")
}
func TestExpectEarliestAndAll(t *testing.T) {
	s, p := pair(t)
	go func() { p.Write([]byte("first later b a END")); p.Close() }()
	r, err := s.Expect(t.Context(), []Case{{Pattern: Literal("later")}, {Pattern: Literal("first")}})
	if err != nil || r.CaseIndex != 1 {
		t.Fatalf("%v %d", err, r.CaseIndex)
	}
	r, err = s.Expect(t.Context(), []Case{{Pattern: All(Literal("a"), Literal("b"))}})
	if err != nil || !strings.Contains(r.Match, "b") {
		t.Fatalf("%v %q", err, r.Match)
	}
	waitText(t, s, "END")
}
func TestExpectReentryAndHandlerErrorTail(t *testing.T) {
	s, p := pair(t)
	go p.Write([]byte("prompttail"))
	failure := errors.New("handler")
	_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(ctx context.Context, s *Session, _ Result) (Action, error) {
		if _, err := s.Expect(ctx, []Case{{Pattern: Literal("tail")}}); !errors.Is(err, ErrBusy) {
			t.Fatalf("reentry: %v", err)
		}
		return Return, failure
	}}})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	waitText(t, s, "tail")
}
func TestExpectZeroWidthKeepsBuffer(t *testing.T) {
	s, p := pair(t)
	go p.Write([]byte("ready"))
	_, err := s.Expect(t.Context(), []Case{{Pattern: mustPattern(t, `r*`)}})
	if !errors.Is(err, ErrEmptyMatch) {
		t.Fatal(err)
	}
	waitText(t, s, "ready")
}
func TestExpectContinueTimers(t *testing.T) {
	for _, action := range []Action{Continue, ContinueWithoutReset} {
		t.Run(fmt.Sprint(action), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, p := pair(t)
				done := make(chan struct{})
				defer func() { s.Close(); <-done }()
				go func() {
					defer close(done)
					time.Sleep(600 * time.Millisecond)
					p.Write([]byte("again"))
					time.Sleep(600 * time.Millisecond)
					p.Write([]byte("done"))
				}()
				_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("again"), Handler: func(context.Context, *Session, Result) (Action, error) { return action, nil }}, {Pattern: Literal("done")}}, WithTimeout(time.Second))
				if action == Continue && err != nil {
					t.Fatal(err)
				}
				if action == ContinueWithoutReset && !errors.Is(err, ErrTimeout) {
					t.Fatal(err)
				}
			})
		})
	}
}
func TestExpectHardDeadlineAndIdleTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		done := make(chan struct{})
		defer func() { s.Close(); <-done }()
		go func() {
			defer close(done)
			for i := 0; i < 5; i++ {
				time.Sleep(400 * time.Millisecond)
				if _, e := p.Write([]byte("again")); e != nil {
					return
				}
			}
		}()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := s.Expect(ctx, []Case{{Pattern: Literal("again"), Handler: func(context.Context, *Session, Result) (Action, error) { return Continue, nil }}}, WithTimeout(800*time.Millisecond))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		s, _ := pair(t)
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("x")}}, WithIdleTimeout(time.Second))
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatal(err)
		}
	})
}
func TestResponderRepeatedCommands(t *testing.T) {
	s, p := pair(t)
	r, err := NewResponder(Rule{Pattern: Literal("confirm?"), Respond: Reply("y\r"), Limit: 2}, Rule{Pattern: Literal("--More--"), Respond: Reply(" ")}, Rule{Pattern: mustPattern(t, `code (\d+):`), Respond: func(ctx context.Context, s *Session, r Result) error {
		_, e := s.Send(ctx, r.Captures[0].Text+"\r")
		return e
	}})
	if err != nil {
		t.Fatal(err)
	}
	device := make(chan error, 1)
	go func() {
		defer p.Close()
		for command := 0; command < 2; command++ {
			for _, step := range []struct{ prompt, reply string }{{"confirm?", "y\r"}, {"confirm?", "y\r"}, {"--More--", " "}, {"code 42:", "42\r"}} {
				if _, e := io.WriteString(p, step.prompt); e != nil {
					device <- e
					return
				}
				buf := make([]byte, len(step.reply))
				if _, e := io.ReadFull(p, buf); e != nil {
					device <- e
					return
				}
				if string(buf) != step.reply {
					device <- fmt.Errorf("reply %q", buf)
					return
				}
			}
			io.WriteString(p, "device>")
		}
		device <- nil
	}()
	for i := 0; i < 2; i++ {
		res, e := r.Expect(t.Context(), s, []Pattern{Literal("device>")})
		if e != nil || res.Match != "device>" {
			t.Fatalf("%v %q", e, res.Match)
		}
	}
	if err := <-device; err != nil {
		t.Fatal(err)
	}
}
func TestResponderLimitAndCompletionTie(t *testing.T) {
	s, p := pair(t)
	go p.Write([]byte("xx"))
	count := 0
	r, err := NewResponder(Rule{Pattern: Literal("x"), Respond: func(context.Context, *Session, Result) error { count++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Expect(t.Context(), s, []Pattern{Literal("done")})
	var re *ResponseError
	if !errors.As(err, &re) || count != 1 {
		t.Fatalf("%v count=%d", err, count)
	}
	waitText(t, s, "x")
	go p.Write([]byte("x"))
	if _, err = r.Expect(t.Context(), s, []Pattern{Literal("x")}); err != nil || count != 1 {
		t.Fatalf("%v count=%d", err, count)
	}
}
func TestResultFormattingIsSafe(t *testing.T) {
	r := Result{Before: "secret", Match: "secret", Captures: []Capture{{Text: "secret", Matched: true}}}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", r, r, r), "secret") {
		t.Fatal("leaked result")
	}
}

func TestResponderRejectedWaitNotifiesObservers(t *testing.T) {
	calls := 0
	s, _ := pair(t, WithObserver(func(e Event) {
		if e.Operation == "expect" && e.Error != "" {
			calls++
		}
	}))
	r, err := NewResponder()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Expect(t.Context(), s, nil); err == nil {
		t.Fatal("accepted missing completion")
	}
	if calls != 1 {
		t.Fatalf("observer calls=%d", calls)
	}
}
