// SPDX-License-Identifier: Apache-2.0

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
			return ContinueKeepTimer, err
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
	for _, action := range []Action{Continue, ContinueKeepTimer} {
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
				if action == ContinueKeepTimer && !errors.Is(err, ErrTimeout) {
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
			for range 5 {
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
func TestRulesRepeatedCommands(t *testing.T) {
	s, p := pair(t)
	r := WithRules(Case{Pattern: Literal("confirm?"), Handler: ReplyThenContinue("y\r"), Limit: 2}, Case{Pattern: Literal("--More--"), Handler: ReplyThenContinue(" ")}, Case{Pattern: mustPattern(t, `code (\d+):`), Handler: Then(func(ctx context.Context, s *Session, r Result) error {
		_, e := s.Send(ctx, r.Captures[0].Text+"\r")
		return e
	})})
	device := make(chan error, 1)
	go func() {
		defer p.Close()
		for range 2 {
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
	for range 2 {
		res, e := s.Expect(t.Context(), []Case{{Pattern: Literal("device>")}}, r)
		if e != nil || res.Match != "device>" {
			t.Fatalf("%v %q", e, res.Match)
		}
	}
	if err := <-device; err != nil {
		t.Fatal(err)
	}
}
func TestRulesLimitAndCompletionTie(t *testing.T) {
	s, p := pair(t)
	go p.Write([]byte("xx"))
	count := 0
	var err error
	r := WithRules(Case{Pattern: Literal("x"), Handler: Then(func(context.Context, *Session, Result) error { count++; return nil })})
	_, err = s.Expect(t.Context(), []Case{{Pattern: Literal("done")}}, r)
	var re *CaseLimitError
	if !errors.As(err, &re) || count != 1 {
		t.Fatalf("%v count=%d", err, count)
	}
	waitText(t, s, "x")
	go p.Write([]byte("x"))
	if _, err = s.Expect(t.Context(), []Case{{Pattern: Literal("x")}}, r); err != nil || count != 1 {
		t.Fatalf("%v count=%d", err, count)
	}
}
func TestResultFormattingIsSafe(t *testing.T) {
	r := Result{Before: "secret", Match: "secret", Captures: []Capture{{Text: "secret", Matched: true}}}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", r, r, r), "secret") {
		t.Fatal("leaked result")
	}
}

func TestRulesRejectedWaitNotifiesObservers(t *testing.T) {
	calls := 0
	s, _ := pair(t, WithObserver(func(e Event) {
		if e.Operation == "expect" && e.Class != "" {
			calls++
		}
	}))
	r := WithRules()
	var err error
	if _, err = s.Expect(t.Context(), nil, r); err == nil {
		t.Fatal("accepted missing completion")
	}
	if calls != 1 {
		t.Fatalf("observer calls=%d", calls)
	}
}

func TestRulesEventsAndNoConsume(t *testing.T) {
	t.Run("timeout wakes peer", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, p := pair(t)
			go func() {
				b := make([]byte, 1)
				if _, err := io.ReadFull(p, b); err == nil {
					_, _ = p.Write([]byte("ready"))
				}
			}()
			calls := 0
			result, err := s.Expect(t.Context(), []Case{
				{Pattern: Timeout(), Limit: 2, Handler: func(ctx context.Context, s *Session, _ Result) (Action, error) {
					calls++
					_, err := s.Send(ctx, "\r")
					return Continue, err
				}},
				{Pattern: Literal("ready")},
			}, WithTimeout(time.Second))
			if err != nil || result.CaseIndex != 1 || calls != 1 {
				t.Fatalf("result=%v calls=%d err=%v", result, calls, err)
			}
		})
	})
	t.Run("finite events", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, _ := pair(t)
			calls := 0
			_, err := s.Expect(t.Context(), []Case{{Pattern: Timeout(), Handler: func(context.Context, *Session, Result) (Action, error) { calls++; return ContinueKeepTimer, nil }}}, WithTimeout(time.Second))
			var limit *CaseLimitError
			if !errors.As(err, &limit) || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	})
	t.Run("parent cancellation bypasses default", func(t *testing.T) {
		s, _ := pair(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := s.Expect(ctx, []Case{{Pattern: Default()}})
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	t.Run("persistent priority and snapshots", func(t *testing.T) {
		hits := 0
		s, p := pair(t, WithBefore(Case{Pattern: Literal("before"), Limit: 2, Handler: func(context.Context, *Session, Result) (Action, error) { hits++; return ContinueKeepTimer, nil }}))
		snapshot := s.Rules()
		snapshot.Before[0].Pattern = Literal("broken")
		go func() { _, _ = p.Write([]byte("done before")) }()
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("done")}}, WithTimeout(time.Millisecond))
		if !errors.Is(err, ErrTimeout) || hits != 1 {
			t.Fatalf("before priority: hits=%d err=%v", hits, err)
		}
	})
	t.Run("glob fold captures and preview", func(t *testing.T) {
		s, p := pair(t)
		go func() { _, _ = p.Write([]byte("prefix LOGIN:alice!")) }()
		pattern, err := Compile(`(?P<user>alice)(x)?!`)
		if err != nil {
			t.Fatal(err)
		}
		result, err := s.Expect(t.Context(), []Case{{Pattern: pattern, NoConsume: true}})
		if err != nil {
			t.Fatal(err)
		}
		c, ok := result.Capture("user")
		if !ok || c.Start != 13 || c.End != 18 || c.Text != "alice" {
			t.Fatalf("capture=%+v", c)
		}
		if a, b, ok := result.Indices(2); ok || a != -1 || b != -1 {
			t.Fatalf("optional=%d,%d,%v", a, b, ok)
		}
		if result.Buffer() != "prefix LOGIN:alice!" {
			t.Fatal("buffer snapshot changed")
		}
		result, err = s.Expect(t.Context(), []Case{{Pattern: Glob("login:?????!").Fold()}})
		if err != nil || result.Match != "LOGIN:alice!" {
			t.Fatalf("%v %v", result, err)
		}
	})
	t.Run("full buffer explicitly consumed", func(t *testing.T) {
		s, p := pair(t, WithBufferLimit(4))
		go func() { _, _ = p.Write([]byte("abcdef!")) }()
		r, err := s.Expect(t.Context(), []Case{{Pattern: FullBuffer()}})
		if err != nil || r.Buffer() != "abcd" {
			t.Fatalf("%v %v", r, err)
		}
		r, err = s.Expect(t.Context(), []Case{{Pattern: Literal("!")}})
		if err != nil || r.Before != "ef" {
			t.Fatalf("%v %v", r, err)
		}
	})
}

func TestMultiAndBackgroundOwnership(t *testing.T) {
	checkReleased := func(s *Session, e Event) {
		if e.Operation == OpExpect && e.Class != ClassBusy && s.expecting.Load() {
			t.Error("completion observer still owns receive reservation")
		}
	}
	var first, second *Session
	s1, p1 := pair(t, WithObserver(func(e Event) { checkReleased(first, e) }))
	first = s1
	s2, p2 := pair(t, WithObserver(func(e Event) { checkReleased(second, e) }))
	second = s2
	go func() { _, _ = p2.Write([]byte("ready")) }()
	index, r, err := expectGroupIndex(t.Context(), []*Session{s1, s2}, []Case{{Pattern: Literal("ready")}})
	if err != nil || index != 1 || r.Match != "ready" {
		t.Fatalf("index=%d result=%v err=%v", index, r, err)
	}
	go func() { _, _ = p1.Write([]byte("both")) }()
	go func() { _, _ = p2.Write([]byte("both")) }()
	callbacks := 0
	index, _, err = expectGroupIndex(t.Context(), []*Session{s1, s2}, []Case{{Pattern: Literal("both"), Handler: func(context.Context, *Session, Result) (Action, error) { callbacks++; return Return, nil }}})
	if err != nil || callbacks != 1 {
		t.Fatalf("callbacks=%d err=%v", callbacks, err)
	}
	waitText(t, []*Session{s1, s2}[1-index], "both")
	go func() { _, _ = p1.Write([]byte("keep")) }()
	waitText(t, s1, "keep")
	b, err := s1.StartBackground(t.Context(), []Case{{Pattern: Literal("end")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s1.Expect(t.Context(), []Case{{Pattern: Literal("end")}}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	b.Stop(t.Context())
	_, err = b.Wait(t.Context())
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	go func() { _, _ = p1.Write([]byte("end")) }()
	waitText(t, s1, "end")
}

type matchOutputFunc func([]byte) (int, error)

func (f matchOutputFunc) Write(data []byte) (int, error) { return f(data) }

func TestGroupPreservesIdleAcrossSlowOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		winner, p1 := pair(t)
		loser, p2 := pair(t, WithOutput(matchOutputFunc(func(data []byte) (int, error) { close(entered); time.Sleep(2 * time.Second); return len(data), nil })))
		go func() { _, _ = p2.Write([]byte("waiting")) }()
		go func() { <-entered; _, _ = p1.Write([]byte("ready")) }()
		index, _, err := expectGroupIndex(t.Context(), []*Session{winner, loser}, []Case{{Pattern: Literal("ready")}}, WithIdleTimeout(time.Second))
		if index != -1 || !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("index=%d err=%v", index, err)
		}
		waitText(t, winner, "ready")
	})
}

func TestEvolutionPatternValidation(t *testing.T) {
	if !errors.Is(Line("a\nb").Fold().ValidateText(), ErrInvalidPattern) {
		t.Fatal("Fold accepted invalid Line")
	}
	_, err := Compile("secret[")
	if !errors.Is(err, ErrInvalidPattern) {
		t.Fatal("compile category lost")
	}
	for _, entry := range []struct {
		p     Pattern
		input string
		match bool
	}{{Glob("[z-a]"), "m", true}, {Glob("[A-z]").Fold(), "_", false}, {Glob("[A-z]").Fold(), "Z", true}} {
		s, peer := pair(t)
		go func() { _, _ = peer.Write([]byte(entry.input)); _ = peer.Close() }()
		_, err := s.Expect(t.Context(), []Case{{Pattern: entry.p}})
		if (err == nil) != entry.match {
			t.Fatalf("pattern=%v err=%v", entry.p, err)
		}
	}
}
func TestTimeoutCallbackRetainsBudgets(t *testing.T) {
	t.Run("earliest deadline after slow trace", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, _ := pair(t, WithTrace(func(TraceEvent) { time.Sleep(4 * time.Second) }))
			_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("x")}}, WithIdleTimeout(time.Second), WithTimeout(3*time.Second))
			if !errors.Is(err, ErrIdleTimeout) {
				t.Fatalf("earlier idle deadline lost: %v", err)
			}
		})
	})

	for _, entry := range []struct {
		name        string
		action      Action
		idle, sleep time.Duration
		want        error
	}{
		{"idle return", Return, 3 * time.Second, 2 * time.Second, ErrIdleTimeout},
		{"idle continue", Continue, 3 * time.Second, 2 * time.Second, ErrIdleTimeout},
		{"idle keep", ContinueKeepTimer, 3 * time.Second, 2 * time.Second, ErrIdleTimeout},
		{"fresh relative", Return, 0, 3 * time.Second, ErrTimeout},
	} {
		t.Run(entry.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, _ := pair(t)
				_, err := s.Expect(t.Context(), []Case{{Pattern: Timeout(), Handler: func(context.Context, *Session, Result) (Action, error) {
					time.Sleep(entry.sleep)
					return entry.action, nil
				}}}, WithTimeout(2*time.Second), WithIdleTimeout(entry.idle))
				if !errors.Is(err, entry.want) {
					t.Fatalf("want=%v err=%v", entry.want, err)
				}
			})
		})
	}
}
func TestGroupContinueCanCompleteAnotherSession(t *testing.T) {
	t.Run("continue resets all relative timers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a, p := pair(t)
			b, q := pair(t)
			go func() { time.Sleep(time.Second); _, _ = p.Write([]byte("PAGE")) }()
			go func() { time.Sleep(2500 * time.Millisecond); _, _ = q.Write([]byte("READY")) }()
			i, _, err := expectGroupIndex(t.Context(), []*Session{a, b}, []Case{{Pattern: Literal("PAGE"), Handler: func(context.Context, *Session, Result) (Action, error) { return Continue, nil }}, {Pattern: Literal("READY")}}, WithTimeout(2*time.Second))
			if err != nil || i != 1 {
				t.Fatalf("index=%d err=%v", i, err)
			}
		})
	})

	t.Run("cross-session group priority", func(t *testing.T) {
		calls := 0
		a, _ := pair(t)
		b, _ := pair(t, WithBefore(Case{Pattern: Literal("PAGE"), Handler: func(context.Context, *Session, Result) (Action, error) { calls++; return ContinueKeepTimer, nil }}))
		a.receive.pending = []byte("READY")
		// Leave the Before match staged to exercise admission before arbitration.
		b.receive.staged = []byte("PAGE")
		i, _, err := expectGroupIndex(t.Context(), []*Session{a, b}, []Case{{Pattern: Literal("READY")}})
		if err != nil || i != 0 || calls != 1 {
			t.Fatalf("index=%d before=%d err=%v", i, calls, err)
		}
	})
	t.Run("same-group round robin", func(t *testing.T) {
		a, _ := pair(t)
		b, _ := pair(t)
		a.receive.pending, b.receive.pending = []byte("XX"), []byte("X")
		var seen []*Session
		i, _, err := expectGroupIndex(t.Context(), []*Session{a, b}, []Case{{Pattern: Literal("X"), Handler: func(_ context.Context, s *Session, _ Result) (Action, error) {
			seen = append(seen, s)
			if len(seen) == 3 {
				return Return, nil
			}
			return ContinueKeepTimer, nil
		}}})
		if err != nil || i != 0 || len(seen) != 3 || seen[0] != a || seen[1] != b || seen[2] != a {
			t.Fatalf("index=%d callbacks=%d err=%v", i, len(seen), err)
		}
	})

	a, p := pair(t)
	b, q := pair(t)
	seen := make(chan struct{})
	go func() { _, _ = p.Write([]byte("PAGE")) }()
	go func() { <-seen; _, _ = q.Write([]byte("READY")) }()
	i, _, err := expectGroupIndex(t.Context(), []*Session{a, b}, []Case{{Pattern: Literal("PAGE"), Handler: func(context.Context, *Session, Result) (Action, error) { close(seen); return ContinueKeepTimer, nil }}, {Pattern: Literal("READY")}}, WithTimeout(time.Second))
	if err != nil || i != 1 {
		t.Fatalf("session=%d err=%v", i, err)
	}
}

// expectGroupIndex keeps existing arbitration regressions expressed as indexes.
func expectGroupIndex(ctx context.Context, sessions []*Session, cases []Case, opts ...ExpectOption) (int, Result, error) {
	g, err := NewGroup(sessions...)
	if err != nil {
		return -1, Result{CaseIndex: -1, RuleIndex: -1}, err
	}
	winner, r, err := g.Expect(ctx, cases, opts...)
	for i, s := range sessions {
		if s == winner {
			return i, r, err
		}
	}
	return -1, r, err
}
