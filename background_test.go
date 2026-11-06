// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

func TestBackgroundLifecycle(t *testing.T) {
	t.Run("pause foreground resume", func(t *testing.T) {
		s, p := pair(t)
		b, err := s.StartBackground(t.Context(), []Case{{Pattern: Literal("background")}})
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Pause(t.Context()); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = p.Write([]byte("foreground")) }()
		waitText(t, s, "foreground")
		if err = b.Resume(t.Context()); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = p.Write([]byte("background")) }()
		result, err := b.Wait(t.Context())
		if err != nil || result.Match != "background" || result.SessionID != s.ID() {
			t.Fatalf("result=%v err=%v", result, err)
		}
	})
	t.Run("paused stop does not inspect foreground input", func(t *testing.T) {
		s, p := pair(t)
		b, e := s.StartBackground(t.Context(), []Case{{Pattern: Literal("bg")}})
		if e != nil {
			t.Fatal(e)
		}
		if e = b.Pause(t.Context()); e != nil {
			t.Fatal(e)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		foreground := make(chan error, 1)
		go func() {
			_, e := s.Expect(t.Context(), []Case{{Pattern: Literal("fg"), Handler: func(context.Context, *Session, Result) (Action, error) { close(entered); <-release; return Return, nil }}})
			foreground <- e
		}()
		go func() { _, _ = p.Write([]byte("fgprivate tail")) }()
		<-entered
		if e = b.Stop(t.Context()); e != nil {
			t.Fatal(e)
		}
		result, e := b.Wait(t.Context())
		close(release)
		if result.Before != "" || !errors.Is(e, context.Canceled) {
			t.Fatalf("paused result leaked foreground state: %v err=%v", result, e)
		}
		if e = <-foreground; e != nil {
			t.Fatal(e)
		}
	})

	for _, kind := range []CallbackFailure{CallbackPanicked, CallbackExited} {
		t.Run(string(kind), func(t *testing.T) {
			s, p := pair(t)
			b, err := s.StartBackground(t.Context(), []Case{{Pattern: Literal("x"), Handler: func(context.Context, *Session, Result) (Action, error) {
				if kind == CallbackPanicked {
					panic("private callback payload")
				}
				runtime.Goexit()
				return Return, nil
			}}})
			if err != nil {
				t.Fatal(err)
			}
			go func() { _, _ = p.Write([]byte("x")) }()
			_, err = b.Wait(t.Context())
			var callback *CallbackError
			if !errors.As(err, &callback) || callback.Kind != kind || s.expecting.Load() {
				t.Fatalf("err=%v busy=%v", err, s.expecting.Load())
			}
		})
	}
	t.Run("default has no relative expiry", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, _ := pair(t)
			b, err := s.StartBackground(t.Context(), []Case{{Pattern: Literal("x")}})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(31 * time.Second)
			if !s.expecting.Load() {
				t.Fatal("background inherited foreground timeout")
			}
			if err = b.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	})
}
