// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGroupDynamicMembership(t *testing.T) {
	a, p := pair(t)
	b, q := pair(t)
	g, err := NewGroup(a)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		s, r, e := g.Expect(ctx, []Case{{Pattern: Literal("READY"), Sessions: []*Session{b}}})
		if e == nil && (s != b || r.SessionID != b.ID()) {
			e = errors.New("wrong selected identity")
		}
		done <- e
	}()
	for !a.expecting.Load() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err = g.Add(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err = g.Remove(ctx, a); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = p.Write([]byte("released")) }()
	waitText(t, a, "released")
	go func() { _, _ = q.Write([]byte("READY")) }()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRulesReplaceAndNoConsume(t *testing.T) {
	t.Run("replacement wakes active wait", func(t *testing.T) {
		s, p := pair(t)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			r, e := s.Expect(ctx, []Case{{Pattern: Literal("never")}})
			if e == nil && (r.Scope != ScopeBefore || r.RuleIndex != 0) {
				e = errors.New("wrong rule origin")
			}
			done <- e
		}()
		if err := s.SetRules(RuleSnapshot{Before: []Case{{Pattern: Literal("READY")}}}); err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = p.Write([]byte("READY")) }()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := s.SetRules(RuleSnapshot{}); err != nil {
			t.Fatal(err)
		}
		if len(s.Rules().Before) != 0 {
			t.Fatal("rules were not cleared")
		}
	})
	t.Run("nonconsuming continue makes progress", func(t *testing.T) {
		s, p := pair(t)
		calls := 0
		go func() { _, _ = p.Write([]byte("OBSEND")) }()
		r, err := s.Expect(t.Context(), []Case{{Pattern: Literal("OBS"), NoConsume: true, Handler: func(context.Context, *Session, Result) (Action, error) { calls++; return ContinueKeepTimer, nil }}, {Pattern: Literal("END")}})
		if err != nil || calls != 1 || r.Before != "OBS" {
			t.Fatalf("calls=%d before=%q err=%v", calls, r.Before, err)
		}
	})
}
