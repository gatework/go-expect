// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
)

func TestReceiveReleasesConsumedAndDetachesSmallRemainder(t *testing.T) {
	for _, tail := range []string{"", "tail"} {
		t.Run(map[bool]string{true: "empty", false: "small"}[tail == ""], func(t *testing.T) {
			a, b := net.Pipe()
			s, err := NewSession(a)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			defer b.Close()
			input := append(bytes.Repeat([]byte("x"), 128<<10), '!')
			input = append(input, tail...)
			if err := s.acceptEvent(readEvent{data: input}); err != nil {
				t.Fatal(err)
			}
			result, err := s.Expect(t.Context(), []Case{{Pattern: Literal("!")}})
			if err != nil || len(result.Before) != 128<<10 || result.Match != "!" {
				t.Fatalf("before=%d match=%q err=%v", len(result.Before), result.Match, err)
			}
			if s.receive.staged != nil {
				t.Fatal("exhausted staged input retains its backing allocation")
			}
			if tail == "" {
				if s.receive.pending != nil {
					t.Fatal("exhausted pending input retains its backing allocation")
				}
				return
			}
			if string(s.receive.pending) != tail || &s.receive.pending[0] == &input[len(input)-len(tail)] {
				t.Fatal("small remainder still owns the consumed burst allocation")
			}
			clear(input)
			if string(s.receive.pending) != tail || len(result.Before) != 128<<10 || result.Before[0] != 'x' {
				t.Fatal("detached remainder or returned result changed")
			}
		})
	}
}

func TestReceiveCancellationDetachesAfterContinuation(t *testing.T) {
	a, b := net.Pipe()
	s, err := NewSession(a)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer b.Close()
	input := append(bytes.Repeat([]byte("x"), 128<<10), "!tail"...)
	if err := s.acceptEvent(readEvent{data: input}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, err := s.Expect(ctx, []Case{{Pattern: Literal("!"), Handler: func(context.Context, *Session, Result) (Action, error) {
		cancel()
		return ContinueKeepTimer, nil
	}}})
	if !errors.Is(err, context.Canceled) || result.Match != "!" || string(s.receive.pending) != "tail" {
		t.Fatalf("result=%q pending=%q err=%v", result.Match, s.receive.pending, err)
	}
	if &s.receive.pending[0] == &input[len(input)-4] {
		t.Fatal("canceled operation retains the consumed burst allocation")
	}
	result, err = s.Expect(t.Context(), []Case{{Pattern: Literal("tail")}})
	if err != nil || result.Match != "tail" {
		t.Fatalf("remainder=%q err=%v", result.Match, err)
	}
}
