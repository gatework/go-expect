// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
func TestSessionCloseReportsOriginalCause(t *testing.T) {
	failure := errors.New("close failure")
	a, b := net.Pipe()
	defer b.Close()
	s, err := NewSession(&closeFailure{Conn: a, err: failure})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.Close(); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
}

type closeFailure struct {
	net.Conn
	err error
}

func (p *closeFailure) Close() error { p.Conn.Close(); return p.err }
func TestSessionReadErrorWithDataPreservesTail(t *testing.T) {
	for _, count := range []int{-1, readChunk + 1} {
		for _, cause := range []error{io.ErrUnexpectedEOF, io.EOF} {
			s, err := NewSession(&errorReader{err: cause, invalidCount: count})
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.Expect(t.Context(), []Case{{Pattern: EOF()}})
			s.Close()
			if err == nil || !errors.Is(err, cause) || errorClass(err) != "error" {
				t.Fatalf("count=%d lost cause %v: %v", count, cause, err)
			}
		}
	}

	failure := errors.New("read failed")
	s, err := NewSession(&errorReader{err: failure})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	waitText(t, s, "ok")
	r, err := s.Expect(t.Context(), []Case{{Pattern: Literal("missing")}})
	if !errors.Is(err, failure) || r.Before != "tail" {
		t.Fatalf("%v %q", err, r.Before)
	}
	waitText(t, s, "tail")

	for _, cause := range []struct {
		name   string
		err    error
		failed bool
	}{
		{"wrapped EOF", fmt.Errorf("read: %w", io.EOF), false},
		{"joined EOF", errors.Join(io.EOF, io.EOF), false},
		{"joined failure", errors.Join(io.EOF, failure), true},
		{"wrapped joined failure", fmt.Errorf("read: %w", errors.Join(io.EOF, failure)), true},
	} {
		for _, pattern := range []Pattern{EOF(), Default()} {
			for _, split := range []bool{false, true} {
				for _, continued := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/split=%v/continued=%v", cause.name, pattern, split, continued), func(t *testing.T) {
						s, err := NewSession(&errorReader{err: cause.err, split: split})
						if err != nil {
							t.Fatal(err)
						}
						defer s.Close()
						waitText(t, s, "ok")
						selected := Case{Pattern: pattern}
						called := false
						if continued {
							selected.Handler = func(context.Context, *Session, Result) (Action, error) {
								called = true
								return ContinueKeepTimer, nil
							}
						}
						r, err := s.Expect(t.Context(), []Case{selected})
						if r.Before != "tail" {
							t.Fatalf("tail = %q, want tail", r.Before)
						}
						if cause.failed {
							if !errors.Is(err, failure) || !errors.Is(err, io.EOF) || called {
								t.Fatalf("lost read failure or ran EOF handler: err=%v called=%v", err, called)
							}
							waitText(t, s, "tail")
						} else if continued {
							if !errors.Is(err, io.EOF) || !called {
								t.Fatalf("EOF continuation: err=%v called=%v", err, called)
							}
						} else if err != nil {
							t.Fatalf("clean EOF: %v", err)
						}
					})
				}
			}
		}
	}
}

type errorReader struct {
	err          error
	read         bool
	split        bool
	invalidCount int
}

func (r *errorReader) Read(p []byte) (int, error) {
	if r.invalidCount != 0 {
		return r.invalidCount, r.err
	}
	if r.read {
		return 0, r.err
	}
	r.read = true
	if r.split {
		return copy(p, []byte("oktail")), nil
	}
	return copy(p, []byte("oktail")), r.err
}
func (r *errorReader) Write(p []byte) (int, error) { return len(p), nil }
func (r *errorReader) Close() error                { return nil }
func TestSessionNoProgressReader(t *testing.T) {
	s, err := NewSession(emptyReader{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, err = s.Expect(t.Context(), []Case{{Pattern: EOF()}})
	if !errors.Is(err, io.ErrNoProgress) {
		t.Fatal(err)
	}
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error)    { return 0, nil }
func (emptyReader) Write(p []byte) (int, error) { return len(p), nil }
func (emptyReader) Close() error                { return nil }

func TestSessionExactBufferLimitWithSplitEOF(t *testing.T) {
	for _, together := range []bool{true, false} {
		name := "separate EOF"
		if together {
			name = "data and EOF"
		}
		t.Run(name, func(t *testing.T) {
			s, err := NewSession(&splitEOFReader{together: together}, WithBufferLimit(4))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, err := s.Expect(t.Context(), []Case{{Pattern: EOF()}})
			if err != nil || r.Before != "abcd" {
				t.Fatalf("exact-size input: %v %q", err, r.Before)
			}
		})
	}
}

type splitEOFReader struct{ together, read bool }

func (r *splitEOFReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	n := copy(p, []byte("abcd"))
	if r.together {
		return n, io.EOF
	}
	return n, nil
}
func (r *splitEOFReader) Write(p []byte) (int, error) { return len(p), nil }
func (r *splitEOFReader) Close() error                { return nil }
