package expect

import (
	"errors"
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
	for i := 0; i < 2; i++ {
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
}

type errorReader struct {
	err  error
	read bool
}

func (r *errorReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, r.err
	}
	r.read = true
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
