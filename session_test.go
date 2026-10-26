package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func pair(t *testing.T, opts ...SessionOption) (*Session, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	s, err := NewSession(a, opts...)
	if err != nil {
		a.Close()
		b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); b.Close() })
	return s, b
}
func waitText(t *testing.T, s *Session, text string) Result {
	t.Helper()
	r, err := s.Expect(t.Context(), []Case{{Pattern: Literal(text)}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestSessionRawBytesAndTail(t *testing.T) {
	s, p := pair(t)
	go func() { p.Write([]byte{0xff, '!', 't', 'a', 'i', 'l'}); p.Close() }()
	r := waitText(t, s, "!")
	if r.Before != string([]byte{0xff}) {
		t.Fatalf("before bytes %x", r.Before)
	}
	waitText(t, s, "tail")
	if _, err := s.Expect(t.Context(), []Case{{Pattern: EOF()}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("missing")}}); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}
func TestSessionObservers(t *testing.T) {
	calls := 0
	s, p := pair(t, WithObserver(func(Event) { calls++ }, func(Event) { calls++ }))
	go p.Write([]byte("ok"))
	waitText(t, s, "ok")
	if calls != 2 {
		t.Fatalf("calls %d", calls)
	}
}
func TestSessionTimeoutPreservesBuffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		go p.Write([]byte("partial"))
		r, err := s.Expect(t.Context(), []Case{{Pattern: Literal("missing")}}, WithTimeout(time.Second))
		if !errors.Is(err, ErrTimeout) || r.Before != "partial" {
			t.Fatalf("%v %q", err, r.Before)
		}
		go p.Write([]byte("done"))
		r, err = s.Expect(t.Context(), []Case{{Pattern: Literal("done")}}, WithTimeout(0))
		if err != nil || r.Before != "partial" {
			t.Fatalf("%v %q", err, r.Before)
		}
	})
}
func TestSessionReadCancellationKeepsConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		ctx, cancel := context.WithCancel(t.Context())
		go func() { time.Sleep(time.Second); cancel() }()
		if _, err := s.Expect(ctx, []Case{{Pattern: Literal("ok")}}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		go p.Write([]byte("ok"))
		waitText(t, s, "ok")
	})
}
func TestSessionBufferLimitRecoverable(t *testing.T) {
	s, p := pair(t, WithBufferLimit(4))
	go p.Write([]byte("abcdDONE"))
	r, err := s.Expect(t.Context(), []Case{{Pattern: Literal("missing")}})
	if !errors.Is(err, ErrBufferLimit) || r.Before != "abcd" {
		t.Fatalf("%v %q", err, r.Before)
	}
	waitText(t, s, "abcd")
	waitText(t, s, "DONE")
}
func TestSessionCloseUnblocksAndIsIdempotent(t *testing.T) {
	s, _ := pair(t)
	result := make(chan error, 1)
	go func() { _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("never")}}); result <- err }()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestSessionCanceledSendDoesNotClose(t *testing.T) {
	s, p := pair(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n, err := s.Send(ctx, "bad"); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("%d %v", n, err)
	}
	done := make(chan string, 1)
	go func() { buf := make([]byte, 3); io.ReadFull(p, buf); done <- string(buf) }()
	if n, err := s.Send(t.Context(), "yes"); n != 3 || err != nil {
		t.Fatalf("%d %v", n, err)
	}
	if <-done != "yes" {
		t.Fatal("wrong bytes")
	}
}
func TestSessionBlockedSendCancellationCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		n, err := s.Send(ctx, "confirm")
		var we *WriteError
		if n != 0 || !errors.As(err, &we) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%d %v", n, err)
		}
		if _, err = s.Send(t.Context(), "again"); !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
		if _, err = p.Read(make([]byte, 1)); err == nil {
			t.Fatal("peer stayed open")
		}
	})
}

// A controlled transport exposes a real short-write boundary to the Session.
type shortTransport struct {
	once   sync.Once
	closed chan struct{}
	writes int
}

func (p *shortTransport) Read([]byte) (int, error)    { <-p.closed; return 0, io.EOF }
func (p *shortTransport) Write(b []byte) (int, error) { p.writes++; return 2, nil }
func (p *shortTransport) Close() error                { p.once.Do(func() { close(p.closed) }); return nil }
func TestSessionShortWriteNeverRetries(t *testing.T) {
	p := &shortTransport{closed: make(chan struct{})}
	s, err := NewSession(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	n, err := s.Send(t.Context(), "yes\r")
	var we *WriteError
	if n != 2 || !errors.As(err, &we) || we.Written != 2 || !errors.Is(err, io.ErrShortWrite) || p.writes != 1 {
		t.Fatalf("%d %v writes=%d", n, err, p.writes)
	}
}
func TestSessionTranscript(t *testing.T) {
	var out bytes.Buffer
	s, p := pair(t, WithTranscript(&out))
	if err := s.Redact("password", "aba", "bab"); err != nil {
		t.Fatal(err)
	}
	go func() {
		for _, part := range []string{"pa", "ssword ab", "ab done"} {
			p.Write([]byte(part))
		}
		p.Close()
	}()
	waitText(t, s, "done")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "******** **** done" {
		t.Fatalf("transcript %q", got)
	}
}
func TestSessionInvalidOptionsLeaveTransportOwnedByCaller(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if _, err := NewSession(a, WithBufferLimit(-1)); err == nil {
		t.Fatal("accepted negative limit")
	}
	go b.Write([]byte("x"))
	buf := make([]byte, 1)
	if _, err := a.Read(buf); err != nil || string(buf) != "x" {
		t.Fatalf("%q %v", buf, err)
	}
}
func TestSessionEOFWithData(t *testing.T) {
	p := &finalReader{data: []byte("hello!")}
	s, err := NewSession(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	waitText(t, s, "!")
	if _, err = s.Expect(t.Context(), []Case{{Pattern: EOF()}}); err != nil {
		t.Fatal(err)
	}
}

type finalReader struct{ data []byte }

func (p *finalReader) Read(b []byte) (int, error) {
	n := copy(b, p.data)
	p.data = p.data[n:]
	return n, io.EOF
}
func (p *finalReader) Write(b []byte) (int, error) { return len(b), nil }
func (p *finalReader) Close() error                { return nil }
func BenchmarkLiteral16K(b *testing.B) {
	text := strings.Repeat("x", 16*1024) + "done!"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c, err := NewConsole()
		if err != nil {
			b.Fatal(err)
		}
		go c.TTY().Write([]byte(text))
		_, err = c.Session().Expect(context.Background(), []Case{{Pattern: Literal("done!")}})
		c.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
