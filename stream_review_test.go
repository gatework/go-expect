package expect

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Inspect only while holding the same gate as queue mutation; timing out here
// also exposes a receiver that reserves that gate while waiting for future I/O.
func waitQueuedBytes(t *testing.T, s *Session, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case s.inputGate <- struct{}{}:
			n := s.queuedBytes
			<-s.inputGate
			if n >= want {
				return
			}
		case <-ctx.Done():
			t.Fatalf("queued bytes did not become visible: want %d", want)
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("queued bytes did not reach %d", want)
		}
	}
}
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

func (p *quietEOFTransport) Read(data []byte) (int, error) { return p.input.Read(data) }
func (p *quietEOFTransport) Write(data []byte) (int, error) {
	select {
	case <-p.allowWrite:
		return len(data), nil
	case <-p.closed:
		return 0, io.ErrClosedPipe
	}
}
func (p *quietEOFTransport) Close() error { p.once.Do(func() { close(p.closed) }); return nil }
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
