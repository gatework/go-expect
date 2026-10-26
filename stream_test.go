package expect

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDeclarationPriorityOptIn(t *testing.T) {
	for _, priority := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "declaration"}[priority], func(t *testing.T) {
			a, b := net.Pipe()
			s, _ := NewSession(a)
			defer s.Close()
			defer b.Close()
			go func() { _, _ = io.WriteString(b, "early late") }()
			var opts []ExpectOption
			if priority {
				opts = append(opts, WithDeclarationPriority())
			}
			r, e := s.Expect(t.Context(), []Case{{Pattern: Literal("late")}, {Pattern: Literal("early")}}, opts...)
			want := 1
			if priority {
				want = 0
			}
			if e != nil || r.CaseIndex != want {
				t.Fatalf("case=%d err=%v", r.CaseIndex, e)
			}
		})
	}
}
func TestStreamingFallbackRetainsBoundary(t *testing.T) {
	a, b := net.Pipe()
	s, _ := NewSession(a, WithBufferLimit(64<<10))
	defer s.Close()
	defer b.Close()
	input := strings.Repeat("x", (64<<10)-2) + "boundary!"
	go func() { _, _ = io.WriteString(b, input) }()
	var streamed string
	r, e := s.Expect(t.Context(), []Case{{Pattern: Literal("boundary!")}}, WithStreamingFallback(func(_ context.Context, _ *Session, r Result) (Action, error) {
		streamed += r.Match
		return Continue, nil
	}))
	if e != nil || len(streamed) != 32<<10 || streamed+r.Before+r.Match != input {
		t.Fatalf("streamed=%d before=%d match=%q err=%v", len(streamed), len(r.Before), r.Match, e)
	}
}
func TestStreamingFallbackDoesNotResetTimeout(t *testing.T) {
	a, b := net.Pipe()
	s, _ := NewSession(a)
	defer s.Close()
	defer b.Close()
	go func() {
		for {
			if _, e := io.WriteString(b, strings.Repeat("x", 64<<10)); e != nil {
				return
			}
		}
	}()
	start := time.Now()
	_, e := s.Expect(t.Context(), []Case{{Pattern: Literal("never")}}, WithTimeout(40*time.Millisecond), WithStreamingFallback(func(context.Context, *Session, Result) (Action, error) { return Continue, nil }))
	if !errors.Is(e, ErrTimeout) || time.Since(start) > time.Second {
		t.Fatalf("err=%v", e)
	}
}

type suffixTransport struct {
	mu     sync.Mutex
	got    string
	closed chan struct{}
	once   sync.Once
	fail   bool
}

func (p *suffixTransport) Read([]byte) (int, error) { <-p.closed; return 0, io.EOF }
func (p *suffixTransport) Close() error             { p.once.Do(func() { close(p.closed) }); return nil }
func (p *suffixTransport) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := min(2, len(b))
	p.got += string(b[:n])
	if p.fail && len(p.got) >= 4 {
		return n, io.ErrClosedPipe
	}
	return n, nil
}
func TestSendAllSuffixAndDefaultSend(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "all"}[all], func(t *testing.T) {
			p := &suffixTransport{closed: make(chan struct{})}
			s, _ := NewSession(p)
			defer s.Close()
			var n int
			var e error
			if all {
				n, e = s.SendAll(t.Context(), "abcdef")
			} else {
				n, e = s.Send(t.Context(), "abcdef")
			}
			if all {
				if n != 6 || e != nil || p.got != "abcdef" {
					t.Fatalf("n=%d err=%v got=%q", n, e, p.got)
				}
			} else if n != 2 || !errors.Is(e, io.ErrShortWrite) {
				t.Fatalf("n=%d err=%v", n, e)
			}
		})
	}
}
func TestSendAllFailureProgressCloses(t *testing.T) {
	p := &suffixTransport{closed: make(chan struct{}), fail: true}
	s, _ := NewSession(p)
	n, e := s.SendAll(t.Context(), "abcdef")
	var w *WriteError
	if n != 4 || !errors.As(e, &w) || w.Written != 4 || !s.isClosed() || p.got != "abcd" {
		t.Fatalf("n=%d err=%v got=%q", n, e, p.got)
	}
}
func TestStandaloneRedactorScopedSwitch(t *testing.T) {
	r, e := NewRedactor([]string{"secret"}, "[X]")
	if e != nil {
		t.Fatal(e)
	}
	out := string(r.Append([]byte("a sec")))
	tail, e := r.SetSecrets(nil)
	if e != nil {
		t.Fatal(e)
	}
	out += string(tail) + string(r.Append([]byte("ret"))) + string(r.Finish())
	if out != "a [X]ret" {
		t.Fatalf("got %q", out)
	}
	r2, _ := NewRedactor(nil, "?")
	if string(r2.Append([]byte("secret"))) != "secret" {
		t.Fatal("rules escaped scope")
	}
}
func TestPollUnsupported(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	s, _ := NewSession(a)
	defer s.Close()
	_, e := s.Poll(t.Context(), []Case{{Pattern: Literal("x")}})
	if !errors.Is(e, ErrPollUnsupported) {
		t.Fatal(e)
	}
}

func TestSendAllCancellationAndBackpressureLimit(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "overflow"}[overflow], func(t *testing.T) {
			a, b := net.Pipe()
			s, _ := NewSession(a, WithBufferLimit(16<<10))
			defer s.Close()
			defer b.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			if overflow {
				go func() { _, _ = io.WriteString(b, strings.Repeat("o", 128<<10)) }()
			}
			n, e := s.SendAll(ctx, strings.Repeat("i", 128<<10))
			want := error(context.DeadlineExceeded)
			if overflow {
				want = ErrBufferLimit
			}
			if n != 0 || !errors.Is(e, want) || !s.isClosed() {
				t.Fatalf("n=%d err=%v", n, e)
			}
		})
	}
}
func TestStreamingOrdinaryMatchBeforeFallback(t *testing.T) {
	a, b := net.Pipe()
	s, _ := NewSession(a)
	defer s.Close()
	defer b.Close()
	input := strings.Repeat("x", 64<<10) + "end"
	go func() { io.WriteString(b, input) }()
	called := false
	r, e := s.Expect(t.Context(), []Case{{Pattern: Literal(strings.Repeat("x", 64<<10))}}, WithStreamingFallback(func(context.Context, *Session, Result) (Action, error) { called = true; return Return, nil }))
	if e != nil || called || len(r.Match) != 64<<10 {
		t.Fatalf("called=%v len=%d err=%v", called, len(r.Match), e)
	}
}
func TestStreamingRedactorChunkOverlap(t *testing.T) {
	for _, chunk := range []int{1, 2, 3, 7, 100} {
		t.Run(string(rune('a'+chunk)), func(t *testing.T) {
			r, _ := NewRedactor([]string{"aba", "bab", "secret"}, "[X]")
			input := "xabababy secret sec"
			var out []byte
			for len(input) > 0 {
				n := min(chunk, len(input))
				out = append(out, r.Append([]byte(input[:n]))...)
				input = input[n:]
			}
			out = append(out, r.Finish()...)
			if string(out) != "x[X]y [X] [X]" {
				t.Fatalf("got %q", out)
			}
		})
	}
}

func TestSendAllCancellationWinsCompletedWrite(t *testing.T) {
	for range 50 {
		ctx, cancel := context.WithCancel(t.Context())
		p := &cancelingWriter{cancel: cancel}
		s, _ := NewSession(p)
		n, e := s.SendAll(ctx, "done")
		s.Close()
		cancel()
		if n != 4 || !errors.Is(e, context.Canceled) {
			t.Fatalf("n=%d err=%v", n, e)
		}
	}
}
func TestSendAllConcurrentExpectKeepsInputOrder(t *testing.T) {
	a, b := net.Pipe()
	s, _ := NewSession(a, WithBufferLimit(256<<10))
	defer s.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	input := strings.Repeat("i", 128<<10)
	output := strings.Repeat("o", 128<<10) + "done"
	peer := make(chan error, 1)
	go func() {
		if _, e := io.WriteString(b, output); e != nil {
			peer <- e
			return
		}
		got := make([]byte, len(input))
		_, e := io.ReadFull(b, got)
		if e == nil && string(got) != input {
			e = errors.New("input differs")
		}
		peer <- e
	}()
	sent := make(chan error, 1)
	go func() { _, e := s.SendAll(ctx, input); sent <- e }()
	r, e := s.Expect(ctx, []Case{{Pattern: Literal("done")}})
	if e != nil || r.Before+r.Match != output {
		t.Fatalf("len=%d err=%v", len(r.Before)+len(r.Match), e)
	}
	if e = <-sent; e != nil {
		t.Fatal(e)
	}
	if e = <-peer; e != nil {
		t.Fatal(e)
	}
}
func TestSendAllFailureDoesNotLeakPayloadInFormatting(t *testing.T) {
	p := &suffixTransport{closed: make(chan struct{}), fail: true}
	s, _ := NewSession(p)
	defer s.Close()
	_, e := s.SendAll(t.Context(), "secret")
	if e == nil || strings.Contains(e.Error(), "secret") {
		t.Fatal("write error exposed payload")
	}
}
