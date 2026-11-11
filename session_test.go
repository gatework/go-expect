// SPDX-License-Identifier: Apache-2.0

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
	s, p := pair(t, WithOutput(&out))
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

func TestInteractiveRedactionAndInputFiltering(t *testing.T) {
	s, p := pair(t, WithRemoveNulls(true), WithStripParity(true))
	if err := s.Redact("secret"); err != nil {
		t.Fatal(err)
	}
	inputR, inputW := io.Pipe()
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- s.Interact(t.Context(), inputR, &out, WithInteractOutputCases(Case{Pattern: Literal("DONE")}))
	}()
	// High bits and NUL removal happen before matching and diagnostic rendering.
	_, err := p.Write([]byte{0xf3, 0, 0xe5, 0xe3, 0xf2, 0xe5, 0xf4, 'D', 'O', 'N', 'E'})
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	_ = inputW.Close()
	if out.String() != "******DONE" {
		t.Fatalf("output=%q", out.String())
	}
}

func TestInteractiveEscapeJoinsAndPreservesSession(t *testing.T) {
	s, _ := pair(t)
	inputR, inputW := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.Interact(t.Context(), inputR, io.Discard) }()
	if _, err := inputW.Write([]byte{0x1d}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = inputW.Close()
	if s.isClosed() {
		t.Fatal("escape retired idle session")
	}

	t.Run("parent cancellation", func(t *testing.T) {
		s, _ := pair(t)
		inputR, inputW := io.Pipe()
		defer inputW.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() { _, _ = inputW.Write([]byte("x")) }()
		err := s.Interact(ctx, inputR, io.Discard, WithInteractInputCases(Case{Pattern: Literal("x"), Handler: Then(func(context.Context, *Session, Result) error { cancel(); return nil })}))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancellation lost: %v", err)
		}
	})
	t.Run("handler panic joins local reader", func(t *testing.T) {
		s, p := pair(t)
		inputR, inputW := io.Pipe()
		defer inputW.Close()
		go func() { _, _ = p.Write([]byte("panic")) }()
		func() {
			defer func() {
				if recover() != "handler panic" {
					t.Error("handler panic lost")
				}
			}()
			_ = s.Interact(t.Context(), inputR, io.Discard, WithInteractOutputCases(Case{Pattern: Literal("panic"), Handler: func(context.Context, *Session, Result) (Action, error) { panic("handler panic") }}))
		}()
		if _, err := inputW.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal("input ownership was not released")
		}
		if s.expecting.Load() {
			t.Fatal("receive reservation leaked")
		}
	})
}

func TestSlowAndNullSendRecordedSafely(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sent bytes.Buffer
		s, p := pair(t, WithSendOutput(&sent))
		if err := s.Redact("secret"); err != nil {
			t.Fatal(err)
		}
		done := make(chan string, 1)
		go func() { b := make([]byte, 8); _, _ = io.ReadFull(p, b); done <- string(b) }()
		if n, err := s.SendSlow(t.Context(), "secret", 2, time.Second); n != 6 || err != nil {
			t.Fatalf("n=%d err=%v", n, err)
		}
		if n, err := s.SendNull(t.Context(), 2); n != 2 || err != nil {
			t.Fatalf("n=%d err=%v", n, err)
		}
		if got := <-done; got != "secret\x00\x00" {
			t.Fatal("transport payload changed")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if sent.String() != "******\x00\x00" {
			t.Fatal("send recording leaked or changed bytes")
		}
	})
}

func TestInteractionEvolution(t *testing.T) {
	t.Run("input full buffer event", func(t *testing.T) {
		s, p := pair(t, WithBufferLimit(8))
		seen := ""
		got := make(chan string, 1)
		go func() { buf := make([]byte, 4); _, _ = io.ReadFull(p, buf); got <- string(buf) }()
		err := s.Interact(t.Context(), io.NopCloser(strings.NewReader("aaaaaaaaaaaa")), io.Discard, WithInteractInputCases(Case{Pattern: mustPattern(t, `a+b`)}, Case{Pattern: FullBuffer(), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
			seen = r.Before
			return ContinueKeepTimer, nil
		}}))
		if err != nil || seen != "aaaaaaaa" {
			t.Fatalf("snapshot=%q err=%v", seen, err)
		}
		if result := <-got; result != "aaaa" {
			t.Fatal(result)
		}
	})
	t.Run("filtered remote forwarding preserves tail", func(t *testing.T) {
		s, p := pair(t)
		input, writer := io.Pipe()
		defer writer.Close()
		go func() { _, _ = p.Write([]byte("prefixHIDEtail")) }()
		var output bytes.Buffer
		err := s.Interact(t.Context(), input, &output, WithInteractOutputForward(false), WithInteractOutputCases(Case{Pattern: Literal("HIDE")}))
		if err != nil || output.String() != "prefix" {
			t.Fatalf("output=%q err=%v", output.String(), err)
		}
		waitText(t, s, "tail")
	})

	t.Run("callback EOF remains an error", func(t *testing.T) {
		s, _ := pair(t)
		failure := errors.New("callback failure")
		err := s.Interact(t.Context(), io.NopCloser(strings.NewReader("x")), io.Discard, WithInteractInputCases(Case{Pattern: Literal("x"), Handler: func(context.Context, *Session, Result) (Action, error) { return Return, errors.Join(failure, io.EOF) }}))
		if !errors.Is(err, failure) || !errors.Is(err, io.EOF) {
			t.Fatalf("callback cause lost: %v", err)
		}
	})
	t.Run("regexp input and captures", func(t *testing.T) {
		s, p := pair(t)
		result := make(chan string, 1)
		go func() { buf := make([]byte, 6); _, _ = io.ReadFull(p, buf); result <- string(buf) }()
		var capture string
		err := s.Interact(t.Context(), io.NopCloser(&chunkStream{Reader: bytes.NewReader([]byte("xxhello42!tail")), chunk: 1}), io.Discard, WithInteractInputCases(Case{Pattern: mustPattern(t, `hello(\d+)!`), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
			capture = r.Captures[0].Text
			return ContinueKeepTimer, nil
		}}))
		if err != nil || capture != "42" {
			t.Fatalf("capture=%q err=%v", capture, err)
		}
		if got := <-result; got != "xxtail" {
			t.Fatalf("forwarded %q", got)
		}
	})
	t.Run("local echo and passthrough", func(t *testing.T) {
		s, p := pair(t)
		_ = s.Redact("~~")
		received := make(chan string, 1)
		go func() { buf := make([]byte, 2); _, _ = io.ReadFull(p, buf); received <- string(buf) }()
		var output bytes.Buffer
		err := s.Interact(t.Context(), io.NopCloser(strings.NewReader("~~")), &output, WithInteractInputCases(Case{Pattern: Literal("~~")}), WithInteractInputForward(true), WithInteractEcho(true))
		if err != nil || output.String() != "**" {
			t.Fatalf("echo=%q err=%v", output.String(), err)
		}
		if got := <-received; got != "~~" {
			t.Fatal(got)
		}
	})
	t.Run("local EOF event", func(t *testing.T) {
		s, _ := pair(t)
		seen := false
		err := s.Interact(t.Context(), io.NopCloser(strings.NewReader("")), io.Discard, WithInteractInputCases(Case{Pattern: EOF(), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
			seen = r.Source == SourceInput
			return Return, nil
		}}))
		if err != nil || !seen {
			t.Fatalf("seen=%v err=%v", seen, err)
		}
	})
	t.Run("local idle event", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s, _ := pair(t)
			input, writer := io.Pipe()
			defer writer.Close()
			seen := false
			err := s.Interact(t.Context(), input, io.Discard, WithInteractInputTimeout(time.Second), WithInteractInputCases(Case{Pattern: Timeout(), Handler: func(context.Context, *Session, Result) (Action, error) { seen = true; return Return, nil }}))
			if err != nil || !seen {
				t.Fatalf("seen=%v err=%v", seen, err)
			}
		})
	})

	t.Run("invalid configuration leaves input untouched", func(t *testing.T) {
		s, _ := pair(t)
		input := &interactionProbe{}
		err := s.Interact(t.Context(), input, io.Discard, WithInteractMatchOptions(WithTimeout(-1)))
		var config *ConfigError
		if !errors.As(err, &config) || input.reads != 0 || input.closes != 0 || s.expecting.Load() {
			t.Fatalf("err=%v reads=%d closes=%d busy=%v", err, input.reads, input.closes, s.expecting.Load())
		}
	})
	t.Run("local callback panic propagates and releases ownership", func(t *testing.T) {
		s, _ := pair(t)
		input := &interactionProbe{Reader: strings.NewReader("~~")}
		func() {
			defer func() {
				if recover() != "local callback" {
					t.Error("callback panic did not reach caller")
				}
			}()
			_ = s.Interact(t.Context(), input, io.Discard, WithInteractInputCases(Case{Pattern: Literal("~~"), Handler: func(context.Context, *Session, Result) (Action, error) { panic("local callback") }}))
		}()
		if s.expecting.Load() || input.closes != 1 {
			t.Fatalf("busy=%v closes=%d", s.expecting.Load(), input.closes)
		}
	})
	t.Run("local declaration priority and metadata", func(t *testing.T) {
		for _, text := range []string{"ab", "abc"} {
			s, _ := pair(t)
			seen := -1
			handler := func(_ context.Context, _ *Session, r Result) (Action, error) {
				if r.Source != SourceInput || r.Scope != ScopeCall || r.RuleIndex != -1 {
					t.Errorf("metadata=%+v", r)
				}
				seen = r.CaseIndex
				return Return, nil
			}
			input := io.NopCloser(&chunkStream{Reader: bytes.NewReader([]byte(text)), chunk: 1})
			err := s.Interact(t.Context(), input, io.Discard, WithInteractInputCases(Case{Pattern: Literal("abc"), Handler: handler}, Case{Pattern: Literal("ab"), Handler: handler}))
			want := 0
			if text == "ab" {
				want = 1
			}
			if err != nil || seen != want {
				t.Fatalf("input=%q selected=%d want=%d err=%v", text, seen, want, err)
			}
		}
	})
	t.Run("remote completion preserves admitted write failure", func(t *testing.T) {
		s, p := pair(t)
		input := io.NopCloser(strings.NewReader("request"))
		go func() {
			b := make([]byte, 1)
			_, _ = p.Read(b) // A partial synchronous write proves admission.
			_, _ = p.Write([]byte("DONE"))
		}()
		err := s.Interact(t.Context(), input, io.Discard, WithInteractOutputCases(Case{Pattern: Literal("DONE")}))
		var writeErr *WriteError
		if !errors.As(err, &writeErr) || writeErr.Written != 1 || !s.isClosed() {
			t.Fatalf("closed=%v err=%v", s.isClosed(), err)
		}
	})

	t.Run("small buffer", func(t *testing.T) {
		s, p := pair(t, WithBufferLimit(8))
		r, w := io.Pipe()
		defer w.Close()
		var out bytes.Buffer
		go func() { _, _ = p.Write(bytes.Repeat([]byte("x"), 32)); _ = p.Close() }()
		if err := s.Interact(t.Context(), r, &out); err != nil || out.Len() != 32 {
			t.Fatalf("bytes=%d err=%v", out.Len(), err)
		}
	})
	t.Run("flush retires", func(t *testing.T) {
		failure := errors.New("flush failed")
		s, p := pair(t)
		_ = s.Redact("secret")
		r, w := io.Pipe()
		defer w.Close()
		go func() { _, _ = p.Write([]byte("sec")) }()
		err := s.Interact(t.Context(), r, failingWriter{failure}, WithInteractOutputCases(Case{Pattern: Literal("sec")}))
		if !errors.Is(err, failure) || !s.isClosed() {
			t.Fatalf("closed=%v err=%v", s.isClosed(), err)
		}
	})
	t.Run("initial panic cleanup", func(t *testing.T) {
		s, p := pair(t)
		go func() { _, _ = p.Write([]byte("pending")) }()
		_, _ = s.Expect(t.Context(), []Case{{Pattern: Literal("pending"), NoConsume: true}})
		r, w := io.Pipe()
		defer w.Close()
		func() {
			defer func() {
				if recover() == nil {
					t.Error("missing panic")
				}
			}()
			_ = s.Interact(t.Context(), r, matchOutputFunc(func([]byte) (int, error) { panic("sink") }))
		}()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzInteractInputPrefixes(f *testing.F) {
	f.Add([]byte("a~~~b\x1d~"), uint8(1))
	f.Add([]byte("~~~~x"), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, size uint8) {
		if len(data) > 128 {
			return
		}
		s, p := pair(t)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		received := make(chan []byte, 1)
		go func() { buf := make([]byte, len(data)); _, _ = io.ReadFull(p, buf); received <- buf }()
		echo := func(ctx context.Context, s *Session, r Result) (Action, error) {
			_, err := s.Send(ctx, r.Match)
			return ContinueKeepTimer, err
		}
		input := io.NopCloser(&chunkStream{Reader: bytes.NewReader(data), chunk: int(size)%16 + 1})
		err := s.Interact(ctx, input, io.Discard, WithInteractInputCases(Case{Pattern: Literal("~~~"), Handler: echo}, Case{Pattern: Literal("~~"), Handler: echo}, Case{Pattern: Literal("~x"), Handler: echo}, Case{Pattern: Literal("\x1d"), Handler: echo}))
		if err != nil {
			t.Fatal(err)
		}
		if got := <-received; !bytes.Equal(got, data) {
			t.Fatalf("forwarded=%x input=%x", got, data)
		}
	})
}

// interactionProbe is used only before worker admission or after the worker joins.
type interactionProbe struct {
	*strings.Reader
	reads, closes int
}

func (p *interactionProbe) Read(b []byte) (int, error) {
	p.reads++
	if p.Reader == nil {
		return 0, io.EOF
	}
	return p.Reader.Read(b)
}
func (p *interactionProbe) Close() error { p.closes++; return nil }

func FuzzInteractInputPatterns(f *testing.F) {
	f.Add([]byte("x~123!#aEND\n"), uint8(1))
	f.Add([]byte("~12#\xe4\xb8\xad~!"), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, size uint8) {
		if len(data) > 128 {
			return
		}
		s, p := pair(t)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		received := make(chan []byte, 1)
		go func() { buf := make([]byte, len(data)); _, _ = io.ReadFull(p, buf); received <- buf }()
		echo := func(ctx context.Context, s *Session, r Result) (Action, error) {
			_, err := s.Send(ctx, r.Match)
			return ContinueKeepTimer, err
		}
		cases := []Case{{Pattern: mustPattern(t, `~(?P<number>[0-9]+)!`), Handler: echo}, {Pattern: Glob("#?"), Handler: echo}, {Pattern: Line("END"), Handler: echo}, {Pattern: Literal("\x1d"), Handler: echo}}
		input := io.NopCloser(&chunkStream{Reader: bytes.NewReader(data), chunk: int(size)%16 + 1})
		if err := s.Interact(ctx, input, io.Discard, WithInteractInputCases(cases...)); err != nil {
			t.Fatal(err)
		}
		if got := <-received; !bytes.Equal(got, data) {
			t.Fatalf("got=%x want=%x", got, data)
		}
	})
}
