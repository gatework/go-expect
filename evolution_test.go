package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewSessionRejectsTypedNil(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		// A typed-nil transport must be rejected before its Read method runs.
		var transport *nilTransport
		s, err := NewSession(transport)
		if err == nil {
			s.Close()
			t.Fatal("typed-nil transport accepted")
		}
	})
	for _, transcript := range []bool{false, true} {
		t.Run("output/transcript="+strconv.FormatBool(transcript), func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			var output *bytes.Buffer
			opt := WithOutput(output)
			if transcript {
				opt = WithTranscript(output)
			}
			s, err := NewSession(a, opt)
			if err == nil {
				s.Close()
				t.Fatal("typed-nil output accepted")
			}
			go b.Write([]byte("x"))
			buf := make([]byte, 1)
			if _, err := a.Read(buf); err != nil {
				t.Fatalf("failed construction took ownership: %v", err)
			}
		})
	}
}

type nilTransport struct{}

func (*nilTransport) Read([]byte) (int, error)    { return 0, io.EOF }
func (*nilTransport) Write(p []byte) (int, error) { return len(p), nil }
func (*nilTransport) Close() error                { return nil }

func TestHandlerAbruptExitCancelsDerivedContext(t *testing.T) {
	for _, kind := range []string{"panic", "goexit"} {
		t.Run(kind, func(t *testing.T) {
			s, p := pair(t)
			go p.Write([]byte("promptTAIL"))
			finished := make(chan struct{})
			var callbackCtx context.Context
			go func() {
				defer close(finished)
				defer func() { recover() }()
				s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(ctx context.Context, _ *Session, _ Result) (Action, error) {
					callbackCtx = ctx
					if kind == "goexit" {
						runtime.Goexit()
					}
					panic("caller panic")
				}}})
			}()
			<-finished
			if callbackCtx == nil || !errors.Is(callbackCtx.Err(), context.Canceled) {
				t.Fatal("derived callback context left alive")
			}
			waitText(t, s, "TAIL")
		})
	}
}

func TestResponderObserverUsesPublicResultIndex(t *testing.T) {
	var events []Event
	s, p := pair(t, WithObserver(func(e Event) {
		if e.Operation == "expect" {
			events = append(events, e)
		}
	}))
	go p.Write([]byte("reply?"))
	failure := errors.New("response failed")
	r, err := NewResponder(Rule{Pattern: Literal("reply?"), Respond: func(context.Context, *Session, Result) error { return failure }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Expect(t.Context(), s, []Pattern{Literal("done")})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].CaseIndex != result.CaseIndex {
		t.Fatalf("event=%v result index=%d", events, result.CaseIndex)
	}
}

func TestCanceledWriteValidatesProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		defer b.Close()
		s, err := NewSession(&invalidCanceledWrite{Conn: a})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		n, err := s.Send(ctx, "yes")
		var we *WriteError
		if n != 0 || !errors.As(err, &we) || we.Written != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("invalid progress escaped: n=%d err=%v", n, err)
		}
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal("transport cause lost during cancellation")
		}
	})
}

func TestClosedWriteValidatesProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := net.Pipe()
		defer b.Close()
		s, err := NewSession(&invalidCanceledWrite{Conn: a})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		done := make(chan writeResult, 1)
		go func() { n, err := s.Send(t.Context(), "yes"); done <- writeResult{n, err} }()
		synctest.Wait()
		s.Close()
		r := <-done
		var we *WriteError
		if r.n != 0 || !errors.As(r.err, &we) || we.Written != 0 || !errors.Is(r.err, io.ErrClosedPipe) {
			t.Fatalf("invalid progress escaped on close: n=%d err=%v", r.n, r.err)
		}
	})
}

type invalidCanceledWrite struct{ net.Conn }

func (w *invalidCanceledWrite) Write(p []byte) (int, error) {
	_, err := w.Conn.Write(p)
	return len(p) + 1, err
}

// A burst models many confirmations already delivered in one receive block.
func BenchmarkContinuedBurst(b *testing.B) {
	for _, count := range []int{128, 4096} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			data := bytes.Repeat([]byte("?"), count)
			b.ReportAllocs()
			b.SetBytes(int64(count))
			for b.Loop() {
				s, err := NewSession(&finalReader{data: data})
				if err != nil {
					b.Fatal(err)
				}
				_, err = s.Expect(context.Background(), []Case{{Pattern: Literal("?"), Handler: func(context.Context, *Session, Result) (Action, error) { return ContinueWithoutReset, nil }}, {Pattern: EOF()}}, WithTimeout(0))
				s.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Exercise the complete receive path: staged blocks, bounded pending bytes,
// continued consumption, detached results and exactly-once transcript delivery.
func FuzzSessionContinuations(f *testing.F) {
	f.Add([]byte("password\x00confirm"), uint8(17), uint8(0))
	f.Add([]byte{255, 255, 128, 0, 1}, uint8(1), uint8(3))
	f.Fuzz(func(t *testing.T, input []byte, chunk, extra uint8) {
		if len(input) > 1024 {
			t.Skip()
		}
		var data []byte
		for i, c := range input {
			data = append(data, c)
			if i%4 == 3 {
				data = append(data, 255)
			}
		}
		var transcript bytes.Buffer
		s, err := NewSession(&chunkStream{Reader: bytes.NewReader(data), chunk: 1 + int(chunk)%32}, WithBufferLimit(5+int(extra)%12), WithTranscript(&transcript))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		var matches []Result
		last, err := s.Expect(t.Context(), []Case{{Pattern: Literal("\xff"), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
			matches = append(matches, r)
			return ContinueWithoutReset, nil
		}}, {Pattern: EOF()}})
		if err != nil {
			t.Fatal(err)
		}
		var reconstructed bytes.Buffer
		for _, r := range matches {
			reconstructed.WriteString(r.Before)
			reconstructed.WriteString(r.Match)
		}
		reconstructed.WriteString(last.Before)
		if !bytes.Equal(data, reconstructed.Bytes()) || !bytes.Equal(data, transcript.Bytes()) || len(matches) != bytes.Count(data, []byte{255}) || last.CaseIndex != 1 {
			t.Fatal("consumption, snapshot or transcript changed input")
		}
	})
}

type chunkStream struct {
	*bytes.Reader
	chunk int
}

func (s *chunkStream) Read(p []byte) (int, error) {
	return s.Reader.Read(p[:min(len(p), s.chunk)])
}
func (*chunkStream) Write(p []byte) (int, error) { return len(p), nil }
func (*chunkStream) Close() error                { return nil }
