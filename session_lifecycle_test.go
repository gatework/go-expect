// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func (*cancelingWriter) Read([]byte) (int, error) { return 0, io.EOF }

func (*cancelingWriter) Close() error { return nil }

func TestReceiveScratchDoesNotChangeQueuedInput(t *testing.T) {
	for _, chunk := range []int{1, 32, readChunk/2 - 1, readChunk / 2, readChunk} {
		t.Run(strconv.Itoa(chunk), func(t *testing.T) {
			data := make([]byte, readChunk*3+37)
			for i := range data {
				data[i] = byte(i * 37)
			}
			var transcript bytes.Buffer
			s, err := NewSession(&chunkStream{Reader: bytes.NewReader(data), chunk: chunk}, WithOutput(&transcript))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var snapshots []Result
			count := 0
			for _, value := range data {
				if value == 255 {
					count++
				}
			}
			last, err := s.Expect(t.Context(), []Case{{Pattern: Literal("\xff"), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
				snapshots = append(snapshots, r)
				return ContinueKeepTimer, nil
			}}, {Pattern: EOF()}})
			if err != nil || len(snapshots) != count {
				t.Fatalf("matches=%d want=%d err=%v", len(snapshots), count, err)
			}
			var rebuilt bytes.Buffer
			for _, r := range snapshots {
				rebuilt.WriteString(r.Before)
				rebuilt.WriteString(r.Match)
			}
			rebuilt.WriteString(last.Before)
			if !bytes.Equal(rebuilt.Bytes(), data) || !bytes.Equal(transcript.Bytes(), data) {
				t.Fatal("reused scratch changed input or detached snapshots")
			}
		})
	}
}

func TestRejectedWaitReportsPendingInput(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(strconv.FormatBool(canceled), func(t *testing.T) {
			s, peer := pair(t)
			go peer.Write([]byte("promptTAIL"))
			waitText(t, s, "prompt")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var opts []ExpectOption
			if canceled {
				cancel()
			} else {
				opts = []ExpectOption{nil}
			}
			r, err := s.Expect(ctx, []Case{{Pattern: Literal("TAIL")}}, opts...)
			if err == nil || r.CaseIndex != -1 || r.Before != "TAIL" {
				t.Fatalf("rejected wait lost pending diagnostics: err=%v before=%q", err, r.Before)
			}
			waitText(t, s, "TAIL")
		})
	}
}

func TestNewSessionRejectsTypedNil(t *testing.T) {
	t.Run("transport", func(t *testing.T) {

		var transport *nilTransport
		s, err := NewSession(transport)
		if err == nil {
			s.Close()
			t.Fatal("typed-nil transport accepted")
		}
	})
	t.Run("output", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		var output *bytes.Buffer
		opt := WithOutput(output)
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

func (*nilTransport) Read([]byte) (int, error) { return 0, io.EOF }

func (*nilTransport) Close() error { return nil }

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

func TestRulesObserverUsesPublicResultIndex(t *testing.T) {
	var events []Event
	s, p := pair(t, WithObserver(func(e Event) {
		if e.Operation == "expect" {
			events = append(events, e)
		}
	}))
	go p.Write([]byte("reply?"))
	failure := errors.New("response failed")
	r := WithRules(Case{Pattern: Literal("reply?"), Handler: Then(func(context.Context, *Session, Result) error { return failure })})
	result, err := s.Expect(t.Context(), []Case{{Pattern: Literal("done")}}, r)
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].CaseIndex != result.CaseIndex {
		t.Fatalf("event=%v result index=%d", events, result.CaseIndex)
	}
}

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
				_, err = s.Expect(context.Background(), []Case{{Pattern: Literal("?"), Handler: func(context.Context, *Session, Result) (Action, error) { return ContinueKeepTimer, nil }}, {Pattern: EOF()}}, WithTimeout(0))
				s.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

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
		s, err := NewSession(&chunkStream{Reader: bytes.NewReader(data), chunk: 1 + int(chunk)%32}, WithBufferLimit(5+int(extra)%12), WithOutput(&transcript))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		var matches []Result
		last, err := s.Expect(t.Context(), []Case{{Pattern: Literal("\xff"), Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
			matches = append(matches, r)
			return ContinueKeepTimer, nil
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

func (*chunkStream) Close() error { return nil }

func FuzzLiteralAllChunks(f *testing.F) {
	f.Add([]byte("!abab!abab"), "aba", "bab", uint8(1))
	f.Add([]byte("needle missing"), "needle", "end", uint8(4))
	f.Add(bytes.Repeat([]byte("a"), 1024), string(bytes.Repeat([]byte("a"), 257)), "a", uint8(1))
	f.Fuzz(func(t *testing.T, input []byte, a, b string, size uint8) {
		if len(input) > 4096 || a == "" || b == "" || len(a) > 1024 || len(b) > 1024 {
			return
		}
		p := All(Literal(a), All(Literal(b), Literal(a)))
		state := newMatchCursor(p)
		chunk := int(size)%32 + 1
		for end := 0; ; end = min(end+chunk, len(input)) {
			got, found, scanErr := state.find(input[:end], false, 0, true)
			want, wantFound, wantErr := p.find(input[:end], end == len(input), 0)
			if scanErr != nil || wantErr != nil || found != wantFound || got.start != want.start || got.end != want.end {
				t.Fatalf("prefix=%d got=(%d,%d,%v) want=(%d,%d,%v,%v)", end, got.start, got.end, found, want.start, want.end, wantFound, wantErr)
			}
			if end == len(input) {
				break
			}
		}
		state.reset()
		got, found, scanErr := state.find(input, true, 0, true)
		want, wantFound, wantErr := p.find(input, true, 0)
		if scanErr != nil || wantErr != nil || found != wantFound || got.start != want.start || got.end != want.end {
			t.Fatal("reset changed matching")
		}
	})
}

func (p *admissionTransport) Read([]byte) (int, error) { <-p.closed; return 0, io.EOF }

func (p *admissionTransport) Close() error { p.once.Do(func() { close(p.closed) }); return nil }

func TestCloseReleasesReceiveStorage(t *testing.T) {
	s, err := NewSession(&chunkStream{Reader: bytes.NewReader(bytes.Repeat([]byte("x"), 1<<20)), chunk: readChunk})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	result, err := s.Expect(t.Context(), []Case{{Pattern: Literal("missing")}})
	if !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.receive.pending != nil || s.receive.staged != nil || s.mailbox.queued != nil {
		t.Fatal("closed session retains receive storage")
	}
	if len(result.Before) != 1<<20 {
		t.Fatal("result snapshot lost")
	}
}

func BenchmarkNestedValidation(b *testing.B) {
	for _, depth := range []int{64, 256, 1024} {
		p := Line("x")
		for range depth {
			p = All(p)
		}
		b.Run(strconv.Itoa(depth)+"/validate", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := p.ValidateText(); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(strconv.Itoa(depth)+"/cursor", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				cursor := newMatchCursor(p)
				runtime.KeepAlive(cursor)
			}
		})
	}
}

func TestSessionSinkFailureStopsBeforeReply(t *testing.T) {
	failure := errors.New("sink broke")
	s, p := pair(t, WithOutput(failingWriter{failure}))
	go p.Write([]byte("confirm?"))
	calls := 0
	_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("confirm?"), Handler: func(context.Context, *Session, Result) (Action, error) { calls++; return Return, nil }}})
	if !errors.Is(err, failure) || calls != 0 {
		t.Fatalf("%v calls=%d", err, calls)
	}
	if _, err = s.Send(t.Context(), "yes"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestSessionLogsAndObserversDoNotLeak(t *testing.T) {
	var out bytes.Buffer
	s, p := pair(t, WithLogger(slog.New(slog.NewTextHandler(&out, nil))), WithObserver(func(e Event) { out.WriteString(e.Class.String()) }))
	go p.Write([]byte("secret"))
	waitText(t, s, "secret")
	if strings.Contains(out.String(), "secret") {
		t.Fatal("payload in diagnostics")
	}
}

func TestSessionDefaultTimeoutOverride(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t, WithDefaultTimeout(time.Second))
		if _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("x")}}); !errors.Is(err, ErrTimeout) {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { defer close(done); time.Sleep(1500 * time.Millisecond); p.Write([]byte("x")) }()
		if _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("x")}}, WithTimeout(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		<-done
	})
}

type tinyProgressStream struct {
	done chan struct{}
	once sync.Once
}

func (s *tinyProgressStream) Read([]byte) (int, error) { <-s.done; return 0, io.EOF }

func (s *tinyProgressStream) Close() error { s.once.Do(func() { close(s.done) }); return nil }

func TestCloseCancelsActiveHandlerWithoutWaiting(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	s, err := NewSession(a, WithDefaultTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entered, finished := make(chan struct{}), make(chan error, 1)
	release := make(chan struct{})
	go b.Write([]byte("ready"))
	go func() {
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("ready"), Handler: func(ctx context.Context, _ *Session, _ Result) (Action, error) {
			close(entered)
			select {
			case <-ctx.Done():
				return Return, ctx.Err()
			case <-release:
				return Return, nil
			}
		}}})
		finished <- err
	}()
	<-entered
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		<-finished
		t.Fatal("Close did not cancel handler")
	}
}

type failingDuplex struct {
	net.Conn
	writeErr, closeErr error
}

func (s *failingDuplex) Close() error { s.Conn.Close(); return s.closeErr }

func TestAutomaticRetirementPreservesCleanupCause(t *testing.T) {
	for _, all := range []bool{false, true} {
		a, b := net.Pipe()
		primary, cleanup := errors.New("write sentinel"), errors.New("cleanup sentinel")
		s, err := NewSession(&failingDuplex{a, primary, cleanup})
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if all {
			n, err = s.SendAll(t.Context(), "secret")
		} else {
			n, err = s.Send(t.Context(), "secret")
		}
		if n != 1 || !errors.Is(err, primary) || !errors.Is(err, cleanup) {
			t.Errorf("all=%v n=%d err=%v", all, n, err)
		}
		b.Close()
		s.Close()
	}
}

func waitQueuedBytes(t *testing.T, s *Session, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case s.mailbox.inputGate <- struct{}{}:
			n := s.mailbox.queuedBytes
			<-s.mailbox.inputGate
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

func (p *quietEOFTransport) Read(data []byte) (int, error) { return p.input.Read(data) }

func (p *quietEOFTransport) Close() error { p.once.Do(func() { close(p.closed) }); return nil }

func (r *referenceAdditiveRedactor) append(data []byte, finish bool) []byte {
	r.pending = append(r.pending, data...)
	r.hidden = append(r.hidden, make([]bool, len(data))...)
	keep := 0
	for _, secret := range r.secrets {
		for start := range r.pending {
			n := min(len(secret), len(r.pending)-start)
			if !bytes.Equal(r.pending[start:start+n], []byte(secret)[:n]) {
				continue
			}
			if n == len(secret) {
				for i := start; i < start+n; i++ {
					r.hidden[i] = true
				}
			} else {
				keep = max(keep, n)
			}
		}
	}
	end := len(r.pending) - keep
	if finish {
		for i := end; i < len(r.pending); i++ {
			r.hidden[i] = true
		}
		end = len(r.pending)
	}
	out := bytes.Clone(r.pending[:end])
	for i := range out {
		if r.hidden[i] {
			out[i] = '*'
		}
	}
	r.pending = r.pending[end:]
	r.hidden = r.hidden[end:]
	return out
}
