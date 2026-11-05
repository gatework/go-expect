package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type admissionTransport struct {
	closed chan struct{}
	once   sync.Once
	writes atomic.Int32
}

func (p *admissionTransport) Read([]byte) (int, error)    { <-p.closed; return 0, io.EOF }
func (p *admissionTransport) Write(b []byte) (int, error) { p.writes.Add(1); return len(b), nil }
func (p *admissionTransport) Close() error                { p.once.Do(func() { close(p.closed) }); return nil }
func TestCanceledBeforeWriteEntry(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "once", true: "all"}[all], func(t *testing.T) {
			p := &admissionTransport{closed: make(chan struct{})}
			s, err := NewSession(p, WithWriteTimeout(0))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			s.lifeMu.Lock()
			go func() {
				var err error
				if all {
					_, err = s.SendAll(ctx, "command")
				} else {
					_, err = s.Send(ctx, "command")
				}
				done <- err
			}()
			deadline := time.Now().Add(time.Second)
			for len(s.writeGate) == 0 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			admitted := len(s.writeGate) > 0
			cancel()
			s.lifeMu.Unlock()
			if !admitted {
				t.Fatal("admission timed out")
			}
			err = <-done
			var we *WriteError
			if !errors.Is(err, context.Canceled) || errors.As(err, &we) || p.writes.Load() != 0 || s.isClosed() {
				t.Fatalf("err=%v writes=%d closed=%v", err, p.writes.Load(), s.isClosed())
			}
			if _, err = s.Send(t.Context(), "next"); err != nil {
				t.Fatal(err)
			}
		})
	}
}
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
	if s.pending != nil || s.staged != nil || s.queued != nil {
		t.Fatal("closed session retains receive storage")
	}
	if len(result.Before) != 1<<20 {
		t.Fatal("result snapshot lost")
	}
}
func FuzzLineCursor(f *testing.F) {
	f.Add([]byte("body\r\nsw# \r\n"), "sw#", uint8(1))
	f.Add([]byte(" a \n  a \n "), " a ", uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, text string, chunk uint8) {
		if len(data) > 4096 || len(text) > 1024 {
			return
		}
		p := Line(text)
		if p.ValidateText() != nil {
			return
		}
		for _, pattern := range []Pattern{p, All(Literal(text), p), All(p, All(Literal(text), p))} {
			cursor := newMatchCursor(pattern)
			for pass := 0; pass < 2; pass++ {
				for end := 0; ; end = min(len(data), end+1+int(chunk)%32) {
					got, ok, err := cursor.find(data[:end], false, 0, true)
					want, wok, we := pattern.find(data[:end], false, 0)
					if err != nil || we != nil || ok != wok || ok && (got.start != want.start || got.end != want.end) {
						t.Fatalf("prefix=%d got=%+v/%v want=%+v/%v", end, got, ok, want, wok)
					}
					if end == len(data) {
						break
					}
				}
				cursor.reset()
			}
		}
	})
}
func BenchmarkLineCursor(b *testing.B) {
	for _, size := range []int{16 << 10, 64 << 10} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			c := newMatchCursor(Line("sw#"))
			data := bytes.Repeat([]byte("x"), size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				c.reset()
				for end := 32; end <= len(data); end += 32 {
					c.find(data[:end], false, 0, true)
				}
			}
		})
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

func TestCapturesReuseDetachedRanges(t *testing.T) {
	p, err := Compile(`((x+))(y)?()`)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), 1024)
	loc, ok, err := p.find(data, true, 0)
	if err != nil || !ok {
		t.Fatal(err)
	}
	match := string(data)
	captures := loc.capturesFromSnapshot(data, match)
	clear(data)
	if captures[0].Text != match || captures[1].Text != match || captures[2].Matched || !captures[3].Matched || captures[3].Text != "" {
		t.Fatal("capture semantics changed")
	}
}

func BenchmarkBufferedExpect(b *testing.B) {
	p := &admissionTransport{closed: make(chan struct{})}
	s, err := NewSession(p, WithDefaultTimeout(0))
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	data := []byte("!")
	cases := []Case{{Pattern: Literal("!")}}
	b.ReportAllocs()
	for b.Loop() {
		s.pending = data
		if _, err := s.Expect(b.Context(), cases); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkDuplicateCaptureSnapshots(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 64<<10)
	offsets := make([]int, 128)
	for i := 0; i < len(offsets); i += 2 {
		offsets[i+1] = len(data)
	}
	loc := location{start: 0, end: len(data), submatches: offsets}
	match := string(data)
	b.ReportAllocs()
	for b.Loop() {
		captures := loc.capturesFromSnapshot(data, match)
		runtime.KeepAlive(captures)
	}
}

func TestMixedCursorContinuation(t *testing.T) {
	long := string(bytes.Repeat([]byte("a"), 257)) + "b"
	block := []byte("start" + long + "\r\nsw# \r\n")
	s, err := NewSession(&chunkStream{Reader: bytes.NewReader(append(bytes.Clone(block), block...)), chunk: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	count := 0
	_, err = s.Expect(t.Context(), []Case{{Pattern: All(Literal("start"), Literal(long), Line("sw#")), Handler: func(context.Context, *Session, Result) (Action, error) {
		count++
		if count == 1 {
			return ContinueWithoutReset, nil
		}
		return Return, nil
	}}})
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
