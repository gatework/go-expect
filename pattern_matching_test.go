// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func TestExpectLiteralAllChunkBoundariesAndReset(t *testing.T) {
	p := All(Literal("aba"), All(Literal("bab"), Literal("aba")))
	s, err := NewSession(&chunkStream{Reader: bytes.NewReader([]byte("!abab!ababtail")), chunk: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var snapshots []Result
	result, err := s.Expect(t.Context(), []Case{{Pattern: p, Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
		snapshots = append(snapshots, r)
		if len(snapshots) == 1 {
			return ContinueKeepTimer, nil
		}
		return Return, nil
	}}})
	if err != nil || result.Before != "!" || result.Match != "abab" || len(snapshots) != 2 {
		t.Fatalf("snapshots=%v result=%#v err=%v", snapshots, result, err)
	}
	for _, snapshot := range snapshots {
		if snapshot.Before != "!" || snapshot.Match != "abab" || len(snapshot.Captures) != 0 {
			t.Fatalf("snapshot fields changed: before=%q match=%q", snapshot.Before, snapshot.Match)
		}
	}
	result, err = s.Expect(t.Context(), []Case{{Pattern: Literal("tail")}})
	if err != nil || result.Match != "tail" {
		t.Fatalf("tail=%q err=%v", result.Match, err)
	}
}

func TestExpectLiteralAllCompetesAndRestartsAfterEOF(t *testing.T) {
	pattern := All(Literal("alpha"), All(Literal("beta"), Literal("alpha")))
	for _, declaration := range []bool{false, true} {
		s, err := NewSession(&chunkStream{Reader: bytes.NewReader([]byte("alpha beta later")), chunk: 16})
		if err != nil {
			t.Fatal(err)
		}
		options := []ExpectOption{}
		if declaration {
			options = append(options, WithMatchOrder(Declaration))
		}
		r, err := s.Expect(t.Context(), []Case{{Pattern: Literal("later")}, {Pattern: pattern}}, options...)
		want := 1
		if declaration {
			want = 0
		}
		if err != nil || r.CaseIndex != want {
			t.Fatalf("declaration=%v case=%d err=%v", declaration, r.CaseIndex, err)
		}
		s.Close()
	}
	s, err := NewSession(&chunkStream{Reader: bytes.NewReader([]byte("alpha")), chunk: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.Expect(t.Context(), []Case{{Pattern: pattern}})
	if !errors.Is(err, io.EOF) || r.Before != "alpha" {
		t.Fatalf("partial=%q err=%v", r.Before, err)
	}
	r, err = s.Expect(t.Context(), []Case{{Pattern: All(Literal("alpha"), Literal("alpha"))}})
	if err != nil || r.Match != "alpha" {
		t.Fatalf("new call=%q err=%v", r.Match, err)
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
			for range 2 {
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
		s.receive.pending = data
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
			return ContinueKeepTimer, nil
		}
		return Return, nil
	}}})
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestExpectCanceledInsideHandlerCannotReturnSuccess(t *testing.T) {
	s, p := pair(t)
	go p.Write([]byte("prompt"))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := s.Expect(ctx, []Case{{Pattern: Literal("prompt"), Handler: func(context.Context, *Session, Result) (Action, error) { cancel(); return Return, nil }}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled handler returned %v", err)
	}
}

func TestExpectBusyObservers(t *testing.T) {
	busy := 0
	s, p := pair(t, WithObserver(func(e Event) {
		if e.Error == "busy" {
			busy++
		}
	}))
	go p.Write([]byte("prompt"))
	_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(ctx context.Context, s *Session, r Result) (Action, error) {
		_, e := s.Expect(ctx, []Case{{Pattern: Literal("other")}})
		if !errors.Is(e, ErrBusy) {
			t.Error(e)
		}
		return Return, nil
	}}})
	if err != nil || busy != 1 {
		t.Fatalf("%v observed=%d", err, busy)
	}
}

func TestSessionConcurrentExpectAndCloseFromHandler(t *testing.T) {
	s, p := pair(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go p.Write([]byte("go"))
	go func() {
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("go"), Handler: func(context.Context, *Session, Result) (Action, error) {
			close(entered)
			<-release
			return Return, s.Close()
		}}})
		done <- err
	}()
	<-entered
	if _, err := s.Expect(t.Context(), []Case{{Pattern: Literal("go")}}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestOptionalCaptureAndSplitUTF8(t *testing.T) {
	s, p := pair(t)
	go func() {
		for _, b := range []byte("中!") {
			p.Write([]byte{b})
		}
		p.Close()
	}()
	r, err := s.Expect(t.Context(), []Case{{Pattern: mustPattern(t, `(中)(x)?()!`)}})
	if err != nil || r.Match != "中!" || len(r.Captures) != 3 || r.Captures[0].Text != "中" || r.Captures[1].Matched || !r.Captures[2].Matched {
		t.Fatalf("%v match=%q captures=%v", err, r.Match, r.Captures)
	}
}

func TestExpectReturnAfterRelativeDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		go p.Write([]byte("prompt"))
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(context.Context, *Session, Result) (Action, error) {
			time.Sleep(2 * time.Second)
			return Return, nil
		}}}, WithTimeout(time.Second))
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("expired handler returned %v", err)
		}
	})
}

func TestExpectReturnAfterIdleDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, p := pair(t)
		go p.Write([]byte("prompt"))
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("prompt"), Handler: func(context.Context, *Session, Result) (Action, error) {
			time.Sleep(2 * time.Second)
			return Return, nil
		}}}, WithIdleTimeout(time.Second))
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("expired handler returned %v", err)
		}
	})
}

func TestFromRegexpPreservesLongestAndIsolation(t *testing.T) {
	r := regexp.MustCompile("a|ab")
	r.Longest()
	posix, err := FromRegexp(regexp.MustCompilePOSIX("a|ab"))
	if err != nil {
		t.Fatal(err)
	}
	hitPOSIX, okPOSIX, err := posix.find([]byte("ab"), true, 0)
	if err != nil || !okPOSIX || hitPOSIX.end != 2 {
		t.Fatal("lost POSIX semantics")
	}
	p, err := FromRegexp(r)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSession(&chunkStream{Reader: bytes.NewReader([]byte("ab")), chunk: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Expect(t.Context(), []Case{{Pattern: p}})
	if err != nil || got.Match != "ab" {
		t.Fatalf("match=%q err=%v", got.Match, err)
	}
	if _, err := FromRegexp(nil); err == nil {
		t.Fatal("nil regexp accepted")
	}
	original := regexp.MustCompile("a|ab")
	p, _ = FromRegexp(original)
	original.Longest()
	hit, ok, err := p.find([]byte("ab"), true, 0)
	if err != nil || !ok || hit.end != 1 {
		t.Fatal("caller mutated pattern")
	}
}

func TestRegexpWinnerCapturesPreviewAndDetached(t *testing.T) {
	for _, declaration := range []bool{false, true} {
		name := "earliest"
		if declaration {
			name = "declaration"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := pair(t)
			input := []byte("z:ab!")
			s.receive.pending = input
			patterns := []Case{{Pattern: mustPattern(t, `(a)(b)?()!`)}, {Pattern: mustPattern(t, `(z)()(x)?`)}}
			chosen := 1
			want := []Capture{{Text: "z", Matched: true, Start: 0, End: 1}, {Matched: true, Start: 1, End: 1}, {Start: -1, End: -1}}
			var opts []ExpectOption
			if declaration {
				chosen = 0
				want = []Capture{{Text: "a", Matched: true, Start: 2, End: 3}, {Text: "b", Matched: true, Start: 3, End: 4}, {Matched: true, Start: 4, End: 4}}
				opts = append(opts, WithMatchOrder(Declaration))
			}
			rejection := errors.New("preview rejection")
			patterns[chosen].Handler = func(_ context.Context, _ *Session, r Result) (Action, error) {
				if r.CaseIndex != chosen || !reflect.DeepEqual(r.Captures, want) {
					t.Fatalf("preview captures=%#v", r.Captures)
				}
				if string(s.receive.pending) != "z:ab!" {
					t.Fatal("preview consumed input")
				}
				return Return, rejection
			}
			preview := cloneCases(patterns)
			for i := range preview {
				preview[i].NoConsume = true
			}
			result, err := s.Expect(t.Context(), preview, opts...)
			if !errors.Is(err, rejection) || string(s.receive.pending) != "z:ab!" {
				t.Fatalf("preview err=%v pending=%q", err, s.receive.pending)
			}
			patterns[chosen].Handler = nil
			final, err := s.Expect(t.Context(), patterns, opts...)
			if err != nil || !reflect.DeepEqual(final.Captures, want) {
				t.Fatalf("winner err=%v captures=%#v", err, final.Captures)
			}
			clear(input)
			if !reflect.DeepEqual(result.Captures, want) || !reflect.DeepEqual(final.Captures, want) {
				t.Fatal("captures alias receive storage")
			}
		})
	}
}

func TestRegexpCaptureSelectionAndAll(t *testing.T) {
	for _, mode := range []string{"ordinary", "longest", "posix"} {
		t.Run(mode, func(t *testing.T) {
			re := regexp.MustCompile(`(a|ab)`)
			want := "a"
			if mode == "longest" {
				re.Longest()
				want = "ab"
			}
			if mode == "posix" {
				re = regexp.MustCompilePOSIX(`(a|ab)`)
				want = "ab"
			}
			pattern, err := FromRegexp(re)
			if err != nil {
				t.Fatal(err)
			}
			s, _ := pair(t)
			s.receive.pending = []byte("ab!")
			result, err := s.Expect(t.Context(), []Case{{Pattern: pattern}, {Pattern: mustPattern(t, `(ab)`)}})
			if err != nil || result.CaseIndex != 0 || len(result.Captures) != 1 || result.Captures[0].Text != want {
				t.Fatalf("match=%q err=%v", result.Match, err)
			}
			s.receive.pending = []byte("ab!")
			result, err = s.Expect(t.Context(), []Case{{Pattern: All(pattern, All(mustPattern(t, `(b)(!)`)))}})
			if err != nil || result.Match != "ab!" || len(result.Captures) != 0 {
				t.Fatalf("all match=%q err=%v", result.Match, err)
			}
		})
	}
}
