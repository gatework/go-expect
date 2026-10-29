package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// Returned diagnostic bytes belong to the caller, including the no-rule path.
func TestPublicRedactorDetachedOutput(t *testing.T) {
	for _, secrets := range [][]string{nil, {"secret"}} {
		r, err := NewRedactor(secrets, "[hidden]")
		if err != nil {
			t.Fatal(err)
		}
		input := []byte("ordinary text")
		first := r.Append(input)
		snapshot := bytes.Clone(first)
		for i := range input {
			input[i] = 'x'
		}
		_ = r.Append([]byte("another long ordinary chunk"))
		_ = r.Finish()
		if !bytes.Equal(first, snapshot) {
			t.Fatalf("returned bytes changed: %q", first)
		}
		if len(first) > 0 {
			first[0] = '!'
		}
		if got := r.Finish(); len(got) != 0 {
			t.Fatalf("repeated finish: %q", got)
		}
	}
}

func TestPublicRedactorSafeChunkAllocations(t *testing.T) {
	for _, secrets := range [][]string{nil, {"secret"}} {
		name := "no_rules"
		if len(secrets) > 0 {
			name = "with_rule"
		}
		t.Run(name, func(t *testing.T) {
			r, err := NewRedactor(secrets, "[hidden]")
			if err != nil {
				t.Fatal(err)
			}
			chunk := bytes.Repeat([]byte("x"), 256)
			_ = r.Append(chunk)
			allocations := testing.AllocsPerRun(100, func() {
				if out := r.Append(chunk); len(out) == 0 {
					t.Fatal("missing safe output")
				}
			})
			if allocations > 1 {
				t.Fatalf("safe chunk allocated %.0f times, want at most one detached output", allocations)
			}
		})
	}
}

func TestPublicRedactorRetainedWindow(t *testing.T) {
	r, err := NewRedactor([]string{"secret"}, "*")
	if err != nil {
		t.Fatal(err)
	}
	input := append(bytes.Repeat([]byte("x"), 1<<20), 's')
	_ = r.Append(input)
	if len(r.pending) > 5 || cap(r.pending) > 2*5 || cap(r.hidden) > 2*5 {
		t.Fatalf("retained data len=%d cap=%d flags=%d", len(r.pending), cap(r.pending), cap(r.hidden))
	}
	if got := string(r.Finish()); got != "xxxx*" {
		t.Fatalf("finish=%q", got)
	}
	if cap(r.pending) != 0 || cap(r.hidden) != 0 {
		t.Fatal("finish retains stream buffers")
	}
}

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
			return ContinueWithoutReset, nil
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
			options = append(options, WithDeclarationPriority())
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

func FuzzPublicRedactorChunks(f *testing.F) {
	f.Add([]byte("abab password pass"), "aba", "password", "[hidden]", uint8(1))
	f.Add([]byte("ababa"), "aba", "bab", "*", uint8(3))
	f.Add([]byte{0, 0xff, 1, 0, 0xff}, string([]byte{0, 0xff}), "", "#", uint8(1))
	f.Add(bytes.Repeat([]byte("a"), 1024), string(bytes.Repeat([]byte("a"), 257)), string(bytes.Repeat([]byte("a"), 512))+"b", "[X]", uint8(31))
	f.Add(bytes.Repeat([]byte{0, 0xff, 1}, 600), string(bytes.Repeat([]byte{0, 0xff, 1}, 100)), "\xff\x01", "#", uint8(3))
	f.Fuzz(func(t *testing.T, input []byte, a, b, replacement string, size uint8) {
		if len(input) > 4096 || len(a) > 1024 || len(b) > 1024 || replacement == "" || len(replacement) > 64 {
			return
		}
		var secrets []string
		for _, s := range []string{a, b} {
			if s != "" {
				secrets = append(secrets, s)
			}
		}
		r, err := NewRedactor(secrets, replacement)
		if err != nil {
			t.Fatal(err)
		}
		var got []byte
		chunk := int(size)%32 + 1
		for pos := 0; pos < len(input); pos += chunk {
			got = append(got, r.Append(input[pos:min(pos+chunk, len(input))])...)
		}
		got = append(got, r.Finish()...)
		if want := referencePublicRedaction(input, secrets, []byte(replacement)); !bytes.Equal(got, want) {
			t.Fatalf("got=%x want=%x", got, want)
		}

		split := len(input) / 2
		got = nil
		for pos := 0; pos < split; pos += chunk {
			got = append(got, r.Append(input[pos:min(pos+chunk, split)])...)
		}
		var next []string
		if b != "" {
			next = []string{b}
		}
		flushed, err := r.SetSecrets(next)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, flushed...)
		for pos := split; pos < len(input); pos += chunk {
			got = append(got, r.Append(input[pos:min(pos+chunk, len(input))])...)
		}
		got = append(got, r.Finish()...)
		want := referencePublicRedaction(input[:split], secrets, []byte(replacement))
		want = append(want, referencePublicRedaction(input[split:], next, []byte(replacement))...)
		if !bytes.Equal(got, want) {
			t.Fatalf("switched rules got=%x want=%x", got, want)
		}
	})
}

// Mark complete occurrences and trailing prefixes using a whole-input oracle,
// then coalesce marked runs independently of the streaming implementation.
func referencePublicRedaction(input []byte, secrets []string, replacement []byte) []byte {
	marked := make([]bool, len(input))
	for pos := range input {
		for _, secret := range secrets {
			n := min(len(secret), len(input)-pos)
			if bytes.Equal(input[pos:pos+n], []byte(secret)[:n]) {
				for i := pos; i < pos+n; i++ {
					marked[i] = true
				}
			}
		}
	}
	var out []byte
	masking := false
	for i, value := range input {
		if marked[i] {
			if !masking {
				out = append(out, replacement...)
			}
			masking = true
		} else {
			out = append(out, value)
			masking = false
		}
	}
	return out
}

func FuzzLiteralAllChunks(f *testing.F) {
	f.Add([]byte("!abab!abab"), "aba", "bab", uint8(1))
	f.Add([]byte("needle missing"), "needle", "end", uint8(4))
	f.Fuzz(func(t *testing.T, input []byte, a, b string, size uint8) {
		if len(input) > 4096 || a == "" || b == "" || len(a) > 64 || len(b) > 64 {
			return
		}
		p := All(Literal(a), All(Literal(b), Literal(a)))
		state := newPatternScan(p)
		chunk := int(size)%32 + 1
		for end := 0; ; end = min(end+chunk, len(input)) {
			got, found := state.findAll(input[:end])
			want, wantFound, wantErr := p.find(input[:end], end == len(input), 0)
			if wantErr != nil || found != wantFound || got.start != want.start || got.end != want.end {
				t.Fatalf("prefix=%d got=(%d,%d,%v) want=(%d,%d,%v,%v)", end, got.start, got.end, found, want.start, want.end, wantFound, wantErr)
			}
			if end == len(input) {
				break
			}
		}
		state.reset()
		got, found := state.findAll(input)
		want, wantFound, wantErr := p.find(input, true, 0)
		if wantErr != nil || found != wantFound || got.start != want.start || got.end != want.end {
			t.Fatal("reset changed matching")
		}
	})
}

// Masked single-byte fragments should not allocate an output for bytes already
// covered by a replacement run, or repeatedly regrow their retained window.
func TestPublicRedactorMaskedByteAllocations(t *testing.T) {
	for _, entry := range []struct {
		name    string
		secrets []string
		input   []byte
	}{
		{"matches", []string{"password", "token"}, bytes.Repeat([]byte("password token "), 64)},
		{"overlaps", []string{"aba", "bab"}, bytes.Repeat([]byte("abab "), 200)},
	} {
		t.Run(entry.name, func(t *testing.T) {
			r, err := NewRedactor(entry.secrets, "[hidden]")
			if err != nil {
				t.Fatal(err)
			}
			allocations := testing.AllocsPerRun(10, func() {
				for i := range entry.input {
					_ = r.Append(entry.input[i : i+1])
				}
				_ = r.Finish()
			})
			if allocations > float64(3*len(entry.input)) {
				t.Fatalf("%.0f allocations for %d masked byte fragments", allocations, len(entry.input))
			}
		})
	}
}

func TestPublicRedactorLongSecretBoundaryFallback(t *testing.T) {
	const size = 4096
	prefix := bytes.Repeat([]byte("a"), size)
	secret := string(prefix) + "b"
	r, err := NewRedactor([]string{secret}, "[X]")
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Append(prefix); len(got) != 0 {
		t.Fatalf("incomplete prefix emitted %d bytes", len(got))
	}
	// The optimized boundary comparison must decline long rules: repeated
	// prefixes otherwise compare nearly the entire secret at every position.
	if r.safeChunk(prefix) {
		t.Fatal("long repeated-prefix rule entered boundary fast path")
	}
	got := r.Append(prefix)
	got = append(got, r.Append([]byte("b!"))...)
	got = append(got, r.Finish()...)
	want := append(bytes.Clone(prefix), "[X]!"...)
	if !bytes.Equal(got, want) {
		t.Fatalf("output length=%d want=%d", len(got), len(want))
	}
	r, err = NewRedactor([]string{secret}, "[X]")
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Append(prefix); len(got) != 0 {
		t.Fatal("incomplete long prefix escaped")
	}
	if got := string(r.Finish()); got != "[X]" {
		t.Fatalf("unfinished long prefix=%q", got)
	}
}
