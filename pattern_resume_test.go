// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"net"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestRegexpLineLocalClassification(t *testing.T) {
	for expr, want := range map[string]bool{
		`[#>] ?$`:         true,
		`(?m)^\S+[#>] ?$`: true,
		`\bend\b`:         true,
		`a.b`:             true,
		`(?i)Router#`:     true,
		`(?m)^$x|y`:       true,
		`(?s)a.b`:         false,
		`^prompt`:         false,
		`\Aprompt`:        false,
		`a\nb`:            false,
		`[^x]+y`:          false,
		`\s+#`:            false,
		`[\x00-\x7f]#`:    false,
		`(?m)a$\n`:        false,
		`(?:x|(?s:.))`:    false,
	} {
		p, err := Compile(expr)
		if err != nil {
			t.Fatal(err)
		}
		if p.lineLocal != want {
			t.Errorf("%q lineLocal=%v want %v", expr, p.lineLocal, want)
		}
	}
	// POSIX classes exclude LF, but Perl classes include it: both must qualify.
	posix, err := FromRegexp(regexp.MustCompilePOSIX(`[^x]+y`))
	if err != nil || posix.lineLocal {
		t.Fatalf("POSIX import lineLocal=%v err=%v", posix.lineLocal, err)
	}
	imported, err := FromRegexp(regexp.MustCompile(`[#>] ?$`))
	if err != nil || !imported.lineLocal {
		t.Fatalf("Perl import lineLocal=%v err=%v", imported.lineLocal, err)
	}
	// Glob stars and question marks cross newlines; bracket classes do not.
	if Glob("*#").lineLocal || Glob("r?#").lineLocal || !Glob("router[#>]").lineLocal || !Glob("router[#>]").Fold().lineLocal {
		t.Fatal("glob classification")
	}
}

func resumePatterns(t testing.TB) []Pattern {
	var patterns []Pattern
	for _, expr := range []string{`[#>] ?$`, `(?m)^(\S+)[#>] ?$`, `\bend\b`, `(?i)a(b+)c`, `x(y)?z`, `\pL+#`, `设备>`, `(?s)a.b`, `^ab`, `[^x]+y`} {
		p, err := Compile(expr)
		if err != nil {
			t.Fatal(err)
		}
		patterns = append(patterns, p, p.Fold())
	}
	longest := regexp.MustCompile(`a+|a+b`)
	longest.Longest()
	for _, re := range []*regexp.Regexp{regexp.MustCompilePOSIX(`a+b|ab+c`), longest} {
		p, err := FromRegexp(re)
		if err != nil {
			t.Fatal(err)
		}
		patterns = append(patterns, p)
	}
	return patterns
}

// Each resumed scan must agree with a full-view search of the same prefix until
// the first match; the waiter clears offsets once any candidate matches.
func checkRegexpResume(t *testing.T, p Pattern, data []byte, chunk int) {
	t.Helper()
	var prior scanProgress
	for end := 0; ; end = min(end+chunk, len(data)) {
		terminal := end == len(data)
		view := data[:end]
		from := p.resumeFrom(prior)
		got, ok, err := p.find(view, terminal, from)
		want, wantOK, wantErr := p.find(view, terminal, 0)
		if !errors.Is(err, wantErr) || ok != wantOK || got.start != want.start || got.end != want.end || !slices.Equal(got.submatches, want.submatches) {
			t.Fatalf("%s input=%q end=%d: got %+v %v %v want %+v %v %v", p.re, data, end, got, ok, err, want, wantOK, wantErr)
		}
		if ok || err != nil || terminal {
			return
		}
		prior = p.advance(view, prior)
	}
}

func TestRegexpResumeMatchesFullScan(t *testing.T) {
	inputs := []string{
		"show run\r\ninterface Gi0/1\r\n description up\r\nrouter# ",
		"line\nline\nend\npartial end",
		"abc\nab\nb" + strings.Repeat("x", 40) + "\nxyz",
		"\xe8\xae\xbe\xe5\xa4\x87>\n设备>",
		"aaa\naab\naaab abbc",
		"first\n#second\n",
		"x\ny\nz",
	}
	for _, p := range resumePatterns(t) {
		for _, input := range inputs {
			for _, chunk := range []int{1, 2, 3, 7, 64} {
				checkRegexpResume(t, p, []byte(input), chunk)
			}
		}
	}
}

func FuzzRegexpResume(f *testing.F) {
	f.Add([]byte("show\r\nrouter# "), uint8(0), uint8(1))
	f.Add([]byte("aaa\naab\naaab abbc"), uint8(20), uint8(2))
	f.Add([]byte("\xe8\xae\n\xbe设备>"), uint8(12), uint8(1))
	patterns := resumePatterns(f)
	f.Fuzz(func(t *testing.T, data []byte, index, size uint8) {
		if len(data) > 4096 {
			return
		}
		checkRegexpResume(t, patterns[int(index)%len(patterns)], data, int(size)%32+1)
	})
}

func TestSessionRegexpResumeAcrossChunks(t *testing.T) {
	// The trailing space is required, so every chunk size has the same winner.
	prompt, err := Compile(`(?m)^(\S+)[#>] $`)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	for range 200 {
		output.WriteString("interface Gi0/1 is up, line protocol is up\r\n")
	}
	output.WriteString("router# ")
	for _, chunk := range []int{1, 5, 256, 4096} {
		s, err := NewSession(&chunkStream{Reader: bytes.NewReader(output.Bytes()), chunk: chunk})
		if err != nil {
			t.Fatal(err)
		}
		result, err := s.Expect(t.Context(), []Case{{Pattern: prompt}})
		_ = s.Close()
		start := output.Len() - len("router# ")
		if err != nil || result.Start != start || result.Match != "router# " || len(result.Captures) != 1 || result.Captures[0].Text != "router" || result.Captures[0].Start != start {
			t.Fatalf("chunk=%d result=%+v err=%v", chunk, result, err)
		}
	}
}

// A NoConsume candidate is observed again once input grows. Its own scan
// progress must not let a literal resume past the hit it left unconsumed.
func TestNoConsumeLiteralReobservedAfterAppend(t *testing.T) {
	s, err := NewSession(&chunkStream{Reader: bytes.NewReader([]byte("abc")), chunk: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var starts []int
	result, err := s.Expect(t.Context(), []Case{
		{Pattern: Literal("ab"), NoConsume: true, Handler: func(_ context.Context, _ *Session, r Result) (Action, error) {
			starts = append(starts, r.Start)
			return Continue, nil
		}},
		{Pattern: Literal("c")},
	})
	if err != nil || result.CaseIndex != 1 || !slices.Equal(starts, []int{0, 0}) {
		t.Fatalf("starts=%v result=%+v err=%v", starts, result, err)
	}
}

// Progress recorded before another owner consumed input is discarded by the
// consume generation, without that owner resetting this waiter.
func TestScanProgressFollowsConsumeGeneration(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	s, err := NewSession(a)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	prompt, err := Compile(`router#`)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []Pattern{Literal("needle"), prompt} {
		w := waiter{session: s}
		if err := w.prepare(t.Context(), []Case{{Pattern: p}}, nil); err != nil {
			t.Fatal(err)
		}
		s.receive.pending = []byte("xx\nneedl\nroute")
		if chosen, _, err := w.scanText(); chosen >= 0 || err != nil {
			t.Fatalf("premature match %d %v", chosen, err)
		}
		// Another owner consumes "xx\n" and new bytes arrive.
		s.receive.consumed++
		s.receive.pending = []byte("needle\nrouter#")
		chosen, loc, err := w.scanText()
		want := map[bool]int{true: 0, false: 7}[p.kind == literalPattern]
		if chosen != 0 || err != nil || loc.start != want {
			t.Fatalf("%s: chosen=%d loc=%+v err=%v", p, chosen, loc, err)
		}
	}
	s.receive.pending = nil
}
