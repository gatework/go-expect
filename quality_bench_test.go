package expect

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
)

func BenchmarkReceivePatterns(b *testing.B) {
	literal := Literal("END!")
	expression, err := Compile(`E(ND)!`)
	if err != nil {
		b.Fatal(err)
	}
	patterns := []struct {
		name    string
		pattern Pattern
	}{
		{"literal", literal},
		{"all_literals", All(Literal("START"), literal)},
		{"nested_all", All(All(Literal("START"), Literal("START")), literal)},
		{"regexp", expression},
		{"mixed_all", All(Literal("START"), expression)},
	}
	for _, entry := range patterns {
		for _, chunk := range []int{32, 1024} {
			b.Run(fmt.Sprintf("%s/chunk_%d", entry.name, chunk), func(b *testing.B) {
				input := append([]byte("START"), bytes.Repeat([]byte("x"), 32<<10)...)
				input = append(input, "END!"...)
				b.SetBytes(int64(len(input)))
				b.ReportAllocs()
				for b.Loop() {
					s, err := NewSession(&chunkStream{Reader: bytes.NewReader(input), chunk: chunk})
					if err != nil {
						b.Fatal(err)
					}
					result, err := s.Expect(context.Background(), []Case{{Pattern: entry.pattern}})
					closeErr := s.Close()
					if err != nil || closeErr != nil || len(result.Before)+len(result.Match) != len(input) {
						b.Fatalf("result length=%d err=%v close=%v", len(result.Before)+len(result.Match), err, closeErr)
					}
				}
			})
		}
	}
}

var benchmarkDiagnostic []byte

func BenchmarkPublicRedactor(b *testing.B) {
	for _, entry := range []struct {
		name    string
		secrets []string
		input   []byte
	}{
		{"no_rules", nil, bytes.Repeat([]byte("x"), 1024)},
		{"safe", []string{"password", "token"}, bytes.Repeat([]byte("x"), 1024)},
		{"matches", []string{"password", "token"}, bytes.Repeat([]byte("password token "), 64)},
		{"overlaps", []string{"aba", "bab"}, bytes.Repeat([]byte("abab "), 200)},
	} {
		for _, chunk := range []int{1, 32, 1024} {
			b.Run(fmt.Sprintf("%s/chunk_%d", entry.name, chunk), func(b *testing.B) {
				r, err := NewRedactor(entry.secrets, "[hidden]")
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.SetBytes(int64(len(entry.input)))
				for b.Loop() {
					for pos := 0; pos < len(entry.input); pos += chunk {
						benchmarkDiagnostic = r.Append(entry.input[pos:min(pos+chunk, len(entry.input))])
					}
					benchmarkDiagnostic = r.Finish()
				}
			})
		}
	}
}

// Allocation totals describe work performed, while tail-cap-B describes the
// buffers that remain owned by a live redactor after the large append returns.
func BenchmarkRedactorRetainedTail(b *testing.B) {
	input := append(bytes.Repeat([]byte("x"), 1<<20), 's')
	r, err := NewRedactor([]string{"secret"}, "[hidden]")
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	for b.Loop() {
		benchmarkDiagnostic = r.Append(input)
		if len(r.pending) != 5 {
			b.Fatal("unexpected retained window")
		}
		b.ReportMetric(float64(cap(r.pending)+cap(r.hidden)), "tail-cap-B")
		benchmarkDiagnostic = r.Finish()
	}
	runtime.KeepAlive(r)
}

// Repeated prefixes make a candidate-by-candidate cross-boundary comparison
// quadratic despite no secret match. The public benchmark excludes setup and
// observes the steady state with a retained prefix as long as each new chunk.
func BenchmarkPublicRedactorLongBoundary(b *testing.B) {
	for _, size := range []int{32, 256, 4096, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			input := bytes.Repeat([]byte("a"), size)
			r, err := NewRedactor([]string{string(input) + "b"}, "[hidden]")
			if err != nil {
				b.Fatal(err)
			}
			if out := r.Append(input); len(out) != 0 {
				b.Fatal("initial prefix escaped")
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				benchmarkDiagnostic = r.Append(input)
				if len(benchmarkDiagnostic) != size {
					b.Fatal("unexpected output length")
				}
			}
			runtime.KeepAlive(r)
		})
	}
}
