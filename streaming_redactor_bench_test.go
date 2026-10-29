package expect

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
)

// These benchmarks use the public API and compile against the round-one source
// snapshot. Rule setup is outside the timed streaming loop.
func BenchmarkPublicRedactorDenseLong(b *testing.B) {
	for _, size := range []int{256, 1024, 4096, 16384, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			input := bytes.Repeat([]byte("a"), size*4)
			r, err := NewRedactor([]string{string(input[:size])}, "[X]")
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			for b.Loop() {
				benchmarkDiagnostic = r.Append(input)
				if string(benchmarkDiagnostic) != "[X]" {
					b.Fatal("dense match differs")
				}
				benchmarkDiagnostic = r.Finish()
				if len(benchmarkDiagnostic) != 0 {
					b.Fatal("overlap escaped")
				}
			}
		})
	}
}

func BenchmarkPublicRedactorMixedDenseByte(b *testing.B) {
	for _, size := range []int{257, 4096, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			r, err := NewRedactor([]string{string(bytes.Repeat([]byte("a"), size)), "a"}, "[X]")
			if err != nil {
				b.Fatal(err)
			}
			_ = r.Append(bytes.Repeat([]byte("a"), size*2))
			input := []byte("a")
			b.ReportAllocs()
			b.SetBytes(1)
			for b.Loop() {
				benchmarkDiagnostic = r.Append(input)
				if len(benchmarkDiagnostic) != 0 {
					b.Fatal("covered byte escaped")
				}
			}
			runtime.KeepAlive(r)
		})
	}
}
