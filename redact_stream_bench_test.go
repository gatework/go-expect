// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
)

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
		if len(r.buffer.Pending) != 5 {
			b.Fatal("unexpected retained window")
		}
		b.ReportMetric(float64(cap(r.buffer.Pending)+cap(r.buffer.Hidden)), "tail-cap-B")
		benchmarkDiagnostic = r.Finish()
	}
	runtime.KeepAlive(r)
}

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

func BenchmarkSessionFilter(b *testing.B) {
	for _, size := range []int{8, 4096, 16384, 65536} {
		for _, chunk := range []int{1, 32, 16384} {
			for _, mode := range []string{"safe", "dense", "mixed"} {
				b.Run(fmt.Sprintf("rule=%d/chunk=%d/%s", size, chunk, mode), func(b *testing.B) {
					var r redactor
					secret := strings.Repeat("a", size)
					r.add([]string{secret})
					if mode == "mixed" {
						r.add([]string{"a"})
					}
					input := bytes.Repeat([]byte("a"), chunk)
					if mode == "safe" {
						input = bytes.Repeat([]byte("x"), chunk)
					}
					_ = r.append([]byte(secret[:len(secret)-1]), false)
					b.ReportAllocs()
					b.SetBytes(int64(chunk))
					for b.Loop() {
						benchmarkDiagnostic = r.append(input, false)
					}
				})
			}
		}
	}
}

func BenchmarkSessionFilterRetention(b *testing.B) {
	for _, sink := range []bool{false, true} {
		b.Run(fmt.Sprintf("sink=%v", sink), func(b *testing.B) {
			const count = 8
			input := append(bytes.Repeat([]byte("x"), 8<<20), []byte("sec")...)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			sessions := make([]*Session, 0, count)
			peers := make([]net.Conn, 0, count)
			for range count {
				a, peer := net.Pipe()
				var options []SessionOption
				if sink {
					options = append(options, WithOutput(io.Discard))
				}
				s, err := NewSession(a, options...)
				if err != nil {
					b.Fatal(err)
				}
				sessions, peers = append(sessions, s), append(peers, peer)
				if err := s.Redact("secret"); err != nil {
					b.Fatal(err)
				}
				if err := s.emit(input, false); err != nil {
					b.Fatal(err)
				}
			}
			defer func() {
				for i, s := range sessions {
					s.Close()
					peers[i].Close()
				}
			}()
			runtime.GC()
			runtime.ReadMemStats(&after)
			retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			for b.Loop() {
				runtime.KeepAlive(sessions)
			}
			b.ReportMetric(float64(retained)/count, "retained-B/session")
			runtime.KeepAlive(sessions)
			runtime.KeepAlive(input)
		})
	}
}
