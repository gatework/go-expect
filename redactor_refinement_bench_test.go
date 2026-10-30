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
					options = append(options, WithTranscript(io.Discard))
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

func BenchmarkCompetingCaptures(b *testing.B) {
	for _, count := range []int{1, 8} {
		for _, all := range []bool{false, true} {
			b.Run(fmt.Sprintf("cases=%d/all=%v", count, all), func(b *testing.B) {
				input := bytes.Repeat([]byte("a"), 64<<10)
				pattern, err := Compile(`(a+)(z)?()`)
				if err != nil {
					b.Fatal(err)
				}
				cases := make([]Case, count)
				for i := range cases {
					cases[i].Pattern = pattern
					if all {
						cases[i].Pattern = All(pattern, pattern)
					}
				}
				a, peer := net.Pipe()
				s, err := NewSession(a)
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close()
				defer peer.Close()
				b.ReportAllocs()
				for b.Loop() {
					s.pending = input
					result, err := s.Expect(b.Context(), cases)
					if err != nil || len(result.Match) != len(input) {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
