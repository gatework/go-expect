package expect

import (
	"bytes"
	"fmt"
	"net"
	"runtime"
	"testing"
)

func BenchmarkReceiveRemainderRetention(b *testing.B) {
	for _, held := range []bool{false, true} {
		for _, remainder := range []int{0, 8, 4096} {
			b.Run(fmt.Sprintf("held_%v/tail_%d", held, remainder), func(b *testing.B) {
				const count = 16
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				sessions := make([]*Session, 0, count)
				peers := make([]net.Conn, 0, count)
				results := make([]Result, 0, count)
				for range count {
					a, peer := net.Pipe()
					s, err := NewSession(a)
					if err != nil {
						b.Fatal(err)
					}
					sessions, peers = append(sessions, s), append(peers, peer)
					input := make([]byte, (1<<20)+1+remainder)
					copy(input, bytes.Repeat([]byte("x"), 1<<20))
					input[1<<20] = '!'
					if err := s.acceptEvent(readEvent{data: input}); err != nil {
						b.Fatal(err)
					}
					result, err := s.Expect(b.Context(), []Case{{Pattern: Literal("!")}})
					if err != nil || len(result.Before) != 1<<20 {
						b.Fatalf("before=%d err=%v", len(result.Before), err)
					}
					if held {
						results = append(results, result)
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
					runtime.KeepAlive(results)
				}
				b.ReportMetric(float64(retained)/count, "retained-B/session")
				runtime.KeepAlive(sessions)
				runtime.KeepAlive(results)
			})
		}
	}
}
