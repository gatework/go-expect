//go:build linux || darwin

package expect

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func benchmarkSocketPair(b *testing.B) (*os.File, *os.File) {
	b.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		b.Fatal(err)
	}
	for _, fd := range fds {
		if err := unix.SetNonblock(fd, true); err != nil {
			b.Fatal(err)
		}
	}
	return os.NewFile(uintptr(fds[0]), "benchmark-session"), os.NewFile(uintptr(fds[1]), "benchmark-peer")
}

// One live Session receives successive messages, isolating the OS pump from
// per-Session construction. Each message requires a distinct readiness/read pass.
func BenchmarkOSReceive(b *testing.B) {
	for _, size := range []int{1, 32, 1024, 16384} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			transport, peer := benchmarkSocketPair(b)
			s, err := NewSession(transport, WithOSPolling())
			if err != nil {
				transport.Close()
				peer.Close()
				b.Fatal(err)
			}
			defer s.Close()
			defer peer.Close()
			input := bytes.Repeat([]byte("x"), size)
			cases := []Case{{Pattern: Literal(string(input))}}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if _, err := peer.Write(input); err != nil {
					b.Fatal(err)
				}
				result, err := s.Expect(context.Background(), cases)
				if err != nil || len(result.Match) != size {
					b.Fatalf("match length=%d err=%v", len(result.Match), err)
				}
			}
		})
	}
}

func benchmarkCPUTime(b *testing.B) time.Duration {
	b.Helper()
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second + time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}

// Idle samples include a fixed 20ms observation period. Wall ns/op is therefore
// a sampling interval; cpu-ns/session-ms and retained-B/session are the useful
// measurements. Run serially, without other tests in the same process.
func BenchmarkIdleSessions(b *testing.B) {
	for _, polling := range []bool{false, true} {
		for _, count := range []int{1, 64} {
			b.Run(fmt.Sprintf("os_%v/count_%d", polling, count), func(b *testing.B) {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				sessions := make([]*Session, 0, count)
				peers := make([]io.Closer, 0, count)
				for range count {
					var transport io.ReadWriteCloser
					var peer io.Closer
					var options []SessionOption
					if polling {
						transport, peer = benchmarkSocketPair(b)
						options = []SessionOption{WithOSPolling()}
					} else {
						transport, peer = net.Pipe()
					}
					s, err := NewSession(transport, options...)
					if err != nil {
						transport.Close()
						peer.Close()
						b.Fatal(err)
					}
					sessions, peers = append(sessions, s), append(peers, peer)
				}
				defer func() {
					for i, s := range sessions {
						s.Close()
						peers[i].Close()
					}
				}()
				time.Sleep(20 * time.Millisecond)
				runtime.GC()
				runtime.ReadMemStats(&after)
				retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
				startCPU := benchmarkCPUTime(b)
				start := time.Now()
				b.ReportAllocs()
				for b.Loop() {
					time.Sleep(20 * time.Millisecond)
				}
				elapsed := time.Since(start)
				cpu := benchmarkCPUTime(b) - startCPU
				b.ReportMetric(float64(cpu.Nanoseconds())/(float64(count)*float64(elapsed)/float64(time.Millisecond)), "cpu-ns/session-ms")
				b.ReportMetric(float64(retained)/float64(count), "retained-B/session")
				runtime.KeepAlive(sessions)
			})
		}
	}
}
