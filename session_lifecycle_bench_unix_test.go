//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package expect

import (
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

func benchmarkCPUTime(b *testing.B) time.Duration {
	b.Helper()
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second + time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}

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
