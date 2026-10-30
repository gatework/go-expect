//go:build linux || darwin

package expect

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func BenchmarkPTYReceive(b *testing.B) {
	for _, size := range []int{1, 32, 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			console, err := NewConsole(WithRaw(), WithSessionOptions(WithOSPolling()))
			if err != nil {
				b.Fatal(err)
			}
			defer console.Close()
			input := bytes.Repeat([]byte("x"), size)
			cases := []Case{{Pattern: Literal(string(input))}}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if _, err := console.TTY().Write(input); err != nil {
					b.Fatal(err)
				}
				result, err := console.Session().Expect(b.Context(), cases)
				if err != nil || len(result.Match) != size {
					b.Fatalf("match=%d err=%v", len(result.Match), err)
				}
			}
		})
	}
}

// Run each population in a fresh process for resource comparisons: Go may keep
// OS threads created by an earlier native-poll population. The sleep is only a
// fixed observation interval; CPU and retained resource metrics describe idle.
func BenchmarkOSIdlePopulation(b *testing.B) {
	benchmarkOSIdlePopulation(b, true)
}

// A separate resource-only run permits correcting heap measurement without
// repeating the native backend's expensive sequential latency measurements.
func BenchmarkOSIdleHeap(b *testing.B) {
	benchmarkOSIdlePopulation(b, false)
}

func benchmarkOSIdlePopulation(b *testing.B, latency bool) {
	for _, count := range []int{1, 64, 256, 1024} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			beforeFDs, beforeThreads := benchmarkFDs(b), benchmarkThreads(b)
			// Resource probes allocate transient buffers. Collect those before
			// the baseline so the second GC cannot subtract them from the heap.
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			sessions := make([]*Session, 0, count)
			peers := make([]*os.File, 0, count)
			for range count {
				a, peer := benchmarkSocketPair(b)
				s, err := NewSession(a, WithOSPolling())
				if err != nil {
					b.Fatal(err)
				}
				sessions, peers = append(sessions, s), append(peers, peer)
			}
			defer func() {
				var cleanup sync.WaitGroup
				for i, s := range sessions {
					cleanup.Go(func() {
						s.Close()
						peers[i].Close()
					})
				}
				cleanup.Wait()
			}()
			time.Sleep(50 * time.Millisecond)
			runtime.GC()
			runtime.ReadMemStats(&after)
			retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			fds, threads := benchmarkFDs(b), benchmarkThreads(b)
			startCPU := benchmarkCPUTime(b)
			start := time.Now()
			for b.Loop() {
				time.Sleep(50 * time.Millisecond)
			}
			elapsed, cpu := time.Since(start), benchmarkCPUTime(b)-startCPU
			b.ReportMetric(float64(cpu.Nanoseconds())/(float64(count)*float64(elapsed)/float64(time.Millisecond)), "cpu-ns/session-ms")
			b.ReportMetric(float64(retained)/float64(count), "retained-B/session")
			b.ReportMetric(float64(fds-beforeFDs), "fd-delta")
			b.ReportMetric(float64(threads-beforeThreads), "thread-delta")
			b.ReportMetric(float64(threads), "threads")
			if !latency {
				runtime.KeepAlive(sessions)
				return
			}
			polls, closes := make([]time.Duration, 0, count), make([]time.Duration, 0, count)
			for _, s := range sessions {
				start := time.Now()
				_, err := s.Poll(b.Context(), []Case{{Pattern: Literal("absent")}})
				polls = append(polls, time.Since(start))
				if !errors.Is(err, ErrTimeout) {
					b.Fatal(err)
				}
			}
			for _, s := range sessions {
				start := time.Now()
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
				closes = append(closes, time.Since(start))
			}
			slices.Sort(polls)
			slices.Sort(closes)
			b.ReportMetric(float64(polls[count/2].Nanoseconds()), "poll-p50-ns")
			b.ReportMetric(float64(polls[(count-1)*95/100].Nanoseconds()), "poll-p95-ns")
			b.ReportMetric(float64(closes[count/2].Nanoseconds()), "close-p50-ns")
			b.ReportMetric(float64(closes[(count-1)*95/100].Nanoseconds()), "close-p95-ns")
			runtime.KeepAlive(sessions)
		})
	}
}

func benchmarkFDs(b *testing.B) int {
	b.Helper()
	path := "/dev/fd"
	if runtime.GOOS == "linux" {
		path = "/proc/self/fd"
	}
	directory, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer directory.Close()
	// Descriptor entries may disappear during enumeration. Count names without
	// statting them, which can fail with EBADF on Darwin's /dev/fd.
	names, err := directory.Readdirnames(-1)
	if err != nil {
		b.Fatal(err)
	}
	return len(names)
}

func benchmarkThreads(b *testing.B) int {
	b.Helper()
	if runtime.GOOS == "linux" {
		status, err := os.ReadFile("/proc/self/status")
		if err != nil {
			b.Fatal(err)
		}
		for line := range strings.SplitSeq(string(status), "\n") {
			if value, ok := strings.CutPrefix(line, "Threads:"); ok {
				count, err := strconv.Atoi(strings.TrimSpace(value))
				if err != nil {
					b.Fatal(err)
				}
				return count
			}
		}
		b.Fatal("missing thread count")
	}
	output, err := exec.Command("ps", "-M", "-p", strconv.Itoa(os.Getpid()), "-o", "pid=").Output()
	if err != nil {
		b.Fatal(err)
	}
	return len(bytes.Split(bytes.TrimSpace(output), []byte("\n")))
}
