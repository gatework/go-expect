//go:build linux || darwin

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPollRealPTYReadyBeforePump(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	c, e := NewConsole(WithRaw(), WithSessionOptions(WithOSPolling()))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for range 100 {
		if _, e = c.TTY().Write([]byte("ready!")); e != nil {
			t.Fatal(e)
		}
		r, e := c.Session().Poll(t.Context(), []Case{{Pattern: Literal("ready!")}})
		if e != nil || r.Match != "ready!" {
			t.Fatalf("match=%q err=%v", r.Match, e)
		}
	}
	start := time.Now()
	_, e = c.Session().Poll(t.Context(), []Case{{Pattern: Literal("absent")}})
	if !errors.Is(e, ErrTimeout) || time.Since(start) > time.Second {
		t.Fatalf("err=%v elapsed=%v", e, time.Since(start))
	}
}
func TestPollPreservesUnmatchedAndZeroTimeoutWaits(t *testing.T) {
	c, e := NewConsole(WithRaw(), WithSessionOptions(WithOSPolling()))
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.TTY().Write([]byte("prefix"))
	r, e := c.Session().Poll(t.Context(), []Case{{Pattern: Literal("suffix")}})
	if !errors.Is(e, ErrTimeout) || r.Before != "prefix" {
		t.Fatalf("before=%q err=%v", r.Before, e)
	}
	go func() { time.Sleep(20 * time.Millisecond); c.TTY().Write([]byte("suffix")) }()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	r, e = c.Session().Expect(ctx, []Case{{Pattern: Literal("suffix")}}, WithTimeout(0))
	if e != nil || r.Before != "prefix" {
		t.Fatalf("before=%q err=%v", r.Before, e)
	}
}
func socketTransport(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	fds, e := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if e != nil {
		t.Fatal(e)
	}
	for _, fd := range fds {
		if e = unix.SetNonblock(fd, true); e != nil {
			t.Fatal(e)
		}
	}
	a, b := os.NewFile(uintptr(fds[0]), "socket-a"), os.NewFile(uintptr(fds[1]), "socket-b")
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}
func TestSendAllPollImmediateAndBlockedProgress(t *testing.T) {
	a, b := socketTransport(t)
	s, e := NewSession(a, WithOSPolling())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	n, e := s.SendAllPoll(t.Context(), "initial")
	if n != 7 || e != nil {
		t.Fatalf("n=%d err=%v", n, e)
	}
	got := make([]byte, 7)
	if _, e = io.ReadFull(b, got); e != nil || string(got) != "initial" {
		t.Fatalf("got=%q err=%v", got, e)
	}
	payload := strings.Repeat("x", 8<<20)
	n, e = s.SendAllPoll(t.Context(), payload)
	var w *WriteError
	if n <= 0 || n >= len(payload) || !errors.Is(e, ErrTimeout) || !errors.As(e, &w) || w.Written != n || !s.isClosed() {
		t.Fatalf("n=%d err=%v", n, e)
	}
}
func TestPollEOFAndPriority(t *testing.T) {
	a, b := socketTransport(t)
	s, e := NewSession(a, WithOSPolling())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	b.Write([]byte("first last"))
	b.Close()
	r, e := s.Poll(t.Context(), []Case{{Pattern: Literal("last")}, {Pattern: Literal("first")}}, WithDeclarationPriority())
	if e != nil || r.CaseIndex != 0 {
		t.Fatalf("case=%d err=%v", r.CaseIndex, e)
	}
	if _, e = s.Poll(t.Context(), []Case{{Pattern: EOF()}}); e != nil {
		t.Fatal(e)
	}
}
func TestSendAllRealBidirectionalBackpressure(t *testing.T) {
	a, b := socketTransport(t)
	s, e := NewSession(a, WithBufferLimit(1<<20), WithWriteTimeout(time.Nanosecond))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	output := strings.Repeat("o", 512<<10)
	input := strings.Repeat("i", 1<<20)
	peer := make(chan error, 1)
	go func() {
		if _, e := io.WriteString(b, output); e != nil {
			peer <- e
			return
		}
		got := make([]byte, len(input))
		_, e := io.ReadFull(b, got)
		if e == nil && !bytes.Equal(got, []byte(input)) {
			e = errors.New("input differs")
		}
		peer <- e
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	n, e := s.SendAll(ctx, input)
	if e != nil || n != len(input) {
		t.Fatalf("n=%d err=%v", n, e)
	}
	if e = <-peer; e != nil {
		t.Fatal(e)
	}
	r, e := s.Expect(ctx, []Case{{Pattern: Literal(output)}})
	if e != nil || r.Match != output {
		t.Fatalf("len=%d err=%v", len(r.Match), e)
	}
}
