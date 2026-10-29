//go:build linux || darwin

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestOSReadTransfersCompactTinyBuffer(t *testing.T) {
	a, b := socketTransport(t)
	access, e := newOSAccess(a)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.Write([]byte("x")); e != nil {
		t.Fatal(e)
	}
	var scratch []byte
	data, e, ready := access.read(0, &scratch)
	if e != nil || !ready || string(data) != "x" {
		t.Fatalf("data=%q ready=%v err=%v", data, ready, e)
	}
	if cap(data) > 2*len(data) {
		t.Fatalf("tiny read retains capacity %d for %d byte", cap(data), len(data))
	}
}
func TestSendAllTinyReadQueueRetainedMemoryAndPoll(t *testing.T) {
	a, b := socketTransport(t)
	const limit = 1024
	s, e := NewSession(a, WithOSPolling(), WithBufferLimit(limit))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	text := strings.Repeat("r", 2<<20)
	sent := make(chan error, 1)
	go func() { _, e := s.SendAll(ctx, text); sent <- e }()
	for n := 1; n <= 256; n++ {
		if _, e = b.Write([]byte("x")); e != nil {
			t.Fatal(e)
		}
		waitQueuedBytes(t, s, n)
	}
	s.inputGate <- struct{}{}
	payload := 0
	for _, event := range s.queued {
		payload += cap(event.data)
	}
	metadata := cap(s.queued) * int(unsafe.Sizeof(readEvent{}))
	logical := s.queuedBytes
	<-s.inputGate
	if logical != 256 || payload+metadata > limit+int(unsafe.Sizeof(readEvent{})) {
		t.Fatalf("logical=%d retained payload=%d metadata=%d limit=%d", logical, payload, metadata, limit)
	}
	r, e := s.Poll(ctx, []Case{{Pattern: Literal(strings.Repeat("x", 256))}})
	if e != nil || len(r.Match) != 256 {
		t.Fatalf("queued match hidden while write blocked: len=%d err=%v", len(r.Match), e)
	}
	select {
	case e := <-sent:
		t.Fatalf("write completed before peer read: %v", e)
	default:
	}
	peer := make(chan error, 1)
	go func() { _, e := io.CopyN(io.Discard, b, int64(len(text))); peer <- e }()
	if e = <-sent; e != nil {
		t.Fatal(e)
	}
	if e = <-peer; e != nil {
		t.Fatal(e)
	}
}

// A pump receiving many tiny messages must not allocate one 16 KiB block per
// message. The returned compact blocks remain independently owned.
func TestOSSmallReadsReuseScratch(t *testing.T) {
	a, b := socketTransport(t)
	access, err := newOSAccess(a)
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var first []byte
	var scratch []byte
	for range 100 {
		if _, err = b.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		data, err, ready := access.read(0, &scratch)
		if err != nil || !ready || string(data) != "x" {
			t.Fatalf("data=%q ready=%v err=%v", data, ready, err)
		}
		if first == nil {
			first = data
		}
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4<<10*100 {
		t.Fatalf("small reads allocated %d bytes, want under 4 KiB/read", allocated)
	}
	if string(first) != "x" {
		t.Fatalf("transferred block changed: %q", first)
	}
}

func TestOSReadScratchTransferOwnership(t *testing.T) {
	a, b := socketTransport(t)
	access, err := newOSAccess(a)
	if err != nil {
		t.Fatal(err)
	}
	var scratch []byte
	var snapshots [][]byte
	for _, input := range [][]byte{[]byte("a"), bytes.Repeat([]byte("b"), readChunk/2), []byte("c"), []byte("d")} {
		if _, err := b.Write(input); err != nil {
			t.Fatal(err)
		}
		data, err, ready := access.read(0, &scratch)
		if err != nil || !ready || !bytes.Equal(data, input) {
			t.Fatalf("read length=%d ready=%v err=%v", len(data), ready, err)
		}
		snapshots = append(snapshots, data)
	}
	for i, want := range []string{"a", strings.Repeat("b", readChunk/2), "c", "d"} {
		if string(snapshots[i]) != want {
			t.Fatalf("read %d changed after scratch reuse", i)
		}
	}
	if data, err, ready := access.read(0, &scratch); len(data) != 0 || err != nil || ready {
		t.Fatalf("idle data=%q err=%v ready=%v", data, err, ready)
	}
}

func TestOSPollingPTYEndOfInput(t *testing.T) {
	c, err := NewConsole(WithRaw(), WithSessionOptions(WithOSPolling()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := c.TTY().Write([]byte("last")); err != nil {
		t.Fatal(err)
	}
	if r, err := c.Session().Expect(ctx, []Case{{Pattern: Literal("last")}}); err != nil || r.Match != "last" {
		t.Fatalf("match=%q err=%v", r.Match, err)
	}
	if err := c.TTY().Close(); err != nil {
		t.Fatal(err)
	}
	// A closed PTY slave can surface EIO; the adapter must expose clean EOF.
	if _, err := c.Session().Expect(ctx, []Case{{Pattern: EOF()}}); err != nil {
		t.Fatal(err)
	}
}

func TestOSReadClosedRawConn(t *testing.T) {
	a, _ := socketTransport(t)
	access, err := newOSAccess(a)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	want := access.raw.Control(func(uintptr) { t.Fatal("closed RawConn called Control callback") })
	if want == nil {
		t.Fatal("closed RawConn did not return an error")
	}
	var scratch []byte
	data, err, ready := access.read(0, &scratch)
	if len(data) != 0 || !ready || !errors.Is(err, want) {
		t.Fatalf("data=%q ready=%v err=%v", data, ready, err)
	}
}
