//go:build linux || darwin

package expect

import (
	"context"
	"io"
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
	data, e, ready := access.read(0)
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
