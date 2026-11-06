// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestRelayFanoutAndRedaction(t *testing.T) {
	source, p := pair(t)
	a, x := pair(t)
	b, y := pair(t)
	if err := source.Redact("secret"); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	got := make(chan string, 2)
	for _, peer := range []io.Reader{x, y} {
		go func(peer io.Reader) { buf := make([]byte, 6); _, _ = io.ReadFull(peer, buf); got <- string(buf) }(peer)
	}
	go func() { _, _ = p.Write([]byte("secretSTOPtail")) }()
	err := Relay(t.Context(), []Route{{Source: source, Targets: []*Session{a, b}, Output: &output, QueueLimit: 3, Cases: []Case{{Pattern: Literal("STOP")}}}})
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != "******" {
		t.Fatalf("output=%q", output.String())
	}
	for range 2 {
		if text := <-got; text != "secret" {
			t.Fatalf("target received %q", text)
		}
	}
	waitText(t, source, "tail")
}

type blockedRelayTransport struct {
	entered, closed chan struct{}
	once            sync.Once
}

func (p *blockedRelayTransport) Read([]byte) (int, error) { <-p.closed; return 0, io.EOF }
func (p *blockedRelayTransport) Write([]byte) (int, error) {
	close(p.entered)
	<-p.closed
	return 0, io.ErrClosedPipe
}
func (p *blockedRelayTransport) Close() error { p.once.Do(func() { close(p.closed) }); return nil }
func TestRelayCancellationJoinsAdmittedWrite(t *testing.T) {
	source, p := pair(t)
	transport := &blockedRelayTransport{entered: make(chan struct{}), closed: make(chan struct{})}
	target, err := NewSession(transport)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	go func() { _, _ = p.Write([]byte("payload")) }()
	go func() { <-transport.entered; cancel() }()
	err = Relay(ctx, []Route{{Source: source, Targets: []*Session{target}, QueueLimit: 4}})
	var routeErr *RouteError
	var writeErr *WriteError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &routeErr) || !errors.As(err, &writeErr) || !target.isClosed() {
		t.Fatalf("closed=%v err=%v", target.isClosed(), err)
	}
	if source.expecting.Load() {
		t.Fatal("source reservation leaked")
	}
}
