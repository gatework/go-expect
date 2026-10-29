//go:build linux || darwin

package expect

import (
	"context"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type observedRawConn struct {
	syscall.RawConn
	reads     atomic.Int32
	controls  atomic.Int32
	entered   chan struct{}
	returning chan struct{}
	release   chan struct{}
}

func (r *observedRawConn) Read(f func(uintptr) bool) error {
	first := r.reads.Add(1) == 1
	if first {
		close(r.entered)
	}
	err := r.RawConn.Read(f)
	if first && r.release != nil {
		close(r.returning)
		<-r.release
	}
	return err
}
func (r *observedRawConn) Control(f func(uintptr)) error {
	r.controls.Add(1)
	return r.RawConn.Control(f)
}

type observedTransport struct {
	*os.File
	raw   *observedRawConn
	wakes atomic.Int32
}

func (p *observedTransport) SyscallConn() (syscall.RawConn, error) { return p.raw, nil }
func (p *observedTransport) SetReadDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		p.wakes.Add(1)
	}
	return p.File.SetReadDeadline(deadline)
}

func TestOSPollingUsesRuntimeWaitWithoutPeriodicWake(t *testing.T) {
	a, _ := socketTransport(t)
	raw, err := a.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	observed := &observedRawConn{RawConn: raw, entered: make(chan struct{})}
	s, err := NewSession(&observedTransport{File: a, raw: observed}, WithOSPolling())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	select {
	case <-observed.entered:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("OS read pump did not enter runtime RawConn.Read")
	}
	controls := observed.controls.Load()
	time.Sleep(35 * time.Millisecond)
	if observed.reads.Load() != 1 || observed.controls.Load() != controls {
		t.Fatalf("idle pump woke: reads=%d controls=%d->%d", observed.reads.Load(), controls, observed.controls.Load())
	}
}

type normalizedTransport struct {
	*os.File
	normalized error
}

func (p *normalizedTransport) NormalizeReadError(err error) error {
	if errors.Is(err, io.EOF) {
		return p.normalized
	}
	return err
}

func TestOSPollingHonorsWrappedReadNormalizer(t *testing.T) {
	a, b := socketTransport(t)
	want := errors.New("normalized read end")
	access, err := newOSAccess(&normalizedTransport{File: a, normalized: want})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	var scratch []byte
	_, got, ready := access.read(0, &scratch)
	if !ready || !errors.Is(got, want) {
		t.Fatalf("ready=%v err=%v, want wrapped normalizer", ready, got)
	}
}

func TestPollCanceledRequestKeepsPumpOwnershipUntilAcknowledged(t *testing.T) {
	a, _ := socketTransport(t)
	raw, err := a.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	observed := &observedRawConn{RawConn: raw, entered: make(chan struct{}), returning: make(chan struct{}), release: make(chan struct{})}
	transport := &observedTransport{File: a, raw: observed}
	s, err := NewSession(transport, WithOSPolling())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer func() {
		select {
		case <-observed.release:
		default:
			close(observed.release)
		}
	}()
	<-observed.entered
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() {
		_, err := s.Poll(ctx, []Case{{Pattern: Literal("absent")}})
		first <- err
	}()
	<-observed.returning // The wake reached the owning reader; it has not acknowledged.
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled poll=%v", err)
	}
	s.pollMu.Lock()
	request := s.pollRequest
	s.pollMu.Unlock()
	if request == nil {
		t.Fatal("caller cancellation relinquished the pump-owned request")
	}
	select {
	case <-request.ack:
		t.Fatal("request acknowledged before its readiness read")
	default:
	}
	second := make(chan error, 1)
	go func() {
		_, err := s.Poll(t.Context(), []Case{{Pattern: Literal("absent")}})
		second <- err
	}()
	close(observed.release)
	if err := <-second; !errors.Is(err, ErrTimeout) {
		t.Fatalf("next poll=%v", err)
	}
	if transport.wakes.Load() != 2 {
		t.Fatalf("next poll did not establish its own readiness boundary: wakes=%d", transport.wakes.Load())
	}
}

func TestPollPublishesReadBeforeImmediateReadAndAck(t *testing.T) {
	a, b := socketTransport(t)
	raw, err := a.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	observed := &observedRawConn{RawConn: raw, entered: make(chan struct{}), returning: make(chan struct{}), release: make(chan struct{})}
	s, err := NewSession(&observedTransport{File: a, raw: observed}, WithOSPolling())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defer func() {
		select {
		case <-observed.release:
		default:
			close(observed.release)
		}
	}()
	<-observed.entered
	if _, err := b.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	<-observed.returning // Prior bytes are read but not yet published.
	ack, owned, err := s.requestPoll()
	if err != nil || !owned {
		t.Fatalf("request owned=%v err=%v", owned, err)
	}
	if _, err := b.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	close(observed.release)
	first := <-s.events
	if string(first.data) != "before" || first.err != nil {
		t.Fatalf("first=%q err=%v", first.data, first.err)
	}
	second := <-s.events
	if string(second.data) != "after" || second.err != nil {
		t.Fatalf("second=%q err=%v", second.data, second.err)
	}
	<-ack
}

type noDeadlineTransport struct{ file *os.File }

func (p *noDeadlineTransport) Read(data []byte) (int, error)  { return p.file.Read(data) }
func (p *noDeadlineTransport) Write(data []byte) (int, error) { return p.file.Write(data) }
func (p *noDeadlineTransport) Close() error                   { return p.file.Close() }
func (p *noDeadlineTransport) SyscallConn() (syscall.RawConn, error) {
	return p.file.SyscallConn()
}

func TestOSPollingConstructionFallback(t *testing.T) {
	a, b := socketTransport(t)
	s, err := NewSession(&noDeadlineTransport{file: a}, WithOSPolling())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.osIO.deadline != nil {
		t.Fatal("transport without deadlines selected runtime reader")
	}
	if _, err := b.Write([]byte("ready")); err != nil {
		t.Fatal(err)
	}
	result, err := s.Poll(t.Context(), []Case{{Pattern: Literal("ready")}})
	if err != nil || result.Match != "ready" {
		t.Fatalf("fallback match=%q err=%v", result.Match, err)
	}
	if _, err := s.Poll(t.Context(), []Case{{Pattern: Literal("absent")}}); !errors.Is(err, ErrTimeout) {
		t.Fatalf("fallback idle=%v", err)
	}
}

func TestPollAfterPumpExitDoesNotPublishUnownedRequest(t *testing.T) {
	a, b := socketTransport(t)
	s, err := NewSession(a, WithOSPolling())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	s.workers.Wait()
	if _, err := s.Poll(t.Context(), []Case{{Pattern: EOF()}}); err != nil {
		t.Fatal(err)
	}
	s.pollMu.Lock()
	request := s.pollRequest
	s.pollMu.Unlock()
	if request != nil {
		t.Fatal("Poll published a request after its owning pump exited")
	}
}

func TestCloseJoinsRuntimePumpAndAcknowledgesPoll(t *testing.T) {
	a, _ := socketTransport(t)
	s, err := NewSession(a, WithOSPolling())
	if err != nil {
		t.Fatal(err)
	}
	ack, _, err := s.requestPoll()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ack:
	default:
		t.Fatal("Close returned before the pump acknowledged its request")
	}
}
