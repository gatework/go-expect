// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type deadlineTransport struct {
	*admissionTransport
	set func(time.Time) error
}

func (p *deadlineTransport) SetWriteDeadline(deadline time.Time) error {
	return p.set(deadline)
}

func TestSendDeadlineSetupFailure(t *testing.T) {
	failure := errors.New("deadline setup failed")
	for _, cause := range []error{failure, net.ErrClosed, errors.Join(os.ErrNoDeadline, failure), errors.Join(errors.ErrUnsupported, failure)} {
		t.Run(cause.Error(), func(t *testing.T) {
			p := &deadlineTransport{
				admissionTransport: &admissionTransport{closed: make(chan struct{})},
				set:                func(time.Time) error { return cause },
			}
			s, err := NewSession(p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			n, err := s.Send(t.Context(), "command")
			if n != 0 || !errors.Is(err, cause) || p.writes.Load() != 0 || !s.isClosed() {
				t.Fatalf("n=%d err=%v writes=%d closed=%v", n, err, p.writes.Load(), s.isClosed())
			}
		})
	}
}

func TestSendUnsupportedDeadlineFallsBack(t *testing.T) {
	for _, cause := range []error{os.ErrNoDeadline, errors.ErrUnsupported} {
		t.Run(cause.Error(), func(t *testing.T) {
			p := &deadlineTransport{
				admissionTransport: &admissionTransport{closed: make(chan struct{})},
				set:                func(time.Time) error { return cause },
			}
			s, err := NewSession(p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			n, err := s.Send(t.Context(), "command")
			if n != 7 || err != nil || p.writes.Load() != 1 || s.isClosed() {
				t.Fatalf("n=%d err=%v writes=%d closed=%v", n, err, p.writes.Load(), s.isClosed())
			}
		})
	}
}

func TestSendCanceledAdmissionRestoresDeadline(t *testing.T) {
	for _, restoreErr := range []error{nil, errors.New("deadline reset failed")} {
		name := "restored"
		if restoreErr != nil {
			name = "restore failed"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			p := &deadlineTransport{
				admissionTransport: &admissionTransport{closed: make(chan struct{})},
				set: func(time.Time) error {
					calls++
					if calls == 1 {
						cancel()
						return nil
					}
					return restoreErr
				},
			}
			s, err := NewSession(p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			n, err := s.Send(ctx, "command")
			var writeErr *WriteError
			if n != 0 || !errors.Is(err, context.Canceled) || errors.As(err, &writeErr) || p.writes.Load() != 0 {
				t.Fatalf("n=%d err=%v writes=%d", n, err, p.writes.Load())
			}
			if s.isClosed() != (restoreErr != nil) || (restoreErr != nil && !errors.Is(err, restoreErr)) {
				t.Fatalf("reset failure lost: err=%v closed=%v", err, s.isClosed())
			}
			if restoreErr == nil {
				if _, err := s.Send(t.Context(), "next"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSendDeadlineResetFailureRetainsProgress(t *testing.T) {
	failure := errors.New("deadline reset failed")
	for _, cause := range []error{failure, errors.Join(net.ErrClosed, failure)} {
		t.Run(cause.Error(), func(t *testing.T) {
			calls := 0
			p := &deadlineTransport{
				admissionTransport: &admissionTransport{closed: make(chan struct{})},
				set: func(time.Time) error {
					calls++
					if calls == 1 {
						return nil
					}
					return cause
				},
			}
			s, err := NewSession(p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			// A background context keeps cancellation separate from deadline reset.
			n, err := s.Send(context.Background(), "command")
			var writeErr *WriteError
			if n != 7 || !errors.As(err, &writeErr) || writeErr.Written != n || !errors.Is(err, cause) || !s.isClosed() {
				t.Fatalf("n=%d err=%v closed=%v", n, err, s.isClosed())
			}
			if p.writes.Load() != 1 {
				t.Fatal("write was replayed")
			}
		})
	}
}

type failedWakeTransport struct {
	net.Conn
	wakeErr, closeErr error
	wake              func() error
	deadlines         atomic.Int32
	closes            atomic.Int32
}

func (p *failedWakeTransport) SetWriteDeadline(deadline time.Time) error {
	if p.deadlines.Add(1) == 2 {
		if p.wake != nil {
			return p.wake()
		}
		return p.wakeErr
	}
	return p.Conn.SetWriteDeadline(deadline)
}

func TestSendDeadlineClearAbnormalExitRetires(t *testing.T) {
	for _, kind := range []CallbackFailure{CallbackPanicked, CallbackExited} {
		t.Run(string(kind), func(t *testing.T) {
			calls := 0
			p := &deadlineTransport{
				admissionTransport: &admissionTransport{closed: make(chan struct{})},
				set: func(time.Time) error {
					calls++
					if calls == 2 {
						if kind == CallbackPanicked {
							panic("deadline setter panic")
						}
						runtime.Goexit()
					}
					return nil
				},
			}
			s, err := NewSession(p)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			done := make(chan any, 1)
			go func() {
				defer func() { done <- recover() }()
				_, _ = s.Send(context.Background(), "command")
			}()
			panicked := <-done
			if (panicked != nil) != (kind == CallbackPanicked) {
				t.Fatal("foreground panic propagation changed")
			}
			if !s.isClosed() || p.writes.Load() != 1 {
				t.Fatalf("closed=%v writes=%d", s.isClosed(), p.writes.Load())
			}
			if n, err := s.Send(t.Context(), "next"); n != 0 || !errors.Is(err, ErrClosed) {
				t.Fatalf("aborted deadline cleanup allowed another write: n=%d err=%v", n, err)
			}
		})
	}
}

func TestSendDeadlineWakeAbnormalExitInterrupts(t *testing.T) {
	for _, kind := range []CallbackFailure{CallbackPanicked, CallbackExited} {
		t.Run(string(kind), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				local, remote := net.Pipe()
				defer remote.Close()
				p := &failedWakeTransport{Conn: local, wake: func() error {
					if kind == CallbackPanicked {
						panic("private panic payload")
					}
					runtime.Goexit()
					return nil
				}}
				s, err := NewSession(p, WithWriteTimeout(0))
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan writeResult, 1)
				go func() { n, err := s.Send(ctx, "command"); done <- writeResult{n, err} }()
				prefix := make([]byte, 2)
				if _, err := io.ReadFull(remote, prefix); err != nil {
					t.Fatal(err)
				}
				cancel()
				synctest.Wait()
				var result writeResult
				select {
				case result = <-done:
				default:
					t.Error("aborted deadline wake left the write blocked")
					_ = local.Close()
					result = <-done
				}
				failure, ok := errors.AsType[*CallbackError](result.err)
				if result.n != 2 || !errors.Is(result.err, context.Canceled) || !ok || failure.Kind != kind {
					t.Fatalf("n=%d err=%v failure=%v", result.n, result.err, failure)
				}
				if p.closes.Load() != 1 || !s.isClosed() {
					t.Fatalf("closes=%d closed=%v", p.closes.Load(), s.isClosed())
				}
			})
		})
	}
}

func (p *failedWakeTransport) Close() error {
	p.closes.Add(1)
	return errors.Join(p.Conn.Close(), p.closeErr)
}

func TestSendDeadlineWakeFailureInterruptsTransport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		local, remote := net.Pipe()
		defer remote.Close()
		p := &failedWakeTransport{Conn: local, wakeErr: errors.New("deadline wake failed"), closeErr: errors.New("transport close failed")}
		s, err := NewSession(p, WithWriteTimeout(0))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan writeResult, 1)
		go func() { n, err := s.Send(ctx, "command"); done <- writeResult{n, err} }()
		prefix := make([]byte, 2)
		if _, err := io.ReadFull(remote, prefix); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		var result writeResult
		select {
		case result = <-done:
		default:
			t.Error("failed cancellation wake left the write blocked")
			// Let a broken implementation unwind instead of hanging the test process.
			_ = local.Close()
			result = <-done
		}
		var writeErr *WriteError
		if result.n != 2 || !errors.As(result.err, &writeErr) || writeErr.Written != 2 || string(prefix) != "co" {
			t.Fatalf("partial progress lost: n=%d err=%v prefix=%q", result.n, result.err, prefix)
		}
		for _, cause := range []error{context.Canceled, p.wakeErr, p.closeErr} {
			if !errors.Is(result.err, cause) {
				t.Errorf("missing cause %v in %v", cause, result.err)
			}
		}
		for range 3 {
			if !errors.Is(s.Close(), p.closeErr) {
				t.Error("close error was not retained")
			}
		}
		if p.closes.Load() != 1 || !s.isClosed() {
			t.Fatalf("closes=%d closed=%v", p.closes.Load(), s.isClosed())
		}
	})
}
