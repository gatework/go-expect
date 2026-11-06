// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"strings"
	"time"
)

// SendSlow writes byte chunks under one cancellable send lock. The interval is
// between chunks; it never splits a write retry. Context bounds the whole send.
func (s *Session) SendSlow(ctx context.Context, text string, chunk int, interval time.Duration) (n int, err error) {
	defer func() {
		s.notify(ctx, Event{SessionID: s.id, RuleIndex: -1, Operation: OpSend, CaseIndex: -1, Bytes: n, Error: errorClass(err)})
	}()
	if ctx == nil {
		return 0, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if chunk <= 0 || interval < 0 {
		return 0, &ConfigError{Option: "slow send", Reason: "positive chunk and nonnegative interval required"}
	}
	if err = s.acquireWrite(ctx); err != nil {
		return 0, err
	}
	defer s.writeGate.release()
	deadline, _ := ctx.Deadline()
	for n < len(text) {
		count, e := s.writeOnce(ctx, text[n:min(len(text), n+chunk)], deadline)
		n += count
		if e != nil {
			if n == 0 {
				return 0, e
			}
			return n, &WriteError{Written: n, Cause: errors.Join(e, s.Close())}
		}

		if n < len(text) && interval > 0 {
			timer := time.NewTimer(interval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return n, &WriteError{Written: n, Cause: errors.Join(ctx.Err(), s.Close())}
			case <-s.done:
				timer.Stop()
				return n, &WriteError{Written: n, Cause: errors.Join(ErrClosed, s.Close())}
			}
		}
	}
	return n, nil
}

// SendNull sends count NUL bytes in bounded chunks under one send reservation.
// WithWriteTimeout bounds the whole operation, including admission.
func (s *Session) SendNull(ctx context.Context, count int) (n int, err error) {
	defer func() {
		s.notify(ctx, Event{SessionID: s.id, Operation: OpSend, CaseIndex: -1, RuleIndex: -1, Bytes: n, Error: errorClass(err)})
	}()
	if ctx == nil {
		return 0, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if count < 0 {
		return 0, &ConfigError{Option: "null count", Reason: "must be nonnegative"}
	}
	deadline, _ := ctx.Deadline()
	if s.opts.writeTimeout > 0 {
		deadline = earlier(deadline, after(time.Now(), s.opts.writeTimeout))
	}
	if err = s.writeGate.acquire(ctx, s.done, deadline); err != nil {
		return 0, err
	}
	defer s.writeGate.release()
	data := strings.Repeat("\x00", min(count, 4096))
	for n < count {
		written, e := s.writeOnce(ctx, data[:min(len(data), count-n)], deadline)
		n += written
		if e != nil {
			return n, s.sendProgressError(n, e)
		}
	}
	return n, nil
}

// Pacer returns the delay before the byte at offset. It runs under send
// serialization and must return a nonnegative duration without blocking.
type Pacer func(offset int) time.Duration

// SendPaced serializes an entire user-defined pacing policy. No admitted partial
// write is retried; cancellation after any progress retires the Session.
func (s *Session) SendPaced(ctx context.Context, text string, pacer Pacer) (n int, err error) {
	defer func() {
		s.notify(ctx, Event{SessionID: s.id, Operation: OpSend, CaseIndex: -1, RuleIndex: -1, Bytes: n, Error: errorClass(err)})
	}()
	if ctx == nil || pacer == nil {
		return 0, &ConfigError{Option: "pacer", Reason: "context and policy required"}
	}
	if err = s.acquireWrite(ctx); err != nil {
		return 0, err
	}
	defer s.writeGate.release()
	completed := false
	defer func() {
		if !completed && n > 0 {
			_ = s.Close()
		}
	}()
	deadline, _ := ctx.Deadline()
	for n < len(text) {
		delay := pacer(n)
		if e := s.pauseSend(ctx, delay); e != nil {
			completed = true
			return n, s.sendProgressError(n, e)
		}
		written, e := s.writeOnce(ctx, text[n:n+1], deadline)
		n += written
		if e != nil {
			completed = true
			return n, s.sendProgressError(n, e)
		}
	}
	completed = true
	return n, nil
}
func (s *Session) pauseSend(ctx context.Context, delay time.Duration) error {
	if delay < 0 {
		return &ConfigError{Option: "pacer delay", Reason: "must be nonnegative"}
	}
	if delay == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	}
}
func (s *Session) sendProgressError(written int, err error) error {
	if written == 0 {
		return err
	}
	return &WriteError{Written: written, Cause: errors.Join(err, s.Close())}
}

// ErrBreakUnsupported indicates that the transport cannot assert a line break.
var ErrBreakUnsupported = errors.New("expect: transport break unsupported")

// SendBreak serializes a transport-specific line break with other writes.
func (s *Session) SendBreak(ctx context.Context, duration time.Duration) error {
	if ctx == nil {
		return &ConfigError{Option: "context", Reason: "nil context"}
	}
	if duration < 0 {
		return &ConfigError{Option: "break", Reason: "negative duration"}
	}
	breaker, ok := s.transport.(interface {
		SendBreak(context.Context, time.Duration) error
	})
	if !ok {
		return ErrBreakUnsupported
	}
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.writeGate.release()
	if err := breaker.SendBreak(ctx, duration); err != nil {
		return errors.Join(err, s.Close())
	}
	return nil
}
