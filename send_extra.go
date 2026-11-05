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
	defer func() { s.notify(ctx, Event{Operation: OpSend, CaseIndex: -1, Bytes: n, Error: errorClass(err)}) }()
	if ctx == nil {
		return 0, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if chunk <= 0 || interval < 0 {
		return 0, &ConfigError{Option: "slow send", Reason: "positive chunk and nonnegative interval required"}
	}
	if err = s.acquireWrite(ctx); err != nil {
		return 0, err
	}
	defer func() { <-s.writeGate }()
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

// SendNull sends count literal NUL bytes as one non-retried write.
func (s *Session) SendNull(ctx context.Context, count int) (int, error) {
	if count < 0 {
		return 0, &ConfigError{Option: "null count", Reason: "must be nonnegative"}
	}
	return s.Send(ctx, strings.Repeat("\x00", count))
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
	defer func() { <-s.writeGate }()
	if err := breaker.SendBreak(ctx, duration); err != nil {
		return errors.Join(err, s.Close())
	}
	return nil
}
