// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"os"
)

// InteractOption configures an owned interactive receive operation.
type InteractOption func(*interactOptions) error
type interactOptions struct {
	cases   []Case
	escape  byte
	actions map[byte]Response
	expect  []ExpectOption
}

// WithEscape sets the local exit byte; the default is control-] (0x1d).
func WithEscape(key byte) InteractOption {
	return func(o *interactOptions) error { o.escape = key; return nil }
}

// WithEscapeAction intercepts a local key and performs a timer-preserving action.
func WithEscapeAction(key byte, response Response) InteractOption {
	return func(o *interactOptions) error {
		if response == nil {
			return &ConfigError{Option: "escape action", Reason: "nil response"}
		}
		if o.actions == nil {
			o.actions = make(map[byte]Response)
		}
		o.actions[key] = response
		return nil
	}
}

// WithInteractCases adds remote pattern actions; Return ends interaction.
func WithInteractCases(cases ...Case) InteractOption {
	cases = append([]Case(nil), cases...)
	return func(o *interactOptions) error {
		if err := validateCases(cases); err != nil {
			return err
		}
		o.cases = append(o.cases, cases...)
		return nil
	}
}

// WithInteractExpectOptions configures matching and timing during interaction.
func WithInteractExpectOptions(opts ...ExpectOption) InteractOption {
	opts = append([]ExpectOption(nil), opts...)
	return func(o *interactOptions) error { o.expect = append(o.expect, opts...); return nil }
}

// Interact relays through the sole Session receiver. It owns input on successful
// admission; input.Close must interrupt Read. Output is borrowed and redacted.
// Escape/EOF cancels and joins the input worker. In-flight write cancellation
// retains the ordinary Send retirement contract. Remote match tails are bounded
// by the explicit 64KiB/32KiB streaming window, not arbitrary regexp prefixes.
func (s *Session) Interact(ctx context.Context, input io.ReadCloser, output io.Writer, opts ...InteractOption) (err error) {
	if ctx == nil || nilInterface(input) || nilInterface(output) {
		return &ConfigError{Option: "interact", Reason: "context and streams required"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o := interactOptions{escape: 0x1d}
	for _, opt := range opts {
		if opt == nil {
			return &ConfigError{Option: "interact", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
			return err
		}
	}
	if !s.expecting.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer s.releaseWait()
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.diagnostics.mu.Lock()
	if s.isClosed() {
		s.diagnostics.mu.Unlock()
		return ErrClosed
	}
	s.diagnostics.interactSink = output
	s.diagnostics.interactOutputErr = nil
	s.diagnostics.interactRedactor = &redactor{}
	s.diagnostics.interactRedactor.add(s.diagnostics.secrets)
	initialErr := s.emitInteractiveLocked(s.receive.pending, false)
	s.diagnostics.mu.Unlock()
	if initialErr != nil {
		input.Close()
		s.diagnostics.mu.Lock()
		s.diagnostics.interactSink = nil
		s.diagnostics.interactRedactor = nil
		s.diagnostics.mu.Unlock()
		return errors.Join(initialErr, s.Close())
	}
	localDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096)
		empty := 0
		for {
			n, err := input.Read(buf)
			if n < 0 || n > len(buf) {
				localDone <- errInvalidReadCount
				cancel()
				return
			}
			if n == 0 && err == nil {
				empty++
				if empty >= 100 {
					err = io.ErrNoProgress
				}
			} else {
				empty = 0
			}
			start := 0
			for i := 0; i < n; i++ {
				response, action := o.actions[buf[i]]
				if buf[i] != o.escape && !action {
					continue
				}
				if i > start {
					if _, e := s.Send(ctx, string(buf[start:i])); e != nil {
						localDone <- e
						cancel()
						return
					}
				}
				if !action {
					localDone <- nil
					cancel()
					return
				}
				if e := response(ctx, s, Result{CaseIndex: -1, RuleIndex: -1, Match: string(buf[i : i+1])}); e != nil {
					localDone <- e
					cancel()
					return
				}
				start = i + 1
			}
			if start < n {
				if _, e := s.Send(ctx, string(buf[start:n])); e != nil {
					localDone <- e
					cancel()
					return
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = nil
				}
				localDone <- err
				cancel()
				return
			}
		}
	}()
	cases := append(append([]Case(nil), o.cases...), Case{Pattern: EOF()})
	expectOpts := append([]ExpectOption{WithTimeout(NoTimeout)}, o.expect...)
	expectOpts = append(expectOpts, WithStreamingFallback(func(context.Context, *Session, Result) (Action, error) { return ContinueKeepTimer, nil }))
	defer func() {
		cancel()
		closeErr := input.Close()
		localErr := <-localDone
		s.diagnostics.mu.Lock()
		flushErr := s.emitInteractiveLocked(nil, true)
		s.diagnostics.interactSink = nil
		s.diagnostics.interactRedactor = nil
		s.diagnostics.mu.Unlock()
		if errors.Is(localErr, context.Canceled) || errors.Is(localErr, ErrClosed) || errors.Is(localErr, os.ErrClosed) || errors.Is(localErr, io.ErrClosedPipe) {
			localErr = nil
		}
		if errors.Is(closeErr, io.ErrClosedPipe) || errors.Is(closeErr, os.ErrClosed) {
			closeErr = nil
		}
		err = errors.Join(err, localErr, closeErr, flushErr)
	}()
	_, remoteErr := s.expectOwned(ctx, cases, expectOpts...)
	if ctx.Err() != nil && errors.Is(remoteErr, context.Canceled) {
		remoteErr = nil
	}
	return errors.Join(parent.Err(), remoteErr)
}
