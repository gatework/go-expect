// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"io"
)

// TraceEvent describes a candidate evaluation without retaining payloads or causes.
type TraceEvent struct {
	SessionID                       SessionID
	Source                          MatchSource
	Scope                           RuleScope
	RuleIndex                       int
	CaseIndex, Buffered, Start, End int
	Pattern                         string
	Matched                         bool
}

// TraceObserver runs synchronously outside locks; it must not reenter Session.
type TraceObserver func(TraceEvent)

// WithTrace enables payload-free candidate diagnostics, independently of completion observers.
func WithTrace(observers ...TraceObserver) SessionOption {
	observers = append([]TraceObserver(nil), observers...)
	return func(o *sessionOptions) error {
		for _, f := range observers {
			if f == nil {
				return &ConfigError{Option: "trace", Reason: "nil observer"}
			}
		}
		o.trace = append(o.trace, observers...)
		return nil
	}
}

// WithSendOutput records actual transport-accepted bytes through a separate
// streaming secret filter. Writers are borrowed, synchronous and never retried.
func WithSendOutput(writers ...io.Writer) SessionOption {
	writers = append([]io.Writer(nil), writers...)
	return func(o *sessionOptions) error {
		for _, w := range writers {
			if nilInterface(w) {
				return &ConfigError{Option: "send output", Reason: "nil writer"}
			}
		}
		o.sendOutputs = append(o.sendOutputs, writers...)
		return nil
	}
}
func (s *Session) emitSent(data []byte, finish bool) error {
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	if s.diagnostics.sendOutputErr != nil {
		return s.diagnostics.sendOutputErr
	}
	if len(s.opts.sendOutputs) == 0 {
		return nil
	}
	completed := false
	defer func() {
		if !completed {
			if v := recover(); v != nil {
				s.diagnostics.sendOutputErr = &CallbackError{Kind: CallbackPanicked}
				panic(v)
			}
			s.diagnostics.sendOutputErr = &CallbackError{Kind: CallbackExited}
		}
	}()
	filtered := s.diagnostics.sentRedactor.append(data, finish)
	if len(filtered) == 0 {
		completed = true
		return nil
	}
	for _, out := range s.opts.sendOutputs {
		n, err := out.Write(filtered)
		if err == nil && n != len(filtered) {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.diagnostics.sendOutputErr = err
			completed = true
			return err
		}
	}
	completed = true
	return nil
}
func (s *Session) emitInteractiveLocked(data []byte, finish bool) error {
	if s.diagnostics.interactOutputErr != nil {
		return s.diagnostics.interactOutputErr
	}
	if s.diagnostics.interactSink == nil {
		return nil
	}
	filtered := s.diagnostics.interactRedactor.append(data, finish)
	if len(filtered) == 0 {
		return nil
	}
	n, err := s.diagnostics.interactSink.Write(filtered)
	if err == nil && n != len(filtered) {
		err = io.ErrShortWrite
	}
	s.diagnostics.interactOutputErr = err
	return err
}
func (s *Session) finishDiagnosticRules() error {
	err := s.emitSent(nil, true)
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	err = errors.Join(err, s.emitInteractiveLocked(nil, true))
	s.diagnostics.redactor = redactor{}
	s.diagnostics.sentRedactor = nil
	s.diagnostics.interactRedactor = nil
	s.diagnostics.interactSink = nil
	s.diagnostics.secrets = nil
	return err
}

// emitInteractiveAccepted runs with diagnostics.mu held. Filtering interactions
// deliver selected safe bytes from dispatch instead of tapping accepted input.
func (s *Session) emitInteractiveAccepted(data []byte, finish bool) error {
	if s.diagnostics.interactDeferred {
		return nil
	}
	return s.emitInteractiveLocked(data, finish)
}
