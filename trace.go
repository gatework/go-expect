// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"io"
)

// TraceEvent describes a candidate evaluation without retaining payloads or causes.
type TraceEvent struct {
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
	filtered := s.diagnostics.sentRedactor.append(data, finish)
	if len(filtered) == 0 {
		return nil
	}
	for _, out := range s.opts.sendOutputs {
		n, err := out.Write(filtered)
		if err == nil && n != len(filtered) {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.diagnostics.sendOutputErr = err
			return err
		}
	}
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
