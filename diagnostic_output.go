// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"io"
	"slices"
	"sync"
)

// diagnosticState serializes borrowed sinks and their independent stream filters.
type diagnosticState struct {
	interactDeferred                            bool
	mu                                          sync.Mutex
	redactor                                    diagnosticRedactor
	sentRedactor, interactRedactor              *diagnosticRedactor
	interactSink                                io.Writer
	secrets                                     []string
	sendOutputErr, interactOutputErr, outputErr error
}

// Redact adds nonempty literal secrets before sending or receiving them.
// Registration cannot retract bytes already delivered to output writers.
func (s *Session) Redact(secrets ...string) error {
	if slices.Contains(secrets, "") {
		return &ConfigError{Option: "redact", Reason: "empty secret"}
	}
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	for _, secret := range secrets {
		duplicate := slices.Contains(s.diagnostics.secrets, secret)
		if !duplicate {
			s.diagnostics.secrets = append(s.diagnostics.secrets, secret)
		}
	}
	if len(s.opts.outputs) > 0 {
		s.diagnostics.redactor.add(secrets)
	}
	if len(s.opts.sendOutputs) > 0 {
		s.diagnostics.sentRedactor.add(secrets)
	}
	if s.diagnostics.interactSink != nil {
		s.diagnostics.interactRedactor.add(secrets)
	}
	return nil
}

func (s *Session) emit(data []byte, finish bool) error {
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	if s.diagnostics.outputErr != nil {
		if finish {
			s.diagnostics.redactor.resetStream()
		}
		return s.diagnostics.outputErr
	}
	if !finish && s.isClosed() {
		return ErrClosed
	}
	if err := s.emitInteractiveAccepted(data, finish); err != nil {
		s.diagnostics.outputErr = err
		return err
	}
	if len(s.opts.outputs) == 0 {
		return nil
	}
	filtered := s.diagnostics.redactor.append(data, finish)
	if len(filtered) == 0 {
		return nil
	}
	for _, out := range s.opts.outputs {
		n, err := out.Write(filtered)
		if err == nil && n != len(filtered) {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.diagnostics.outputErr = err
			return err
		}
	}
	return nil
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
	s.diagnostics.redactor = diagnosticRedactor{}
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
