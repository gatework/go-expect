// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
)

// Session owns one duplex transport and one matching buffer. The transport must
// support concurrent Read/Write and guarantee Close interrupts both. After a
// successful NewSession, callers must not perform I/O on that transport.
// Expect and Poll are exclusive. Sends are serialized; Close and Redact are
// concurrency safe. SendAll may queue bounded input for the next Expect.
// Do not copy a Session. Callback and output execution is cooperative.
type Session struct {
	id            SessionID
	ruleState     atomic.Pointer[ruleProgram]
	rulesChanged  chan struct{}
	ruleVersion   uint64
	transport     io.ReadWriteCloser
	opts          sessionOptions
	mailbox       inputMailbox
	done          chan struct{}
	workers       sync.WaitGroup
	lifeMu        sync.Mutex
	interruptOnce sync.Once
	interruptErr  error
	closeOnce     sync.Once
	closeErr      error
	handlerCancel context.CancelFunc
	writeGate     gate
	osIO          *osAccess
	expecting     atomic.Bool
	receive       receiveState
	diagnostics   diagnosticState
}

// NewSession transfers transport ownership only on success. Options are
// validated before any reads, writes or closes take place.
func NewSession(transport io.ReadWriteCloser, opts ...SessionOption) (*Session, error) {
	if nilInterface(transport) {
		return nil, &ConfigError{Option: "transport", Reason: "nil transport"}
	}
	o := defaults()
	if len(opts) > 0 {
		var err error
		o, err = applySessionOptions(o, opts)
		if err != nil {
			return nil, err
		}
	}
	var access *osAccess
	if o.osPolling {
		var err error
		access, err = newOSAccess(transport)
		if err != nil {
			return nil, err
		}
	}
	s := &Session{
		id:           SessionID(nextSessionID.Add(1)),
		rulesChanged: make(chan struct{}, 1),
		transport:    transport,
		opts:         o,
		mailbox: inputMailbox{
			events:     make(chan readEvent, 1),
			inputGate:  make(chan struct{}, 1),
			inputReady: make(chan struct{}, 1),
		},
		done:      make(chan struct{}),
		writeGate: make(gate, 1),
		osIO:      access,
	}
	if len(o.before)+len(o.after) > 0 {
		s.ruleVersion = 1
		s.ruleState.Store(&ruleProgram{RuleSnapshot: RuleSnapshot{Before: o.before, After: o.after}, version: 1})
	}
	if len(o.sendOutputs) > 0 {
		s.diagnostics.sentRedactor = &diagnosticRedactor{}
	}
	s.workers.Go(s.readLoop)
	return s, nil
}

// Interfaces may contain nil pointers (or other nil-capable concrete types).
func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

// Close interrupts I/O and joins owned workers. It never waits for application
// callbacks or closes borrowed output writers. Repeated calls return the same error.
func (s *Session) Close() error {
	transportErr := s.interrupt()
	s.closeOnce.Do(func() {
		s.workers.Wait()
		// Claim idle receive state; an active owner performs its own cleanup on exit.
		if s.expecting.CompareAndSwap(false, true) {
			s.releaseReceiveStorage()
			s.expecting.Store(false)
		}
		s.mailbox.retire()
		outputErr := s.emit(nil, true)
		outputErr = errors.Join(outputErr, s.finishDiagnosticRules())
		s.closeErr = errors.Join(transportErr, outputErr)
	})
	return s.closeErr
}

// String formats the handle without exposing owned stream or command state.
func (*Session) String() string { return "expect.Session" }

// GoString formats the handle without exposing owned stream or command state.
func (s *Session) GoString() string { return s.String() }

func (s *Session) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// interrupt is safe inside an owned worker: it retires the transport without
// joining that worker. Close owns the subsequent join and diagnostic flush.
func (s *Session) interrupt() error {
	s.interruptOnce.Do(func() {
		s.lifeMu.Lock()
		close(s.done)
		s.mailbox.notifyWait()
		if s.handlerCancel != nil {
			s.handlerCancel()
		}
		s.lifeMu.Unlock()
		s.interruptErr = s.transport.Close()
	})
	return s.interruptErr
}
