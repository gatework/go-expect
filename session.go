// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const readChunk = 16 << 10

var errInvalidReadCount = errors.New("expect: invalid read count")

type readEvent struct {
	data []byte
	err  error
}

// Session owns one duplex transport and one matching buffer. The transport must
// support concurrent Read/Write and guarantee Close interrupts both. After a
// successful NewSession, callers must not perform I/O on that transport.
// Expect and Poll are exclusive. Sends are serialized; Close and Redact are
// concurrency safe. SendAll may queue bounded input for the next Expect.
// Do not copy a Session. Callback and output execution is cooperative.
type Session struct {
	transport     io.ReadWriteCloser
	opts          sessionOptions
	mailbox       inputMailbox
	done          chan struct{}
	workers       sync.WaitGroup
	lifeMu        sync.Mutex
	closeOnce     sync.Once
	closeErr      error
	handlerCancel context.CancelFunc
	writeGate     gate
	osIO          *osAccess
	expecting     atomic.Bool
	receive       receiveState
	diagnostics   diagnosticState
}

// receiveState is accessed only by the exclusive wait owner.
type receiveState struct {
	pending, staged           []byte
	pendingAlloc, stagedAlloc int
	stagedErr, terminal       error
}

// diagnosticState serializes borrowed sinks and their independent stream filters.
type diagnosticState struct {
	mu                                          sync.Mutex
	redactor                                    redactor
	sentRedactor, interactRedactor              *redactor
	interactSink                                io.Writer
	secrets                                     []string
	sendOutputErr, interactOutputErr, outputErr error
}

// NewSession transfers transport ownership only on success. Options are
// validated before any reads, writes or closes take place.
func NewSession(transport io.ReadWriteCloser, opts ...SessionOption) (*Session, error) {
	if nilInterface(transport) {
		return nil, &ConfigError{Option: "transport", Reason: "nil transport"}
	}
	o := defaults()
	for _, opt := range opts {
		if opt == nil {
			return nil, &ConfigError{Option: "session", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
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
	s := &Session{transport: transport, opts: o, mailbox: inputMailbox{events: make(chan readEvent, 1), inputGate: make(chan struct{}, 1), inputReady: make(chan struct{}, 1)}, done: make(chan struct{}), writeGate: make(gate, 1), osIO: access}
	if len(o.sendOutputs) > 0 {
		s.diagnostics.sentRedactor = &redactor{}
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
func (s *Session) readLoop() {
	if s.osIO != nil {
		s.osReadLoop()
		return
	}
	defer close(s.mailbox.events)
	empty := 0
	data := make([]byte, readChunk)
	for {
		n, err := s.transport.Read(data)
		if n < 0 || n > len(data) {
			n = 0
			err = errors.Join(errInvalidReadCount, err)
		}
		if n == 0 && err == nil {
			empty++
			if empty < 100 {
				select {
				case <-s.done:
					return
				default:
				}
				continue
			}
			err = io.ErrNoProgress
		} else {
			empty = 0
		}
		// Small terminal reads must not transfer an entire 16 KiB allocation.
		// Copy only their bytes, retaining the scratch block for the next Read.
		// Large reads transfer ownership and receive a fresh block below.
		var received []byte
		if n > 0 && n < readChunk/2 {
			received = make([]byte, n)
			copy(received, data[:n])
		} else if n > 0 {
			received = data[:n]
		}
		if !s.publishRead(readEvent{data: received, err: err}) {
			return
		}
		if err != nil {
			return
		}
		if n >= readChunk/2 {
			data = make([]byte, readChunk)
		}
	}
}
func (s *Session) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Close interrupts I/O and joins owned workers. It never waits for application
// callbacks or closes borrowed output writers. Repeated calls return the same error.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		s.lifeMu.Lock()
		close(s.done)
		if s.handlerCancel != nil {
			s.handlerCancel()
		}
		s.lifeMu.Unlock()
		transportErr := s.transport.Close()
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

// Redact adds nonempty literal secrets before sending or receiving them.
// Registration cannot retract bytes already delivered to output writers.
func (s *Session) Redact(secrets ...string) error {
	for _, secret := range secrets {
		if secret == "" {
			return &ConfigError{Option: "redact", Reason: "empty secret"}
		}
	}
	s.diagnostics.mu.Lock()
	defer s.diagnostics.mu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	for _, secret := range secrets {
		duplicate := false
		for _, old := range s.diagnostics.secrets {
			if old == secret {
				duplicate = true
				break
			}
		}
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
	if err := s.emitInteractiveLocked(data, finish); err != nil {
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
func errorClass(err error) ErrClass {
	switch {
	case err == nil:
		return ClassOK
	case errors.Is(err, errInvalidReadCount):
		return ClassError
	case errors.Is(err, context.Canceled):
		return ClassCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return ClassDeadline
	case errors.Is(err, ErrClosed):
		return ClassClosed
	case errors.Is(err, ErrBusy):
		return ClassBusy
	case errors.Is(err, ErrTimeout):
		return ClassTimeout
	case errors.Is(err, ErrIdleTimeout):
		return ClassIdleTimeout
	case errors.Is(err, io.EOF):
		return ClassEOF
	default:
		return ClassError
	}
}

//nolint:contextcheck // Rejected nil-context calls still emit metadata; they have no parent context.
func (s *Session) notify(ctx context.Context, event Event) {
	if s.opts.structuredLogger != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		s.opts.structuredLogger.LogAttrs(ctx, slog.LevelInfo, "expect", slog.String("op", event.Operation.String()), slog.Int("case", event.CaseIndex), slog.Int("bytes", event.Bytes), slog.String("class", event.Error.String()))
	}

	for _, observer := range s.opts.observers {
		observer(event)
	}
}

// Send writes text once. No partial write is retried. A failure after entering
// transport.Write closes the Session and returns its progress in WriteError.
// An invalid transport count is rejected and reported as zero.
func (s *Session) Send(ctx context.Context, text string) (n int, err error) {
	defer func() { s.notify(ctx, Event{Operation: OpSend, CaseIndex: -1, Bytes: n, Error: errorClass(err)}) }()
	if ctx == nil {
		return 0, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if s.isClosed() {
		return 0, ErrClosed
	}
	deadline, _ := ctx.Deadline()
	if s.opts.writeTimeout > 0 {
		deadline = earlier(deadline, after(time.Now(), s.opts.writeTimeout))
	}
	if err = s.writeGate.acquire(ctx, s.done, deadline); err != nil {
		return 0, err
	}
	defer func() { <-s.writeGate }()
	if len(text) == 0 {
		return 0, nil
	}
	return s.writeOnce(ctx, text, deadline)
}

type writeResult struct {
	n   int
	err error
}

// SendLine appends the configured line ending (LF by default).
func (s *Session) SendLine(ctx context.Context, text string) (int, error) {
	return s.Send(ctx, text+s.opts.lineEnding)
}

// publishRead is called only by the sole pump. The notification can collapse:
// receivers always inspect the event channel under inputGate before waiting.
func (s *Session) publishRead(event readEvent) bool {
	if s.opts.removeNulls || s.opts.stripParity {
		n := 0
		for _, b := range event.data {
			if s.opts.stripParity {
				b &= 0x7f
			}
			if s.opts.removeNulls && b == 0 {
				continue
			}
			event.data[n] = b
			n++
		}
		event.data = event.data[:n]
		if n == 0 && event.err == nil {
			return !s.isClosed()
		}
	}

	select {
	case s.mailbox.events <- event:
		s.mailbox.signalInput()
		return true
	case <-s.done:
		return false
	}
}

// String formats the handle without exposing owned stream or command state.
func (*Session) String() string { return "expect.Session" }

// GoString formats the handle without exposing owned stream or command state.
func (p *Session) GoString() string { return p.String() }
