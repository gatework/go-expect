package expect

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
)

const readChunk = 16 << 10

type readEvent struct {
	data []byte
	err  error
}

// Session owns one duplex transport and one matching buffer. The transport must
// support concurrent Read/Write and guarantee Close interrupts both. After a
// successful NewSession, callers must not perform I/O on that transport.
// Expect is exclusive. Send is serialized; Close and Redact are concurrency safe.
// Do not copy a Session. Callback and output execution is cooperative.
type Session struct {
	transport io.ReadWriteCloser
	opts      sessionOptions
	events    chan readEvent
	done      chan struct{}
	workers   sync.WaitGroup
	lifeMu    sync.Mutex
	closeOnce sync.Once
	closeErr  error
	writeGate chan struct{}
	expecting atomic.Bool
	// Only the active Expect accesses the protocol state below.
	pending, staged     []byte
	stagedErr, terminal error
	diagMu              sync.Mutex
	redactor            redactor
	outputErr           error
}

// NewSession transfers transport ownership only on success. Options are
// validated before any reads, writes or closes take place.
func NewSession(transport io.ReadWriteCloser, opts ...SessionOption) (*Session, error) {
	if nilInterface(transport) {
		return nil, errors.New("expect: nil transport")
	}
	o := defaults()
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("expect: nil session option")
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	s := &Session{transport: transport, opts: o, events: make(chan readEvent, 1), done: make(chan struct{}), writeGate: make(chan struct{}, 1)}
	s.workers.Add(1)
	go s.readLoop()
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
	defer s.workers.Done()
	defer close(s.events)
	empty := 0
	for {
		data := make([]byte, readChunk)
		n, err := s.transport.Read(data)
		if n < 0 || n > len(data) {
			n = 0
			err = errors.New("expect: invalid read count")
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
		select {
		case s.events <- readEvent{data: data[:n], err: err}:
		case <-s.done:
			return
		}
		if err != nil {
			return
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
		s.lifeMu.Unlock()
		transportErr := s.transport.Close()
		s.workers.Wait()
		outputErr := s.emit(nil, true)
		s.closeErr = errors.Join(transportErr, outputErr)
	})
	return s.closeErr
}

// Redact adds nonempty literal secrets before sending or receiving them.
// Registration cannot retract bytes already delivered to output writers.
func (s *Session) Redact(secrets ...string) error {
	for _, secret := range secrets {
		if secret == "" {
			return errors.New("expect: empty secret")
		}
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	s.redactor.add(secrets)
	return nil
}
func (s *Session) emit(data []byte, finish bool) error {
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	if s.outputErr != nil {
		return s.outputErr
	}
	if !finish && s.isClosed() {
		return ErrClosed
	}
	filtered := s.redactor.append(data, finish)
	if len(filtered) == 0 {
		return nil
	}
	for _, out := range s.opts.outputs {
		n, err := out.Write(filtered)
		if err == nil && n != len(filtered) {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.outputErr = err
			return err
		}
	}
	return nil
}
func errorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrClosed):
		return "closed"
	case errors.Is(err, ErrBusy):
		return "busy"
	case errors.Is(err, ErrTimeout):
		return "timeout"
	case errors.Is(err, ErrIdleTimeout):
		return "idle_timeout"
	case errors.Is(err, io.EOF):
		return "eof"
	default:
		return "error"
	}
}
func (s *Session) notify(event Event) {
	if s.opts.logger != nil {
		s.opts.logger.Printf("expect %s case=%d bytes=%d status=%s", event.Operation, event.CaseIndex, event.Bytes, event.Error)
	}
	for _, observer := range s.opts.observers {
		observer(event)
	}
}

// Send writes text once. No partial write is retried. A failure after entering
// transport.Write closes the Session and returns its progress in WriteError.
// An invalid transport count is rejected and reported as zero.
func (s *Session) Send(ctx context.Context, text string) (n int, err error) {
	defer func() { s.notify(Event{Operation: "send", CaseIndex: -1, Bytes: n, Error: errorClass(err)}) }()
	if ctx == nil {
		return 0, errors.New("expect: nil context")
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if s.isClosed() {
		return 0, ErrClosed
	}
	if s.opts.writeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.writeTimeout)
		defer cancel()
	}
	select {
	case s.writeGate <- struct{}{}:
		defer func() { <-s.writeGate }()
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.done:
		return 0, ErrClosed
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	s.lifeMu.Lock()
	if s.isClosed() {
		s.lifeMu.Unlock()
		return 0, ErrClosed
	}
	if len(text) == 0 {
		s.lifeMu.Unlock()
		return 0, nil
	}
	result := make(chan writeResult, 1)
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		n, e := s.transport.Write([]byte(text))
		if n < 0 || n > len(text) {
			n, e = 0, errors.Join(errors.New("expect: invalid write count"), e)
		}
		if e == nil && n != len(text) {
			e = io.ErrShortWrite
		}
		result <- writeResult{n, e}
	}()
	s.lifeMu.Unlock()
	select {
	case resultValue := <-result:
		n, err = resultValue.n, resultValue.err
	case <-ctx.Done():
		s.Close()
		r := <-result
		return r.n, &WriteError{Written: r.n, Cause: errors.Join(ctx.Err(), r.err)}
	case <-s.done:
		r := <-result
		return r.n, &WriteError{Written: r.n, Cause: errors.Join(ErrClosed, r.err)}
	}
	if err != nil {
		s.Close()
		return n, &WriteError{Written: n, Cause: err}
	}
	return n, nil
}

type writeResult struct {
	n   int
	err error
}

// SendLine appends the configured line ending (LF by default).
func (s *Session) SendLine(ctx context.Context, text string) (int, error) {
	return s.Send(ctx, text+s.opts.lineEnding)
}
