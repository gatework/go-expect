package expect

import (
	"context"
	"errors"
)

// Poll matches buffered bytes and performs one OS readiness pass without waiting
// for future input. No match returns ErrTimeout. A zero Expect timeout instead
// disables that timer and continues waiting. Requires WithOSPolling.
func (s *Session) Poll(ctx context.Context, cases []Case, opts ...ExpectOption) (Result, error) {
	if s.osIO == nil {
		return Result{CaseIndex: -1}, ErrPollUnsupported
	}
	opts = append(append([]ExpectOption(nil), opts...), func(o *expectOptions) error { o.poll = true; return nil })
	return s.Expect(ctx, cases, opts...)
}

func (s *Session) acceptEvent(event readEvent) error {
	s.staged = event.data
	s.stagedAlloc = cap(event.data)
	if len(s.staged) == 0 {
		s.staged = nil
		s.stagedAlloc = 0
	}
	s.stagedErr = event.err
	if len(event.data) == 0 && event.err != nil {
		s.terminal = event.err
		s.stagedErr = nil
		return s.emit(nil, true)
	}
	return nil
}

// SendAll writes only the unsent suffix after positive partial writes. Its
// context owns the complete deadline, including serialization; WithWriteTimeout
// applies only to Send. Receive data is queued up to WithBufferLimit while the
// peer writes before reading. Exceeding that bound fails instead of deadlocking.
// Every error after transport entry closes the Session and reports total progress.
func (s *Session) SendAll(ctx context.Context, text string) (int, error) {
	return s.sendAll(ctx, text, false)
}

// SendAllPoll uses immediate OS write readiness for every suffix, permitting an
// initial write without waiting. A blocked suffix returns ErrTimeout with total
// progress and closes the Session. Requires WithOSPolling.
func (s *Session) SendAllPoll(ctx context.Context, text string) (int, error) {
	if s.osIO == nil {
		return 0, ErrPollUnsupported
	}
	return s.sendAll(ctx, text, true)
}
func (s *Session) sendAll(ctx context.Context, text string, poll bool) (n int, err error) {
	defer func() { s.notify(Event{Operation: "send_all", CaseIndex: -1, Bytes: n, Error: errorClass(err)}) }()
	if ctx == nil {
		return 0, errors.New("expect: nil context")
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if err = s.acquireWrite(ctx); err != nil {
		return 0, err
	}
	defer func() { <-s.writeGate }()
	if len(text) == 0 {
		return 0, nil
	}
	mode := writeAll
	if poll {
		mode = writeAllReady
	}
	results, err := s.startWrite(ctx, text, mode)
	if err != nil {
		return 0, err
	}
	// Drain without competing with an active Expect receive. The input gate also
	// preserves order when a later Expect claims queued input.
	events := s.events
	s.signalInput() // Inspect already published input before relying on future notifications.
	for {
		select {
		case r := <-results:
			return s.finishWrite(ctx, r)
		case <-ctx.Done():
			cleanup := s.Close()
			r := <-results
			return r.n, &WriteError{Written: r.n, Cause: errors.Join(ctx.Err(), r.err, cleanup)}
		case <-s.done:
			r := <-results
			return r.n, &WriteError{Written: r.n, Cause: errors.Join(ErrClosed, r.err, s.Close())}
		case <-s.inputReady:
			// Reserve ownership only after a readiness notification, and never hold
			// it while waiting for future input. Existing queued bytes stay visible.
			select {
			case s.inputGate <- struct{}{}:
				select {
				case event, ok := <-events:
					if !ok {
						events = nil
					}
					if ok {
						if e := s.queueEvent(event); e != nil {
							<-s.inputGate
							cleanup := s.Close()
							r := <-results
							return r.n, &WriteError{Written: r.n, Cause: errors.Join(e, r.err, cleanup)}
						}
					}
				default:
				}
				<-s.inputGate
			case r := <-results:
				return s.finishWrite(ctx, r)
			case <-ctx.Done():
				cleanup := s.Close()
				r := <-results
				return r.n, &WriteError{Written: r.n, Cause: errors.Join(ctx.Err(), r.err, cleanup)}
			case <-s.done:
				r := <-results
				return r.n, &WriteError{Written: r.n, Cause: errors.Join(ErrClosed, r.err, s.Close())}
			}
		}
	}
}

// queueEvent coalesces the queued stream into one ownership unit: at most limit
// payload capacity plus one fixed readEvent header, regardless of read sizes.
// The input gate protects both queue mutation and dequeue order. A terminal
// event belongs after all its bytes and therefore remains at the coalesced end.
func (s *Session) queueEvent(event readEvent) error {
	if s.isClosed() {
		return ErrClosed
	}
	return s.inputMailbox.enqueue(event, s.opts.limit)
}
