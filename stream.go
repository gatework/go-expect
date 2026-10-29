package expect

import (
	"context"
	"errors"
	"io"
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
	if s.isClosed() {
		return 0, ErrClosed
	}
	if len(text) == 0 {
		return 0, nil
	}
	s.lifeMu.Lock()
	if s.isClosed() {
		s.lifeMu.Unlock()
		return 0, ErrClosed
	}
	results := make(chan writeResult, 1)
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		data := []byte(text)
		total := 0
		var e error
		for total < len(text) {
			if e = ctx.Err(); e != nil {
				break
			}
			var count int
			if poll {
				count, e = s.osIO.writeNow(data[total:])
			} else {
				count, e = s.transport.Write(data[total:])
			}
			if count < 0 || count > len(text)-total {
				count = 0
				e = errors.Join(errors.New("expect: invalid write count"), e)
			}
			total += count
			if e != nil {
				break
			}
			if count == 0 {
				e = io.ErrNoProgress
				break
			}
		}
		results <- writeResult{total, e}
	}()
	s.lifeMu.Unlock()
	// Drain without competing with an active Expect receive. The input gate also
	// preserves order when a later Expect claims queued input.
	events := s.events
	s.signalInput() // Inspect already published input before relying on future notifications.
	for {
		select {
		case r := <-results:
			n, err = r.n, r.err
			if stop := ctx.Err(); stop != nil {
				err = errors.Join(stop, err)
			} else if s.isClosed() {
				err = errors.Join(ErrClosed, err)
			}
			if err != nil {
				err = errors.Join(err, s.Close())
				return n, &WriteError{Written: n, Cause: err}
			}
			return n, nil
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
				n, err = r.n, r.err
				if stop := ctx.Err(); stop != nil {
					err = errors.Join(stop, err)
				} else if s.isClosed() {
					err = errors.Join(ErrClosed, err)
				}
				if err != nil {
					err = errors.Join(err, s.Close())
					return n, &WriteError{Written: n, Cause: err}
				}
				return n, nil
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
	if len(event.data) == 0 && event.err == nil {
		return nil
	}
	if s.queuedBytes+len(event.data) > s.opts.limit {
		return ErrBufferLimit
	}
	if len(s.queued) == 0 {
		data := event.data
		if cap(data) > s.opts.limit {
			data = make([]byte, len(event.data))
			copy(data, event.data)
		}
		s.queued = []readEvent{{data: data, err: event.err}}
		s.queuedBytes = len(data)
		return nil
	}
	queued := &s.queued[0]
	size := len(queued.data) + len(event.data)
	if size > cap(queued.data) {
		capacity := min(s.opts.limit, max(size, 2*cap(queued.data)))
		data := make([]byte, size, capacity)
		copy(data, queued.data)
		copy(data[len(queued.data):], event.data)
		queued.data = data
	} else {
		queued.data = append(queued.data, event.data...)
	}
	if event.err != nil {
		queued.err = event.err
	}
	s.queuedBytes = size
	return nil
}
