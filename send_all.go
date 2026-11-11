// SPDX-License-Identifier: Apache-2.0

package expect

import "context"

// SendAll drains peer input while writing positive-progress unsent suffixes. Its
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
	defer func() {
		s.notify(ctx, Event{SessionID: s.id, RuleIndex: -1, Operation: OpSendAll, CaseIndex: -1, Bytes: n, Class: errorClass(err)})
	}()
	if ctx == nil {
		return 0, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if err = s.acquireWrite(ctx); err != nil {
		return 0, err
	}
	defer s.writeGate.release()
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
	wait := sendAllWait{session: s, ctx: ctx, results: results, events: s.mailbox.events}
	return wait.run()
}

// sendAllWait waits for one admitted SendAll write while moving published
// input into the receive buffer, so that a peer blocked on its own output
// cannot deadlock the write.
type sendAllWait struct {
	session *Session
	ctx     context.Context //nolint:containedctx // scoped to one sendAll call
	results <-chan writeResult
	events  <-chan readEvent
}

func (w *sendAllWait) run() (int, error) {
	s := w.session
	s.mailbox.signalInput() // Inspect already published input before relying on future notifications.
	for {
		select {
		case r := <-w.results:
			return s.finishWrite(w.ctx, r)
		case <-w.ctx.Done():
			return s.abortWrite(w.results, w.ctx.Err())
		case <-s.done:
			return s.abortWrite(w.results, ErrClosed)
		case <-s.mailbox.inputReady:
			if n, done, err := w.onInputReady(); done {
				return n, err
			}
		}
	}
}

// onInputReady reserves the input gate only after a readiness notification,
// queues at most one published event and releases the gate; it never holds
// the gate while waiting for future input. done reports that the write
// finished or was aborted instead.
func (w *sendAllWait) onInputReady() (n int, done bool, err error) {
	s := w.session
	select {
	case s.mailbox.inputGate <- struct{}{}:
		err = w.queueOne()
		<-s.mailbox.inputGate
		if err != nil {
			n, err = s.abortWrite(w.results, err)
			return n, true, err
		}
		return 0, false, nil
	case r := <-w.results:
		n, err = s.finishWrite(w.ctx, r)
	case <-w.ctx.Done():
		n, err = s.abortWrite(w.results, w.ctx.Err())
	case <-s.done:
		n, err = s.abortWrite(w.results, ErrClosed)
	}
	return n, true, err
}

// queueOne moves one already published read event, if any, into the receive
// buffer. Existing queued bytes stay visible.
func (w *sendAllWait) queueOne() error {
	select {
	case event, ok := <-w.events:
		if !ok {
			w.events = nil
			return nil
		}
		return w.session.queueEvent(event)
	default:
		return nil
	}
}
