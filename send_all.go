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
		s.notify(ctx, Event{RuleIndex: -1, Operation: OpSendAll, CaseIndex: -1, Bytes: n, Error: errorClass(err)})
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
	events := s.mailbox.events
	s.mailbox.signalInput() // Inspect already published input before relying on future notifications.
	for {
		select {
		case r := <-results:
			return s.finishWrite(ctx, r)
		case <-ctx.Done():
			return s.abortWrite(results, ctx.Err())
		case <-s.done:
			return s.abortWrite(results, ErrClosed)
		case <-s.mailbox.inputReady:
			// Reserve ownership only after a readiness notification, and never hold
			// it while waiting for future input. Existing queued bytes stay visible.
			select {
			case s.mailbox.inputGate <- struct{}{}:
				select {
				case event, ok := <-events:
					if !ok {
						events = nil
					}
					if ok {
						if e := s.queueEvent(event); e != nil {
							<-s.mailbox.inputGate
							return s.abortWrite(results, e)
						}
					}
				default:
				}
				<-s.mailbox.inputGate
			case r := <-results:
				return s.finishWrite(ctx, r)
			case <-ctx.Done():
				return s.abortWrite(results, ctx.Err())
			case <-s.done:
				return s.abortWrite(results, ErrClosed)
			}
		}
	}
}
