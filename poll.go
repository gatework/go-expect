// SPDX-License-Identifier: Apache-2.0

package expect

import "context"

// Poll matches buffered bytes and performs one OS readiness pass without waiting
// for future input. No match returns ErrTimeout. A zero Expect timeout instead
// disables that timer and continues waiting. Requires WithOSPolling.
func (s *Session) Poll(ctx context.Context, cases []Case, opts ...ExpectOption) (Result, error) {
	if s.osIO == nil {
		return Result{SessionID: s.id, CaseIndex: -1, RuleIndex: -1}, ErrPollUnsupported
	}
	opts = append(append([]ExpectOption(nil), opts...), func(o *expectOptions) error { o.poll = true; return nil })
	return s.Expect(ctx, cases, opts...)
}

func (s *Session) acceptEvent(event readEvent) error {
	s.receive.staged = event.data
	s.receive.stagedAlloc = cap(event.data)
	if len(s.receive.staged) == 0 {
		s.receive.staged = nil
		s.receive.stagedAlloc = 0
	}
	s.receive.stagedErr = event.err
	if len(event.data) == 0 && event.err != nil {
		s.receive.terminal = event.err
		s.receive.stagedErr = nil
		return s.emit(nil, true)
	}
	return nil
}

// queueEvent coalesces the queued stream into one ownership unit: at most limit
// payload capacity plus one fixed readEvent header, regardless of read sizes.
// The input gate protects both queue mutation and dequeue order. A terminal
// event belongs after all its bytes and therefore remains at the coalesced end.
func (s *Session) queueEvent(event readEvent) error {
	if s.isClosed() {
		return ErrClosed
	}
	err := s.mailbox.enqueue(event, s.opts.limit)
	s.mailbox.notifyWait()
	return err
}
