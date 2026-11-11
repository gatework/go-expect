// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"time"
)

func (w *waiter) fill() error {
	w.session.receive.version++
	var err error
	// A full buffer is not yet overflow: an independent terminal event
	// may follow. Keep bounded lookahead to distinguish that from more data.
	if len(w.session.receive.pending) >= w.session.opts.limit && len(w.session.receive.staged) > 0 {
		return ErrBufferLimit
	}
	n := w.session.opts.limit - len(w.session.receive.pending)
	if w.o.streaming != nil && len(w.session.receive.pending) < w.streamHigh() {
		n = min(n, w.streamHigh()-len(w.session.receive.pending))
	}
	if n > len(w.session.receive.staged) {
		n = len(w.session.receive.staged)
	}
	data := w.session.receive.staged[:n]
	if len(w.session.receive.pending) == 0 {
		// The pump transfers exclusive ownership. Cap the slice so a later
		// append cannot overwrite bytes still waiting in staged.
		w.session.receive.pending = data[:n:n]
		w.session.receive.pendingAlloc = w.session.receive.stagedAlloc
	} else {
		grow := len(w.session.receive.pending)+n > cap(w.session.receive.pending)
		w.session.receive.pending = append(w.session.receive.pending, data...)
		if grow {
			w.session.receive.pendingAlloc = cap(w.session.receive.pending)
		}
	}
	w.session.receive.staged = w.session.receive.staged[n:]
	if len(w.session.receive.staged) == 0 {
		w.session.receive.staged = nil
		w.session.receive.stagedAlloc = 0
	}
	if w.forwardMatcher != nil {
		w.forwardMatcher.append(data)
	}
	w.idle = after(time.Now(), w.o.idle)
	if err = w.session.emit(data, false); err != nil {
		return errors.Join(err, w.session.Close())
	}
	if len(w.session.receive.staged) == 0 && w.session.receive.stagedErr != nil {
		w.session.receive.terminal = w.session.receive.stagedErr
		w.session.receive.stagedErr = nil
		if err = w.session.emit(nil, true); err != nil {
			return errors.Join(err, w.session.Close())
		}
	}

	return nil

}

// wait blocks for the next receive step: input, a rules change, a control
// function, the deadline, cancellation or closure.
func (w *waiter) wait(ctx context.Context) error {
	wake := w.armTimer()
	select {
	case w.session.mailbox.inputGate <- struct{}{}:
	case <-w.session.rulesChanged:
		return nil
	case f := <-w.control:
		f()
		return nil
	case <-wake:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.session.done:
		return ErrClosed
	}
	return w.receiveGated(ctx, wake)
}

// armTimer arms the reusable timer for the earlier deadline and returns its
// channel, or nil without a deadline.
func (w *waiter) armTimer() <-chan time.Time {
	deadline := earlier(w.relative, w.idle)
	if deadline.IsZero() {
		return nil
	}
	if w.timer == nil {
		w.timer = time.NewTimer(time.Until(deadline))
	} else {
		w.timer.Reset(time.Until(deadline))
	}
	return w.timer.C
}

// receiveGated runs while w holds the input gate; every path releases it.
func (w *waiter) receiveGated(ctx context.Context, wake <-chan time.Time) error {
	if event, ok := w.session.mailbox.takeQueued(); ok {
		<-w.session.mailbox.inputGate
		if e := w.session.acceptEvent(event); e != nil {
			return errors.Join(e, w.session.Close())
		}
		return nil
	}
	if w.o.poll && w.pollAck == nil && !w.polled {
		var e error
		w.pollAck, w.pollOwned, e = w.session.requestPoll()
		if e != nil {
			<-w.session.mailbox.inputGate
			return errors.Join(e, w.session.Close())
		}
	}
	if w.o.poll && w.polled {
		return w.takePolled()
	}
	return w.awaitGated(ctx, wake)
}

// takePolled completes a Poll whose readiness boundary was reached: it accepts
// one published event or reports ErrTimeout. It releases the input gate.
func (w *waiter) takePolled() error {
	select {
	case event, ok := <-w.session.mailbox.events:
		<-w.session.mailbox.inputGate
		if ok {
			if e := w.session.acceptEvent(event); e != nil {
				return errors.Join(e, w.session.Close())
			}
		}
		return nil
	default:
		<-w.session.mailbox.inputGate
		return ErrTimeout
	}
}

// awaitGated waits, holding the input gate, for the next published event or
// any other receive step. It releases the gate.
func (w *waiter) awaitGated(ctx context.Context, wake <-chan time.Time) error {
	select {
	case <-w.session.rulesChanged:
		<-w.session.mailbox.inputGate
		return nil
	case f := <-w.control:
		<-w.session.mailbox.inputGate
		f()
		return nil
	case <-w.pollAck:
		<-w.session.mailbox.inputGate
		// A canceled caller's request stays pump-owned through its ack. A
		// later Poll waits for it, then requests its own readiness boundary.
		w.polled = w.pollOwned
		w.pollAck = nil
		return nil
	case <-ctx.Done():
		<-w.session.mailbox.inputGate
		w.stopTimer()
		return ctx.Err()
	case <-w.session.done:
		<-w.session.mailbox.inputGate
		w.stopTimer()
		return ErrClosed
	case <-wake:
		<-w.session.mailbox.inputGate
		return nil
	case event, ok := <-w.session.mailbox.events:
		<-w.session.mailbox.inputGate
		w.stopTimer()
		if !ok {
			if w.session.receive.terminal == nil {
				w.session.receive.terminal = io.EOF
			}
			return nil
		}
		if err := w.session.acceptEvent(event); err != nil {
			return errors.Join(err, w.session.Close())
		}
		return nil
	}
}
