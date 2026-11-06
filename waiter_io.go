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
func (w *waiter) wait(ctx context.Context) error {
	var err error
	deadline := earlier(w.relative, w.idle)
	var wake <-chan time.Time
	if !deadline.IsZero() {
		if w.timer == nil {
			w.timer = time.NewTimer(time.Until(deadline))
		} else {
			w.timer.Reset(time.Until(deadline))
		}
		wake = w.timer.C
	}
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
		select {
		case event, ok := <-w.session.mailbox.events:
			<-w.session.mailbox.inputGate
			if ok {
				if e := w.session.acceptEvent(event); e != nil {
					return errors.Join(e, w.session.Close())
				}
				return nil
			}
		default:
			<-w.session.mailbox.inputGate
			return ErrTimeout
		}
		return nil
	}
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
		if err = w.session.acceptEvent(event); err != nil {
			return errors.Join(err, w.session.Close())
		}
	}
	return nil

}
