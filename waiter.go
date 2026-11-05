// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"time"
)

// waiter owns all matching state for one exclusive receive operation.
type waiter struct {
	session           *Session
	cases             []Case
	origins           []caseOrigin
	counts, scanned   []int
	o                 expectOptions
	result            Result
	relative, idle    time.Time
	timer             *time.Timer
	cursors           []*matchCursor
	pollAck           chan struct{}
	pollOwned, polled bool
	handlerCtx        context.Context
	cancelHandlers    context.CancelFunc
}
type selection struct {
	chosen                        int
	loc                           location
	streaming, deadlineEvent, eof bool
}

func (w *waiter) failure(err error) (Result, error) {
	return Result{CaseIndex: -1, RuleIndex: -1, Before: string(w.session.receive.pending)}, err
}
func (w *waiter) stopTimer() {
	if w.timer != nil {
		w.timer.Stop()
	}
}
func (w *waiter) close() {
	w.stopTimer()
	if w.cancelHandlers != nil {
		w.cancelHandlers()
		w.session.lifeMu.Lock()
		w.session.handlerCancel = nil
		w.session.lifeMu.Unlock()
	}
}
func (w *waiter) prepare(ctx context.Context, cases []Case, opts []ExpectOption) error {
	var err error
	if ctx == nil {
		return &ConfigError{Option: "context", Reason: "nil context"}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	o := expectOptions{timeout: w.session.opts.timeout}
	for _, opt := range opts {
		if opt == nil {
			return &ConfigError{Option: "expect", Reason: "nil option"}
		}
		if err = opt(&o); err != nil {
			return err
		}
	}
	w.o = o
	w.cases, w.origins = prepareCases(cases, w.session.opts.before, w.o.rules, w.session.opts.after)
	if len(w.cases) == 0 {
		return &ConfigError{Option: "cases", Reason: "empty"}
	}
	if err = validateCases(w.cases); err != nil {
		return err
	}
	for i := range w.cases {
		if w.limit(i) > 0 {
			w.counts = make([]int, len(w.cases))
			break
		}
	}
	w.handlerCtx = ctx
	now := time.Now()
	start := now
	if !w.o.started.IsZero() {
		start = w.o.started
	}
	w.relative = after(start, w.o.timeout)
	w.idle = after(now, w.o.idle)
	w.scanned = make([]int, len(w.cases))
	return nil

}
func (w *waiter) run(ctx context.Context) (Result, error) {
	for {
		if err := ctx.Err(); err != nil {
			return w.failure(err)
		}
		if w.session.isClosed() {
			return w.failure(ErrClosed)
		}
		hit, err := w.scan()
		if err != nil {
			return w.failure(err)
		}
		if hit.chosen >= 0 {
			done, err := w.dispatch(ctx, hit)
			if done {
				return w.result, err
			}
			continue
		}
		if len(w.session.receive.staged) > 0 {
			if err := w.fill(); err != nil {
				return w.failure(err)
			}
			continue
		}
		if err := w.wait(ctx); err != nil {
			return w.failure(err)
		}
	}
}
func (w *waiter) scan() (selection, error) {
	now := time.Now()
	chosen := -1
	var loc location
	deadlineEvent := false
	eof := false
	if !w.relative.IsZero() && !now.Before(w.relative) {
		chosen = eventCase(w.cases, timeoutPattern)
		if chosen < 0 {
			return selection{}, ErrTimeout
		}
		deadlineEvent = true
		loc = location{start: len(w.session.receive.pending), end: len(w.session.receive.pending)}
	}
	if !w.idle.IsZero() && !now.Before(w.idle) {
		return selection{}, ErrIdleTimeout
	}
	if chosen < 0 {
		for i, c := range w.cases {
			from := 0
			if c.Pattern.kind == literalPattern {
				from = max(0, w.scanned[i]-len(c.Pattern.text)+1)
			}
			var hit location
			var ok bool
			var e error
			if c.Pattern.kind == allPattern || c.Pattern.kind == linePattern || (c.Pattern.kind == literalPattern && len(c.Pattern.failure) > 0) {
				if w.cursors == nil {
					w.cursors = make([]*matchCursor, len(w.cases))
				}
				if w.cursors[i] == nil {
					cursor := newMatchCursor(c.Pattern)
					w.cursors[i] = &cursor
				}
				hit, ok, e = w.cursors[i].find(w.session.receive.pending, w.session.receive.terminal != nil, from, true)
			} else {
				hit, ok, e = c.Pattern.find(w.session.receive.pending, w.session.receive.terminal != nil, from)
			}
			if !w.o.preview && len(w.session.opts.trace) > 0 {
				event := TraceEvent{CaseIndex: w.origin(i).index, Buffered: len(w.session.receive.pending), Start: hit.start, End: hit.end, Pattern: c.Pattern.String(), Matched: ok}
				for _, observer := range w.session.opts.trace {
					observer(event)
				}
			}
			w.scanned[i] = len(w.session.receive.pending)
			if e != nil {
				return selection{}, e
			}
			if ok && (chosen < 0 || w.origin(i).group < w.origin(chosen).group || (w.origin(i).group == w.origin(chosen).group && !w.o.declaration && hit.start < loc.start)) {
				chosen = i
				loc = hit
				if w.o.declaration {
					break
				}
			}
		}
	}
	streaming := false
	if chosen < 0 && w.o.streaming != nil && len(w.session.receive.pending) >= streamTriggerBytes {
		chosen = len(w.cases)
		loc = location{start: 0, end: streamConsumeBytes}
		streaming = true
	}
	if chosen < 0 && w.session.receive.terminal != nil {
		if errors.Is(w.session.receive.terminal, io.EOF) && !errors.Is(w.session.receive.terminal, errInvalidReadCount) {
			chosen = eventCase(w.cases, eofPattern)

			if chosen >= 0 {
				loc = location{start: len(w.session.receive.pending), end: len(w.session.receive.pending)}
				eof = true
			}

		}
		if chosen < 0 {
			return selection{}, w.session.receive.terminal
		}
	}
	if chosen < 0 && len(w.session.receive.pending) >= w.session.opts.limit && len(w.session.receive.staged) > 0 {
		chosen = eventCase(w.cases, fullBufferPattern)
		if chosen < 0 {
			return selection{}, ErrBufferLimit
		}
		loc = location{start: len(w.session.receive.pending), end: len(w.session.receive.pending)}
	}
	return selection{chosen, loc, streaming, deadlineEvent, eof}, nil

}
func (w *waiter) dispatch(ctx context.Context, hit selection) (bool, error) {
	chosen, loc, streaming, deadlineEvent, eof := hit.chosen, hit.loc, hit.streaming, hit.deadlineEvent, hit.eof
	// Matching may take time. Recheck deadlines before consuming input or
	// allowing a response, not only before the scan started.
	if err := operationError(ctx, w.relative, w.idle); err != nil && !deadlineEvent {
		return w.reject(err)
	}
	if err := ctx.Err(); err != nil {
		return w.reject(err)
	}
	if w.session.isClosed() {
		return w.reject(ErrClosed)
	}
	index := chosen
	ruleIndex := -1
	scope := ScopeCall
	if !streaming {
		origin := w.origin(chosen)
		index, ruleIndex, scope = origin.index, origin.rule, origin.scope
	}

	if streaming {
		index = -1
	}
	w.result = Result{CaseIndex: index, RuleIndex: ruleIndex, Scope: scope, Start: loc.start, End: loc.end, Before: string(w.session.receive.pending[:loc.start]), Match: string(w.session.receive.pending[loc.start:loc.end])}
	w.result.Captures = loc.capturesFromSnapshot(w.session.receive.pending, w.result.Match)
	if !streaming && w.limit(chosen) > 0 && w.counts[chosen] >= w.limit(chosen) {
		return true, &ResponseError{RuleIndex: ruleIndex}
	}

	// Advance without copying the remaining burst on every continuation.
	if !w.o.noConsume && !deadlineEvent {
		w.session.receive.pending = w.session.receive.pending[loc.end:]
	}
	if len(w.session.receive.pending) == 0 {
		w.session.receive.pending = nil
		w.session.receive.pendingAlloc = 0
	}
	clear(w.scanned)
	for i := range w.cursors {
		if w.cursors[i] != nil {
			w.cursors[i].reset()
		}
	}
	var handler Handler
	if streaming {
		handler = w.o.streaming
	} else {
		handler = w.cases[chosen].Handler
		if w.counts != nil {
			w.counts[chosen]++
		}
	}
	if handler == nil || w.o.preview {
		return true, nil
	}
	if w.cancelHandlers == nil {
		w.handlerCtx, w.cancelHandlers = context.WithCancel(ctx)
		w.session.lifeMu.Lock()
		if w.session.isClosed() {
			w.cancelHandlers()
		} else {
			w.session.handlerCancel = w.cancelHandlers
		}
		w.session.lifeMu.Unlock()
	}
	handlerDeadline := earlier(w.relative, w.idle)
	if deadlineEvent {
		handlerDeadline = earlier(after(time.Now(), w.o.timeout), w.idle)
	}
	//nolint:contextcheck // handlerCtx derives from this request in dispatch and is canceled by Close.
	action, handlerErr := w.session.callHandler(w.handlerCtx, handlerDeadline, handler, w.result)
	if handlerErr != nil {
		if errors.Is(handlerErr, context.DeadlineExceeded) && ctx.Err() == nil {
			if !w.relative.IsZero() && !time.Now().Before(w.relative) {
				handlerErr = errors.Join(handlerErr, ErrTimeout)
			} else if !w.idle.IsZero() && !time.Now().Before(w.idle) {
				handlerErr = errors.Join(handlerErr, ErrIdleTimeout)
			}
		}
		return true, handlerErr
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if w.o.noConsume && action != Return {
		return true, &ConfigError{Option: "no consume", Reason: "handler must Return"}
	}
	switch action {
	case Return:
		if deadlineEvent {
			return true, ctx.Err()
		}
		return true, operationError(ctx, w.relative, w.idle)
	case Continue:
		if !streaming {
			w.relative = after(time.Now(), w.o.timeout)
		}
	case ContinueKeepTimer:
	default:
		return true, &ConfigError{Option: "handler action", Reason: "unknown action"}
	}
	if eof {
		return true, io.EOF
	} // EOF continuations cannot spin forever.

	return false, nil

}
func (w *waiter) fill() error {
	var err error
	// A full buffer is not yet overflow: an independent terminal event
	// may follow. Keep bounded lookahead to distinguish that from more data.
	if len(w.session.receive.pending) >= w.session.opts.limit && len(w.session.receive.staged) > 0 {
		return ErrBufferLimit
	}
	n := w.session.opts.limit - len(w.session.receive.pending)
	if w.o.streaming != nil && len(w.session.receive.pending) < streamTriggerBytes {
		n = min(n, streamTriggerBytes-len(w.session.receive.pending))
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

func (w *waiter) origin(index int) caseOrigin {
	if len(w.origins) == 0 {
		return caseOrigin{index: index, rule: -1, group: 1, scope: ScopeCall}
	}
	return w.origins[index]
}
func (w *waiter) limit(index int) int {
	c := w.cases[index]
	if c.Limit > 0 {
		return c.Limit
	}
	if w.origin(index).rule >= 0 || c.Pattern.kind == timeoutPattern || c.Pattern.kind == defaultPattern || c.Pattern.kind == fullBufferPattern {
		return 1
	}
	return 0
}

func (w *waiter) reject(err error) (bool, error) { w.result, _ = w.failure(err); return true, err }
