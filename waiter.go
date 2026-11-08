// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"time"
)

// waiter owns all matching state for one exclusive receive operation.
type waiter struct {
	snapshotAllowed   func() bool
	forwardMatcher    *inputMatcher
	callbackMode      func() (func() error, error)
	deliver           func(Result, selection) error
	foreground        []Case
	rulesAt           *ruleProgram
	suppressed        map[int]uint64
	session           *Session
	cases             []Case
	origins           []caseOrigin
	counts            []int
	scanned           []scanProgress
	smallScans        [8]scanProgress
	lastReset         bool
	control           <-chan func()
	park              func(context.Context) error
	stopped           bool
	controlErr        error
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
	if w.snapshotAllowed != nil && !w.snapshotAllowed() {
		return Result{SessionID: w.session.id, CaseIndex: -1, RuleIndex: -1}, err
	}
	return Result{SessionID: w.session.id, CaseIndex: -1, RuleIndex: -1, Before: string(w.session.receive.pending)}, err
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
		w.cancelHandlers = nil
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
	if len(opts) > 0 {
		var e error
		o, e = applyExpectOptions(o, opts)
		if e != nil {
			return e
		}
	}

	w.o = o
	w.foreground = cases
	w.rulesAt = w.session.ruleState.Load()
	var beforeRules, afterRules []Case
	if w.rulesAt != nil {
		beforeRules, afterRules = w.rulesAt.Before, w.rulesAt.After
	}
	w.cases, w.origins = prepareCases(cases, beforeRules, w.o.rules, afterRules)
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
	w.relative = after(now, w.o.timeout)
	w.idle = after(now, w.o.idle)
	if len(w.cases) > 8 {
		w.scanned = make([]scanProgress, len(w.cases)-8)
	}
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
		select {
		case <-w.session.rulesChanged:
		case f := <-w.control:
			f()
		default:
		}
		if w.park != nil {
			if err := w.park(ctx); err != nil {
				return w.failure(err)
			}
		}
		if w.stopped {
			return w.result, w.controlErr
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
func (w *waiter) origin(index int) caseOrigin {
	if len(w.origins) == 0 {
		return caseOrigin{index: index, rule: -1, group: callPriority, scope: ScopeCall}
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

func applyExpectOptions(o expectOptions, opts []ExpectOption) (expectOptions, error) {
	for _, opt := range opts {
		if opt == nil {
			return o, &ConfigError{Option: "expect", Reason: "nil option"}
		}
		if e := opt(&o); e != nil {
			return o, e
		}
	}
	return o, nil
}
func (w *waiter) scanOffset(i int) scanProgress {
	if i < 8 {
		return w.smallScans[i]
	}
	return w.scanned[i-8]
}
func (w *waiter) setScanOffset(i int, n scanProgress) {
	if i < 8 {
		w.smallScans[i] = n
	} else {
		w.scanned[i-8] = n
	}
}
func (w *waiter) streamHigh() int { return min(streamTriggerBytes, w.session.opts.limit) }
func (w *waiter) streamLow() int  { return max(1, w.streamHigh()/2) }

func budgetError(ctx context.Context, relative, idle time.Time) error {
	return operationError(ctx, relative, idle)
}

func (w *waiter) enableForwarding() error {
	var cases []Case
	for _, c := range w.cases {
		if bindSession(c, w.session) && !c.NoConsume {
			cases = append(cases, c)
		}
	}
	matcher, err := newInputMatcher(cases)
	if err != nil {
		return err
	}
	matcher.append(w.session.receive.pending)
	w.forwardMatcher = matcher
	return nil
}
