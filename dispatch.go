// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"io"
	"time"
)

// dispatch acts on one selected match: it rechecks admission, records the
// result, enforces the case limit, delivers, consumes the match and runs the
// handler. done reports that the operation ended.
func (w *waiter) dispatch(ctx context.Context, hit selection) (done bool, err error) {
	w.lastReset = false
	if w.rulesAt != w.session.ruleState.Load() {
		w.refreshRules()
		return false, nil
	}
	if err := w.admissible(ctx, hit.deadlineEvent); err != nil {
		return w.reject(err)
	}
	index, ruleIndex, scope := w.recordResult(hit)
	if !hit.streaming && w.limit(hit.chosen) > 0 && w.counts[hit.chosen] >= w.limit(hit.chosen) {
		return true, &CaseLimitError{SessionID: w.session.id, CaseIndex: index, RuleIndex: ruleIndex, Scope: scope}
	}
	if w.deliver != nil {
		if err := w.deliver(w.result, hit); err != nil {
			return true, err
		}
	}
	w.consumeMatch(hit)
	return w.runHandler(ctx, hit)
}

// admissible rechecks deadlines, cancellation and closure. Matching may take
// time, so this runs again before input is consumed or a response allowed.
func (w *waiter) admissible(ctx context.Context, deadlineEvent bool) error {
	if err := operationError(ctx, w.relative, w.idle); err != nil && !deadlineEvent {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.session.isClosed() {
		return ErrClosed
	}
	return nil
}

// recordResult fills w.result for hit, traces event and FullBuffer matches,
// and returns the public identity of the matched case.
func (w *waiter) recordResult(hit selection) (index, ruleIndex int, scope RuleScope) {
	chosen, loc := hit.chosen, hit.loc
	index, ruleIndex, scope = -1, -1, ScopeCall
	if !hit.streaming {
		origin := w.origin(chosen)
		index, ruleIndex, scope = origin.index, origin.rule, origin.scope
	}
	pending := w.session.receive.pending
	w.result = Result{SessionID: w.session.id, CaseIndex: index, RuleIndex: ruleIndex, Scope: scope, Start: loc.start, End: loc.end, Before: string(pending[:loc.start]), Match: string(pending[loc.start:loc.end])}
	if hit.deadlineEvent || hit.eof || (!hit.streaming && w.cases[chosen].Pattern.kind == fullBufferPattern) {
		for _, observer := range w.session.opts.trace {
			observer(TraceEvent{SessionID: w.session.id, CaseIndex: index, RuleIndex: ruleIndex, Scope: scope, Buffered: len(pending), Start: loc.start, End: loc.end, Pattern: w.cases[chosen].Pattern.String(), Matched: true})
		}
	}
	w.result.Captures = loc.capturesFromSnapshot(pending, w.result.Match)
	return index, ruleIndex, scope
}

// consumeMatch advances the receive buffer past the match, without copying
// the remaining burst, unless the case keeps its input.
func (w *waiter) consumeMatch(hit selection) {
	receive := &w.session.receive
	if (hit.streaming || !w.cases[hit.chosen].NoConsume) && !hit.deadlineEvent {
		receive.version++
		receive.consumed++
		if w.forwardMatcher != nil {
			w.forwardMatcher.consume(hit.loc.end)
		}
		receive.pending = receive.pending[hit.loc.end:]
	}
	if len(receive.pending) == 0 {
		receive.pending = nil
		receive.pendingAlloc = 0
	}
}

// runHandler calls the selected handler and applies its action. Without a
// handler the match ends the operation with its budget error.
func (w *waiter) runHandler(ctx context.Context, hit selection) (done bool, err error) {
	var handler Handler
	if hit.streaming {
		handler = w.o.streaming
	} else {
		handler = w.cases[hit.chosen].Handler
		if w.counts != nil {
			w.counts[hit.chosen]++
		}
	}
	if handler == nil {
		if hit.deadlineEvent {
			return true, budgetError(ctx, time.Time{}, w.idle)
		}
		return true, budgetError(ctx, w.relative, w.idle)
	}
	effectiveRelative := w.relative
	if hit.deadlineEvent {
		effectiveRelative = after(time.Now(), w.o.timeout)
	}
	action, err := w.invokeHandler(ctx, handler, w.result, effectiveRelative, w.idle)
	if err != nil {
		return true, err
	}
	if !hit.streaming && w.cases[hit.chosen].NoConsume && !hit.deadlineEvent && !hit.eof && action != Return {
		if w.suppressed == nil {
			w.suppressed = make(map[int]uint64)
		}
		w.suppressed[hit.chosen] = w.session.receive.version
	}
	if done, err = w.applyAction(action, hit.streaming); done || err != nil {
		return done, err
	}
	if hit.eof {
		return true, io.EOF // EOF continuations cannot spin forever.
	}
	return false, nil
}
