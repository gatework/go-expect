// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"io"
	"time"
)

func (w *waiter) dispatch(ctx context.Context, hit selection) (bool, error) {
	w.lastReset = false
	if w.rulesAt != w.session.ruleState.Load() {
		w.refreshRules()
		return false, nil
	}
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
	w.result = Result{SessionID: w.session.id, CaseIndex: index, RuleIndex: ruleIndex, Scope: scope, Start: loc.start, End: loc.end, Before: string(w.session.receive.pending[:loc.start]), Match: string(w.session.receive.pending[loc.start:loc.end])}
	if deadlineEvent || eof || (!streaming && w.cases[chosen].Pattern.kind == fullBufferPattern) {
		for _, observer := range w.session.opts.trace {
			observer(TraceEvent{SessionID: w.session.id, CaseIndex: index, RuleIndex: ruleIndex, Scope: scope, Buffered: len(w.session.receive.pending), Start: loc.start, End: loc.end, Pattern: w.cases[chosen].Pattern.String(), Matched: true})
		}
	}
	w.result.Captures = loc.capturesFromSnapshot(w.session.receive.pending, w.result.Match)
	if !streaming && w.limit(chosen) > 0 && w.counts[chosen] >= w.limit(chosen) {
		return true, &CaseLimitError{SessionID: w.session.id, CaseIndex: index, RuleIndex: ruleIndex, Scope: scope}
	}

	if w.deliver != nil {
		if err := w.deliver(w.result, hit); err != nil {
			return true, err
		}
	}

	// Advance without copying the remaining burst on every continuation.
	if (streaming || !w.cases[chosen].NoConsume) && !deadlineEvent {
		w.session.receive.version++
		w.session.receive.consumed++
		if w.forwardMatcher != nil {
			w.forwardMatcher.consume(loc.end)
		}
		w.session.receive.pending = w.session.receive.pending[loc.end:]
	}
	if len(w.session.receive.pending) == 0 {
		w.session.receive.pending = nil
		w.session.receive.pendingAlloc = 0
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
	if handler == nil {
		if deadlineEvent {
			return true, budgetError(ctx, time.Time{}, w.idle)
		}
		return true, budgetError(ctx, w.relative, w.idle)
	}
	effectiveRelative := w.relative
	if deadlineEvent {
		effectiveRelative = after(time.Now(), w.o.timeout)
	}
	action, handlerErr := w.invokeHandler(ctx, handler, w.result, effectiveRelative, w.idle)
	if handlerErr != nil {
		return true, handlerErr
	}

	if !streaming && w.cases[chosen].NoConsume && !deadlineEvent && !eof && action != Return {
		if w.suppressed == nil {
			w.suppressed = make(map[int]uint64)
		}
		w.suppressed[chosen] = w.session.receive.version
	}

	done, actionErr := w.applyAction(action, streaming)
	if done || actionErr != nil {
		return done, actionErr
	}

	if eof {
		return true, io.EOF
	} // EOF continuations cannot spin forever.

	return false, nil

}
