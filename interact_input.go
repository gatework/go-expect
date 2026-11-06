// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"time"
)

var errInputStopped = errors.New("expect: local input stopped")
var errInputCanceled = errors.New("expect: local input canceled")

type inputActions struct {
	session  *Session
	waiter   *waiter
	cases    []Case
	counts   []int
	controls chan<- func()
	options  interactOptions
	idle     time.Time
}

func (r *inputActions) invoke(ctx context.Context, index int, result Result, deadlineEvent bool) error {
	done := make(chan struct{})
	var actionErr error
	var stop bool
	f := func() {
		w, s := r.waiter, r.session
		defer func() {
			if actionErr != nil {
				w.controlErr = actionErr
			}
			stop = w.stopped
			close(done)
		}()
		result.SessionID = s.id
		result.Source = SourceInput
		result.Scope = ScopeCall
		result.CaseIndex = index
		result.RuleIndex = -1
		c := r.cases[index]
		for _, observer := range s.opts.trace {
			observer(TraceEvent{SessionID: s.id, Source: SourceInput, Scope: ScopeCall, CaseIndex: index, RuleIndex: -1, Buffered: len(result.Before) + len(result.Match), Start: result.Start, End: result.End, Pattern: c.Pattern.String(), Matched: true})
		}
		limit := c.Limit
		if limit == 0 && (c.Pattern.kind == timeoutPattern || c.Pattern.kind == defaultPattern || c.Pattern.kind == eofPattern || c.Pattern.kind == fullBufferPattern) {
			limit = 1
		}
		if limit > 0 && r.counts[index] >= limit {
			actionErr = &CaseLimitError{SessionID: s.id, Source: SourceInput, Scope: ScopeCall, CaseIndex: index, RuleIndex: -1}
			w.stopped = true
			return
		}
		r.counts[index]++
		w.result = result
		idle := r.idle
		if deadlineEvent {
			idle = after(time.Now(), r.options.inputTimeout)
		}
		if r.options.inputForward && len(result.Match) > 0 {
			_, actionErr = s.Send(ctx, result.Match)
		}
		if actionErr == nil && r.options.echo && len(result.Match) > 0 {
			actionErr = s.interactiveWrite([]byte(result.Match), false)
		}
		var action Action
		if actionErr == nil {
			action, actionErr = w.invokeHandler(ctx, c.Handler, result, w.relative, idle)
		}
		if actionErr != nil {
			w.stopped = true
			return
		}
		stop, actionErr = w.applyAction(action, false)
		if stop || actionErr != nil {
			w.stopped = true
			return
		}
		if deadlineEvent && action == Continue {
			r.idle = after(time.Now(), r.options.inputTimeout)
		}
	}
	select {
	case r.controls <- f:
	case <-ctx.Done():
		return errInputCanceled
	}
	// The owner closes done even when a callback panics or calls Goexit.
	select {
	case <-done:
		if actionErr == nil && stop {
			return errInputStopped
		}
		return actionErr
	case <-ctx.Done():
		select {
		case <-done:
			if actionErr != nil {
				return actionErr
			}
		default:
		}
		return errInputCanceled
	}
}
