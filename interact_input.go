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

// invoke runs input case index on the receive owner and waits for it. It
// returns errInputStopped when the action stops the interaction and
// errInputCanceled when ctx ends before the owner accepts the action.
func (r *inputActions) invoke(ctx context.Context, index int, result Result, deadlineEvent bool) error {
	done := make(chan struct{})
	var actionErr error
	var stop bool
	f := func() {
		w := r.waiter
		defer func() {
			if actionErr != nil {
				w.controlErr = actionErr
			}
			stop = w.stopped
			close(done)
		}()
		actionErr = r.act(ctx, index, result, deadlineEvent)
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

// act runs input case index on the receive owner: it traces the match,
// enforces the case limit, forwards or echoes the match, calls the handler
// and applies its action. Any failure or stop marks the waiter stopped.
func (r *inputActions) act(ctx context.Context, index int, result Result, deadlineEvent bool) error {
	w, s := r.waiter, r.session
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
		w.stopped = true
		return &CaseLimitError{SessionID: s.id, Source: SourceInput, Scope: ScopeCall, CaseIndex: index, RuleIndex: -1}
	}
	r.counts[index]++
	w.result = result
	idle := r.idle
	if deadlineEvent {
		idle = after(time.Now(), r.options.inputTimeout)
	}
	var err error
	if r.options.inputForward && len(result.Match) > 0 {
		_, err = s.Send(ctx, result.Match)
	}
	if err == nil && r.options.echo && len(result.Match) > 0 {
		err = s.interactiveWrite([]byte(result.Match), false)
	}
	var action Action
	if err == nil {
		action, err = w.invokeHandler(ctx, c.Handler, result, w.relative, idle)
	}
	if err != nil {
		w.stopped = true
		return err
	}
	stop, err := w.applyAction(action, false)
	if stop || err != nil {
		w.stopped = true
		return err
	}
	if deadlineEvent && action == Continue {
		r.idle = after(time.Now(), r.options.inputTimeout)
	}
	return nil
}
