// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"time"
)

type inputActions struct {
	session  *Session
	waiter   *waiter
	cases    []Case
	counts   []int
	controls chan<- func()
}

func (r *inputActions) invoke(ctx context.Context, index int, text string) error {
	s, w, cases, counts, controls := r.session, r.waiter, r.cases, r.counts, r.controls
	done := make(chan struct{})
	var actionErr error
	var stop bool
	f := func() {
		defer func() { stop = w.stopped; close(done) }()
		r := Result{Source: SourceInput, Scope: ScopeCall, CaseIndex: index, RuleIndex: -1, Match: text, End: len(text)}
		for _, observer := range s.opts.trace {
			observer(TraceEvent{Source: SourceInput, Scope: ScopeCall, CaseIndex: index, RuleIndex: -1, Buffered: len(text), End: len(text), Pattern: cases[index].Pattern.String(), Matched: true})
		}
		if cases[index].Limit > 0 && counts[index] >= cases[index].Limit {
			actionErr = &ResponseError{Source: SourceInput, Scope: ScopeCall, CaseIndex: index, RuleIndex: -1}
			w.stopped = true
			return
		}
		counts[index]++
		w.result = r
		handler := cases[index].Handler
		action := Return
		if handler != nil {
			if w.cancelHandlers == nil {
				w.handlerCtx, w.cancelHandlers = context.WithCancel(ctx)
				s.lifeMu.Lock()
				if s.isClosed() {
					w.cancelHandlers()
				} else {
					s.handlerCancel = w.cancelHandlers
				}
				s.lifeMu.Unlock()
			}
			//nolint:contextcheck // Stored handlerCtx derives from ctx and is canceled by Close.
			action, actionErr = s.callHandler(w.handlerCtx, earlier(w.relative, w.idle), handler, r)
		}
		actionErr = errors.Join(actionErr, budgetError(ctx, w.relative, w.idle))
		if actionErr != nil {
			w.stopped = true
			return
		}
		switch action {
		case Return:
			w.stopped = true
		case Continue:
			w.relative = after(time.Now(), w.o.timeout)
		case ContinueKeepTimer:
		default:
			actionErr = &ConfigError{Option: "handler action", Reason: "unknown action"}
			w.stopped = true
		}
	}
	select {
	case controls <- f:
	case <-ctx.Done():
		return nil
	}
	select {
	case <-done:
		if actionErr == nil && stop {
			return io.EOF
		}
		return actionErr
	case <-ctx.Done():
		return nil
	}
}

// inputTrie shares literal prefixes and records declaration priority.
// Pending input is retained only while a higher-priority declaration can finish.
type inputTrie struct {
	edges           map[byte]*inputTrie
	first, terminal int
}

func compileInputTrie(cases []Case) *inputTrie {
	root := &inputTrie{first: -1, terminal: -1}
	for i, c := range cases {
		node := root
		for j := range len(c.Pattern.text) {
			if node.edges == nil {
				node.edges = make(map[byte]*inputTrie)
			}
			b := c.Pattern.text[j]
			if node.edges[b] == nil {
				node.edges[b] = &inputTrie{first: i, terminal: -1}
			}
			node = node.edges[b]
		}
		if node.terminal < 0 {
			node.terminal = i
		}
	}
	return root
}
func (t *inputTrie) match(data []byte, eof bool) (index int, partial bool) {
	index = -1
	node := t
	for _, b := range data {
		node = node.edges[b]
		if node == nil {
			return index, false
		}
		if node.terminal >= 0 && (index < 0 || node.terminal < index) {
			index = node.terminal
		}
	}
	if !eof && len(node.edges) > 0 && (index < 0 || node.first < index) {
		return -1, true
	}
	return index, false
}
