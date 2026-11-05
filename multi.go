// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Background is an owned asynchronous wait. Continue actions keep watching;
// Return, EOF, cancellation or failure completes it. Stop cancels and joins it.
type Background struct {
	done   chan struct{}
	cancel context.CancelFunc
	result Result
	err    error
}

// StartBackground reserves the receive engine before returning. No competing
// reader is created; an overlapping foreground wait returns ErrBusy.
func (s *Session) StartBackground(ctx context.Context, cases []Case, opts ...ExpectOption) (*Background, error) {
	if ctx == nil {
		return nil, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.expecting.CompareAndSwap(false, true) {
		return nil, ErrBusy
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &Background{done: make(chan struct{}), cancel: cancel}
	cases = append([]Case(nil), cases...)
	opts = append([]ExpectOption(nil), opts...)
	w := waiter{session: s, result: Result{CaseIndex: -1, RuleIndex: -1}}
	if err := w.prepare(ctx, cases, opts); err != nil {
		cancel()
		s.releaseWait()
		return nil, err
	}
	go func() {
		defer close(b.done)
		defer cancel()
		func() {
			defer s.releaseWait()
			defer w.close()
			b.result, b.err = w.run(ctx)
		}()
		s.notifyExpect(ctx, b.result, b.err)
	}()
	return b, nil
}

// Wait joins the background wait and returns its final snapshot.
func (b *Background) Wait() (Result, error) { <-b.done; return b.result, b.err }

// Stop cancels the wait without closing its transport and joins callbacks.
func (b *Background) Stop() { b.cancel(); <-b.done }

// ExpectAny reserves every listed Session, previews matches without callbacks or
// consumption, then dispatches only the first ready winner. Simultaneous matches
// may choose either session. Losing input remains available to subsequent waits.
func ExpectAny(ctx context.Context, sessions []*Session, cases []Case, opts ...ExpectOption) (int, Result, error) {
	empty := Result{CaseIndex: -1, RuleIndex: -1}
	if ctx == nil || len(sessions) == 0 {
		return -1, empty, &ConfigError{Option: "multi wait", Reason: "context and sessions required"}
	}
	if err := ctx.Err(); err != nil {
		return -1, empty, err
	}
	seen := make(map[*Session]bool, len(sessions))
	for _, s := range sessions {
		if s == nil || seen[s] {
			return -1, empty, &ConfigError{Option: "multi wait", Reason: "nil or duplicate session"}
		}
		seen[s] = true
	}
	acquired := 0
	for _, s := range sessions {
		if !s.expecting.CompareAndSwap(false, true) {
			for _, s := range sessions[:acquired] {
				s.releaseWait()
			}
			return -1, empty, ErrBusy
		}
		acquired++
	}
	defer func() {
		for _, s := range sessions[:acquired] {
			s.releaseWait()
		}
	}()
	original := ctx
	previewCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		index int
		idle  time.Time
		err   error
	}
	outcomes := make(chan outcome, len(sessions))
	var workers sync.WaitGroup
	start := time.Now()
	common := append(append([]ExpectOption(nil), opts...), func(o *expectOptions) error { o.started = start; return nil })
	probe := append(append([]ExpectOption(nil), common...), func(o *expectOptions) error { o.preview = true; o.noConsume = true; return nil })
	cases = append([]Case(nil), cases...)
	for i, s := range sessions {
		workers.Go(func() {
			w := waiter{session: s, result: Result{CaseIndex: -1, RuleIndex: -1}}
			defer w.close()
			e := w.prepare(previewCtx, cases, probe)
			if e == nil {
				_, e = w.run(previewCtx)
			}
			outcomes <- outcome{index: i, idle: w.idle, err: e}
		})
	}
	winner := -1
	var winnerIdle time.Time
	var failures []error
	for range sessions {
		out := <-outcomes
		if out.err == nil {
			winner = out.index
			winnerIdle = out.idle
			break
		}
		failures = append(failures, out.err)
	}
	cancel()
	workers.Wait()
	if err := original.Err(); err != nil {
		return -1, empty, err
	}
	if winner < 0 {
		return -1, empty, errors.Join(failures...)
	}
	result, err := func() (Result, error) {
		w := waiter{session: sessions[winner], result: Result{CaseIndex: -1, RuleIndex: -1}}
		if e := w.prepare(original, cases, common); e != nil {
			return w.failure(e)
		}
		defer w.close()
		// Preview acceptance started this idle budget; joining other previews must
		// not grant the selected session a second idle interval.
		w.idle = winnerIdle
		return w.run(original)
	}()
	for _, s := range sessions[:acquired] {
		s.releaseWait()
	}
	acquired = 0
	sessions[winner].notifyExpect(original, result, err)
	return winner, result, err
}

// String formats the handle without exposing owned stream or command state.
func (*Background) String() string { return "expect.Background" }

// GoString formats the handle without exposing owned stream or command state.
func (p *Background) GoString() string { return p.String() }
