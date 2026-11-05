// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
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

// ExpectAny reserves all sessions and keeps watching them across continuations.
// Before/call/After priority applies across sessions; equal groups rotate fairly.
// Return completes the operation; unconsumed input remains on each session.
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

	wake := make(chan struct{}, 1)
	waits := make([]waiter, len(sessions))
	active := make([]bool, len(sessions))
	for i, s := range sessions {
		waits[i] = waiter{session: s, result: empty}
		if err := waits[i].prepare(ctx, cases, opts); err != nil {
			return -1, empty, err
		}
		active[i] = true
	}
	for _, s := range sessions {
		s.mailbox.wake.Store(&wake)
	}
	cleaned := false
	defer func() {
		if cleaned {
			return
		}
		for i, s := range sessions {
			s.mailbox.wake.Store(nil)
			waits[i].close()
		}
	}()
	g := anyCoordinator{sessions: sessions, waits: waits, active: active, cached: make([]selection, len(sessions)), hasCached: make([]bool, len(sessions))}
	for {
		if err := ctx.Err(); err != nil {
			return -1, empty, err
		}
		step, scanErr := g.scan(ctx)
		if scanErr != nil {
			return -1, empty, scanErr
		}
		chosen, hit, progress, remaining, deadline := step.chosen, step.hit, step.progress, step.remaining, step.deadline
		if chosen >= 0 {
			w := &waits[chosen]
			g.hasCached[chosen] = false
			done, e := w.dispatch(ctx, hit)
			if done {
				for j, s := range sessions {
					s.mailbox.wake.Store(nil)
					waits[j].close()
				}
				cleaned = true
				for _, s := range sessions[:acquired] {
					s.releaseWait()
				}
				acquired = 0
				sessions[chosen].notifyExpect(ctx, w.result, e)
				return chosen, w.result, e
			}
			if w.lastReset {
				now := time.Now()
				for i := range waits {
					if active[i] {
						waits[i].relative = after(now, waits[i].o.timeout)
					}
				}
			}
			g.next = (chosen + 1) % len(sessions)
			continue
		}
		if remaining == 0 {
			return -1, empty, errors.Join(g.failures...)
		}
		if progress {
			continue
		}
		var timer *time.Timer
		var tick <-chan time.Time
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			tick = timer.C
		}
		select {
		case <-wake:
		case <-tick:
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return -1, empty, ctx.Err()
		}
		if timer != nil {
			timer.Stop()
		}
	}

}

// String formats the handle without exposing owned stream or command state.
func (*Background) String() string { return "expect.Background" }

// GoString formats the handle without exposing owned stream or command state.
func (p *Background) GoString() string { return p.String() }

type anyCoordinator struct {
	cached    []selection
	hasCached []bool
	sessions  []*Session
	waits     []waiter
	active    []bool
	failures  []error
	next      int
}
type anyStep struct {
	chosen    int
	hit       selection
	progress  bool
	remaining int
	deadline  time.Time
}

func (g *anyCoordinator) scan(ctx context.Context) (anyStep, error) {
	chosen := -1
	var hit selection
	progress := false
	remaining := 0
	var deadline time.Time
	for k := range g.sessions {
		i := (g.next + k) % len(g.sessions)
		if !g.active[i] {
			continue
		}
		remaining++
		w := &g.waits[i]
		candidate, advanced, e := g.ready(ctx, i)
		progress = progress || advanced
		if e != nil {
			g.active[i] = false
			g.failures = append(g.failures, e)
			progress = true
			continue
		}
		if candidate.chosen >= 0 {
			g.cached[i], g.hasCached[i] = candidate, true
			group := 1
			if candidate.chosen < len(w.cases) {
				group = w.origin(candidate.chosen).group
			}
			oldGroup := 4
			if chosen >= 0 {
				oldGroup = 1
				if hit.chosen < len(g.waits[chosen].cases) {
					oldGroup = g.waits[chosen].origin(hit.chosen).group
				}
			}
			if chosen < 0 || group < oldGroup {
				chosen = i
				hit = candidate
			}
			continue
		}
		deadline = earlier(deadline, earlier(w.relative, w.idle))
	}
	return anyStep{chosen, hit, progress, remaining, deadline}, nil
}

// ready admits at most one queued read and fills once before arbitration. This
// makes newly accepted Before rules visible without draining a noisy session.
func (g *anyCoordinator) ready(ctx context.Context, index int) (selection, bool, error) {
	w := &g.waits[index]
	s := w.session
	if s.isClosed() {
		return selection{}, false, ErrClosed
	}
	if g.hasCached[index] && budgetError(ctx, w.relative, w.idle) == nil {
		return g.cached[index], false, nil
	}
	g.hasCached[index] = false
	hit, err := w.scan()
	if err != nil || hit.chosen >= 0 {
		return hit, false, err
	}
	advanced := false
	if len(s.receive.staged) == 0 && s.receive.terminal == nil {
		select {
		case s.mailbox.inputGate <- struct{}{}:
		case <-ctx.Done():
			return selection{}, false, ctx.Err()
		}
		event, ok := s.mailbox.takeQueued()
		if !ok {
			select {
			case event, ok = <-s.mailbox.events:
				if !ok {
					s.receive.terminal = io.EOF
					advanced = true
				}
			default:
			}
		}
		<-s.mailbox.inputGate
		if ok {
			if err = s.acceptEvent(event); err != nil {
				return selection{}, true, errors.Join(err, s.Close())
			}
			advanced = true
		}
	}
	if len(s.receive.staged) > 0 {
		if err = w.fill(); err != nil {
			return selection{}, true, err
		}
		advanced = true
	}
	if advanced {
		hit, err = w.scan()
	}
	return hit, advanced, err
}
