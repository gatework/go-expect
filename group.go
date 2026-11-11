// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"time"
)

// Group is a mutable set of borrowed Sessions. Only one Expect may run on a
// Group. Membership controls run outside its callbacks and acknowledge input
// ownership changes before returning. Group never closes its member transports.
type Group struct {
	mu       sync.Mutex
	sessions []*Session
	run      *groupRun
	prepare  func(*waiter) error
	failFast bool
}

// NewGroup validates a borrowed member set; it performs no transport I/O.
func NewGroup(sessions ...*Session) (*Group, error) {
	seen := make(map[*Session]bool, len(sessions))
	for _, s := range sessions {
		if s == nil || seen[s] {
			return nil, &ConfigError{Option: "group", Reason: "nil or duplicate session"}
		}
		seen[s] = true
	}
	return &Group{sessions: slices.Clone(sessions)}, nil
}

// SessionError associates a failed member with its immutable identity.
// Formatting omits transport errors; Unwrap explicitly exposes the cause.
type SessionError struct {
	SessionID SessionID
	Cause     error
}

func (e *SessionError) Error() string {
	return "expect: group member failed (" + errorClass(e.Cause).String() + ")"
}
func (e *SessionError) Unwrap() error { return e.Cause }

// GoString omits the underlying transport error.
func (e *SessionError) GoString() string { return e.Error() }

// ErrNoSessions means a Group has no remaining members to receive from.
var ErrNoSessions = errors.New("expect: no sessions")

type groupMember struct {
	w                       waiter
	active, owns, hasCached bool
	cached                  selection
	version                 uint64
}
type groupCommand struct {
	ctx   context.Context //nolint:containedctx // the caller's request context travels with the command to the run goroutine
	apply func(*groupRun) error
	reply chan error
}
type groupRun struct {
	fatal    error
	group    *Group
	ctx      context.Context //nolint:containedctx // a run is a lifecycle object bounded by its Start context
	cases    []Case
	opts     []MatchOption
	members  []*groupMember
	commands chan groupCommand
	done     chan struct{}
	wake     chan struct{}
	failures []error
	next     int
}

// Expect keeps every active member eligible across continuations. Before/call/
// After groups apply across members, with round-robin ties. Case.Sessions binds
// a declaration; an empty selector matches all members. Return completes the wait.
func (g *Group) Expect(ctx context.Context, cases []Case, opts ...MatchOption) (*Session, Result, error) {
	empty := Result{CaseIndex: -1, RuleIndex: -1}
	if err := waitForControl(ctx); err != nil {
		return nil, empty, err
	}
	ctx = context.WithValue(ctx, callbackContextKey{}, true)
	g.mu.Lock()
	if g.run != nil {
		g.mu.Unlock()
		return nil, empty, ErrBusy
	}
	r := &groupRun{group: g, ctx: ctx, cases: cloneCases(cases), opts: slices.Clone(opts), commands: make(chan groupCommand), done: make(chan struct{}), wake: make(chan struct{}, 1)}
	g.run = r
	sessions := slices.Clone(g.sessions)
	g.mu.Unlock()
	defer r.close()
	if len(sessions) == 0 {
		return nil, empty, ErrNoSessions
	}
	for _, s := range sessions {
		if err := r.admit(s); err != nil {
			return nil, empty, err
		}
	}
	return r.wait()
}
func (r *groupRun) admit(s *Session) error {
	if !s.expecting.CompareAndSwap(false, true) {
		return ErrBusy
	}
	m := &groupMember{w: waiter{session: s, result: Result{SessionID: s.id, CaseIndex: -1, RuleIndex: -1}}, active: true, owns: true}
	if err := m.w.prepare(r.ctx, r.cases, r.opts); err != nil {
		s.releaseWait()
		return err
	}
	if r.group.prepare != nil {
		if err := r.group.prepare(&m.w); err != nil {
			m.w.close()
			s.releaseWait()
			return err
		}
	}
	r.members = append(r.members, m)
	s.mailbox.wake.Store(&r.wake)
	return nil
}
func (m *groupMember) release() {
	if !m.owns {
		return
	}
	m.w.session.mailbox.wake.Store(nil)
	m.w.close()
	m.owns = false
	m.w.session.releaseWait()
}
func (r *groupRun) close() {
	for _, m := range r.members {
		m.release()
	}
	r.group.mu.Lock()
	if r.group.run == r {
		r.group.run = nil
	}
	close(r.done)
	r.group.mu.Unlock()
}

// Add installs a member; during Expect its receive reservation is acquired first.
func (g *Group) Add(ctx context.Context, s *Session) error { return g.change(ctx, s, true) }

// Remove releases a member's reservation and preserves its unconsumed input.
func (g *Group) Remove(ctx context.Context, s *Session) error { return g.change(ctx, s, false) }

func (g *Group) change(ctx context.Context, s *Session, add bool) error {
	if err := waitForControl(ctx); err != nil {
		return err
	}
	if s == nil {
		return &ConfigError{Option: "group", Reason: "nil session"}
	}
	g.mu.Lock()
	if g.run == nil {
		err := g.changeMembers(s, add)
		g.mu.Unlock()
		return err
	}
	r := g.run
	g.mu.Unlock()
	c := groupCommand{ctx: ctx, reply: make(chan error, 1)}
	c.apply = func(r *groupRun) error { return g.applyChange(r, s, add) }
	select {
	case r.commands <- c:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrClosed
	}
	return awaitControl(ctx, c.reply, r.done)
}

// applyChange changes membership on the run goroutine of an active Expect.
func (g *Group) applyChange(r *groupRun, s *Session, add bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if add {
		if s.isClosed() {
			return ErrClosed
		}
		if slices.Contains(g.sessions, s) {
			return &ConfigError{Option: "group", Reason: "duplicate session"}
		}
		if err := r.admit(s); err != nil {
			return err
		}
	} else if err := r.remove(s); err != nil {
		return err
	}
	return g.changeMembers(s, add)
}

func (g *Group) changeMembers(s *Session, add bool) error {
	for i, v := range g.sessions {
		if v == s {
			if add {
				return &ConfigError{Option: "group", Reason: "duplicate session"}
			}
			g.sessions = append(g.sessions[:i], g.sessions[i+1:]...)
			return nil
		}
	}
	if !add {
		return &ConfigError{Option: "group", Reason: "unknown session"}
	}
	g.sessions = append(g.sessions, s)
	return nil
}
func (r *groupRun) command(c groupCommand) {
	if err := c.ctx.Err(); err != nil {
		c.reply <- err
		return
	}
	c.reply <- c.apply(r)
}

func (r *groupRun) wait() (*Session, Result, error) {
	empty := Result{CaseIndex: -1, RuleIndex: -1}
	for {
		if err := r.ctx.Err(); err != nil {
			return nil, empty, err
		}
		select {
		case c := <-r.commands:
			r.command(c)
		default:
		}
		chosen, hit, progress, remaining, deadline := r.scan()
		switch {
		case r.fatal != nil:
			return nil, empty, r.fatal
		case chosen >= 0:
			if out, finished := r.dispatch(chosen, hit); finished {
				return out.session, out.result, out.err
			}
		case remaining == 0:
			if len(r.failures) == 0 {
				return nil, empty, ErrNoSessions
			}
			return nil, empty, errors.Join(r.failures...)
		case !progress:
			r.sleep(deadline)
		}
	}
}

// groupOutcome is how a group Expect ended.
type groupOutcome struct {
	session *Session
	result  Result
	err     error
}

// dispatch delivers the chosen member's match; finished reports that the
// group operation ended with out.
func (r *groupRun) dispatch(chosen int, hit selection) (out groupOutcome, finished bool) {
	m := r.members[chosen]
	w := &m.w
	m.hasCached = false
	done, err := w.dispatch(r.ctx, hit)
	if done {
		for _, member := range r.members {
			member.release()
		}
		w.session.notifyExpect(r.ctx, w.result, err)
		return groupOutcome{session: w.session, result: w.result, err: err}, true
	}
	if w.lastReset {
		now := time.Now()
		for _, member := range r.members {
			if member.active {
				member.w.relative = after(now, member.w.o.timeout)
			}
		}
	}
	r.next = (chosen + 1) % len(r.members)
	return groupOutcome{}, false
}

// sleep blocks until a membership command, a wake-up, the deadline or
// cancellation.
func (r *groupRun) sleep(deadline time.Time) {
	var tick <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		tick = timer.C
	}
	select {
	case c := <-r.commands:
		r.command(c)
	case <-r.wake:
	case <-tick:
	case <-r.ctx.Done():
	}
}

// remove releases member s and keeps the round-robin position on the member
// that followed it.
func (r *groupRun) remove(s *Session) error {
	found := slices.IndexFunc(r.members, func(m *groupMember) bool { return m.w.session == s })
	if found < 0 {
		return &ConfigError{Option: "group", Reason: "unknown session"}
	}
	r.members[found].release()
	r.members = append(r.members[:found], r.members[found+1:]...)
	if found < r.next {
		r.next--
	}
	if len(r.members) > 0 {
		r.next %= len(r.members)
	} else {
		r.next = 0
	}
	return nil
}

func (r *groupRun) scan() (chosen int, hit selection, progress bool, remaining int, deadline time.Time) {
	chosen = -1
	for k := range r.members {
		i := (r.next + k) % len(r.members)
		m := r.members[i]
		if !m.active {
			continue
		}
		remaining++
		candidate, advanced, err := r.ready(m)
		progress = progress || advanced
		if err != nil {
			m.active = false
			m.release()
			r.failures = append(r.failures, &SessionError{SessionID: m.w.session.id, Cause: err})
			if r.group.failFast {
				r.fatal = r.failures[len(r.failures)-1]
				return
			}
			progress = true
			continue
		}
		if candidate.chosen >= 0 {
			m.cached = candidate
			m.hasCached = true
			m.version = m.w.session.receive.version
			if chosen < 0 || candidateGroup(&m.w, candidate) < candidateGroup(&r.members[chosen].w, hit) {
				chosen = i
				hit = candidate
			}
		} else {
			deadline = earlier(deadline, earlier(m.w.relative, m.w.idle))
		}
	}
	return
}
func candidateGroup(w *waiter, hit selection) rulePriority {
	if hit.chosen < len(w.cases) {
		return w.origin(hit.chosen).group
	}
	return callPriority
}
func (r *groupRun) ready(m *groupMember) (selection, bool, error) {
	w := &m.w
	s := w.session
	if s.isClosed() {
		return selection{}, false, ErrClosed
	}
	if m.hasCached && m.version == s.receive.version && w.rulesAt == s.ruleState.Load() && budgetError(r.ctx, w.relative, w.idle) == nil {
		return m.cached, false, nil
	}
	m.hasCached = false
	hit, err := w.scan()
	if err != nil || hit.chosen >= 0 {
		return hit, false, err
	}
	advanced := false
	if len(s.receive.staged) == 0 && s.receive.terminal == nil {
		select {
		case s.mailbox.inputGate <- struct{}{}:
		case <-r.ctx.Done():
			return selection{}, false, r.ctx.Err()
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
