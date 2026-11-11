// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
)

// Route forwards unmatched bytes from one borrowed Session. Matched bytes are
// consumed unless ForwardMatches is set. Output is a borrowed, redacted operator
// stream; Targets receive raw protocol bytes. A source occurs in only one route.
// QueueLimit bounds queued plus in-flight bytes per target (default: source limit).
type Route struct {
	Source         *Session
	Targets        []*Session
	Output         io.Writer
	Cases          []Case
	ForwardMatches bool
	QueueLimit     int
}

// RouteError preserves the total accepted progress of a failing target.
// Error formats metadata only; Unwrap explicitly exposes the underlying cause.
type RouteError struct {
	Source, Target SessionID
	Written        int
	Cause          error
}

func (e *RouteError) Error() string {
	return "expect: route failed (" + errorClass(e.Cause).String() + ")"
}
func (e *RouteError) Unwrap() error { return e.Cause }

// GoString omits the underlying transport error.
func (e *RouteError) GoString() string { return e.Error() }

type relayQueue struct {
	source, target *Session
	limit, used    int
	mu             sync.Mutex
	chunks         [][]byte
	closed         bool
	wake, space    chan struct{}
	done           chan struct{}
	err            error
	written        int
}
type relayRoute struct {
	route     Route
	targets   []*relayQueue
	filter    diagnosticRedactor
	outputErr error
}

// Relay connects sources and targets with one receive owner per source. EOF or
// Return ends the relay and drains queued output. Failure cancels all routes,
// joins admitted writes and never replays bytes. Sessions and sinks are borrowed.
func Relay(ctx context.Context, routes []Route, opts ...MatchOption) (err error) {
	if e := waitForControl(ctx); e != nil {
		return e
	}
	if len(routes) == 0 {
		return &ConfigError{Option: "relay", Reason: "empty routes"}
	}
	p, err := newRelayPlan(routes)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, q := range p.queues {
		go q.run(runCtx, cancel)
	}
	completed := false
	defer func() {
		if !completed {
			cancel()
		}
		p.close(&err)
	}()
	g, _ := NewGroup(p.sources...)
	g.failFast = true
	g.prepare = func(w *waiter) error { return p.bySource[w.session].prepare(runCtx, w) }
	options := append([]MatchOption{WithTimeout(NoTimeout)}, opts...)
	options = append(options, WithStreamingFallback(func(context.Context, *Session, Result) (Action, error) { return ContinueKeepTimer, nil }))
	_, _, err = g.Expect(runCtx, p.cases, options...)
	if err != nil {
		cancel()
	} else {
		completed = true
	}
	return err
}

// relayPlan is the validated wiring of a Relay: one route per source, one
// queue per target and the group cases that end each source at EOF.
type relayPlan struct {
	sources  []*Session
	bySource map[*Session]*relayRoute
	cases    []Case
	queues   []*relayQueue
}

func newRelayPlan(routes []Route) (*relayPlan, error) {
	p := &relayPlan{sources: make([]*Session, 0, len(routes)), bySource: make(map[*Session]*relayRoute, len(routes))}
	for _, route := range routes {
		if err := p.add(route); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// add validates route and adds its queues and cases.
func (p *relayPlan) add(route Route) error {
	if route.Source == nil || p.bySource[route.Source] != nil {
		return &ConfigError{Option: "route", Reason: "nil or duplicate source"}
	}
	if route.QueueLimit < 0 {
		return &ConfigError{Option: "route queue", Reason: "negative limit"}
	}
	if route.Output != nil && nilInterface(route.Output) {
		return &ConfigError{Option: "route output", Reason: "nil writer"}
	}
	if e := validateCases(route.Cases); e != nil {
		return e
	}
	r := &relayRoute{route: route}
	p.bySource[route.Source] = r
	p.sources = append(p.sources, route.Source)
	limit := route.QueueLimit
	if limit == 0 {
		limit = route.Source.opts.limit
	}
	seen := make(map[*Session]bool, len(route.Targets))
	for _, target := range route.Targets {
		if target == nil || target == route.Source || seen[target] {
			return &ConfigError{Option: "route target", Reason: "nil, duplicate or self target"}
		}
		seen[target] = true
		q := &relayQueue{source: route.Source, target: target, limit: limit, wake: make(chan struct{}, 1), space: make(chan struct{}, 1), done: make(chan struct{})}
		r.targets = append(r.targets, q)
		p.queues = append(p.queues, q)
	}
	for _, c := range cloneCases(route.Cases) {
		if c.NoConsume {
			return &ConfigError{Option: "route case", Reason: "NoConsume would replay forwarded bytes"}
		}
		c.Sessions = []*Session{route.Source}
		p.cases = append(p.cases, c)
	}
	p.cases = append(p.cases, Case{Pattern: EOF(), Sessions: []*Session{route.Source}})
	return nil
}

// close finishes every queue, joins the target writers and flushes route
// output, joining their failures into *err.
func (p *relayPlan) close(err *error) {
	for _, q := range p.queues {
		q.finish()
	}
	for _, q := range p.queues {
		<-q.done
		*err = errors.Join(*err, q.err)
	}
	for _, r := range p.bySource {
		if e := r.output(nil, true); e != nil {
			*err = errors.Join(*err, e, r.route.Source.Close())
		}
	}
}

// prepare makes w forward this route's unmatched (and, with ForwardMatches,
// matched) source bytes to the targets and output.
func (r *relayRoute) prepare(ctx context.Context, w *waiter) error {
	for _, c := range w.cases {
		if c.NoConsume {
			return &ConfigError{Option: "route rule", Reason: "NoConsume would replay forwarded bytes"}
		}
	}
	if e := w.enableForwarding(); e != nil {
		return e
	}
	w.deliver = func(result Result, hit selection) error {
		if !hit.streaming && w.cases[hit.chosen].NoConsume {
			return &ConfigError{Option: "route rule", Reason: "NoConsume would replay forwarded bytes"}
		}
		if hit.deadlineEvent {
			return nil
		}
		data := result.Before
		if hit.streaming || r.route.ForwardMatches {
			data += result.Match
		}
		return r.forward(ctx, []byte(data))
	}
	return nil
}

func (r *relayRoute) forward(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	for _, q := range r.targets {
		if err := q.enqueue(ctx, data); err != nil {
			return err
		}
	}
	if err := r.output(data, false); err != nil {
		return errors.Join(err, r.route.Source.Close())
	}
	return nil
}
func (r *relayRoute) output(data []byte, finish bool) error {
	if r.outputErr != nil {
		return r.outputErr
	}
	returned := false
	defer func() {
		if !returned {
			if v := recover(); v != nil {
				r.outputErr = &CallbackError{Kind: CallbackPanicked}
				panic(v)
			}
			r.outputErr = &CallbackError{Kind: CallbackExited}
		}
	}()
	if r.route.Output == nil {
		returned = true
		return nil
	}
	s := r.route.Source
	s.diagnostics.mu.Lock()
	r.filter.add(s.diagnostics.secrets)
	s.diagnostics.mu.Unlock()
	filtered := r.filter.append(data, finish)
	if len(filtered) == 0 {
		returned = true
		return nil
	}
	n, err := r.route.Output.Write(filtered)
	if err == nil && n != len(filtered) {
		err = io.ErrShortWrite
	}
	returned = true
	r.outputErr = err
	return err
}
func (q *relayQueue) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
func (q *relayQueue) enqueue(ctx context.Context, data []byte) error {
	for len(data) > 0 {
		n := min(len(data), q.limit)
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return ErrClosed
		}
		if q.used+n <= q.limit {
			q.chunks = append(q.chunks, bytes.Clone(data[:n]))
			q.used += n
			q.mu.Unlock()
			q.signal(q.wake)
			data = data[n:]
			continue
		}
		q.mu.Unlock()
		select {
		case <-q.space:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (q *relayQueue) finish() { q.mu.Lock(); q.closed = true; q.mu.Unlock(); q.signal(q.wake) }
func (q *relayQueue) run(ctx context.Context, cancel context.CancelFunc) {
	returned := false
	defer close(q.done)
	defer func() {
		if !returned {
			kind := CallbackExited
			if recover() != nil {
				kind = CallbackPanicked
			}
			q.err = &RouteError{Source: q.source.id, Target: q.target.id, Written: q.written, Cause: &CallbackError{Kind: kind}}
			cancel()
		}
	}()
	for {
		q.mu.Lock()
		if len(q.chunks) == 0 {
			closed := q.closed
			q.mu.Unlock()
			if closed {
				returned = true
				return
			}
			select {
			case <-q.wake:
				continue
			case <-ctx.Done():
				returned = true
				return
			}
		}
		chunk := q.chunks[0]
		q.chunks[0] = nil
		q.chunks = q.chunks[1:]
		q.mu.Unlock()
		n, err := q.target.SendAll(ctx, string(chunk))
		q.written += n
		q.mu.Lock()
		q.used -= len(chunk)
		q.mu.Unlock()
		q.signal(q.space)
		if err != nil {
			q.err = &RouteError{Source: q.source.id, Target: q.target.id, Written: q.written, Cause: err}
			returned = true
			cancel()
			return
		}
	}
}
