// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"time"
)

// Background owns an asynchronous receive operation. Pause releases its receive
// reservation after the current callback. Controls must be called outside callbacks.
// Return, EOF, an error or cancellation completes it; Continue keeps listening.
type Background struct {
	sessionID SessionID
	done      chan struct{}
	cancel    context.CancelFunc
	controls  chan func()
	state     *backgroundState
	result    Result
	err       error
}

// StartBackground reserves the receiver synchronously. Relative timing defaults
// to NoTimeout; explicit options override it. Callback panic/Goexit are reported
// by Wait as CallbackError without retaining the panic value.
func (s *Session) StartBackground(ctx context.Context, cases []Case, opts ...MatchOption) (*Background, error) {
	if ctx == nil {
		return nil, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.expecting.CompareAndSwap(false, true) {
		return nil, ErrBusy
	}
	ctx, cancel := context.WithCancel(context.WithValue(ctx, callbackContextKey{}, true))
	b := &Background{sessionID: s.id, done: make(chan struct{}), cancel: cancel, controls: make(chan func()), result: Result{SessionID: s.id, CaseIndex: -1, RuleIndex: -1}}
	w := waiter{session: s, result: b.result, control: b.controls}
	options := append([]MatchOption{WithTimeout(NoTimeout)}, opts...)
	if err := w.prepare(ctx, cloneCases(cases), options); err != nil {
		cancel()
		s.releaseWait()
		return nil, err
	}
	b.state = &backgroundState{b: b, w: &w, owns: true}
	w.park = b.state.park
	w.snapshotAllowed = func() bool { return b.state.owns }
	go b.state.run(ctx)
	return b, nil
}

type backgroundState struct {
	b            *Background
	w            *waiter
	owns, paused bool
	pausedAt     time.Time
}

func (r *backgroundState) run(ctx context.Context) {
	b, w := r.b, r.w
	returned := false
	defer close(b.done)
	defer func() {
		if !returned {
			kind := CallbackExited
			if recover() != nil {
				kind = CallbackPanicked
			}
			b.result = w.result
			b.err = &CallbackError{Kind: kind}
		}
		w.close()
		if r.owns {
			r.owns = false
			w.session.releaseWait()
		}
		b.cancel()
		notified := false
		defer func() {
			if !notified {
				kind := CallbackExited
				if recover() != nil {
					kind = CallbackPanicked
				}
				b.err = &CallbackError{Kind: kind}
			}
		}()
		w.session.notifyExpect(ctx, b.result, b.err)
		notified = true
	}()
	b.result, b.err = w.run(ctx)
	returned = true
}

func (r *backgroundState) park(ctx context.Context) error {
	for r.paused {
		select {
		case f := <-r.b.controls:
			f()
		case <-ctx.Done():
			return ctx.Err()
		case <-r.w.session.done:
			return ErrClosed
		}
	}
	return nil
}

// Pause waits for the current callback, freezes local budgets and releases input.
// Parent context deadlines continue to run while paused. Repeated Pause is harmless.
func (b *Background) Pause(ctx context.Context) error {
	return b.request(ctx, func() error {
		r := b.state
		if r.paused {
			return nil
		}
		r.w.close()
		r.owns = false
		r.paused = true
		r.pausedAt = time.Now()
		r.w.session.releaseWait()
		return nil
	})
}

// Resume reacquires input or returns ErrBusy, leaving the background paused.
func (b *Background) Resume(ctx context.Context) error {
	return b.request(ctx, func() error {
		r := b.state
		if !r.paused {
			return nil
		}
		if r.w.session.isClosed() {
			return ErrClosed
		}
		if !r.w.session.expecting.CompareAndSwap(false, true) {
			return ErrBusy
		}
		elapsed := time.Since(r.pausedAt)
		if !r.w.relative.IsZero() {
			r.w.relative = r.w.relative.Add(elapsed)
		}
		if !r.w.idle.IsZero() {
			r.w.idle = r.w.idle.Add(elapsed)
		}
		r.owns = true
		r.paused = false
		// Scan progress checks the consume generation; a foreground owner that
		// consumed input while paused invalidates it without a reset here.
		r.w.suppressed = nil
		return nil
	})
}

// Wait joins the operation. Canceling ctx bounds the join, not the operation.
func (b *Background) Wait(ctx context.Context) (Result, error) {
	if err := validateControlContext(ctx); err != nil {
		return Result{SessionID: b.sessionID, CaseIndex: -1, RuleIndex: -1}, err
	}
	select {
	case <-b.done:
		return b.result, b.err
	default:
	}
	select {
	case <-b.done:
		return b.result, b.err
	case <-ctx.Done():
		return Result{SessionID: b.sessionID, CaseIndex: -1, RuleIndex: -1}, ctx.Err()
	}
}

// Stop cancels the operation and waits for cooperative callbacks and cleanup.
func (b *Background) Stop(ctx context.Context) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	b.cancel()
	select {
	case <-b.done:
		return nil
	default:
	}
	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Background) request(ctx context.Context, f func() error) error {
	if err := waitForControl(ctx); err != nil {
		return err
	}
	reply := make(chan error, 1)
	command := func() {
		if err := ctx.Err(); err != nil {
			reply <- err
			return
		}
		reply <- f()
	}
	select {
	case b.controls <- command:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return ErrClosed
	}
	return awaitControl(ctx, reply, b.done)
}
