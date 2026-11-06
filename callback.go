// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"time"
)

// invokeHandler is shared by remote and local matching. Only the receive owner
// installs the lifecycle cancellation scope or enters user code.
func (w *waiter) invokeHandler(ctx context.Context, handler Handler, r Result, relative, idle time.Time) (action Action, err error) {
	if e := budgetError(ctx, relative, idle); e != nil {
		return Return, e
	}
	if handler == nil {
		return Return, budgetError(ctx, relative, idle)
	}
	if w.cancelHandlers == nil {
		w.handlerCtx, w.cancelHandlers = context.WithCancel(ctx)
		w.session.lifeMu.Lock()
		if w.session.isClosed() {
			w.cancelHandlers()
		} else {
			w.session.handlerCancel = w.cancelHandlers
		}
		w.session.lifeMu.Unlock()
	}
	if w.callbackMode != nil {
		restore, e := w.callbackMode()
		if e != nil {
			return Return, e
		}
		defer func() { err = errors.Join(err, restore()) }()
	}
	//nolint:contextcheck // Stored handlerCtx is derived from ctx above and canceled by Close.
	action, err = w.session.callHandler(w.handlerCtx, earlier(relative, idle), handler, r)
	return action, errors.Join(err, budgetError(ctx, relative, idle))
}
func (w *waiter) applyAction(action Action, streaming bool) (bool, error) {
	w.lastReset = false
	switch action {
	case Return:
		return true, nil
	case Continue:
		if !streaming {
			w.relative = after(time.Now(), w.o.timeout)
			w.lastReset = true
		}
	case ContinueKeepTimer:
	default:
		return true, &ConfigError{Option: "handler action", Reason: "unknown action"}
	}
	return false, nil
}
