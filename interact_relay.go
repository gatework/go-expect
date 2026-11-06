// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// interactionInput makes input closure single-owner even during panic cleanup.
type interactionInput struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (i *interactionInput) Close() error {
	i.once.Do(func() { i.err = i.ReadCloser.Close() })
	return i.err
}

// relayInput performs input I/O while matching/actions remain serialized with
// the remote receive owner. Closing input interrupts the sole local read pump.
func relayInput(ctx context.Context, s *Session, input io.ReadCloser, o interactOptions, controls chan<- func(), w *waiter) error {
	matcher := o.matcher
	var err error
	readCtx, cancel := context.WithCancel(ctx)
	events := make(chan readEvent, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(events)
		empty := 0
		buf := make([]byte, 4096)
		for {
			n, e := input.Read(buf)
			if n < 0 || n > len(buf) {
				n = 0
				e = errors.Join(errInvalidReadCount, e)
			}
			if n == 0 && e == nil {
				empty++
				if empty >= 100 {
					e = io.ErrNoProgress
				}
			} else {
				empty = 0
			}
			if n > 0 || e != nil {
				var data []byte
				if n >= len(buf)/2 {
					data = buf[:n]
					buf = make([]byte, 4096)
				} else {
					data = append([]byte(nil), buf[:n]...)
				}
				select {
				case events <- readEvent{data: data, err: e}:
				case <-readCtx.Done():
					return
				}
			}
			if e != nil {
				return
			}
		}
	}()
	defer func() { cancel(); _ = input.Close(); <-done }()
	actions := inputActions{session: s, waiter: w, cases: o.inputs, counts: make([]int, len(o.inputs)), controls: controls, options: o, idle: after(time.Now(), o.inputTimeout)}
	r := inputRelay{matcher: matcher, actions: &actions, controls: controls}
	for {
		var timer *time.Timer
		var tick <-chan time.Time
		if !actions.idle.IsZero() {
			timer = time.NewTimer(time.Until(actions.idle))
			tick = timer.C
		}
		var event readEvent
		select {
		case e, ok := <-events:
			if !ok {
				event.err = io.EOF
			} else {
				event = e
			}
		case <-tick:
			index := eventCase(o.inputs, timeoutPattern)
			if index < 0 {
				return ErrIdleTimeout
			}
			err = actions.invoke(ctx, index, Result{}, true)
			if errors.Is(err, errInputStopped) || errors.Is(err, errInputCanceled) {
				return nil
			}
			if err != nil {
				return err
			}
			continue
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return nil
		}
		if timer != nil {
			timer.Stop()
		}
		if len(event.data) > 0 {
			actions.idle = after(time.Now(), o.inputTimeout)
			r.pending = append(r.pending, event.data...)
			matcher.append(event.data)
		}
		//nolint:errorlint // io.Reader EOF is an exact protocol sentinel; joined failures remain errors.
		eof := event.err == io.EOF
		if err = r.drain(ctx, eof); err != nil {
			if errors.Is(err, errInputStopped) {
				return nil
			}
			return err
		}
		if len(r.pending) == 0 {
			r.pending = nil
		}

		if event.err != nil {
			if eof {
				if i := eventCase(o.inputs, eofPattern); i >= 0 {
					e := actions.invoke(ctx, i, Result{}, false)
					if e != nil && !errors.Is(e, errInputStopped) && !errors.Is(e, errInputCanceled) {
						return e
					}
				}
				return nil
			}
			//nolint:errorlint // Only raw read errors caused by our cleanup are benign.
			if ctx.Err() != nil && (event.err == io.ErrClosedPipe || event.err == os.ErrClosed) {
				return nil
			}
			return event.err
		}
	}
}

type inputRelay struct {
	matcher  *inputMatcher
	actions  *inputActions
	controls chan<- func()
	pending  []byte
}

func (r *inputRelay) drain(ctx context.Context, eof bool) error {
	for len(r.pending) > 0 {
		index, loc, safe, e := r.matcher.choose(r.pending, eof)
		if e != nil {
			return e
		}
		if safe > 0 {
			if _, e = r.actions.session.Send(ctx, string(r.pending[:safe])); e != nil {
				return e
			}
			r.pending = r.pending[safe:]
			r.matcher.consume(safe)
			continue
		}
		if index >= 0 && loc.end > r.actions.session.opts.limit {
			if err := r.full(ctx); err != nil {
				return err
			}
			continue
		}
		if index >= 0 {
			if index == len(r.actions.cases) {
				select {
				case r.controls <- func() { r.actions.waiter.stopped = true }:
				case <-ctx.Done():
				}
				return errInputStopped
			}
			result := Result{Start: loc.start, End: loc.end, Match: string(r.pending[loc.start:loc.end]), Before: string(r.pending[:loc.start])}
			result.Captures = loc.capturesFromSnapshot(r.pending, result.Match)
			if e = r.actions.invoke(ctx, index, result, false); e != nil {
				if errors.Is(e, errInputStopped) || errors.Is(e, errInputCanceled) {
					return errInputStopped
				}
				return e
			}
			r.pending = r.pending[loc.end:]
			r.matcher.consume(loc.end)
			continue
		}
		if len(r.pending) >= r.actions.session.opts.limit {
			if err := r.full(ctx); err != nil {
				return err
			}
			continue
		}
		break
	}
	return nil
}

func (r *inputRelay) full(ctx context.Context) error {
	index := eventCase(r.actions.cases, fullBufferPattern)
	if index < 0 {
		return ErrBufferLimit
	}
	n := min(len(r.pending), r.actions.session.opts.limit)
	result := Result{Before: string(r.pending[:n]), End: n}
	if err := r.actions.invoke(ctx, index, result, false); err != nil {
		if errors.Is(err, errInputCanceled) {
			return errInputStopped
		}
		return err
	}
	r.pending = r.pending[n:]
	r.matcher.consume(n)
	return nil
}
