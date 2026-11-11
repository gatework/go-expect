// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
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
	readCtx, cancel := context.WithCancel(ctx)
	events := make(chan readEvent, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		pumpInput(readCtx, input, events)
	}()
	defer func() { cancel(); _ = input.Close(); <-done }()
	actions := inputActions{session: s, waiter: w, cases: o.inputs, counts: make([]int, len(o.inputs)), controls: controls, options: o, idle: after(time.Now(), o.inputTimeout)}
	r := inputRelay{matcher: o.matcher, actions: &actions, controls: controls}
	for {
		if finished, err := r.step(ctx, events); finished {
			return err
		}
	}
}

// inputBufferSize is the read size of the local input pump.
const inputBufferSize = 4096

// maxEmptyReads is how many consecutive (0, nil) reads count as no progress.
const maxEmptyReads = 100

// inputReader performs the local input reads of one interaction.
type inputReader struct {
	input io.Reader
	buf   []byte
	empty int
}

// next performs one read. publish reports whether event carries data or an
// error; last reports that reading has ended.
func (r *inputReader) next() (event readEvent, publish, last bool) {
	n, e := r.input.Read(r.buf)
	if n < 0 || n > len(r.buf) {
		n = 0
		e = errors.Join(errInvalidReadCount, e)
	}
	if n == 0 && e == nil {
		r.empty++
		if r.empty >= maxEmptyReads {
			e = io.ErrNoProgress
		}
	} else {
		r.empty = 0
	}
	if n == 0 && e == nil {
		return readEvent{}, false, false
	}
	var data []byte
	if n >= len(r.buf)/2 {
		// Hand the large buffer over instead of copying it.
		data = r.buf[:n]
		r.buf = make([]byte, inputBufferSize)
	} else {
		data = bytes.Clone(r.buf[:n])
	}
	return readEvent{data: data, err: e}, true, e != nil
}

// pumpInput is the sole local read pump. It closes events when reading ends
// or ctx is cancelled.
func pumpInput(ctx context.Context, input io.Reader, events chan<- readEvent) {
	defer close(events)
	r := inputReader{input: input, buf: make([]byte, inputBufferSize)}
	for {
		event, publish, last := r.next()
		if publish {
			select {
			case events <- event:
			case <-ctx.Done():
				return
			}
		}
		if last {
			return
		}
	}
}

type inputRelay struct {
	matcher  *inputMatcher
	actions  *inputActions
	controls chan<- func()
	pending  []byte
}

// step waits for the next input event or idle deadline and handles it;
// finished reports that the interaction ended with err.
func (r *inputRelay) step(ctx context.Context, events <-chan readEvent) (finished bool, err error) {
	event, idle, live := r.await(ctx, events)
	switch {
	case !live:
		return true, nil
	case idle:
		return r.idleTimeout(ctx)
	}
	return r.handle(ctx, event)
}

// await returns the next read event (a closed pump reads as EOF), or idle when
// the input timeout elapsed first; live is false once ctx is done.
func (r *inputRelay) await(ctx context.Context, events <-chan readEvent) (event readEvent, idle, live bool) {
	var tick <-chan time.Time
	if !r.actions.idle.IsZero() {
		timer := time.NewTimer(time.Until(r.actions.idle))
		defer timer.Stop()
		tick = timer.C
	}
	select {
	case e, open := <-events:
		if !open {
			return readEvent{err: io.EOF}, false, true
		}
		return e, false, true
	case <-tick:
		return readEvent{}, true, true
	case <-ctx.Done():
		return readEvent{}, false, false
	}
}

// idleTimeout runs the Timeout input case; without one the interaction fails.
func (r *inputRelay) idleTimeout(ctx context.Context) (finished bool, err error) {
	index := eventCase(r.actions.cases, timeoutPattern)
	if index < 0 {
		return true, ErrIdleTimeout
	}
	err = r.actions.invoke(ctx, index, Result{}, true)
	if errors.Is(err, errInputStopped) || errors.Is(err, errInputCanceled) {
		return true, nil
	}
	return err != nil, err
}

// handle forwards or matches newly read input, then ends the interaction on a
// read error.
func (r *inputRelay) handle(ctx context.Context, event readEvent) (finished bool, err error) {
	if len(event.data) > 0 {
		r.actions.idle = after(time.Now(), r.actions.options.inputTimeout)
		r.pending = append(r.pending, event.data...)
		r.matcher.append(event.data)
	}
	//nolint:errorlint // io.Reader EOF is an exact protocol sentinel; joined failures remain errors.
	eof := event.err == io.EOF
	if err = r.drain(ctx, eof); err != nil {
		if errors.Is(err, errInputStopped) {
			return true, nil
		}
		return true, err
	}
	if len(r.pending) == 0 {
		r.pending = nil
	}
	switch {
	case event.err == nil:
		return false, nil
	case eof:
		return true, r.endOfInput(ctx)
	//nolint:errorlint // Only raw read errors caused by our cleanup are benign.
	case ctx.Err() != nil && (event.err == io.ErrClosedPipe || event.err == os.ErrClosed):
		return true, nil
	}
	return true, event.err
}

// endOfInput runs the EOF input case, if any; stopping or cancelling it ends
// the interaction cleanly.
func (r *inputRelay) endOfInput(ctx context.Context) error {
	i := eventCase(r.actions.cases, eofPattern)
	if i < 0 {
		return nil
	}
	if e := r.actions.invoke(ctx, i, Result{}, false); e != nil && !errors.Is(e, errInputStopped) && !errors.Is(e, errInputCanceled) {
		return e
	}
	return nil
}

// drain forwards and matches pending input until it must wait for more bytes.
func (r *inputRelay) drain(ctx context.Context, eof bool) error {
	for len(r.pending) > 0 {
		index, loc, safe, e := r.matcher.choose(r.pending, eof)
		if e != nil {
			return e
		}
		progressed, err := r.advance(ctx, index, loc, safe)
		if err != nil {
			return err
		}
		if !progressed {
			break
		}
	}
	return nil
}

// advance acts on one matcher decision: it forwards safe bytes, stops, runs
// the chosen input case or the FullBuffer case. progressed is false when the
// pending input must wait for more bytes.
func (r *inputRelay) advance(ctx context.Context, index int, loc location, safe int) (progressed bool, err error) {
	limit := r.actions.session.opts.limit
	switch {
	case safe > 0:
		if _, err = r.actions.session.Send(ctx, string(r.pending[:safe])); err != nil {
			return true, err
		}
		r.pending = r.pending[safe:]
		r.matcher.consume(safe)
		return true, nil
	case index >= 0 && loc.end > limit:
		return true, r.full(ctx)
	case index == len(r.actions.cases):
		select {
		case r.controls <- func() { r.actions.waiter.stopped = true }:
		case <-ctx.Done():
		}
		return true, errInputStopped
	case index >= 0:
		return true, r.match(ctx, index, loc)
	case len(r.pending) >= limit:
		return true, r.full(ctx)
	}
	return false, nil
}

// match runs input case index on the matched bytes and consumes them.
func (r *inputRelay) match(ctx context.Context, index int, loc location) error {
	result := Result{Start: loc.start, End: loc.end, Match: string(r.pending[loc.start:loc.end]), Before: string(r.pending[:loc.start])}
	result.Captures = loc.capturesFromSnapshot(r.pending, result.Match)
	if e := r.actions.invoke(ctx, index, result, false); e != nil {
		if errors.Is(e, errInputStopped) || errors.Is(e, errInputCanceled) {
			return errInputStopped
		}
		return e
	}
	r.pending = r.pending[loc.end:]
	r.matcher.consume(loc.end)
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
