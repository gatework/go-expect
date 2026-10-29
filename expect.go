// Copyright 2018 Netflix, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expect

import (
	"context"
	"errors"
	"io"
	"time"
)

// Expect consumes input through the earliest currently visible match. Ties use
// declaration order. Continuations consume their match and rescan the remaining
// buffer, without rereading or replaying bytes. Context cancellation and timeout
// leave unmatched input available to a subsequent call.
func (s *Session) Expect(ctx context.Context, cases []Case, opts ...ExpectOption) (Result, error) {
	result, err := s.expect(ctx, cases, nil, opts...)
	s.notifyExpect(result, err)
	return result, err
}
func (s *Session) notifyExpect(result Result, err error) {
	s.notify(Event{Operation: "expect", CaseIndex: result.CaseIndex, Bytes: len(result.Before) + len(result.Match), Error: errorClass(err)})
}
func (s *Session) expect(ctx context.Context, cases []Case, guards []func(Result) error, opts ...ExpectOption) (result Result, err error) {
	result.CaseIndex = -1
	if !s.expecting.CompareAndSwap(false, true) {
		return result, ErrBusy
	}
	defer s.expecting.Store(false)
	defer s.releaseReceiveStorage()
	failure := func(e error) (Result, error) { return Result{CaseIndex: -1, Before: string(s.pending)}, e }
	if ctx == nil {
		return failure(errors.New("expect: nil context"))
	}
	if err = ctx.Err(); err != nil {
		return failure(err)
	}
	if len(cases) == 0 {
		return failure(errors.New("expect: no cases"))
	}
	cases = append([]Case(nil), cases...)
	for _, c := range cases {
		if err = c.Pattern.validate(false); err != nil {
			return failure(err)
		}
	}
	o := expectOptions{timeout: s.opts.timeout}
	for _, opt := range opts {
		if opt == nil {
			return failure(errors.New("expect: nil expect option"))
		}
		if err = opt(&o); err != nil {
			return failure(err)
		}
	}
	// One cancellable handler scope per wait avoids per-continuation allocation
	// when relative and idle timers are disabled. Close never waits for callbacks.
	handlerCtx, cancelHandlers := context.WithCancel(ctx)
	s.lifeMu.Lock()
	if s.isClosed() {
		cancelHandlers()
	} else {
		s.handlerCancel = cancelHandlers
	}
	s.lifeMu.Unlock()
	defer func() {
		cancelHandlers()
		s.lifeMu.Lock()
		s.handlerCancel = nil
		s.lifeMu.Unlock()
	}()
	now := time.Now()
	relative := after(now, o.timeout)
	idle := after(now, o.idle)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	// Match state is local to this call and resets whenever input is consumed.
	// Ordinary patterns retain only their existing scan offsets. Allocate All
	// leaf state only when a pure literal tree is actually present.
	scanned := make([]int, len(cases))
	var allScans []patternScan
	for i, c := range cases {
		if c.Pattern.kind != allPattern {
			continue
		}
		state := newPatternScan(c.Pattern)
		if len(state.all) == 0 {
			continue
		}
		if allScans == nil {
			allScans = make([]patternScan, len(cases))
		}
		allScans[i] = state
	}
	var pollAck chan struct{}
	pollOwned := false
	polled := false
	for {
		if err = ctx.Err(); err != nil {
			return failure(err)
		}
		if s.isClosed() {
			return failure(ErrClosed)
		}
		now = time.Now()
		if !relative.IsZero() && !now.Before(relative) {
			return failure(ErrTimeout)
		}
		if !idle.IsZero() && !now.Before(idle) {
			return failure(ErrIdleTimeout)
		}
		chosen := -1
		var loc location
		for i, c := range cases {
			from := 0
			if c.Pattern.kind == literalPattern {
				from = max(0, scanned[i]-len(c.Pattern.text)+1)
			}
			var hit location
			var ok bool
			var e error
			if allScans != nil && len(allScans[i].all) > 0 {
				hit, ok = allScans[i].findAll(s.pending)
			} else {
				hit, ok, e = c.Pattern.find(s.pending, s.terminal != nil, from)
			}
			scanned[i] = len(s.pending)
			if e != nil {
				return failure(e)
			}
			if ok && (chosen < 0 || (!o.declaration && hit.start < loc.start)) {
				chosen = i
				loc = hit
				if o.declaration {
					break
				}
			}
		}
		streaming := false
		if chosen < 0 && o.streaming != nil && len(s.pending) >= 64<<10 {
			chosen = len(cases)
			loc = location{start: 0, end: 32 << 10}
			streaming = true
		}
		eof := false
		if chosen < 0 && s.terminal != nil {
			if errors.Is(s.terminal, io.EOF) {
				for i, c := range cases {
					if c.Pattern.kind == eofPattern {
						chosen = i
						loc = location{start: len(s.pending), end: len(s.pending)}
						eof = true
						break
					}
				}
			}
			if chosen < 0 {
				return failure(s.terminal)
			}
		}
		if chosen >= 0 {
			// Matching may take time. Recheck deadlines before consuming input or
			// allowing a response, not only before the scan started.
			if err := operationError(ctx, relative, idle); err != nil {
				return failure(err)
			}
			if s.isClosed() {
				return failure(ErrClosed)
			}
			index := chosen
			if streaming {
				index = -1
			}
			result = Result{CaseIndex: index, Before: string(s.pending[:loc.start]), Match: string(s.pending[loc.start:loc.end]), Captures: loc.captures}
			if !streaming && guards != nil && guards[chosen] != nil {
				if err = guards[chosen](result); err != nil {
					return result, err
				}
			}
			// Advance without copying the remaining burst on every continuation.
			s.pending = s.pending[loc.end:]
			if len(s.pending) == 0 {
				s.pending = nil
				s.pendingAlloc = 0
			}
			clear(scanned)
			for i := range allScans {
				allScans[i].reset()
			}
			var handler Handler
			if streaming {
				handler = o.streaming
			} else {
				handler = cases[chosen].Handler
			}
			if handler == nil {
				return result, nil
			}
			action, handlerErr := s.callHandler(handlerCtx, earlier(relative, idle), handler, result)
			if handlerErr != nil {
				if errors.Is(handlerErr, context.DeadlineExceeded) && ctx.Err() == nil {
					if !relative.IsZero() && !time.Now().Before(relative) {
						handlerErr = errors.Join(handlerErr, ErrTimeout)
					} else if !idle.IsZero() && !time.Now().Before(idle) {
						handlerErr = errors.Join(handlerErr, ErrIdleTimeout)
					}
				}
				return result, handlerErr
			}
			if err := ctx.Err(); err != nil {
				return result, err
			}
			switch action {
			case Return:
				return result, operationError(ctx, relative, idle)
			case Continue:
				if !streaming {
					relative = after(time.Now(), o.timeout)
				}
			case ContinueWithoutReset:
			default:
				return result, errors.New("expect: invalid handler action")
			}
			if eof {
				return result, io.EOF
			} // EOF continuations cannot spin forever.
			continue
		}
		// A full buffer is not yet overflow: an independent terminal event
		// may follow. Keep bounded lookahead to distinguish that from more data.
		if len(s.pending) >= s.opts.limit && len(s.staged) > 0 {
			return failure(ErrBufferLimit)
		}
		if len(s.staged) > 0 {
			n := s.opts.limit - len(s.pending)
			if o.streaming != nil && len(s.pending) < 64<<10 {
				n = min(n, (64<<10)-len(s.pending))
			}
			if n > len(s.staged) {
				n = len(s.staged)
			}
			data := s.staged[:n]
			if len(s.pending) == 0 {
				// The pump transfers exclusive ownership. Cap the slice so a later
				// append cannot overwrite bytes still waiting in staged.
				s.pending = data[:n:n]
				s.pendingAlloc = s.stagedAlloc
			} else {
				grow := len(s.pending)+n > cap(s.pending)
				s.pending = append(s.pending, data...)
				if grow {
					s.pendingAlloc = cap(s.pending)
				}
			}
			s.staged = s.staged[n:]
			if len(s.staged) == 0 {
				s.staged = nil
				s.stagedAlloc = 0
			}
			idle = after(time.Now(), o.idle)
			if err = s.emit(data, false); err != nil {
				return failure(errors.Join(err, s.Close()))
			}
			if len(s.staged) == 0 && s.stagedErr != nil {
				s.terminal = s.stagedErr
				s.stagedErr = nil
				if err = s.emit(nil, true); err != nil {
					return failure(errors.Join(err, s.Close()))
				}
			}
			continue
		}
		deadline := earlier(relative, idle)
		var wake <-chan time.Time
		if !deadline.IsZero() {
			timer.Reset(time.Until(deadline))
			wake = timer.C
		}
		select {
		case s.inputGate <- struct{}{}:
		case <-wake:
			continue
		case <-ctx.Done():
			return failure(ctx.Err())
		case <-s.done:
			return failure(ErrClosed)
		}
		if len(s.queued) > 0 {
			event := s.queued[0]
			s.queued[0] = readEvent{}
			s.queued = nil
			s.queuedBytes -= len(event.data)
			<-s.inputGate
			if e := s.acceptEvent(event); e != nil {
				return failure(errors.Join(e, s.Close()))
			}
			continue
		}
		if o.poll && pollAck == nil && !polled {
			var e error
			pollAck, pollOwned, e = s.requestPoll()
			if e != nil {
				<-s.inputGate
				return failure(errors.Join(e, s.Close()))
			}
		}
		if o.poll && polled {
			select {
			case event, ok := <-s.events:
				<-s.inputGate
				if ok {
					if e := s.acceptEvent(event); e != nil {
						return failure(errors.Join(e, s.Close()))
					}
					continue
				}
			default:
				<-s.inputGate
				return failure(ErrTimeout)
			}
			continue
		}
		select {
		case <-pollAck:
			<-s.inputGate
			// A canceled caller's request stays pump-owned through its ack. A
			// later Poll waits for it, then requests its own readiness boundary.
			polled = pollOwned
			pollAck = nil
			continue
		case <-ctx.Done():
			<-s.inputGate
			timer.Stop()
			return failure(ctx.Err())
		case <-s.done:
			<-s.inputGate
			timer.Stop()
			return failure(ErrClosed)
		case <-wake:
			<-s.inputGate
			continue
		case event, ok := <-s.events:
			<-s.inputGate
			timer.Stop()
			if !ok {
				if s.terminal == nil {
					s.terminal = io.EOF
				}
				continue
			}
			if err = s.acceptEvent(event); err != nil {
				return failure(errors.Join(err, s.Close()))
			}
		}
	}
}

// Only the exclusive receive owner releases matching storage. In particular,
// Close must not clear it while a cooperative handler is still inspecting input.
// Track the complete allocation because slicing a consumed prefix hides that
// size from cap. Detach only at operation return, never per continuation.
func (s *Session) releaseReceiveStorage() {
	releaseRemainder(&s.pending, &s.pendingAlloc)
	releaseRemainder(&s.staged, &s.stagedAlloc)
}

func releaseRemainder(data *[]byte, allocation *int) {
	if len(*data) == 0 {
		*data = nil
		*allocation = 0
	} else if *allocation >= 64<<10 && len(*data) <= *allocation/4 {
		remaining := make([]byte, len(*data))
		copy(remaining, *data)
		*data = remaining
		*allocation = cap(remaining)
	}
}

func (s *Session) callHandler(ctx context.Context, deadline time.Time, handler Handler, result Result) (Action, error) {
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	return handler(ctx, s, result)
}

func operationError(ctx context.Context, relative, idle time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	if !relative.IsZero() && !now.Before(relative) {
		return ErrTimeout
	}
	if !idle.IsZero() && !now.Before(idle) {
		return ErrIdleTimeout
	}
	return nil
}

func after(now time.Time, d time.Duration) time.Time {
	if d == 0 {
		return time.Time{}
	}
	return now.Add(d)
}
func earlier(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() || a.Before(b) {
		return a
	}
	return b
}
