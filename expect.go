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
	if ctx == nil {
		return result, errors.New("expect: nil context")
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if len(cases) == 0 {
		return result, errors.New("expect: no cases")
	}
	cases = append([]Case(nil), cases...)
	for _, c := range cases {
		if err = c.Pattern.validate(false); err != nil {
			return result, err
		}
	}
	o := expectOptions{timeout: s.opts.timeout}
	for _, opt := range opts {
		if opt == nil {
			return result, errors.New("expect: nil expect option")
		}
		if err = opt(&o); err != nil {
			return result, err
		}
	}
	now := time.Now()
	relative := after(now, o.timeout)
	idle := after(now, o.idle)
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	// Literal scan positions are local to this call and reset on consumption.
	scanned := make([]int, len(cases))
	failure := func(e error) (Result, error) { return Result{CaseIndex: -1, Before: string(s.pending)}, e }
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
				from = scanned[i] - len(c.Pattern.text) + 1
				if from < 0 {
					from = 0
				}
			}
			hit, ok, e := c.Pattern.find(s.pending, s.terminal != nil, from)
			if e != nil {
				return failure(e)
			}
			scanned[i] = len(s.pending)
			if ok && (chosen < 0 || hit.start < loc.start) {
				chosen = i
				loc = hit
			}
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
			result = Result{CaseIndex: chosen, Before: string(s.pending[:loc.start]), Match: string(s.pending[loc.start:loc.end]), Captures: loc.captures}
			if guards != nil && guards[chosen] != nil {
				if err = guards[chosen](result); err != nil {
					return result, err
				}
			}
			// Advance without copying the remaining burst on every continuation.
			s.pending = s.pending[loc.end:]
			for i := range scanned {
				scanned[i] = 0
			}
			handler := cases[chosen].Handler
			if handler == nil {
				return result, nil
			}
			action, handlerErr := s.callHandler(ctx, earlier(relative, idle), handler, result)
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
				relative = after(time.Now(), o.timeout)
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
			if n > len(s.staged) {
				n = len(s.staged)
			}
			data := s.staged[:n]
			if len(s.pending) == 0 {
				// The pump transfers exclusive ownership. Cap the slice so a later
				// append cannot overwrite bytes still waiting in staged.
				s.pending = data[:n:n]
			} else {
				s.pending = append(s.pending, data...)
			}
			s.staged = s.staged[n:]
			idle = after(time.Now(), o.idle)
			if err = s.emit(data, false); err != nil {
				s.Close()
				return failure(err)
			}
			if len(s.staged) == 0 && s.stagedErr != nil {
				s.terminal = s.stagedErr
				s.stagedErr = nil
				if err = s.emit(nil, true); err != nil {
					s.Close()
					return failure(err)
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
		case <-ctx.Done():
			timer.Stop()
			return failure(ctx.Err())
		case <-s.done:
			timer.Stop()
			return failure(ErrClosed)
		case <-wake:
			continue
		case event, ok := <-s.events:
			timer.Stop()
			if !ok {
				if s.terminal == nil {
					s.terminal = io.EOF
				}
				continue
			}
			s.staged = event.data
			s.stagedErr = event.err
			if len(s.staged) == 0 && event.err != nil {
				s.terminal = event.err
				s.stagedErr = nil
				if err = s.emit(nil, true); err != nil {
					s.Close()
					return failure(err)
				}
			}
		}
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
