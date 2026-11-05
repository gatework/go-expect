// SPDX-License-Identifier: Apache-2.0

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
	"time"
)

const (
	streamTriggerBytes = 64 << 10
	receiveDetachBytes = 64 << 10
)

// Expect consumes input through the earliest currently visible match. Ties use
// declaration order. Continuations consume their match and rescan the remaining
// buffer, without rereading or replaying bytes. Context cancellation and timeout
// leave unmatched input available to a subsequent call. Callers must not change
// the foreground cases until the call returns.
func (s *Session) Expect(ctx context.Context, cases []Case, opts ...ExpectOption) (Result, error) {
	result, err := s.expect(ctx, cases, opts...)
	s.notifyExpect(ctx, result, err)
	return result, err
}
func (s *Session) notifyExpect(ctx context.Context, result Result, err error) {
	s.notify(ctx, Event{Source: result.Source, Scope: result.Scope, RuleIndex: result.RuleIndex, Operation: OpExpect, CaseIndex: result.CaseIndex, Bytes: len(result.Before) + len(result.Match), Error: errorClass(err)})
}
func (s *Session) expect(ctx context.Context, cases []Case, opts ...ExpectOption) (result Result, err error) {
	result.CaseIndex = -1
	result.RuleIndex = -1
	if !s.expecting.CompareAndSwap(false, true) {
		return result, ErrBusy
	}
	defer s.releaseWait()
	return s.expectOwned(ctx, cases, opts...)
}
func (s *Session) releaseWait() {
	s.releaseReceiveStorage()
	s.expecting.Store(false)
	if s.isClosed() && s.expecting.CompareAndSwap(false, true) {
		s.releaseReceiveStorage()
		s.expecting.Store(false)
	}
}
func (s *Session) expectOwned(ctx context.Context, cases []Case, opts ...ExpectOption) (Result, error) {
	w := waiter{session: s, result: Result{CaseIndex: -1, RuleIndex: -1}}
	if err := w.prepare(ctx, cases, opts); err != nil {
		return w.failure(err)
	}
	defer w.close()
	return w.run(ctx)
}

// Only the exclusive receive owner releases matching storage. In particular,
// Close must not clear it while a cooperative handler is still inspecting input.
// Track the complete allocation because slicing a consumed prefix hides that
// size from cap. Detach only at operation return, never per continuation.
func (s *Session) releaseReceiveStorage() {
	if s.isClosed() {
		s.receive.pending = nil
		s.receive.staged = nil
		s.receive.pendingAlloc = 0
		s.receive.stagedAlloc = 0
		return
	}

	releaseRemainder(&s.receive.pending, &s.receive.pendingAlloc)
	releaseRemainder(&s.receive.staged, &s.receive.stagedAlloc)
}

func releaseRemainder(data *[]byte, allocation *int) {
	if len(*data) == 0 {
		*data = nil
		*allocation = 0
	} else if *allocation >= receiveDetachBytes && len(*data) <= *allocation/4 {
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
