// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
)

// Operation identifies an observed operation.
type Operation string

// Operation names emitted by completion observers.
const (
	OpExpect   Operation = "expect"
	OpInteract Operation = "interact"
	OpSend     Operation = "send"
	OpSendAll  Operation = "send_all"
)

func (o Operation) String() string { return string(o) }

// ErrorClass is a payload-free error classification.
type ErrorClass string

// Error classes exposed by metadata diagnostics.
const (
	ClassOK          ErrorClass = ""
	ClassCanceled    ErrorClass = "canceled"
	ClassDeadline    ErrorClass = "deadline"
	ClassClosed      ErrorClass = "closed"
	ClassBusy        ErrorClass = "busy"
	ClassTimeout     ErrorClass = "timeout"
	ClassIdleTimeout ErrorClass = "idle_timeout"
	ClassEOF         ErrorClass = "eof"
	ClassError       ErrorClass = "error"
)

func (c ErrorClass) String() string { return string(c) }

// Event contains metadata only. Class classifies the error, never a raw message.
type Event struct {
	SessionID        SessionID
	Source           MatchSource
	Scope            RuleScope
	RuleIndex        int
	Operation        Operation
	CaseIndex, Bytes int
	Class            ErrorClass
}

// Observer is invoked once per completed public Expect, Send or SendAll operation.
// Observers run synchronously outside locks and must not block indefinitely.
type Observer func(Event)

// MatchSource identifies the byte stream evaluated by a match.
type MatchSource uint8

// Sources distinguish remote receive data from local interactive input.
const (
	SourceReceive MatchSource = iota
	SourceInput
)

func (s MatchSource) String() string {
	switch s {
	case SourceInput:
		return "input"
	case SourceReceive:
		return "receive"
	default:
		return "unknown"
	}
}

// SessionID is a process-local identity that remains stable across Group changes.
type SessionID uint64

var nextSessionID atomic.Uint64

// ID returns this Session's immutable identity.
func (s *Session) ID() SessionID { return s.id }

func errorClass(err error) ErrorClass {
	switch {
	case err == nil:
		return ClassOK
	case errors.Is(err, errInvalidReadCount):
		return ClassError
	case errors.Is(err, context.Canceled):
		return ClassCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return ClassDeadline
	case errors.Is(err, ErrClosed):
		return ClassClosed
	case errors.Is(err, ErrBusy):
		return ClassBusy
	case errors.Is(err, ErrTimeout):
		return ClassTimeout
	case errors.Is(err, ErrIdleTimeout):
		return ClassIdleTimeout
	case errors.Is(err, io.EOF):
		return ClassEOF
	default:
		return ClassError
	}
}

//nolint:contextcheck // Rejected nil-context calls still emit metadata; they have no parent context.
func (s *Session) notify(ctx context.Context, event Event) {
	if s.opts.structuredLogger != nil {
		if ctx == nil {
			ctx = context.Background()
		}
		s.opts.structuredLogger.LogAttrs(ctx, slog.LevelInfo, "expect", slog.Uint64("session", uint64(event.SessionID)), slog.String("op", event.Operation.String()), slog.Int("case", event.CaseIndex), slog.Int("bytes", event.Bytes), slog.String("class", event.Class.String()), slog.String("source", event.Source.String()), slog.String("scope", string(event.Scope)), slog.Int("rule", event.RuleIndex))
	}

	for _, observer := range s.opts.observers {
		observer(event)
	}
}

// TraceEvent describes a candidate evaluation without retaining payloads or causes.
type TraceEvent struct {
	SessionID                       SessionID
	Source                          MatchSource
	Scope                           RuleScope
	RuleIndex                       int
	CaseIndex, Buffered, Start, End int
	Pattern                         string
	Matched                         bool
}

// TraceObserver runs synchronously outside locks; it must not reenter Session.
type TraceObserver func(TraceEvent)
