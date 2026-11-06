// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"fmt"
	"regexp/syntax"
	"sync/atomic"
)

// Op identifies an observed operation.
type Op string

// Operation names emitted by completion observers.
const (
	OpExpect   Op = "expect"
	OpInteract Op = "interact"
	OpSend     Op = "send"
	OpSendAll  Op = "send_all"
)

func (o Op) String() string { return string(o) }

// ErrClass is a payload-free error classification.
type ErrClass string

// Error classes exposed by metadata diagnostics.
const (
	ClassOK          ErrClass = ""
	ClassCanceled    ErrClass = "canceled"
	ClassDeadline    ErrClass = "deadline"
	ClassClosed      ErrClass = "closed"
	ClassBusy        ErrClass = "busy"
	ClassTimeout     ErrClass = "timeout"
	ClassIdleTimeout ErrClass = "idle_timeout"
	ClassEOF         ErrClass = "eof"
	ClassError       ErrClass = "error"
)

func (c ErrClass) String() string { return string(c) }

// ConfigError identifies a rejected option without retaining caller payloads.
type ConfigError struct {
	Option string
	Reason string
}

func (e *ConfigError) Error() string { return "expect: invalid " + e.Option + ": " + e.Reason }

// CompileError retains the syntax category, never the original expression.
type CompileError struct{ Code syntax.ErrorCode }

func (e *CompileError) Error() string {
	return fmt.Sprintf("expect: invalid regular expression (%s)", e.Code)
}

// Event contains metadata only. Error is a classification, never a raw message.
type Event struct {
	SessionID        SessionID
	Source           MatchSource
	Scope            RuleScope
	RuleIndex        int
	Operation        Op
	CaseIndex, Bytes int
	Error            ErrClass
}

// Observer is invoked once per completed public Expect, Send or SendAll operation.
// Observers run synchronously outside locks and must not block indefinitely.
type Observer func(Event)

// Unwrap identifies a rejected pattern without revealing its expression.
func (e *CompileError) Unwrap() error { return ErrInvalidPattern }

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
