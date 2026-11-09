// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"fmt"
	"regexp/syntax"
)

// Sentinel errors describe protocol and lifecycle failures.
var (
	ErrInvalidPattern  = errors.New("expect: invalid pattern")
	ErrClosed          = errors.New("expect: session closed")
	ErrBusy            = errors.New("expect: another expect operation is active")
	ErrTimeout         = errors.New("expect: timeout")
	ErrIdleTimeout     = errors.New("expect: idle timeout")
	ErrBufferLimit     = errors.New("expect: buffer limit reached")
	ErrEmptyMatch      = errors.New("expect: empty match")
	ErrPollUnsupported = errors.New("expect: OS polling unsupported")
)

// WriteError reports a failed, non-retryable write. Error never prints payloads
// or the underlying error message; Unwrap exposes the original cause explicitly.
type WriteError struct {
	Written int
	Cause   error
}

func (e *WriteError) Error() string {
	return fmt.Sprintf("expect: write failed (written=%d class=%s)", e.Written, errorClass(e.Cause))
}
func (e *WriteError) Unwrap() error { return e.Cause }

// GoString formats metadata without exposing payloads.
func (e *WriteError) GoString() string { return e.Error() }

// CaseLimitError identifies a response rule that exceeded its per-call limit.
type CaseLimitError struct {
	SessionID            SessionID
	Source               MatchSource
	Scope                RuleScope
	CaseIndex, RuleIndex int
}

func (e *CaseLimitError) Error() string { return "expect: case limit exceeded" }

// CallbackFailure identifies how an asynchronous callback failed.
type CallbackFailure string

const (
	// CallbackPanicked means a callback panicked; its payload is deliberately omitted.
	CallbackPanicked CallbackFailure = "panic"
	// CallbackExited means a callback terminated its goroutine without returning.
	CallbackExited CallbackFailure = "goexit"
)

// CallbackError reports abnormal callback completion without retaining payloads.
type CallbackError struct{ Kind CallbackFailure }

func (e *CallbackError) Error() string { return "expect: callback failed (" + string(e.Kind) + ")" }

// GoString formats case-limit metadata without exposing user fields.
func (e *CaseLimitError) GoString() string { return e.Error() }

// GoString formats callback failure metadata without a panic value.
func (e *CallbackError) GoString() string { return e.Error() }

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

// Unwrap identifies a rejected pattern without revealing its expression.
func (e *CompileError) Unwrap() error { return ErrInvalidPattern }
