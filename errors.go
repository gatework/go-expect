// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"fmt"
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

// ResponseError identifies a response rule that exceeded its per-call limit.
type ResponseError struct{ RuleIndex int }

func (e *ResponseError) Error() string { return "expect: response limit exceeded" }
