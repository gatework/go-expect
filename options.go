package expect

import (
	"errors"
	"io"
	"log"
	"time"
)

var (
	ErrClosed      = errors.New("expect: session closed")
	ErrBusy        = errors.New("expect: another expect operation is active")
	ErrTimeout     = errors.New("expect: timeout")
	ErrIdleTimeout = errors.New("expect: idle timeout")
	ErrBufferLimit = errors.New("expect: buffer limit reached")
	ErrEmptyMatch  = errors.New("expect: empty match")
)

// WriteError reports a failed, non-retryable write. Error never prints payloads
// or the underlying error message; Unwrap exposes the original cause explicitly.
type WriteError struct {
	Written int
	Cause   error
}

func (e *WriteError) Error() string    { return "expect: write failed" }
func (e *WriteError) Unwrap() error    { return e.Cause }
func (e *WriteError) GoString() string { return e.Error() }

// ResponseError identifies a response rule that exceeded its per-call limit.
type ResponseError struct{ RuleIndex int }

func (e *ResponseError) Error() string { return "expect: response limit exceeded" }

// Event contains metadata only. Error is a classification, never a raw message.
type Event struct {
	Operation        string
	CaseIndex, Bytes int
	Error            string
}

// Observer is invoked once per completed public Expect or Send operation.
// Observers run synchronously outside locks and must not block indefinitely.
type Observer func(Event)

type sessionOptions struct {
	timeout, writeTimeout time.Duration
	limit                 int
	lineEnding            string
	outputs               []io.Writer
	observers             []Observer
	logger                *log.Logger
}

// SessionOption configures a Session before it takes ownership of the transport.
type SessionOption func(*sessionOptions) error

func defaults() sessionOptions {
	return sessionOptions{timeout: 30 * time.Second, writeTimeout: 10 * time.Second, limit: 8 << 20, lineEnding: "\n"}
}

// WithDefaultTimeout sets the relative Expect timeout; zero disables it.
func WithDefaultTimeout(d time.Duration) SessionOption {
	return func(o *sessionOptions) error {
		if d < 0 {
			return errors.New("expect: negative timeout")
		}
		o.timeout = d
		return nil
	}
}

// WithWriteTimeout bounds each Send, including serialization wait; zero disables it.
func WithWriteTimeout(d time.Duration) SessionOption {
	return func(o *sessionOptions) error {
		if d < 0 {
			return errors.New("expect: negative write timeout")
		}
		o.writeTimeout = d
		return nil
	}
}

// WithBufferLimit bounds unconsumed matching input; overflow never discards data.
func WithBufferLimit(n int) SessionOption {
	return func(o *sessionOptions) error {
		if n <= 0 {
			return errors.New("expect: buffer limit must be positive")
		}
		o.limit = n
		return nil
	}
}

// WithLineEnding selects LF, CR or CRLF for SendLine. Send and Reply are verbatim.
func WithLineEnding(s string) SessionOption {
	return func(o *sessionOptions) error {
		if s != "\n" && s != "\r" && s != "\r\n" {
			return errors.New("expect: invalid line ending")
		}
		o.lineEnding = s
		return nil
	}
}

// WithOutput copies received input through the streaming secret filter.
// Writers are borrowed, synchronous and must not reenter Session methods.
// Failure closes the Session; delivery to earlier writers is not replayed.
func WithOutput(writers ...io.Writer) SessionOption {
	writers = append([]io.Writer(nil), writers...)
	return func(o *sessionOptions) error {
		for _, w := range writers {
			if nilInterface(w) {
				return errors.New("expect: nil output")
			}
		}
		o.outputs = append(o.outputs, writers...)
		return nil
	}
}

// WithTranscript records the same filtered receive stream as WithOutput.
func WithTranscript(w io.Writer) SessionOption { return WithOutput(w) }

// WithObserver adds metadata observers in declaration order.
func WithObserver(observers ...Observer) SessionOption {
	observers = append([]Observer(nil), observers...)
	return func(o *sessionOptions) error {
		for _, f := range observers {
			if f == nil {
				return errors.New("expect: nil observer")
			}
		}
		o.observers = append(o.observers, observers...)
		return nil
	}
}

// WithLogger enables metadata-only diagnostics; payloads and causes are omitted.
func WithLogger(l *log.Logger) SessionOption {
	return func(o *sessionOptions) error {
		if l == nil {
			return errors.New("expect: nil logger")
		}
		o.logger = l
		return nil
	}
}

type expectOptions struct{ timeout, idle time.Duration }

// ExpectOption changes the timing of a single Expect operation.
type ExpectOption func(*expectOptions) error

// WithTimeout sets the relative timer, including elapsed callback time.
// Zero disables it. Continue can reset it; context deadlines cannot be reset.
func WithTimeout(d time.Duration) ExpectOption {
	return func(o *expectOptions) error {
		if d < 0 {
			return errors.New("expect: negative timeout")
		}
		o.timeout = d
		return nil
	}
}

// WithIdleTimeout limits time without new input; zero disables it.
func WithIdleTimeout(d time.Duration) ExpectOption {
	return func(o *expectOptions) error {
		if d < 0 {
			return errors.New("expect: negative idle timeout")
		}
		o.idle = d
		return nil
	}
}
