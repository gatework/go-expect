// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"io"
	"log/slog"
	"time"
)

type sessionOptions struct {
	trace                    []TraceObserver
	sendOutputs              []io.Writer
	removeNulls, stripParity bool
	timeout, writeTimeout    time.Duration
	limit                    int
	lineEnding               string
	outputs                  []io.Writer
	observers                []Observer
	structuredLogger         *slog.Logger
	osPolling                bool
	before, after            []Case
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
			return &ConfigError{Option: "timeout", Reason: "must be nonnegative"}
		}
		o.timeout = d
		return nil
	}
}

// WithWriteTimeout bounds each Send, including serialization wait; zero disables it.
func WithWriteTimeout(d time.Duration) SessionOption {
	return func(o *sessionOptions) error {
		if d < 0 {
			return &ConfigError{Option: "write timeout", Reason: "must be nonnegative"}
		}
		o.writeTimeout = d
		return nil
	}
}

// WithBufferLimit bounds unconsumed matching input; overflow never discards data.
func WithBufferLimit(n int) SessionOption {
	return func(o *sessionOptions) error {
		if n <= 0 {
			return &ConfigError{Option: "buffer limit", Reason: "must be positive"}
		}
		o.limit = n
		return nil
	}
}

// WithLineEnding selects LF, CR or CRLF for SendLine. Send and Reply are verbatim.
func WithLineEnding(s string) SessionOption {
	return func(o *sessionOptions) error {
		if s != "\n" && s != "\r" && s != "\r\n" {
			return &ConfigError{Option: "line ending", Reason: "must be LF, CR or CRLF"}
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
				return &ConfigError{Option: "output", Reason: "nil writer"}
			}
		}
		o.outputs = append(o.outputs, writers...)
		return nil
	}
}

// WithObserver adds metadata observers in declaration order.
func WithObserver(observers ...Observer) SessionOption {
	observers = append([]Observer(nil), observers...)
	return func(o *sessionOptions) error {
		for _, f := range observers {
			if f == nil {
				return &ConfigError{Option: "observer", Reason: "nil callback"}
			}
		}
		o.observers = append(o.observers, observers...)
		return nil
	}
}

// WithLogger enables metadata-only diagnostics; payloads and causes are omitted.
func WithLogger(logger *slog.Logger) SessionOption {
	return func(o *sessionOptions) error {
		if logger == nil {
			return &ConfigError{Option: "logger", Reason: "nil logger"}
		}
		o.structuredLogger = logger
		return nil
	}
}

// WithOSPolling enables sole-reader OS readiness coordination for Poll and
// SendAllPoll. The transport must expose syscall.Conn and a nonblocking OS fd.
// Unsupported transports fail construction before ownership transfers.
// A working SetReadDeadline(time.Time) error capability selects the runtime
// poller; otherwise construction selects the bounded native-poll fallback.
// Wrappers may forward NormalizeReadError(error) error for transport-specific
// read errors, such as PTY EIO at EOF. Control errors are never normalized.
func WithOSPolling() SessionOption {
	return func(o *sessionOptions) error { o.osPolling = true; return nil }
}
