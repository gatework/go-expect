// SPDX-License-Identifier: Apache-2.0

// Package spawn owns interactive child processes through PTYs on Unix and
// pseudoconsoles on Windows. Arguments are literal; no shell is implicit.
package spawn

import (
	"context"
	"errors"
	"fmt"
	"strings"

	expect "github.com/gatework/go-expect"
)

// Option configures a process before child creation.
type Option func(*options) error
type options struct {
	rows, cols int
	raw        bool
	session    []expect.SessionOption
	env        []string
	dir        string
}

// WithSize sets terminal rows and columns (default: 24 by 80). Windows limits
// dimensions to 32767; Unix supports 65535. Validation precedes child creation.
func WithSize(rows, cols int) Option {
	return func(o *options) error {
		if rows < 1 || cols < 1 || rows > 65535 || cols > 65535 {
			return &expect.ConfigError{Option: "size", Reason: "dimensions must be in 1..65535"}
		}
		o.rows, o.cols = rows, cols
		return nil
	}
}

// WithSessionOptions configures the owned conversation Session.
func WithSessionOptions(opts ...expect.SessionOption) Option {
	opts = append([]expect.SessionOption(nil), opts...)
	return func(o *options) error { o.session = append(o.session, opts...); return nil }
}

// WithEnv replaces the child's environment. Nil inherits; an empty nonnil slice
// creates an empty environment, following os/exec semantics.
func WithEnv(env []string) Option {
	var snapshot []string
	if env != nil {
		snapshot = append([]string{}, env...)
	}
	return func(o *options) error {
		for _, v := range snapshot {
			if strings.ContainsRune(v, 0) || !strings.ContainsRune(v, '=') {
				return &expect.ConfigError{Option: "environment", Reason: "invalid entry"}
			}
		}
		o.env = snapshot
		return nil
	}
}

// WithDir sets the child working directory; empty inherits the parent directory.
func WithDir(dir string) Option {
	return func(o *options) error {
		if strings.ContainsRune(dir, 0) {
			return &expect.ConfigError{Option: "directory", Reason: "contains NUL"}
		}
		o.dir = dir
		return nil
	}
}

// WithRaw configures a raw Unix slave. Windows returns ErrUnsupported.
func WithRaw() Option { return func(o *options) error { o.raw = true; return nil } }

// ErrUnsupported identifies a process or terminal capability unavailable here.
var ErrUnsupported = errors.New("spawn: capability unsupported")

// ExitStatus is a detached, payload-free child status. Signal is empty for a
// normal exit; a signaled Unix exit has Code -1. PID identifies the started child.
type ExitStatus struct {
	PID, Code int
	Signal    string
}

// ExitError reports failed completion without formatting program arguments.
type ExitError struct {
	Status ExitStatus
	Cause  error
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("spawn: process exited (code=%d signal=%s)", e.Status.Code, e.Status.Signal)
}
func (e *ExitError) Unwrap() error { return e.Cause }

// GoString omits the underlying process error.
func (e *ExitError) GoString() string { return e.Error() }

// Terminal exposes portable resizing. Assert EchoController for Unix echo control.
type Terminal interface{ SetSize(rows, cols int) error }

// EchoController is an optional terminal capability, absent on Windows.
type EchoController interface{ SetEcho(bool) error }

// Process owns a child, terminal and Session. Do not copy it. Wait and Close are
// safe concurrently. Consume output before Wait; Close interrupts undrained I/O.
type Process struct{ state *processState }

// Start selects the platform terminal backend and starts a literal command.
func Start(ctx context.Context, program string, args []string, opts ...Option) (*Process, error) {
	if ctx == nil {
		return nil, &expect.ConfigError{Option: "context", Reason: "nil context"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if program == "" || strings.ContainsRune(program, 0) {
		return nil, &expect.ConfigError{Option: "program", Reason: "empty or contains NUL"}
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, 0) {
			return nil, &expect.ConfigError{Option: "argument", Reason: "contains NUL"}
		}
	}
	o := options{rows: 24, cols: 80}
	for _, opt := range opts {
		if opt == nil {
			return nil, &expect.ConfigError{Option: "spawn", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	state, err := startProcess(ctx, program, append([]string(nil), args...), o)
	if err != nil {
		return nil, err
	}
	return &Process{state: state}, nil
}

// Session returns the owned conversation Session.
func (p *Process) Session() *expect.Session { return p.state.sessionHandle() }

// PID returns the immutable child process identifier.
func (p *Process) PID() int { return p.state.pid() }

// Wait joins the child and returns its status plus a typed error on failure.
func (p *Process) Wait() (ExitStatus, error) { return p.state.wait() }

// Close interrupts I/O, cancels the child and joins cleanup. It does not erase status.
func (p *Process) Close() error { return p.state.close() }

// SetSize resizes the terminal.
func (p *Process) SetSize(rows, cols int) error { return p.state.terminal().SetSize(rows, cols) }

// Terminal returns the backend's terminal controls without exposing its transport.
func (p *Process) Terminal() Terminal { return p.state.terminal() }

// String omits child program and environment data.
func (*Process) String() string { return "spawn.Process" }

// GoString omits child program and environment data.
func (p *Process) GoString() string { return p.String() }
