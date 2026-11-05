//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

// Package spawn owns the lifecycle of a child with a controlling PTY.
package spawn

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	expect "github.com/gatework/go-expect"
)

// Option configures the child's terminal before it starts.
type Option func(*options) error
type options struct{ console []expect.ConsoleOption }

// WithSize sets terminal rows and columns.
func WithSize(rows, cols int) Option { return WithConsoleOptions(expect.WithSize(rows, cols)) }

// WithConsoleOptions configures the owned PTY and Session.
func WithConsoleOptions(opts ...expect.ConsoleOption) Option {
	opts = append([]expect.ConsoleOption(nil), opts...)
	return func(o *options) error { o.console = append(o.console, opts...); return nil }
}

// Process owns a child and its PTY. Do not copy it. Wait and Close are concurrent safe.
// Close cancels the child and joins Wait; exit status is reported by Wait.
type Process struct {
	console  *expect.Console
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	done     chan struct{}
	waitErr  error
	once     sync.Once
	closeErr error
}

// Start starts program with literal arguments in a new session and process group.
// Cancellation sends SIGHUP to that group. WaitDelay kills an unresponsive direct
// child after two seconds; descendants must cooperate with group SIGHUP.
func Start(ctx context.Context, program string, args []string, opts ...Option) (*Process, error) {
	if ctx == nil {
		return nil, &expect.ConfigError{Option: "context", Reason: "nil context"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		if opt == nil {
			return nil, &expect.ConfigError{Option: "spawn", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	c, err := expect.NewConsole(o.console...)
	if err != nil {
		return nil, err
	}
	childCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(childCtx, program, args...)
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGHUP)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	if err = c.Start(cmd); err != nil {
		cancel()
		return nil, errors.Join(err, c.Close())
	}
	p := &Process{console: c, cmd: cmd, cancel: cancel, done: make(chan struct{})}
	go func() { p.waitErr = cmd.Wait(); cancel(); close(p.done) }()
	return p, nil
}

// Session returns the owned conversation session.
func (p *Process) Session() *expect.Session { return p.console.Session() }

// Wait joins the child once and retains its actual exit error or signal.
func (p *Process) Wait() error { <-p.done; return p.waitErr }

// Close cancels the process, closes its PTY, and waits for it to be reaped.
func (p *Process) Close() error {
	p.once.Do(func() { p.cancel(); p.closeErr = p.console.Close(); <-p.done })
	return p.closeErr
}

// SetSize resizes the child's terminal.
func (p *Process) SetSize(rows, cols int) error { return p.console.SetSize(rows, cols) }

// SetEcho changes terminal echo.
func (p *Process) SetEcho(enabled bool) error { return p.console.SetEcho(enabled) }

// String formats the handle without exposing owned stream or command state.
func (*Process) String() string { return "spawn.Process" }

// GoString formats the handle without exposing owned stream or command state.
func (p *Process) GoString() string { return p.String() }
