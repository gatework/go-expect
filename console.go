//go:build linux || darwin || freebsd

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
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// Console owns a PTY and Session. Start attaches a caller-owned process to a
// controlling terminal; the caller remains responsible for cancellation and Wait.
type Console struct {
	session *Session
	tty     *os.File
	master  *os.File
}
type consoleOptions struct {
	raw        bool
	rows, cols uint16
	session    []SessionOption
}

// ConsoleOption configures the PTY adapter.
type ConsoleOption func(*consoleOptions) error

// WithSessionOptions configures the Console's underlying Session.
func WithSessionOptions(opts ...SessionOption) ConsoleOption {
	opts = append([]SessionOption(nil), opts...)
	return func(o *consoleOptions) error { o.session = append(o.session, opts...); return nil }
}

// WithRaw configures the slave in raw mode, including disabling echo.
func WithRaw() ConsoleOption { return func(o *consoleOptions) error { o.raw = true; return nil } }

// NewConsole creates a PTY and a Session. Failure closes every acquired handle.
func NewConsole(opts ...ConsoleOption) (*Console, error) {
	o := consoleOptions{rows: 24, cols: 80}
	for _, opt := range opts {
		if opt == nil {
			return nil, &ConfigError{Option: "console", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	master, slave, err := pty.Open()
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			master.Close()
			slave.Close()
		}
	}()
	// os.NewFile registers nonblocking descriptors with the runtime poller.
	// Duplicate before wrapping: two os.File values must never own the same fd.
	raw, err := master.SyscallConn()
	if err != nil {
		return nil, err
	}
	fd := -1
	var controlErr error
	err = raw.Control(func(original uintptr) {
		fd, controlErr = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 0)
		if controlErr == nil {
			controlErr = unix.SetNonblock(fd, true)
		}
	})
	if err != nil || controlErr != nil {
		if fd >= 0 {
			unix.Close(fd)
		}
		return nil, errors.Join(err, controlErr)
	}
	pollable := os.NewFile(uintptr(fd), master.Name())
	master.Close()
	master = pollable
	if err = master.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	if err = setConsoleSize(master, o.rows, o.cols); err != nil {
		return nil, err
	}
	if o.raw {
		if _, err = term.MakeRaw(int(slave.Fd())); err != nil {
			return nil, err
		}
	}
	transport := &ptyTransport{master: master, slave: slave, done: make(chan struct{})}
	session, err := NewSession(transport, o.session...)
	if err != nil {
		return nil, err
	}
	ok = true
	return &Console{session: session, tty: slave, master: master}, nil
}

// Session returns the single owner of Console's matching and sending operations.
func (c *Console) Session() *Session { return c.session }

// TTY returns the slave; the master is intentionally not exposed for competing reads.
// When attaching manually, release the parent slave after start to observe EOF.
// Console.Start performs that release automatically.
func (c *Console) TTY() *os.File { return c.tty }

// Close closes the Session and both PTY handles, not any attached process.
func (c *Console) Close() error { return c.session.Close() }

type ptyTransport struct {
	master, slave *os.File
	done          chan struct{}
	once          sync.Once
	err           error
}

func (p *ptyTransport) Read(b []byte) (int, error) {
	n, err := p.master.Read(b)
	return n, p.NormalizeReadError(err)
}
func (p *ptyTransport) Write(b []byte) (int, error) { return p.master.Write(b) }
func (p *ptyTransport) Close() error {
	p.once.Do(func() {
		close(p.done)
		a := p.slave.Close()
		b := p.master.Close()
		if errors.Is(a, os.ErrClosed) {
			a = nil
		}
		if errors.Is(b, os.ErrClosed) {
			b = nil
		}
		p.err = errors.Join(a, b)
	})
	return p.err
}

func (p *ptyTransport) SyscallConn() (syscall.RawConn, error) { return p.master.SyscallConn() }
func (p *ptyTransport) SetWriteDeadline(deadline time.Time) error {
	return p.master.SetWriteDeadline(deadline)
}
func (p *ptyTransport) SetReadDeadline(deadline time.Time) error {
	return p.master.SetReadDeadline(deadline)
}
func (p *ptyTransport) NormalizeReadError(err error) error {
	if errors.Is(err, unix.EIO) {
		return io.EOF
	}
	return err
}

// WithSize sets the initial terminal dimensions. The default is 24 rows by 80 columns.
func WithSize(rows, cols int) ConsoleOption {
	return func(o *consoleOptions) error {
		if rows <= 0 || rows > 65535 || cols <= 0 || cols > 65535 {
			return &ConfigError{Option: "size", Reason: "dimensions must be in 1..65535"}
		}
		o.rows, o.cols = uint16(rows), uint16(cols)
		return nil
	}
}
func setConsoleSize(master *os.File, rows, cols uint16) error {
	raw, err := master.SyscallConn()
	if err != nil {
		return err
	}
	var inner error
	err = raw.Control(func(fd uintptr) {
		inner = unix.IoctlSetWinsize(int(fd), unix.TIOCSWINSZ, &unix.Winsize{Row: rows, Col: cols})
	})
	return errors.Join(err, inner)
}

// SetSize changes the terminal dimensions, notifying its foreground process group.
func (c *Console) SetSize(rows, cols int) error {
	if rows <= 0 || rows > 65535 || cols <= 0 || cols > 65535 {
		return &ConfigError{Option: "size", Reason: "dimensions must be in 1..65535"}
	}
	return setConsoleSize(c.master, uint16(rows), uint16(cols))
}

// SetEcho changes ECHO without changing the terminal's other flags.
func (c *Console) SetEcho(enabled bool) error {
	raw, err := c.master.SyscallConn()
	if err != nil {
		return err
	}
	var inner error
	err = raw.Control(func(fd uintptr) {
		state, e := unix.IoctlGetTermios(int(fd), termiosRead)
		if e != nil {
			inner = e
			return
		}
		if enabled {
			state.Lflag |= unix.ECHO
		} else {
			state.Lflag &^= unix.ECHO
		}
		inner = unix.IoctlSetTermios(int(fd), termiosWrite, state)
	})
	return errors.Join(err, inner)
}

// Start attaches cmd to the PTY as a new session with a controlling terminal.
// The caller owns cancellation and Wait. On success the parent's slave is closed.
func (c *Console) Start(cmd *exec.Cmd) error {
	if cmd == nil {
		return &ConfigError{Option: "command", Reason: "nil command"}
	}
	if cmd.Process != nil {
		return &ConfigError{Option: "command", Reason: "already started"}
	}
	attr := syscall.SysProcAttr{}
	if cmd.SysProcAttr != nil {
		attr = *cmd.SysProcAttr
	}
	if attr.Setpgid || attr.Foreground || attr.Noctty {
		return &ConfigError{Option: "command", Reason: "incompatible terminal attributes"}
	}
	attr.Setsid = true
	attr.Setctty = true
	attr.Ctty = 0
	cmd.SysProcAttr = &attr
	cmd.Stdin, cmd.Stdout, cmd.Stderr = c.tty, c.tty, c.tty
	if err := cmd.Start(); err != nil {
		return err
	}
	// EOF requires that only the child retain the slave. Never turn a successful
	// process start into an error that would leave ownership of Wait ambiguous.
	_ = c.tty.Close()
	return nil
}

// String formats the handle without exposing owned stream or command state.
func (*Console) String() string { return "expect.Console" }

// GoString formats the handle without exposing owned stream or command state.
func (p *Console) GoString() string { return p.String() }
