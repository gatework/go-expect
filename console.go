//go:build linux || darwin

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
	"sync"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// Console adapts an owned PTY to Session. It does not start, wait for, signal or
// reap processes. Tty is intended for a caller-owned exec.Cmd or local device.
type Console struct {
	session *Session
	tty     *os.File
}
type consoleOptions struct {
	raw     bool
	session []SessionOption
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
	var o consoleOptions
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("expect: nil console option")
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
	if o.raw {
		if _, err = term.MakeRaw(int(slave.Fd())); err != nil {
			return nil, err
		}
	}
	transport := &ptyTransport{master: master, slave: slave}
	session, err := NewSession(transport, o.session...)
	if err != nil {
		return nil, err
	}
	ok = true
	return &Console{session: session, tty: slave}, nil
}

// Session returns the single owner of Console's matching and sending operations.
func (c *Console) Session() *Session { return c.session }

// Tty returns the slave; the master is intentionally not exposed for competing reads.
// Close the parent's slave after starting a process when waiting for process EOF.
func (c *Console) Tty() *os.File { return c.tty }

// Close closes the Session and both PTY handles, not any attached process.
func (c *Console) Close() error { return c.session.Close() }

type ptyTransport struct {
	master, slave *os.File
	once          sync.Once
	err           error
}

func (p *ptyTransport) Read(b []byte) (int, error) {
	n, err := p.master.Read(b)
	if errors.Is(err, unix.EIO) {
		err = io.EOF
	}
	return n, err
}
func (p *ptyTransport) Write(b []byte) (int, error) { return p.master.Write(b) }
func (p *ptyTransport) Close() error {
	p.once.Do(func() {
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
