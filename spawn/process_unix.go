//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

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

// Console provides the Unix Terminal and optional echo capability.
var (
	_ Terminal       = (*expect.Console)(nil)
	_ EchoController = (*expect.Console)(nil)
)

// Process owns a child and its PTY. Do not copy it. Wait and Close are concurrent safe.
// Close cancels the child and joins Wait; exit status is reported by Wait.
type processState struct {
	console  *expect.Console
	cmd      *exec.Cmd
	cancel   context.CancelFunc
	done     chan struct{}
	waitErr  error
	status   ExitStatus
	once     sync.Once
	closeErr error
}

func startProcess(ctx context.Context, program string, args []string, o options) (*processState, error) {
	consoleOpts := []expect.ConsoleOption{expect.WithSize(o.rows, o.cols), expect.WithSessionOptions(o.session...)}
	if o.raw {
		consoleOpts = append(consoleOpts, expect.WithRaw())
	}
	c, err := expect.NewConsole(consoleOpts...)
	if err != nil {
		return nil, err
	}
	childCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(childCtx, program, args...)
	cmd.Env = o.env
	cmd.Dir = o.dir
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
	p := &processState{console: c, cmd: cmd, cancel: cancel, done: make(chan struct{})}
	go func() {
		cause := cmd.Wait()
		p.status = ExitStatus{PID: cmd.Process.Pid, Code: cmd.ProcessState.ExitCode()}
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			p.status.Signal = status.Signal().String()
		}
		if cause != nil {
			p.waitErr = &ExitError{Status: p.status, Cause: cause}
		}
		cancel()
		close(p.done)
	}()
	return p, nil
}

func (p *processState) sessionHandle() *expect.Session { return p.console.Session() }
func (p *processState) pid() int                       { return p.cmd.Process.Pid }
func (p *processState) wait() (ExitStatus, error)      { <-p.done; return p.status, p.waitErr }
func (p *processState) close() error {
	p.once.Do(func() { p.cancel(); p.closeErr = p.console.Close(); <-p.done })
	return p.closeErr
}
func (p *processState) terminal() Terminal { return p.console }
