//go:build windows

// SPDX-License-Identifier: Apache-2.0

// Package conpty runs Windows console programs through an owned pseudoconsole.
// Windows 10 1809 or newer is required. Streams contain UTF-8 and VT sequences.
package conpty

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"unsafe"

	expect "github.com/gatework/go-expect"
	"golang.org/x/sys/windows"
)

// This attribute takes a HANDLE value, unlike the pointer-valued attributes
// accepted by x/sys Update. Keep its uintptr conversion at the syscall boundary.
var updateAttribute = windows.NewLazySystemDLL("kernel32.dll").NewProc("UpdateProcThreadAttribute")

// Option configures a process before any child is started.
type Option func(*options) error
type options struct {
	rows, cols int
	session    []expect.SessionOption
}

// WithSize sets terminal rows and columns; both default to 24 by 80.
func WithSize(rows, cols int) Option {
	return func(o *options) error {
		if rows < 1 || rows > 32767 || cols < 1 || cols > 32767 {
			return &expect.ConfigError{Option: "size", Reason: "dimensions must be in 1..32767"}
		}
		o.rows, o.cols = rows, cols
		return nil
	}
}

// WithSessionOptions configures the conversation session.
func WithSessionOptions(opts ...expect.SessionOption) Option {
	opts = append([]expect.SessionOption(nil), opts...)
	return func(o *options) error { o.session = append(o.session, opts...); return nil }
}

// ExitError reports a nonzero child exit code without retaining command text.
type ExitError struct{ Code uint32 }

func (e *ExitError) Error() string { return fmt.Sprintf("conpty: process exited (code=%d)", e.Code) }

// Process owns a Windows job, its child, pseudoconsole and conversation session.
// Close terminates the job and joins the child. Wait returns its exit status.
// Wait requires output to be consumed; Close can interrupt an undrained stream.
type Process struct {
	session   *expect.Session
	transport *transport
	done      chan struct{}
	waitErr   error
	stop      func() bool
	once      sync.Once
	closeErr  error
}

// Start runs program and literal arguments with a controlling pseudoconsole.
// Child creation is suspended until it belongs to the kill-on-close Windows job.
func Start(ctx context.Context, program string, args []string, opts ...Option) (*Process, error) {
	if ctx == nil {
		return nil, &expect.ConfigError{Option: "context", Reason: "nil context"}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o := options{rows: 24, cols: 80}
	for _, opt := range opts {
		if opt == nil {
			return nil, &expect.ConfigError{Option: "conpty", Reason: "nil option"}
		}
		if err := opt(&o); err != nil {
			return nil, err
		}
	}
	executable, err := exec.LookPath(program)
	if err != nil {
		return nil, err
	}
	app, err := windows.UTF16PtrFromString(executable)
	if err != nil {
		return nil, err
	}
	command, err := windows.UTF16PtrFromString(windows.ComposeCommandLine(append([]string{executable}, args...)))
	if err != nil {
		return nil, err
	}
	t, err := newTransport(o.rows, o.cols)
	if err != nil {
		return nil, err
	}
	s, err := expect.NewSession(t, o.session...)
	if err != nil {
		return nil, errors.Join(err, t.Close())
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.Close()
		}
	}()
	attributes, err := windows.NewProcThreadAttributeList(1)
	if err != nil {
		return nil, err
	}
	defer attributes.Delete()
	// PSEUDOCONSOLE expects the handle value itself, rather than its address.
	if result, _, callErr := updateAttribute.Call(uintptr(unsafe.Pointer(attributes.List())), 0, windows.PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, uintptr(t.console), unsafe.Sizeof(t.console), 0, 0); result == 0 {
		return nil, callErr
	}
	// Null standard handles with STARTF_USESTDHANDLES request console handles,
	// rather than inheriting a host's redirected pipes (for example go test/CI).
	si := windows.StartupInfoEx{StartupInfo: windows.StartupInfo{Cb: uint32(unsafe.Sizeof(windows.StartupInfoEx{})), Flags: windows.STARTF_USESTDHANDLES}, ProcThreadAttributeList: attributes.List()}
	var pi windows.ProcessInformation
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = windows.CreateProcess(app, command, nil, nil, false, windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_UNICODE_ENVIRONMENT|windows.CREATE_SUSPENDED, nil, nil, &si.StartupInfo, &pi); err != nil {
		return nil, err
	}
	defer windows.CloseHandle(pi.Thread)
	if err = windows.AssignProcessToJobObject(t.job, pi.Process); err != nil {
		_ = windows.TerminateProcess(pi.Process, 1)
		_, _ = windows.WaitForSingleObject(pi.Process, windows.INFINITE)
		_ = windows.CloseHandle(pi.Process)
		return nil, err
	}
	if _, err = windows.ResumeThread(pi.Thread); err != nil {
		_ = windows.TerminateProcess(pi.Process, 1)
		_, _ = windows.WaitForSingleObject(pi.Process, windows.INFINITE)
		_ = windows.CloseHandle(pi.Process)
		return nil, err
	}
	p := &Process{session: s, transport: t, done: make(chan struct{})}
	go func() {
		_, waitErr := windows.WaitForSingleObject(pi.Process, windows.INFINITE)
		var code uint32
		statusErr := windows.GetExitCodeProcess(pi.Process, &code)
		_ = windows.CloseHandle(pi.Process)
		p.waitErr = errors.Join(waitErr, statusErr)
		if p.waitErr == nil && code != 0 {
			p.waitErr = &ExitError{Code: code}
		}
		// Normal teardown drains the final frame. Explicit Close closes the pipes
		// first, so a full receive queue cannot deadlock process cancellation.
		t.closeConsole()
		close(p.done)
	}()
	p.stop = context.AfterFunc(ctx, func() { _ = s.Close() })
	ok = true
	return p, nil
}

// Session returns the owned conversation session.
func (p *Process) Session() *expect.Session { return p.session }

// Wait joins the child and pseudoconsole teardown.
func (p *Process) Wait() error { <-p.done; return p.waitErr }

// Close terminates every process in the job, closes I/O and joins teardown.
func (p *Process) Close() error {
	p.once.Do(func() { p.stop(); p.closeErr = p.session.Close(); <-p.done })
	return p.closeErr
}

// SetSize resizes the active pseudoconsole.
func (p *Process) SetSize(rows, cols int) error {
	if rows < 1 || rows > 32767 || cols < 1 || cols > 32767 {
		return &expect.ConfigError{Option: "size", Reason: "dimensions must be in 1..32767"}
	}
	p.transport.mu.Lock()
	defer p.transport.mu.Unlock()
	if p.transport.console == 0 {
		return expect.ErrClosed
	}
	return windows.ResizePseudoConsole(p.transport.console, windows.Coord{X: int16(cols), Y: int16(rows)})
}

type transport struct {
	input, output     *os.File
	console, job      windows.Handle
	mu                sync.Mutex
	consoleOnce, once sync.Once
	err               error
}

func newTransport(rows, cols int) (*transport, error) {
	inRead, inWrite, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	outRead, outWrite, err := os.Pipe()
	if err != nil {
		_ = inRead.Close()
		_ = inWrite.Close()
		return nil, err
	}
	defer inRead.Close()
	defer outWrite.Close()
	t := &transport{input: inWrite, output: outRead}
	// These ends are used synchronously by ConPTY and never read/written by Go.
	if err = windows.CreatePseudoConsole(windows.Coord{X: int16(cols), Y: int16(rows)}, windows.Handle(inRead.Fd()), windows.Handle(outWrite.Fd()), 0, &t.console); err != nil {
		_ = t.Close()
		return nil, err
	}
	t.job, err = windows.CreateJobObject(nil, nil)
	if err != nil {
		_ = t.Close()
		return nil, err
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(t.job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		_ = t.Close()
		return nil, err
	}
	return t, nil
}
func (t *transport) Read(b []byte) (int, error)  { return t.output.Read(b) }
func (t *transport) Write(b []byte) (int, error) { return t.input.Write(b) }
func (t *transport) closeConsole() {
	t.consoleOnce.Do(func() {
		t.mu.Lock()
		h := t.console
		t.console = 0
		t.mu.Unlock()
		if h != 0 {
			windows.ClosePseudoConsole(h)
		}
	})
}
func (t *transport) Close() error {
	t.once.Do(func() {
		// Close the output pipe before ClosePseudoConsole, including on older Windows.
		var c error
		if t.job != 0 {
			c = windows.CloseHandle(t.job)
		}
		a := t.output.Close()
		b := t.input.Close()
		t.closeConsole()
		t.err = errors.Join(a, b, c)
	})
	return t.err
}

// String formats the handle without exposing owned stream or command state.
func (*Process) String() string { return "conpty.Process" }

// GoString formats the handle without exposing owned stream or command state.
func (p *Process) GoString() string { return p.String() }
