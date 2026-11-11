//go:build windows

// SPDX-License-Identifier: Apache-2.0

package spawn

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"unicode/utf16"
	"unsafe"

	"golang.org/x/sys/windows"

	expect "github.com/gatework/go-expect"
)

// This attribute takes a HANDLE value, unlike the pointer-valued attributes
// accepted by x/sys Update. Keep its uintptr conversion at the syscall boundary.
var updateAttribute = windows.NewLazySystemDLL("kernel32.dll").NewProc("UpdateProcThreadAttribute")

// Process owns a Windows job, its child, pseudoconsole and conversation session.
// Close terminates the job and joins the child. Wait returns its exit status.
// Wait requires output to be consumed; Close can interrupt an undrained stream.
type processState struct {
	session   *expect.Session
	transport *transport
	done      chan struct{}
	waitErr   error
	status    ExitStatus
	processID int
	stop      func() bool
	once      sync.Once
	closeErr  error
}

func startProcess(ctx context.Context, program string, args []string, o options) (*processState, error) {
	if o.rows > 32767 || o.cols > 32767 {
		return nil, &expect.ConfigError{Option: "size", Reason: "Windows dimensions must be in 1..32767"}
	}
	if o.raw {
		return nil, ErrUnsupported
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
	var directory *uint16
	if o.dir != "" {
		directory, err = windows.UTF16PtrFromString(o.dir)
		if err != nil {
			return nil, err
		}
	}
	var environment *uint16
	if o.env != nil {
		entries := windowsEnvironment(o.env)
		block := utf16.Encode([]rune(strings.Join(entries, "\x00") + "\x00\x00"))
		environment = &block[0]
	}
	if err = windows.CreateProcess(app, command, nil, nil, false, windows.EXTENDED_STARTUPINFO_PRESENT|windows.CREATE_UNICODE_ENVIRONMENT|windows.CREATE_SUSPENDED, environment, directory, &si.StartupInfo, &pi); err != nil {
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
	p := &processState{session: s, transport: t, done: make(chan struct{}), processID: int(pi.ProcessId)}
	go func() {
		_, waitErr := windows.WaitForSingleObject(pi.Process, windows.INFINITE)
		var code uint32
		statusErr := windows.GetExitCodeProcess(pi.Process, &code)
		_ = windows.CloseHandle(pi.Process)
		p.status = ExitStatus{PID: p.processID, Code: int(code)}
		p.waitErr = errors.Join(waitErr, statusErr)
		if p.waitErr == nil && code != 0 {
			p.waitErr = &ExitError{Status: p.status}
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

func (p *processState) sessionHandle() *expect.Session { return p.session }
func (p *processState) pid() int                       { return p.processID }
func (p *processState) wait() (ExitStatus, error)      { <-p.done; return p.status, p.waitErr }
func (p *processState) close() error {
	p.once.Do(func() { p.stop(); p.closeErr = p.session.Close(); <-p.done })
	return p.closeErr
}
func (p *processState) terminal() Terminal { return windowsTerminal{p} }

type windowsTerminal struct{ p *processState }

func (t windowsTerminal) SetSize(rows, cols int) error { return t.p.setSize(rows, cols) }

// SetSize resizes the active pseudoconsole.
func (p *processState) setSize(rows, cols int) error {
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
			// Closing a kill-on-close job can report exit status zero. Terminate
			// explicitly so interrupted children retain a nonzero exit status.
			c = windows.TerminateJobObject(t.job, 1)
			c = errors.Join(c, windows.CloseHandle(t.job))
		}
		a := t.output.Close()
		b := t.input.Close()
		t.closeConsole()
		t.err = errors.Join(a, b, c)
	})
	return t.err
}
