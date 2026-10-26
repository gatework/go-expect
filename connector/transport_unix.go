//go:build linux || darwin

package connector

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

type processTransport struct {
	master   *os.File
	cmd      *exec.Cmd
	done     chan struct{}
	once     sync.Once
	closeErr error
}

func (p *processTransport) Read(b []byte) (int, error) {
	n, err := p.master.Read(b)
	if errors.Is(err, unix.EIO) {
		err = io.EOF
	}
	return n, err
}
func (p *processTransport) Write(b []byte) (int, error) { return p.master.Write(b) }
func (p *processTransport) Close() error {
	p.once.Do(func() {
		p.closeErr = p.master.Close()
		// Kill only the owned process through os.Process, which guards already reaped
		// children. Never signal a potentially reused process-group identifier.
		if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			p.closeErr = errors.Join(p.closeErr, err)
		}
		<-p.done
	})
	return p.closeErr
}

func startProcess(ctx context.Context, cmd *exec.Cmd) (*processTransport, error) {
	if ctx == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	master, slave, err := pty.Open()
	if err != nil {
		return nil, err
	}
	defer slave.Close()
	owned := false
	defer func() {
		if !owned {
			master.Close()
		}
	}()
	if _, err = term.MakeRaw(int(slave.Fd())); err != nil {
		return nil, err
	}
	if err = pty.Setsize(slave, &pty.Winsize{Rows: 24, Cols: 160}); err != nil {
		return nil, err
	}
	fd, err := unix.FcntlInt(master.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err = unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return nil, err
	}
	old := master
	master = os.NewFile(uintptr(fd), old.Name())
	old.Close()
	if err = master.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	p := &processTransport{master: master, cmd: cmd, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	owned = true
	if err = ctx.Err(); err != nil {
		return nil, errors.Join(err, p.Close())
	}
	return p, nil
}

func openTransport(ctx context.Context, cfg Config) (io.ReadWriteCloser, error) {
	program, args := transportCommand(cfg)
	return startProcess(ctx, exec.Command(program, args...))
}

func transportCommand(cfg Config) (string, []string) {
	port := cfg.Port
	if cfg.Protocol == "telnet" {
		if port == 0 {
			port = 23
		}
		program := cfg.TelnetPath
		if program == "" {
			program = "telnet"
		}
		return program, []string{cfg.Host, strconv.Itoa(port)}
	}
	if port == 0 {
		port = 22
	}
	program := cfg.SSHPath
	if program == "" {
		program = "ssh"
	}
	seconds := int64(cfg.LoginTimeout / time.Second)
	if cfg.LoginTimeout%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		seconds = 1
	}
	args := []string{"-F", "none", "-tt", "-e", "none", "-o", "StrictHostKeyChecking=yes", "-o", "UpdateHostKeys=no", "-o", "NumberOfPasswordPrompts=1", "-o", "ConnectionAttempts=1", "-o", "ClearAllForwardings=yes", "-o", "PermitLocalCommand=no", "-o", "ConnectTimeout=" + strconv.FormatInt(seconds, 10), "-p", strconv.Itoa(port)}
	if cfg.KnownHosts != "" {
		args = append(args, "-o", "UserKnownHostsFile="+quoteSSHPath(cfg.KnownHosts))
	}
	if cfg.IdentityFile != "" {
		args = append(args, "-i", cfg.IdentityFile)
	}
	if cfg.Username != "" {
		args = append(args, "-l", cfg.Username)
	}
	return program, append(args, "--", cfg.Host)
}
func quoteSSHPath(path string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(path) + `"`
}
