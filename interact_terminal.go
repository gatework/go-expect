//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// InteractTerminal temporarily puts a caller-owned local terminal in raw mode.
// Input is duplicated for cancellation and the original handle remains open.
// The caller must not read input or change its terminal settings during the call.
func (s *Session) InteractTerminal(ctx context.Context, input *os.File, output io.Writer, opts ...InteractOption) (err error) {
	if ctx == nil || input == nil || nilInterface(output) {
		return &ConfigError{Option: "interact terminal", Reason: "context and streams required"}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	o, w, err := s.prepareInteraction(ctx, input, output, opts)
	defer func() { s.notifyInteract(ctx, w, err) }()
	if err != nil {
		return err
	}
	if !s.expecting.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer s.releaseWait()
	if s.isClosed() {
		return ErrClosed
	}
	local, fd, err := duplicateInput(input)
	if err != nil {
		return err
	}
	defer func() {
		if e := local.Close(); !errors.Is(e, os.ErrClosed) {
			err = errors.Join(err, e)
		}
	}()
	// Changing O_NONBLOCK affects duplicated descriptors too. Restore it after
	// the input worker has been interrupted and joined, even on callback panic.
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	state, err := term.MakeRaw(fd)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, term.Restore(fd, state))
		_, e := unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags)
		err = errors.Join(err, e)
	}()
	if o.cooked {
		o.callbackMode = func() (func() error, error) {
			if e := term.Restore(fd, state); e != nil {
				return nil, e
			}
			return func() error { _, e := term.MakeRaw(fd); return e }, nil
		}
	}
	// Rewrap only after nonblocking is set, so Go registers the runtime poller.
	if err = unix.SetNonblock(fd, true); err != nil {
		return err
	}
	duplicate, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return err
	}
	owned := os.NewFile(uintptr(duplicate), input.Name())
	if err = owned.SetReadDeadline(time.Time{}); err != nil {
		_ = owned.Close()
		return err
	}
	defer owned.Close()
	return s.interactOwned(ctx, owned, output, o, w)
}

// duplicateInput duplicates input's descriptor (close-on-exec) so that the
// caller's handle stays open and untouched while the duplicate is closed to
// cancel the input worker.
func duplicateInput(input *os.File) (local *os.File, fd int, err error) {
	raw, err := input.SyscallConn()
	if err != nil {
		return nil, -1, err
	}
	fd = -1
	var inner error
	err = raw.Control(func(original uintptr) { fd, inner = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 0) })
	if err != nil || inner != nil {
		return nil, -1, errors.Join(err, inner)
	}
	return os.NewFile(uintptr(fd), input.Name()), fd, nil
}
