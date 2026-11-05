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
	raw, err := input.SyscallConn()
	if err != nil {
		return err
	}
	fd := -1
	var inner error
	err = raw.Control(func(original uintptr) { fd, inner = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 0) })
	if err != nil || inner != nil {
		return errors.Join(err, inner)
	}
	local := os.NewFile(uintptr(fd), input.Name())
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
