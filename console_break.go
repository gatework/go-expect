//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

func (p *ptyTransport) SendBreak(ctx context.Context, duration time.Duration) error {
	raw, err := p.master.SyscallConn()
	if err != nil {
		return err
	}
	control := func(request uint) error {
		var inner error
		e := raw.Control(func(fd uintptr) { inner = unix.IoctlSetInt(int(fd), request, 0) })
		return errors.Join(e, inner)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = control(unix.TIOCSBRK); err != nil {
		return err
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		err = ctx.Err()
	case <-p.done:
		err = ErrClosed
	}
	return errors.Join(err, control(unix.TIOCCBRK))
}

// SendBreak asserts a terminal break through the serialized Session write path.
func (c *Console) SendBreak(ctx context.Context, duration time.Duration) error {
	return c.session.SendBreak(ctx, duration)
}
