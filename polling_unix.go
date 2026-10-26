//go:build linux || darwin

package expect

import (
	"errors"
	"io"
	"syscall"

	"golang.org/x/sys/unix"
)

type osAccess struct {
	raw       syscall.RawConn
	normalize func(error) error
}

func newOSAccess(transport io.ReadWriteCloser) (*osAccess, error) {
	conn, ok := transport.(syscall.Conn)
	if !ok {
		return nil, ErrPollUnsupported
	}
	raw, e := conn.SyscallConn()
	if e != nil {
		return nil, errors.Join(ErrPollUnsupported, e)
	}
	var inner error
	e = raw.Control(func(fd uintptr) { inner = unix.SetNonblock(int(fd), true) })
	if e != nil || inner != nil {
		return nil, errors.Join(ErrPollUnsupported, e, inner)
	}
	a := &osAccess{raw: raw}
	if p, ok := transport.(*ptyTransport); ok {
		a.normalize = p.normalizeReadError
	}
	return a, nil
}
func (a *osAccess) read(wait int) ([]byte, error, bool) {
	var data []byte
	var err error
	ready := false
	control := a.raw.Control(func(fd uintptr) {
		poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		n, e := unix.Poll(poll, wait)
		if e == unix.EINTR {
			return
		}
		if e != nil {
			err = e
			ready = true
			return
		}
		if n == 0 {
			return
		}
		data = make([]byte, readChunk)
		n, e = unix.Read(int(fd), data)
		if e == unix.EAGAIN || e == unix.EWOULDBLOCK || e == unix.EINTR {
			data = nil
			return
		}
		ready = true
		if n < 0 {
			n = 0
		}
		// A tiny read must transfer tiny storage, not retain the entire scratch
		// allocation in a stalled sender's receive queue.
		if n == 0 {
			data = nil
		} else if n < readChunk/2 {
			small := make([]byte, n)
			copy(small, data[:n])
			data = small
		} else {
			data = data[:n]
		}
		err = e
		if n == 0 && err == nil {
			err = io.EOF
		}
	})
	if control != nil {
		return nil, control, true
	}
	if a.normalize != nil {
		err = a.normalize(err)
	}
	return data, err, ready
}
func (a *osAccess) writeNow(data []byte) (int, error) {
	n := 0
	var err error
	control := a.raw.Control(func(fd uintptr) {
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
		count, e := unix.Poll(fds, 0)
		if e != nil {
			err = e
			return
		}
		if count == 0 {
			err = ErrTimeout
			return
		}
		n, err = unix.Write(int(fd), data)
		if n < 0 {
			n = 0
		}
		if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
			err = ErrTimeout
		}
	})
	return n, errors.Join(err, control)
}

func (s *Session) osReadLoop() {
	defer s.workers.Done()
	defer close(s.events)
	for {
		var ack chan struct{}
		select {
		case <-s.done:
			return
		case ack = <-s.pollRequests:
		default:
		}
		wait := 10
		if ack != nil {
			wait = 0
		}
		data, err, ready := s.osIO.read(wait)
		if ready && !s.publishRead(readEvent{data: data, err: err}) {
			if ack != nil {
				close(ack)
			}
			return
		}
		if ack != nil {
			close(ack)
		}
		if err != nil {
			return
		}
	}
}
