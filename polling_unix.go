//go:build linux || darwin

package expect

import (
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type osAccess struct {
	raw       syscall.RawConn
	normalize func(error) error
	deadline  func(time.Time) error
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
	if p, ok := transport.(interface{ NormalizeReadError(error) error }); ok {
		a.normalize = p.NormalizeReadError
	}
	// Choose once, before ownership transfer. A transport that cannot wake the
	// runtime reader with a deadline retains the bounded native-poll backend.
	if p, ok := transport.(interface{ SetReadDeadline(time.Time) error }); ok {
		if err := p.SetReadDeadline(time.Time{}); err == nil {
			a.deadline = p.SetReadDeadline
		}
	}
	return a, nil
}
func (a *osAccess) read(wait int, scratch *[]byte) ([]byte, error, bool) {
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
		data, err, ready = a.readReady(fd, scratch)
	})
	if control != nil {
		return nil, control, true
	}
	return data, err, ready
}

// readReady is used only inside a RawConn callback, so the descriptor cannot be
// closed and reused during the syscall. EAGAIN gives ownership back to the
// runtime poller; no sleeping syscall or extra descriptor is required.
func (a *osAccess) readReady(fd uintptr, scratch *[]byte) ([]byte, error, bool) {
	if *scratch == nil {
		*scratch = make([]byte, readChunk)
	}
	var n int
	var err error
	for {
		n, err = unix.Read(int(fd), *scratch)
		if err != unix.EINTR {
			break
		}
	}
	if err == unix.EAGAIN || err == unix.EWOULDBLOCK {
		return nil, nil, false
	}
	var data []byte
	if n > 0 && n < readChunk/2 {
		data = make([]byte, n)
		copy(data, (*scratch)[:n])
	} else if n > 0 {
		data = (*scratch)[:n]
		*scratch = nil
	}
	if n == 0 && err == nil {
		err = io.EOF
	}
	// Only read errors are normalized. RawConn and readiness/control failures
	// must retain their original meaning, including an ordinary transport's EIO.
	if a.normalize != nil {
		err = a.normalize(err)
	}
	return data, err, true
}

func (a *osAccess) readWait(scratch *[]byte) ([]byte, error, bool) {
	if a.deadline == nil {
		return a.read(10, scratch)
	}
	var data []byte
	var err error
	ready := false
	rawErr := a.raw.Read(func(fd uintptr) bool {
		// Cold idle sessions should not retain a 16 KiB scratch allocation.
		// This zero-time readiness check occurs only on runtime wakes; once a
		// scratch buffer exists, Read directly supplies EAGAIN to the poller.
		if *scratch == nil {
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			var count int
			for {
				count, err = unix.Poll(fds, 0)
				if err != unix.EINTR {
					break
				}
			}
			if err != nil {
				ready = true
				return true
			}
			if count == 0 {
				return false
			}
		}
		data, err, ready = a.readReady(fd, scratch)
		return ready
	})
	if rawErr != nil {
		return nil, rawErr, true
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
	defer s.stopPoll()
	// Only this pump touches scratch; compact events never borrow its storage.
	var scratch []byte
	for {
		if s.isClosed() {
			return
		}
		s.pollMu.Lock()
		request := s.pollRequest
		var wakeErr error
		if request != nil && s.osIO.deadline != nil {
			// Publication and deadline wake use the same mutex. The request
			// remains owned until its immediate read and event publication finish.
			wakeErr = s.osIO.deadline(time.Time{})
		}
		s.pollMu.Unlock()
		var data []byte
		var err error
		var ready bool
		switch {
		case wakeErr != nil:
			err, ready = wakeErr, true
		case request != nil:
			data, err, ready = s.osIO.read(0, &scratch)
		default:
			data, err, ready = s.osIO.readWait(&scratch)
			if s.osIO.deadline != nil && errors.Is(err, os.ErrDeadlineExceeded) {
				// The request that interrupted this wait is processed on the
				// next turn. Any earlier read is always published first.
				continue
			}
		}
		if ready && !s.publishRead(readEvent{data: data, err: err}) {
			return
		}
		if request != nil {
			s.finishPoll()
		}
		if err != nil {
			return
		}
	}
}

// requestPoll publishes at most one request. Cancellation relinquishes only the
// caller's wait: the pump keeps the request and wake deadline until its ack.
func (s *Session) requestPoll() (chan struct{}, bool, error) {
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	if s.pollStopped {
		return nil, false, nil
	}
	if s.pollRequest != nil {
		return s.pollRequest.ack, false, nil
	}
	request := &pollRequest{ack: make(chan struct{})}
	s.pollRequest = request
	if s.osIO.deadline != nil {
		if err := s.osIO.deadline(time.Now()); err != nil {
			return request.ack, true, err
		}
	}
	return request.ack, true, nil
}

func (s *Session) finishPoll() {
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	s.acknowledgePoll()
}

func (s *Session) stopPoll() {
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	s.pollStopped = true
	s.acknowledgePoll()
}

func (s *Session) acknowledgePoll() {
	if s.pollRequest != nil {
		close(s.pollRequest.ack)
		s.pollRequest = nil
	}
}
