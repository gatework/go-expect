// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"io"
)

const readChunk = 16 << 10

var errInvalidReadCount = errors.New("expect: invalid read count")

type readEvent struct {
	data []byte
	err  error
}

// receiveState is accessed only by the exclusive wait owner. version changes
// on every append or consumption; consumed changes only when a prefix is
// consumed, so scan progress recorded against it survives appended input.
type receiveState struct {
	version, consumed         uint64
	pending, staged           []byte
	pendingAlloc, stagedAlloc int
	stagedErr, terminal       error
}

func (s *Session) readLoop() {
	if s.osIO != nil {
		s.osReadLoop()
		return
	}
	defer close(s.mailbox.events)
	empty := 0
	data := make([]byte, readChunk)
	for {
		n, err := s.transport.Read(data)
		if n < 0 || n > len(data) {
			n = 0
			err = errors.Join(errInvalidReadCount, err)
		}
		if n == 0 && err == nil {
			empty++
			if empty < 100 {
				select {
				case <-s.done:
					return
				default:
				}
				continue
			}
			err = io.ErrNoProgress
		} else {
			empty = 0
		}
		// Small terminal reads must not transfer an entire 16 KiB allocation.
		// Copy only their bytes, retaining the scratch block for the next Read.
		// Large reads transfer ownership and receive a fresh block below.
		var received []byte
		if n > 0 && n < readChunk/2 {
			received = make([]byte, n)
			copy(received, data[:n])
		} else if n > 0 {
			received = data[:n]
		}
		if !s.publishRead(readEvent{data: received, err: err}) {
			return
		}
		if err != nil {
			return
		}
		if n >= readChunk/2 {
			data = make([]byte, readChunk)
		}
	}
}

// publishRead is called only by the sole pump. The notification can collapse:
// receivers always inspect the event channel under inputGate before waiting.
func (s *Session) publishRead(event readEvent) bool {
	if s.opts.removeNulls || s.opts.stripParity {
		n := 0
		for _, b := range event.data {
			if s.opts.stripParity {
				b &= 0x7f
			}
			if s.opts.removeNulls && b == 0 {
				continue
			}
			event.data[n] = b
			n++
		}
		event.data = event.data[:n]
		if n == 0 && event.err == nil {
			return !s.isClosed()
		}
	}

	select {
	case s.mailbox.events <- event:
		s.mailbox.signalInput()
		return true
	case <-s.done:
		return false
	}
}
