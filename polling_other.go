//go:build !linux && !darwin && !freebsd

// SPDX-License-Identifier: Apache-2.0

package expect

import "io"

type osAccess struct{}

func newOSAccess(io.ReadWriteCloser) (*osAccess, error) { return nil, ErrPollUnsupported }
func (*osAccess) writeNow([]byte) (int, error)          { return 0, ErrPollUnsupported }
func (s *Session) osReadLoop()                          { close(s.mailbox.events) }
func (*Session) requestPoll() (chan struct{}, bool, error) {
	return nil, false, ErrPollUnsupported
}
