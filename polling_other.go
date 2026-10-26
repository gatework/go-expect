//go:build !linux && !darwin

package expect

import "io"

type osAccess struct{}

func newOSAccess(io.ReadWriteCloser) (*osAccess, error) { return nil, ErrPollUnsupported }
func (*osAccess) writeNow([]byte) (int, error)          { return 0, ErrPollUnsupported }
func (s *Session) osReadLoop()                          { s.workers.Done(); close(s.events) }
