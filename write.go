// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"time"
)

var errInvalidWriteCount = errors.New("expect: invalid write count")

// acquireWrite owns admission only; cancellation before transport entry does not
// retire the session. The caller releases the gate after the complete operation.
func (s *Session) acquireWrite(ctx context.Context) error {
	return s.writeGate.acquire(ctx, s.done, time.Time{})
}

type writeMode uint8

const (
	writeOnce writeMode = iota
	writeAll
	writeAllReady
)

func (s *Session) startWrite(ctx context.Context, text string, mode writeMode) (<-chan writeResult, error) {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.isClosed() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(chan writeResult, 1)
	s.workers.Go(func() {
		if err := ctx.Err(); err != nil {
			result <- writeResult{err: err}
			return
		}
		data := []byte(text)
		// Conversion can be expensive. Check again immediately before admission.
		if err := ctx.Err(); err != nil {
			result <- writeResult{err: err}
			return
		}
		// The caller consumes this admission message before observing completion.
		result <- writeResult{}
		total := 0
		var err error
		completed := false
		defer func() {
			if !completed {
				kind := CallbackExited
				if recover() != nil {
					kind = CallbackPanicked
				}
				result <- writeResult{total, errors.Join(err, &CallbackError{Kind: kind})}
			}
		}()
		for total < len(data) {
			if mode != writeOnce && total > 0 {
				if err = ctx.Err(); err != nil {
					break
				}
			}
			var count int
			if mode == writeAllReady {
				count, err = s.osIO.writeNow(data[total:])
			} else {
				count, err = s.transport.Write(data[total:])
			}
			if count < 0 || count > len(data)-total {
				count = 0
				err = errors.Join(errInvalidWriteCount, err)
			}
			prior := total
			total += count
			if count > 0 {
				err = errors.Join(err, s.emitSent(data[prior:total], false))
			}
			if mode == writeOnce {
				if err == nil && total != len(data) {
					err = io.ErrShortWrite
				}
				break
			}
			if err != nil {
				break
			}
			if count == 0 {
				err = io.ErrNoProgress
				break
			}
		}
		completed = true
		result <- writeResult{total, err}
	})
	if admission := <-result; admission.err != nil {
		return nil, admission.err
	}
	return result, nil
}

// A ready write result must not randomly defeat an already-ready cancellation.
func (s *Session) finishWrite(ctx context.Context, r writeResult) (int, error) {
	err := r.err
	if stop := ctx.Err(); stop != nil {
		err = errors.Join(stop, err)
	} else if s.isClosed() {
		err = errors.Join(ErrClosed, err)
	}
	if err != nil {
		return r.n, &WriteError{Written: r.n, Cause: errors.Join(err, s.Close())}
	}
	return r.n, nil
}

// abortWrite retires the transport, joins the admitted write and preserves all causes.
func (s *Session) abortWrite(results <-chan writeResult, cause error) (int, error) {
	cleanup := s.Close()
	r := <-results
	return r.n, &WriteError{Written: r.n, Cause: errors.Join(cause, r.err, cleanup)}
}

// deadlineWriter can interrupt a synchronous write without an owned worker.
type deadlineWriter interface{ SetWriteDeadline(time.Time) error }

// writeSync falls back before transport entry if deadlines are unsupported.
// The lifecycle counter includes the syscall, so Close still joins all I/O.
func (s *Session) writeSync(ctx context.Context, text string, deadline time.Time) (result writeResult, supported bool, admission error) {
	d, ok := s.transport.(deadlineWriter)
	if !ok {
		return writeResult{}, false, nil
	}
	if err := d.SetWriteDeadline(deadline); err != nil {
		return writeResult{}, false, nil
	}
	data := []byte(text)
	s.lifeMu.Lock()
	if s.isClosed() {
		s.lifeMu.Unlock()
		return writeResult{}, true, ErrClosed
	}
	if err := writeAdmissionError(ctx, deadline); err != nil {
		s.lifeMu.Unlock()
		_ = d.SetWriteDeadline(time.Time{})
		return writeResult{}, true, err
	}
	s.workers.Add(1)
	s.lifeMu.Unlock()
	// Release lifecycle references even when a borrowed sink panics or exits.
	var canceled chan struct{}
	var stop func() bool
	completed := false
	defer func() {
		if stop != nil && !stop() {
			<-canceled
		}
		resetErr := d.SetWriteDeadline(time.Time{})
		if errors.Is(resetErr, io.ErrClosedPipe) || errors.Is(resetErr, net.ErrClosed) || errors.Is(resetErr, os.ErrClosed) {
			resetErr = nil
		}
		s.workers.Done()
		result.err = errors.Join(result.err, resetErr)
		if !completed {
			_ = s.Close()
		}
	}()
	if ctx.Done() != nil {
		done := make(chan struct{})
		canceled = done
		stop = context.AfterFunc(ctx, func() { _ = d.SetWriteDeadline(time.Now()); close(done) })
	}

	n, err := s.transport.Write(data)
	if n < 0 || n > len(data) {
		n = 0
		err = errors.Join(errInvalidWriteCount, err)
	}
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if n > 0 {
		err = errors.Join(err, s.emitSent(data[:n], false))
	}

	if cause := ctx.Err(); cause != nil {
		err = errors.Join(cause, err)
	} else if !deadline.IsZero() && !time.Now().Before(deadline) {
		err = errors.Join(context.DeadlineExceeded, err)
	}
	completed = true
	return writeResult{n, err}, true, nil
}

// writeOnce runs under writeGate and preserves pre-admission cancellation.
func (s *Session) writeOnce(ctx context.Context, text string, deadline time.Time) (n int, err error) {
	if r, ok, admission := s.writeSync(ctx, text, deadline); ok {
		if admission != nil {
			return 0, admission
		}
		return s.finishWrite(ctx, r)
	}
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	result, err := s.startWrite(ctx, text, writeOnce)
	if err != nil {
		return 0, err
	}
	select {
	case resultValue := <-result:
		n, err = resultValue.n, resultValue.err
	case <-ctx.Done():
		return s.abortWrite(result, ctx.Err())
	case <-s.done:
		return s.abortWrite(result, ErrClosed)
	}
	return s.finishWrite(ctx, writeResult{n, err})
}

func writeAdmissionError(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
