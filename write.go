// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"errors"
	"fmt"
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

// writeSync falls back only on an explicit unsupported-deadline error.
// The lifecycle counter includes deadline management and the syscall.
func (s *Session) writeSync(ctx context.Context, text string, deadline time.Time) (result writeResult, supported bool, admission error) {
	d, ok := s.transport.(deadlineWriter)
	if !ok {
		return writeResult{}, false, nil
	}
	s.lifeMu.Lock()
	if s.isClosed() {
		s.lifeMu.Unlock()
		return writeResult{}, true, ErrClosed
	}
	if err := writeAdmissionError(ctx, deadline); err != nil {
		s.lifeMu.Unlock()
		return writeResult{}, true, err
	}
	s.workers.Add(1)
	s.lifeMu.Unlock()
	// Release lifecycle references even when a transport or borrowed sink exits.
	completed, cleaned := false, false
	defer func() {
		s.workers.Done()
		if !completed || !cleaned {
			_ = s.Close()
		}
	}()
	if err := d.SetWriteDeadline(deadline); err != nil {
		if onlyDeadlineErrors(err, os.ErrNoDeadline, errors.ErrUnsupported) {
			completed, cleaned = true, true
			return writeResult{}, false, nil
		}
		cleanupErr := s.interrupt()
		completed, cleaned = true, true
		return writeResult{}, true, errors.Join(fmt.Errorf("expect: set write deadline: %w", err), cleanupErr)
	}
	data := []byte(text)
	if err := writeAdmissionError(ctx, deadline); err != nil {
		cleanupErr := s.clearWriteDeadline(d)
		completed, cleaned = true, true
		return writeResult{}, true, errors.Join(err, cleanupErr)
	}
	finishCancellation := s.watchWriteCancellation(ctx, d)
	defer func() {
		if finishCancellation != nil {
			result.err = errors.Join(result.err, finishCancellation())
		}
		result.err = errors.Join(result.err, s.clearWriteDeadline(d))
		cleaned = true
	}()

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

// watchWriteCancellation owns the cancellation callback and returns its join.
// A failed deadline wake must interrupt I/O without waiting for this write.
func (s *Session) watchWriteCancellation(ctx context.Context, d deadlineWriter) func() error {
	if ctx.Done() == nil {
		return nil
	}
	done := make(chan struct{})
	var wakeErr error
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		completed := false
		defer func() {
			if !completed {
				kind := CallbackExited
				if recover() != nil {
					kind = CallbackPanicked
				}
				wakeErr = errors.Join(wakeErr, &CallbackError{Kind: kind})
				wakeErr = errors.Join(wakeErr, s.interrupt())
			}
		}()
		if err := d.SetWriteDeadline(time.Now()); err != nil {
			wakeErr = fmt.Errorf("expect: interrupt write deadline: %w", err)
			wakeErr = errors.Join(wakeErr, s.interrupt())
		}
		completed = true
	})
	return func() error {
		if !stop() {
			<-done
		}
		return wakeErr
	}
}

func (s *Session) clearWriteDeadline(d deadlineWriter) error {
	err := d.SetWriteDeadline(time.Time{})
	if err == nil {
		return nil
	}
	// A peer may close immediately after accepting the write. Its final input
	// must remain drainable; the closed transport has no deadline left to reuse.
	if onlyDeadlineErrors(err, io.ErrClosedPipe, net.ErrClosed, os.ErrClosed) {
		return nil
	}
	return errors.Join(fmt.Errorf("expect: clear write deadline: %w", err), s.interrupt())
}

// Every cause must match an allowed capability or terminal error. A sentinel in
// one branch must not discard another branch's actual deadline failure.
//
//nolint:errorlint // Inspect every unwrap branch; errors.As could hide another failure.
func onlyDeadlineErrors(err error, allowed ...error) bool {
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		matched := false
		for _, cause := range wrapped.Unwrap() {
			if cause == nil {
				continue
			}
			if !onlyDeadlineErrors(cause, allowed...) {
				return false
			}
			matched = true
		}
		return matched
	case interface{ Unwrap() error }:
		return onlyDeadlineErrors(wrapped.Unwrap(), allowed...)
	default:
		for _, target := range allowed {
			if errors.Is(err, target) {
				return true
			}
		}
		return false
	}
}

// writeOnce runs under writeGate and preserves pre-admission cancellation.
func (s *Session) writeOnce(ctx context.Context, text string, deadline time.Time) (n int, err error) {
	if r, ok, admission := s.writeSync(ctx, text, deadline); ok {
		if admission != nil {
			if s.isClosed() {
				admission = errors.Join(admission, s.Close())
			}
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

// Send writes text once. No partial write is retried. A failure after entering
// transport.Write closes the Session and returns its progress in WriteError.
// Deadline setup or restoration failure also closes the Session. An invalid
// transport count is rejected and reported as zero.
func (s *Session) Send(ctx context.Context, text string) (n int, err error) {
	defer func() {
		s.notify(ctx, Event{SessionID: s.id, RuleIndex: -1, Operation: OpSend, CaseIndex: -1, Bytes: n, Class: errorClass(err)})
	}()
	if ctx == nil {
		return 0, &ConfigError{Option: "context", Reason: "nil context"}
	}
	if err = ctx.Err(); err != nil {
		return 0, err
	}
	if s.isClosed() {
		return 0, ErrClosed
	}
	deadline, _ := ctx.Deadline()
	if s.opts.writeTimeout > 0 {
		deadline = earlier(deadline, after(time.Now(), s.opts.writeTimeout))
	}
	if err = s.writeGate.acquire(ctx, s.done, deadline); err != nil {
		return 0, err
	}
	defer s.writeGate.release()
	if len(text) == 0 {
		return 0, nil
	}
	return s.writeOnce(ctx, text, deadline)
}

type writeResult struct {
	n   int
	err error
}

// SendLine appends the configured line ending (LF by default).
func (s *Session) SendLine(ctx context.Context, text string) (int, error) {
	return s.Send(ctx, text+s.opts.lineEnding)
}
