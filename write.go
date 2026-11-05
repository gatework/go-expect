package expect

import (
	"context"
	"errors"
	"io"
)

// acquireWrite owns admission only; cancellation before transport entry does not
// retire the session. The caller releases the gate after the complete operation.
func (s *Session) acquireWrite(ctx context.Context) error {
	select {
	case s.writeGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		<-s.writeGate
		return err
	}
	if s.isClosed() {
		<-s.writeGate
		return ErrClosed
	}
	return nil
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
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
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
				err = errors.Join(errors.New("expect: invalid write count"), err)
			}
			total += count
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
		result <- writeResult{total, err}
	}()
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
