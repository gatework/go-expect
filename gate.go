// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"context"
	"time"
)

// gate is a cancellable lock; ownership spans a complete protocol operation.
type gate chan struct{}

func (g gate) acquire(ctx context.Context, done <-chan struct{}, deadline time.Time) error {
	select {
	case g <- struct{}{}:
	default:
		var wake <-chan time.Time
		if !deadline.IsZero() {
			timer := time.NewTimer(time.Until(deadline))
			defer timer.Stop()
			wake = timer.C
		}
		select {
		case g <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return ErrClosed
		case <-wake:
			return context.DeadlineExceeded
		}
	}
	if err := ctx.Err(); err != nil {
		g.release()
		return err
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		g.release()
		return context.DeadlineExceeded
	}
	select {
	case <-done:
		g.release()
		return ErrClosed
	default:
		return nil
	}
}
func (g gate) release() { <-g }
