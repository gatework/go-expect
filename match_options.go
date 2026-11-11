// SPDX-License-Identifier: Apache-2.0

package expect

import "time"

type matchOptions struct {
	timeout, idle time.Duration
	declaration   bool
	streaming     Handler
	poll          bool
	rules         []Case
}

// MatchOption configures matching or timing for a single Expect operation.
type MatchOption func(*matchOptions) error

// WithTimeout sets the relative timer, including elapsed callback time.
// Zero disables it. Continue can reset it; context deadlines cannot be reset.
func WithTimeout(d time.Duration) MatchOption {
	return func(o *matchOptions) error {
		if d < 0 {
			return &ConfigError{Option: "timeout", Reason: "must be nonnegative"}
		}
		o.timeout = d
		return nil
	}
}

// WithIdleTimeout limits time without new input; zero disables it.
func WithIdleTimeout(d time.Duration) MatchOption {
	return func(o *matchOptions) error {
		if d < 0 {
			return &ConfigError{Option: "idle timeout", Reason: "must be nonnegative"}
		}
		o.idle = d
		return nil
	}
}

// WithStreamingFallback consumes max(1, H/2) bytes when H bytes are pending,
// where H is min(64 KiB, the session buffer limit), and no ordinary case matches. Result.CaseIndex is -1 and Match is the consumed data.
// Continue and ContinueKeepTimer both preserve all existing timers.
func WithStreamingFallback(handler Handler) MatchOption {
	return func(o *matchOptions) error {
		if handler == nil {
			return &ConfigError{Option: "streaming fallback", Reason: "nil handler"}
		}
		o.streaming = handler
		return nil
	}
}

// NoTimeout disables a relative timeout; parent context deadlines still apply.
const NoTimeout time.Duration = 0

// MatchOrder chooses text-case arbitration within a rule group.
type MatchOrder uint8

// Match orders select earliest-byte or declaration-first arbitration.
const (
	Earliest MatchOrder = iota
	Declaration
)

// WithMatchOrder selects earliest-byte or declaration-order matching.
func WithMatchOrder(order MatchOrder) MatchOption {
	return func(o *matchOptions) error {
		if order != Earliest && order != Declaration {
			return &ConfigError{Option: "match order", Reason: "unknown order"}
		}
		o.declaration = order == Declaration
		return nil
	}
}
