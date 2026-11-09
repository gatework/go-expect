// SPDX-License-Identifier: Apache-2.0

package expect

import "context"

// Response is a borrowed side-effect callback adapted by Then.
type Response func(context.Context, *Session, Result) error

// Then adapts a response to a timer-preserving continuation handler.
func Then(response Response) Handler {
	return func(ctx context.Context, s *Session, r Result) (Action, error) {
		if response == nil {
			return Return, &ConfigError{Option: "response", Reason: "nil response"}
		}
		return ContinueKeepTimer, response(ctx, s, r)
	}
}

// ReplyThenContinue sends text verbatim and continues without resetting timers.
func ReplyThenContinue(text string) Handler { return Then(Reply(text)) }

// Reply returns a Response that sends text verbatim, including explicit line ends.
func Reply(text string) Response {
	return func(ctx context.Context, s *Session, _ Result) error { _, err := s.Send(ctx, text); return err }
}
