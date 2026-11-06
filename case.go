// SPDX-License-Identifier: Apache-2.0

package expect

import "context"

// Action determines whether a matched Case returns or continues waiting.
type Action uint8

// Actions determine whether a handler returns or continues waiting.
const (
	Return Action = iota
	Continue
	ContinueKeepTimer
)

// Handler runs after consumption, outside internal locks. It may send or close,
// but may not call Expect recursively on the same Session.
type Handler func(context.Context, *Session, Result) (Action, error)

// Case associates a pattern with an optional handler (nil means Return).
type Case struct {
	// NoConsume preserves matched input. Continue suppresses this candidate until the buffer changes.
	NoConsume bool
	// Sessions binds a Group case to members; an empty slice selects every member.
	Sessions []*Session
	Pattern  Pattern
	Handler  Handler
	Limit    int
}

func (Case) String() string { return "expect.Case" }

// GoString formats metadata without exposing payloads.
func (c Case) GoString() string { return c.String() }
