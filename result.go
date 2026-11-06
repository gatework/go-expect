// SPDX-License-Identifier: Apache-2.0

package expect

import "fmt"

// Capture distinguishes an unmatched optional group from an empty matched group.
type Capture struct {
	Text       string
	Matched    bool
	Name       string
	Start, End int
}

func (Capture) String() string { return "expect.Capture" }

// GoString formats metadata without exposing payloads.
func (c Capture) GoString() string { return c.String() }

// Result is a detached snapshot of the final match, not the whole conversation.
// Before contains pending input on failure; CaseIndex is -1 without a match.
// Explicit fields contain unredacted bytes; formatting emits only metadata.
type Result struct {
	SessionID     SessionID
	Source        MatchSource
	CaseIndex     int
	RuleIndex     int
	Scope         RuleScope
	Start, End    int
	Before, Match string
	Captures      []Capture
}

func (r Result) String() string { return fmt.Sprintf("expect.Result{case:%d}", r.CaseIndex) }

// GoString formats metadata without exposing payloads.
func (r Result) GoString() string { return r.String() }

// Buffer returns the raw consumed segment (or the no-consume snapshot).
func (r Result) Buffer() string { return r.Before + r.Match }

// Capture returns the first matched named group.
func (r Result) Capture(name string) (Capture, bool) {
	for _, c := range r.Captures {
		if c.Name == name {
			return c, c.Matched
		}
	}
	return Capture{}, false
}

// Indices returns half-open byte offsets within the buffer visible to this match.
// Group zero is the entire match; higher groups index Captures from one.
func (r Result) Indices(group int) (int, int, bool) {
	if group == 0 {
		return r.Start, r.End, r.CaseIndex >= 0 || r.RuleIndex >= 0 || r.End > r.Start
	}
	if group < 1 || group > len(r.Captures) {
		return -1, -1, false
	}
	c := r.Captures[group-1]
	return c.Start, c.End, c.Matched
}
