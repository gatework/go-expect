// SPDX-License-Identifier: Apache-2.0

package expect

import "context"

// RuleScope identifies a persistent or call-scoped rule group.
type RuleScope string

// Scopes identify the persistent or per-call rule group.
const (
	ScopeBefore RuleScope = "before"
	ScopeCall   RuleScope = "call"
	ScopeAfter  RuleScope = "after"
)

// RuleSnapshot is a detached declaration snapshot; callback closures remain borrowed.
type RuleSnapshot struct{ Before, After []Case }

// WithBefore installs session rules that precede call cases.
func WithBefore(cases ...Case) SessionOption { return sessionRules(true, cases) }

// WithAfter installs session rules consulted after call cases.
func WithAfter(cases ...Case) SessionOption { return sessionRules(false, cases) }
func sessionRules(before bool, cases []Case) SessionOption {
	snapshot := cloneCases(cases)
	return func(o *sessionOptions) error {
		if err := validateCases(snapshot); err != nil {
			return err
		}
		if before {
			o.before = append(o.before, snapshot...)
		} else {
			o.after = append(o.after, snapshot...)
		}
		return nil
	}
}

// WithRules activates reusable cases alongside foreground cases. Foreground
// cases win equal-position ties. A zero rule limit means one response per wait.
func WithRules(cases ...Case) ExpectOption {
	snapshot := cloneCases(cases)
	return func(o *expectOptions) error {
		if err := validateCases(snapshot); err != nil {
			return err
		}
		o.rules = append(o.rules, snapshot...)
		return nil
	}
}
func validateCases(cases []Case) error {
	for _, c := range cases {
		if err := c.Pattern.validate(false); err != nil {
			return err
		}
		if c.Limit < 0 {
			return &ConfigError{Option: "case", Reason: "negative limit"}
		}
	}
	return nil
}

// Rules returns the configured persistent declarations without exposing storage.
func (s *Session) Rules() RuleSnapshot {
	p := s.ruleState.Load()
	if p == nil {
		return RuleSnapshot{}
	}
	return cloneRules(p.RuleSnapshot)
}

type rulePriority uint8

const (
	beforePriority rulePriority = iota
	callPriority
	afterPriority
)

type caseOrigin struct {
	index, rule int
	group       rulePriority
	scope       RuleScope
}

func prepareCases(foreground, before, rules, after []Case) ([]Case, []caseOrigin) {
	if len(before)+len(rules)+len(after) == 0 {
		return foreground, nil
	}
	cases := make([]Case, 0, len(foreground)+len(before)+len(rules)+len(after))
	origins := make([]caseOrigin, 0, cap(cases))
	add := func(items []Case, scope RuleScope, group rulePriority, isRule bool) {
		for i, c := range items {
			origin := caseOrigin{index: i, rule: -1, group: group, scope: scope}
			if isRule {
				origin.index = -1
				origin.rule = i
			}
			cases = append(cases, c)
			origins = append(origins, origin)
		}
	}
	add(before, ScopeBefore, beforePriority, true)
	add(foreground, ScopeCall, callPriority, false)
	add(rules, ScopeCall, callPriority, true)
	add(after, ScopeAfter, afterPriority, true)
	return cases, origins
}
func eventCase(cases []Case, kind patternKind) int {
	for i, c := range cases {
		if c.Pattern.kind == kind || (c.Pattern.kind == defaultPattern && (kind == timeoutPattern || kind == eofPattern)) {
			return i
		}
	}
	return -1
}

type ruleProgram struct {
	RuleSnapshot
	version uint64
}

func cloneCases(cases []Case) []Case {
	out := append([]Case(nil), cases...)
	for i := range out {
		out[i].Sessions = append([]*Session(nil), out[i].Sessions...)
	}
	return out
}
func cloneRules(r RuleSnapshot) RuleSnapshot {
	return RuleSnapshot{Before: cloneCases(r.Before), After: cloneCases(r.After)}
}

// SetRules atomically replaces persistent Before/After declarations. Active waits
// observe the new snapshot after the current callback; timers are preserved.
func (s *Session) SetRules(r RuleSnapshot) error {
	r = cloneRules(r)
	if err := validateCases(r.Before); err != nil {
		return err
	}
	if err := validateCases(r.After); err != nil {
		return err
	}
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.isClosed() {
		return ErrClosed
	}
	s.ruleVersion++
	s.ruleState.Store(&ruleProgram{RuleSnapshot: r, version: s.ruleVersion})
	select {
	case s.rulesChanged <- struct{}{}:
	default:
	}
	s.mailbox.notifyWait()
	return nil
}

func (w *waiter) refreshRules() {
	p := w.session.ruleState.Load()
	if p == w.rulesAt {
		return
	}
	oldCases, oldOrigins, oldCounts := w.cases, w.origins, w.counts
	w.rulesAt = p
	var before, after []Case
	if p != nil {
		before, after = p.Before, p.After
	}
	w.cases, w.origins = prepareCases(w.foreground, before, w.o.rules, after)
	w.counts = nil
	for i := range w.cases {
		if w.limit(i) > 0 {
			w.counts = make([]int, len(w.cases))
			break
		}
	}
	if w.counts != nil && oldCounts != nil {
		for i := range w.cases {
			n := w.origin(i)
			if n.group != callPriority {
				continue
			}
			for j := range oldCases {
				o := caseOrigin{index: j, rule: -1, group: callPriority, scope: ScopeCall}
				if len(oldOrigins) > 0 {
					o = oldOrigins[j]
				}
				if n == o {
					w.counts[i] = oldCounts[j]
					break
				}
			}
		}
	}
	w.scanned = nil
	if len(w.cases) > 8 {
		w.scanned = make([]int, len(w.cases)-8)
	}
	clear(w.smallScans[:])
	w.cursors = nil
	w.suppressed = nil
	if w.forwardMatcher != nil {
		_ = w.enableForwarding()
	}
}

// bindSession checks selectors before evaluating a case.
func bindSession(c Case, s *Session) bool {
	if len(c.Sessions) == 0 {
		return true
	}
	for _, v := range c.Sessions {
		if v == s {
			return true
		}
	}
	return false
}

// waitForControl rejects controls called using a callback's execution context.
type callbackContextKey struct{}

func validateControlContext(ctx context.Context) error {
	if ctx == nil {
		return &ConfigError{Option: "context", Reason: "nil context"}
	}
	if ctx.Value(callbackContextKey{}) != nil {
		return &ConfigError{Option: "control", Reason: "cannot synchronously control the current callback"}
	}
	return nil
}
func waitForControl(ctx context.Context) error {
	if err := validateControlContext(ctx); err != nil {
		return err
	}
	return ctx.Err()
}
