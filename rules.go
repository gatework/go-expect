// SPDX-License-Identifier: Apache-2.0

package expect

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
	snapshot := append([]Case(nil), cases...)
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
	snapshot := append([]Case(nil), cases...)
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
	return RuleSnapshot{Before: append([]Case(nil), s.opts.before...), After: append([]Case(nil), s.opts.after...)}
}

type caseOrigin struct {
	index, rule, group int
	scope              RuleScope
}

func prepareCases(foreground, before, rules, after []Case) ([]Case, []caseOrigin) {
	if len(before)+len(rules)+len(after) == 0 {
		return foreground, nil
	}
	cases := make([]Case, 0, len(foreground)+len(before)+len(rules)+len(after))
	origins := make([]caseOrigin, 0, cap(cases))
	add := func(items []Case, scope RuleScope, group int, isRule bool) {
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
	add(before, ScopeBefore, 0, true)
	add(foreground, ScopeCall, 1, false)
	add(rules, ScopeCall, 1, true)
	add(after, ScopeAfter, 2, true)
	return cases, origins
}
func eventCase(cases []Case, kind uint8) int {
	for i, c := range cases {
		if c.Pattern.kind == kind || (c.Pattern.kind == defaultPattern && (kind == timeoutPattern || kind == eofPattern)) {
			return i
		}
	}
	return -1
}
