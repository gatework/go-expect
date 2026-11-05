// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"regexp"
	"strings"
)

// Glob matches unanchored Tcl-style Unicode glob text. Stars cross newlines.
// Invalid glob declarations fail pattern validation. Literal/Line support raw bytes.
func Glob(pattern string) Pattern {
	var expr strings.Builder
	expr.WriteString("(?s:")
	chars := []rune(pattern)
	for i := 0; i < len(chars); i++ {
		switch chars[i] {
		case '*':
			expr.WriteString(".*")
		case '?':
			expr.WriteByte('.')
		case '\\':
			i++
			if i == len(chars) {
				return Pattern{kind: invalidPattern}
			}
			expr.WriteString(regexp.QuoteMeta(string(chars[i])))
		case '[':
			end := i + 1
			for end < len(chars) && chars[end] != ']' {
				end++
			}
			if end == len(chars) || end == i+1 {
				return Pattern{kind: invalidPattern}
			}
			expr.WriteByte('[')
			for j := i + 1; j < end; j++ {
				if chars[j] == '^' || chars[j] == '\\' {
					expr.WriteByte('\\')
				}
				expr.WriteRune(chars[j])
			}
			expr.WriteByte(']')
			i = end
		default:
			expr.WriteString(regexp.QuoteMeta(string(chars[i])))
		}
	}
	expr.WriteByte(')')
	p, err := Compile(expr.String())
	if err != nil {
		return Pattern{kind: invalidPattern}
	}
	return p
}

// Fold returns an isolated case-insensitive text pattern. Unicode regexp folding
// applies to Literal and regexp patterns; Line keeps its final-line anchoring.
// Fold selects Go leftmost-first mode, including for imported POSIX/Longest regexps.
func (p Pattern) Fold() Pattern {
	switch p.kind {
	case literalPattern:
		p, _ = Compile("(?i:" + regexp.QuoteMeta(p.text) + ")")
	case regexpPattern:
		p, _ = Compile("(?i:" + p.re.String() + ")")
	case linePattern:
		p, _ = Compile(`(?i:(?:\A|[\r\n])[ \t\x00]*` + regexp.QuoteMeta(p.text) + `[ \t\x00]*[\r\n]*\z)`)
	case allPattern:
		children := make([]Pattern, len(p.all))
		for i, child := range p.all {
			children[i] = child.Fold()
		}
		p = All(children...)
	}
	return p
}

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
