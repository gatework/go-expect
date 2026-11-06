// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Glob matches unanchored Tcl-style Unicode glob text. Stars cross newlines.
// Invalid glob declarations fail pattern validation. Literal/Line support raw bytes.
func Glob(pattern string) Pattern { return compileGlob(pattern, false) }
func compileGlob(pattern string, fold bool) Pattern {
	if !utf8.ValidString(pattern) {
		return Pattern{kind: invalidPattern}
	}
	var expr strings.Builder
	if fold {
		expr.WriteString("(?is:")
	} else {
		expr.WriteString("(?s:")
	}
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
				ch := chars[j]
				if fold {
					ch = unicode.ToLower(ch)
				}
				if j+2 < end && chars[j+1] == '-' {
					hi := chars[j+2]
					if fold {
						hi = unicode.ToLower(hi)
					}
					lo := ch
					if lo > hi {
						lo, hi = hi, lo
					}
					fmtRune := func(r rune) {
						if r == '^' || r == '\\' || r == ']' || r == '-' {
							expr.WriteByte('\\')
						}
						expr.WriteRune(r)
					}
					fmtRune(lo)
					expr.WriteByte('-')
					fmtRune(hi)
					j += 2
					continue
				}
				if ch == '^' || ch == '\\' || ch == ']' || ch == '-' {
					expr.WriteByte('\\')
				}
				expr.WriteRune(ch)
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
	p.glob = true
	p.text = pattern
	return p
}

// Fold returns an isolated case-insensitive text pattern. Unicode regexp folding
// applies to Literal and regexp patterns; Line keeps its final-line anchoring.
// Fold selects Go leftmost-first mode, including for imported POSIX/Longest regexps.
func (p Pattern) Fold() Pattern {
	if p.kind != eofPattern && p.kind != timeoutPattern && p.kind != defaultPattern && p.kind != fullBufferPattern {
		if p.validate(true) != nil {
			return Pattern{kind: invalidPattern}
		}
	}
	switch p.kind {
	case literalPattern:
		p = foldCompile("(?i:" + regexp.QuoteMeta(p.text) + ")")
	case regexpPattern:
		if p.glob {
			return compileGlob(p.text, true)
		}
		p = foldCompile("(?i:" + p.re.String() + ")")
	case linePattern:
		p = foldCompile(`(?i:(?:\A|[\r\n])[ \t\x00]*` + regexp.QuoteMeta(p.text) + `[ \t\x00]*[\r\n]*\z)`)
	case allPattern:
		children := make([]Pattern, len(p.all))
		for i, child := range p.all {
			children[i] = child.Fold()
		}
		p = All(children...)
	default: // Event patterns are unchanged by folding.
	}
	return p
}

func foldCompile(expr string) Pattern {
	p, e := Compile(expr)
	if e != nil {
		return Pattern{kind: invalidPattern}
	}
	return p
}
