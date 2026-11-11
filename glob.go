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
	expr, ok := globExpr([]rune(pattern), fold)
	if !ok {
		return Pattern{kind: invalidPattern}
	}
	p, err := Compile(expr)
	if err != nil {
		return Pattern{kind: invalidPattern}
	}
	p.glob = true
	p.text = pattern
	return p
}

// globExpr translates glob text to equivalent regexp source; ok is false for
// a dangling escape or an empty or unterminated bracket class.
func globExpr(chars []rune, fold bool) (expr string, ok bool) {
	var b strings.Builder
	if fold {
		b.WriteString("(?is:")
	} else {
		b.WriteString("(?s:")
	}
	for i := 0; i < len(chars); i++ {
		switch chars[i] {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteByte('.')
		case '\\':
			i++
			if i == len(chars) {
				return "", false
			}
			b.WriteString(regexp.QuoteMeta(string(chars[i])))
		case '[':
			end, ok := writeGlobClass(&b, chars, i, fold)
			if !ok {
				return "", false
			}
			i = end
		default:
			b.WriteString(regexp.QuoteMeta(string(chars[i])))
		}
	}
	b.WriteByte(')')
	return b.String(), true
}

// writeGlobClass writes the bracket class opening at chars[open] and returns
// the index of its closing bracket.
func writeGlobClass(b *strings.Builder, chars []rune, open int, fold bool) (end int, ok bool) {
	end = open + 1
	for end < len(chars) && chars[end] != ']' {
		end++
	}
	if end == len(chars) || end == open+1 {
		return 0, false
	}
	lower := func(r rune) rune {
		if fold {
			return unicode.ToLower(r)
		}
		return r
	}
	b.WriteByte('[')
	for j := open + 1; j < end; j++ {
		ch := lower(chars[j])
		if j+2 < end && chars[j+1] == '-' {
			lo, hi := ch, lower(chars[j+2])
			if lo > hi {
				lo, hi = hi, lo
			}
			writeClassRune(b, lo)
			b.WriteByte('-')
			writeClassRune(b, hi)
			j += 2
			continue
		}
		writeClassRune(b, ch)
	}
	b.WriteByte(']')
	return end, true
}

// writeClassRune writes r inside a bracket class, escaping class metacharacters.
func writeClassRune(b *strings.Builder, r rune) {
	if r == '^' || r == '\\' || r == ']' || r == '-' {
		b.WriteByte('\\')
	}
	b.WriteRune(r)
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
