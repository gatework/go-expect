// SPDX-License-Identifier: Apache-2.0

// Copyright 2018 Netflix, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0

package expect

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"slices"
	"unicode/utf8"

	"github.com/gatework/go-expect/internal/redact"
)

// Pattern is an immutable literal, regular expression, EOF or All pattern.
// Its zero value is invalid. It never contains per-operation match state.
type Pattern struct {
	kind    patternKind
	glob    bool
	text    string
	re      *regexp.Regexp
	all     []Pattern
	failure []int
	// lineLocal regexps cannot consume LF or assert the start of text, so a
	// failed scan only needs to resume at the final incomplete line.
	lineLocal bool
}

const literalKMPThreshold = 256

type patternKind uint8

const invalidPattern patternKind = 255

// Pattern kinds are internal matching discriminators.
const (
	literalPattern patternKind = iota + 1
	regexpPattern
	eofPattern
	allPattern
	linePattern
	timeoutPattern
	defaultPattern
	fullBufferPattern
)

// Literal matches a nonempty byte string, including arbitrary binary data.
func Literal(text string) Pattern {
	p := Pattern{kind: literalPattern, text: text}
	if len(text) > literalKMPThreshold {
		p.failure = redact.Prefix(text)
	}
	return p
}

// Line matches a byte-exact final line with optional space, tab and NUL padding.
// The match includes its preceding CR/LF delimiter and trailing CR/LF bytes.
// Empty text and text containing CR or LF are invalid declarations.
func Line(text string) Pattern {
	return Pattern{kind: linePattern, text: text, failure: redact.Prefix(text)}
}

// IsZero distinguishes an unspecified pattern from a malformed declaration.
func (p Pattern) IsZero() bool { return p.kind == 0 }

// ValidateText checks a nonempty text declaration before transport access.
// EOF and regular expressions matching empty input are not text declarations.
func (p Pattern) ValidateText() error {
	empty := false
	var visit func(Pattern) error
	visit = func(p Pattern) error {
		if p.kind == allPattern {
			if len(p.all) == 0 {
				return ErrInvalidPattern
			}
			for _, child := range p.all {
				if err := visit(child); err != nil {
					return err
				}
			}
			return nil
		}
		if err := p.validate(true); err != nil {
			return err
		}
		if p.kind == regexpPattern && p.re.MatchString("") {
			empty = true
		}
		return nil
	}
	if err := visit(p); err != nil {
		return err
	}
	if empty {
		return ErrEmptyMatch
	}
	return nil
}

// Compile compiles a Go regular expression. Empty matches are rejected at runtime.
func Compile(expr string) (Pattern, error) {
	r, e := regexp.Compile(expr)
	if e != nil {
		if detail, ok := errors.AsType[*syntax.Error](e); ok {
			return Pattern{}, &CompileError{Code: detail.Code}
		}
		return Pattern{}, ErrInvalidPattern
	}
	return Pattern{kind: regexpPattern, re: r, lineLocal: lineLocal(expr, syntax.Perl)}, nil
}

// FromRegexp snapshots a compiled expression, including POSIX/Longest semantics.
// Later changes to the caller's expression do not affect the Pattern.
func FromRegexp(re *regexp.Regexp) (Pattern, error) {
	if re == nil {
		return Pattern{}, ErrInvalidPattern
	}
	// Copy is appropriate here to isolate Longest settings, as documented by regexp.
	//nolint:staticcheck // SA1019: preserving independent Longest configuration.
	// The source syntax is not recorded, so both Compile and CompilePOSIX
	// interpretations of the expression must qualify.
	return Pattern{kind: regexpPattern, re: re.Copy(), lineLocal: lineLocal(re.String(), syntax.Perl, syntax.POSIX)}, nil
}

// lineLocal reports whether every match lies within one LF-delimited line and
// observes the same assertions after that LF as at the start of a slice.
// Each flag set that parses the expression must satisfy both conditions.
func lineLocal(expr string, flags ...syntax.Flags) bool {
	parsed := false
	for _, f := range flags {
		tree, err := syntax.Parse(expr, f)
		if err != nil {
			continue
		}
		if !avoidsLF(tree) {
			return false
		}
		parsed = true
	}
	return parsed
}

// avoidsLF rejects any node that can consume LF. A text-start assertion is
// rejected because it is true at a resumed slice start but false after LF.
// Line-start, word-boundary and end assertions agree at both positions.
func avoidsLF(re *syntax.Regexp) bool {
	switch re.Op {
	case syntax.OpAnyChar, syntax.OpBeginText:
		return false
	case syntax.OpLiteral:
		// No rune folds to LF, so FoldCase literals are covered by their runes.
		if slices.Contains(re.Rune, '\n') {
			return false
		}
	case syntax.OpCharClass:
		for i := 0; i+1 < len(re.Rune); i += 2 {
			if re.Rune[i] <= '\n' && '\n' <= re.Rune[i+1] {
				return false
			}
		}
	default:
	}
	for _, sub := range re.Sub {
		if !avoidsLF(sub) {
			return false
		}
	}
	return true
}

// Timeout matches the relative wait deadline, never parent cancellation.
func Timeout() Pattern { return Pattern{kind: timeoutPattern} }

// Default matches a relative timeout or clean EOF when selected in declaration order.
func Default() Pattern { return Pattern{kind: defaultPattern} }

// FullBuffer explicitly accepts a full unmatched buffer and consumes that snapshot.
func FullBuffer() Pattern { return Pattern{kind: fullBufferPattern} }

// EOF matches a clean end of input, after buffered text has been matched.
func EOF() Pattern { return Pattern{kind: eofPattern} }

// All matches all supplied text patterns, in any order, consuming through the
// last match. Repeated patterns may refer to the same bytes. Captures are omitted.
func All(patterns ...Pattern) Pattern {
	return Pattern{kind: allPattern, all: append([]Pattern(nil), patterns...)}
}
func (p Pattern) String() string {
	names := []string{"invalid", "literal", "regexp", "eof", "all", "line", "timeout", "default", "full_buffer"}
	kind := "invalid"
	if int(p.kind) < len(names) {
		kind = names[p.kind]
	}
	size := len(p.text)
	if p.re != nil {
		size = len(p.re.String())
	}
	if p.kind == allPattern {
		size = len(p.all)
	}
	if p.glob {
		kind = "glob"
	}
	return fmt.Sprintf("expect.Pattern{%s,len=%d}", kind, size)
}

// GoString formats metadata without exposing payloads.
func (p Pattern) GoString() string { return p.String() }
func (p Pattern) validate(textOnly bool) error {
	switch p.kind {
	case literalPattern:
		if p.text != "" {
			return nil
		}
	case linePattern:
		if p.text != "" && !bytes.ContainsAny([]byte(p.text), "\r\n") {
			return nil
		}
	case regexpPattern:
		if p.re != nil {
			return nil
		}
	case eofPattern, timeoutPattern, defaultPattern, fullBufferPattern:
		if !textOnly {
			return nil
		}
	case allPattern:
		if len(p.all) > 0 {
			for _, child := range p.all {
				if e := child.validate(true); e != nil {
					return e
				}
			}
			return nil
		}
	default:
	}
	return ErrInvalidPattern
}

type location struct {
	start, end int
	submatches []int
	names      []string
}

func (p Pattern) find(data []byte, terminal bool, scanFrom int) (location, bool, error) {
	return p.findLocation(data, terminal, scanFrom, true)
}

// scanProgress records an unsuccessful scan of an unchanged prefix. The zero
// value means nothing was scanned. mark is where the next search may start
// (adjusted for literal length); scanned bounds bytes already searched for LF.
type scanProgress struct{ mark, scanned int }

// resumeFrom returns where a match can start after prior unsuccessful scans.
// A literal can only end in bytes after its mark. A lineLocal mark is already
// the start of the last scanned line. Other patterns search the whole view.
func (p Pattern) resumeFrom(prior scanProgress) int {
	switch {
	case p.kind == literalPattern:
		return max(0, prior.mark-len(p.text)+1)
	case p.kind == regexpPattern && p.lineLocal:
		return prior.mark
	default:
		return 0
	}
}

// advance records an unsuccessful search of data. A lineLocal match starting
// before the last LF would end before it with unchanged context, so the failed
// scan would already have found it. Each byte is searched for LF once per wait.
func (p Pattern) advance(data []byte, prior scanProgress) scanProgress {
	if p.kind != regexpPattern || !p.lineLocal {
		return scanProgress{mark: len(data), scanned: len(data)}
	}
	mark := prior.mark
	if i := bytes.LastIndexByte(data[min(prior.scanned, len(data)):], '\n'); i >= 0 {
		mark = min(prior.scanned, len(data)) + i + 1
	}
	return scanProgress{mark: mark, scanned: len(data)}
}

// All children only need their extent; ordinary candidates retain capture
// offsets until Expect chooses its final winner. No candidate copies payloads.
func (p Pattern) findLocation(data []byte, terminal bool, scanFrom int, captures bool) (location, bool, error) {
	switch p.kind {
	case linePattern:
		return p.findLine(data)
	case literalPattern:
		i := bytes.Index(data[scanFrom:], []byte(p.text))
		if i < 0 {
			return location{}, false, nil
		}
		i += scanFrom
		return location{start: i, end: i + len(p.text)}, true, nil
	case regexpPattern:
		view := data
		if !terminal {
			view = data[:completeUTF8(data)]
		}
		// Only resumeFrom supplies a nonzero regexp offset (lineLocal patterns).
		from := min(scanFrom, len(view))
		var idx []int
		if captures {
			idx = p.re.FindSubmatchIndex(view[from:])
		} else {
			idx = p.re.FindIndex(view[from:])
		}
		if idx == nil {
			return location{}, false, nil
		}
		if from > 0 {
			for k, v := range idx {
				if v >= 0 {
					idx[k] = v + from
				}
			}
		}
		if idx[0] == idx[1] {
			return location{}, false, ErrEmptyMatch
		}
		return location{start: idx[0], end: idx[1], submatches: idx[2:], names: p.re.SubexpNames()[1:]}, true, nil
	case allPattern:
		loc := location{start: len(data)}
		for _, child := range p.all {
			hit, ok, err := child.findLocation(data, terminal, 0, false)
			if err != nil || !ok {
				return location{}, false, err
			}
			if hit.start < loc.start {
				loc.start = hit.start
			}
			if hit.end > loc.end {
				loc.end = hit.end
			}
		}
		return loc, true, nil
	default: // Event patterns are selected by the waiter.
	}
	return location{}, false, nil
}

// capturesFromSnapshot runs once for the winner before callback dispatch or consumption.
// Each string owns its bytes independently of the session receive storage.
func (loc location) capturesFromSnapshot(data []byte, match string) []Capture {
	if len(loc.submatches) == 0 {
		return nil
	}
	captures := make([]Capture, len(loc.submatches)/2)
	for i := range captures {
		captures[i].Start = -1
		captures[i].End = -1
		if i < len(loc.names) {
			captures[i].Name = loc.names[i]
		}
	}
	snapshots := make(map[[2]int]string)
	for i := range captures {
		start, end := loc.submatches[2*i], loc.submatches[2*i+1]
		if start >= 0 {
			key := [2]int{start, end}
			text, ok := snapshots[key]
			if !ok {
				if start == loc.start && end == loc.end && len(match) == end-start {
					text = match
				} else {
					text = string(data[start:end])
				}
				snapshots[key] = text
			}
			name := ""
			if i < len(loc.names) {
				name = loc.names[i]
			}
			captures[i] = Capture{Text: text, Matched: true, Name: name, Start: start, End: end}
		}
	}
	return captures
}

// Retain only a trailing incomplete encoding, never rewrite invalid bytes.
func completeUTF8(data []byte) int {
	start := max(len(data)-utf8.UTFMax, 0)
	for i := len(data) - 1; i >= start; i-- {
		if utf8.RuneStart(data[i]) {
			if !utf8.FullRune(data[i:]) {
				return i
			}
			break
		}
	}
	return len(data)
}

// patternScan belongs to one Expect call. Pure literal All trees can retain
// their first hits as input grows; regular expressions always see the full view.
type patternScan struct {
	literals []literalScan
}

type literalScan struct {
	text    string
	failure []int
	matched int
	scanned int
	hit     location
	found   bool
}

func (s *patternScan) findLiterals(data []byte) (location, bool) {
	loc := location{start: len(data)}
	complete := true
	for i := range s.literals {
		literal := &s.literals[i]
		if !literal.found {
			if len(literal.failure) > 0 {
				for i := literal.scanned; i < len(data); i++ {
					for literal.matched > 0 && data[i] != literal.text[literal.matched] {
						literal.matched = literal.failure[literal.matched-1]
					}
					if data[i] == literal.text[literal.matched] {
						literal.matched++
					}
					if literal.matched == len(literal.text) {
						literal.hit = location{start: i + 1 - len(literal.text), end: i + 1}
						literal.found = true
						break
					}
				}
			} else {
				from := max(0, literal.scanned-len(literal.text)+1)
				index := bytes.Index(data[from:], []byte(literal.text))
				if index >= 0 {
					start := from + index
					literal.hit = location{start: start, end: start + len(literal.text)}
					literal.found = true
				}
			}
			literal.scanned = len(data)
		}
		if !literal.found {
			complete = false
			continue
		}
		loc.start = min(loc.start, literal.hit.start)
		loc.end = max(loc.end, literal.hit.end)
	}
	if !complete {
		return location{}, false
	}
	return loc, true
}

func (s *patternScan) reset() {
	for i := range s.literals {
		s.literals[i].scanned = 0
		s.literals[i].matched = 0
		s.literals[i].hit = location{}
		s.literals[i].found = false
	}
}

func (p Pattern) findLine(data []byte) (location, bool, error) {
	end := len(data)
	for end > 0 && (data[end-1] == '\r' || data[end-1] == '\n') {
		end--
	}
	line := bytes.LastIndexAny(data[:end], "\r\n") + 1
	index := bytes.LastIndex(data[line:end], []byte(p.text))
	if index < 0 {
		return location{}, false, nil
	}
	start := line + index
	pad := func(b byte) bool { return b == ' ' || b == '\t' || b == 0 }
	for _, b := range data[start+len(p.text) : end] {
		if !pad(b) {
			return location{}, false, nil
		}
	}
	for _, b := range data[line:start] {
		if !pad(b) {
			return location{}, false, nil
		}
	}
	if line > 0 {
		line--
	}
	return location{start: line, end: len(data)}, true, nil
}
