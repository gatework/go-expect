// Copyright 2018 Netflix, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
package expect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"
)

// Pattern is an immutable literal, regular expression, EOF or All pattern.
// Its zero value is invalid. It never contains per-operation match state.
type Pattern struct {
	kind    uint8
	text    string
	re      *regexp.Regexp
	all     []Pattern
	failure []int
}

const literalKMPThreshold = 256

const (
	literalPattern uint8 = iota + 1
	regexpPattern
	eofPattern
	allPattern
	linePattern
)

// Literal matches a nonempty byte string, including arbitrary binary data.
func Literal(text string) Pattern {
	p := Pattern{kind: literalPattern, text: text}
	if len(text) > literalKMPThreshold {
		p.failure = literalPrefix(text)
	}
	return p
}

// Line matches a byte-exact final line with optional space, tab and NUL padding.
// The match includes its preceding CR/LF delimiter and trailing CR/LF bytes.
// Empty text and text containing CR or LF are invalid declarations.
func Line(text string) Pattern {
	return Pattern{kind: linePattern, text: text, failure: literalPrefix(text)}
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
				return errors.New("expect: invalid pattern")
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
		return Pattern{}, errors.New("expect: invalid regular expression")
	}
	return Pattern{kind: regexpPattern, re: r}, nil
}

// FromRegexp snapshots a compiled expression, including POSIX/Longest semantics.
// Later changes to the caller's expression do not affect the Pattern.
func FromRegexp(re *regexp.Regexp) (Pattern, error) {
	if re == nil {
		return Pattern{}, errors.New("expect: nil regular expression")
	}
	// Copy is appropriate here to isolate Longest settings, as documented by regexp.
	//nolint:staticcheck // SA1019: preserving independent Longest configuration.
	return Pattern{kind: regexpPattern, re: re.Copy()}, nil
}

// EOF matches a clean end of input, after buffered text has been matched.
func EOF() Pattern { return Pattern{kind: eofPattern} }

// All matches all supplied text patterns, in any order, consuming through the
// last match. Repeated patterns may refer to the same bytes. Captures are omitted.
func All(patterns ...Pattern) Pattern {
	return Pattern{kind: allPattern, all: append([]Pattern(nil), patterns...)}
}
func (p Pattern) String() string   { return "expect.Pattern" }
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
	case eofPattern:
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
	}
	return errors.New("expect: invalid pattern")
}

// Capture distinguishes an unmatched optional group from an empty matched group.
type Capture struct {
	Text    string
	Matched bool
}

func (Capture) String() string     { return "expect.Capture" }
func (c Capture) GoString() string { return c.String() }

// Result is a detached snapshot of the final match, not the whole conversation.
// Before contains pending input on failure; CaseIndex is -1 without a match.
// Explicit fields contain unredacted bytes; formatting emits only metadata.
type Result struct {
	CaseIndex     int
	Before, Match string
	Captures      []Capture
}

func (r Result) String() string   { return fmt.Sprintf("expect.Result{case:%d}", r.CaseIndex) }
func (r Result) GoString() string { return r.String() }

// Action determines whether a matched Case returns or continues waiting.
type Action uint8

const (
	Return Action = iota + 1
	Continue
	ContinueWithoutReset
)

// Handler runs after consumption, outside internal locks. It may send or close,
// but may not call Expect recursively on the same Session.
type Handler func(context.Context, *Session, Result) (Action, error)

// Case associates a pattern with an optional handler (nil means Return).
type Case struct {
	Pattern Pattern
	Handler Handler
}

func (Case) String() string     { return "expect.Case" }
func (c Case) GoString() string { return c.String() }

type location struct {
	start, end int
	submatches []int
}

func (p Pattern) find(data []byte, terminal bool, scanFrom int) (location, bool, error) {
	return p.findLocation(data, terminal, scanFrom, true)
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
		var idx []int
		if captures {
			idx = p.re.FindSubmatchIndex(view)
		} else {
			idx = p.re.FindIndex(view)
		}
		if idx == nil {
			return location{}, false, nil
		}
		if idx[0] == idx[1] {
			return location{}, false, ErrEmptyMatch
		}
		return location{start: idx[0], end: idx[1], submatches: idx[2:]}, true, nil
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
	}
	return location{}, false, nil
}

// capturesFromSnapshot runs once for the winner before guards or consumption.
// Each string owns its bytes independently of the session receive storage.
func (loc location) capturesFromSnapshot(data []byte, match string) []Capture {
	if len(loc.submatches) == 0 {
		return nil
	}
	captures := make([]Capture, len(loc.submatches)/2)
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
			captures[i] = Capture{Text: text, Matched: true}
		}
	}
	return captures
}

// Retain only a trailing incomplete encoding, never rewrite invalid bytes.
func completeUTF8(data []byte) int {
	start := len(data) - utf8.UTFMax
	if start < 0 {
		start = 0
	}
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
func literalPrefix(text string) []int {
	failure := make([]int, len(text))
	for i, j := 1, 0; i < len(text); i++ {
		for j > 0 && text[i] != text[j] {
			j = failure[j-1]
		}
		if text[i] == text[j] {
			j++
		}
		failure[i] = j
	}
	return failure
}
