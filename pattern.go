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
	kind uint8
	text string
	re   *regexp.Regexp
	all  []Pattern
}

const (
	literalPattern uint8 = iota + 1
	regexpPattern
	eofPattern
	allPattern
)

// Literal matches a nonempty byte string, including arbitrary binary data.
func Literal(text string) Pattern { return Pattern{kind: literalPattern, text: text} }

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

// materializeCaptures runs once for the winner before guards or consumption.
// Each string owns its bytes independently of the session receive storage.
func (loc location) materializeCaptures(data []byte) []Capture {
	if len(loc.submatches) == 0 {
		return nil
	}
	captures := make([]Capture, len(loc.submatches)/2)
	for i := range captures {
		start, end := loc.submatches[2*i], loc.submatches[2*i+1]
		if start >= 0 {
			captures[i] = Capture{Text: string(data[start:end]), Matched: true}
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
	all []literalScan
}

type literalScan struct {
	text    string
	scanned int
	hit     location
	found   bool
}

func newPatternScan(p Pattern) patternScan {
	if p.kind != allPattern {
		return patternScan{}
	}
	count := literalCount(p)
	if count == 0 {
		return patternScan{}
	}
	state := patternScan{all: make([]literalScan, 0, count)}
	state.addLiterals(p)
	return state
}

func literalCount(p Pattern) int {
	if p.kind == literalPattern {
		return 1
	}
	if p.kind != allPattern {
		return 0
	}
	total := 0
	for _, child := range p.all {
		count := literalCount(child)
		if count == 0 {
			return 0
		}
		total += count
	}
	return total
}

func (s *patternScan) addLiterals(p Pattern) {
	if p.kind == literalPattern {
		s.all = append(s.all, literalScan{text: p.text})
		return
	}
	for _, child := range p.all {
		s.addLiterals(child)
	}
}

func (s *patternScan) findAll(data []byte) (location, bool) {
	loc := location{start: len(data)}
	complete := true
	for i := range s.all {
		literal := &s.all[i]
		if !literal.found {
			from := max(0, literal.scanned-len(literal.text)+1)
			index := bytes.Index(data[from:], []byte(literal.text))
			literal.scanned = len(data)
			if index >= 0 {
				start := from + index
				literal.hit = location{start: start, end: start + len(literal.text)}
				literal.found = true
			}
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
	for i := range s.all {
		s.all[i].scanned = 0
		s.all[i].hit = location{}
		s.all[i].found = false
	}
}
