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
	captures   []Capture
}

func (p Pattern) find(data []byte, terminal bool, scanFrom int) (location, bool, error) {
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
		idx := p.re.FindSubmatchIndex(view)
		if idx == nil {
			return location{}, false, nil
		}
		if idx[0] == idx[1] {
			return location{}, false, ErrEmptyMatch
		}
		loc := location{start: idx[0], end: idx[1]}
		for i := 2; i < len(idx); i += 2 {
			c := Capture{}
			if idx[i] >= 0 {
				c.Text = string(data[idx[i]:idx[i+1]])
				c.Matched = true
			}
			loc.captures = append(loc.captures, c)
		}
		return loc, true, nil
	case allPattern:
		loc := location{start: len(data)}
		for _, child := range p.all {
			hit, ok, err := child.find(data, terminal, 0)
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
