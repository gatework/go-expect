// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"regexp"
	"regexp/syntax"
	"unicode/utf8"

	"github.com/gatework/go-expect/internal/redact"
)

// inputMatcher keeps absolute literal offsets across buffer consumption. Each
// byte advances KMP once per declaration; overlapping completions are retained
// until a higher-priority partial prefix resolves. Regexp captures stay in RE2.
type inputMatcher struct {
	cases     []Case
	literals  []inputLiteral
	prefixes  []*prefixProgram
	base, end int
}
type inputHit struct{ start, end int }

type inputLiteral struct {
	text    string
	failure []int
	matched int
	hits    []inputHit
}

func newInputMatcher(cases []Case) (*inputMatcher, error) {
	m := &inputMatcher{cases: cases, literals: make([]inputLiteral, len(cases)), prefixes: make([]*prefixProgram, len(cases))}
	for i, c := range cases {
		if c.Pattern.kind == literalPattern {
			failure := c.Pattern.failure
			if len(failure) == 0 {
				failure = redact.Prefix(c.Pattern.text)
			}
			m.literals[i] = inputLiteral{text: c.Pattern.text, failure: failure}
			continue
		}
		var expression string
		switch c.Pattern.kind {
		case regexpPattern:
			expression = c.Pattern.re.String()
		case linePattern:
			if !utf8.ValidString(c.Pattern.text) {
				continue
			}
			expression = `(?:\A|[\r\n])[ \t\x00]*` + regexp.QuoteMeta(c.Pattern.text) + `[ \t\x00]*[\r\n]*\z`
		default:
			continue
		}
		re, err := syntax.Parse(expression, syntax.Perl)
		if err != nil {
			return nil, ErrInvalidPattern
		}
		prog, err := syntax.Compile(re.Simplify())
		if err != nil {
			return nil, ErrInvalidPattern
		}
		m.prefixes[i] = &prefixProgram{prog: prog}
	}
	return m, nil
}
func (m *inputMatcher) append(data []byte) {
	for i := range m.literals {
		l := &m.literals[i]
		if l.text == "" {
			continue
		}
		for j, b := range data {
			for l.matched > 0 && b != l.text[l.matched] {
				l.matched = l.failure[l.matched-1]
			}
			if b == l.text[l.matched] {
				l.matched++
			}
			if l.matched == len(l.text) {
				end := m.end + j + 1
				l.hits = append(l.hits, inputHit{start: end - len(l.text), end: end})
				l.matched = l.failure[l.matched-1]
			}
		}
	}
	m.end += len(data)
}
func (m *inputMatcher) consume(n int) {
	m.base += n
	for i := range m.literals {
		l := &m.literals[i]
		for len(l.hits) > 0 && l.hits[0].start < m.base {
			l.hits = l.hits[1:]
		}
		for l.matched > 0 && m.end-l.matched < m.base {
			l.matched = l.failure[l.matched-1]
		}
	}
}

// choose returns a complete candidate or the number of bytes safe to forward.
func (m *inputMatcher) choose(data []byte, eof bool) (int, location, int, error) {
	chosen := -1
	var hit location
	var inline [8]int
	partials := inline[:min(len(m.cases), len(inline))]
	if len(m.cases) > len(inline) {
		partials = make([]int, len(m.cases))
	}
	for i := range m.cases {
		scan, err := m.scanCase(i, data, eof)
		if err != nil {
			return -1, location{}, 0, err
		}
		partials[i] = scan.partial
		if scan.matched && (chosen < 0 || scan.loc.start < hit.start || (scan.loc.start == hit.start && i < chosen)) {
			chosen, hit = i, scan.loc
		}
	}
	partialStart, partialCase := len(data), len(m.cases)
	for i, start := range partials {
		if i != chosen && (start < partialStart || (start == partialStart && i < partialCase)) {
			partialStart, partialCase = start, i
		}
	}
	if !eof && partialCase < len(m.cases) && partialCase != chosen && (chosen < 0 || partialStart < hit.start || (partialStart == hit.start && partialCase < chosen)) {
		return -1, location{}, partialStart, nil
	}
	if chosen >= 0 {
		return chosen, hit, hit.start, nil
	}
	return -1, location{}, len(data), nil
}

// caseScan is one input case's view of pending data: its complete match, if
// any, and the earliest offset where a match could still begin (len(data)
// when none can).
type caseScan struct {
	loc     location
	matched bool
	partial int
}

// scanCase evaluates input case i on data. Event patterns never match bytes.
func (m *inputMatcher) scanCase(i int, data []byte, eof bool) (caseScan, error) {
	scan := caseScan{partial: len(data)}
	if l := &m.literals[i]; l.text != "" {
		if len(l.hits) > 0 {
			scan.loc = location{start: l.hits[0].start - m.base, end: l.hits[0].end - m.base}
			scan.matched = true
		}
		if !eof && l.matched > 0 {
			scan.partial = m.end - l.matched - m.base
		}
		return scan, nil
	}
	c := m.cases[i]
	switch c.Pattern.kind {
	case eofPattern, timeoutPattern, defaultPattern, fullBufferPattern:
		return scan, nil
	default:
	}
	prefix := m.prefixes[i]
	start := 0
	if prefix != nil {
		start = prefix.start(data, m.base, eof)
	}
	if prefix == nil || prefix.hasMatch {
		loc, ok, err := c.Pattern.find(data, eof, 0)
		if err != nil {
			return caseScan{}, err
		}
		scan.loc, scan.matched = loc, ok
	}
	if !eof {
		scan.partial = start
	}
	return scan, nil
}

type prefixProgram struct {
	prog          *syntax.Prog
	active        map[uint32]int
	base, scanned int
	previous      rune
	hasMatch      bool
	stableMatch   bool
}
type prefixThread struct {
	pc    uint32
	start int
}

// start computes the earliest suffix that may match future input. This is only
// a prefix feasibility machine; final extents/captures use regexp. Conditions
// involving unknown future runes are conservative, so bytes are never lost.
func (p *prefixProgram) start(data []byte, base int, eof bool) int {
	if p.active == nil || p.base != base {
		p.active = make(map[uint32]int)
		p.base = base
		p.scanned = 0
		p.previous = -1
		p.hasMatch = false
		p.stableMatch = false
	}
	visible := len(data)
	if !eof {
		visible = completeUTF8(data)
	}
	for p.scanned < visible {
		pos := p.scanned
		ch, width := utf8.DecodeRune(data[pos:visible])
		p.seed(pos)
		ready := p.expand(p.active, syntax.EmptyOpContext(p.previous, ch), false, pos, p.previous)
		p.stableMatch = p.stableMatch || p.containsMatch(ready)
		next := make(map[uint32]int)
		for pc, start := range ready {
			inst := &p.prog.Inst[pc]
			switch inst.Op {
			case syntax.InstRune, syntax.InstRune1, syntax.InstRuneAny, syntax.InstRuneAnyNotNL:
				if inst.MatchRune(ch) {
					if old, ok := next[inst.Out]; !ok || start < old {
						next[inst.Out] = start
					}
				}
			default:
			}
		}
		p.active = next
		p.previous = ch
		p.scanned += width
	}
	p.seed(visible)
	p.hasMatch = p.stableMatch || p.containsMatch(p.expand(p.active, syntax.EmptyOpContext(p.previous, -1), false, visible, p.previous))
	ready := p.expand(p.active, 0, true, visible, p.previous)
	first := min(visible, len(data))
	for _, start := range ready {
		first = min(first, start)
	}
	return first
}
func (p *prefixProgram) seed(pos int) {
	pc := uint32(p.prog.Start)
	if old, ok := p.active[pc]; !ok || pos < old {
		p.active[pc] = pos
	}
}
func (p *prefixProgram) containsMatch(states map[uint32]int) bool {
	for pc := range states {
		if p.prog.Inst[pc].Op == syntax.InstMatch {
			return true
		}
	}
	return false
}

func (p *prefixProgram) expand(seed map[uint32]int, condition syntax.EmptyOp, future bool, pos int, previous rune) map[uint32]int {
	seen := make(map[uint32]int)
	out := make(map[uint32]int)
	stack := make([]prefixThread, 0, len(seed))
	for pc, start := range seed {
		stack = append(stack, prefixThread{pc, start})
	}
	for len(stack) > 0 {
		t := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if old, ok := seen[t.pc]; ok && old <= t.start {
			continue
		}
		seen[t.pc] = t.start
		inst := &p.prog.Inst[t.pc]
		push := func(pc uint32) { stack = append(stack, prefixThread{pc, t.start}) }
		switch inst.Op {
		case syntax.InstAlt, syntax.InstAltMatch:
			push(inst.Out)
			push(inst.Arg)
		case syntax.InstCapture, syntax.InstNop:
			push(inst.Out)
		case syntax.InstEmptyWidth:
			required := syntax.EmptyOp(inst.Arg)
			allowed := required&condition == required
			if future {
				allowed = (required&syntax.EmptyBeginText == 0 || pos == 0) && (required&syntax.EmptyBeginLine == 0 || pos == 0 || previous == '\n')
			}
			if allowed {
				push(inst.Out)
			}
		case syntax.InstFail:
		default:
			out[t.pc] = t.start
		}
	}
	return out
}
