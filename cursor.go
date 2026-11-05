package expect

// matchCursor owns progress for one wait. Only literal hits are monotonic:
// final-line and regexp matches must be reconsidered as the visible input grows.
type matchCursor struct {
	pattern  Pattern
	literal  patternScan
	line     lineCursor
	children []matchCursor
}

func newMatchCursor(p Pattern) matchCursor {
	c := matchCursor{pattern: p, literal: newPatternScan(p)}
	if p.kind == linePattern {
		c.line.prefix = literalPrefix(p.text)
	}
	if p.kind == allPattern && len(c.literal.literals) == 0 {
		for _, child := range p.all {
			c.children = append(c.children, newMatchCursor(child))
		}
	}
	return c
}
func (c *matchCursor) find(data []byte, terminal bool, from int, captures bool) (location, bool, error) {
	if len(c.literal.literals) > 0 {
		loc, ok := c.literal.findLiterals(data)
		return loc, ok, nil
	}
	if c.pattern.kind == linePattern {
		loc, ok := c.line.find(data, c.pattern.text)
		return loc, ok, nil
	}
	if c.pattern.kind == allPattern {
		loc := location{start: len(data)}
		for i := range c.children {
			hit, ok, err := c.children[i].find(data, terminal, 0, false)
			if err != nil || !ok {
				return location{}, false, err
			}
			loc.start = min(loc.start, hit.start)
			loc.end = max(loc.end, hit.end)
		}
		return loc, true, nil
	}
	return c.pattern.findLocation(data, terminal, from, captures)
}
func (c *matchCursor) reset() {
	c.literal.reset()
	c.line = lineCursor{prefix: c.line.prefix}
	for i := range c.children {
		c.children[i].reset()
	}
}

type lineCursor struct {
	prefix                      []int
	scanned, matched, lineStart int
	firstNonPad                 int // offset + 1; zero means the line contains only padding
	valid                       bool
	hitStart                    int
}

func linePad(b byte) bool { return b == ' ' || b == '\t' || b == 0 }
func (c *lineCursor) find(data []byte, text string) (location, bool) {
	for i := c.scanned; i < len(data); i++ {
		b := data[i]
		if b == '\r' || b == '\n' {
			c.lineStart = i + 1
			c.matched = 0
			c.firstNonPad = 0
			continue
		}
		// Any content after a delimiter starts a new final line.
		if i == c.lineStart {
			c.valid = false
		}
		if !linePad(b) {
			c.valid = false
			if c.firstNonPad == 0 {
				c.firstNonPad = i + 1
			}
		}
		for c.matched > 0 && b != text[c.matched] {
			c.matched = c.prefix[c.matched-1]
		}
		if b == text[c.matched] {
			c.matched++
		}
		if c.matched == len(text) {
			start := i + 1 - len(text)
			if c.firstNonPad == 0 || c.firstNonPad-1 >= start {
				c.valid = true
				c.hitStart = max(0, c.lineStart-1)
			}
			c.matched = c.prefix[c.matched-1]
		}
	}
	c.scanned = len(data)
	return location{start: c.hitStart, end: len(data)}, c.valid
}
