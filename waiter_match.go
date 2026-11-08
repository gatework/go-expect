// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"io"
	"time"
)

func (w *waiter) scan() (selection, error) {
	w.refreshRules()
	now := time.Now()
	chosen := -1
	var loc location
	deadlineEvent := false
	eof := false
	due := budgetDue(w.relative, w.idle, now)
	if errors.Is(due, ErrIdleTimeout) {
		return selection{}, due
	}
	if errors.Is(due, ErrTimeout) {
		chosen = w.eventCase(timeoutPattern)
		if chosen < 0 {
			return selection{}, due
		}
		deadlineEvent = true
		loc = location{start: len(w.session.receive.pending), end: len(w.session.receive.pending)}
	}

	if chosen < 0 {
		var err error
		chosen, loc, err = w.scanText()
		if err != nil {
			return selection{}, err
		}
	}

	streaming := false
	if chosen < 0 && w.o.streaming != nil && w.forwardMatcher != nil && len(w.session.receive.pending) > 0 {
		_, _, safe, err := w.forwardMatcher.choose(w.session.receive.pending, w.session.receive.terminal != nil)
		if err != nil {
			return selection{}, err
		}
		if safe > 0 {
			chosen = len(w.cases)
			loc = location{end: safe}
			streaming = true
		} else if len(w.session.receive.pending) >= w.session.opts.limit {
			return selection{}, ErrBufferLimit
		}
	} else if chosen < 0 && w.o.streaming != nil && len(w.session.receive.pending) >= w.streamHigh() {
		chosen = len(w.cases)
		loc = location{start: 0, end: w.streamLow()}
		streaming = true
	}
	if chosen < 0 && w.session.receive.terminal != nil {
		if errors.Is(w.session.receive.terminal, io.EOF) && !errors.Is(w.session.receive.terminal, errInvalidReadCount) {
			chosen = w.eventCase(eofPattern)

			if chosen >= 0 {
				loc = location{start: len(w.session.receive.pending), end: len(w.session.receive.pending)}
				eof = true
			}

		}
		if chosen < 0 {
			return selection{}, w.session.receive.terminal
		}
	}
	if chosen < 0 && len(w.session.receive.pending) >= w.session.opts.limit && len(w.session.receive.staged) > 0 {
		chosen = w.eventCase(fullBufferPattern)
		if chosen < 0 {
			return selection{}, ErrBufferLimit
		}
		loc = location{start: len(w.session.receive.pending), end: len(w.session.receive.pending)}
	}
	return selection{chosen, loc, streaming, deadlineEvent, eof}, nil

}
func (w *waiter) eventCase(kind patternKind) int {
	for i, c := range w.cases {
		if bindSession(c, w.session) && (c.Pattern.kind == kind || (c.Pattern.kind == defaultPattern && (kind == timeoutPattern || kind == eofPattern))) {
			return i
		}
	}
	return -1
}

func (w *waiter) scanText() (int, location, error) {
	chosen := -1
	var loc location
	for i, c := range w.cases {
		if !bindSession(c, w.session) {
			continue
		}
		if v, ok := w.suppressed[i]; ok && v == w.session.receive.version {
			continue
		}
		// Offsets are cleared whenever input is consumed or rules change, so a
		// nonzero offset records an unsuccessful scan of an unchanged prefix.
		prior := w.scanOffset(i)
		from := c.Pattern.resumeFrom(prior)
		var hit location
		var ok bool
		var e error
		if c.Pattern.kind == allPattern || c.Pattern.kind == linePattern || (c.Pattern.kind == literalPattern && len(c.Pattern.failure) > 0) {
			if w.cursors == nil {
				w.cursors = make([]*matchCursor, len(w.cases))
			}
			if w.cursors[i] == nil {
				cursor := newMatchCursor(c.Pattern)
				w.cursors[i] = &cursor
			}
			hit, ok, e = w.cursors[i].find(w.session.receive.pending, w.session.receive.terminal != nil, from, true)
		} else {
			hit, ok, e = c.Pattern.find(w.session.receive.pending, w.session.receive.terminal != nil, from)
		}
		if len(w.session.opts.trace) > 0 {
			event := TraceEvent{SessionID: w.session.id, Scope: w.origin(i).scope, RuleIndex: w.origin(i).rule, CaseIndex: w.origin(i).index, Buffered: len(w.session.receive.pending), Start: hit.start, End: hit.end, Pattern: c.Pattern.String(), Matched: ok}
			for _, observer := range w.session.opts.trace {
				observer(event)
			}
		}
		w.setScanOffset(i, c.Pattern.advance(w.session.receive.pending, prior))
		if e != nil {
			return -1, location{}, e
		}
		if ok && (chosen < 0 || w.origin(i).group < w.origin(chosen).group || (w.origin(i).group == w.origin(chosen).group && !w.o.declaration && hit.start < loc.start)) {
			chosen = i
			loc = hit
			if w.o.declaration {
				break
			}
		}
	}
	return chosen, loc, nil
}
