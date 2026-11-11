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

// scanText returns the winning text case: the highest-priority group, then
// the earliest start unless declaration order was requested.
func (w *waiter) scanText() (int, location, error) {
	chosen := -1
	var loc location
	// Progress is valid only while the buffer has grown by appends since it
	// was recorded. Rule replacement rebuilds it separately in refreshRules.
	if w.progressAt != w.session.receive.consumed {
		w.resetProgress()
		w.progressAt = w.session.receive.consumed
	}
	for i, c := range w.cases {
		if !bindSession(c, w.session) {
			continue
		}
		if v, ok := w.suppressed[i]; ok && v == w.session.receive.version {
			continue
		}
		hit, ok, e := w.findCase(i, c)
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

// findCase matches case i on the receive buffer, resuming its recorded
// progress, traces the attempt and records new progress.
func (w *waiter) findCase(i int, c Case) (location, bool, error) {
	receive := &w.session.receive
	// Nonzero progress records unsuccessful scans of an unchanged prefix.
	prior := w.scanOffset(i)
	from := c.Pattern.resumeFrom(prior)
	var hit location
	var ok bool
	var e error
	if c.Pattern.kind == allPattern || c.Pattern.kind == linePattern || (c.Pattern.kind == literalPattern && len(c.Pattern.failure) > 0) {
		hit, ok, e = w.cursor(i, c).find(receive.pending, receive.terminal != nil, from, true)
	} else {
		hit, ok, e = c.Pattern.find(receive.pending, receive.terminal != nil, from)
	}
	if len(w.session.opts.trace) > 0 {
		origin := w.origin(i)
		event := TraceEvent{SessionID: w.session.id, Scope: origin.scope, RuleIndex: origin.rule, CaseIndex: origin.index, Buffered: len(receive.pending), Start: hit.start, End: hit.end, Pattern: c.Pattern.String(), Matched: ok}
		for _, observer := range w.session.opts.trace {
			observer(event)
		}
	}
	if ok {
		w.resetCaseProgress(i)
	} else {
		w.setScanOffset(i, c.Pattern.advance(receive.pending, prior))
	}
	return hit, ok, e
}

// cursor returns the incremental match cursor of case i, creating it on
// first use.
func (w *waiter) cursor(i int, c Case) *matchCursor {
	if w.cursors == nil {
		w.cursors = make([]*matchCursor, len(w.cases))
	}
	if w.cursors[i] == nil {
		cursor := newMatchCursor(c.Pattern)
		w.cursors[i] = &cursor
	}
	return w.cursors[i]
}
