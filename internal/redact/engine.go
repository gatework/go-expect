// SPDX-License-Identifier: Apache-2.0

// Package redact owns incremental coverage and bounded streaming storage.
// Rendering and rule-scope policy belong to the caller; no synchronization is implied.
package redact

import "bytes"

// Buffer owns pending bytes and their coverage marks. Consumers render a prefix
// then retain only the suffix required by their chosen policy.
type Buffer struct {
	Pending, Storage []byte
	Hidden, Marks    []bool
}

// Rule owns per-stream progress and an immutable compiled prefix table.
type Rule struct {
	Failure          []int
	Matched, Covered int
}

// Scan marks newly completed matches and optionally ambiguous prefixes.
func (rule *Rule) Scan(secret, data []byte, b *Buffer, prior int, finish, trackPrefix bool) {
	mark := func(start, end int) {
		// Match intervals arrive in order, including overlapping occurrences.
		// Each covered byte is marked at most once per rule, not per occurrence.
		for i := max(start, rule.Covered); i < end; i++ {
			b.Hidden[i] = true
		}
		rule.Covered = max(rule.Covered, end)
	}
	if len(rule.Failure) > 0 {
		for i, value := range data {
			for rule.Matched > 0 && value != secret[rule.Matched] {
				rule.Matched = rule.Failure[rule.Matched-1]
			}
			if value == secret[rule.Matched] {
				rule.Matched++
			}
			if rule.Matched == len(secret) {
				end := prior + i + 1
				mark(end-len(secret), end)
				rule.Matched = rule.Failure[rule.Matched-1]
			}
		}
		if finish && rule.Matched > 0 {
			mark(len(b.Pending)-rule.Matched, len(b.Pending))
		}
		return
	}
	// Old pending candidates are already scanned. A short rule only searches
	// newly completed candidates and the bounded boundary overlap.
	for start := max(0, prior-len(secret)+1); start <= len(b.Pending)-len(secret); {
		i := bytes.Index(b.Pending[start:], secret)
		if i < 0 {
			break
		}
		i += start
		mark(i, i+len(secret))
		start = i + 1
	}
	if !trackPrefix {
		return
	}
	rule.Matched = 0
	for n := min(len(secret)-1, len(b.Pending)); n > 0; n-- {
		if bytes.Equal(b.Pending[len(b.Pending)-n:], secret[:n]) {
			rule.Matched = n
			break
		}
	}
	if finish && rule.Matched > 0 {
		mark(len(b.Pending)-rule.Matched, len(b.Pending))
	}
}

// Grow reserves streaming storage without changing existing coverage.
func (b *Buffer) Grow(size, reserve int) {
	if cap(b.Pending) >= size {
		return
	}
	if cap(b.Storage) >= size {
		n := copy(b.Storage[:size], b.Pending)
		copy(b.Marks[:size], b.Hidden)
		b.Pending = b.Storage[:n]
		b.Hidden = b.Marks[:n]
		return
	}
	// The caller chooses its retention policy; reserve room to amortize compaction.
	b.Detach(max(size, reserve))
}

// Detach copies retained data into an independently owned allocation.
func (b *Buffer) Detach(capacity int) {
	if capacity == 0 {
		b.Pending, b.Hidden, b.Storage, b.Marks = nil, nil, nil, nil
		return
	}
	pending := make([]byte, len(b.Pending), capacity)
	copy(pending, b.Pending)
	hidden := make([]bool, len(b.Hidden), capacity)
	copy(hidden, b.Hidden)
	b.Pending, b.Storage = pending, pending
	b.Hidden, b.Marks = hidden, hidden
}

// Prefix compiles the KMP failure table for literal text.
func Prefix(text string) []int {
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
