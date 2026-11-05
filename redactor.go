// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"

	"github.com/gatework/go-expect/internal/redact"
)

// redactor retains only an ambiguous suffix. Hidden flags survive overlap and
// rule additions, so a byte once suppressed can never become visible again.
// The owning Session serializes access; raw protocol buffers never enter it.
type redactor struct {
	buffer  redact.Buffer
	secrets [][]byte
	rules   []redact.Rule
}

func (r *redactor) add(secrets []string) {
	for _, s := range secrets {
		duplicate := false
		for _, old := range r.secrets {
			if string(old) == s {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		secret := []byte(s)
		rule := redact.Rule{}
		if len(secret) > literalKMPThreshold {
			rule.Failure = redact.Prefix(s)
		}
		// Only a new rule scans the existing suffix. It cannot reach bytes already
		// emitted; existing rules keep both their prefix state and hidden coverage.
		rule.Scan(secret, r.buffer.Pending, &r.buffer, 0, false, true)
		r.secrets = append(r.secrets, secret)
		r.rules = append(r.rules, rule)
	}
}

func (r *redactor) append(data []byte, finish bool) []byte {
	if len(r.secrets) == 0 {
		return data
	}
	if len(r.buffer.Pending) == 0 && !finish {
		safe := true
		for _, secret := range r.secrets {
			if bytes.IndexByte(data, secret[0]) >= 0 {
				safe = false
				break
			}
		}
		if safe {
			return data
		}
	}
	prior := len(r.buffer.Pending)
	r.buffer.Grow(prior+len(data), 2*len(r.buffer.Pending))
	r.buffer.Pending = append(r.buffer.Pending, data...)
	r.buffer.Hidden = r.buffer.Hidden[:len(r.buffer.Pending)]
	clear(r.buffer.Hidden[prior:])
	keep := 0
	for i, secret := range r.secrets {
		rule := &r.rules[i]
		rule.Scan(secret, data, &r.buffer, prior, false, true)
		keep = max(keep, rule.Matched)
	}
	end := len(r.buffer.Pending) - keep
	if finish {
		for i := end; i < len(r.buffer.Pending); i++ {
			r.buffer.Hidden[i] = true
		}
		end = len(r.buffer.Pending)
	}
	out := bytes.Clone(r.buffer.Pending[:end])
	for i := range out {
		if r.buffer.Hidden[i] {
			out[i] = '*'
		}
	}
	r.buffer.Pending = r.buffer.Pending[end:]
	r.buffer.Hidden = r.buffer.Hidden[end:]
	for i := range r.rules {
		r.rules[i].Covered = max(0, r.rules[i].Covered-end)
	}
	switch {
	case finish:
		r.resetStream()
	case cap(r.buffer.Storage) > max(64<<10, 4*len(r.buffer.Pending)):
		// Detach a small suffix from a large transient input. Cold retention needs
		// no speculative spare capacity; grow reserves room on the next append.
		r.buffer.Detach(len(r.buffer.Pending))
	}
	return out
}

func (r *redactor) resetStream() {
	r.buffer.Pending, r.buffer.Hidden, r.buffer.Storage, r.buffer.Marks = nil, nil, nil, nil
	for i := range r.rules {
		r.rules[i].Matched, r.rules[i].Covered = 0, 0
	}
}
