// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"slices"
	"sync"

	"github.com/gatework/go-expect/internal/redact"
)

// Redactor filters one diagnostic stream independently of any Session. Rules,
// retained prefixes and replacement runs are scoped to this instance. Methods
// are concurrency safe. Formatting never exposes secrets or retained data.
type Redactor struct {
	buffer      redact.Buffer
	mu          sync.Mutex
	secrets     [][]byte
	rules       []redact.Rule
	keep        int
	replacement []byte

	masking bool
}

// NewRedactor copies literal secrets and the nonempty custom replacement.
func NewRedactor(secrets []string, replacement string) (*Redactor, error) {
	if replacement == "" {
		return nil, &ConfigError{Option: "replacement", Reason: "empty replacement"}
	}
	if err := validateSecrets(secrets); err != nil {
		return nil, err
	}
	r := &Redactor{replacement: []byte(replacement)}
	r.set(secrets)
	return r, nil
}
func (*Redactor) String() string { return "expect.Redactor" }

// GoString formats metadata without exposing payloads.
func (r *Redactor) GoString() string { return r.String() }
func validateSecrets(secrets []string) error {
	if slices.Contains(secrets, "") {
		return &ConfigError{Option: "redact", Reason: "empty secret"}
	}
	return nil
}

// SetSecrets ends the old rule scope and returns its safe final bytes. Suspected
// old secret prefixes are masked before new rules take effect. Invalid rules
// leave the existing scope untouched. Callers must deliver the returned bytes.
func (r *Redactor) SetSecrets(secrets []string) ([]byte, error) {
	if err := validateSecrets(secrets); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.append(nil, true)
	r.set(secrets)
	return out, nil
}

// Append returns only bytes that cannot begin a future secret match. It replaces
// each contiguous union of overlapping or adjacent matches once, across chunks.
func (r *Redactor) Append(data []byte) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.append(data, false)
}

// Finish ends a stream and conservatively masks any retained secret prefix.
// Repeated calls are empty; the instance may then filter a fresh stream.
func (r *Redactor) Finish() []byte { r.mu.Lock(); defer r.mu.Unlock(); return r.append(nil, true) }

func (r *Redactor) set(secrets []string) {
	r.secrets = nil
	r.rules = make([]redact.Rule, len(secrets))
	r.keep = 0
	for index, s := range secrets {
		r.secrets = append(r.secrets, []byte(s))
		r.keep = max(r.keep, len(s)-1)
		if len(s) <= literalKMPThreshold {
			continue
		}
		failure := redact.Prefix(s)
		r.rules[index].Failure = failure
	}
}
func (r *Redactor) append(data []byte, finish bool) []byte {
	if len(r.secrets) == 0 {
		// Even the pass-through path returns independently owned bytes.
		return bytes.Clone(data)
	}
	if !finish && r.safeChunk(data) {
		return r.appendSafe(data)
	}
	prior := len(r.buffer.Pending)
	r.buffer.Grow(prior+len(data), 2*r.keep)
	r.buffer.Pending = append(r.buffer.Pending, data...)
	r.buffer.Hidden = r.buffer.Hidden[:len(r.buffer.Pending)]
	clear(r.buffer.Hidden[prior:])
	for index, secret := range r.secrets {
		r.rules[index].Scan(secret, data, &r.buffer, prior, finish, finish)
	}
	end := max(0, len(r.buffer.Pending)-r.keep)
	if finish {
		end = len(r.buffer.Pending)
	}
	var out []byte
	for i := range end {
		if r.buffer.Hidden[i] {
			if !r.masking {
				if out == nil {
					out = make([]byte, 0, max(end, len(r.replacement)))
				}
				out = append(out, r.replacement...)
			}
			r.masking = true
		} else {
			if out == nil {
				out = make([]byte, 0, max(end, len(r.replacement)))
			}
			out = append(out, r.buffer.Pending[i])
			r.masking = false
		}
	}
	r.buffer.Pending = r.buffer.Pending[end:]
	r.buffer.Hidden = r.buffer.Hidden[end:]
	for i := range r.rules {
		r.rules[i].Covered = max(0, r.rules[i].Covered-end)
	}
	// Large transient work detaches once, without speculatively allocating spare
	// storage that Finish may immediately discard. A subsequent append grows a
	// second suffix of room, amortizing compaction for tiny continued chunks.
	if len(r.buffer.Pending) == 0 {
		r.buffer.Pending, r.buffer.Hidden, r.buffer.Storage, r.buffer.Marks = nil, nil, nil, nil
	} else if cap(r.buffer.Storage) > 2*r.keep {
		r.buffer.Detach(len(r.buffer.Pending))
	}
	if finish {
		r.masking = false
		for i := range r.rules {
			r.rules[i].Matched = 0
		}
	}
	return out
}

// safeChunk avoids building and marking a full work buffer when every byte is
// visible. Matches crossing the retained window still require the general path.
func (r *Redactor) safeChunk(data []byte) bool {
	if r.keep >= literalKMPThreshold {
		return false
	}
	for _, hidden := range r.buffer.Hidden {
		if hidden {
			return false
		}
	}
	for _, secret := range r.secrets {
		// Candidate-by-candidate boundary comparisons can be quadratic in the
		// secret length. Keep that work bounded; long rules use the general path.
		if len(secret) > literalKMPThreshold {
			return false
		}
		if bytes.Contains(data, secret) || bytes.Contains(r.buffer.Pending, secret) {
			return false
		}
		for start := max(0, len(r.buffer.Pending)-len(secret)+1); start < len(r.buffer.Pending); start++ {
			n := len(r.buffer.Pending) - start
			if n < len(secret) && len(data) >= len(secret)-n && bytes.Equal(r.buffer.Pending[start:], secret[:n]) && bytes.Equal(data[:len(secret)-n], secret[n:]) {
				return false
			}
		}
	}
	return true
}

func (r *Redactor) appendSafe(data []byte) []byte {
	total := len(r.buffer.Pending) + len(data)
	end := max(0, total-r.keep)
	var out []byte
	if end > 0 {
		out = make([]byte, end)
		n := copy(out, r.buffer.Pending)
		copy(out[n:], data)
		r.masking = false
	}
	// Keep only the bounded suffix; reuse it on subsequent safe chunks. A large
	// input never becomes a retained allocation owned by the redactor.
	tail := total - end
	if r.buffer.Storage == nil && tail > 0 {
		// A cold safe append needs only its suffix. Reserve extra room only
		// when later chunks actually need the general append path.
		r.buffer.Detach(tail)
	} else {
		r.buffer.Grow(tail, 2*r.keep)
	}
	copy(r.buffer.Pending[:tail], r.buffer.Pending[min(end, len(r.buffer.Pending)):])
	r.buffer.Pending = r.buffer.Pending[:tail]
	fromData := min(len(data), tail)
	copy(r.buffer.Pending[tail-fromData:], data[len(data)-fromData:])
	r.buffer.Hidden = r.buffer.Hidden[:tail]
	clear(r.buffer.Hidden)
	return out
}
