package expect

import (
	"bytes"
	"errors"
	"sync"
)

// Redactor filters one diagnostic stream independently of any Session. Rules,
// retained prefixes and replacement runs are scoped to this instance. Methods
// are concurrency safe. Formatting never exposes secrets or retained data.
type Redactor struct {
	mu          sync.Mutex
	secrets     [][]byte
	rules       []redactionRule
	keep        int
	replacement []byte
	pending     []byte
	hidden      []bool
	storage     []byte
	marks       []bool
	masking     bool
}

type redactionRule struct {
	failure []int
	matched int
	covered int
}

// NewRedactor copies literal secrets and the nonempty custom replacement.
func NewRedactor(secrets []string, replacement string) (*Redactor, error) {
	if replacement == "" {
		return nil, errors.New("expect: empty replacement")
	}
	if err := validateSecrets(secrets); err != nil {
		return nil, err
	}
	r := &Redactor{replacement: []byte(replacement)}
	r.set(secrets)
	return r, nil
}
func (*Redactor) String() string     { return "expect.Redactor" }
func (r *Redactor) GoString() string { return r.String() }
func validateSecrets(secrets []string) error {
	for _, s := range secrets {
		if s == "" {
			return errors.New("expect: empty secret")
		}
	}
	return nil
}
func (r *Redactor) set(secrets []string) {
	r.secrets = nil
	r.rules = make([]redactionRule, len(secrets))
	r.keep = 0
	for index, s := range secrets {
		r.secrets = append(r.secrets, []byte(s))
		r.keep = max(r.keep, len(s)-1)
		if len(s) <= 256 {
			continue
		}
		failure := make([]int, len(s))
		for i, matched := 1, 0; i < len(s); i++ {
			for matched > 0 && s[i] != s[matched] {
				matched = failure[matched-1]
			}
			if s[i] == s[matched] {
				matched++
			}
			failure[i] = matched
		}
		r.rules[index].failure = failure
	}
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
func (r *Redactor) append(data []byte, finish bool) []byte {
	if len(r.secrets) == 0 {
		// Even the pass-through path returns independently owned bytes.
		return bytes.Clone(data)
	}
	if !finish && r.safeChunk(data) {
		return r.appendSafe(data)
	}
	prior := len(r.pending)
	r.grow(prior + len(data))
	r.pending = append(r.pending, data...)
	r.hidden = r.hidden[:len(r.pending)]
	clear(r.hidden[prior:])
	for index, secret := range r.secrets {
		rule := &r.rules[index]
		covered := rule.covered
		mark := func(start, end int) {
			// Occurrences arrive in order. Their covered intervals can only
			// extend rightward, so dense overlaps never re-mark the same range.
			for i := max(start, covered); i < end; i++ {
				r.hidden[i] = true
			}
			covered = max(covered, end)
		}
		if len(rule.failure) > 0 {
			for i, value := range data {
				for rule.matched > 0 && value != secret[rule.matched] {
					rule.matched = rule.failure[rule.matched-1]
				}
				if value == secret[rule.matched] {
					rule.matched++
				}
				if rule.matched == len(secret) {
					end := prior + i + 1
					mark(end-len(secret), end)
					rule.matched = rule.failure[rule.matched-1]
				}
			}
			if finish && rule.matched > 0 {
				mark(len(r.pending)-rule.matched, len(r.pending))
			}
		} else {
			// Old pending bytes have already been searched (or proved safe by
			// safeChunk). Only boundary candidates and newly appended bytes can
			// add a match, even when a different long rule retains a huge suffix.
			for start := max(0, prior-len(secret)+1); start <= len(r.pending)-len(secret); {
				i := bytes.Index(r.pending[start:], secret)
				if i < 0 {
					break
				}
				i += start
				mark(i, i+len(secret))
				start = i + 1
			}
		}
		if finish && len(rule.failure) == 0 {
			for n := min(len(secret)-1, len(r.pending)); n > 0; n-- {
				if bytes.Equal(r.pending[len(r.pending)-n:], secret[:n]) {
					mark(len(r.pending)-n, len(r.pending))
					break
				}
			}
		}
		rule.covered = covered
	}
	end := max(0, len(r.pending)-r.keep)
	if finish {
		end = len(r.pending)
	}
	var out []byte
	for i := 0; i < end; i++ {
		if r.hidden[i] {
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
			out = append(out, r.pending[i])
			r.masking = false
		}
	}
	r.pending = r.pending[end:]
	r.hidden = r.hidden[end:]
	for i := range r.rules {
		r.rules[i].covered = max(0, r.rules[i].covered-end)
	}
	// Large transient work detaches once, without speculatively allocating spare
	// storage that Finish may immediately discard. A subsequent append grows a
	// second suffix of room, amortizing compaction for tiny continued chunks.
	if len(r.pending) == 0 {
		r.pending, r.hidden, r.storage, r.marks = nil, nil, nil, nil
	} else if cap(r.storage) > 2*r.keep {
		r.detach(len(r.pending))
	}
	if finish {
		r.masking = false
		for i := range r.rules {
			r.rules[i].matched = 0
		}
	}
	return out
}

func (r *Redactor) grow(size int) {
	if cap(r.pending) >= size {
		return
	}
	if cap(r.storage) >= size {
		n := copy(r.storage[:size], r.pending)
		copy(r.marks[:size], r.hidden)
		r.pending = r.storage[:n]
		r.hidden = r.marks[:n]
		return
	}
	r.detach(max(size, 2*r.keep))
}

func (r *Redactor) detach(capacity int) {
	pending := make([]byte, len(r.pending), capacity)
	copy(pending, r.pending)
	hidden := make([]bool, len(r.hidden), capacity)
	copy(hidden, r.hidden)
	r.storage, r.pending = pending, pending
	r.marks, r.hidden = hidden, hidden
}

// safeChunk avoids building and marking a full work buffer when every byte is
// visible. Matches crossing the retained window still require the general path.
func (r *Redactor) safeChunk(data []byte) bool {
	if r.keep >= 256 {
		return false
	}
	for _, hidden := range r.hidden {
		if hidden {
			return false
		}
	}
	for _, secret := range r.secrets {
		// Candidate-by-candidate boundary comparisons can be quadratic in the
		// secret length. Keep that work bounded; long rules use the general path.
		if len(secret) > 256 {
			return false
		}
		if bytes.Contains(data, secret) || bytes.Contains(r.pending, secret) {
			return false
		}
		for start := max(0, len(r.pending)-len(secret)+1); start < len(r.pending); start++ {
			n := len(r.pending) - start
			if n < len(secret) && len(data) >= len(secret)-n && bytes.Equal(r.pending[start:], secret[:n]) && bytes.Equal(data[:len(secret)-n], secret[n:]) {
				return false
			}
		}
	}
	return true
}

func (r *Redactor) appendSafe(data []byte) []byte {
	total := len(r.pending) + len(data)
	end := max(0, total-r.keep)
	var out []byte
	if end > 0 {
		out = make([]byte, end)
		n := copy(out, r.pending)
		copy(out[n:], data)
		r.masking = false
	}
	// Keep only the bounded suffix; reuse it on subsequent safe chunks. A large
	// input never becomes a retained allocation owned by the redactor.
	tail := total - end
	if r.storage == nil && tail > 0 {
		// A cold safe append needs only its suffix. Reserve extra room only
		// when later chunks actually need the general append path.
		r.detach(tail)
	} else {
		r.grow(tail)
	}
	copy(r.pending[:tail], r.pending[min(end, len(r.pending)):])
	r.pending = r.pending[:tail]
	fromData := min(len(data), tail)
	copy(r.pending[tail-fromData:], data[len(data)-fromData:])
	r.hidden = r.hidden[:tail]
	clear(r.hidden)
	return out
}
