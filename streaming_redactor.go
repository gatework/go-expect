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
	replacement []byte
	pending     []byte
	hidden      []bool
	masking     bool
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
	for _, s := range secrets {
		r.secrets = append(r.secrets, []byte(s))
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
	r.pending = append(r.pending, data...)
	r.hidden = append(r.hidden, make([]bool, len(data))...)
	keep := 0
	for _, secret := range r.secrets {
		keep = max(keep, len(secret)-1)
		for start := 0; start <= len(r.pending)-len(secret); {
			i := bytes.Index(r.pending[start:], secret)
			if i < 0 {
				break
			}
			i += start
			for j := i; j < i+len(secret); j++ {
				r.hidden[j] = true
			}
			start = i + 1
		}
		if finish {
			for n := min(len(secret)-1, len(r.pending)); n > 0; n-- {
				if bytes.Equal(r.pending[len(r.pending)-n:], secret[:n]) {
					for i := len(r.pending) - n; i < len(r.pending); i++ {
						r.hidden[i] = true
					}
					break
				}
			}
		}
	}
	end := max(0, len(r.pending)-keep)
	if finish {
		end = len(r.pending)
	}
	var out []byte
	for i := 0; i < end; i++ {
		if r.hidden[i] {
			if !r.masking {
				out = append(out, r.replacement...)
			}
			r.masking = true
		} else {
			out = append(out, r.pending[i])
			r.masking = false
		}
	}
	// Detach the retained window so a large append does not remain live.
	r.pending = append([]byte(nil), r.pending[end:]...)
	r.hidden = append([]bool(nil), r.hidden[end:]...)
	if finish {
		r.masking = false
	}
	return out
}
