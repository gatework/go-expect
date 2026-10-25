package expect

import "bytes"

// redactor retains only an ambiguous suffix. Hidden flags survive overlap and
// rule additions, so a byte once suppressed can never become visible again.
// The owning Session serializes access; raw protocol buffers never enter it.
type redactor struct {
	secrets [][]byte
	pending []byte
	hidden  []bool
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
		if !duplicate {
			r.secrets = append(r.secrets, []byte(s))
		}
	}
}
func (r *redactor) append(data []byte, finish bool) []byte {
	if len(r.secrets) == 0 {
		return data
	}
	r.pending = append(r.pending, data...)
	r.hidden = append(r.hidden, make([]bool, len(data))...)
	keep := 0
	for _, secret := range r.secrets {
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
		max := len(secret) - 1
		if max > len(r.pending) {
			max = len(r.pending)
		}
		for n := max; n > 0; n-- {
			if bytes.Equal(r.pending[len(r.pending)-n:], secret[:n]) {
				if n > keep {
					keep = n
				}
				break
			}
		}
	}
	end := len(r.pending) - keep
	if finish {
		for i := end; i < len(r.pending); i++ {
			r.hidden[i] = true
		}
		end = len(r.pending)
	}
	out := append([]byte(nil), r.pending[:end]...)
	for i := range out {
		if r.hidden[i] {
			out[i] = '*'
		}
	}
	copy(r.pending, r.pending[end:])
	r.pending = r.pending[:len(r.pending)-end]
	copy(r.hidden, r.hidden[end:])
	r.hidden = r.hidden[:len(r.hidden)-end]
	return out
}
