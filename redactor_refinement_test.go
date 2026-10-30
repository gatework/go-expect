package expect

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestSessionRedactorReleasesLargeStorage(t *testing.T) {
	for _, tail := range []string{"", "sec"} {
		name := "empty"
		if tail != "" {
			name = "prefix"
		}
		t.Run(name, func(t *testing.T) {
			var r redactor
			r.add([]string{"secret"})
			input := append(bytes.Repeat([]byte("x"), 8<<20), tail...)
			out := r.append(input, false)
			if len(out) != 8<<20 || string(r.pending) != tail {
				t.Fatal("wrong safe output or ambiguous suffix")
			}
			if cap(r.pending) > 256 || cap(r.hidden) > 256 {
				t.Fatalf("tiny suffix retains burst: pending cap=%d hidden cap=%d", cap(r.pending), cap(r.hidden))
			}
			if got := r.append(nil, true); string(got) != strings.Repeat("*", len(tail)) {
				t.Fatalf("finish=%q", got)
			}
			if cap(r.pending) != 0 || cap(r.hidden) != 0 {
				t.Fatal("finish retains stream storage")
			}
			if got := r.append([]byte("secret!"), true); string(got) != "******!" {
				t.Fatalf("restarted stream=%q", got)
			}
		})
	}
}

func TestSessionRedactorLongMixedAdditiveRules(t *testing.T) {
	for _, size := range []int{256, 257, 4096} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			long := strings.Repeat("a", size) + "b"
			var r redactor
			r.add([]string{long, "aaa"})
			var got []byte
			prefix := strings.Repeat("a", size-2)
			for i := range prefix {
				got = append(got, r.append([]byte{prefix[i]}, false)...)
			}
			// The new long rule must be seeded from pending input, while the first
			// rule keeps its incremental state. Duplicate registration is harmless.
			added := strings.Repeat("a", size-1) + "c"
			r.add([]string{added, long})
			suffix := "ac!" + long + "!" + added + "!aa"
			for i := range suffix {
				got = append(got, r.append([]byte{suffix[i]}, false)...)
			}
			got = append(got, r.append(nil, true)...)
			want := referenceRedaction([]byte(prefix+suffix), []string{long, "aaa", added})
			if !bytes.Equal(got, want) {
				t.Fatalf("long additive redaction differs at size %d", size)
			}
		})
	}
}

func TestSessionEmitWithoutSinkSkipsFilter(t *testing.T) {
	s, _ := pair(t)
	if err := s.Redact("secret"); err != nil {
		t.Fatal(err)
	}
	if err := s.emit([]byte("sec"), false); err != nil {
		t.Fatal(err)
	}
	if len(s.redactor.pending) != 0 {
		t.Fatal("no-sink session retained diagnostic bytes")
	}
	if err := s.Redact(""); err == nil {
		t.Fatal("no-sink session skipped secret validation")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.emit(nil, false); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed emit=%v", err)
	}
	if err := s.Redact("secret"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed redact=%v", err)
	}
}

type countedFailureWriter struct {
	calls int
	err   error
}

func (w *countedFailureWriter) Write([]byte) (int, error) {
	w.calls++
	return 0, w.err
}

func TestSessionCloseReleasesFilterAfterSinkFailure(t *testing.T) {
	failure := errors.New("sink failure")
	sink := &countedFailureWriter{err: failure}
	s, _ := pair(t, WithTranscript(sink))
	secret := strings.Repeat("a", 65536) + "b"
	if err := s.Redact(secret); err != nil {
		t.Fatal(err)
	}
	if err := s.emit([]byte("safe"+secret[:len(secret)-1]), false); !errors.Is(err, failure) {
		t.Fatalf("emit: %v", err)
	}
	if len(s.redactor.pending) == 0 {
		t.Fatal("fixture did not retain an ambiguous suffix")
	}
	if err := s.Close(); !errors.Is(err, failure) {
		t.Fatalf("Close lost sink error: %v", err)
	}
	if cap(s.redactor.pending)+cap(s.redactor.hidden)+cap(s.redactor.storage)+cap(s.redactor.marks) != 0 {
		t.Fatal("Close retained filter storage after sink failure")
	}
	if sink.calls != 1 {
		t.Fatalf("sink was retried %d times", sink.calls)
	}
}

// referenceAdditiveRedactor intentionally rescans all pending bytes. Emitted
// bytes are final; added rules may only affect the un-emitted suffix.
type referenceAdditiveRedactor struct {
	secrets []string
	pending []byte
	hidden  []bool
}

func (r *referenceAdditiveRedactor) append(data []byte, finish bool) []byte {
	r.pending = append(r.pending, data...)
	r.hidden = append(r.hidden, make([]bool, len(data))...)
	keep := 0
	for _, secret := range r.secrets {
		for start := range r.pending {
			n := min(len(secret), len(r.pending)-start)
			if !bytes.Equal(r.pending[start:start+n], []byte(secret)[:n]) {
				continue
			}
			if n == len(secret) {
				for i := start; i < start+n; i++ {
					r.hidden[i] = true
				}
			} else {
				keep = max(keep, n)
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
	out := bytes.Clone(r.pending[:end])
	for i := range out {
		if r.hidden[i] {
			out[i] = '*'
		}
	}
	r.pending = r.pending[end:]
	r.hidden = r.hidden[end:]
	return out
}
func FuzzSessionRedactorAdditiveRules(f *testing.F) {
	f.Add([]byte("ababab!abc!ab"), "aba", "abc", uint16(3), uint8(1))
	f.Add([]byte(strings.Repeat("a", 700)+"c"), strings.Repeat("a", 400)+"b", strings.Repeat("a", 300)+"c", uint16(301), uint8(32))
	f.Add([]byte{0xff, 0, 0xff, 0, 1}, "\xff\x00\xff", "\x00\xff\x00", uint16(2), uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, first, added string, at uint16, size uint8) {
		if len(data) > 4096 || len(first) > 1024 || len(added) > 1024 || first == "" || added == "" {
			return
		}
		split := int(at) % (len(data) + 1)
		var r redactor
		r.add([]string{first})
		oracle := referenceAdditiveRedactor{secrets: []string{first}}
		chunk := int(size)%64 + 1
		var got, want []byte
		feed := func(input []byte) {
			for len(input) > 0 {
				n := min(chunk, len(input))
				got = append(got, r.append(input[:n], false)...)
				want = append(want, oracle.append(input[:n], false)...)
				if !bytes.Equal(got, want) {
					t.Fatalf("stream differs: got=%x want=%x", got, want)
				}
				input = input[n:]
			}
		}
		feed(data[:split])
		r.add([]string{added, first})
		oracle.secrets = append(oracle.secrets, added)
		feed(data[split:])
		got = append(got, r.append(nil, true)...)
		want = append(want, oracle.append(nil, true)...)
		if !bytes.Equal(got, want) {
			t.Fatalf("final differs: got=%x want=%x", got, want)
		}
	})
}
