// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
)

func TestPublicRedactorDetachedOutput(t *testing.T) {
	for _, secrets := range [][]string{nil, {"secret"}} {
		r, err := NewRedactor(secrets, "[hidden]")
		if err != nil {
			t.Fatal(err)
		}
		input := []byte("ordinary text")
		first := r.Append(input)
		snapshot := bytes.Clone(first)
		for i := range input {
			input[i] = 'x'
		}
		_ = r.Append([]byte("another long ordinary chunk"))
		_ = r.Finish()
		if !bytes.Equal(first, snapshot) {
			t.Fatalf("returned bytes changed: %q", first)
		}
		if len(first) > 0 {
			first[0] = '!'
		}
		if got := r.Finish(); len(got) != 0 {
			t.Fatalf("repeated finish: %q", got)
		}
	}
}

func TestPublicRedactorSafeChunkAllocations(t *testing.T) {
	for _, secrets := range [][]string{nil, {"secret"}} {
		name := "no_rules"
		if len(secrets) > 0 {
			name = "with_rule"
		}
		t.Run(name, func(t *testing.T) {
			r, err := NewRedactor(secrets, "[hidden]")
			if err != nil {
				t.Fatal(err)
			}
			chunk := bytes.Repeat([]byte("x"), 256)
			_ = r.Append(chunk)
			allocations := testing.AllocsPerRun(100, func() {
				if out := r.Append(chunk); len(out) == 0 {
					t.Fatal("missing safe output")
				}
			})
			if allocations > 1 {
				t.Fatalf("safe chunk allocated %.0f times, want at most one detached output", allocations)
			}
		})
	}
}

func TestPublicRedactorRetainedWindow(t *testing.T) {
	r, err := NewRedactor([]string{"secret"}, "*")
	if err != nil {
		t.Fatal(err)
	}
	input := append(bytes.Repeat([]byte("x"), 1<<20), 's')
	_ = r.Append(input)
	if len(r.buffer.Pending) > 5 || cap(r.buffer.Pending) > 2*5 || cap(r.buffer.Hidden) > 2*5 {
		t.Fatalf("retained data len=%d cap=%d flags=%d", len(r.buffer.Pending), cap(r.buffer.Pending), cap(r.buffer.Hidden))
	}
	if got := string(r.Finish()); got != "xxxx*" {
		t.Fatalf("finish=%q", got)
	}
	if cap(r.buffer.Pending) != 0 || cap(r.buffer.Hidden) != 0 {
		t.Fatal("finish retains stream buffers")
	}
}

func FuzzPublicRedactorChunks(f *testing.F) {
	f.Add([]byte("abab password pass"), "aba", "password", "[hidden]", uint8(1))
	f.Add([]byte("ababa"), "aba", "bab", "*", uint8(3))
	f.Add([]byte{0, 0xff, 1, 0, 0xff}, string([]byte{0, 0xff}), "", "#", uint8(1))
	f.Add(bytes.Repeat([]byte("a"), 1024), string(bytes.Repeat([]byte("a"), 257)), string(bytes.Repeat([]byte("a"), 512))+"b", "[X]", uint8(31))
	f.Add(bytes.Repeat([]byte{0, 0xff, 1}, 600), string(bytes.Repeat([]byte{0, 0xff, 1}, 100)), "\xff\x01", "#", uint8(3))
	f.Fuzz(func(t *testing.T, input []byte, a, b, replacement string, size uint8) {
		if len(input) > 4096 || len(a) > 1024 || len(b) > 1024 || replacement == "" || len(replacement) > 64 {
			return
		}
		var secrets []string
		for _, s := range []string{a, b} {
			if s != "" {
				secrets = append(secrets, s)
			}
		}
		r, err := NewRedactor(secrets, replacement)
		if err != nil {
			t.Fatal(err)
		}
		var got []byte
		chunk := int(size)%32 + 1
		for pos := 0; pos < len(input); pos += chunk {
			got = append(got, r.Append(input[pos:min(pos+chunk, len(input))])...)
		}
		got = append(got, r.Finish()...)
		if want := referencePublicRedaction(input, secrets, []byte(replacement)); !bytes.Equal(got, want) {
			t.Fatalf("got=%x want=%x", got, want)
		}

		split := len(input) / 2
		got = nil
		for pos := 0; pos < split; pos += chunk {
			got = append(got, r.Append(input[pos:min(pos+chunk, split)])...)
		}
		var next []string
		if b != "" {
			next = []string{b}
		}
		flushed, err := r.SetSecrets(next)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, flushed...)
		for pos := split; pos < len(input); pos += chunk {
			got = append(got, r.Append(input[pos:min(pos+chunk, len(input))])...)
		}
		got = append(got, r.Finish()...)
		want := referencePublicRedaction(input[:split], secrets, []byte(replacement))
		want = append(want, referencePublicRedaction(input[split:], next, []byte(replacement))...)
		if !bytes.Equal(got, want) {
			t.Fatalf("switched rules got=%x want=%x", got, want)
		}
	})
}

func referencePublicRedaction(input []byte, secrets []string, replacement []byte) []byte {
	marked := make([]bool, len(input))
	for pos := range input {
		for _, secret := range secrets {
			n := min(len(secret), len(input)-pos)
			if bytes.Equal(input[pos:pos+n], []byte(secret)[:n]) {
				for i := pos; i < pos+n; i++ {
					marked[i] = true
				}
			}
		}
	}
	var out []byte
	masking := false
	for i, value := range input {
		if marked[i] {
			if !masking {
				out = append(out, replacement...)
			}
			masking = true
		} else {
			out = append(out, value)
			masking = false
		}
	}
	return out
}

func TestPublicRedactorMaskedByteAllocations(t *testing.T) {
	for _, entry := range []struct {
		name    string
		secrets []string
		input   []byte
	}{
		{"matches", []string{"password", "token"}, bytes.Repeat([]byte("password token "), 64)},
		{"overlaps", []string{"aba", "bab"}, bytes.Repeat([]byte("abab "), 200)},
	} {
		t.Run(entry.name, func(t *testing.T) {
			r, err := NewRedactor(entry.secrets, "[hidden]")
			if err != nil {
				t.Fatal(err)
			}
			allocations := testing.AllocsPerRun(10, func() {
				for i := range entry.input {
					_ = r.Append(entry.input[i : i+1])
				}
				_ = r.Finish()
			})
			if allocations > float64(3*len(entry.input)) {
				t.Fatalf("%.0f allocations for %d masked byte fragments", allocations, len(entry.input))
			}
		})
	}
}

func TestPublicRedactorLongSecretBoundaryFallback(t *testing.T) {
	const size = 4096
	prefix := bytes.Repeat([]byte("a"), size)
	secret := string(prefix) + "b"
	r, err := NewRedactor([]string{secret}, "[X]")
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Append(prefix); len(got) != 0 {
		t.Fatalf("incomplete prefix emitted %d bytes", len(got))
	}

	if r.safeChunk(prefix) {
		t.Fatal("long repeated-prefix rule entered boundary fast path")
	}
	got := r.Append(prefix)
	got = append(got, r.Append([]byte("b!"))...)
	got = append(got, r.Finish()...)
	want := append(bytes.Clone(prefix), "[X]!"...)
	if !bytes.Equal(got, want) {
		t.Fatalf("output length=%d want=%d", len(got), len(want))
	}
	r, err = NewRedactor([]string{secret}, "[X]")
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Append(prefix); len(got) != 0 {
		t.Fatal("incomplete long prefix escaped")
	}
	if got := string(r.Finish()); got != "[X]" {
		t.Fatalf("unfinished long prefix=%q", got)
	}
}

func TestSessionRedactorReleasesLargeStorage(t *testing.T) {
	for _, tail := range []string{"", "sec"} {
		name := "empty"
		if tail != "" {
			name = "prefix"
		}
		t.Run(name, func(t *testing.T) {
			var r diagnosticRedactor
			r.add([]string{"secret"})
			input := append(bytes.Repeat([]byte("x"), 8<<20), tail...)
			out := r.append(input, false)
			if len(out) != 8<<20 || string(r.buffer.Pending) != tail {
				t.Fatal("wrong safe output or ambiguous suffix")
			}
			if cap(r.buffer.Pending) > 256 || cap(r.buffer.Hidden) > 256 {
				t.Fatalf("tiny suffix retains burst: pending cap=%d hidden cap=%d", cap(r.buffer.Pending), cap(r.buffer.Hidden))
			}
			if got := r.append(nil, true); string(got) != strings.Repeat("*", len(tail)) {
				t.Fatalf("finish=%q", got)
			}
			if cap(r.buffer.Pending) != 0 || cap(r.buffer.Hidden) != 0 {
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
			var r diagnosticRedactor
			r.add([]string{long, "aaa"})
			var got []byte
			prefix := strings.Repeat("a", size-2)
			for i := range prefix {
				got = append(got, r.append([]byte{prefix[i]}, false)...)
			}

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
	if len(s.diagnostics.redactor.secrets) != 0 || len(s.diagnostics.redactor.rules) != 0 {
		t.Fatal("no-sink session retained rules")
	}
	if len(s.diagnostics.redactor.buffer.Pending) != 0 {
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

func TestSessionCloseReleasesFilterAfterSinkFailure(t *testing.T) {
	failure := errors.New("sink failure")
	sink := &countedFailureWriter{err: failure}
	s, _ := pair(t, WithOutput(sink))
	secret := strings.Repeat("a", 65536) + "b"
	if err := s.Redact(secret); err != nil {
		t.Fatal(err)
	}
	if err := s.emit([]byte("safe"+secret[:len(secret)-1]), false); !errors.Is(err, failure) {
		t.Fatalf("emit: %v", err)
	}
	if len(s.diagnostics.redactor.buffer.Pending) == 0 {
		t.Fatal("fixture did not retain an ambiguous suffix")
	}
	if err := s.Close(); !errors.Is(err, failure) {
		t.Fatalf("Close lost sink error: %v", err)
	}
	if cap(s.diagnostics.redactor.buffer.Pending)+cap(s.diagnostics.redactor.buffer.Hidden)+cap(s.diagnostics.redactor.buffer.Storage)+cap(s.diagnostics.redactor.buffer.Marks) != 0 {
		t.Fatal("Close retained filter storage after sink failure")
	}
	if len(s.diagnostics.redactor.secrets) != 0 || len(s.diagnostics.redactor.rules) != 0 {
		t.Fatal("closed session retained rules")
	}
	if sink.calls != 1 {
		t.Fatalf("sink was retried %d times", sink.calls)
	}
}

type referenceAdditiveRedactor struct {
	secrets []string
	pending []byte
	hidden  []bool
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
		var r diagnosticRedactor
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
