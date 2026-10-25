package expect

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestRedactorBoundaries(t *testing.T) {
	cases := []struct {
		name           string
		secrets, parts []string
		want           string
	}{
		{"overlap", []string{"aba", "bab"}, []string{"a", "b", "a", "b"}, "****"},
		{"unfinished", []string{"password"}, []string{"hello pa", "ss"}, "hello ****"},
		{"diverging suffix", []string{"password"}, []string{"pa", "no"}, "pano"},
		{"binary", []string{string([]byte{0xff, 0x00})}, []string{string([]byte{0xff}), string([]byte{0x00, '!'})}, "**!"},
		{"no secrets", nil, []string{"hello", " world"}, "hello world"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var r redactor
			r.add(tt.secrets)
			var out []byte
			for _, part := range tt.parts {
				out = append(out, r.append([]byte(part), false)...)
			}
			out = append(out, r.append(nil, true)...)
			if string(out) != tt.want {
				t.Fatalf("got %q want %q", out, tt.want)
			}
		})
	}
}
func TestRedactorRuleAdditionPreservesHiddenBytes(t *testing.T) {
	var r redactor
	r.add([]string{"aba"})
	got := r.append([]byte("aba"), false)
	r.add([]string{"abc"})
	got = append(got, r.append([]byte("bc!"), true)...)
	if string(got) != "*****!" {
		t.Fatalf("got %q", got)
	}
}
func TestRedactorCloseMasksPendingPrefix(t *testing.T) {
	var out bytes.Buffer
	s, p := pair(t, WithTranscript(&out))
	if err := s.Redact("password"); err != nil {
		t.Fatal(err)
	}
	go p.Write([]byte("pa"))
	waitText(t, s, "pa")
	if out.Len() != 0 {
		t.Fatalf("prefix exposed: %q", out.String())
	}
	s.Close()
	if out.String() != "**" {
		t.Fatalf("close output %q", out.String())
	}
}
func TestDiagnosticFormatting(t *testing.T) {
	p := Literal("secret")
	r, _ := NewResponder(Rule{Pattern: p, Respond: Reply("secret")})
	for _, v := range []any{p, r, Rule{Pattern: p, Respond: Reply("secret")}, Capture{Text: "secret", Matched: true}, Case{Pattern: p}, &WriteError{Cause: fmt.Errorf("secret")}} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", v, v, v), "secret") {
			t.Fatalf("formatting leaked %T", v)
		}
	}
}

// Reference filtering scans a complete string independently of streaming state.
func referenceRedaction(input []byte, secrets []string) []byte {
	output := append([]byte(nil), input...)
	for start := range input {
		for _, secret := range secrets {
			if len(secret) == 0 {
				continue
			}
			rest := input[start:]
			n := len(secret)
			if len(rest) < n {
				n = len(rest)
			}
			if bytes.Equal(rest[:n], []byte(secret)[:n]) {
				for j := start; j < start+n; j++ {
					output[j] = '*'
				}
			}
		}
	}
	return output
}
func FuzzRedactorChunks(f *testing.F) {
	f.Add([]byte("abab password pass"), "aba", "password", uint8(1))
	f.Add([]byte("ababa"), "aba", "bab", uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, a, b string, size uint8) {
		if len(data) > 4096 || len(a) > 64 || len(b) > 64 {
			return
		}
		var secrets []string
		for _, v := range []string{a, b} {
			if v != "" {
				secrets = append(secrets, v)
			}
		}
		var r redactor
		r.add(secrets)
		var got []byte
		chunk := int(size)%32 + 1
		for pos := 0; pos < len(data); {
			end := pos + chunk
			if end > len(data) {
				end = len(data)
			}
			got = append(got, r.append(data[pos:end], false)...)
			pos = end
		}
		got = append(got, r.append(nil, true)...)
		if want := referenceRedaction(data, secrets); !bytes.Equal(got, want) {
			t.Fatalf("got %x want %x", got, want)
		}
	})
}
func FuzzMatchChunks(f *testing.F) {
	f.Add([]byte("hello world"), "world", uint8(1))
	f.Fuzz(func(t *testing.T, data []byte, needle string, size uint8) {
		if len(data) > 4096 || needle == "" || len(needle) > 256 {
			return
		}
		pattern := Literal(needle)
		chunk := int(size)%32 + 1
		scanned := 0
		found := -1
		for end := 0; ; {
			from := scanned - len(needle) + 1
			if from < 0 {
				from = 0
			}
			loc, ok, err := pattern.find(data[:end], false, from)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				found = loc.start
				break
			}
			if end == len(data) {
				break
			}
			scanned = end
			end += chunk
			if end > len(data) {
				end = len(data)
			}
		}
		if want := bytes.Index(data, []byte(needle)); found != want {
			t.Fatalf("got %d want %d", found, want)
		}
	})
}
