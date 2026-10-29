package expect

import (
	"bytes"
	"testing"
)

func TestPublicRedactorDenseWarmSuffixReuse(t *testing.T) {
	r, err := NewRedactor([]string{string(bytes.Repeat([]byte("a"), 4096))}, "[X]")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(r.Append(bytes.Repeat([]byte("a"), 8192))); got != "[X]" {
		t.Fatalf("dense replacement=%q", got)
	}
	allocations := testing.AllocsPerRun(100, func() {
		if got := r.Append([]byte("a")); len(got) != 0 {
			t.Fatalf("continued covered interval emitted %q", got)
		}
	})
	if allocations != 0 {
		t.Fatalf("warm covered suffix allocated %.0f times, want zero", allocations)
	}
	if got := r.Finish(); len(got) != 0 {
		t.Fatalf("covered finish emitted %q", got)
	}
}

func TestPublicRedactorLongDenseDifferential(t *testing.T) {
	for _, size := range []int{257, 1024, 4096} {
		secret := string(bytes.Repeat([]byte("ab"), size)) + "a"
		input := append(bytes.Repeat([]byte("ab"), size*3), "!abab"...)
		for _, chunk := range []int{1, 31, 1024, len(input)} {
			r, err := NewRedactor([]string{secret, "bab"}, "[X]")
			if err != nil {
				t.Fatal(err)
			}
			var got []byte
			for pos := 0; pos < len(input); pos += chunk {
				got = append(got, r.Append(input[pos:min(pos+chunk, len(input))])...)
			}
			got = append(got, r.Finish()...)
			if want := referencePublicRedaction(input, []string{secret, "bab"}, []byte("[X]")); !bytes.Equal(got, want) {
				t.Fatalf("size=%d chunk=%d got=%x want=%x", size, chunk, got, want)
			}
		}
	}
}

func TestPublicRedactorShortRulesAcrossLongRetainedSuffix(t *testing.T) {
	secrets := []string{string(bytes.Repeat([]byte("a"), 1024)), "a", "bc", "d!"}
	input := append(bytes.Repeat([]byte("a"), 4096), "bcbc !d"...)
	r, err := NewRedactor(secrets, "[X]")
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for _, value := range input {
		got = append(got, r.Append([]byte{value})...)
	}
	got = append(got, r.Finish()...)
	if want := referencePublicRedaction(input, secrets, []byte("[X]")); !bytes.Equal(got, want) {
		t.Fatalf("got=%q want=%q", got, want)
	}
}
