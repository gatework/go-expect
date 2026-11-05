package expect

import (
	"regexp"
	"testing"
)

func TestLineMatchesPriorPromptPattern(t *testing.T) {
	for _, literal := range []string{"sw#", "设备>", "  literal "} {
		p := Line(literal)
		re := regexp.MustCompile(`(?:\A|[\r\n])[ \t\x00]*` + regexp.QuoteMeta(literal) + `[ \t\x00]*[\r\n]*\z`)
		for _, before := range []string{"", "body\n", "body\r\n", "prefix"} {
			for _, pad := range []string{"", " \t\x00"} {
				for _, after := range []string{"", " \t\x00", "\r\n", " \r\n\n", "\n "} {
					input := before + pad + literal + after
					got, ok, err := p.find([]byte(input), false, 0)
					want := re.FindStringIndex(input)
					if err != nil || ok != (want != nil) || ok && (got.start != want[0] || got.end != want[1]) {
						t.Fatalf("%q: %+v %v %v want %v", input, got, ok, err, want)
					}
				}
			}
		}
	}
}
func TestPatternTextPreflight(t *testing.T) {
	if !(Pattern{}).IsZero() || Literal("").IsZero() {
		t.Fatal("zero distinction")
	}
	empty, _ := Compile("a*")
	for _, p := range []Pattern{{}, Literal(""), Line(""), Line("a\nb"), EOF(), All(Literal("x"), EOF()), empty} {
		if p.ValidateText() == nil {
			t.Fatal("accepted invalid text pattern")
		}
	}
	for _, p := range []Pattern{Line("\xd6\xd0>"), Literal("x"), All(Literal("x"))} {
		if err := p.ValidateText(); err != nil {
			t.Fatal(err)
		}
	}
	hit, ok, err := Line("\xd6\xd0>").find([]byte("body\r\n\xd6\xd0> "), false, 0)
	if err != nil || !ok || hit.start != 5 {
		t.Fatalf("binary line %+v %v %v", hit, ok, err)
	}
}

func FuzzLineRegexParity(f *testing.F) {
	for _, input := range []string{"x\r\nsw# \r\n", " sw#", "body\nsw#\n ", "\x00sw#\x00", "sw#sw#", "body\r\n设备> "} {
		f.Add(input, uint8(0))
	}
	f.Fuzz(func(t *testing.T, input string, index uint8) {
		if len(input) > 65536 {
			t.Skip()
		}
		literals := []string{"sw#", "设备>", " a ", " "}
		literal := literals[int(index)%len(literals)]
		p := Line(literal)
		r := regexp.MustCompile(`(?:\A|[\r\n])[ \t\x00]*` + regexp.QuoteMeta(literal) + `[ \t\x00]*[\r\n]*\z`)
		got, ok, err := p.find([]byte(input), false, 0)
		want := r.FindStringIndex(input)
		if err != nil || ok != (want != nil) || ok && (got.start != want[0] || got.end != want[1]) {
			t.Fatalf("offset mismatch %+v %v want %v", got, ok, want)
		}
	})
}
