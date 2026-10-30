package expect

import (
	"errors"
	"reflect"
	"regexp"
	"testing"
)

func TestRegexpWinnerCapturesBeforeGuardAndDetached(t *testing.T) {
	for _, declaration := range []bool{false, true} {
		name := "earliest"
		if declaration {
			name = "declaration"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := pair(t)
			input := []byte("z:ab!")
			s.pending = input
			patterns := []Case{{Pattern: mustPattern(t, `(a)(b)?()!`)}, {Pattern: mustPattern(t, `(z)()(x)?`)}}
			chosen := 1
			want := []Capture{{Text: "z", Matched: true}, {Matched: true}, {}}
			var opts []ExpectOption
			if declaration {
				chosen = 0
				want = []Capture{{Text: "a", Matched: true}, {Text: "b", Matched: true}, {Matched: true}}
				opts = append(opts, WithDeclarationPriority())
			}
			rejection := errors.New("guard rejection")
			guards := make([]func(Result) error, 2)
			guards[chosen] = func(r Result) error {
				if r.CaseIndex != chosen || !reflect.DeepEqual(r.Captures, want) {
					t.Fatalf("guard captures=%#v", r.Captures)
				}
				if string(s.pending) != "z:ab!" {
					t.Fatal("guard ran after consumption")
				}
				return rejection
			}
			result, err := s.expect(t.Context(), patterns, guards, opts...)
			if !errors.Is(err, rejection) || string(s.pending) != "z:ab!" {
				t.Fatalf("guard err=%v pending=%q", err, s.pending)
			}
			final, err := s.Expect(t.Context(), patterns, opts...)
			if err != nil || !reflect.DeepEqual(final.Captures, want) {
				t.Fatalf("winner err=%v captures=%#v", err, final.Captures)
			}
			clear(input)
			if !reflect.DeepEqual(result.Captures, want) || !reflect.DeepEqual(final.Captures, want) {
				t.Fatal("captures alias receive storage")
			}
		})
	}
}
func TestRegexpCaptureSelectionAndAll(t *testing.T) {
	for _, mode := range []string{"ordinary", "longest", "posix"} {
		t.Run(mode, func(t *testing.T) {
			re := regexp.MustCompile(`(a|ab)`)
			want := "a"
			if mode == "longest" {
				re.Longest()
				want = "ab"
			}
			if mode == "posix" {
				re = regexp.MustCompilePOSIX(`(a|ab)`)
				want = "ab"
			}
			pattern, err := FromRegexp(re)
			if err != nil {
				t.Fatal(err)
			}
			s, _ := pair(t)
			s.pending = []byte("ab!")
			result, err := s.Expect(t.Context(), []Case{{Pattern: pattern}, {Pattern: mustPattern(t, `(ab)`)}})
			if err != nil || result.CaseIndex != 0 || len(result.Captures) != 1 || result.Captures[0].Text != want {
				t.Fatalf("match=%q err=%v", result.Match, err)
			}
			s.pending = []byte("ab!")
			result, err = s.Expect(t.Context(), []Case{{Pattern: All(pattern, All(mustPattern(t, `(b)(!)`)))}})
			if err != nil || result.Match != "ab!" || len(result.Captures) != 0 {
				t.Fatalf("all match=%q err=%v", result.Match, err)
			}
		})
	}
}
