// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
)

func BenchmarkReceivePatterns(b *testing.B) {
	literal := Literal("END!")
	expression, err := Compile(`E(ND)!`)
	if err != nil {
		b.Fatal(err)
	}
	patterns := []struct {
		name    string
		pattern Pattern
	}{
		{"literal", literal},
		{"all_literals", All(Literal("START"), literal)},
		{"nested_all", All(All(Literal("START"), Literal("START")), literal)},
		{"regexp", expression},
		{"mixed_all", All(Literal("START"), expression)},
		{"mixed_literal_first", All(literal, Line("END!"))},
	}
	for _, entry := range patterns {
		for _, chunk := range []int{32, 1024} {
			b.Run(fmt.Sprintf("%s/chunk_%d", entry.name, chunk), func(b *testing.B) {
				input := append([]byte("START"), bytes.Repeat([]byte("x"), 32<<10)...)
				if entry.name == "mixed_literal_first" {
					input = append(input, '\n')
				}
				input = append(input, "END!"...)
				b.SetBytes(int64(len(input)))
				b.ReportAllocs()
				for b.Loop() {
					s, err := NewSession(&chunkStream{Reader: bytes.NewReader(input), chunk: chunk})
					if err != nil {
						b.Fatal(err)
					}
					result, err := s.Expect(context.Background(), []Case{{Pattern: entry.pattern}})
					closeErr := s.Close()
					if err != nil || closeErr != nil || len(result.Before)+len(result.Match) != len(input) {
						b.Fatalf("result length=%d err=%v close=%v", len(result.Before)+len(result.Match), err, closeErr)
					}
				}
			})
		}
	}
}

func BenchmarkCompetingCaptures(b *testing.B) {
	for _, count := range []int{1, 8} {
		for _, all := range []bool{false, true} {
			b.Run(fmt.Sprintf("cases=%d/all=%v", count, all), func(b *testing.B) {
				input := bytes.Repeat([]byte("a"), 64<<10)
				pattern, err := Compile(`(a+)(z)?()`)
				if err != nil {
					b.Fatal(err)
				}
				cases := make([]Case, count)
				for i := range cases {
					cases[i].Pattern = pattern
					if all {
						cases[i].Pattern = All(pattern, pattern)
					}
				}
				a, peer := net.Pipe()
				s, err := NewSession(a)
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close()
				defer peer.Close()
				b.ReportAllocs()
				for b.Loop() {
					s.receive.pending = input
					result, err := s.Expect(b.Context(), cases)
					if err != nil || len(result.Match) != len(input) {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
