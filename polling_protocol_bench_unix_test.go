//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func BenchmarkOSReceive(b *testing.B) {
	for _, size := range []int{1, 32, 1024, 16384} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			transport, peer := benchmarkSocketPair(b)
			s, err := NewSession(transport, WithOSPolling())
			if err != nil {
				transport.Close()
				peer.Close()
				b.Fatal(err)
			}
			defer s.Close()
			defer peer.Close()
			input := bytes.Repeat([]byte("x"), size)
			cases := []Case{{Pattern: Literal(string(input))}}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if _, err := peer.Write(input); err != nil {
					b.Fatal(err)
				}
				result, err := s.Expect(context.Background(), cases)
				if err != nil || len(result.Match) != size {
					b.Fatalf("match length=%d err=%v", len(result.Match), err)
				}
			}
		})
	}
}
