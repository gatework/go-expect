package expect

import (
	"bytes"
	"context"
	"strconv"
	"testing"
)

func BenchmarkFragmentedReceive(b *testing.B) {
	for _, chunk := range []int{1, 32, 1024, 16384} {
		b.Run(strconv.Itoa(chunk), func(b *testing.B) {
			data := append(bytes.Repeat([]byte("x"), 16<<10), []byte("END!")...)
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				s, err := NewSession(&chunkStream{Reader: bytes.NewReader(data), chunk: chunk})
				if err != nil {
					b.Fatal(err)
				}
				result, err := s.Expect(context.Background(), []Case{{Pattern: Literal("END!")}})
				s.Close()
				if err != nil || len(result.Before) != 16<<10 {
					b.Fatalf("match failed: %v", err)
				}
			}
		})
	}
}
