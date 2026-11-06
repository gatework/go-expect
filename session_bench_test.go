// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
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

// BenchmarkSerializedSend measures the admission/deadline path on a long-lived
// session, independently of connection establishment and receive matching.
func BenchmarkSerializedSend(b *testing.B) {
	for _, entry := range []struct {
		name    string
		timeout time.Duration
	}{{"default", 10 * time.Second}, {"unlimited", 0}} {
		b.Run(entry.name, func(b *testing.B) {
			local, remote := net.Pipe()
			s, err := NewSession(local, WithWriteTimeout(entry.timeout))
			if err != nil {
				b.Fatal(err)
			}
			done := make(chan struct{})
			go func() { _, _ = io.Copy(io.Discard, remote); close(done) }()
			defer func() { _ = s.Close(); _ = remote.Close(); <-done }()
			text := strings.Repeat("command ", 8)
			b.ReportAllocs()
			b.SetBytes(int64(len(text)))
			for b.Loop() {
				if n, err := s.Send(context.Background(), text); n != len(text) || err != nil {
					b.Fatalf("n=%d err=%v", n, err)
				}
			}
		})
	}
}

// BenchmarkReusedExpect isolates steady-state matching from transport setup.
func BenchmarkReusedExpect(b *testing.B) {
	re, err := Compile("END")
	if err != nil {
		b.Fatal(err)
	}
	for _, entry := range []struct {
		name    string
		pattern Pattern
	}{
		{"literal", Literal("END")}, {"regexp", re},
	} {
		b.Run(entry.name, func(b *testing.B) {
			local, remote := net.Pipe()
			s, err := NewSession(local)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			defer remote.Close()
			cases := []Case{{Pattern: entry.pattern}}
			input := []byte("END")
			b.ReportAllocs()
			for b.Loop() {
				s.receive.pending = input
				if _, err := s.Expect(context.Background(), cases); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkInteractPartialPrefix(b *testing.B) {
	for _, size := range []int{4096, 8192, 16384} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			data := bytes.Repeat([]byte("a"), size)
			pattern := Literal(string(data) + "b")
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				stream := &admissionTransport{closed: make(chan struct{})}
				s, err := NewSession(stream, WithWriteTimeout(NoTimeout))
				if err != nil {
					b.Fatal(err)
				}
				input := io.NopCloser(&chunkStream{Reader: bytes.NewReader(data), chunk: 1})
				err = s.Interact(context.Background(), input, io.Discard, WithInteractInputCases(Case{Pattern: pattern}))
				_ = s.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
