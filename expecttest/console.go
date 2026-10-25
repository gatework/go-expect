//go:build linux || darwin

// Package expecttest provides test-owned PTY consoles without logging goroutines.
package expecttest

import (
	"bytes"
	"io"
	"sync"
	"testing"

	expect "github.com/gatework/go-expect"
)

// NewConsole creates a Console, forwards filtered receive output to t.Log and
// registers cleanup. It fails the test if construction fails.
func NewConsole(t testing.TB, opts ...expect.ConsoleOption) *expect.Console {
	t.Helper()
	writer := NewWriter(t)
	opts = append(opts, expect.WithSessionOptions(expect.WithOutput(writer)))
	c, err := expect.NewConsole(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close console: %v", err)
		}
	})
	return c
}

// NewWriter logs complete lines synchronously and flushes the last fragment at
// test cleanup. Long lines flush in bounded fragments. No reader goroutine exists.
func NewWriter(t testing.TB) io.Writer {
	w := &testWriter{t: t}
	t.Cleanup(w.flush)
	return w
}

type testWriter struct {
	mu      sync.Mutex
	t       testing.TB
	pending []byte
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	for len(p) > 0 {
		size := 4096 - len(w.pending)
		if size > len(p) {
			size = len(p)
		}
		w.pending = append(w.pending, p[:size]...)
		p = p[size:]
		for {
			end := bytes.IndexByte(w.pending, '\n')
			if end < 0 {
				break
			}
			w.t.Log(string(w.pending[:end]))
			w.pending = append(w.pending[:0], w.pending[end+1:]...)
		}
		if len(w.pending) == 4096 {
			w.t.Log(string(w.pending))
			w.pending = w.pending[:0]
		}
	}
	return n, nil
}
func (w *testWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) > 0 {
		w.t.Log(string(w.pending))
		w.pending = nil
	}
}
