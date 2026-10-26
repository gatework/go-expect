package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"regexp"
	"sync"
	"testing"
	"time"
)

func TestFromRegexpPreservesLongestAndIsolation(t *testing.T) {
	r := regexp.MustCompile("a|ab")
	r.Longest()
	posix, err := FromRegexp(regexp.MustCompilePOSIX("a|ab"))
	if err != nil {
		t.Fatal(err)
	}
	hitPOSIX, okPOSIX, err := posix.find([]byte("ab"), true, 0)
	if err != nil || !okPOSIX || hitPOSIX.end != 2 {
		t.Fatal("lost POSIX semantics")
	}
	p, err := FromRegexp(r)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSession(&chunkStream{Reader: bytes.NewReader([]byte("ab")), chunk: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Expect(t.Context(), []Case{{Pattern: p}})
	if err != nil || got.Match != "ab" {
		t.Fatalf("match=%q err=%v", got.Match, err)
	}
	if _, err := FromRegexp(nil); err == nil {
		t.Fatal("nil regexp accepted")
	}
	original := regexp.MustCompile("a|ab")
	p, _ = FromRegexp(original)
	original.Longest()
	hit, ok, err := p.find([]byte("ab"), true, 0)
	if err != nil || !ok || hit.end != 1 {
		t.Fatal("caller mutated pattern")
	}
}

type tinyProgressStream struct {
	done chan struct{}
	once sync.Once
}

func (s *tinyProgressStream) Read([]byte) (int, error)    { <-s.done; return 0, io.EOF }
func (s *tinyProgressStream) Write(p []byte) (int, error) { return min(1, len(p)), nil }
func (s *tinyProgressStream) Close() error                { s.once.Do(func() { close(s.done) }); return nil }
func TestSendAllDoesNotAllocateEachUnsentSuffix(t *testing.T) {
	text := string(make([]byte, 4096))
	allocations := testing.AllocsPerRun(3, func() {
		s, err := NewSession(&tinyProgressStream{done: make(chan struct{})})
		if err != nil {
			t.Fatal(err)
		}
		n, err := s.SendAll(t.Context(), text)
		s.Close()
		if err != nil || n != len(text) {
			t.Fatalf("written=%d err=%v", n, err)
		}
	})
	if allocations > 100 {
		t.Fatalf("partial writes allocate each suffix: %.0f allocations", allocations)
	}
}

func TestCloseCancelsActiveHandlerWithoutWaiting(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	s, err := NewSession(a, WithDefaultTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	entered, finished := make(chan struct{}), make(chan error, 1)
	release := make(chan struct{})
	go b.Write([]byte("ready"))
	go func() {
		_, err := s.Expect(t.Context(), []Case{{Pattern: Literal("ready"), Handler: func(ctx context.Context, _ *Session, _ Result) (Action, error) {
			close(entered)
			select {
			case <-ctx.Done():
				return Return, ctx.Err()
			case <-release:
				return Return, nil
			}
		}}})
		finished <- err
	}()
	<-entered
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		<-finished
		t.Fatal("Close did not cancel handler")
	}
}

type failingDuplex struct {
	net.Conn
	writeErr, closeErr error
}

func (s *failingDuplex) Write([]byte) (int, error) { return 1, s.writeErr }
func (s *failingDuplex) Close() error              { s.Conn.Close(); return s.closeErr }
func TestAutomaticRetirementPreservesCleanupCause(t *testing.T) {
	for _, all := range []bool{false, true} {
		a, b := net.Pipe()
		primary, cleanup := errors.New("write sentinel"), errors.New("cleanup sentinel")
		s, err := NewSession(&failingDuplex{a, primary, cleanup})
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if all {
			n, err = s.SendAll(t.Context(), "secret")
		} else {
			n, err = s.Send(t.Context(), "secret")
		}
		if n != 1 || !errors.Is(err, primary) || !errors.Is(err, cleanup) {
			t.Errorf("all=%v n=%d err=%v", all, n, err)
		}
		b.Close()
		s.Close()
	}
}
