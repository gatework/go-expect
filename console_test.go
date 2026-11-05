//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestConsoleDeviceDialogue(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var transcript bytes.Buffer
	c, err := NewConsole(WithRaw(), WithSize(40, 200), WithSessionOptions(WithOutput(&transcript)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.SetSize(24, 80); err != nil {
		t.Fatal(err)
	}
	if err = c.SetEcho(false); err != nil {
		t.Fatal(err)
	}
	s := c.Session()
	if err = s.Redact("s3cr3t"); err != nil {
		t.Fatal(err)
	}
	// Static shell program; no caller-controlled text enters the command string.
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `
 exec </dev/tty >/dev/tty 2>&1
 [ "$(stty size)" = "24 80" ] || exit 16
 printf 'Password:'
 IFS= read -r password
 [ "$password" = 's3cr3t' ] || exit 11
 printf 'device>'
 for expected in show save; do
   IFS= read -r command
   [ "$command" = "$expected" ] || exit 12
   for attempt in 1 2; do
     printf 'Apply? [y/N] '
     IFS= read -r answer
     [ "$answer" = y ] || exit 13
   done
   printf -- '--More--'
   key=$(dd bs=1 count=1 2>/dev/null)
   [ "$key" = ' ' ] || exit 14
   printf 'Repeat code 42:'
   IFS= read -r code
   [ "$code" = 42 ] || exit 15
   printf 'applied:%s\ndevice>' "$command"
 done
 `)
	cmd.Stdin = c.TTY()
	cmd.Stdout = c.TTY()
	cmd.Stderr = c.TTY()
	if err = c.Start(cmd); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	login := WithRules(Case{Pattern: Literal("Password:"), Handler: ReplyThenContinue("s3cr3t\n")})
	if _, err = s.Expect(ctx, []Case{{Pattern: Literal("device>")}}, login); err != nil {
		t.Fatal(err)
	}
	rules := WithRules(Case{Pattern: Literal("Apply? [y/N] "), Handler: ReplyThenContinue("y\n"), Limit: 2}, Case{Pattern: Literal("--More--"), Handler: ReplyThenContinue(" ")}, Case{Pattern: mustPattern(t, `Repeat code (\d+):`), Handler: Then(func(ctx context.Context, s *Session, r Result) error {
		_, err := s.SendLine(ctx, r.Captures[0].Text)
		return err
	})})
	for _, command := range []string{"show", "save"} {
		if _, err = s.SendLine(ctx, command); err != nil {
			t.Fatal(err)
		}
		result, e := s.Expect(ctx, []Case{{Pattern: Literal("device>")}}, rules)
		if e != nil || !strings.Contains(result.Before, "applied:"+command) {
			t.Fatalf("%s: %v %q", command, e, result.Before)
		}
	}
	if _, err = s.Expect(ctx, []Case{{Pattern: EOF()}}); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(transcript.String(), "s3cr3t") {
		t.Fatal("password in transcript")
	}
}
func TestConsoleCanonicalEchoAndRawClose(t *testing.T) {
	c, err := NewConsole()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Session().SendLine(t.Context(), "hello"); err != nil {
		t.Fatal(err)
	}
	waitText(t, c.Session(), "hello")
	getState := func() unix.Termios {
		t.Helper()
		raw, e := c.master.SyscallConn()
		if e != nil {
			t.Fatal(e)
		}
		var value *unix.Termios
		var inner error
		if e = raw.Control(func(fd uintptr) { value, inner = unix.IoctlGetTermios(int(fd), termiosRead) }); e != nil || inner != nil {
			t.Fatal(errors.Join(e, inner))
		}
		return *value
	}
	original := getState()
	if err = c.SetEcho(false); err != nil {
		t.Fatal(err)
	}
	expected := original
	expected.Lflag &^= unix.ECHO
	if got := getState(); got != expected {
		t.Fatal("SetEcho changed unrelated terminal flags")
	}
	if err = c.SetEcho(true); err != nil {
		t.Fatal(err)
	}
	expected.Lflag |= unix.ECHO
	if got := getState(); got != expected {
		t.Fatal("SetEcho did not restore echo")
	}
	// Exercise raw takeover on a borrowed canonical terminal and verify restore.
	if _, err = io.ReadFull(c.TTY(), make([]byte, len("hello\n"))); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Session().Send(t.Context(), "\x1d\n"); err != nil {
		t.Fatal(err)
	}
	remote, _ := pair(t)
	if e := remote.InteractTerminal(t.Context(), c.TTY(), io.Discard, WithInteractExpectOptions(WithTimeout(-1))); e == nil {
		t.Fatal("invalid options admitted")
	}
	if got := getState(); got != expected {
		t.Fatal("invalid interaction changed terminal")
	}
	if err = remote.InteractTerminal(t.Context(), c.TTY(), io.Discard); err != nil {
		t.Fatal(err)
	}
	if got := getState(); got != expected {
		t.Fatal("InteractTerminal did not restore the borrowed terminal")
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Session().Expect(t.Context(), []Case{{Pattern: EOF()}}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestConsoleLineEnding(t *testing.T) {
	c, err := NewConsole(WithRaw(), WithSessionOptions(WithLineEnding("\r\n")))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan string, 1)
	go func() { b := make([]byte, 3); io.ReadFull(c.TTY(), b); done <- string(b) }()
	if n, err := c.Session().SendLine(t.Context(), "x"); n != 3 || err != nil {
		t.Fatalf("%d %v", n, err)
	}
	if got := <-done; got != "x\r\n" {
		t.Fatalf("%q", got)
	}
}

func TestConsoleInitializationFailureClosesDescriptors(t *testing.T) {
	directory := "/dev/fd"
	count := func() int {
		t.Helper()
		dir, err := os.Open(directory)
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()
		// /dev/fd entries can disappear before a metadata lookup. Count names
		// while keeping the directory descriptor alive, without lstat calls.
		names, err := dir.Readdirnames(-1)
		if err != nil {
			t.Fatal(err)
		}
		return len(names)
	}
	// Warm the runtime poller before comparing descriptor counts.
	c, err := NewConsole()
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	before := count()
	for i := 0; i < 20; i++ {
		if c, err := NewConsole(WithSessionOptions(WithBufferLimit(0))); err == nil {
			c.Close()
			t.Fatal("invalid limit accepted")
		}
	}
	if after := count(); after != before {
		t.Fatalf("fd count before=%d after=%d", before, after)
	}
}

func BenchmarkLiteral16K(b *testing.B) {
	text := strings.Repeat("x", 16*1024) + "done!"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c, err := NewConsole()
		if err != nil {
			b.Fatal(err)
		}
		go c.TTY().Write([]byte(text))
		_, err = c.Session().Expect(context.Background(), []Case{{Pattern: Literal("done!")}})
		c.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
