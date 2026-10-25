//go:build linux || darwin

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
)

func TestConsoleDeviceDialogue(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var transcript bytes.Buffer
	c, err := NewConsole(WithRaw(), WithSessionOptions(WithTranscript(&transcript)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := c.Session()
	if err = s.Redact("s3cr3t"); err != nil {
		t.Fatal(err)
	}
	// Static shell program; no caller-controlled text enters the command string.
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `
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
	cmd.Stdin = c.Tty()
	cmd.Stdout = c.Tty()
	cmd.Stderr = c.Tty()
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c.Tty().Close()
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	login, _ := NewResponder(Rule{Pattern: Literal("Password:"), Respond: Reply("s3cr3t\n")})
	if _, err = login.Expect(ctx, s, []Pattern{Literal("device>")}); err != nil {
		t.Fatal(err)
	}
	rules, _ := NewResponder(Rule{Pattern: Literal("Apply? [y/N] "), Respond: Reply("y\n"), Limit: 2}, Rule{Pattern: Literal("--More--"), Respond: Reply(" ")}, Rule{Pattern: mustPattern(t, `Repeat code (\d+):`), Respond: func(ctx context.Context, s *Session, r Result) error {
		_, err := s.SendLine(ctx, r.Captures[0].Text)
		return err
	}})
	for _, command := range []string{"show", "save"} {
		if _, err = s.SendLine(ctx, command); err != nil {
			t.Fatal(err)
		}
		result, e := rules.Expect(ctx, s, []Pattern{Literal("device>")})
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
	go func() { b := make([]byte, 3); io.ReadFull(c.Tty(), b); done <- string(b) }()
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
