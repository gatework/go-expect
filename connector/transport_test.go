//go:build linux || darwin

package connector

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestTransportArguments(t *testing.T) {
	cfg, err := normalizeConfig(Config{Host: "2001:db8::1", Username: "operator", Password: "never-in-argv", Port: 2222, KnownHosts: "/tmp/known hosts", IdentityFile: "/tmp/private key"})
	if err != nil {
		t.Fatal(err)
	}
	program, args := transportCommand(cfg)
	joined := strings.Join(args, "|")
	if program != "ssh" || strings.Contains(joined, cfg.Password) || !strings.Contains(joined, "StrictHostKeyChecking=yes") || !strings.Contains(joined, "-e|none") || !strings.Contains(joined, `UserKnownHostsFile="/tmp/known hosts"`) {
		t.Fatalf("bad SSH args: %q", args)
	}
	cfg.Protocol = "telnet"
	cfg.Port = 0
	program, args = transportCommand(cfg)
	if program != "telnet" || strings.Join(args, "|") != "2001:db8::1|23" {
		t.Fatalf("bad Telnet args: %q", args)
	}
}

func TestOwnedProcessCloseInterruptsReadAndReaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	p, err := startProcess(ctx, exec.Command("/bin/sh", "-c", "printf ready; read line; printf '%s' \"$line\"; read line"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	got := make([]byte, 5)
	if _, err = io.ReadFull(p, got); err != nil || string(got) != "ready" {
		t.Fatalf("child output: %v %q", err, got)
	}
	if _, err = p.Write([]byte("reply\n")); err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadFull(p, got); err != nil || string(got) != "reply" {
		t.Fatalf("raw tty: %v %q", err, got)
	}
	readDone := make(chan error, 1)
	go func() { _, err := p.Read(got); readDone <- err }()
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-readDone:
		if err == nil {
			t.Fatal("closed read succeeded")
		}
	case <-ctx.Done():
		t.Fatal("read blocked")
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		t.Fatal("child not reaped")
	}
	if p.cmd.ProcessState == nil {
		t.Fatal("missing process state")
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDialFailureAndCanceledStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := startProcess(ctx, exec.Command("/bin/sh")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled start: %v", err)
	}
	if _, err := startProcess(t.Context(), exec.Command("/no-such-executable")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing client: %v", err)
	}
}
