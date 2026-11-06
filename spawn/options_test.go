//go:build linux || darwin || freebsd || windows

// SPDX-License-Identifier: Apache-2.0

package spawn

import (
	"context"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	expect "github.com/gatework/go-expect"
)

func TestProcessConfigurationAndExitStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	// macOS temporary directories may be spelled through /var -> /private/var.
	program, args := "/bin/sh", []string{"-c", `test "$EXPECT_CHILD" = ready && test "$PWD" -ef "$EXPECT_DIR" && printf READY; exit 7`}
	if runtime.GOOS == "windows" {
		program = "cmd.exe"
		args = []string{"/d", "/q", "/c", `echo %EXPECT_CHILD% & cd & exit /b 7`}
	}
	env := append(os.Environ(), "EXPECT_CHILD=ready", "EXPECT_DIR="+dir)
	// A wide terminal keeps long CI temporary paths on one VT output line.
	p, err := Start(ctx, program, args, WithDir(dir), WithEnv(env), WithSize(24, 512))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal("READY").Fold()}}); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		if _, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal(dir).Fold()}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.EOF()}}); err != nil {
		t.Fatal(err)
	}
	status, err := p.Wait()
	var exit *ExitError
	if status.PID != p.PID() || status.PID <= 0 || status.Code != 7 || !errors.As(err, &exit) || exit.Status != status {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	_, echo := p.Terminal().(EchoController)
	if echo == (runtime.GOOS == "windows") {
		t.Fatal("terminal capability does not match backend")
	}
}
