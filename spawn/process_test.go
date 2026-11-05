//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package spawn

import (
	"context"
	"testing"
	"time"

	expect "github.com/gatework/go-expect"
)

func TestControllingTerminalLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := Start(ctx, "/bin/sh", []string{"-c", `exec </dev/tty >/dev/tty 2>&1; stty size; printf 'ready!'; read answer; test "$answer" = yes`}, WithSize(40, 200))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal("40 200")}}); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal("ready!")}}); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Session().SendLine(ctx, "yes"); err != nil {
		t.Fatal(err)
	}
	if err = p.Wait(); err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Start(ctx, "/bin/sh", []string{"-c", `trap 'exit 0' HUP; printf ready; while :; do sleep 1; done`})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal("ready")}}); err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
}
