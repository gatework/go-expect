//go:build windows

// SPDX-License-Identifier: Apache-2.0

package conpty

import (
	"context"
	"errors"
	"testing"
	"time"

	expect "github.com/gatework/go-expect"
)

func TestProcessDialogueAndCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	p, err := Start(ctx, "cmd.exe", []string{"/d", "/q", "/c", "echo READY & set /p answer= & echo DONE"}, WithSize(40, 120))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.SetSize(24, 80); err != nil {
		t.Fatal(err)
	}
	_, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal("READY"), Handler: expect.ReplyThenContinue("yes\r")}, {Pattern: expect.Literal("DONE")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Session().Expect(ctx, []expect.Case{{Pattern: expect.EOF()}}); err != nil {
		t.Fatal(err)
	}
	if err = p.Wait(); err != nil {
		t.Fatal(err)
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	childCtx, stop := context.WithCancel(ctx)
	child, err := Start(childCtx, "cmd.exe", []string{"/d", "/q", "/c", "echo WAITING & set /p answer="})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = child.Session().Expect(childCtx, []expect.Case{{Pattern: expect.Literal("WAITING")}}); err != nil {
		t.Fatal(err)
	}
	stop()
	if err = child.Close(); err != nil {
		t.Fatal(err)
	}
	var exit *ExitError
	if err = child.Wait(); err == nil || !errors.As(err, &exit) {
		t.Fatalf("canceled process: %v", err)
	}
}
