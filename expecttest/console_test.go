//go:build linux || darwin || freebsd

// SPDX-License-Identifier: Apache-2.0

package expecttest

import (
	"testing"

	expect "github.com/gatework/go-expect"
)

func TestConsole(t *testing.T) {
	c := NewConsole(t, expect.WithRaw())
	go c.TTY().Write([]byte("ready"))
	r, err := c.Session().Expect(t.Context(), []expect.Case{{Pattern: expect.Literal("ready")}})
	if err != nil || r.Match != "ready" {
		t.Fatalf("%v %q", err, r.Match)
	}
}
