//go:build linux || darwin

package expecttest

import (
	expect "github.com/gatework/go-expect"
	"testing"
)

func TestConsole(t *testing.T) {
	c := NewConsole(t, expect.WithRaw())
	go c.TTY().Write([]byte("ready"))
	r, err := c.Session().Expect(t.Context(), []expect.Case{{Pattern: expect.Literal("ready")}})
	if err != nil || r.Match != "ready" {
		t.Fatalf("%v %q", err, r.Match)
	}
}
