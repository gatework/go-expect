// SPDX-License-Identifier: Apache-2.0

package expect

import (
	"errors"
	"io"
	"testing"
)

func TestInputMailboxTerminalEvent(t *testing.T) {
	var mailbox inputMailbox
	if err := mailbox.enqueue(readEvent{err: io.EOF}, 1); err != nil {
		t.Fatal(err)
	}
	event, ok := mailbox.takeQueued()
	if !ok || len(event.data) != 0 || !errors.Is(event.err, io.EOF) {
		t.Fatalf("terminal event = %#v, present = %v", event, ok)
	}
	if _, ok := mailbox.takeQueued(); ok {
		t.Fatal("terminal event delivered twice")
	}
	if err := mailbox.enqueue(readEvent{}, 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := mailbox.takeQueued(); ok {
		t.Fatal("empty read published an event")
	}
}
