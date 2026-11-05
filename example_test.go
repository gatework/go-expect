// SPDX-License-Identifier: Apache-2.0

package expect_test

import (
	"context"
	"fmt"
	"io"
	"net"

	expect "github.com/gatework/go-expect"
)

func ExampleSession_Expect() {
	local, remote := net.Pipe()
	session, err := expect.NewSession(local, expect.WithLineEnding("\r"))
	if err != nil {
		panic(err)
	}
	defer session.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer remote.Close()
		_, _ = remote.Write([]byte("Password:"))
		b := make([]byte, 7)
		_, _ = io.ReadFull(remote, b)
		_, _ = remote.Write([]byte("router#"))
	}()
	if err = session.Redact("secret"); err != nil {
		panic(err)
	}
	result, err := session.Expect(context.Background(), []expect.Case{
		{Pattern: expect.Literal("Password:"), Handler: expect.ReplyThenContinue("secret\r"), Limit: 1},
		{Pattern: expect.Line("router#")},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(result.CaseIndex, result.Match)
	<-done
	// Output: 1 router#
}
