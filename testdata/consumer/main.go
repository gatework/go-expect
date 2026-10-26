package main

import (
	"context"
	"fmt"
	expect "github.com/gatework/go-expect"
	"net"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a, b := net.Pipe()
	defer b.Close()
	s, err := expect.NewSession(a)
	if err != nil {
		a.Close()
		return err
	}
	defer s.Close()
	go b.Write([]byte("stream>"))
	if _, err = s.Expect(ctx, []expect.Case{{Pattern: expect.Literal("stream>")}}); err != nil {
		return err
	}
	c, err := expect.NewConsole(expect.WithRaw())
	if err != nil {
		return err
	}
	defer c.Close()
	go c.TTY().Write([]byte("pty>"))
	if _, err = c.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal("pty>")}}); err != nil {
		return err
	}
	fmt.Println("consumer: stream and PTY passed")
	return nil
}
