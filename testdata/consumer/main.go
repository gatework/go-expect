package main

import (
	"context"
	"fmt"
	expect "github.com/gatework/go-expect"
	"github.com/gatework/go-expect/connector"
	"io"
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
	go c.Tty().Write([]byte("pty>"))
	if _, err = c.Session().Expect(ctx, []expect.Case{{Pattern: expect.Literal("pty>")}}); err != nil {
		return err
	}
	fmt.Println("consumer: stream and PTY passed")
	return connectorSmoke(ctx)
}

func connectorSmoke(ctx context.Context) error {
	client, server := net.Pipe()
	defer server.Close()
	done := make(chan error, 1)
	go func() {
		if _, err := io.WriteString(server, "router#"); err != nil {
			done <- err
			return
		}
		command := make([]byte, len("show version\r"))
		if _, err := io.ReadFull(server, command); err != nil {
			done <- err
			return
		}
		if string(command) != "show version\r" {
			done <- fmt.Errorf("unexpected command")
			return
		}
		_, err := io.WriteString(server, "show version\r\nversion 1\r\nrouter#")
		done <- err
	}()
	d, err := connector.New(connector.CiscoIOS(), connector.Config{Host: "local.example"}, connector.WithDialer(func(context.Context, connector.Config) (io.ReadWriteCloser, error) { return client, nil }))
	if err != nil {
		client.Close()
		return err
	}
	defer d.Close()
	if err = d.Connect(ctx); err != nil {
		return err
	}
	r, err := d.Execute(ctx, connector.Command{Text: "show version"})
	if err != nil {
		return err
	}
	if !r.Complete || r.Output != "version 1\n" {
		return fmt.Errorf("invalid connector receipt")
	}
	if err = <-done; err != nil {
		return err
	}
	fmt.Println("consumer: connector passed")
	return nil
}
