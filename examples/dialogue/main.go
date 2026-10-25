// Command dialogue runs a local simulated device; no network connection is made.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	expect "github.com/gatework/go-expect"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, device := net.Pipe()
	s, err := expect.NewSession(client)
	if err != nil {
		client.Close()
		device.Close()
		return err
	}
	defer s.Close()
	if err = s.Redact("example-password"); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { defer device.Close(); done <- simulate(device) }()
	// Always join the simulated peer, including early failure paths.
	defer func() { s.Close(); device.Close(); <-done }()
	login, err := expect.NewResponder(expect.Rule{Pattern: expect.Literal("Password:"), Respond: expect.Reply("example-password\n")})
	if err != nil {
		return err
	}
	if _, err = login.Expect(ctx, s, []expect.Pattern{expect.Literal("device>")}); err != nil {
		return err
	}
	fmt.Println("logged in")
	code, err := expect.Compile(`Repeat code (\d+):`)
	if err != nil {
		return err
	}
	rules, err := expect.NewResponder(
		expect.Rule{Pattern: expect.Literal("Apply? [y/N] "), Respond: expect.Reply("y\n"), Limit: 2},
		expect.Rule{Pattern: expect.Literal("--More--"), Respond: expect.Reply(" ")},
		expect.Rule{Pattern: code, Respond: func(ctx context.Context, s *expect.Session, r expect.Result) error {
			_, err := s.SendLine(ctx, r.Captures[0].Text)
			return err
		}},
	)
	if err != nil {
		return err
	}
	for _, command := range []string{"show status", "save configuration"} {
		if _, err = s.SendLine(ctx, command); err != nil {
			return err
		}
		if _, err = rules.Expect(ctx, s, []expect.Pattern{expect.Literal("device>")}); err != nil {
			return err
		}
		fmt.Println("completed:", command)
	}
	_, err = s.Expect(ctx, []expect.Case{{Pattern: expect.EOF()}})
	return err
}
func simulate(conn net.Conn) error {
	in := bufio.NewReader(conn)
	exchange := func(prompt, reply string) error {
		if _, err := io.WriteString(conn, prompt); err != nil {
			return err
		}
		buf := make([]byte, len(reply))
		if _, err := io.ReadFull(in, buf); err != nil {
			return err
		}
		if string(buf) != reply {
			return fmt.Errorf("unexpected reply")
		}
		return nil
	}
	if err := exchange("Password:", "example-password\n"); err != nil {
		return err
	}
	if _, err := io.WriteString(conn, "device>"); err != nil {
		return err
	}
	for _, command := range []string{"show status\n", "save configuration\n"} {
		actual, err := in.ReadString('\n')
		if err != nil {
			return err
		}
		if actual != command {
			return fmt.Errorf("unexpected command")
		}
		for i := 0; i < 2; i++ {
			if err := exchange("Apply? [y/N] ", "y\n"); err != nil {
				return err
			}
		}
		if err := exchange("--More--", " "); err != nil {
			return err
		}
		if err := exchange("Repeat code 42:", "42\n"); err != nil {
			return err
		}
		if _, err := io.WriteString(conn, "done\ndevice>"); err != nil {
			return err
		}
	}
	return nil
}
