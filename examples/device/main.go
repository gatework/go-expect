// A local synthetic device; no network equipment is contacted.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/gatework/go-expect/connector"
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
	client, server := net.Pipe()
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- simulate(server) }()
	device, err := connector.New(connector.CiscoIOS(), connector.Config{Host: "local.example", Password: "example-password"}, connector.WithDialer(func(context.Context, connector.Config) (io.ReadWriteCloser, error) { return client, nil }))
	if err != nil {
		client.Close()
		return err
	}
	defer device.Close()
	if err = device.Connect(ctx); err != nil {
		return err
	}
	result, err := device.ExecuteScript(ctx, []connector.Command{
		{Text: "show interfaces"},
		{Text: "apply example", Interactions: []connector.Interaction{{Pattern: `Apply example\? \[y/N\]`, Reply: "y\r"}}},
	})
	if err != nil {
		return err
	}
	for i, r := range result.Steps {
		fmt.Printf("step %d complete=%v output=%q\n", i+1, r.Complete, r.Output)
	}
	if err = device.Close(); err != nil {
		return err
	}
	return <-done
}
func simulate(conn net.Conn) error {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	steps := []struct{ output, input string }{
		{"Password:", "example-password\r"},
		{"\r\nrouter#", "show interfaces\r"},
		{"show interfaces\r\nEthernet1 up\r\n--More--", " "},
		{"Ethernet2 down\r\nrouter#", "apply example\r"},
		{"apply example\r\nApply example? [y/N]", "y\r"},
	}
	for _, step := range steps {
		if _, err := io.WriteString(conn, step.output); err != nil {
			return err
		}
		got := make([]byte, len(step.input))
		if _, err := io.ReadFull(reader, got); err != nil {
			return err
		}
		if string(got) != step.input {
			return fmt.Errorf("unexpected simulated input")
		}
	}
	_, err := io.WriteString(conn, "accepted\r\nrouter#")
	return err
}
