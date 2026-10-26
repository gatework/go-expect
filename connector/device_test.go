package connector

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	expect "github.com/gatework/go-expect"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type exchange struct{ want, reply string }

func deviceFixture(t *testing.T, profile Profile, cfg Config, greeting string, steps []exchange, opts ...Option) *Device {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		if _, err := io.WriteString(server, greeting); err != nil {
			done <- err
			return
		}
		reader := bufio.NewReader(server)
		for _, step := range steps {
			got := make([]byte, len(step.want))
			if _, err := io.ReadFull(reader, got); err != nil {
				done <- err
				return
			}
			if string(got) != step.want {
				done <- fmt.Errorf("unexpected device input")
				return
			}
			if _, err := io.WriteString(server, step.reply); err != nil {
				done <- err
				return
			}
		}
		_, err := io.Copy(io.Discard, reader)
		done <- err
	}()
	cfg.Host = "device.example"
	if cfg.CommandTimeout == 0 {
		cfg.CommandTimeout = time.Second
	}
	opts = append(opts, WithDialer(func(context.Context, Config) (io.ReadWriteCloser, error) { return client, nil }))
	d, err := New(profile, cfg, opts...)
	if err != nil {
		client.Close()
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		d.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, io.ErrClosedPipe) {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("fixture did not exit")
		}
	})
	if err := d.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestLoginPagingConfirmationAndReceipts(t *testing.T) {
	p := CiscoIOS()
	var seen []Receipt
	d := deviceFixture(t, p, Config{Username: "admin", Password: "private"}, "Username:", []exchange{
		{"admin\r", "Password:"}, {"private\r", "\r\nr1#"},
		{"show version\r", "show version\r\nfirst\r\n--More--"},
		{" ", "second\r\nr1#"},
		{"reload\r", "reload\r\nProceed? [y/N]"}, {"y\r", "accepted\r\nr1#"},
	}, WithReceipt(func(r Receipt) error { seen = append(seen, r); return nil }))
	result, err := d.ExecuteScript(t.Context(), []Command{
		{Text: "show version"},
		{Text: "reload", Interactions: []Interaction{{Pattern: `Proceed\? \[y/N\]`, Reply: "y\r"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Steps) != 2 || len(seen) != 2 {
		t.Fatalf("receipts: %d/%d", len(result.Steps), len(seen))
	}
	r := result.Steps[0]
	if r.Output != "first\nsecond\n" || !r.Complete || r.Written != len("show version\r") || !strings.Contains(r.Raw, "--More--") {
		t.Fatalf("bad first receipt: output=%q complete=%v written=%d", r.Output, r.Complete, r.Written)
	}
}

func TestScriptPreflightAndFailureStopsWrites(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"bad\r", "bad\r\n% Invalid input detected\r\nr1#"}})
	if _, err := d.ExecuteScript(t.Context(), []Command{{Text: "must not send"}, {Text: "bad\nsecond"}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid script: %v", err)
	}
	result, err := d.ExecuteScript(t.Context(), []Command{{Text: "bad"}, {Text: "must not send"}})
	if !errors.Is(err, ErrDevice) || len(result.Steps) != 1 || !result.Steps[0].Complete {
		t.Fatalf("failure receipt: %v %+v", err, result)
	}
	if _, err := d.Execute(t.Context(), Command{Text: "must not send"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("failed session reusable: %v", err)
	}
}

func TestPromptIdentityAndExplicitModeChange(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{
		{"show text\r", "show text\r\nnot-a-prompt#\r\nbody\r\nr1#"},
		{"configure terminal\r", "configure terminal\r\nr1(config)#"},
		{"description test\r", "description test\r\nok\r\nr1(config)#"},
	})
	r, err := d.Execute(t.Context(), Command{Text: "show text"})
	if err != nil || !strings.Contains(r.Output, "not-a-prompt#") {
		t.Fatalf("wrong completion: %v %q", err, r.Output)
	}
	if _, err = d.Execute(t.Context(), Command{Text: "configure terminal", Prompt: `(?:\A|[\r\n])r1\(config\)#[ \t]*\z`}); err != nil {
		t.Fatal(err)
	}
	if _, err = d.Execute(t.Context(), Command{Text: "description test"}); err != nil {
		t.Fatal(err)
	}
	if d.CurrentPrompt() != "r1(config)#" {
		t.Fatal("prompt not updated")
	}
}

func TestOutputBudgetIncludesPagedSegments(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{MaxOutputBytes: 64}, "r1#", []exchange{
		{"show\r", strings.Repeat("a", 25) + "\r\n--More--"},
		{" ", strings.Repeat("b", 25) + "\r\n--More--"},
	})
	r, err := d.Execute(t.Context(), Command{Text: "show"})
	if !errors.Is(err, ErrOutputLimit) || r.Complete || len(r.Raw) > 64 {
		t.Fatalf("budget: %v %d", err, len(r.Raw))
	}
}

func TestPagingEraseDoesNotEnterOutput(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{
		{"show\r", "show\r\nfirst\r\n--More--"},
		{" ", strings.Repeat("\b", 8) + strings.Repeat(" ", 8) + strings.Repeat("\b", 8) + "second\r\nr1#"},
	})
	r, err := d.Execute(t.Context(), Command{Text: "show"})
	if err != nil || r.Output != "first\nsecond\n" || !strings.Contains(r.Raw, "\b") {
		t.Fatalf("paging output: %v %q", err, r.Output)
	}
}

func TestCloseInterruptsActiveCommand(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"wait\r", ""}})
	done := make(chan error, 1)
	go func() { _, err := d.Execute(context.Background(), Command{Text: "wait"}); done <- err }()
	deadline := time.After(time.Second)
	for !d.busy.Load() {
		select {
		case <-deadline:
			t.Fatal("command did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if _, err := d.Execute(t.Context(), Command{Text: "another"}); !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent command: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed command succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt")
	}
}

func TestReceiptCallbackFailurePreservesCompletion(t *testing.T) {
	marker := errors.New("private callback details")
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"show\r", "show\r\nsecret\r\nr1#"}}, WithReceipt(func(Receipt) error { return marker }))
	result, err := d.ExecuteScript(t.Context(), []Command{{Text: "show"}, {Text: "never"}})
	if !errors.Is(err, marker) || len(result.Steps) != 1 || !result.Steps[0].Complete {
		t.Fatalf("callback evidence lost: %v", err)
	}
	if strings.Contains(fmt.Sprintf("%v %#v", result, err), "secret") || strings.Contains(err.Error(), "private") {
		t.Fatal("unsafe diagnostics")
	}
}

func TestLoginChallengeAndEnable(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{Password: "login-secret", EnablePassword: "enable-secret", Challenges: []Interaction{{Pattern: `Token:`, Respond: func(context.Context, expect.Result) (string, error) { return "123456\r", nil }, Sensitive: true}}}, "Token:", []exchange{
		{"123456\r", "r1>"}, {"enable\r", "Password:"}, {"enable-secret\r", "\r\nr1#"},
		{"show\r", "show\r\nok\r\nr1#"},
	})
	if _, err := d.Enable(t.Context()); err != nil {
		t.Fatal(err)
	}
	if d.CurrentPrompt() != "r1#" {
		t.Fatal("enable did not update prompt")
	}
	if _, err := d.Execute(t.Context(), Command{Text: "show"}); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownConfirmationTimesOutWithoutAnswer(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{CommandTimeout: 30 * time.Millisecond}, "r1#", []exchange{{"change\r", "Are you sure? [y/N]"}})
	receipt, err := d.Execute(t.Context(), Command{Text: "change"})
	if !errors.Is(err, context.DeadlineExceeded) || receipt.Complete || !strings.Contains(receipt.Raw, "Are you sure?") {
		t.Fatalf("unknown confirmation: %v", err)
	}
	if d.Connected() {
		t.Fatal("timed out session remains connected")
	}
}

func TestRepeatedConfirmationStopsBeforeAnotherReply(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"change\r", "Confirm?"}, {"y\r", "Confirm?"}})
	_, err := d.Execute(t.Context(), Command{Text: "change", Interactions: []Interaction{{Pattern: `Confirm\?`, Reply: "y\r"}}})
	if !errors.Is(err, ErrResponseLimit) {
		t.Fatalf("unbounded confirmation: %v", err)
	}
}

func TestPanickingResponseRetiresSession(t *testing.T) {
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"change\r", "Confirm?"}})
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic was swallowed")
			}
		}()
		_, _ = d.Execute(t.Context(), Command{Text: "change", Interactions: []Interaction{{Pattern: `Confirm\?`, Respond: func(context.Context, expect.Result) (string, error) { panic("callback") }}}})
	}()
	if d.Connected() {
		t.Fatal("panic left uncertain connection reusable")
	}
}

func TestCloseDuringDialReclaimsLateStream(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	client, server := net.Pipe()
	defer server.Close()
	d, err := New(CiscoIOS(), Config{Host: "example"}, WithDialer(func(ctx context.Context, _ Config) (io.ReadWriteCloser, error) {
		close(entered)
		<-release
		return client, ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	done := make(chan error, 1)
	go func() { done <- d.Connect(t.Context()) }()
	<-entered
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("dial leak")
	}
	if _, err = server.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("late stream not closed: %v", err)
	}
}

func TestCloseCancelsResponseContext(t *testing.T) {
	entered := make(chan struct{})
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"change\r", "Confirm?"}})
	done := make(chan error, 1)
	go func() {
		_, err := d.Execute(context.Background(), Command{Text: "change", Interactions: []Interaction{{Pattern: `Confirm\?`, Respond: func(ctx context.Context, _ expect.Result) (string, error) {
			close(entered)
			<-ctx.Done()
			return "", ctx.Err()
		}}}})
		done <- err
	}()
	<-entered
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wrong cancellation: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Close did not cancel response context")
	}
}

func TestCustomPromptPreservesLeadingSpaces(t *testing.T) {
	p := CiscoIOS()
	p.LoginPrompt = `(?:\A|[\r\n])  router#[ \t]*\z`
	d := deviceFixture(t, p, Config{}, "  router#", []exchange{{"show\r", "show\r\nbody\r\n  router#"}})
	r, err := d.Execute(t.Context(), Command{Text: "show"})
	if err != nil || r.Prompt != "  router#" {
		t.Fatalf("prompt identity lost: %v %q", err, r.Prompt)
	}
}

func TestDialerReceivesDetachedChallenges(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	d, err := New(CiscoIOS(), Config{Host: "example", LoginTimeout: 50 * time.Millisecond, Challenges: []Interaction{{Pattern: `Token:`, Reply: "123\r"}}}, WithDialer(func(_ context.Context, cfg Config) (io.ReadWriteCloser, error) {
		cfg.Challenges[0].Pattern = `Wrong:`
		return client, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	done := make(chan error, 1)
	go func() {
		if _, err := io.WriteString(server, "Token:"); err != nil {
			done <- err
			return
		}
		reply := make([]byte, 4)
		if _, err := io.ReadFull(server, reply); err != nil {
			done <- err
			return
		}
		if string(reply) != "123\r" {
			done <- errors.New("bad challenge reply")
			return
		}
		_, err := io.WriteString(server, "r1#")
		done <- err
	}()
	if err = d.Connect(t.Context()); err != nil {
		t.Error(err)
	}
	d.Close()
	if err = <-done; err != nil {
		t.Error(err)
	}
}

func TestReceiptCancellationPreservesCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	d := deviceFixture(t, CiscoIOS(), Config{}, "r1#", []exchange{{"show\r", "show\r\nok\r\nr1#"}}, WithReceipt(func(Receipt) error { cancel(); return nil }))
	r, err := d.Execute(ctx, Command{Text: "show"})
	if !errors.Is(err, context.Canceled) || !r.Complete {
		t.Fatalf("late callback cancellation: %v", err)
	}
}

type partialTransport struct {
	net.Conn
	cause error
}

func (p *partialTransport) Write(b []byte) (int, error) {
	n, err := p.Conn.Write(b[:1])
	return n, errors.Join(err, p.cause)
}

func TestPartialWriteRetainsProgressAndNeverReplays(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	cause := errors.New("transport failure")
	d, err := New(CiscoIOS(), Config{Host: "example"}, WithDialer(func(context.Context, Config) (io.ReadWriteCloser, error) {
		return &partialTransport{client, cause}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	done := make(chan []byte, 1)
	go func() { _, _ = io.WriteString(server, "r1#"); received, _ := io.ReadAll(server); done <- received }()
	if err = d.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := d.ExecuteScript(t.Context(), []Command{{Text: "change"}, {Text: "never"}})
	if !errors.Is(err, cause) || len(result.Steps) != 1 || result.Steps[0].Written != 1 || result.Steps[0].Complete {
		t.Fatalf("partial write: %v %+v", err, result)
	}
	select {
	case received := <-done:
		if string(received) != "c" {
			t.Fatal("write replayed")
		}
	case <-time.After(time.Second):
		t.Fatal("failed stream not closed")
	}
}

func TestAuthenticationFailureRetiresConnection(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	d, err := New(CiscoIOS(), Config{Host: "example"}, WithDialer(func(context.Context, Config) (io.ReadWriteCloser, error) { return client, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	done := make(chan error, 1)
	go func() { _, err := io.WriteString(server, "Permission denied"); done <- err }()
	if err = d.Connect(t.Context()); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("authentication failure: %v", err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if d.Connected() {
		t.Fatal("failed login connected")
	}
}
