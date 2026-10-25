# go-expect

A Go library for scripted conversations with interactive programs and duplex
streams. It supports literal/regexp matching, `exp_continue`-style callbacks,
reusable password/confirmation/paging rules, cancellation, bounded buffers and
explicit secret redaction.

```go
import expect "github.com/gatework/go-expect"
```

Requires Go 1.26+. The PTY adapter is supported and tested on Linux and macOS.
This is an API evolution of Netflix/go-expect; see [migration](docs/migration.md).
The original Apache-2.0 license and copyright notices are retained.

## Password, confirmation and paging

Start with an already connected `io.ReadWriteCloser` (for example `net.Conn`), or
create a Console and attach its `Tty()` to a caller-owned `exec.Cmd`. A successful
`NewSession` takes exclusive ownership of the stream, including closing it. The
stream's `Close` must interrupt both `Read` and `Write`.

```go
s, err := expect.NewSession(conn)
if err != nil {
    return err // conn still belongs to the caller
}
defer s.Close()

// Register before the secret can be sent or echoed.
if err := s.Redact(password); err != nil {
    return err
}
login, err := expect.NewResponder(expect.Rule{
    Pattern: expect.Literal("Password:"),
    Respond: expect.Reply(password + "\r"),
})
if err != nil {
    return err
}
if _, err := login.Expect(ctx, s, []expect.Pattern{
    expect.Literal("device>"),
}); err != nil {
    return err
}

rules, err := expect.NewResponder(
    expect.Rule{
        Pattern: expect.Literal("Apply change? [y/N] "),
        Respond: expect.Reply("y\r"),
        Limit: 2,
    },
    expect.Rule{
        Pattern: expect.Literal("--More--"),
        Respond: expect.Reply(" "),
        Limit: 100,
    },
)
if err != nil {
    return err
}
for _,command := range commands {
    if _, err := s.Send(ctx, command + "\r"); err != nil {
        return err
    }
    if _, err := rules.Expect(ctx, s, []expect.Pattern{
        expect.Literal("device>"),
    }); err != nil {
        return err
    }
}
```

Use a parent context deadline to bound the entire operation. A returned prompt
means the prompt matched; inspect device-specific failure output separately.
There are no built-in "answer yes to everything" rules, host-key acceptance,
connection establishment or command retries.

A complete runnable example, including dynamic regexp replies:

```sh
go run ./examples/dialogue
```

It simulates a local device with `net.Pipe`; it does not connect to a real device.

## Continue matching

```go
result, err := s.Expect(ctx, []expect.Case{
    {
        Pattern: expect.Literal("Password:"),
        Handler: func(ctx context.Context, s *expect.Session, r expect.Result) (expect.Action, error) {
            _, err := s.Send(ctx, password + "\r")
            return expect.ContinueWithoutReset, err
        },
    },
    {Pattern: expect.Literal("device>")},
}, expect.WithTimeout(30*time.Second))
```

`Continue` resets the relative timer. `ContinueWithoutReset` preserves it.
Neither extends the parent context deadline. Responder uses the latter.
`Result` contains the final segment only; install `WithTranscript(writer)` to
record the filtered receive history across intermediate responses.

## PTY and processes

```go
c, err := expect.NewConsole(
    expect.WithSessionOptions(expect.WithDefaultTimeout(10*time.Second)),
)
if err != nil {
    return err
}
defer c.Close()
cmd := exec.CommandContext(ctx, "your-program", "literal-argument")
cmd.Stdin, cmd.Stdout, cmd.Stderr = c.Tty(), c.Tty(), c.Tty()
if err := cmd.Start(); err != nil {
    return err
}
// The child has inherited the slave. Release the parent's copy for EOF.
c.Tty().Close()
// Use c.Session().Expect / Send; always arrange cmd.Wait and failure cleanup.
```

Console does not own the child process. `WithRaw()` is available for applications
requiring unbuffered keys and no terminal echo; canonical mode is the default.
See the complete process cleanup and device dialogue in `console_test.go`.

## Contracts and verification

[API contracts](docs/contracts.md) describe matching, deadlines, error recovery,
ownership, thread safety and redaction. Test utilities live in `expecttest`.

```sh
bash script/ci
```

This runs format checks, module verification, vet, race tests (including local
PTY), fuzz smokes, benchmarks, the dialogue example and an independent consumer
module. CI covers Linux/macOS on Go 1.26 and 1.27. Local tests and CI runs are
separate evidence; neither substitutes for real device interoperability.

## Automatic releases

Each push to `master` publishes a release after the complete CI gate passes.
Versions start at `v0.1.0` and increment the patch number automatically. The
publisher reuses the exact CI source artifact, verifies uploaded checksums and
bytes, and resumes interrupted drafts without replacing published assets.
PRs and feature branches do not publish. See [release operations](docs/releasing.md)
for version policy, retries, queue behavior and local validation commands.
