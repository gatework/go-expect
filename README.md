# go-expect

Go 1.26+ conversations with interactive programs and duplex streams: bounded
matching, continuations, reusable rules, process terminals and streaming secret
redaction. Import `github.com/gatework/go-expect`.

`NewSession` takes exclusive ownership of an `io.ReadWriteCloser` only on success.
Its `Close` must interrupt concurrent reads and writes. Register secrets with
`Redact` before sending or receiving them; matching results retain raw bytes.

The example below is copied from [example_test.go](example_test.go), compiled and
executed by `go test`. `script/readme.py` checks that the copy stays synchronized.

<!-- example:start -->
```go
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
```
<!-- example:end -->

Run the longer password, confirmation and paging example with
`go run ./examples/dialogue`. Device authentication, vendor profiles and backup
policy belong to [go-net-connector](https://github.com/gatework/go-net-connector),
not to the stream engine.

| Task | API and behavior |
| --- | --- |
| Match text | `Literal`, `Line`, `Compile`, `FromRegexp`, `Glob`, `All` |
| Case folding | `Pattern.Fold`; Unicode regexp folding, Go leftmost-first matching |
| Preview | `Case.NoConsume`; Continue observes each unchanged candidate once |
| Inspect match | `Result.Buffer`, `Capture(name)`, `Indices(group)`; half-open byte offsets |
| Repeat | `Continue` resets the relative timer; `ContinueKeepTimer` preserves it; zero action is `Return` |
| Recover timeout | `Timeout`, `Default`, `FullBuffer` cases with bounded `Case.Limit` |
| Reuse rules | `WithBefore`, `WithAfter`, per-call `WithRules`; `Session.Rules` returns a copy |
| Reply | `ReplyThenContinue`, or `Then(Response)` for dynamic replies |
| Send | `Send` writes once; `SendAll` drains peer input while writing suffixes |
| Slow devices | `SendSlow`, `SendPaced`, bounded `SendNull`, transport-capability `SendBreak` |
| Trace safely | `WithLogger(*slog.Logger)`, `WithObserver`, `WithTrace`; metadata only |
| Record streams | `WithOutput` and `WithSendOutput`; independent redacted streams |
| Take over | `Interact`, local Literal/Go regexp/Glob actions and input EOF/idle events; `InteractTerminal` for Unix raw input |
| Watch several | `Group.Expect`; bound cases, live Add/Remove, fair continuation across members |
| Watch asynchronously | `StartBackground`; Pause/Resume for foreground takeover; context-bounded Stop/Wait |
| Normalize input | Opt-in `WithRemoveNulls(true)`, `WithStripParity(true)` |

Before rules take precedence over call cases, then After rules. Within a group,
the earliest visible byte wins; declaration order breaks ties. Per-call rules
follow foreground cases, so completion wins an exact tie. `WithMatchOrder(Declaration)`
selects the first declared matching case within its group. Limits count per wait;
rule/event defaults are one invocation, and ordinary cases default to unlimited.

`Timeout` handles only the relative wait timer; parent cancellation and idle
timeouts remain errors. Its callback receives a fresh relative budget, allowing
it to send and `Continue`. `Default` handles relative timeout or clean EOF.
`FullBuffer` explicitly consumes the full unmatched snapshot; without it overflow
returns `ErrBufferLimit` and preserves data. No automatic trimming occurs.

Use a parent context to bound the whole dialogue. A matching prompt is not proof
that a command succeeded. Sends are serialized, but callers must sequence commands
and responses. Failures after write admission retire the session and return exact
`WriteError.Written` progress; commands are never retried implicitly.

`Interact` owns its input reader after admission and closes it on exit. Output is
borrowed and filtered. Control-] exits by default. Use `WithInteractInputCases`
for local text patterns and EOF/Timeout cases, `WithInteractInputTimeout` for
local idle timing, and `WithInteractOutputCases` for remote patterns; callbacks run on the receive owner. `InteractTerminal` borrows an
`*os.File`, duplicates it, enables raw mode and restores the original terminal
state. Interaction retains a matching window of min(64 KiB, buffer limit), consuming
half when unmatched (at least one byte), so patterns
requiring arbitrary historical prefixes are unsuitable. `Stop` and interactive
cleanup join cooperative callbacks; `Session.Close` does not wait for user code.

| Process adapter | Ownership and platform |
| --- | --- |
| `Console.Start(*exec.Cmd)` | New session and controlling terminal; caller must cancel and Wait |
| `spawn.Start(ctx, program, args, ...)` | Unix PTY or Windows 10 1809+ ConPTY, with common environment/directory/status options |

`spawn.Process.Wait` returns `(ExitStatus, error)`; a failed exit is a typed
`spawn.ExitError`. `PID` and `Terminal` expose identity and terminal capabilities
without exposing the transport. Unix PTYs and Windows ConPTY default to 24 by 80.
Unix cancellation sends group SIGHUP with a two-second direct-child kill fallback;
descendants must cooperate. Windows uses a kill-on-close job. Consume output
before Wait, or Close to interrupt an undrained child. FreeBSD has compile/vet
coverage; native runtime coverage is Linux, macOS and Windows.

`Session.SetRules` atomically replaces Before/After rules at the next scan boundary.
`Case.Sessions` binds Group declarations; an empty selector matches every member.
Group controls acknowledge ownership changes, and run outside callbacks. Background
Pause releases input after callbacks finish, freezing local budgets while parent
deadlines keep running. Resume returns ErrBusy while a foreground receiver owns it.

`Relay` connects explicit `Route{Source, Targets, Output, Cases}` declarations.
Targets receive raw bytes through bounded queues; operator Output is independently
redacted. Matched bytes are withheld unless ForwardMatches is selected. EOF/Return
drains queued output; errors cancel and join admitted writes without replay.

| Tcl expect convention | This library |
| --- | --- |
| Declaration order | Earliest byte by default; opt into `Declaration` |
| 10-second timeout | 30-second wait, 10-second Send |
| -1 means unlimited | `NoTimeout` (zero); negatives are invalid |
| Unhandled timeout returns | `ErrTimeout` |
| `match_max` may drop old data | Bounded input; overflow preserves it |
| NUL removal by default | Bytes preserved unless filtering is enabled |
| Tcl ARE expressions | Go RE2; no backreferences or lookahead |
| Typical send terminator CR | `Send`/`Reply` verbatim, `SendLine` defaults to LF |

See [module organization](docs/architecture.md), [contracts](docs/contracts.md), [migration](docs/migration.md),
[verification](docs/verification.md) and [release operations](docs/releasing.md).
The source retains Netflix's Apache-2.0 license and copyright notices.

Run `bash script/ci` for local checks. CI runs Linux/macOS Go 1.26/1.27 and Windows
runtime tests. Default-branch pushes publish after all gates pass. Before v1,
`BREAKING:` or `BREAKING CHANGE:` commit trailers select the next minor version;
ordinary changes increment patch. No compatibility aliases are provided.
