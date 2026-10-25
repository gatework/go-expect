# Migrating from Netflix/go-expect

This is a deliberate API change, not a drop-in module replacement. Change imports
to `github.com/gatework/go-expect` and use Go 1.26 or newer. No parallel legacy
reader/matcher remains.

| Old API | New API / responsibility |
| --- | --- |
| `Console.Expect(opts...) (string,error)` | `Console.Session().Expect(ctx, []Case, opts...) (Result,error)` |
| `ExpectString` / `Expectf` | A Case with `Literal(text)`; format strings with `fmt.Sprintf` |
| `String(...)` | One `Literal` Pattern per Case |
| `Regexp` / `RegexpPattern` | `Compile(expr)`; check the compile error |
| `Then(func(*bytes.Buffer)error)` | Case.Handler receives context, Session and detached Result; returns Action/error |
| `All(opts...)` | `All(patterns...)`, text patterns only; attach one handler to its outer Case |
| `ExpectEOF` / `EOF` / `PTSClosed` | Case with `EOF()`; PTY EIO normalization is inside Console |
| `Error(err)` | Handle the returned error with `errors.Is`/`errors.As` |
| `Console.Send` / `SendLine` | Corresponding Session method with context |
| Console configuration | `WithSessionOptions(...)` inside NewConsole |
| `WithStdout` | Session `WithOutput`; optional Redact filter applies |
| Payload observers | `WithObserver`, metadata only; inspect explicit Result fields for raw data |
| `WithStdin`, `Console.Read/Write/Fd` | No implicit relay or raw competing reader; drive Send explicitly with managed lifetime |
| `WithCloser` | Compose ownership in the supplied duplex transport; caller owns external sinks/processes |
| `ReaderLease` / `NewChanReader` / `PassthroughPipe` | Replaced by Session's cancellable waits and single owned pump |
| `NewTestConsole` / `NewTestWriter` | `expecttest.NewConsole` / `expecttest.NewWriter`, cleanup registered on testing.TB |
| `StripTrailingEmptyLines` | Application-specific presentation; not part of the interaction kernel |

Timing changed: legacy WithTimeout was effectively an idle timer reset for every
rune. New WithTimeout bounds a whole wait, WithIdleTimeout is explicit, and zero
means disabled. Use a parent context for an absolute operation deadline.

Matching changed: compare currently visible candidates by byte start position,
then declaration order. Captured input remains binary-safe. Buffers persist after
read timeouts, and successful matches retain their unread tail. Results from
continued conversations describe the final segment, so use a transcript for the
whole receive history.

Always check Send errors. A failed in-flight send closes the session and must not
be retried automatically: a device may have already received part of the reply.

Console still does not start or own processes. Attach Tty to exec.Cmd, Start,
close the parent's slave when appropriate, and arrange Wait and failure cleanup.
