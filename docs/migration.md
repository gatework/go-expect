# Migration to v0.4.0

This is a direct API evolution. Removed names have no aliases or transition code.

| Previous API | Replacement |
| --- | --- |
| ExpectAny(ctx, sessions, cases, opts...) | NewGroup(sessions...), then group.Expect(ctx, cases, opts...) |
| Index returned by ExpectAny | Selected *Session; use its immutable ID() |
| One case list applied everywhere | Optional Case.Sessions selects specific Group members |
| WithNoConsume() | Set Case.NoConsume on individual declarations |
| ResponseError | CaseLimitError, including SessionID and case/rule coordinates |
| Background.Stop() / Wait() | Stop(ctx) / Wait(ctx); Pause(ctx) / Resume(ctx) enable takeover |
| Background inherits session timeout | NoTimeout by default; explicit WithTimeout still overrides |
| conpty.Start | spawn.Start on every supported platform |
| spawn.WithConsoleOptions | spawn.WithSize, WithRaw, WithSessionOptions |
| Process.Wait() error | Process.Wait() (spawn.ExitStatus, error) |
| Process.SetEcho | Assert spawn.EchoController on Process.Terminal() |

A paused background does not own or inspect foreground input. Its relative and
idle budgets freeze; the parent context never freezes. Resume while another
receiver is active returns ErrBusy without changing the paused state. Stop
cancels the operation; its context bounds joining cooperative callbacks. A Wait
context only bounds the join, not the operation. Do not synchronously invoke
controls from their own callbacks. Async callback panic/Goexit reports a payload-free
CallbackError. Foreground callback panic/Goexit still propagates after cleanup.

SetRules replaces both persistent groups, validates/clones the complete snapshot
before installation, and resets replaced-rule counts. Foreground/per-call counts
and timers remain. New snapshots apply after the current callback. Empty groups
clear rules. Handler closures remain borrowed. NoConsume Continue suppresses that
candidate until the receive-buffer or rules generation changes, avoiding a busy loop.

Local interaction now accepts Literal, Line, All, Go regexp/Glob and EOF/Timeout/
Default/FullBuffer events. WithInteractInputTimeout is an independent idle interval. A local
EOF callback error is an error, even if errors.Is(err, io.EOF). Literal patterns
share immutable KMP prefix tables; regex feasibility is conservative within the
explicit matching window. Patterns that remain ambiguous at the window limit
return ErrBufferLimit. Input NoConsume is rejected: use WithInteractInputForward
for matched-byte passthrough. Echo is redacted; cooked callbacks require
InteractTerminal. Remote passthrough remains the default; disabling it filters
matched bytes and forwards only safe unmatched prefixes.

Relay sources/targets are borrowed Sessions. Each source has one Route; repeated
sources and self-targets are rejected. Targets use serialized raw writes, operator
outputs use independent secret filters. QueueLimit bounds queued/in-flight payload
per route/target, with the source's matching limit as default; transport and
redaction storage are additional. Slow targets apply backpressure. Multiple sources
can interleave chunks at one target; no command/response association is implied.
On failure, already delivered bytes are not replayed. NoConsume route cases/rules
are rejected because preserving forwarded input would duplicate delivery.

spawn.WithEnv follows os/exec: nil inherits, an explicit empty environment replaces
it. WithDir defaults to inherited directory. ExitStatus carries PID, Code and Unix
Signal; ExitError formatting excludes program/environment data. Terminal supports
resizing everywhere and EchoController only on Unix. Close preserves actual exit
status. Unix descendants must cooperate with group SIGHUP; Windows uses a Job.

---

## Historical migration to v0.3.0

Replace `WithEscapeAction(key, handler)` with
`WithInteractInputCases(Case{Pattern: Literal(string([]byte{key})), Handler: handler})`.
Local sequences can contain multiple bytes; other local pattern kinds are rejected.
Rename `WithInteractCases` to `WithInteractOutputCases`. `WithEscape` remains the
exit-byte option. No compatibility aliases or transition wrappers are provided.

ExpectAny now listens to all sessions after Continue. Continue resets active
relative budgets together; ContinueKeepTimer and per-session idle budgets retain
their deadlines. Equal rule groups rotate by session order. Use Result.Source,
Scope and RuleIndex to identify match origin; trace/completion metadata and
ResponseError expose the same coordinates. CompileError now also matches
ErrInvalidPattern through errors.Is.

# Migration from v0.1.x to v0.2.0

This update intentionally removes old interfaces. There are no aliases or
transition wrappers. Move callers directly to the unified Case/Session model.

| Previous API | Current API |
| --- | --- |
| Rule / NewResponder / Responder.Expect | Case{Limit, Handler}, Session.Expect with WithRules |
| Rule.Respond | Case.Handler = Then(response), or ReplyThenContinue(text) |
| ContinueWithoutReset | ContinueKeepTimer |
| WithDeclarationPriority | WithMatchOrder(Declaration) |
| WithLogger(*log.Logger) | WithLogger(*slog.Logger) |
| Nonzero Return | Return is zero; use named Action values |

Before/After rules take precedence by group. Ordinary call cases default to no
limit, reusable rules default to one per wait, and completion wins a scoped-rule
tie. WithRules validates through Expect, not a separate responder constructor.
EOF rules are supported. Rule outcomes expose Scope and RuleIndex; CaseIndex is
reserved for foreground cases. ConfigError and CompileError support errors.As;
ErrInvalidPattern supports errors.Is.

Result now exposes byte offsets and named captures. Registering Redact without an
output retains secret definitions for future Interact; matching tables are still
lazy, and Close clears the definitions. Fold selects Go leftmost-first regexp
semantics, including for imported POSIX/Longest patterns.

Replace manual child stdio attachment with Console.Start to obtain a controlling
terminal, or use spawn.Start for owned process cleanup. A successful Start closes
the parent's slave. Windows uses the dedicated conpty package. Neither process
adapter changes the core transport ownership contract.

The earlier device connector was moved to
[go-net-connector](https://github.com/gatework/go-net-connector). Historical release
behavior and benchmark evidence remain available in Git at v0.1.4.
