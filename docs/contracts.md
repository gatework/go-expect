# API contracts

## Session ownership and concurrency

`NewSession(io.ReadWriteCloser, ...SessionOption)` validates options before
starting I/O. Nil and typed-nil transports/output writers are rejected. On success
it owns the transport; on failure ownership remains with
the caller. The transport must allow concurrent Read/Write and make Close unblock
both. A `net.Conn` satisfies this; an SSH adapter must implement the same lifetime
contract. An arbitrary Reader/Writer without interruptible Close does not.

Use constructors, not zero-valued Session or Console. Do not copy these objects.
Session permits one active Expect; another or nested Expect returns `ErrBusy`.
A handler may call Send, Redact or Close. Sends serialize with cancellable lock
acquisition. This prevents interleaved writes, but does not assign concurrent
commands to their responses: command sequencing belongs to the caller.

Close marks the session closed, closes the transport, waits for I/O workers and
finishes pending secret-filter output. It is idempotent and retains cleanup
errors. It does not wait for user handlers or observers. Output sinks and logs
are borrowed and never closed. Sinks must not reenter Session; observers run
outside internal locks, may be called concurrently by different operations, and
must synchronize any shared state. User code and synchronous sinks must cooperate
with cancellation; a context cannot forcibly stop them.

The single receive pump uses 16 KiB blocks and a bounded queue. The match buffer
(default 8 MiB) has a separate bound; a staged block and the pump's fixed-size
blocks are additional bounded storage.
Small reads copy only received bytes so the pump can reuse its private scratch
block; larger reads transfer block ownership. Neither path reuses queued bytes.
`ErrBufferLimit` preserves input. A later Expect can consume already buffered text
and resume reading. At exactly the limit,
a bounded next read event distinguishes EOF from overflow; if no event arrives,
the ordinary wait deadline applies. No automatic tail
trimming or search window silently changes matching semantics.
Exhausted receive slices release their storage. At an operation boundary a
remaining slice detaches if its original allocation is at least 64 KiB and the
remainder uses at most one quarter of it. Continuations do not copy the entire
remaining burst; returned Results remain independently owned.

## Matching and results

`Literal` matches nonempty raw byte strings. `Compile` accepts Go regexp syntax;
compilation errors do not echo the expression. Patterns are immutable.
`All` accepts one or more text patterns, in any order, consuming from the first
match through the last; repeated patterns can match the same occurrence and
aggregate captures are omitted. EOF is not a text pattern and cannot occur in All
or a responder rule.

The earliest start position in the currently available buffer wins. Declaration
order breaks ties. This differs from legacy rune-by-rune matching. A short pattern
can match before a longer alternative has arrived. Regex greediness and end
anchors see the currently available buffer, not future bytes. Use complete,
unambiguous prompts; do not rely on packet boundaries or future alternatives.
Trailing incomplete UTF-8 is held back from regex inspection until more input or
EOF. Invalid complete encodings follow Go regexp behavior; stored bytes are never
rewritten. Literals can match arbitrary binary sequences.

A successful match consumes Before and Match. The tail remains for the next
iteration/call. Empty/zero-width matches fail before consumption or callbacks.
Handlers run after consumption; a handler error preserves the tail and never
replays the completed action. Invalid handler actions return an error.
Handler panics and `runtime.Goexit` propagate normally; derived context cleanup
and exclusive-wait release still run. Such exits do not emit a completion event.

Result contains `CaseIndex`, `Before`, `Match` and `Captures`. A Capture has `Text`
and `Matched`, distinguishing an unmatched optional group from an empty match.
Result fields are detached snapshots with raw input. Formatting (`%v`, `%+v`,
`%#v`) omits contents. Explicit field access or serialization is raw by design.
Result describes only the final segment, not all the intermediate replies.

Input arriving together with an error is processed before that error. Buffered
text matches precede EOF. An explicit EOF case returns success with remaining
input in Before; otherwise Expect returns `io.EOF`. A continuing EOF handler runs
once and returns EOF, never spins. EOF is distinct from Session.Close and from a
child process exit status. Linux PTY EIO is normalized to EOF only in the PTY
adapter. Other errors retain `errors.Is`/`errors.As` access.

## Timing, sending and failure recovery

Default Expect timeout: 30 seconds. Default Send timeout: 10 seconds. Zero
explicitly disables either relative timeout; negative durations are invalid.
`WithIdleTimeout` is disabled by default and resets when input is accepted by the
matching engine. Buffer inspection and handlers do not reset it.

`Continue` resets the relative Expect timeout after the handler.
`ContinueWithoutReset` keeps the original deadline. The parent context deadline
is never extended. A handler receives a context bounded by the current relative,
idle and parent deadlines. Sends inside handlers also have the write timeout.
Context checks are cooperative; expensive regex calls and user code are not
forcibly interrupted. Match timeout and idle timeout return `ErrTimeout` and
`ErrIdleTimeout`; parent cancellation returns its context error.

Read timeout/cancellation preserves the session and unconsumed buffer. Result.Before
contains the current unmatched prefix. Zero timeout is unlimited, not polling.
Pre-canceled and invalid calls also report an existing pending prefix when they
acquire the exclusive wait. ErrBusy returns no input because another wait owns it.

Send attempts one transport Write. It never retries a short write, the remaining
suffix or a failed confirmation. Cancellation while waiting for the send lock
leaves the session open. Once a write starts, cancellation, short write or failure
closes the session, joins the write and returns `WriteError{Written, Cause}`.
`Written` is an actual transport byte count, not evidence the device executed a
command. Out-of-range transport counts are rejected and reported as zero on every
exit path. Cancellation retains both its context cause and the transport failure.
Cancellation and Close are checked again when a write result is received, so an
already observable stop is not hidden by a simultaneously ready result.
`errors.Is` unwraps the original cause; a short nil-error write becomes
`io.ErrShortWrite`. A successful zero-length Send performs no transport write.

Send is verbatim. SendLine appends LF by default; WithLineEnding accepts LF, CR or
CRLF. Reply never appends a line ending implicitly.

## Responder

NewResponder copies and validates immutable rules. Rule callbacks are borrowed
closures and must synchronize their own state if shared. Limit=0 means one
response, a positive value sets the per-call limit, and negative values are
invalid. Counts are independent across calls and sessions.

Responder.Expect explicitly activates the rules for that call. Completion
patterns win only at the same start position; otherwise earliest position wins.
A dynamic Response reads the matched Result and returns an error on failure.
All responses continue without resetting the timer. Limit checks run before
consuming or replying; `ResponseError.RuleIndex` is zero-based. On a response
failure, Result.CaseIndex is -1; on completion it indexes the completion slice.
EOF and timeout return through the same matching engine. Register failure prompts
as completion patterns and inspect their index; the library does not infer
business success, unknown answers, credentials or command retries.

## Diagnostics and secrets

Call Redact with nonempty secrets before sending or receiving them. Registration
is append-only and cannot retract earlier output. Password replies are not guessed
or automatically registered; the caller decides what is secret. Register the
password itself, without its line terminator, to mask common echo behavior.

WithOutput and WithTranscript receive the same receive-only filtered stream,
as input is accepted by the matcher. Continued matches and subsequent Expect
calls do not replay it. The transcript includes received tails beyond a match
that were already accepted, but not unread data discarded by Close. To drain to
the peer's end, Expect EOF before Close.

The filter masks every byte of literal secrets with `*`, including overlapping
matches. It retains possible secret prefixes across chunks; EOF and Close mask
unfinished prefixes conservatively. Added rules also apply to pending un-emitted
bytes. Masked output is not necessarily immediate or suitable as a transparent
protocol relay. Raw matching input and Result fields are unaffected.

Logs and Observer Events contain operation, case index, byte count and error
classification only. All registered observers receive each completion, including
failed/rejected waits. They do not receive raw error messages or payloads.
Responder event case indices match the public result, never internal rule indices.
A sink failure stops processing and closes the session; previously delivered output and
response effects are not retried.

## Explicit compatibility operations

`FromRegexp` takes an isolated snapshot including POSIX/Longest matching settings.
`WithDeclarationPriority` chooses the first declared matching case instead of the
earliest byte. Without the option, earliest-byte ordering is unchanged.
`WithStreamingFallback` explicitly consumes 32 KiB when 64 KiB is pending and no
ordinary case matches. Its retained tail is bounded; it is not a guarantee for
patterns spanning arbitrarily long prefixes. Its callbacks do not reset timers.
`Poll` and `SendAllPoll` require `WithOSPolling` and an OS-backed nonblocking stream;
zero relative timeout still means wait without that timer, never poll.
When the transport also supports working `SetReadDeadline(time.Time) error`, the
sole read pump waits in the Go runtime poller. Otherwise construction selects the
native poll fallback. This choice never changes midstream. Each explicit Poll
publishes one request and wakes the runtime reader with a deadline. The pump
publishes earlier bytes, clears that deadline, performs an immediate readiness
read, publishes its bytes, and acknowledges the request. A canceled caller leaves
that request owned by the pump until acknowledgment; the next Poll establishes
its own readiness boundary. Close joins the pump without adding wake descriptors.
Adapters may forward `NormalizeReadError(error) error` for raw read errors. PTY
adapters use it to translate EIO into EOF; ordinary EIO and control errors retain
their original meaning.
`SendAll` sends only positive-progress unsent suffixes, queues bounded peer input
to prevent duplex backpressure deadlock, and retires on any subsequent failure.
Its parent context owns the deadline; WithWriteTimeout applies to Send only.
`NewRedactor` supports a chosen replacement and scoped secret changes without
allowing previously hidden bytes to reappear. The Session filter is unchanged.
Public redaction compiles rules longer than 256 bytes once per rule scope and
maintains their incremental KMP state. It merges ordered covered intervals,
reuses a bounded retained suffix, and detaches oversized transient storage.

Close cancels the active handler context but does not wait for user code. Handlers
must cooperate with cancellation. Automatic retirement preserves independent
cleanup causes through errors.Is/As; writes still expose exact Written progress.
