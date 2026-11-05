# API contracts

## Ownership and concurrency

A successful `NewSession` owns the transport exclusively. Failed construction
performs no I/O and leaves ownership with the caller. Nil and typed-nil streams
and writers are rejected. Transports must support concurrent Read/Write and make
Close interrupt both. Sinks are borrowed, synchronous, never closed or retried,
and must not reenter Session. Observers may execute concurrently and must protect
shared application state. Constructors are required; do not copy Session or Process.

One receive pump owns Read. Expect, Poll, Interact and background waits reserve
one exclusive receiver; overlapping operations return `ErrBusy`. `ExpectAny`
reserves all sessions atomically from the caller's perspective, releases every
reservation on failure, previews without handlers or consumption, joins previews,
and dispatches only the first ready winner. Simultaneous winners are unspecified.
Losers retain their accepted input. Receive output recording can run on losers.
The winner retains its preview idle deadline while other previews are joined.
Receive reservations are released before completion observers run.

Sends serialize across entire operations. This prevents byte interleaving, not
command/response misassociation. Close marks the session closed, interrupts I/O,
joins owned I/O workers and flushes secret filters. It is idempotent and returns
stable joined cleanup errors. It cancels the active handler context but does not
wait for user callbacks. Background.Stop and Interact cleanup do join callbacks;
callbacks must cooperate. Handler panic/Goexit propagates with deferred cleanup.

## Matching, rules and bounds

Literal and Line match raw bytes, including invalid UTF-8. Line matches the final
line with optional space/tab/NUL padding and includes CR/LF delimiters. Regexp and
Glob use Go Unicode/RE2 semantics. Glob is unanchored, uses `*`, `?`, ranges and
backslash escapes; stars cross newlines. A glob that matches empty input is
rejected when selected, like any zero-width regexp. Incomplete trailing UTF-8
waits for more bytes or EOF before regexp inspection.

FromRegexp snapshots the original expression including POSIX/Longest settings.
Fold recompiles into Go's Unicode case-insensitive, leftmost-first regexp mode;
it deliberately selects that mode even for an imported Longest expression.
All is a conjunction of text patterns in any order; repeated leaves may share
an occurrence, and aggregate captures are omitted. Event patterns are not text.
Invalid patterns return ErrInvalidPattern; CompileError retains only syntax code.

Selection priority is Before, call cases/rules, After. Within a group the earliest
visible start wins and declaration order breaks ties; Declaration order mode
chooses the first matching declaration. Foreground cases precede scoped rules.
Persistent rules and WithRules declarations are immutable snapshots; returned
Rules slices can be edited without changing the Session. Foreground Case slices
must remain unchanged until Expect returns. Callback closures remain
borrowed. Case.Limit is per wait and checked before consumption/side effects.
Ordinary cases default to unlimited; rules and Timeout/Default/FullBuffer default
to one. Negative limits are invalid. ResponseError exposes the exhausted rule index.

A match normally consumes Before and Match. WithNoConsume preserves them and
requires handlers to Return. Result contains detached raw strings, final segment
only, and half-open byte Start/End offsets relative to the buffer seen by that
match. Capture holds Name, Text, Matched and Start/End; absent groups use -1/-1.
Indices(0) addresses the whole match. Result.RuleIndex plus Scope identify a rule;
CaseIndex is -1 for rule matches and failures. Explicit access/serialization is
raw; String/GoString formatting omits payloads, as do Pattern and Capture.

Continue resets the relative timer. ContinueKeepTimer preserves it; Return is
zero. ReplyThenContinue and Then use ContinueKeepTimer. Parent deadlines are never
extended. Handler errors preserve the remaining tail and never replay responses.
EOF matches after buffered text and carries remaining input in Before; continuing
EOF handlers run once then return io.EOF. Invalid read counts remain failures,
even if their cause contains EOF. Unix PTY EIO alone is normalized by the adapter.

The match buffer defaults to 8 MiB, plus bounded staged/pump storage. Small reads
copy their used bytes, large reads transfer exclusive blocks. Overflow preserves
input and returns ErrBufferLimit. Exactly-full input waits for one bounded
lookahead to distinguish EOF from overflow. A FullBuffer case explicitly consumes
the full snapshot; there is no implicit drop. WithStreamingFallback, and Interact,
explicitly transfer 32 KiB when 64 KiB remains unmatched. These modes cannot
promise matches across arbitrary historical prefixes. Empty slices release
storage; operation return detaches small tails of allocations at least 64 KiB.

## Deadlines and writes

Wait defaults to 30 seconds; Send defaults to 10 seconds including lock admission.
NoTimeout=0 disables the relative timer; negative durations are ConfigError.
Idle timing is disabled by default and resets only on accepted nonempty input.
Parent cancellation, relative expiry and idle expiry have distinct errors.
Timeout cases handle only the relative timer, with a fresh callback budget so a
send is possible. Continue rearms that timer; ContinueKeepTimer can immediately
fire it again, subject to Limit. Default handles relative timeout or clean EOF,
never parent cancellation or arbitrary transport errors. Read-side cancellation
preserves the session and pending input. Invalid/pre-canceled exclusive waits
also snapshot pending input; ErrBusy exposes none.

Send attempts exactly one Write. Deadline-capable transports use synchronous
writes; unsupported transports use a joined worker. Cancellation before admission
preserves the session. Any failure after transport entry retires it, joins the
write and returns WriteError with exact transport progress and all causes.
Invalid counts are rejected as zero; short nil-error writes become io.ErrShortWrite.
A ready cancellation wins over a simultaneous write result. Written bytes do not
prove remote execution. Empty sends perform no transport Write.

SendAll writes only positive-progress unsent suffixes while draining bounded peer
input to avoid duplex backpressure deadlock. Its parent context bounds the whole
operation; WithWriteTimeout applies only to Send. SendSlow holds serialization
across chunks and delays; cancellation after progress retires the session.
SendNull sends literal NULs. SendBreak requires an interruptible transport break
capability; Unix PTYs implement it. Send and Reply are verbatim; SendLine appends
the configured LF (default), CR or CRLF.

Poll/SendAllPoll require WithOSPolling and a nonblocking OS-backed stream. The sole
pump uses runtime deadline coordination when available, native readiness otherwise.
Canceled polls leave their request pump-owned until acknowledgement; a later Poll
establishes a fresh readiness boundary. No second transport reader is created.

## Diagnostics and redaction

Redact validates nonempty literal secrets and registers append-only definitions
before they can appear. It never retracts delivered output. Definitions remain
available for future Interact even without sinks; scanning tables are created
only for active outputs. Close clears all private definitions and filter storage.
WithOutput records receive bytes on acceptance, WithSendOutput records actual
transport-accepted bytes. Each stream has independent prefix state. Overlaps and
unfinished secret prefixes are masked conservatively; EOF/Close flush them.
Adding rules also scans un-emitted suffixes. Raw matching/results stay unchanged.

Sink failures retire the Session, retain their cause and are never retried.
Previously delivered outputs and actions are not replayed. Diagnostic callbacks,
slog records and TraceEvent contain metadata only, never input, regex source or
raw error text. ConfigError does not retain caller option values. WriteError's
formatted message includes progress/class only; errors.Is/As explicitly unwraps
causes. Standalone Redactor supports replacement strings and scoped rules; its
Finish resets stream state while retaining its rules.

## Interaction and processes

Interact owns its input reader after receive admission; Close must interrupt its
Read. It closes and joins input on EOF, escape, remote completion, cancellation or
panic. Output is borrowed and redacted, including the already pending prefix.
The default escape is control-]; escape actions intercept individual local bytes,
and remote cases use the same waiter as Expect. Input errors and parent
cancellation propagate. InteractTerminal duplicates a borrowed terminal, changes
raw mode/nonblocking flags temporarily and restores them after joining input.
The caller must not concurrently read/change that terminal.

Console.Start attaches stdio to the slave with Setsid/Setctty/Ctty=0, rejects
conflicting terminal attributes, and closes the parent's slave after successful
start. The caller still owns cancellation and Wait. Console.Close closes handles.
SetSize and SetEcho use RawConn.Control, preserving descriptor blocking state.

spawn.Start owns the child and PTY. Context cancellation signals the process group
with SIGHUP; a two-second WaitDelay kills an unresponsive direct child. Descendants
must cooperate with SIGHUP. Close joins the sole child waiter; Wait reports exit
status independently of Close. Unix sizes are 1..65535, default 24 by 80.

conpty.Start owns a Windows job and pseudoconsole, creates the child suspended,
assigns it to the kill-on-close job, then resumes it. Close closes output/input
pipes before closing the pseudoconsole to avoid old-Windows teardown deadlock,
kills the job and joins the child. Normal Wait drains final console output and
may wait for the caller to consume it; Close interrupts this. ExitError reports
nonzero status. ConPTY sizes are 1..32767 and streams contain UTF-8/VT sequences.
Unix-only echo/break controls and OS polling are not provided by this adapter.
EOF is never a substitute for checking process Wait.
