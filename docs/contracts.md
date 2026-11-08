# API contracts

## Ownership and concurrency

NewSession takes exclusive ownership of a duplex transport only on success.
Options validate before I/O. Close must interrupt concurrent Read/Write. Nil and
typed-nil streams/sinks are rejected. One receive pump owns transport Read;
Expect, Poll, Interact, Background and Group reserve one receiver per Session.
Conflicting receivers return ErrBusy. Send operations serialize entire operations;
callers still sequence commands and responses. Borrowed sinks execute synchronously,
are never closed/retried, must not reenter Session and must cooperate with shutdown.
Constructors are required; do not copy Session, Group, Background or Process.

Close marks closed, cancels the active handler scope, interrupts I/O, joins owned
workers and flushes filters. It is idempotent, retaining stable cleanup errors.
It does not wait for user callbacks. Foreground panic/Goexit propagates after defer
cleanup. Library-owned asynchronous callback boundaries return CallbackError without
retaining panic payloads. Send sink panic always releases lifecycle references;
async send failures retain actual transport progress. Observers run after receive
reservations release and may run concurrently across operations.

A SessionID is immutable and process-local. Result/Event/TraceEvent/errors identify
members by this ID, so dynamic Group membership does not change their identity.

## Matching, rules and bounds

Literal/Line match raw bytes, including invalid UTF-8. Line accepts a nonempty
final line, optional space/tab/NUL padding and CR/LF delimiters. Go regexp/Glob
use RE2/Unicode semantics. Glob is unanchored, stars cross newlines, reversed
ranges work, and Fold lowercases range endpoints before ordering. FromRegexp
snapshots POSIX/Longest settings; Fold deliberately selects leftmost-first mode.
All is a conjunction in any order; repeated leaves may share occurrences and omit
aggregate captures. Empty selected matches fail with ErrEmptyMatch. Incomplete
trailing UTF-8 waits for more bytes/EOF. CompileError unwraps ErrInvalidPattern.

Priority is Before, foreground/per-call rules, After. Within a group the earliest
visible byte wins and declarations break ties; Declaration selects the first
matching declaration. Foreground cases precede per-call rules. Case.Limit defaults
to unlimited for ordinary text and one for reusable rules/Timeout/Default/FullBuffer.
Limits are per operation and checked before consumption/side effects. CaseLimitError
identifies SessionID, Source, Scope, CaseIndex and RuleIndex.

SetRules atomically replaces cloned Before/After snapshots. Empty snapshots clear
them. Active waits switch after the current callback; replaced rules get fresh
counters, foreground/per-call counters and deadlines remain. Rules returns detached
slices; closures remain borrowed. Caller foreground declarations must remain
unchanged until their operation returns. Case.Sessions binds Group members; an
empty selector matches every member. Event cases honor these selectors too.

Case.NoConsume preserves Before/Match. Continue may observe an unchanged text
candidate only once per receive-buffer/rules generation; other cases and fresh
input can then progress. Return completes the operation. Result contains raw,
detached final-segment strings, captures and half-open byte offsets. Unmatched
captures use -1/-1. Capture(name), Indices(group) and Buffer provide explicit raw
access. Formatting omits payloads. Failures use -1 case/rule indices.

Matching defaults to 8 MiB plus bounded staged/pump/queued storage. This is not a
whole-process heap limit. Small reads copy only used bytes; large blocks transfer
ownership. Overflow preserves unmatched input. Exactly full remote input has one
bounded lookahead to distinguish overflow from EOF. FullBuffer explicitly consumes
the snapshot; no bytes are dropped implicitly. Generic streaming uses H=min(64 KiB,
limit) and consumes max(1,H/2), so arbitrary historical-prefix patterns are unsuitable.
Filtered interaction/Relay instead preserve potential prefixes and report overflow
when they cannot be resolved within the configured window.

## Timers and writes

Foreground Expect defaults to 30 seconds, Send to 10 seconds including admission.
NoTimeout=0 disables relative timing; negatives are invalid. Idle resets only on
accepted nonempty input. Parent cancellation wins; among local expired budgets the
earlier relative/idle deadline determines the cause. Only relative Timeout/Default
can be handled. Timeout callbacks get a fresh relative budget, bounded by idle and
parent deadlines. Budgets are checked before and after callbacks, including ignored
contexts. Continue resets relative timing; ContinueKeepTimer preserves it. Streaming
continuations preserve timers. EOF continuations terminate with io.EOF after one
callback, preventing spins. Invalid read counts remain errors even with EOF causes.

Send makes one non-retried Write. Deadline-capable transports write synchronously;
others use a joined worker. Only os.ErrNoDeadline or errors.ErrUnsupported selects
the worker fallback; every cause in a joined error must be unsupported. Other
deadline setup failures retire the Session without writing.
Before-admission cancellation preserves the Session if the deadline is restored.
Failed restoration retires it, retaining both cancellation and restoration causes.
Failure after entry closes it and returns WriteError with exact progress and causes.
Invalid counts become zero; short nil-error writes become io.ErrShortWrite. Ready
cancellation beats completion. Bytes accepted do not prove remote execution.
Cancellation that cannot wake the writer with a deadline interrupts the transport
instead. It never joins its own active write; the caller joins and flushes through
Close. Deadline errors remain inspectable with errors.Is/As. Clearing an already
closed transport is harmless: a peer's final input remains drainable. A joined
error that also contains another failure is retained and retires the Session.
Foreground deadline setter panic/Goexit propagates after lifecycle cleanup and
retires the Session. At the owned cancellation callback boundary it becomes
CallbackError without a panic payload, interrupts the write, and retains progress.

SendAll writes positive-progress suffixes while draining bounded peer input to
avoid duplex deadlock. Its parent context bounds the whole operation. SendSlow and
SendPaced hold serialization across every chunk/delay and use the parent context.
Pacer runs before each byte and returns a nonnegative duration. Cancellation/failure
or callback abort after progress retires the Session. SendNull uses <=4096-byte
chunks under one reservation; Send's write timeout bounds the complete operation.
Send/Reply are verbatim; SendLine appends configured LF/CR/CRLF. SendBreak requires
an interruptible transport capability. Poll/SendAllPoll require WithOSPolling and
never create a second transport reader; canceled requests stay pump-owned to ack.

## Groups and background operations

NewGroup borrows a validated member set without I/O. Group.Expect reserves all
initial members, releasing reservations on failed admission. Add during an active
wait acquires the new receiver before acknowledgement; Remove releases it and keeps
pending input. Controls execute outside callbacks and use contexts to bound waiting.
A Group has only one active Expect. Case.Sessions supports heterogeneous declarations;
empty selectors apply to all members. Before/call/After priority spans the group,
with round-robin ties. Continue resets active relative budgets together; each member
retains its own idle/counters. Cached candidates are invalidated by rule/buffer
changes. Return completes; failed members are identified by SessionError, and all
failures are joined if nobody remains. An empty group returns ErrNoSessions.

StartBackground reserves synchronously and defaults to NoTimeout; explicit timeout
options are a Go extension. Pause waits for callbacks, freezes remaining local
budgets and releases input. Paused background operations do not inspect foreground
buffers. Parent contexts keep running. Resume returns ErrBusy without leaving the
paused state if another owner is active. Stop cancels then joins; its context bounds
the join. Wait's context only bounds joining. Controls are not called synchronously
from their own callbacks. Return/EOF/error/cancellation ends the operation; Continue
keeps it active. Callback panic/Goexit becomes CallbackError before Wait completes.

## Interaction, Relay and diagnostics

Interact validates options/programs before admission or local I/O, then owns input;
Close must interrupt Read. It closes input once and joins its local read pump on
every exit. Output is borrowed/redacted, including pending data. Input supports
Literal, Line, All, Go regexp/Glob and EOF/Timeout/Default/FullBuffer. Local idle timing is
independent (WithInteractInputTimeout); missing timeout handlers return ErrIdleTimeout.
Literal matching advances KMP once per byte/declaration and retains overlapping
completions while higher-priority partials resolve. Regex feasibility is conservative
inside the window; final extents/captures come from Go regexp. Unmatched bytes batch
forward; local matched sequences are consumed unless input forwarding is enabled.
NoConsume input cases are rejected. Declared input cases precede control-] escape.
Local actions run on the same owner as remote actions. Normal stop is separate from
EOF or callback failure. Echo uses redacted output. Remote passthrough defaults on;
disabling it withholds matched bytes, forwarding safe unmatched prefixes only.

InteractTerminal duplicates a borrowed file, restores raw/nonblocking modes after
joining input and leaves the original open. Cooked callbacks temporarily restore
terminal modes even on panic/Goexit; direct Interact rejects that option. The caller
must not read/change the borrowed terminal concurrently. Final output flush failure
retires the Session; wrapped WriteError causes are never suppressed as cleanup EOF.

Relay routes each source once and can fan out to raw Session targets plus a redacted
operator Output. No implicit shell or connection is created. Targets/sinks are
borrowed. Self-targets/duplicates are rejected. QueueLimit bounds queued/in-flight
payload per route/target, defaulting to the source limit; transport/redaction storage
is additional. Slow targets apply backpressure. Multiple sources serialize chunks
at one target; protocol association is the caller's job. Matched bytes are withheld
unless ForwardMatches is set. NoConsume route declarations/rules are rejected.
EOF/Return drains queued output. Any failure cancels all routes, joins admitted
writes and retains RouteError progress. No delivered bytes are replayed. The source
is retired on operator-output failure; other transports retire only after admitted
write failure/cancellation or their own sink error.

Redact registers append-only literal definitions before use; matching/results remain
raw. Receive, send, interactive output and each Relay operator stream have independent
prefix filters. Overlaps/unfinished prefixes mask conservatively; Close/EOF flush.
New rules scan retained un-emitted suffixes. Sinks are never retried; failures retain
causes and retire affected Sessions. Event/TraceEvent/slog contain metadata only,
including SessionID/source/scope/rule. OpInteract identifies completed interactions.
Compile/configuration and callback errors format no expressions or panic values.

## Process adapters

Console remains a Unix PTY adapter for caller-owned exec.Cmd. Start attaches a new
session/controlling terminal (Setsid/Setctty/Ctty=0), rejecting conflicting attributes;
the caller owns cancellation/Wait. The parent's slave closes after successful Start.
Size/echo controls preserve descriptor blocking state.

spawn.Start is the owned cross-platform entry. Unix uses a PTY, group SIGHUP and a
two-second direct-child kill fallback; descendants must cooperate. Windows 10 1809+
uses UTF-8/VT ConPTY, suspended child assignment to a kill-on-close Job, then resume.
Windows closes I/O before pseudoconsole teardown to avoid undrained-output deadlocks.
Close cancels, joins and retains status. Wait returns (ExitStatus,error); ExitError
reports nonzero exit/signal without command text. Consume output before Wait or
Close to interrupt it. Environment nil inherits; an explicit empty slice replaces.
Dir empty inherits. Terminal supports resize; Unix also implements EchoController.
Windows rejects raw-slave configuration and enforces its own size limit. Unsupported
platforms return spawn.ErrUnsupported; FreeBSD has compile/vet coverage only.
EOF never replaces checking the process exit result.
