# Changelog

## v0.4.2

- Track agent guidance and ownership documentation with the source tree.
- Enforce generic stream, process and redaction boundaries from the resolved
  production import graph, including indirect reverse dependencies.
- Run architecture checks and positive/negative controls in the local and CI
  gates. The stream runtime and public Go API are unchanged.

## v0.4.1

- Retire transports when setting, updating or clearing a write deadline fails;
  retain accepted byte counts and all primary/cleanup errors without replay.
- Separate interruption from worker joining so cancellation callbacks can retire
  blocked writes without deadlock. Abnormal deadline callbacks preserve safe errors.
- Keep lifecycle, the sole input pump, writes, options and diagnostic redaction
  with their state owners. The public Go API remains unchanged.
- Add deterministic deadline/cancellation regressions and document the downstream
  network-automation ownership and migration contracts.

## v0.4.0

- BREAKING: replace ExpectAny with mutable Group/Case.Sessions, WithNoConsume
  with Case.NoConsume, and ResponseError with CaseLimitError. No aliases remain.
- BREAKING: background waits default to NoTimeout, support Pause/Resume, and
  require contexts for Stop/Wait. Abnormal asynchronous callbacks return CallbackError.
- BREAKING: unify Unix PTY and Windows ConPTY behind spawn.Start; remove the
  conpty package and WithConsoleOptions. Wait returns (ExitStatus, error).
- Add atomic persistent-rule replacement and stable SessionID metadata.
- Add local Go regexp/Glob and input EOF/idle actions, forwarding/echo controls,
  cooked terminal callbacks, filtered remote forwarding and bounded Relay routes.
- Preserve local callback EOF failures, release send lifecycle references on
  sink panic, enforce earliest deadlines, and prevent paused background state reads.
- Replace root-rescanned local tries with incremental KMP completion queues;
  keep steady-state match/send allocation targets and bounded input pump storage.
- Add SendPaced and chunked SendNull, common child environment/directory/exit
  status, smaller private receive modules and direct migration documentation.

## v0.3.0

- BREAKING: replace WithEscapeAction and WithInteractCases with
  WithInteractInputCases (byte-exact Literal sequences) and
  WithInteractOutputCases (remote patterns). No aliases remain.
- Keep every ExpectAny session active across continuations, rotate equal-priority
  sessions, and retain per-session idle budgets and callback limits.
- Validate interaction options before ownership, I/O or terminal changes; preserve
  admitted WriteError progress, release locks on writer panic, and retire sessions
  when the final redacted output flush fails. Adapt streaming to small buffers.
- Execute local actions on the receive owner; share sequence prefixes, batch
  ordinary input, and preserve chunk-independent action ordering.
- Enforce callback/idle budgets after timeout handlers; validate original Fold
  declarations; support reversed glob ranges and folded range endpoints.
- Add source/scope/rule metadata and virtual-event traces; CompileError unwraps
  ErrInvalidPattern without exposing the expression.
- Remove unused coordinator preview state and avoid default-option heap escapes:
  reused END matching uses 1 allocation for Literal and 2 for captureless regexp.


## v0.2.0

- BREAKING: unify Case/rules; remove Rule, Responder and NewResponder. Use
  WithRules, WithBefore/WithAfter and Case{Handler, Limit} directly.
- BREAKING: Return is zero; replace ContinueWithoutReset with ContinueKeepTimer,
  WithDeclarationPriority with WithMatchOrder, and log.Logger with slog.Logger.
  No compatibility aliases or transition wrappers remain.
- Add controlling-terminal Console.Start, owned spawn and Windows ConPTY adapters;
  terminal size/echo controls, local terminal interaction and runtime Windows CI.
- Add Timeout/Default/FullBuffer cases, Glob/Fold, no-consume preview, capture names
  and byte offsets, background/multi-session waits, and opt-in input normalization.
- Add slow/NUL/break sending, metadata-only candidate tracing and redacted send logs.
  Redact retains uncompiled definitions for future interaction even without sinks.
- Split the receive waiter, group Session state, share internal redaction mechanics,
  and use synchronous deadline-capable sends with precise cancellation/progress.
- Add typed diagnostics/configuration errors, safe regexp syntax categories,
  compiled README examples, subject-based test organization and broader lint gates.
- Compile-check the Unix adapter on FreeBSD; Linux/macOS remain the Unix runtime
  matrix. Archive raw historical documentation outside the main tree.
- Breaking commit markers select minor versions before v1; add SPDX headers and
  remove the obsolete Netflix-internal OSSMETADATA marker.

## v0.1.4

- Flatten per-wait All cursors, retain mixed literal progress and share immutable Line prefix tables.
- Allocate stateful cursors and handler scopes only when used; share per-call Responder counters and callbacks.
- Skip secret compilation without output sinks and release private rules when closing a Session.
- Preserve underlying invalid-read-count causes without treating them as a clean EOF.

## v0.1.3

- BREAKING: remove WithTranscript; use WithOutput for the filtered receive stream.
- Add byte-exact Line patterns, IsZero and ValidateText preflight checks, and shared long-literal prefix tables.
- Check worker admission before writes; pre-admission cancellation leaves the session usable.
- Release retired receive buffers; calls after Close no longer expose historical input.
- Match final lines incrementally and validate nested patterns in one traversal.
- Reuse detached strings for identical capture ranges and avoid unused wait scopes/timers.
- Isolate PTY benchmarks and add Windows test cross-compilation and long-literal fuzz coverage.

## v0.1.1

- BREAKING: rename Console.Tty to Console.TTY; update callers as described in
  docs/migration.md. That release used the former automatic patch policy.
- Add opt-in Poll, SendAll, declaration priority, bounded streaming fallback and
  standalone scoped redaction; default matching and single-write sends remain.
- Preserve compiled regexp semantics through FromRegexp and cancel active handler
  contexts on Close without waiting for user callbacks.
- Retain cleanup errors and exact progress on automatic retirement; make repeated
  positive partial writes use one send buffer rather than copying every suffix.
- Keep go-expect focused on generic streams and PTYs. The unreleased duplicate
  device package is removed; device-layer migration belongs to go-net-connector.
- Preserve cancellation/Close when write completion races with stopping, include
  pending input in rejected waits, and reduce fragmented receive allocations.
- Verify source archive files and permissions against the tested Git tree.

## v0.1.0

- Move module path to `github.com/gatework/go-expect`, requiring Go 1.26.
- Add owned duplex Session with bounded byte buffering, cancellation, serialized
  writes with exact failure progress, and deterministic worker shutdown.
- Add immutable patterns, detached results, Continue/ContinueWithoutReset,
  relative/idle/context deadlines, and reusable per-call Responder rules.
- Add explicit overlapping streaming secret redaction and metadata-only observers.
- Rework Console as a Linux/macOS PTY adapter with pollable I/O and optional raw mode.
- Replace legacy readers, passthrough pipes and unmanaged test logging goroutines.
- Add device-dialogue tests, fuzz targets, migration guidance, independent consumer
  smoke and Linux/macOS CI for Go 1.26/1.27.
- Harden typed-nil validation, callback cleanup and write-progress validation;
  align response events with public results and avoid repeated unread-tail copies.
- Automatically publish default-branch snapshots after the complete CI gate,
  reusing verified source artifacts with patch versioning and resumable drafts.

This release changes public APIs and timeout semantics; see docs/migration.md.
