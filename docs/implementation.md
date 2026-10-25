# Session and responder implementation

Approved scope: evolve the module to github.com/gatework/go-expect; a generic owned duplex Session, PTY Console adapter, typed matching/continuation, reusable per-call Responder, bounded I/O and explicit streaming secret redaction. No SSH connection establishment, process ownership, terminal emulator, commit, push or release.

## Contract

Session owns an io.ReadWriteCloser after successful construction. Close must interrupt its reads and writes. A single receive pump feeds a bounded queue. Expect has exclusive ownership of the match buffer; concurrent/nested Expect returns ErrBusy. Timeout/cancellation preserves unread data. Writes serialize; a failed in-flight write closes the session and reports exact progress without retries. Defaults: 30s Expect, 10s Send, 8 MiB buffer, LF line ending. Zero timeout disables the corresponding relative timer. Context bounds operations; user callbacks/sinks cooperate.

Matching chooses the earliest visible byte position, declaration order breaks ties. Consume only through the match, preserve the tail. Continue resets relative timeout; ContinueWithoutReset preserves it. Responder completion wins same-position ties; responses never reset the timer. Rule Limit=0 means one, positive values bound responses per call. Reject empty/zero-width matches before effects. Raw results are independent snapshots; transcript is receive-only and filtered. Secrets are registered before exposure and never removed.

## Tasks

1. Baseline regression and benchmark evidence.
2. Owned session I/O and PTY adapter.
3. Typed matching, continuation and responder.
4. Streaming redaction, documentation, examples, migration and verification.

## Execution record

- Baseline: clean master at 73e0943. Existing race suite passed during planning; vet found unreachable code in passthrough_pipe.go.
- Ruling: implement on an in-place feature branch, preserving the user-visible workspace; no unrelated changes exist and no commits are authorized.
- Pre-flight: Session owns I/O, matcher owns consumption, Responder reuses matcher; output redaction never changes matching input.
- Ruling: interpret WithTimeout(0) as disabled, avoiding consumption of a buffered prompt followed by an already-expired callback context.
- Task 1: RED reproduced: observer calls 1 rather than 2; ff byte rewritten as efbfbd; ReaderLease EOF waits for context expiry. Baseline PTY 16KiB literal (3 iterations): 14.264 ms/op, 142387789 B/op, 49192 allocs/op. Machine-dependent evidence, not a hard performance gate.
- Tasks 2/3: owned stream, PTY adapter, typed matching and Responder implemented. Session/matcher/PTY/race suites pass. Local shell device covers login, two commands, repeated confirmation, paging and capture replies.
- Ruling: maintain synchronous borrowed output sinks (which must not reenter Session); I/O cancellation does not forcibly terminate user callbacks or sinks.
- New regressions found and fixed: a handler canceling its parent could return success; rejected concurrent waits skipped observers. Both tests failed before fixes and now pass.
- Task 4: streaming redaction, fuzz targets, expecttest cleanup, migration and CI added. Initial fuzz smokes passed (literal 942642 cases, redactor 113431 cases).
- Review focus: cancel-vs-send races and exact write progress; EOF with buffered bytes; ambiguous regex/UTF-8 boundaries; redaction overlap and pending suffix finalization; Close from callbacks; per-call response counts; source and independent consumer behavior. Changes are intentionally uncommitted, so review the working tree including untracked files against 73e0943.

- Final review fixes in progress: TestExpectReturnAfterRelativeDeadline RED -> enforce deadline before Return and before matched-input consumption. TestSessionExactBufferLimitWithSplitEOF RED -> bounded lookahead distinguishes split EOF from true buffer overflow.
- Ruling: a buffer exactly at its limit may wait for one bounded next event to distinguish EOF from overflow, under normal deadlines; no bytes are silently discarded.
- Added goleak TestMain and verified original error propagation, no-progress Reader and Close error retention.
- Final independent review: no critical findings; two important deadline/EOF findings fixed with failing regressions followed by green race suite. No declined-to-judge items.
- Ruling: fix the reviewer-classified minor missing Responder rejection event as a public observer-contract defect; TestResponderRejectedWaitNotifiesObservers demonstrated 0 instead of 1 event before the fix. No known review findings deferred.
- Tasks 1–4: complete. Final full gates passed on macOS Go1.27.1 and Linux Go1.27.2; minimum Go1.26.0 race suites passed on both platforms. See verification.md for evidence boundaries and benchmark comparison.
- Test portability correction: use directory Readdirnames instead of os.ReadDir/lstat when counting /dev/fd; Go1.26 macOS full race suite passed after this fix. Product code unchanged by this correction.
- All independent review findings addressed; no deferred findings. No commit, push or release performed.

## Follow-up self-audit

- Re-read lifecycle, matching, response, redaction and adapter boundaries. Four
  focused regressions failed before fixes: typed-nil transport acceptance,
  handler panic/Goexit leaking a derived context, Responder event index mismatch,
  and invalid write progress escaping cancellation validation.
- Fixed validation before ownership transfer, deferred callback-context cleanup,
  public result notification and worker-side write progress normalization.
  Cancellation preserves both context and transport causes.
- Removed repeated unread-tail copies during continued matches and adopted
  bounded receive slices when pending is empty. A whole-session continuation
  fuzz target checks chunking, buffer limits, snapshots and transcript conservation.
- No new public orchestration layer or implicit response rules were added.
- Independent review reported no findings or declined-to-judge behaviors; its
  full race suite passed. See verification.md for measured performance and gates.
