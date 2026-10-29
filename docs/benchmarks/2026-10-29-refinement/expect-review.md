# Independent expect refinement review

Compliance verdict: PASS for the reviewed implementation after the mixed-rule redaction correction and final allocation delta. Quality verdict: APPROVE at source-review level. No remaining code blockers were identified. Full task acceptance belongs to the root/implementer gates; the final delta review below records the additional source and saved-artifact checks without claiming unreviewed gates passed.

The comparison was against `/tmp/connector-refinement-20261029/expect-baseline`, the round-one WIP baseline, using `/tmp/connector-refinement-20261029/expect-review.diff` and the current source under `/Users/crate/Works/go/.worktrees/connector-unification/go-expect`. The initial diff was then reviewed together with the implementer's follow-up short-rule scan correction. No shared production or test files were edited by the reviewer. No benchmarks or heavy tests were run. The only executed probe copied the redactor into a temporary standalone program and counted search invocations; it did not measure time.

## Resolved blocker: short rules rescanned the long retained suffix

The initial candidate restarted each short rule's `bytes.Index` loop at offset zero on every Append. A long rule retains up to its length minus one bytes, so rules consisting of `a` repeated L times plus the one-byte rule `a`, followed by one-byte chunks, rediscovered about L old short-rule matches per input byte. The new covered-interval state avoided repeated marking but did not avoid repeated searching. This left O(N*L) work in a mixed-rule dense stream and failed the intended long-rule scaling requirement.

An independent copied-source probe inserted only a counter immediately before the short-rule `bytes.Index` call. After warming with 2*L bytes, it appended 256 one-byte chunks:

| Long-rule length L | Initial candidate search calls | Corrected candidate search calls |
| --- | ---: | ---: |
| 257 | 65,792 | 256 |
| 1,024 | 262,144 | 256 |
| 4,096 | 1,048,576 | 256 |
| 16,384 | 4,194,304 | 256 |

The implementer changed the start to `max(0, prior-len(secret)+1)` in `streaming_redactor.go:144-155`. Existing hidden flags already record old matches, so only candidates overlapping the new boundary or beginning in new input need searching. The corrected probe is independent of L, and the source reasoning also covers short matches crossing the boundary. The implementer added `TestPublicRedactorShortRulesAcrossLongRetainedSuffix` and a mixed-rule dense-byte benchmark. Those additions were read; execution of the full suite and benchmark remains with the designated gates.

Probe files are in `/tmp/connector-refinement-20261029/expect-review-probe`. `streaming_redactor_red.go.txt` preserves the initial instrumented source; `streaming_redactor.go` contains the corrected instrumented copy, and `main.go` supplies the workload. Both executions used `GOTOOLCHAIN=go1.27.3 GOWORK=off GOFLAGS=-mod=readonly go run` on the two standalone Go files and exited successfully.

## Runtime polling and lifecycle review

- `polling_unix.go:21-46` selects the runtime backend only when construction-time read-deadline capability succeeds. Backend choice is stable afterward. Transports without working read deadlines retain the existing native-poll fallback.
- `readWait` uses `RawConn.Read`; the callback returns false on EAGAIN/no readiness. The cold-scratch readiness check is zero-time and occurs inside the runtime callback. There is one owning pump, no per-request goroutine, no added wake descriptor, and no descriptor retained outside its RawConn callback.
- `requestPoll` publishes its single request and applies the deadline while holding `pollMu`. The pump clears the wake under the same mutex before its immediate pass. A prior runtime read is published before processing the new request. The explicit pass publishes its event before `finishPoll` closes the acknowledgment. This preserves the required ordering for prior input and the readiness boundary.
- A caller leaving through cancellation, a match, or timeout does not erase pump ownership. A later Poll borrowing the outstanding acknowledgment uses `pollOwned=false`, then requests its own boundary. `stopPoll` marks pump termination and acknowledges outstanding work, preventing new unowned requests after exit.
- Close interrupts the owned transport, permits publication to leave through `done`, joins the pump, and does not clear receive state or wait for cooperative handlers. The poll mutex is not held across RawConn.Read or event-channel publication. Scratch buffers remain pump-owned; small events copy their bytes and large events transfer ownership.
- Read normalization is capability-based. It is applied only in `readReady`, after the syscall/EOF conversion. RawConn and readiness/control failures bypass normalization. The PTY adapter translates EIO; ordinary transports retain EIO. Deadline forwarding was added to the owning PTY adapter. Connector-side process/wrapper forwarding is outside this source review and is assigned to root.
- Read the new tests for runtime wait without periodic wake, canceled-request ownership, publication before acknowledgment, construction fallback, pump exit, and Close acknowledgment, plus existing real-PTY, scratch-ownership, and closed-RawConn tests. No lifecycle blocker was identified by tracing the reachable branches. Runtime CPU/thread/FD behavior still requires the planned measurement evidence.

## Receive storage review

- `expect.go:38-42` arranges release before clearing the exclusive receive flag, so cleanup never races with admission of another receive operation. Close does not mutate pending/staged storage.
- Exhausted pending/staged slices are nilled. `pendingAlloc` and `stagedAlloc` retain the original backing-size information after prefix slicing. Allocation growth resets the tracked size, and the staged-to-pending capacity cap prevents a later append from overwriting still-staged bytes.
- At operation return, the helper detaches only a nonempty remainder from an allocation of at least 64 KiB when that remainder is no more than one quarter of the allocation. Continuations only slice; they do not copy the retained burst each time.
- Timeout/cancellation/error returns retain unmatched bytes. Returned Before/Match/capture strings use owned conversions, so detachment or a later receive does not mutate returned evidence. The two new storage tests cover exhausted storage, a small retained tail, detached result bytes, and cancellation after a continuation. The planned held/discarded-result memory samples remain external validation.

## Redaction correctness and complexity review

- Rules longer than 256 bytes compile a KMP failure table once per rule scope and carry matched-prefix state only over new data. Complete-match fallback preserves overlapping matches. On Finish, the longest retained proper prefix contains every shorter candidate prefix, so marking that interval preserves conservative masking.
- Ordered matches have monotonically advancing ends. The per-rule covered frontier marks only newly covered bytes; retained hidden flags preserve earlier coverage. Emission's masking state coalesces overlapping and adjacent unions across chunk boundaries. The corrected short-rule loop keeps bytes.Index and examines at most the fixed short-rule overlap plus new data.
- Grow/compact retains up to twice the maximum suffix window and amortizes tiny-chunk movement. Oversized transient storage is detached after the append. Finish releases stream buffers and resets match/masking state. SetSecrets first finishes the old scope, then compiles the new scope; invalid rules are rejected before mutation.
- Rules and replacement bytes are copied, no-rule output is cloned, and both general and safe paths allocate detached output. Binary matching is byte-based. No new unsafe code, global rule cache, or pool was introduced.
- Read the independent whole-input differential oracle, binary/long-rule fuzz seeds, SetSecrets split-scope checks, dense overlap tests, retained-window/ownership tests, and new mixed-rule case. No redaction correctness blocker remains. The operation-count probe establishes removal of the identified size multiplier; it is not a substitute for the required serial timing samples and regression threshold.

## Scope and evidence boundaries

The Go module still declares Go 1.26.0; inspected production changes do not require newer language/library APIs. The reviewed go.mod contains no local replace. The reviewer made no commits, pushes, release actions, or changes outside temporary review artifacts. Existing WIP was preserved. Generic stream/matching/redaction responsibility remains in expect; device/vendor/auth/save responsibility remains outside it.

The root-owned final gates must establish the requested Go 1.26/1.27 Darwin/Linux race/vet, lint/fuzz/worker/PG and source-contract results, patched-toolchain vulnerability scan, serial benchmark samples, idle-session resource/latency measurements, and held/discarded-result memory evidence. This report makes no published dependency-adoption, remote-CI, release, or real-device claim.

Reviewed production file SHA-256 values after the mixed-rule correction, before the later allocation delta (the final redactor hash is recorded below):

```text
37efcaf456126d867861faceb6bdc06670bf4db025ef3731a9d6bf9581e19917  console.go
41b4f3de3606b88a26ab11b02cfbc3c7c7a3ea5a27c9e47ac2a681452a7f3195  expect.go
a60c50b83a89d7268e99f5aa3353238c7ad88b046c432137147ab9ccb61030fa  polling_other.go
6b36912a67bc3f8bfac8304b5d473562b6f581d592f5a3b53557d3bfe100c0dc  polling_unix.go
82fc64a207503978ffb0e214da0247b925f7e048d329e8d670fb48f241e555b9  session.go
6813b9c91abfeeb5f20bc256c3e7b2334922e4da96f7435460e33f97fb9efcfb  stream.go
38f35b12dae8f7c6255480ccc123a18670b0ed0915cc9ad92feddd470b75fbde  streaming_redactor.go
```


## Final allocation and measurement delta review

Final delta verdict: APPROVE. No new code or measurement-method blocker was identified. This pass compared `/tmp/connector-refinement-20261029/streaming_redactor-initial-candidate.go.txt` with current `streaming_redactor.go`, read the changed idle-population harness and rerun scripts, and read the saved rerun artifacts. It performed source/artifact reads only: no test, build, benchmark, or new operation-count execution during the exclusive compute window.

The production delta consists of two capacity decisions. Oversized transient storage now detaches with capacity equal to the retained suffix length, and a cold safe append allocates only its current tail. Both retain the same bytes and hidden flags; neither touches rule compilation, KMP progress, covered intervals, prefix masking, or output ownership. `detach` allocates independent byte and mark arrays with matching lengths. A later `grow` still reserves at least twice `keep` when additional capacity is required, so one-byte continued general appends regain the previous amortized compaction behavior after the first necessary growth. Empty streams, zero retained tail, Finish, and SetSecrets retain their existing reset paths. The earlier independent search-count evidence remains applicable to the unchanged search loop, but was not rerun against this capacity-only delta.

The idle heap correction is valid for live Go heap comparison. FD/thread probes now run before the first GC and baseline `ReadMemStats`; the second snapshot is taken after session creation, settling, and a second GC, before the next FD/thread probes. This removes the specific bias where temporary baseline probe buffers were counted before creation and reclaimed before the second snapshot. The sessions and peers stay live through use and deferred cleanup. `BenchmarkOSIdleHeap` shares the same measurement path and exits only after heap, FD, thread, and idle CPU observations, skipping the later Poll/Close latency collection. That skip cannot alter the earlier heap snapshot. The original latency artifacts therefore remain separate from the corrected heap evidence.

Baseline and candidate copies of `polling_unix_bench_test.go` have identical SHA-256 values. The rerun script uses a fresh process for each population and variant, ten samples per case, and alternating baseline/candidate order; it does not overlap measurements. The progress artifact records all rerun invocations exiting zero and completion. The final redactor benchstat artifact contains ten samples per row, no significant time regression greater than 5%, and no allocation-size regression greater than 5%. In particular, dense one-shot B/op is now effectively equal to baseline while the mixed-rule hot suffix remains approximately constant across long-rule sizes. These are saved artifact observations, not reviewer reruns.

The corrected heap benchstat artifact reports positive live-heap differences for all four populations. Candidate medians at 64/256/1024 sessions are approximately 1.94 KiB per session, with two FDs per session and a fixed observed thread count of seven. The single-session retained-heap comparison has visibly higher variation (candidate ±30%, baseline ±23%) and should not be presented as exact object size. `retained-B/session` is the post-GC difference in live Go heap divided by population; it includes peer/harness/runtime heap overhead and does not include all process RSS or OS-thread/stack memory. The idle benchmark's default ns/op is a fixed sleep observation interval, not a throughput or latency score. The heap-only variant does not supply replacement Poll/Close latency measurements. Original raw samples should remain alongside corrected samples, with the original heap numbers explicitly superseded.

Saved artifacts read in this pass:

- `expect-redactor-final-benchstat.txt` and the rerun progress log.
- `expect-idle-heap-final-benchstat.txt`.
- `expect-redactor-allocation-fix.log` (records a successful targeted test invocation; this review did not rerun it).
- `run-expect-acceptance-rerun.py` and `run-expect-benchmarks.py`.

Final reviewed hashes:

```text
aa9f51fbb5771d8de810fc6ad61dd59b80d517c231bfc3369bb326b86f8ab278  streaming_redactor.go
f0756d4f6d300c691d38a9e7dcd44e7ecfa8d008ae1808bbc09e857f16819847  polling_unix_bench_test.go
f0756d4f6d300c691d38a9e7dcd44e7ecfa8d008ae1808bbc09e857f16819847  expect-benchmark-baseline/polling_unix_bench_test.go
```
