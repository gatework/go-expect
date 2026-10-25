# Verification record

Local acceptance was performed on `feat/session-responder`, based on upstream
commit `73e0943`, before publication. The records below describe that checkpoint;
remote runs and published versions are available in the repository's
[Actions](https://github.com/gatework/go-expect/actions) and
[Releases](https://github.com/gatework/go-expect/releases). No real network device
was used for local acceptance.

## Acceptance

- macOS arm64, Go 1.27.1: `bash script/ci` passed (module verification, vet,
  race + goleak, both fuzz smokes, PTY benchmark, dialogue example and independent
  consumer module exercising stream + PTY).
- Linux arm64 container, Go 1.27.2: the same complete gate passed.
- Go 1.26.0 on macOS and Linux arm64: full race suites passed.
- Independent review identified relative/idle expiry after a callback and exact
  buffer-limit split EOF; both now have regressions that failed before their fixes.
- Observer notification for rejected Responder calls was also fixed with a
  failing event-count regression.
- `actionlint`, `bash -n` and `git diff --check` passed.
- The macOS Go 1.26 descriptor-leak test initially failed in `/dev/fd` metadata
  enumeration. Counting names while holding the directory open fixed the test
  portability issue; the full Go 1.26/1.27 macOS race suites then passed.

Tests use real local PTYs, a shell child, net.Pipe, deterministic synthetic time,
and controlled short-write/error readers. There is no real SSH/device, weak-link
network or remote CI evidence in these results. CI is configured to repeat the
checks on Go 1.26/1.27, Linux/macOS after a future authorized push.

## Same-machine benchmark comparison

Workload: create a canonical Console, write 16 KiB of `x` followed by `done!`,
match that literal and close. Both versions include PTY setup and use three
measured iterations on the same macOS arm64 machine. Timing varies; no hard
performance threshold is imposed.

| Source | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Original 73e0943 implementation | 14,264,458 | 142,387,789 | 49,192 |
| New Session implementation, final gate | 487,458 | 420,112 | 60 |

This narrow workload demonstrates removal of rune-by-rune full-buffer copying.
It does not establish throughput or latency on remote network devices, nor a
speedup for every regular expression or output sink.

## Self-audit and evolution

The follow-up audit reproduced four defects before applying fixes:

- A typed-nil transport was accepted; transport and output/transcript validation
  now rejects it before ownership transfer or I/O.
- Panic or `runtime.Goexit` from a handler bypassed derived-context cancellation;
  cleanup now runs with defer, preserving the unread tail and releasing the wait.
- Responder failure events exposed an internal rule index instead of the public
  result index. Notification now happens after result normalization.
- In-flight cancellation bypassed validation of a transport's impossible write
  count. All completion paths now validate progress, including concurrent Close,
  and cancellation retains the original transport cause.

The new regressions passed 30 repetitions under the race detector. The complete
`script/ci` passed again on macOS Go 1.27.1 and Linux Go 1.27.2, now including
`FuzzSessionContinuations` alongside the matcher and redactor fuzz targets. The
new target verifies byte conservation, detached results, transcript delivery and
bounded-buffer continuation across arbitrary chunk boundaries. Go 1.26.0 full
race suites also passed on macOS and Linux. `actionlint`, shell syntax checks and
`git diff --check` passed. An independent read-only review found no
Critical, Important or Minor issues and independently passed the full race suite.

Continuation consumption now advances the owned slice instead of copying the
unread tail after every match. Empty matching buffers adopt capped receive slices,
so later appends cannot overwrite staged bytes. Same-machine measurements used
`BenchmarkContinuedBurst`, `-benchtime=200ms -count=3`, including session setup:

| Matches per burst | Before median ns/op | After median ns/op | Before B/op | After B/op |
| --- | ---: | ---: | ---: | ---: |
| 128 | 12,919 | 13,451 | 17,808 | 17,680 |
| 4,096 | 372,186 | 309,798 | 21,776 | 17,680 |

The larger burst improved by about 17% in elapsed time and 19% in allocated bytes;
the smaller burst showed no timing improvement. Both dropped from 15 to 14
allocations. This is a local synthetic continuation workload, not device evidence.

## Automatic release workflow validation

- Added default-branch automatic patch releases, starting at `v0.1.0`, after
  the complete OS/Go matrix, quality checks and extracted-package consumer pass.
- Nineteen release tests passed on Python 3.14 and macOS system Python 3.9. They
  cover real temporary Git archives, checksums, revision identity, version order,
  reruns, conflicting/moving tags, partial draft recovery and immutable assets.
- The independent reviewer reproduced three release blockers: repository API
  trailing-slash 404, conflicting draft/tag identity and unfinished starter uploads.
  Regressions failed before fixes; the follow-up review approved all three fixes.
  A further regression prevents an existing prerelease draft from being published
  by the stable release path.
- Final review independently passed all 19 release tests with no remaining
  findings, including late older tags/drafts that must not replace a newer release.
- The updated complete `script/ci`, including tidy verification and shuffled race
  tests, passed on macOS Go 1.27.1 and Linux Go 1.27.2.
- Pinned golangci-lint 2.15.0 reported no issues; govulncheck 1.9.0 reported no
  reachable vulnerabilities. This is a reachability result, not a claim that every
  transitive module has no vulnerability record.
- A disposable copy of the current working tree was committed locally for package
  verification; the source archive and its extracted stream/PTY consumer passed.
  The original checkout was not committed or switched.
- A read-only GitHub release plan using baseline commit `73e0943` selected
  `v0.1.0`. This exercised the remote repository/releases lookup and ancestry
  checks; it did not publish the current uncommitted implementation.
- actionlint 1.7.12 passed with one exact diagnostic exception for GitHub's newer
  `concurrency.queue` key. All other workflow checks remain enabled.

At the local acceptance checkpoint, GitHub-hosted execution, draft mutation and
public release/download had not been verified. See the linked remote runs for
publication evidence, and releasing.md for operation and the GitHub token
permission limitation.
