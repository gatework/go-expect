# Post-release self-audit

Baseline: published `v0.1.0`, commit `2f69597`. This follow-up is local work on
`fix/session-release-audit`; it does not change the published tag or release.
The review covered transport ownership, cancellation and shutdown, matching and
response consumption, diagnostics/redaction, PTY/test adapters, CI and publication.

## Reproduced defects and corrections

1. **Write completion could hide cancellation.** A transport that canceled the
   context before returning all written bytes occasionally produced a successful
   Send and left the Session open. Both select cases can be ready, and selection
   is random. Send now rechecks cancellation and Close after taking a write result,
   preserves progress/transport causes, and closes failed sessions. A regression
   failed under repeated race runs before the fix. A corresponding external-Close
   regression exercises completion while Close is already observable.
2. **Rejected waits omitted retained input from diagnostics.** A pre-canceled
   context or invalid option returned empty Before even when pending contained
   input from a previous match. Failures now snapshot pending after acquiring the
   exclusive wait. ErrBusy cannot inspect another wait's state and still returns
   no input. Both failing cases preserve their tail for the next successful wait.
3. **The archive verifier trusted its own label.** Replacing a source file while
   retaining the Git archive commit header and recomputing SHA256SUMS passed the
   old verifier. The verifier now compares every tracked path, blob content,
   executable mode and symlink target against the local Git tree for the tested
   SHA. It rejects missing/duplicate/extra paths and incompatible entry types.
4. **Archive permissions needed stricter validation.** Independent review showed
   that source mode 000, directory mode 000 and directory mode 04755 were accepted
   by the first tree-verification implementation. Explicit compatible mode sets
   now reject these. New packages use tar.umask=022, and verification still accepts
   the ordinary Git default modes in the existing v0.1.0 archive.

All reproduced defects were demonstrated with failing regressions before their
fixes. No response is retried, no default rule guesses a device answer, and the
receive/matching ownership model remains unchanged.

## Fragmented receive allocation

The pump previously allocated a complete 16 KiB block for every Read, including
one-byte reads and zero-progress retries. It now keeps a private scratch block for
reads smaller than 8 KiB, transfers a copy of only the received bytes, and reuses
scratch. Larger reads transfer the entire block and allocate fresh scratch.
Queued bytes, staged tails and Result snapshots never alias reusable scratch.

Measurement: Apple M5/macOS, Go 1.27.1, 16 KiB of input plus `END!`, including
Session construction and Close. The baseline was extracted from 2f69597 with only
the identical new benchmark added. Baseline and candidate ran serially, with
`-cpu=1 -benchtime=300ms -count=10`; benchstat compared all ten samples.

| Read size | Before median | After median | Before allocation | After allocation |
| --- | ---: | ---: | ---: | ---: |
| 1 byte | 17.983 ms | 5.293 ms | 262,324 KiB | 132.0 KiB |
| 32 bytes | 553.5 us | 180.3 us | 8,323.9 KiB | 131.9 KiB |
| 1,024 bytes | 26.04 us | 14.10 us | 378.7 KiB | 122.8 KiB |
| 16,384 bytes | 6.279 us | 5.959 us | 86.50 KiB | 70.50 KiB |

For one-byte reads this reduces allocated bytes by 99.95% and elapsed time by
70.57%; allocation count is essentially unchanged. This synthetic test does not
measure network latency or real-device throughput. Full CI benchmark smokes are
not used for these performance comparisons.

## Completion evidence

| Requirement | Evidence |
| --- | --- |
| Preserve cancellation/Close and exact write progress | Repeated race regressions; existing partial-write, timeout and serialized-send tests |
| Keep receive ownership and detached results | Binary transcript/result reconstruction at 1/32/8191/8192/16384-byte reads; Session continuation fuzz |
| Preserve unmatched input and matching behavior | New pre-cancel/invalid-option regressions; full matcher/Responder/EOF/deadline suite |
| Preserve redaction and local device dialogue | Redactor fuzz, actual local PTY shell dialogue, example and independent consumer |
| Verify actual release source | 23 Python tests including recomputed manifests, altered payload, inventory, modes and symlink fixtures |
| Remain compatible with published artifact | New verifier accepted downloaded v0.1.0 assets against its exact Git tree |
| Independently review corrections | Read-only reviewer reproduced permission failures, then passed all 23 release tests and accepted the fixes |

Final gates passed: complete `script/ci` on macOS Go 1.27.1 and Linux Go 1.27.2;
full Go 1.26.0 race suites on both systems; 23 release tests on macOS Python
3.9/3.14 and Linux; golangci-lint, govulncheck reachability scan, actionlint and
shell/diff checks. The current worktree was copied into a disposable Git repository
for canonical package verification and extracted consumer tests on both systems.
Original checkout HEAD and the published release were unchanged.

The initial Linux Go 1.26 toolchain download was slow (about 20 KiB/s in a range
probe). After confirming ongoing download activity, that temporary test container
was stopped and the same gate was rerun with GOPROXY=https://goproxy.cn,direct.
Go checksum verification remained enabled; no dependency or CI setting changed.

The source verifier checks Git content, paths, types and compatible permissions;
it is not a general validator for arbitrary tar extended metadata. Publication
continues to trust the same-run CI artifact and its immutable artifact ID. No new
remote CI, release, real device or network-interoperability evidence is claimed
for this uncommitted follow-up.
