# Second foundation refinement, 2026-10-29

The runtime reader, receive storage, and public redactor refinement passes the local correctness gates and the performance acceptance threshold. The comparison uses the complete round-one WIP snapshot, not Git HEAD. No commit, push, publication, dependency replacement, or go.work was created. Previous-round evidence remains unchanged.

## Result and scope

An OS transport with working `SetReadDeadline(time.Time) error` now waits in the Go runtime poller through one `RawConn.Read` pump. Transports without that capability retain the construction-selected native-poll fallback. Explicit Poll requests have one mutex-protected publication/deadline boundary; cancellation leaves the request owned by the pump until acknowledgment. The pump publishes earlier bytes, clears the wake deadline, reads immediate readiness, publishes those bytes, and acknowledges. Close interrupts and joins the pump. There is no wake FD, periodic runtime polling, or escaped descriptor.

Structural `NormalizeReadError(error) error` handles wrapped transport read errors. The owned PTY forwards deadlines and normalizes PTY EIO to EOF; control errors and ordinary transport EIO retain their meaning. Connector-side wrapper forwarding and source-pair integration are verified by the connector workstream, not inferred from these expect results.

Exhausted pending/staged buffers release their references. Original allocation size remains tracked after slicing. At operation return, a remainder detaches if the original allocation is at least 64KiB and the remainder is at most one quarter of it. Continuations retain their existing cheap slice advancement, cancellation keeps remaining input, returned Results are detached, and concurrent Close does not clear matching state.

Public Redactor rules longer than 256 bytes compile KMP tables once per rule scope and scan only new bytes. Short rules keep bytes.Index but search only new candidates and the short boundary overlap. Per-rule covered interval ends avoid repeatedly marking dense overlaps. A bounded backing buffer amortizes tiny-chunk movement. Large transient and cold safe suffixes detach at exact size; spare capacity is allocated only when a later append needs it. Finish, SetSecrets, binary input, overlapping/adjacent replacement union, and output ownership retain their contracts.

## Measurement method

All samples used Go1.27.3, Darwin arm64, Apple M5, GOMAXPROCS=4. Baseline and candidate used identical benchmark sources, serial execution, ten samples per variant, and alternating order between samples. Every idle population ran in a fresh process because Go may retain OS threads from a previous population. Ordinary timings used 100ms benchmark intervals. Idle measurements used three fixed 50ms observation intervals, with separate sequential per-session Poll/Close latency measurement. Retention measurements hold live sessions through GC; their `retained-B/session` metric is meaningful, while their keepalive-only ns/op and B/op are not throughput/allocation measurements.

The full initial run took 542 seconds. The native-poll 1024-session processes spent roughly 30–38 seconds each on the observation plus sequential Poll/Close measurements. The candidate completed each in roughly 0.25 seconds. Those process durations are harness totals; use the per-operation latency distribution below for a latency claim.

The initial sample set caught two allocation regressions: matches with 32-byte chunks used 7.70% more B/op, and dense Append+Finish used approximately 15% more. Eager spare suffix capacity was removed and all affected redactor workloads were remeasured in ten interleaved samples. Final matching-chunk and dense B/op equal baseline, and the safe retained-tail metric is 10 bytes on both sides. No statistically significant regression exceeding 5% remains in the measured timing, B/op, or allocation workloads. First samples and the rejected implementation are retained for inspection.

The initial idle heap baseline incorrectly included transient allocations from the FD/thread probes. Its count-one heap result was negative and is invalid. The corrected helper collects those allocations before the baseline. Ten fresh-process resource-only samples for each population now supply all accepted idle heap figures. This rerun shares setup with the latency benchmark and closes sessions concurrently only after measurement. The original CPU/FD/thread/latency series remains available; the corrected resource series independently confirms the same behavior.

## Selected timings and allocation results

Medians, ten samples per variant. Listed improvements are statistically significant at p<0.05.

| Workload | Baseline | Final candidate | Interpretation |
| --- | ---: | ---: | --- |
| Real PTY, 32-byte receipt | 11.169µs | 7.079µs | 36.62% faster |
| OS socket, 32-byte receipt | 10.837µs | 5.889µs | 45.66% faster |
| Dense long rule, 4KiB rule / 16KiB input | 12.519ms | 34.29µs | 99.73% faster |
| Dense long rule, 16KiB rule / 64KiB input | 198.270ms | 136.6µs | 99.93% faster |
| Dense long rule, 64KiB rule / 256KiB input | 3.203s | 549.2µs | 99.98% faster; B/op unchanged |
| Mixed 64KiB and one-byte rules, warm one-byte Append | 521.416µs | 18.03ns | Zero steady-state allocations |
| Long no-match boundary, 64KiB chunk | 300.7µs / 512KiB B/op | 133.3µs / 64KiB B/op | 55.66% faster; 87.50% fewer allocated bytes |
| Short matches, one-byte chunks | 35.59µs / 16KiB B/op | 21.30µs / 2.031KiB B/op | 40.15% faster; 87.30% fewer allocated bytes |

For the KMP workloads, increasing rule/input length fourfold from 4KiB to 16KiB to 64KiB increases candidate time approximately fourfold. Mixed long/short one-byte Append stays at 18.45/18.35/18.03ns for long rules of 257/4096/65536 bytes. The independent review also counted exactly one short-rule search per byte at every tested retained length after the boundary-scan correction.

The ordinary literal, All, nested All, regexp, mixed pattern, fragmented receive, no-rule redactor, and safe redactor workloads have no significant timing regression exceeding 5%. Their complete results are retained rather than reduced to a combined geometric mean.

## Idle resources and latency

Accepted corrected resource-series medians. CPU is process CPU nanoseconds per session per millisecond of observation; it includes process/harness background work. FD delta includes both ends of each socket pair.

| Live sessions | CPU ns/session-ms, before → after | Actual threads, before → after | Added FDs, both variants | Retained heap/session, before → after |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 12,537 → 2,631 | 8 → 7 | 2 | 2.039KiB → 1.555KiB |
| 64 | 2,526 → 37.22 | 70 → 7 | 128 | 7.080KiB → 1.940KiB |
| 256 | 2,241 → 9.508 | 262 → 7 | 512 | 7.148KiB → 1.934KiB |
| 1024 | 7,979 → 2.003 | 1004 → 7 | 2048 | 8.579KiB → 1.938KiB |

The runtime candidate shows no per-session thread growth and no extra FDs. A deterministic RawConn observer test also verifies that an idle runtime pump neither reenters Read nor performs periodic Control calls. The single-session heap estimate is noisier (candidate confidence interval ±30%); larger populations are more stable. CPU is not claimed to be exactly zero.

From the original complete latency series at 1024 live sessions: Poll p50 **14.544ms → 43.04µs**, Poll p95 **76.014ms → 66.71µs**, Close p50 **5.734ms → 3.480µs**, and Close p95 **10.869ms → 5.980µs**. Every population's full distribution and statistical comparison is included in the artifacts.

## Receive retention

This is a generic expect workload: each of sixteen live sessions receives a 1MiB burst, matches its terminator, and retains the indicated tail. It is not a measurement of a connector device session; connector already uses an approximately 64KiB streaming fallback. Holding the returned Result intentionally retains its independently owned 1MiB Before string.

| Result ownership | Remaining bytes | Baseline retained/session | Candidate retained/session |
| --- | ---: | ---: | ---: |
| Discarded | 0 | 1051.45KiB | 19.34KiB |
| Discarded | 8 | 1051.21KiB | 18.92KiB |
| Discarded | 4096 | 1051.11KiB | 22.98KiB |
| Held | 0 | 2.026MiB | 1.018MiB |
| Held | 8 | 2.026MiB | 1.018MiB |
| Held | 4096 | 2.026MiB | 1.022MiB |

The candidate's remaining session storage grows with the remainder, while caller-held Results retain their own snapshots as required. Session/scratch overhead remains in these heap figures.

## Verification and artifact map

Final implementation checks passed:

- Darwin arm64 Go1.27.3: complete `script/ci`, including gofmt, module verification/tidy, vet, full race/shuffle suite, all five fixed 100,000-execution fuzz targets, benchmark smoke, dialogue, and disposable source consumer without replace/go.work.
- Darwin arm64 Go1.26.0 and Linux arm64 Go1.26.0/Go1.27.3: full race/shuffle suite and vet. Linux used disposable source copies in Docker with `--init --network none` and a read-only module cache.
- golangci-lint v2.15.0 under Go1.27.3: 0 issues; 23 release-script tests passed; patched-toolchain govulncheck: no vulnerabilities found.
- Independent final source review: approved, including the mixed-rule correction, final allocation adjustment, and corrected heap setup. Expected failing regressions, subsequent passes, and 20 repeated race polling checks are retained.

Artifacts are in [benchmarks/2026-10-29-refinement](benchmarks/2026-10-29-refinement/):

- [metadata.json](benchmarks/2026-10-29-refinement/metadata.json): source/benchmark/binary SHA-256, toolchain, sampling details and superseded-series labels.
- [Final redactor comparison](benchmarks/2026-10-29-refinement/expect-redactor-final-benchstat.txt), [receive/PTY comparison](benchmarks/2026-10-29-refinement/expect-throughput-benchstat.txt), [corrected idle heap/resources](benchmarks/2026-10-29-refinement/expect-idle-heap-final-benchstat.txt), [original idle CPU/FD/thread/latency](benchmarks/2026-10-29-refinement/expect-idle-benchstat.txt), and [receive retention](benchmarks/2026-10-29-refinement/expect-retention-benchstat.txt). Adjacent baseline/candidate text files contain every raw sample. Initial throughput redactor and dense allocation rows are superseded by the final redactor comparison; original idle heap rows are invalid and superseded by the corrected resource comparison.
- [Final CI](benchmarks/2026-10-29-refinement/expect-ci-final.log), separate final platform/lint logs, [independent review](benchmarks/2026-10-29-refinement/expect-review.md), regression logs, deterministic operation-count probe sources, and exact measurement scripts.

These are local/source-level results. They do not claim remote CI, real-device acceptance, publication, or adoption of a newly versioned expect dependency. Root-owned connector/worker/PostgreSQL/Firemon source-contract checks are separate evidence.
