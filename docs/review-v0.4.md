# Go-native Expect capability evolution (v0.4 candidate)

Baseline: v0.3.0 (`b952757`). This implementation covers the approved capability
roadmap without a Tcl interpreter or compatibility aliases. Precise semantics and
API replacements are in [contracts](contracts.md) and [migration](migration.md).

## Implemented stages

1. Correctness and receive internals: separate local stop from EOF/error; defer
   write lifecycle cleanup on sink abort; typed async callback failure; one budget
   classifier; immutable rule snapshots and per-operation state. Pattern/Case/Result,
   matching, dispatch and receive I/O now have distinct private responsibilities.
2. Session capabilities: stable IDs, per-case selectors/no-consume, atomic SetRules,
   mutable Group with owned Add/Remove acknowledgements, and explicit background
   Pause/Resume with context-bounded joins. Paused operations never inspect a
   foreground owner's buffer. Selected candidates revalidate rule generations.
3. Interaction and processes: incremental local Literal matching and conservative
   Go-regexp prefix feasibility, input EOF/idle/full-buffer events, forwarding/echo/
   cooked callbacks, bounded explicit Relay routes, paced/NUL sends, and a shared
   spawn entry for Unix PTY/Windows ConPTY with environment/directory/exit metadata.

Each transport still has one reader. Foreground callbacks run on the receive
owner. Relay targets serialize raw protocol writes, while each operator output
uses an independent redactor. Explicit controls avoid implicit same-session
reentrancy. Dynamic updates clone definitions; callback closures remain borrowed.

## Reproduced failures and regression coverage

Before implementation, temporary public-API probes demonstrated:

- A local callback returning io.EOF was reported as successful interaction.
- Background Goexit produced a successful zero-result Wait.
- Recovering a synchronous send-output panic left Close blocked on a lifecycle ref.
- A slow trace crossing idle=1ms and relative=3ms was classified as relative timeout.
- Long partial Literal input rescanned the accumulated prefix on every byte.

Necessary regressions now cover these failures plus paused-stop ownership, live
membership/selector changes, no-consume progress, input capture/EOF/idle/full-buffer
semantics, terminal restoration, filtered output tails and Relay fanout/backpressure/
progress. A new input-pattern fuzz target reconstructs raw input across arbitrary
chunks using Literal, regexp, Glob and Line; the existing nine targets remain.

## Performance evidence

The reused END benchmark retains its allocation targets:

| Workload | Allocations/op |
| --- | ---: |
| Reused Literal END | 1 |
| Reused captureless regexp END | 2 |
| Send, default write timeout | 3 |
| Send, unlimited write timeout | 1 |

For one-byte chunks containing an incomplete repeated Literal prefix, the audit
probe took approximately 0.44/1.91/6.76 seconds at 4/8/16 KiB. The new full Interact
benchmark measured approximately 2.8/6.4/10.9 milliseconds, with 80/143/253 KiB
allocated per operation. These are descriptive shared-host samples, not universal
latency claims. Literal progress is incremental; regex capture selection remains
Go regexp and does not promise universal linear streaming performance.

## Verification and boundaries

Local verification includes full script/ci (ten fuzz targets at 100,000 executions),
shuffled race/goleak tests, pinned Linux/Windows golangci-lint 2.15.0, Windows
cross-compilation, FreeBSD vet, README example synchronization, source-consumer
smokes, allocation/prefix benchmarks and all 24 release-automation tests. Relevant
checks are rerun after subsequent candidate fixes. Native Linux/macOS/Windows
acceptance is established by the feature branch's matching Actions commit, not
by local cross-compilation. No tag or release is created for this candidate.

Intentional boundaries: RE2/Unicode, Go contexts/errors, bounded buffers and explicit
ownership are retained instead of Tcl dialect/default compatibility. Prefix
feasibility may retain more input conservatively until the explicit window limit.
Borrowed sinks/callbacks remain cooperative; cooked mode does not grant concurrent
reader ownership. Relay backpressure can pause routing and cross-source chunks
can interleave at one target. Unix descendants must cooperate with SIGHUP;
Windows owns a Job. FreeBSD remains compile/vet-only. Tcl interpreter commands,
script debugger and host-language conveniences use Go/application facilities.
