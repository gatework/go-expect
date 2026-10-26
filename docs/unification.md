# 2026-10-26 generic core unification

Based on c66cbd2. The reviewed additive compatibility implementation was ported
from the preserved connector candidate; existing core small-read, continuation,
cancellation and ownership regressions remain in the full suite. Newly ported
tests first failed on absent Poll/SendAll/scoped redactor APIs. New lifecycle
tests then failed on uncancelled handlers and omitted cleanup causes before fixes.

The generic module now owns all matching, streaming, PTY readiness and redaction.
Device policy was removed; migration is documented in connector.md. Console's
slave accessor is TTY. FromRegexp retains Longest/POSIX mode and snapshots changes.
At the implementation checkpoint no commit, push or release had been performed. The consumer must use a published
GitHub dependency; a local replacement is not an acceptable delivery substitute.

Fresh independent review found one Important: SendAll repeatedly converted the
remaining suffix to bytes. A failing 4096-byte/one-byte-progress allocation test
observed 4112 allocations; the worker now converts once and slices that buffer.
The regression and full suite pass after the fix. Added explicit CompilePOSIX
coverage in addition to Longest and snapshot isolation. No other Important or
Critical generic-core findings. Consumer/release/device validation was outside
this milestone review; Continue reset semantics intentionally remain unchanged.

The user subsequently authorized version publication. The v0.1.1 release follows
the repository automatic patch policy; the changelog explicitly marks TTY as a
pre-v1 breaking API change. Consumer migration is a separate in-progress step.

The first remote candidate used cached Go 1.26.9 despite a newer supported patch
being available. Its pure matcher fuzz smoke ended with the fuzz coordinator's
context deadline (no failing input); local Go 1.26.10 race and the same fuzz smoke
passed. setup-go now checks latest patches for every matrix/package/quality job.
The same fuzz gates remain enabled; publication requires a fully green new SHA.

The latest-patch remote retry hit the same deadline-only failure in a different
fuzz target. This matches the coordinator deadline race reported upstream at
https://github.com/golang/go/issues/75804. CI fuzz smokes now each require 100,000
executions instead of racing a five-second cutoff. The targets, assertions and
fatal exit handling are unchanged; no timeout or test failure is waived.

Both macos-latest jobs remained queued with no assigned runner across the first
three attempts. The matrix now pins the supported macos-15 image instead of the
moving macOS 26 alias; Linux/macOS and both Go versions remain required gates.
This is a runner-capacity fallback, not a skipped or locally substituted job.

The macOS 15 Arm64 pool also left both jobs unassigned. The final capacity
fallback uses the documented standard macos-15-intel pool, exercising the
supported Darwin/amd64 target in addition to local Darwin/arm64 verification.
Both remote macOS jobs remain mandatory; no publication gate is bypassed.
