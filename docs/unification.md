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
