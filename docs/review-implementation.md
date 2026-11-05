# Implementation of the 2026-11-05 review

The review baseline is v0.1.4 (`b75ec0a`). The table tracks all 16 recommendations across v0.2.0 and the v0.3.0
candidate; coverage means the stated Go capability, rather than full Tcl parity. Public interfaces are changed directly; removed
names have no aliases or transition wrappers.

| Item | Implementation |
| --- | --- |
| A1 | Console.Start controls /dev/tty; spawn owns child cancellation and Wait |
| A2 | Timeout/Default/FullBuffer event cases, bounded callback limits |
| A3 | Before/After/scoped Case rules, independent counters and snapshots |
| A4 | Initial/runtime window size and runtime echo flag preservation |
| A5 | Glob/Fold, no-consume preview, named captures, byte ranges, Buffer |
| A6 | Single-pump Interact, local actions/escape, remote cases, raw-terminal helper |
| A7 | Serialized slow/NUL/break sending with cancellation/progress |
| A8 | Metadata candidate trace, slog and independent redacted send recording |
| A9 | ExpectAny, background waits, optional byte filters, Windows ConPTY |
| B1 | Per-call waiter preparation/scan/dispatch/fill/wait, named receive/diagnostic/mailbox state |
| B2 | Shared internal/redact coverage/KMP/storage mechanics with separate rendering policies |
| B3 | Op/ErrClass, ConfigError, CompileError and ErrInvalidPattern; progress/class write errors |
| B4 | Zero Return; unified Case/Limit; ContinueKeepTimer, Then and ReplyThenContinue |
| B5 | WaitGroup.Go, deadline-capable synchronous sends, common write retirement and cancellable gate |
| B6 | Tests regrouped by subject, executable Example and checked README copy |
| B7 | Broader lint/complexity caps, breaking-minor selection, SPDX, compact docs, FreeBSD assessment |

Receive cursors and native polling remain private in the root package: their
methods operate directly on immutable Pattern and owned Session state. Moving
these methods mechanically to another package would require exposing internal
protocol types or introduce a dependency cycle. The independent redaction engine
provides the useful package boundary; no exported compatibility surface is added.

Existing non-OS timer tests already ran inside testing/synctest. Their apparent
long sleeps advance virtual time. A flaky real-deadline SendAll cancellation test
has now also moved into a virtual-time bubble after CPU load exposed cancellation
before transport admission. OS readiness benchmarks/tests retain real waits.

OSSMETADATA contained only Netflix's internal `osslifecycle=active` marker, had no
build/release consumers and has been removed. Original copyright and license text
remain; Go files consistently include SPDX headers.

Raw historical docs/logs were saved outside the source tree as
`/root/go-expect-review-archive/2026-11-05-pre-v0.2-docs.tar.gz` before cleanup;
Git at v0.1.4 remains the portable recovery source. Benchstat summaries remain in
`docs/benchmarks`. This local archive is not a published Release attachment.

Platform boundaries: Linux process/runtime checks run locally. Native Windows
ConPTY checks are added to CI, with local cross-compilation/vet only. Darwin and
FreeBSD are compile/vet checked locally; FreeBSD has no runtime CI claim. Unix
process-group SIGHUP requires cooperative descendants; WaitDelay kills only the
direct child. ConPTY uses a kill-on-close job for descendant cleanup.

The v0.2.0 verification recorded full script/ci (eight fuzz targets), lint and
cross-platform compile checks. Those results describe that release, not the
current candidate. Candidate verification and allocations are recorded separately
in [the v0.3.0 review](review-v0.3.md). Native Windows/macOS runtime results require
their CI runners; no remote candidate run is claimed.

The [benchmark comparison](benchmarks/2026-11-05-spec/README.md) records both the
Send allocation reduction (default 9 to 3; unlimited 4 to 1) and added receive
construction cost. Median timings on a shared host are descriptive, not a claim
of statistically significant improvement for every workload.

References for the platform implementation: [controlling terminals](https://man7.org/linux/man-pages/man4/tty_ioctl.4.html),
[ConPTY creation](https://learn.microsoft.com/en-us/windows/console/creating-a-pseudoconsole-session),
and [ConPTY teardown ordering](https://learn.microsoft.com/en-us/windows/console/closepseudoconsole).

The subsequent audit corrected the earlier first-winner coordinator: Continue
must restore watching all sessions, rather than locking subsequent matches to the
first session. v0.3.0 retains independent idle timers/counters and rotates within
rule groups. Local interaction supports multi-byte literals; remote interaction
supports full patterns. Go RE2/Unicode and Fold's leftmost-first mode remain
intentional differences from Tcl. FreeBSD remains compile/vet-only. These limits
are not claims of complete Tcl compatibility.
