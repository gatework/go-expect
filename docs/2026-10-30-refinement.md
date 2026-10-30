# Session filtering and capture refinement

The Session's private per-byte filter now keeps incremental KMP state for rules
longer than 256 bytes, bounds short-rule boundary scans and merges overlap
coverage. New rules inspect only un-emitted bytes. Safe chunks bypass stream
storage when no rule can begin in them. Scratch retention is bounded at 64 KiB
or four times the ambiguous suffix; larger transient buffers detach. Finish
releases stream storage, including after a sink failure, without retrying output.
Sessions without diagnostic sinks skip filtering while retaining validation.
The public Redactor's replacement and rule-scope behavior remains separate.

Regexp candidates retain offsets until selection. Only the winning Case copies
capture strings; All children compute extents. Captures detach before guards or
consumption. Earliest/declaration ordering, optional and empty groups, and
POSIX/Longest modes retain their existing semantics.

The baseline is committed source `6e4e2dc054b1cd400801381d57b35f08f89a398f`.
The evidence directory records ten serial interleaved samples on Apple M5 arm64,
Go 1.27.3, GOMAXPROCS=4. Filter and capture setup is outside the timed loop.
Retention measures live heap after an 8 MiB append across eight sessions; its
ns/op and allocation columns do not measure append throughput. The first filter
variant was rejected for repeated large-chunk allocation; the final variant
reuses bounded scratch storage and bypasses provably safe chunks.

The final eight-byte-rule/16 KiB safe-chunk case drops from 8.55 microseconds to
1.08 microseconds with one allocation per append. Dense long-rule cases avoid
quadratic rescanning. Retained heap drops from about 16.03 MiB to 19.6 KiB per
session in the burst fixture. Eight competing regexp cases allocate 77% fewer
bytes; eight All cases allocate 94% fewer bytes and run about 19% faster.
Existing receive/public-redactor controls show no significant timing increase
above 5%; an unchanged public no-rule case increases 3.16% in this sample.

Measured results, complete verification logs and source digests are recorded in
[the evidence directory](benchmarks/2026-10-30-refinement). Publication and remote
CI are verified separately from these local measurements.

Local verification passed: full script/ci on Darwin Go 1.27.3, including six
100000-execution fuzz targets, race/shuffle, vet, example and source consumer;
golangci-lint reported zero issues and govulncheck found no vulnerabilities.
Full race/shuffle and vet passed on Darwin Go 1.26.0 and Linux arm64 Go 1.26.0
and 1.27.3. Release automation tests and paired connector source contracts passed.
Linux uses disposable sources, no network and a read-only module cache.

Initial remote CI caught a benchmark-only macOS Intel Go1.26 compatibility
issue: os.ReadDir(/dev/fd) tried to stat transient descriptor entries and returned
EBADF. The FD probe now counts directory names without statting entries. Its
regression benchmarks pass locally on Darwin Go1.26/1.27 and Linux Go1.27;
independent review passed. The initial remote failure is retained as evidence.
