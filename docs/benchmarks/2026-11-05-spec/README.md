# Review implementation measurements

Baseline: v0.1.4 (`b75ec0a`). Candidate: the working-tree implementation of the
2026-11-05 specification. Go 1.27.3 linux/amd64, AMD EPYC 7K62, GOMAXPROCS=2;
six samples per workload, 150 ms per sample. Baseline/candidate ran sequentially
on a shared host. Values below are medians, not significance-tested conclusions.
Full medians are in summary.json; raw runs and Go-source hashes are archived
outside the tree under `/root/go-expect-review-archive/2026-11-05-spec`.

| Workload | Baseline | Candidate |
| --- | --- | --- |
| Send, default timeout | 6438.5 ns; 648 B; 9 allocations | 2734 ns; 192 B; 3 allocations |
| Send, unlimited timeout | 4942 ns; 264 B; 4 allocations | 1911.5 ns; 64 B; 1 allocation |
| Standalone redaction allocations | safe: 1022/4; matches: 259/8 | Same allocation counts |

Send measurements reuse one net.Pipe Session with a continuously draining peer,
and exclude construction. The exact same added benchmark was compiled against
the baseline. Deadline-capable Send avoids a per-write worker, uses the transport
for its write budget, and allocates context cancellation machinery only when the
parent can actually cancel. Non-deadline transports keep the joined-worker path.

Fragmented receive constructs a Session for each 16 KiB dialogue. It now uses two
additional allocations and about 0.36 KiB more per dialogue for the expanded
session/options and WaitGroup.Go admission. Median time changes ranged from
-3.8% to +9.7% across chunk sizes. Redaction medians ranged from -3.8% to +12.2%.
These changes are retained explicitly; this is not a claim of universal speedup.
During measurement, waiter option escape and incorrect shared buffer reserve
policy were identified and fixed, rather than hiding their extra allocations.

Reproduce with:

```sh
GOMAXPROCS=2 go test -run '^$' -bench '^BenchmarkSerializedSend$' -benchmem -benchtime=150ms -count=6 .
GOMAXPROCS=2 go test -run '^$' -bench '^BenchmarkFragmentedReceive$' -benchmem -benchtime=150ms -count=6 .
GOMAXPROCS=2 go test -run '^$' -bench '^BenchmarkPublicRedactor$/(safe|matches)/chunk_(1|1024)$' -benchmem -benchtime=150ms -count=6 .
```
