# v0.3.0 candidate: review and verification

This round checks `/root/go-expect.md` against v0.2.0, rather than treating the
previous implementation table as proof of complete behavior. The 16-item map
remains in [review-implementation.md](review-implementation.md); precise public
behavior is in [contracts.md](contracts.md), with direct API changes in
[migration.md](migration.md). No compatibility aliases were added.

## Corrections implemented

- ExpectAny retains one waiter per session across continuations. Relative reset
  applies to all active sessions; idle budgets and counters remain independent.
  Before/call/After priority applies across sessions, with round-robin ties.
  Accepted input is inspected before arbitration without draining a noisy peer.
  Unselected candidates are cached: rescanning them with advanced literal cursors
  previously skipped available matches. Reservations are released before observers.
- Interaction validates options and local sequence bounds before input ownership
  or terminal changes. Input actions accept byte-exact Literal sequences, share
  trie prefixes, and batch ordinary bytes. Actions run on the receive owner, with
  declaration priority across read boundaries and EOF prefix resolution.
- Cleanup preserves admitted WriteError causes/progress, unlocks diagnostics on
  writer panic and retires sessions after final redactor flush errors. Small
  buffers use H=min(64 KiB, limit), consuming max(1, H/2) when unmatched.
- Timeout callbacks receive a fresh relative budget. Parent, idle and effective
  relative deadlines are checked after callbacks, including Return/Continue and
  callbacks that ignore their context. Callback failures preserve budget causes.
- Fold validates original declarations before recompiling. Glob accepts reversed
  ranges and lowercases folded endpoints before ordering. CompileError unwraps
  ErrInvalidPattern while retaining only a syntax category.
- Result, completion/trace events and ResponseError expose match source, scope and
  declaration coordinates. Virtual event selections are traced without payloads.
  Nonmatching send events use -1 case/rule coordinates.
- Empty-option constructors/preparation avoid option heap escapes. Up to eight
  literal scan offsets live inline; unused coordinator preview state is removed.

## Allocation evidence

`BenchmarkReusedExpect` reuses a Session, its cases and the pending `END` bytes,
excluding transport setup. The identical benchmark was also run against the
published v0.2.0 source archive on this host with Go 1.27.3, linux/amd64.

| Operation | v0.2.0 allocations/op | Candidate allocations/op |
| --- | ---: | ---: |
| Reused Literal END | 3 | 1 |
| Reused captureless regexp END | 4 | 2 |
| Serialized Send, default write timeout | 3 | 3 |
| Serialized Send, unlimited write timeout | 1 | 1 |

The short matching workloads retain only their detached result string and, for
regexp, its index slice. Timing samples on this shared host vary under concurrent
checks; these results establish reduced allocations, not a universal latency gain.
Run `go test -run '^$' -bench 'Benchmark(ReusedExpect|SerializedSend)$' -benchmem .`
to reproduce the workload. Long/fragmented patterns still use stateful cursors.

## Verification and limits

Local checks: full `script/ci`, including module verification/tidy, vet, Windows
cross-compilation, FreeBSD vet, shuffled race tests, all nine fuzz targets at
100,000 executions each, benchmarks, compiled README example and disposable
source consumer. After the final candidate-cache fix, the full shuffled race
suite was rerun, plus targeted timer/group-priority/fairness regressions. Pinned
golangci-lint 2.15.0 runs for Linux and Windows and all 24 release-script tests
passed. Darwin/FreeBSD vet and Windows cross-compilation were also checked.

Local interaction intentionally accepts only Literal sequences; remote cases use
the full matcher. Glob/Fold retain Go Unicode/RE2 policies, including Fold's
leftmost-first mode for imported Longest patterns. Arbitrary historical-prefix
matching is outside the bounded streaming contract. Windows/macOS runtime checks
require their CI runners, and FreeBSD has compile/vet coverage only. No remote CI,
new tag or release publication is claimed by this round.
