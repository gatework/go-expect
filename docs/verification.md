# Verification

`make check` checks the gatework standard files, formatting, module tidiness and
integrity, vet, the shared lint configuration, package ownership from the
production import graph (`architecture.yaml`), shuffled race tests with goleak,
reachable vulnerabilities, the compiled README example copy, Windows and FreeBSD
cross-compilation, eleven bounded fuzz targets, benchmark smokes, the dialogue
and an independent consumer.
The dialogue uses a local duplex stream; Unix process tests verify /dev/tty,
window size, authentication, EOF, cancellation and process reaping.

The 2026-11-07 candidate passed the complete local gate and golangci-lint on
Go 1.27.3, and `go test ./...` on Go 1.26.0, with `GOWORK=off`. The updated
architecture guard also passed four negative/positive control tests, including
an internal helper that tries to hide a reversed process dependency. This round
changes ownership guidance and its gate; the stream runtime is unchanged.

GitHub Actions runs Linux/macOS with Go 1.26/1.27 and native Windows tests for
ConPTY. Cross-compilation on a Linux development machine establishes buildability,
not Windows/macOS/FreeBSD runtime behavior. FreeBSD is compile-checked only and is
not in the runtime support matrix. Real network-device interoperability remains
outside these local fixtures.

Lint comes from the shared gatework standard configuration. Production functions
are limited to 80 lines, 50 statements and cognitive complexity 30; tests are
exempt from size caps.
Metadata reporting for rejected nil contexts and stored per-wait handler contexts
have narrowly documented analyzer exceptions.

Existing timer-only tests already use testing/synctest. Their time.Sleep calls
advance virtual time; OS/PTY readiness tests retain real waits. Additional tests
cover new ownership and event semantics, not trivial option setters.

The v0.4 candidate adds regression coverage for callback EOF and abnormal exits,
send-sink panic cleanup, paused background ownership, live Group membership,
rule replacement, nonconsuming progress, local regex captures/EOF/idle/terminal
modes and Relay backpressure/progress/redaction. Input-pattern fuzzing reconstructs
raw bytes across arbitrary read boundaries. The partial-prefix benchmark records
4/8/16 KiB scaling, separately from reused Expect and long-lived Send allocations.

The shared gate uses published go-standards v1.1.0 by default. Standard development can opt into a checkout with `make check STANDARDS_DIR=/path/to/go-standards`; adjacent directories are not selected implicitly. The lifecycle read-error regression covers wrapped and joined EOF, independent failures, split reads, continuation handlers and retained tails.

The 2026-11-16 candidate passed the complete `make check` on Go 1.27.4, including race/shuffle, eleven 100,000-execution fuzz targets, Windows compile, FreeBSD vet and the disposable source consumer. Full package tests also passed on Go 1.26.0. These local results do not establish native Windows or real-device behavior.

## Dependency and mailbox follow-up (2026-11-16)

All four direct requirements were checked against `@latest` and remain current.
The retained indirect test requirements are testify v1.12.1 and
`go.yaml.in/yaml/v3` v3.0.6; `go test go.uber.org/goleak` also passed with this
selection. Unused upstream module-graph entries (kr/pretty, objx and check.v1)
are not pinned merely to override upstream metadata: `go mod tidy` removes those
unnecessary requirements. The Go floor remains 1.26.0.

The mailbox stores one coalesced event rather than a one-element slice. Eight
200 ms samples per case on Go 1.27.4, darwin/arm64, compared with benchstat,
showed single-event allocation falling from 48 B / 1 allocation to zero and
coalesced 16-chunk allocation falling from 1968 B / 5 allocations to 1920 B /
4 allocations. Single-event time fell about 15%; coalesced time had no significant
change. These are mailbox microbenchmarks, not end-to-end throughput claims.

The v0.6.2 release candidate passed the complete `make check` using published
standard 1.1.0, including eleven 100,000-execution fuzz targets, cross-platform
compile/vet and the independent stream/PTY consumer. Full package tests also
passed on Go 1.26.0. Unrelated standard-development changes are outside this
release candidate.
