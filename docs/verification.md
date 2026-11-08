# Verification

`bash script/ci` checks gofmt, the compiled README example copy, module integrity,
vet, Windows and FreeBSD cross-compilation, shuffled race tests with goleak, eleven
bounded fuzz targets, benchmark smokes, the dialogue and an independent consumer.
It also checks package ownership from the production import graph and runs
negative controls for forbidden application and reverse ownership dependencies.
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

Lint includes errorlint, revive, exhaustive, contextcheck and errname alongside
vet/staticcheck/unused/ineffassign. Complexity caps are 44 for gocyclo and 160
lines/100 statements for production functions. Tests are exempt from size caps;
contextcheck cannot infer testing.Context inheritance across nested test bubbles.
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
