# Verification

`bash script/ci` checks gofmt, the compiled README example copy, module integrity,
vet, Windows and FreeBSD cross-compilation, shuffled race tests with goleak, eight
bounded fuzz targets, benchmark smokes, the dialogue and an independent consumer.
The dialogue uses a local duplex stream; Unix process tests verify /dev/tty,
window size, authentication, EOF, cancellation and process reaping.

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
