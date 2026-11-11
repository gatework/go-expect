# Module organization

The root `expect` package owns a conversation with one byte stream. Its public
interface is the session, matching rules, results and redaction operations. The
implementation keeps cancellation, accepted bytes and resource retirement under
the same session owner. Package boundaries follow separately owned resources.

| Module | Ownership and interface |
| --- | --- |
| `expect` | `Session` owns the supplied duplex transport, read pump, receive state, write admission and diagnostic streams. `Console` adapts a Unix PTY to that contract. |
| `spawn` | Owns a child process and its terminal. Construction and close manage the process lifetime on each supported platform. |
| `expecttest` | Owns test conversation fixtures and cleanup. It exercises the public conversation interface. |
| `internal/redact` | Owns the matching algorithm and retained suffix storage shared by the public redactor and session diagnostics. |

Within the root package, files follow the state they maintain:

| Responsibility | Implementation |
| --- | --- |
| Session construction, interruption, close and worker ownership | `session.go` |
| Session options, validation and input normalization configuration | `session_options.go` |
| Sole input pump and publication to the receive mailbox | `session_read.go`, `mailbox.go`, `polling_*.go` |
| Receive matching and rules | `expect.go`, `waiter*.go`, `pattern.go`, `cursor.go`, `rules.go` |
| Serialized transport writes and deadline lifecycle | `write.go`, `send_all.go`, `send_extra.go` |
| Interactive routing and background matching | `interact*.go`, `background.go`, `group.go`, `relay.go` |
| Metadata events, classification and observers | `diagnostics.go` |
| Session-owned borrowed output streams and redaction state | `diagnostic_output.go`, `diagnostic_redaction.go` |
| Independently usable streaming redactor | `redact.go` |
| Error contracts | `errors.go` |

`Session` is the lifecycle owner, while the exclusive receive operation owns
matching state. The diagnostic mutex protects borrowed sinks and their independent
stream filters. A diagnostic filter never changes raw protocol evidence.
Interruption retires I/O without joining the worker that requested it; close then
joins workers and flushes diagnostics. Output writers remain borrowed.

The public `Redactor` has its own synchronization and configurable replacement.
The private `diagnosticRedactor` is serialized by its session and retains hidden
coverage across rule additions. Their distinct policies use the same internal
matching implementation.

Device authentication, command acceptance, persistence confirmation, inventory and
CLI configuration belong to callers such as go-net-connector. This package does
not import the network automation applications. Existing race, cancellation,
partial-write, fuzz and external-consumer checks exercise these ownership rules.

`architecture.yaml`, enforced by the shared archcheck in `make check`, constrains
the production import graph: generic stream packages cannot import the
applications, the root session cannot depend on its process or fixture
consumers, and the redaction algorithm cannot depend on the session owner.

## Organization verification

The 2026-11-07 organization pass moved declarations using Go AST positions and
renamed the private diagnostic filter with gopls. Public types and function
contracts remain unchanged. The earlier deadline and interruption repairs remain
part of the working tree.

Validation passed with Go 1.26.0 (`go test ./...`, `go vet ./...`) and Go 1.27.3
(`script/ci`, race/shuffle tests and configured lint). The full CI script includes
Windows compile checks, FreeBSD vet, ten fuzz targets at 100,000 executions each,
examples, and the disposable source-consumer check. The modernization scan only
suggested integer-range loops in unrelated examples/tests; those files were not
rewritten by this structural change. Source-consumer evidence is distinct from a
published-version consumer or real network device test.
