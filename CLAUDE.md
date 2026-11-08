# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

@AGENTS.md

## Project

`github.com/gatework/go-expect` (package `expect`, Go 1.26+) is an expect-style engine for conversations with interactive programs and duplex streams: bounded matching, continuations, reusable rules, PTY/ConPTY processes and streaming secret redaction. It is a fork that keeps Netflix's Apache-2.0 notices; every Go file starts with `// SPDX-License-Identifier: Apache-2.0`.

## Commands

```bash
GOWORK=off bash script/ci           # full local gate (what CI runs on Linux/macOS)
go test ./...                       # quick test pass
go test -race -shuffle=on -count=1 -timeout=90s ./...   # CI test invocation
go test -run 'TestName' -count=1 .  # single test in root package
go test -run 'TestName' ./spawn     # single test in a subpackage
go test -run '^$' -fuzz '^FuzzMatchChunks$' -fuzztime=100000x -parallel=2 .   # one fuzz target
go test -run '^$' -bench . -benchmem -benchtime=3x .                           # benchmark smoke
golangci-lint run                   # lint (CI pins v2.15.0; config in .golangci.yml)
go run ./examples/dialogue          # runnable dialogue example
bash script/consumer                # compile a consumer against a snapshot of this module
python3 -m unittest discover -s script/tests -v                                # release-script tests
```

`script/ci` also enforces `gofmt -l` and `go mod tidy -diff` cleanliness, the import-graph ownership gate (`script/check-architecture.py`), Windows test cross-compilation, FreeBSD vet and every bounded fuzz target listed in the script; add new fuzz targets there.

The full benchmark suite exceeds `go test`'s default 10-minute timeout; pass `-timeout 0` or select benchmarks with `-bench`. Compare revisions with repeated runs and benchstat, measured serially (see `docs/benchmarks/README.md`).

**README example sync:** the README code block between `<!-- example:start -->`/`<!-- example:end -->` must be an exact copy of `example_test.go`. After editing `example_test.go`, run `python3 script/readme.py --write`.

Lint caps for production code: gocyclo `min-complexity: 45`, funlen 160 lines / 100 statements (tests exempt). `exhaustive` treats `default` as exhaustive.

## Architecture

See `docs/architecture.md` (file-to-responsibility map) and `docs/contracts.md` (normative behavior contracts — check it before changing semantics). Big picture:

- **`Session` is the single lifecycle owner** of the supplied `io.ReadWriteCloser` (taken only on successful `NewSession`; options validate before any I/O). One read pump (`session_read.go` → `mailbox.go`, with `polling_*.go` for Unix readiness) is the only transport reader.
- **Exclusive receiver:** `Expect`, `Poll`, `Interact`, `StartBackground` and `Group` each reserve the single receive slot per session; conflicts return `ErrBusy`. The receiver owns matching state (`expect.go`, `waiter*.go`, `pattern.go`, `cursor.go`, `rules.go`).
- **Incremental matching:** per-case scan progress lets literals and line-local regexps resume instead of rescanning. It is valid only while the receive buffer grows by appends; every consumption or rule change must invalidate it.
- **Writes are serialized** (`write.go`, `send_all.go`, `send_extra.go`); a failure after write admission retires the session and reports exact `WriteError.Written` progress.
- **Close** marks closed, cancels handler scope, interrupts I/O, joins owned workers, flushes diagnostic filters. It is idempotent and does not wait on user callbacks.
- **Matching priority:** Before rules → foreground cases → per-call rules → After rules; within a group the earliest visible byte wins, declaration order breaks ties (`WithMatchOrder(Declaration)` changes this). Overflow returns `ErrBufferLimit` and preserves data unless a `FullBuffer` case consumes it.
- **Redaction:** `internal/redact` holds the shared matching engine used by the public `Redactor` (`redact.go`) and the session's `diagnosticRedactor` (`diagnostic_redaction.go`). Redaction applies only to diagnostic/output streams; `Result` keeps raw bytes.
- **Subpackages:** `spawn` owns a child process + terminal (Unix PTY via `creack/pty`, Windows 10 1809+ ConPTY with kill-on-close job). `Console` in the root adapts a Unix PTY for `*exec.Cmd`. `expecttest` provides test conversation fixtures.

## Testing conventions

- `lifecycle_test.go` uses `goleak`; tests must not leak goroutines.
- Timer-only tests use `testing/synctest` (`time.Sleep` there advances virtual time). OS/PTY readiness tests use real waits.
- Platform-specific code/tests use `_unix`/`_windows`/`_other`/`_bsd`/`_linux` file suffixes; keep the Windows cross-compile and FreeBSD vet passing.

## Commits and releases

Conventional-commit subjects (`feat:`, `fix:`, `perf:`, `refactor:`, `test:`, `docs:`, `chore:`); put benchstat output in `perf:` commit bodies. Pushes to `master` auto-release after all CI gates pass (`script/release.py`, see `docs/releasing.md`): ordinary changes bump patch; before v1 a commit line starting `BREAKING:` or `BREAKING CHANGE:` bumps minor. No compatibility aliases are kept for removed APIs. Record user-visible changes in `CHANGELOG.md` under the next version heading.
