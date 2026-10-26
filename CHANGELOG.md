# Changelog

## v0.1.1

- BREAKING: rename Console.Tty to Console.TTY; update callers as described in
  docs/migration.md. The module remains pre-v1 and uses automatic patch versions.
- Add opt-in Poll, SendAll, declaration priority, bounded streaming fallback and
  standalone scoped redaction; default matching and single-write sends remain.
- Preserve compiled regexp semantics through FromRegexp and cancel active handler
  contexts on Close without waiting for user callbacks.
- Retain cleanup errors and exact progress on automatic retirement; make repeated
  positive partial writes use one send buffer rather than copying every suffix.
- Keep go-expect focused on generic streams and PTYs. The unreleased duplicate
  device package is removed; device-layer migration belongs to go-net-connector.
- Preserve cancellation/Close when write completion races with stopping, include
  pending input in rejected waits, and reduce fragmented receive allocations.
- Verify source archive files and permissions against the tested Git tree.

## v0.1.0

- Move module path to `github.com/gatework/go-expect`, requiring Go 1.26.
- Add owned duplex Session with bounded byte buffering, cancellation, serialized
  writes with exact failure progress, and deterministic worker shutdown.
- Add immutable patterns, detached results, Continue/ContinueWithoutReset,
  relative/idle/context deadlines, and reusable per-call Responder rules.
- Add explicit overlapping streaming secret redaction and metadata-only observers.
- Rework Console as a Linux/macOS PTY adapter with pollable I/O and optional raw mode.
- Replace legacy readers, passthrough pipes and unmanaged test logging goroutines.
- Add device-dialogue tests, fuzz targets, migration guidance, independent consumer
  smoke and Linux/macOS CI for Go 1.26/1.27.
- Harden typed-nil validation, callback cleanup and write-progress validation;
  align response events with public results and avoid repeated unread-tail copies.
- Automatically publish default-branch snapshots after the complete CI gate,
  reusing verified source artifacts with patch versioning and resumable drafts.

This release changes public APIs and timeout semantics; see docs/migration.md.
