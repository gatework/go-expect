# Changelog

## Unreleased

- Add the `connector` device layer: owned OpenSSH/Telnet clients, bounded login
  and MFA/enable exchanges, exact prompts, command-scoped confirmations, paging,
  script preflight, full responses and per-command completion receipts.
- Add Cisco IOS, Huawei and H3C profiles, validated configuration collection,
  explicit save evidence and locked private atomic backups with commit receipts.

- Preserve cancellation/Close when a completed write becomes ready at the same
  time, and include pending input in rejected-wait diagnostics.
- Reduce fragmented receive allocations by reusing private read scratch safely.
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
