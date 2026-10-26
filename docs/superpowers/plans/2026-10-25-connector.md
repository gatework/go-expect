# Device connector implementation plan

> Execute inline with executing-plans; request an independent final code review.

**Goal:** Implement the selected network-device connection and operation layer.
**Architecture:** connector owns transport, authentication and command lifecycle;
expect remains the sole matching engine. Profiles supply device-specific rules.
**Tech stack:** Go 1.26+, existing PTY dependencies, local OpenSSH/Telnet.
**Spec:** ../specs/2026-10-25-connector-design.md

## Tasks

- [x] Types/config/profile validation and script parsing (`types.go`, `profile.go`).
- [x] Owned connection, authentication, command/script/mode lifecycle and bounded
  response collection (`device.go`, `response.go`, dialogue regression tests).
- [x] OpenSSH/Telnet process transport, strict host-key arguments, cancellation
  and child reaping (`transport_unix.go`, PTY tests).
- [x] Validated config, explicit save evidence, private atomic backup and receipt
  on persistence errors (`backup_unix.go`, storage tests).
- [x] Runnable device example, external consumer, documentation and full gates.
- [x] Independent review and regression fixes.

## Review focus

Repeated prompts must not reset deadlines or output budgets. Close races must
not leak a newly opened transport. Failed callbacks preserve command completion
evidence and stop scripts. Invalid later steps must prevent all earlier writes.
Failed/empty collection must preserve an existing backup. Mode changes and pager
markers must not truncate otherwise valid output.

## Decisions / evidence

- User selected an in-repository connector layer, not a full Ruby application port.
- Baseline contains uncommitted expect/release audit changes; preserve all of them.
- Native clients are executable dependencies, as in the Ruby reference; local PTY
  tests do not establish remote SSH or device interoperability.
- Added x/text v0.43.0 for NFC/case-folded backup destination locks; first selected
  v0.36.0 failed vulnerability scanning and was replaced before acceptance.
- Independent review's six findings all fixed with regressions; see
  ../../audits/2026-10-25-connector.md for gates and evidence boundaries.
