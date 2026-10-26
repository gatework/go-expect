# Device connector

User-selected scope: add the device connection and operation layer to this
repository. Netdisco inventories, TextFSM and topology remain higher layers.

## Approach

Keep the generic expect package independent. Add `connector` using its owned
Session and matcher. Use OpenSSH and Telnet clients over an owned raw PTY, as
the reference Ruby connector does. Native SSH/Telnet implementations would add
protocol and trust-management responsibilities; importing the adjacent Go
application would introduce unrelated application dependencies and outdated
Expect APIs. Both reference projects are read-only.

## Contract

- Validate and snapshot profiles, commands and connection options before I/O.
- OpenSSH uses strict known-host verification; credentials never enter argv.
  Telnet is explicit. No downgrade, trust replacement or command replay.
- Own the client process and stream. Close interrupts active I/O and reaps the
  child. All login/command work has finite deadlines. Callbacks cooperate.
- One operation owns the device at a time, including entire scripts and backup
  collection. Concurrent/nested operations fail busy; Close may interrupt them.
- Learn a complete prompt at login, then match that exact line. Mode transitions
  must declare a new completion pattern; matching it updates the current prompt.
- Preserve raw response and cleaned output across every pagination/confirmation
  segment. Bound cumulative output, response counts and script output.
- Confirmations are command-scoped. Passwords, enable authentication and paging
  have explicit finite counts. Never guess a reply to an unknown prompt.
- Return receipts for completed and failed commands, including bytes written,
  response completion, output, elapsed time and error. A prompt is completion
  evidence, not proof of business state or persistence.
- Preserve partial results and stop on failure. After an in-flight operation
  fails, retire the session rather than reuse ambiguous pending input.
- RunningConfig requires a profile validator. SaveConfig requires explicit
  success evidence. Backup uses a private same-directory temporary file,
  fsync, rename and directory fsync, under a nonblocking path lock. Failure
  after rename reports the committed receipt; failure before it preserves old
  bytes. No directory creation or automatic reconnection.
- Provide Cisco IOS, Huawei and H3C profiles and custom profile support; tests
  establish synthetic behavior only, not every vendor/firmware combination.

## Verification

Test local duplex dialogues, actual owned PTY child lifecycle, cancellation,
script preflight/no replay, split paging/confirmation, cumulative limits, error
and secret formatting, strict SSH argv, config rejection and backup replacement.
Run race tests, lint, existing CI, Linux/macOS and standalone consumer checks.
No commit, push, release or real-device write is part of this request.
