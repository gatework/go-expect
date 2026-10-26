# Device connector verification — 2026-10-25

User-selected scope: the device connection/operation layer in this repository.
Netdisco, TextFSM, topology and migration of the adjacent application are excluded.
Existing uncommitted Session/release audit work was preserved.

## Implemented

- Owned OpenSSH/Telnet clients over raw pollable PTYs; strict host trust and no
  automatic fallback, replay or reconnect. Custom owned stream dialers supported.
- Login credentials, passphrases and dynamic challenges; explicit enable;
  exact learned prompts and declared mode transitions.
- Command-scoped confirmations, bounded paging, cumulative response/script
  budgets, total command deadlines and cancellation.
- Script preflight, complete/partial raw response and output, command-write
  progress, completion evidence and synchronous receipt observers.
- Basic Cisco IOS, Huawei and H3C profiles, custom validators, save completion
  evidence, private locked atomic backups with separate commit/durability flags.
- Public documentation, runnable synthetic device, CI and external consumer.

## Regression evidence

Tests were added before the corresponding feature/fix and observed failing.
The independent reviewer found six issues, all addressed and re-reviewed:

1. Close now cancels an active cooperative response callback.
2. NFC/case-folded destination names share the same backup lock.
3. Config/save validation precedes observer receipts.
4. Known pager erasure is removed; residual control bytes fail built-in config
   validation rather than entering a backup. Raw bytes remain available.
5. Custom dialers receive detached challenge slices.
6. Leading horizontal whitespace in learned prompts remains significant.

Additional regressions cover rejected-lock descriptor cleanup, panic retirement,
observer cancellation, negative save sentences, partial writes with no replay,
unknown/repeated confirmation, late dial results, failure evidence and old-backup
preservation. The reviewer independently passed the connector race suite and
device example, with no remaining actionable findings.

## Acceptance

| Environment/check | Result |
| --- | --- |
| macOS arm64 Go 1.27.2, complete `script/ci` | Pass |
| Linux arm64 Go 1.27.2, complete `script/ci` | Pass |
| macOS arm64 Go 1.26.0, full race suite | Pass |
| Linux arm64 Go 1.26.0, full race suite | Pass |
| golangci-lint 2.15.0 | No issues |
| govulncheck 1.9.0 under Go 1.27.2 | No reachable vulnerabilities |
| Python release regression suite | 23 tests passed |
| Verified source archive, extracted external consumer on macOS/Linux | Pass |
| Shell syntax and working-tree diff whitespace | Pass |

Both complete gates include module verification, vet, race/goleak, existing fuzz
smokes, both examples and an independent consumer exercising stream, PTY and
connector APIs. The source-package check uses a disposable Git snapshot of the
working tree and the existing archive verifier; it does not commit this checkout.

The initial scan found [GO-2026-5970](https://pkg.go.dev/vuln/GO-2026-5970) in the
first selected x/text version. The final dependency is v0.43.0 and the final scan
is clean. The initial host Go 1.27.1 scan also reported the Windows-only
[GO-2026-6604](https://pkg.go.dev/vuln/GO-2026-6604); backup code is compiled only on
Linux/macOS. Final acceptance used Go 1.27.2, while Go 1.26.0 runs establish the
declared language/API compatibility floor. Production builds should use patched
supported toolchains.

These results establish local synthetic dialogues, actual local child-process
ownership and filesystem behavior. They do not establish remote SSH/Telnet or
real-device/firmware interoperability, performance under real networks, or a
new remote CI/release. No real equipment was contacted; no commit, push or release
was performed in the working repository.
