# Network-device connections and operations

`github.com/gatework/go-expect/connector` adds the device layer above the generic
Expect engine. It covers SSH/Telnet client ownership, login/MFA, explicit enable,
command and script execution, confirmations, paging, configuration collection,
save evidence and local backups. Netdisco, TextFSM, fleet scheduling and topology
remain application responsibilities. The adjacent `go-net-connector` application
has not been migrated by this change.

## SSH connection

```go
device, err := connector.New(connector.CiscoIOS(), connector.Config{
    Host: "192.0.2.10",
    Username: os.Getenv("DEVICE_USERNAME"),
    Password: os.Getenv("DEVICE_PASSWORD"),
    EnablePassword: os.Getenv("DEVICE_ENABLE_PASSWORD"),
    KnownHosts: "/path/to/known_hosts",
})
if err != nil { return err }
defer device.Close()
if err := device.Connect(ctx); err != nil { return err }

// Explicitly request privilege escalation when the device requires it.
if _, err := device.Enable(ctx); err != nil { return err }
r, err := device.Execute(ctx, connector.Command{Text: "show version"})
if err != nil { return err }
fmt.Print(r.Output) // sensitive data; choose its destination deliberately
```

The built-in transport requires an installed `ssh` or, for explicit
`Protocol: "telnet"`, `telnet` executable. It runs argv directly, owns a raw PTY,
closes the stream and kills/reaps the owned client on Close. There is no shell
command construction. Telnet protocol negotiation belongs to the installed
client. Linux and macOS are supported; other platforms can provide `WithDialer`
for stream operations, but local Backup is unsupported there.

SSH uses strict known-host verification, disables escape commands and configuration
files (`-F none`), and never puts passwords in argv. Register the trusted host key
before connecting. `KnownHosts`, `IdentityFile`, an available SSH agent and
`KeyPassphrase` support explicit key authentication. No automatic host-key
acceptance/replacement, legacy algorithm downgrade or SSH-to-Telnet fallback is
performed. See the [OpenSSH command manual](https://man.openbsd.org/ssh.1) and
[configuration manual](https://man.openbsd.org/ssh_config.5) for client semantics.
Disabling client configuration also excludes implicit proxies and local commands;
use an explicitly owned custom `Dialer` for other connection policies.

## Interactions and mode changes

```go
commands := []connector.Command{
    {Text: "show interfaces"}, // profile paging applies automatically
    {
        Text: "apply selected-change",
        Interactions: []connector.Interaction{{
            Pattern: `Apply selected-change\? \[y/N\]`,
            Reply: "y\r",
            Limit: 1,
        }},
    },
}
result, err := device.ExecuteScript(ctx, commands)
// result.Steps retains previous completions and the failed attempt, if any.
```

Unknown confirmations receive no response and eventually time out. Rules use
Go regex syntax. Limit zero means **one reply**, not unlimited. Reply bytes are
verbatim: include the appropriate line ending. `Respond(ctx, expect.Result)`
supports dynamic replies/captures and overrides Reply. `Sensitive` registers a
reply before sending it. `Config.Challenges` supplies explicit login/MFA rules.
Callbacks must cooperate with cancellation and return write/response failures.

Login learns a complete prompt, including significant leading spaces. Subsequent
commands match that exact prompt line; a stray `#` or another device-like line
does not end the response. A mode-changing command must supply `Command.Prompt`,
for example `(?:\A|[\r\n])router\(config\)#[ \t]*\z`. Successful completion updates
the current prompt for subsequent commands. Custom patterns must describe the
entire completion prompt, including their line boundary. As with any streaming
Expect regex, an exact prompt line inside command data can be ambiguous; choose
profile rules suited to the device and operation.

`Raw` preserves received bytes across all intermediate matches, including prompt
and page markers. `Output` removes noncaptured interaction markers and the final
prompt, normalizes CRLF, removes an exact first-line command echo, and handles
common immediate pager backspace/erase-line sequences. It is not a general terminal
emulator. Other control sequences remain explicit; built-in configuration
validators reject them instead of persisting corrupted configuration text.

## Deadlines, ownership and evidence

Defaults are 30 seconds for login and each command, 10 seconds per write, 8 MiB
raw response per command and 32 MiB per script. Zero-valued configuration fields
select these finite defaults. Every prompt/reply shares the command's original
deadline and cumulative output budget. A parent context can bound the whole
script; pagination does not extend it. Returned partial raw output is capped at
the configured budget, and ErrOutputLimit explicitly identifies truncation.

New validates/snapshots configuration and profiles without opening a connection.
ParseScript accepts LF/CRLF and full-line `#` comments. ExecuteScript validates all
commands before sending the first. One operation owns the Device, including an
entire script/backup; concurrent or nested operations return ErrBusy. Close may
interrupt an active operation, including its cooperative response callback. A
custom Dialer must honor its context and return a stream whose Close interrupts
both Read and Write; a late returned stream is reclaimed by Connect.

An in-flight failure retires the session, including deadline, partial write,
device error, limit, validation failure, observer failure or callback panic.
Panics/Goexit propagate after cleanup. Construct a new Device for an explicit
reconnection; commands are never replayed. A preflight rejection does not retire
a healthy device. Close during custom dialing cancels it; the Connect caller
must still join that call before considering its custom dialer fully stopped.

Each Receipt carries command-write progress, raw/output text, prompt, duration,
completion and error. Complete means a completion prompt was seen; it can coexist
with an error. A successful prompt does not prove a configuration change is
correct or persistent. `WithReceipt` runs synchronously after command/profile
validation; its error stops subsequent steps while preserving completion evidence.
Observers must return promptly and must not start nested device operations.
Raw fields are sensitive; default formatting and JSON for Config, Command,
Receipt and Configuration expose no payload. Errors wrap original causes for
explicit `errors.Is/As`; their displayed messages omit underlying details.

## Configuration and backup

```go
configuration, err := device.RunningConfig(ctx)
if err != nil { return err }
_ = configuration.Text

backup, err := device.Backup(ctx, "/private/existing/directory/router.cfg")
if err != nil {
    // A directory-sync failure can follow a completed atomic replacement.
    // backup.Committed distinguishes that state; do not blindly retry commands.
    return err
}
fmt.Println(backup.SHA256, backup.Bytes, backup.Durable)

// Separate, explicit operation: persist configuration on the device.
_, err = device.SaveConfig(ctx)
```

CiscoIOS, Huawei and H3C are basic profiles, not a promise of all firmware modes.
Their collection requires nonempty configuration ending in `end`/`return` and no
unhandled terminal controls. Custom Profile.ValidateConfig must establish the
appropriate completeness and cleanup rules. SaveConfig requires both a final
prompt and the profile's SaveSuccess expression. It is device-reported evidence,
not a flash-media durability test. No implicit save, commit or rollback occurs.

Backup holds a nonblocking sidecar lock before collection, writes a 0600 temporary
file, syncs it, atomically renames, then syncs the directory. It rejects symbolic
link/nonregular destinations, preserves old bytes on failed collection/pre-rename
failure and returns Committed/Durable flags for later errors. The parent directory
must already exist and be trusted. Stable lock files are intentionally retained.
NFC/case-folded names share a lock to cover macOS filename aliases; this deliberately
serializes some distinct names on case-sensitive filesystems. Uncooperative external
writers are not constrained by the advisory lock. Custom validators are trusted
code and may inspect raw data through separately captured application state.

## Verification

`go run ./examples/device` demonstrates password login, paging, confirmation and
receipts with a local simulated device. `bash script/ci` includes that example,
race/goleak tests, actual local PTY child cleanup, filesystem regressions and an
independent module consuming connector. See [connector verification](audits/2026-10-25-connector.md)
for current evidence. Synthetic tests do not establish remote device compatibility.
