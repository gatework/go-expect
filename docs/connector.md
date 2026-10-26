# Device-layer migration

The duplicate `github.com/gatework/go-expect/connector` package has been removed.
Use `github.com/gatework/go-net-connector/connector` and its `vendors` package for
authentication, device commands, configuration and backups. go-expect does not
import the device library, so the dependency graph remains one-way.

The three former convenience profiles are superseded by the canonical vendor
profiles. Review their command, privilege and output conventions; they are not
source-compatible aliases. Set explicit command prompts for mode transitions.
Key authentication and configurable line endings are tracked by the companion
device-library migration. Do not assume an older consumer release has migrated.

`Console.Tty()` is now `Console.TTY()`. It still returns the borrowed slave;
close the parent's copy after starting an attached child to observe EOF.
