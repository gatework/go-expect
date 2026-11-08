# Write lifecycle refinement

The subsequent module-organization pass preserves this public interface and all
write lifecycle fixes. Session construction and retirement, the input pump,
transport writing, and diagnostics now live with their respective private state.
Session options have one definition location. See [module organization](architecture.md)
for the ownership map; no caller migration is required for these file moves.

The public Go interface is unchanged. This change strengthens the owned transport
contract for network automation callers: a failed deadline operation is a failed
session lifecycle, not permission to retry a command.

Custom transports that intentionally do not support write deadlines should omit
`SetWriteDeadline`, or return an error wrapping `os.ErrNoDeadline` or
`errors.ErrUnsupported`. An arbitrary setter error now closes the session and
returns the cause without entering `Write`.
Joining an unsupported sentinel with another failure does not permit fallback;
every cause must describe an unsupported deadline.

Cancellation before `Write` still leaves the session usable when its deadline can
be cleared. Failed clearing retires the session. After write entry, cancellation,
deadline wake failure, clearing failure, and cleanup errors are retained alongside
the exact accepted byte count in `WriteError`. Callers must not infer device
execution or replay permission from that count.

Clearing a transport that is already closed remains harmless, preserving the
existing ability to drain a peer's final response. A joined closed-transport error
with an additional failure is not suppressed. A subsequent send still fails
before writing to the closed transport.

Internally, interruption is separate from joining workers. A cancellation callback
can therefore close a blocked transport when its deadline setter fails, without
waiting for the write whose completion depends on that callback. Public `Close`
still joins all owned work, flushes diagnostics, and retains stable cleanup errors.
Write completion and deadline-cleanup completion are tracked separately, so a
foreground setter panic or `runtime.Goexit` cannot leave the session reusable.
An abnormal setter exit in the cancellation callback becomes `CallbackError`,
without its panic payload, and interrupts the blocked write before joining it.

Validation uses deterministic `testing/synctest` cancellation and partial-write
regressions, existing race/leak tests, the repository CI script, and serialized-send
benchmarks. The four-repository workspace is local integration evidence; it does
not establish adoption of a new published module or real-device acceptance.
