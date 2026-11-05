# Migration to v0.3.0

Replace `WithEscapeAction(key, handler)` with
`WithInteractInputCases(Case{Pattern: Literal(string([]byte{key})), Handler: handler})`.
Local sequences can contain multiple bytes; other local pattern kinds are rejected.
Rename `WithInteractCases` to `WithInteractOutputCases`. `WithEscape` remains the
exit-byte option. No compatibility aliases or transition wrappers are provided.

ExpectAny now listens to all sessions after Continue. Continue resets active
relative budgets together; ContinueKeepTimer and per-session idle budgets retain
their deadlines. Equal rule groups rotate by session order. Use Result.Source,
Scope and RuleIndex to identify match origin; trace/completion metadata and
ResponseError expose the same coordinates. CompileError now also matches
ErrInvalidPattern through errors.Is.

# Migration from v0.1.x to v0.2.0

This update intentionally removes old interfaces. There are no aliases or
transition wrappers. Move callers directly to the unified Case/Session model.

| Previous API | Current API |
| --- | --- |
| Rule / NewResponder / Responder.Expect | Case{Limit, Handler}, Session.Expect with WithRules |
| Rule.Respond | Case.Handler = Then(response), or ReplyThenContinue(text) |
| ContinueWithoutReset | ContinueKeepTimer |
| WithDeclarationPriority | WithMatchOrder(Declaration) |
| WithLogger(*log.Logger) | WithLogger(*slog.Logger) |
| Nonzero Return | Return is zero; use named Action values |

Before/After rules take precedence by group. Ordinary call cases default to no
limit, reusable rules default to one per wait, and completion wins a scoped-rule
tie. WithRules validates through Expect, not a separate responder constructor.
EOF rules are supported. Rule outcomes expose Scope and RuleIndex; CaseIndex is
reserved for foreground cases. ConfigError and CompileError support errors.As;
ErrInvalidPattern supports errors.Is.

Result now exposes byte offsets and named captures. Registering Redact without an
output retains secret definitions for future Interact; matching tables are still
lazy, and Close clears the definitions. Fold selects Go leftmost-first regexp
semantics, including for imported POSIX/Longest patterns.

Replace manual child stdio attachment with Console.Start to obtain a controlling
terminal, or use spawn.Start for owned process cleanup. A successful Start closes
the parent's slave. Windows uses the dedicated conpty package. Neither process
adapter changes the core transport ownership contract.

The earlier device connector was moved to
[go-net-connector](https://github.com/gatework/go-net-connector). Historical release
behavior and benchmark evidence remain available in Git at v0.1.4.
