# go-expect development

Follow [the gatework Go engineering standard](../go-standards/STANDARD.md) (go-standards/STANDARD.md); this file adds only what is specific to go-expect.

Use `golang-how-to` for Go implementation, review and debugging.

- Before editing, inspect Git status and linked worktrees; preserve concurrent
  changes and stage only the intended paths.
- Read [architecture](docs/architecture.md) before changing package ownership,
  session lifecycle, matching or diagnostics. Session owns its transport and
  workers; diagnostic writers are borrowed. Device policy belongs to callers.
- Preserve accepted bytes and partial write evidence when later deadlines,
  callbacks, diagnostics or cleanup fail. Cancellation retires I/O; automatic
  replay cannot establish delivery.
- Public API changes need caller inspection, ownership/error documentation and
  external-consumer checks. Explicit raw access stays separate from safe
  diagnostic formatting and redaction.
- Run affected tests first. Before delivery, follow
  [verification](docs/verification.md) and run `make check`.
  Go versions and lint rules come from go.mod and the shared standard files.
- Use synthetic streams, local PTYs and child processes for normal tests.
  Cross-compilation, local runtime checks and real-device evidence are distinct.
- Track agent guidance, architecture and migration documentation alongside code.
  Publication and deployment require explicit task authorization.
