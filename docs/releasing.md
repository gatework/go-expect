# CI and automatic releases

A push to the repository's default branch (`master` today) runs the Linux/macOS
Go 1.26/1.27 matrix and quality checks. Only when every check and the source-package
consumer smoke pass does the same workflow publish a GitHub Release. Feature
branches and pull requests run validation and packaging without release permission.
A local commit alone does not start GitHub Actions: it must be pushed to GitHub.

## Version policy

The first version is `v0.1.0`. Each subsequent unreleased default-branch snapshot
increments the highest stable tag's patch number. For example, `v0.1.9` becomes
`v0.1.10`. Prerelease tags do not change that sequence. A version tag already on
the same commit is reused. A previously published commit is verified on rerun,
without creating another version or replacing attachments.

This is deliberate automatic patch versioning, not semantic inference from commit
messages. Before an incompatible API change after stabilization, choose an
appropriate minor/major release policy. Go major versions 2 and above require a
matching module import path; the automatic incrementer rejects that transition.

The workflow queues default-branch pushes, and the publisher also serializes
publication. GitHub orders waiting runs by queue arrival, which can differ from
commit order. If a newer descendant has already been released, a late older run
is skipped rather than assigned a higher version. Divergent history fails closed.
A push containing several commits produces one release for its final snapshot.
Queue capacity is GitHub's documented maximum of 100 pending runs.

## Gates and artifacts

- Four test jobs run formatting, module verification/tidy checks, vet, shuffled
  race tests with goleak, three fuzz smokes, benchmarks, the dialogue example and
  the independent stream/PTY consumer.
- The quality job runs pinned golangci-lint and govulncheck, actionlint and release
  regression tests. The static linters are govet, ineffassign, staticcheck and
  unused. Tool versions live in the workflow, separate from library dependencies.
- The package job archives the exact checked-out commit, embeds its SHA and
  computes SHA-256 checksums. It runs the consumer against the extracted archive.
- The publisher downloads that artifact by its immutable ID from the same run.
  It does not rebuild the source package. Uploaded files are `source.tar.gz`,
  `REVISION` and `SHA256SUMS`; this library does not ship application binaries.

The source archive has a `go-expect/` prefix and a deterministic gzip header.
`REVISION`, the archive's Git commit header and the workflow SHA must agree.
`SHA256SUMS` covers both the archive and revision file. Artifact names include the
run attempt so full reruns do not overwrite an earlier attempt's artifact.

## Publication and retries

Only the release job receives `contents: write`, using GitHub's short-lived
`GITHUB_TOKEN`; no personal access token or additional secret is needed. Checkout
credentials are not persisted. Both the workflow and publisher check the event
and default branch. Tag pushes do not trigger another run.

Publication creates a draft targeted at the tested full SHA. It uploads the exact
CI files, downloads every attachment and compares bytes before making the release
public. The published release and final tag are read back. The summary links the
release and identifies the tested commit. Go consumers install from the tag:

```sh
go get github.com/gatework/go-expect@v0.1.0
```

For a transient failure, use **Re-run failed jobs** on the original Actions run.
An incomplete draft for that commit receives only missing files; identical files
are reused. A zero-byte attachment explicitly marked `starter` by GitHub after an
interrupted upload is deleted and uploaded again, only within that matching draft.
Conflicting uploaded bytes, extra attachments, missing published assets or an
unfinished draft for a different commit stop publication. No script deletes,
clobbers or force-moves a published version. Inspect a damaged draft before
removing it manually, then rerun the original verified commit. Authentication/API
errors are failures, never interpreted as an absent release.

GitHub may require `Workflows: write` when the target commit's workflow files
differ from the current default branch. `GITHUB_TOKEN` cannot grant that extra
permission. If another push changes workflows while an older release is queued,
that older run may fail publication; inspect the job and release the latest
validated default-branch snapshot. The workflow does not request a broader token
or change the target SHA to work around this restriction.

The workflow also supports **Run workflow** on the default branch. It reruns the
full gates and either releases an unreleased snapshot or verifies the existing
release. Selecting another branch validates it without publishing.

## Local checks

```sh
bash script/ci
python3 -m unittest discover -s script/tests -v
# actionlint 1.7.12 predates GitHub's concurrency queue property:
actionlint -ignore '^unexpected key "queue" for "concurrency" section\.'
python3 script/release.py package --directory /tmp/go-expect-source
python3 script/release.py verify --directory /tmp/go-expect-source
```

Packaging reads committed Git content, not uncommitted files, and refuses an
existing output directory. `release` without `--publish` is a read-only remote
release plan (it fetches local Git refs). `--publish` is reserved for the Actions
default-branch job. Automated tests use disposable local Git repositories and a
fake GitHub backend; local success is not proof of a completed GitHub release.

## References

- [GitHub concurrency and queue ordering](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)
- [Artifact IDs and immutable uploads](https://github.com/actions/upload-artifact)
- [Go module publication and immutable versions](https://go.dev/doc/modules/publishing)
