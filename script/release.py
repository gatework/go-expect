#!/usr/bin/env python3
"""Build, verify and publish the tested source snapshot without replacing releases."""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile

ASSETS = ("source.tar.gz", "REVISION", "SHA256SUMS")
VERSION = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")


def run(*args, data=None):
    return subprocess.run(args, input=data, stdout=subprocess.PIPE, check=True).stdout


def git(*args):
    return run("git", *args).decode().strip()


def package(directory, sha):
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("expected a full commit SHA")
    directory.mkdir(parents=True, exist_ok=False)
    archive = run("git", "-c", "tar.umask=022", "archive", "--format=tar", "--prefix=go-expect/", sha)
    with (directory / "source.tar.gz").open("wb") as output:
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, mtime=0) as compressed:
            compressed.write(archive)
    (directory / "REVISION").write_text(sha + "\n")
    (directory / "SHA256SUMS").write_text("".join(
        f"{hashlib.sha256((directory / name).read_bytes()).hexdigest()}  {name}\n"
        for name in ASSETS[:2]))
    verify(directory, sha)


def verify(directory, sha):
    if set(p.name for p in directory.iterdir()) != set(ASSETS):
        raise ValueError("missing or unexpected release assets")
    if (directory / "REVISION").read_text() != sha + "\n":
        raise ValueError("artifact revision does not match the tested commit")
    expected = "".join(
        f"{hashlib.sha256((directory / name).read_bytes()).hexdigest()}  {name}\n"
        for name in ASSETS[:2])
    if (directory / "SHA256SUMS").read_text() != expected:
        raise ValueError("artifact checksum mismatch")
    archive = gzip.decompress((directory / "source.tar.gz").read_bytes())
    actual = run("git", "get-tar-commit-id", data=archive).decode().strip()
    if actual != sha:
        raise ValueError("archive commit does not match the tested commit")
    verify_tree(archive, sha)


def verify_tree(archive, sha):
    """Check actual files against Git, not just a self-declared archive header."""
    expected = {}
    directories = {"go-expect"}
    for entry in run("git", "ls-tree", "-rz", "--full-tree", sha).split(b"\0"):
        if not entry:
            continue
        metadata, path = entry.split(b"\t", 1)
        mode, kind, oid = metadata.decode("ascii").split()
        if kind != "blob" or mode not in ("100644", "100755", "120000"):
            raise ValueError("source package requires ordinary tracked files or symlinks")
        name = "go-expect/" + os.fsdecode(path)
        expected[name] = (mode, oid)
        parts = name.split("/")
        directories.update("/".join(parts[:n]) for n in range(1, len(parts)))
    seen = set()
    files = set()
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as source:
        for member in source:
            if member.name in seen:
                raise ValueError("duplicate archive member")
            seen.add(member.name)
            if member.isdir():
                if member.name not in directories or member.mode not in (0o755, 0o775):
                    raise ValueError("unexpected archive directory or permissions")
                continue
            if member.name not in expected:
                raise ValueError("unexpected archive path")
            mode, oid = expected[member.name]
            if mode == "120000":
                if not member.issym() or member.mode != 0o777:
                    raise ValueError("archive symlink type mismatch")
                content = os.fsencode(member.linkname)
            else:
                allowed = (0o755, 0o775) if mode == "100755" else (0o644, 0o664)
                if not member.isfile() or member.mode not in allowed:
                    raise ValueError("archive file type or executable mode mismatch")
                content = source.extractfile(member).read()
            if content != run("git", "cat-file", "blob", oid):
                raise ValueError("archive contents differ from the tested Git tree")
            files.add(member.name)
    if files != set(expected):
        raise ValueError("archive is missing tracked source files")


def version_key(tag):
    match = VERSION.fullmatch(tag)
    if match is None:
        raise ValueError(f"invalid stable version: {tag}")
    return tuple(map(int, match.groups()))


def select_version(sha, tags, releases, ancestor):
    """Reuse a commit's version; reserve drafts; reject divergent publication."""
    stable = {tag: commit for tag, commit in tags.items() if VERSION.fullmatch(tag)}
    existing = [r for r in releases if VERSION.fullmatch(r["tag_name"]) and
                (stable.get(r["tag_name"]) == sha or
                 (r["draft"] and r["target_commitish"] == sha))]
    if len(existing) > 1:
        raise ValueError("multiple releases refer to the same commit")
    if existing:
        target = stable.get(existing[0]["tag_name"])
        if target is not None and target != sha:
            raise ValueError("existing tag contradicts the draft target")
        if not existing[0]["draft"]:
            return existing[0]["tag_name"], existing[0]
    latest = max(stable, key=version_key) if stable else None
    if latest is not None and stable[latest] != sha:
        prior = stable[latest]
        if ancestor(sha, prior):
            return None, None  # A newer snapshot has already been published.
        if not ancestor(prior, sha):
            raise ValueError("release history diverges from the candidate commit")
    if existing:
        return existing[0]["tag_name"], existing[0]
    for release in releases:
        if release["draft"] and VERSION.fullmatch(release["tag_name"]):
            raise ValueError("finish the existing draft before publishing another commit")
    same = [tag for tag, commit in stable.items() if commit == sha]
    if len(same) > 1:
        raise ValueError("multiple stable tags refer to the same commit")
    if same:
        return same[0], None
    if stable:
        major, minor, patch = version_key(latest)
        if major >= 2:
            raise ValueError("major versions >= 2 require a matching Go module path")
        return f"v{major}.{minor}.{patch + 1}", None
    return "v0.1.0", None


class GitHub:
    def __init__(self, repository):
        self.repository = repository
        self.root = f"repos/{repository}"

    def api(self, path, payload=None):
        args = ["gh", "api", f"{self.root}/{path}" if path else self.root]
        data = None
        if payload is not None:
            args += ["--method", "PATCH" if path.startswith("releases/") else "POST", "--input", "-"]
            data = json.dumps(payload).encode()
        return json.loads(run(*args, data=data))

    def releases(self):
        pages = json.loads(run("gh", "api", "--paginate", "--slurp", f"{self.root}/releases?per_page=100"))
        return [release for page in pages for release in page]

    def assets(self, release):
        pages = json.loads(run("gh", "api", "--paginate", "--slurp", f"{self.root}/releases/{release['id']}/assets?per_page=100"))
        return [asset for page in pages for asset in page]

    def download(self, asset):
        return run("gh", "api", "-H", "Accept: application/octet-stream", f"{self.root}/releases/assets/{asset['id']}")

    def upload(self, tag, path):
        run("gh", "release", "upload", tag, str(path), "--repo", self.repository)

    def delete_asset(self, asset):
        run("gh", "api", "--method", "DELETE", f"{self.root}/releases/assets/{asset['id']}")

    def tag_commit(self, tag):
        refs = self.api(f"git/matching-refs/tags/{tag}")
        exact = [ref for ref in refs if ref["ref"] == f"refs/tags/{tag}"]
        if not exact:
            return None
        obj = exact[0]["object"]
        seen = set()
        while obj["type"] == "tag":
            if obj["sha"] in seen or len(seen) >= 10:
                raise ValueError("invalid annotated tag chain")
            seen.add(obj["sha"])
            obj = self.api(f"git/tags/{obj['sha']}")["object"]
        if obj["type"] != "commit":
            raise ValueError("release tag does not identify a commit")
        return obj["sha"]


def publish(api, tag, sha, directory, existing, notes):
    verify(directory, sha)
    if existing is not None and existing["prerelease"]:
        raise ValueError("automatic stable publication cannot reuse a prerelease")
    def verify_tag():
        commit = api.tag_commit(tag)
        if commit is not None and commit != sha:
            raise ValueError("remote tag does not match the tested commit")
        if existing is not None and not existing["draft"] and commit is None:
            raise ValueError("published release has no tag")
    verify_tag()
    if existing is None:
        existing = api.api("releases", {"tag_name": tag, "target_commitish": sha,
                           "name": tag, "body": notes, "draft": True, "prerelease": False})
    if existing["draft"] and existing["target_commitish"] != sha:
        raise ValueError("draft target differs from the tested commit")
    remote = {asset["name"]: asset for asset in api.assets(existing)}
    if set(remote) - set(ASSETS):
        raise ValueError("unexpected remote assets; refusing to replace a release")
    for name in ASSETS:
        path = directory / name
        if (name in remote and remote[name].get("state") == "starter" and
                remote[name].get("size") == 0 and existing["draft"]):
            # GitHub may leave an unfinished asset after an upload 502. Only
            # this draft's explicitly unfinished upload may be removed.
            api.delete_asset(remote.pop(name))
        if name not in remote:
            if not existing["draft"]:
                raise ValueError("published release is missing assets")
            api.upload(tag, path)
        elif api.download(remote[name]) != path.read_bytes():
            raise ValueError("remote asset differs; refusing to overwrite it")
    # Re-read every attachment, including uploads from this attempt, before publishing.
    assets = api.assets(existing)
    if len(assets) != len(ASSETS) or {a["name"] for a in assets} != set(ASSETS):
        raise ValueError("remote asset inventory mismatch")
    for asset in assets:
        if api.download(asset) != (directory / asset["name"]).read_bytes():
            raise ValueError("remote asset read-back mismatch")
    if existing["draft"]:
        verify_tag()
        api.api(f"releases/{existing['id']}", {"draft": False, "make_latest": "true"})
    final = api.api(f"releases/{existing['id']}")
    if final["draft"] or final["prerelease"] or final["tag_name"] != tag:
        raise ValueError("release publication read-back failed")
    return final["html_url"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["package", "verify", "release"])
    parser.add_argument("--directory", type=Path, default=Path("dist"))
    parser.add_argument("--sha", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--publish", action="store_true", help="otherwise release is a read-only plan")
    args = parser.parse_args()
    sha = args.sha or git("rev-parse", "HEAD")
    if args.command == "package":
        package(args.directory, sha)
        return
    verify(args.directory, sha)
    if args.command == "verify":
        return
    repository = os.environ["GITHUB_REPOSITORY"]
    api = GitHub(repository)
    repo = api.api("")
    branch = repo["default_branch"]
    if args.publish and (os.environ.get("GITHUB_ACTIONS") != "true" or
                         os.environ.get("GITHUB_EVENT_NAME") not in ("push", "workflow_dispatch") or
                         os.environ.get("GITHUB_REF") != f"refs/heads/{branch}"):
        raise ValueError("publishing requires a default-branch Actions run")
    git("fetch", "origin", f"refs/heads/{branch}:refs/remotes/origin/{branch}", "--tags")
    def ancestor(a, b):
        result = subprocess.run(["git", "merge-base", "--is-ancestor", a, b])
        if result.returncode not in (0, 1):
            raise ValueError("cannot establish commit ancestry")
        return result.returncode == 0
    if not ancestor(sha, f"origin/{branch}"):
        raise ValueError("tested commit is not on the default branch")
    tags = {tag: git("rev-parse", f"{tag}^{{commit}}") for tag in git("tag", "--list", "v*").splitlines()}
    tag, existing = select_version(sha, tags, api.releases(), ancestor)
    if tag is None:
        print("Skipped: a newer snapshot has already been released.")
        return
    print(f"Release plan: {tag} -> {sha}")
    if not args.publish:
        return
    notes = f"Tested source commit: `{sha}`\n\nInstall: `go get github.com/gatework/go-expect@{tag}`\n\n"
    stable = [t for t in tags if VERSION.fullmatch(t) and tags[t] != sha and ancestor(tags[t], sha)]
    start = tags[max(stable, key=version_key)] if stable else None
    notes += "Changes:\n\n" + git("log", "--format=- %h %s", f"{start}..{sha}" if start else "-1", *([] if start else [sha])) + "\n"
    url = publish(api, tag, sha, args.directory, existing, notes)
    git("fetch", "origin", f"refs/tags/{tag}:refs/tags/{tag}")
    if git("rev-parse", f"{tag}^{{commit}}") != sha:
        raise ValueError("published tag does not resolve to the tested commit")
    print(url)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f"Published [{tag}]({url}) from `{sha}`; all assets verified byte-for-byte.\n")


if __name__ == "__main__":
    main()
