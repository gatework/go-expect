import importlib.util
import gzip
import io
import os
from pathlib import Path
import subprocess
import tempfile
import tarfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("release", Path(__file__).parents[1] / "release.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class VersionTests(unittest.TestCase):
    def test_repository_lookup_has_no_trailing_slash(self):
        with patch.object(release, "run", return_value=b'{"default_branch":"master"}') as request:
            release.GitHub("owner/repo").api("")
        request.assert_called_once_with("gh", "api", "repos/owner/repo", data=None)

    def test_first_and_numeric_patch(self):
        self.assertEqual(release.select_version("new", {}, [], lambda a, b: False)[0], "v0.1.0")
        self.assertEqual(release.select_version("new", {"v0.1.9": "old", "v0.1.10": "newer", "v0.2.0-rc.1": "rc"}, [], lambda a, b: b == "new")[0], "v0.1.11")

    def test_rerun_reuses_published_or_partial_draft(self):
        for draft in [False, True]:
            existing = {"tag_name": "v0.1.0", "draft": draft, "target_commitish": "sha"}
            self.assertEqual(release.select_version("sha", {} if draft else {"v0.1.0": "sha"}, [existing], lambda a, b: False), ("v0.1.0", existing))

    def test_unknown_draft_blocks_new_version(self):
        with self.assertRaises(ValueError):
            release.select_version("new", {}, [{"tag_name": "v0.1.0", "draft": True, "target_commitish": "old"}], lambda a, b: False)

    def test_draft_target_cannot_override_an_existing_tag(self):
        with self.assertRaises(ValueError):
            release.select_version("new", {"v0.1.0": "old"}, [{"tag_name": "v0.1.0", "draft": True, "target_commitish": "new"}], lambda a, b: False)

    def test_old_ci_does_not_publish_newer_version(self):
        self.assertEqual(release.select_version("old", {"v0.1.1": "new"}, [], lambda a, b: a == "old"), (None, None))

    def test_old_tag_or_draft_cannot_replace_latest(self):
        draft = {"tag_name": "v0.1.0", "draft": True, "target_commitish": "old"}
        for releases in [[], [draft]]:
            self.assertEqual(release.select_version("old", {"v0.1.0": "old", "v0.1.1": "new"}, releases, lambda a, b: a == "old"), (None, None))

    def test_divergent_history_and_unsupported_major_fail(self):
        for tag, ancestor in [("v0.1.0", lambda a, b: False), ("v2.0.0", lambda a, b: b == "new")]:
            with self.assertRaises(ValueError):
                release.select_version("new", {tag: "old"}, [], ancestor)


class FakeGitHub:
    def __init__(self):
        self.state = None
        self.files = {}
        self.uploads = 0
        self.publications = 0
        self.corrupt = False
        self.tags = {}
        self.starters = set()
        self.deletions = 0

    def api(self, path, payload=None):
        if path == "releases":
            self.state = dict(payload, id=1, html_url="https://example.invalid/release")
        elif payload is not None:
            self.publications += 1
            self.state.update(payload)
            self.tags[self.state["tag_name"]] = self.state["target_commitish"]
        return self.state.copy()

    def assets(self, existing):
        return [{"name": name, "id": name, "size": len(data), "state": "starter" if name in self.starters else "uploaded"} for name, data in self.files.items()]

    def tag_commit(self, tag):
        return self.tags.get(tag)

    def delete_asset(self, asset):
        self.deletions += 1
        del self.files[asset["name"]]
        self.starters.discard(asset["name"])

    def download(self, asset):
        return b"corrupted" if self.corrupt else self.files[asset["id"]]

    def upload(self, tag, path):
        self.uploads += 1
        self.files[path.name] = path.read_bytes()


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.previous = Path.cwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, self.previous)
        subprocess.run(["git", "init", "-q"], check=True)
        (self.root / "go.mod").write_text("module example.test/library\n\ngo 1.26.0\n")
        subprocess.run(["git", "add", "go.mod"], check=True)
        subprocess.run(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"], check=True)
        self.sha = release.git("rev-parse", "HEAD")
        self.dist = self.root / "dist"
        release.package(self.dist, self.sha)

    def test_repeatable_package_and_tampering(self):
        other = self.root / "other"
        release.package(other, self.sha)
        for name in release.ASSETS:
            self.assertEqual((self.dist / name).read_bytes(), (other / name).read_bytes())
        with self.assertRaises(ValueError):
            release.verify(self.dist, "0" * 40)
        (self.dist / "source.tar.gz").write_bytes(b"corrupt")
        with self.assertRaises(ValueError):
            release.verify(self.dist, self.sha)

    def test_foreign_archive_with_recomputed_manifest_rejected(self):
        (self.dist / "REVISION").write_text("0" * 40 + "\n")
        (self.dist / "SHA256SUMS").write_text("".join(f"{release.hashlib.sha256((self.dist / name).read_bytes()).hexdigest()}  {name}\n" for name in release.ASSETS[:2]))
        with self.assertRaises(ValueError):
            release.verify(self.dist, "0" * 40)

    def rewrite_archive(self, transform):
        archive = gzip.decompress((self.dist / "source.tar.gz").read_bytes())
        output = io.BytesIO()
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as original:
            members = [(m, original.extractfile(m).read() if m.isfile() else None) for m in original]
            with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT,
                              pax_headers=original.pax_headers) as changed:
                for member, data in transform(members):
                    if data is not None:
                        member.size = len(data)
                    changed.addfile(member, io.BytesIO(data) if data is not None else None)
        (self.dist / "source.tar.gz").write_bytes(gzip.compress(output.getvalue(), mtime=0))
        (self.dist / "SHA256SUMS").write_text("".join(f"{release.hashlib.sha256((self.dist / name).read_bytes()).hexdigest()}  {name}\n" for name in release.ASSETS[:2]))

    def test_changed_tree_with_correct_commit_header_is_rejected(self):
        self.rewrite_archive(lambda items: [(m, data.replace(b"example.test/library", b"example.test/changed") if data else data) for m, data in items])
        with self.assertRaises(ValueError):
            release.verify(self.dist, self.sha)

    def test_invalid_archive_inventory_and_modes_are_rejected(self):
        original = (self.dist / "source.tar.gz").read_bytes()
        def altered_file(items, name=None, mode=None, kind=None):
            member, data = items[-1]
            if name is not None:
                member.name = name
            if mode is not None:
                member.mode = mode
            if kind is not None:
                member.type = kind
                member.linkname = "go-expect/go.mod"
                data = None
            return items[:-1] + [(member, data)]
        mutations = {
            "duplicate": lambda items: items + items[-1:],
            "missing": lambda items: items[:-1],
            "traversal": lambda items: altered_file(items, name="go-expect/../escape"),
            "executable": lambda items: altered_file(items, mode=0o755),
            "unreadable": lambda items: altered_file(items, mode=0),
            "setuid": lambda items: altered_file(items, mode=0o4644),
            "hardlink": lambda items: altered_file(items, kind=tarfile.LNKTYPE),
        }
        for name, transform in mutations.items():
            with self.subTest(name=name):
                (self.dist / "source.tar.gz").write_bytes(original)
                self.rewrite_archive(transform)
                with self.assertRaises(ValueError):
                    release.verify(self.dist, self.sha)

    def test_unusable_directory_permissions_are_rejected(self):
        original = (self.dist / "source.tar.gz").read_bytes()
        for mode in (0, 0o4755):
            with self.subTest(mode=mode):
                def change(items):
                    items[0][0].mode = mode
                    return items
                (self.dist / "source.tar.gz").write_bytes(original)
                self.rewrite_archive(change)
                with self.assertRaises(ValueError):
                    release.verify(self.dist, self.sha)

    def test_tracked_symlink_and_executable_are_preserved(self):
        (self.root / "tool").write_text("#!/bin/sh\nexit 0\n")
        (self.root / "tool").chmod(0o755)
        os.symlink("go.mod", self.root / "module-link")
        subprocess.run(["git", "add", "tool", "module-link"], check=True)
        subprocess.run(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "tracked modes"], check=True)
        sha = release.git("rev-parse", "HEAD")
        release.package(self.root / "mode-package", sha)
        release.verify(self.root / "mode-package", sha)

    def test_publish_then_rerun_is_read_only(self):
        api = FakeGitHub()
        release.publish(api, "v0.1.0", self.sha, self.dist, None, "notes")
        self.assertFalse(api.state["draft"])
        self.assertEqual(api.uploads, 3)
        release.publish(api, "v0.1.0", self.sha, self.dist, api.state.copy(), "notes")
        self.assertEqual(api.uploads, 3)
        self.assertEqual(api.publications, 1)

    def test_incomplete_draft_resumes_without_overwrite(self):
        api = FakeGitHub()
        api.api("releases", {"tag_name": "v0.1.0", "target_commitish": self.sha, "draft": True, "prerelease": False})
        api.files["REVISION"] = (self.dist / "REVISION").read_bytes()
        release.publish(api, "v0.1.0", self.sha, self.dist, api.state.copy(), "notes")
        self.assertEqual(api.uploads, 2)
        self.assertEqual(api.publications, 1)

    def test_interrupted_starter_upload_can_resume(self):
        api = FakeGitHub()
        api.api("releases", {"tag_name": "v0.1.0", "target_commitish": self.sha, "draft": True, "prerelease": False})
        api.files["source.tar.gz"] = b""
        api.starters.add("source.tar.gz")
        release.publish(api, "v0.1.0", self.sha, self.dist, api.state.copy(), "notes")
        self.assertEqual(api.deletions, 1)
        self.assertFalse(api.state["draft"])

    def test_wrong_tag_never_uploads_or_publishes(self):
        api = FakeGitHub()
        api.tags["v0.1.0"] = "0" * 40
        with self.assertRaises(ValueError):
            release.publish(api, "v0.1.0", self.sha, self.dist, None, "notes")
        self.assertEqual(api.uploads, 0)
        self.assertEqual(api.publications, 0)

    def test_tag_moved_during_upload_is_caught_before_publication(self):
        api = FakeGitHub()
        with patch.object(api, "tag_commit", side_effect=[None, "0" * 40]):
            with self.assertRaises(ValueError):
                release.publish(api, "v0.1.0", self.sha, self.dist, None, "notes")
        self.assertEqual(api.publications, 0)

    def test_bad_remote_bytes_never_publish(self):
        api = FakeGitHub()
        api.corrupt = True
        with self.assertRaises(ValueError):
            release.publish(api, "v0.1.0", self.sha, self.dist, None, "notes")
        self.assertTrue(api.state["draft"])
        self.assertEqual(api.publications, 0)

    def test_foreign_prerelease_draft_is_not_published(self):
        api = FakeGitHub()
        api.api("releases", {"tag_name": "v0.1.0", "target_commitish": self.sha, "draft": True, "prerelease": True})
        with self.assertRaises(ValueError):
            release.publish(api, "v0.1.0", self.sha, self.dist, api.state.copy(), "notes")
        self.assertEqual(api.publications, 0)
        self.assertEqual(api.uploads, 0)

    def test_published_missing_asset_is_not_repaired(self):
        api = FakeGitHub()
        release.publish(api, "v0.1.0", self.sha, self.dist, None, "notes")
        del api.files["REVISION"]
        with self.assertRaises(ValueError):
            release.publish(api, "v0.1.0", self.sha, self.dist, api.state.copy(), "notes")
        self.assertEqual(api.uploads, 3)

    def test_api_failure_is_not_release_absence(self):
        with patch.object(release, "run", side_effect=subprocess.CalledProcessError(1, "gh")):
            with self.assertRaises(subprocess.CalledProcessError):
                release.GitHub("owner/repo").releases()


if __name__ == "__main__":
    unittest.main()
