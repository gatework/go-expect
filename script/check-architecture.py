#!/usr/bin/env python3
"""Check generic stream ownership using the production Go import graph."""
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
MODULE = "github.com/gatework/go-expect"
APPLICATIONS = (
    "github.com/gatework/go-net-connector",
    "github.com/gatework/go-vxlan",
    "github.com/gatework/go-wireless",
)


def violations(packages):
    failures = []
    for package in packages:
        name = package["ImportPath"]
        local = name.removeprefix(MODULE).lstrip("/")
        # Transitive local imports prevent an internal shim from hiding reversed
        # ownership. Standard-library dependencies remain unrestricted here.
        for dependency in set(package.get("Imports", [])) | set(package.get("Deps", [])):
            if any(dependency == app or dependency.startswith(app + "/")
                   for app in APPLICATIONS):
                failures.append(f"generic stream package {local or 'expect'} imports {dependency}")
            if local == "" and dependency.startswith(MODULE + "/"):
                if not dependency.startswith(MODULE + "/internal/"):
                    failures.append(f"session imports independently owned {dependency}")
            if local.startswith("internal/redact") and (
                dependency == MODULE or dependency.startswith(MODULE + "/")
            ):
                failures.append(f"redaction algorithm imports session ownership: {dependency}")
    return sorted(set(failures))


def packages():
    env = dict(os.environ, GOWORK="off", GOFLAGS="-mod=readonly")
    stream = subprocess.check_output(
        ["go", "list", "-json", "./..."], cwd=ROOT, env=env, text=True
    )
    decoder = json.JSONDecoder()
    while stream.strip():
        package, end = decoder.raw_decode(stream.lstrip())
        yield package
        stream = stream.lstrip()[end:]


def main():
    failures = violations(packages())
    if failures:
        raise SystemExit("architecture violations:\n" + "\n".join(failures))
    print("PASS: generic stream, process and redaction ownership")


if __name__ == "__main__":
    main()
