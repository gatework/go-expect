#!/usr/bin/env python3
"""Keep the README example identical to its compiled Go source."""
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]
readme = root / "README.md"
start, end = "<!-- example:start -->", "<!-- example:end -->"
text = readme.read_text()
a, remainder = text.split(start, 1)
_, b = remainder.split(end, 1)
example = (root / "example_test.go").read_text()
expected = a + start + "\n```go\n" + example + "```\n" + end + b
if "--write" in sys.argv:
    readme.write_text(expected)
elif expected != text:
    raise SystemExit("README example is stale: python3 script/readme.py --write")
