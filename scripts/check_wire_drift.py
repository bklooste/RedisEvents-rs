#!/usr/bin/env python3
"""Fail if the shared wire assets (Lua scripts, golden vectors) in wire-spec/ have drifted from the C# RedisEvents library.

Usage: check_wire_drift.py <path to a RedisEvents (C#) checkout>

The Lua scripts are the ownership protocol itself: a C# and a Rust instance under one consumer name
only split partitions fairly if both send the same script text, so the copy here must match the C#
raw string literals byte for byte.
"""

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS = {"ReleaseScript": "release.lua", "LeaseScript": "lease.lua"}


def extract(source: str, name: str) -> str:
    """Returns a C# raw string literal's value: the closing quotes' indentation is stripped from every line."""
    match = re.search(r'private const string ' + name + r' = """\n(.*?)\n( *)""";', source, re.S)
    if match is None:
        sys.exit(f"{name} not found; has OwnershipRegistry.cs been restructured?")
    body, indent = match.group(1), len(match.group(2))
    return "\n".join(line[indent:] if line.strip() else "" for line in body.split("\n"))


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit(__doc__)

    registry = pathlib.Path(sys.argv[1]) / "src/RedisEvents/Ownership/OwnershipRegistry.cs"
    source = registry.read_text(encoding="utf-8")

    drifted = []
    for name, file in SCRIPTS.items():
        ours = (ROOT / "wire-spec/lua" / file).read_text(encoding="utf-8")
        if ours != extract(source, name):
            drifted.append(f"wire-spec/lua/{file} differs from {name} in {registry}")

    vectors = pathlib.Path(sys.argv[1]) / "tst/vectors"
    theirs = {f.name for f in vectors.glob("*.json")}
    ours = {f.name for f in (ROOT / "wire-spec/vectors").glob("*.json")}
    for name in sorted(theirs | ours):
        if name not in ours or name not in theirs:
            drifted.append(f"vectors/{name} exists on one side only")
        elif (ROOT / "wire-spec/vectors" / name).read_bytes() != (vectors / name).read_bytes():
            drifted.append(f"wire-spec/vectors/{name} differs from {vectors / name}")

    for line in drifted:
        print(f"DRIFT: {line}", file=sys.stderr)
    if not drifted:
        print(f"wire-spec matches {registry.parents[3]}")
    return 1 if drifted else 0


if __name__ == "__main__":
    sys.exit(main())
