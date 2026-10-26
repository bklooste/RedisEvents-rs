#!/usr/bin/env bash
# Mirrors the C# repo's Nerdbank.GitVersioning scheme: version.json holds "major.minor"; the patch is the
# git height (commits since version.json last changed). Rewrites the workspace version and the internal
# path-dependency requirements in place (CI does this on a throwaway checkout; never commit the result)
# and prints the version. Needs full history (fetch-depth: 0).
set -euo pipefail
cd "$(dirname "$0")/.."
mm=$(sed -n 's/.*"version": *"\([0-9]*\.[0-9]*\)".*/\1/p' version.json)
base=$(git log -1 --format=%H -- version.json)
height=$(git rev-list --count "$base..HEAD")
version="$mm.$height"
sed -i "0,/^version = \".*\"/s//version = \"$version\"/" Cargo.toml
sed -i -E "s/(path = \"\.\.\/redisevents[a-z-]*\", version = )\"[^\"]*\"/\1\"$mm.0\"/" crates/*/Cargo.toml
echo "$version"
