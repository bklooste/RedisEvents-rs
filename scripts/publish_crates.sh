#!/usr/bin/env bash
# Publishes every library crate in dependency order, skipping any whose version is already on crates.io
# (idempotent re-runs). Auth: CARGO_REGISTRY_TOKEN in the environment (OIDC token or secret).
# Usage: publish_crates.sh <version>
#
# After each upload it waits until the version is in the sparse index before publishing the next crate.
# cargo's own wait gives up after 60s, but a brand-new crate's index entry has taken ~20 minutes to
# appear, and a dependent published before then fails with "no matching package named ... found".
set -euo pipefail
version=$1
crates=(redisevents redisevents-msgpack redisevents-projections redisevents-restate redisevents-axum redisevents-eventsourcing)
# The wait only matters for brand-new crates; for an existing crate the index updates within seconds.
# An OIDC trusted-publishing token lasts about 30 minutes, so a first publish (all crates new) uses the
# CARGO_REGISTRY_TOKEN secret, which does not expire mid-run (docs/PUBLISHING.md).
index_wait_secs=${INDEX_WAIT_SECS:-2700}

# Sparse index path: 1/2/3-char names have their own buckets, longer names use the first 2 and next 2 chars.
index_url() {
  local n=$1
  case ${#n} in
    1) echo "https://index.crates.io/1/$n" ;;
    2) echo "https://index.crates.io/2/$n" ;;
    3) echo "https://index.crates.io/3/${n:0:1}/$n" ;;
    *) echo "https://index.crates.io/${n:0:2}/${n:2:2}/$n" ;;
  esac
}

in_index() {
  curl -fsS "$(index_url "$1")" 2>/dev/null | grep -q "\"vers\":\"$2\""
}

wait_for_index() {
  local c=$1 waited=0
  until in_index "$c" "$version"; do
    if (( waited >= index_wait_secs )); then
      echo "::error::$c $version is not in the crates.io index after ${index_wait_secs}s; re-run to continue"
      exit 1
    fi
    sleep 15
    waited=$((waited + 15))
  done
  echo "$c $version is in the index (after ${waited}s)"
}

for c in "${crates[@]}"; do
  if curl -fsS -A "RedisEvents-rs CI" "https://crates.io/api/v1/crates/$c/$version" >/dev/null 2>&1; then
    echo "$c $version already published, skipping"
  else
    cargo publish -p "$c" --allow-dirty
  fi
  wait_for_index "$c"
done
