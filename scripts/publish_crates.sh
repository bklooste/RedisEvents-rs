#!/usr/bin/env bash
# Publishes every library crate in dependency order, skipping any whose version is already on crates.io
# (idempotent re-runs). Auth: CARGO_REGISTRY_TOKEN in the environment (OIDC token or secret).
# Usage: publish_crates.sh <version>
set -euo pipefail
version=$1
crates=(redisevents redisevents-msgpack redisevents-projections redisevents-restate redisevents-axum redisevents-eventsourcing)
for c in "${crates[@]}"; do
  if curl -fsS -A "RedisEvents-rs CI" "https://crates.io/api/v1/crates/$c/$version" >/dev/null 2>&1; then
    echo "$c $version already published, skipping"
  else
    cargo publish -p "$c" --allow-dirty
  fi
done
