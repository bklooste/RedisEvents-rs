#!/usr/bin/env bash
# Sample interop matrix: write {csharp,rust} x read {csharp,rust}.
# For each pairing: compose up, own write->read check, both C# Scenarify suites, compose down.
# A pairing whose Rust image is missing is SKIP. Needs CSHARP_SAMPLE_DIR (cqrs-simple-micro-service checkout).
set -uo pipefail
cd "$(dirname "$0")"
: "${CSHARP_SAMPLE_DIR:?set CSHARP_SAMPLE_DIR to a cqrs-simple-micro-service checkout}"
CSHARP_SAMPLE_DIR=$(cd "$CSHARP_SAMPLE_DIR" && pwd); export CSHARP_SAMPLE_DIR
export RUST_COMMAND_IMAGE=${RUST_COMMAND_IMAGE:-redisevents-inventory-command:local}
export RUST_VIEWS_IMAGE=${RUST_VIEWS_IMAGE:-redisevents-inventory-views:local}
PAIRINGS=${PAIRINGS:-"csharp/csharp csharp/rust rust/csharp rust/rust"}
WRITE=http://localhost:54105 READ=http://localhost:54106
# Overrides read by Scenarify's fixture (env vars: ':' is '__' on Linux).
export SvcHttpUrl Streams__ConnectionString="127.0.0.1:6479,allowAdmin=false"
# Same key prefix for the compose services AND the Scenarify test processes below (they publish through the
# C# event store and inherit this): C# and Rust default to different prefixes when nothing is set.
export STREAMS_KEY_PREFIX=${STREAMS_KEY_PREFIX:-inventory:re:}
dc() { docker compose -f compose.yml "$@"; }

wait_http() { for _ in $(seq 60); do curl -fsS -o /dev/null "$1" 2>/dev/null && return 0; sleep 1; done; return 1; }

cross_check() {
  local name="interop-$RANDOM-$(date +%s)"
  curl -fsS -X POST "$WRITE/InventoryCommand/Add?name=$name" -o /dev/null || { echo "Add failed"; return 1; }
  for _ in $(seq 30); do
    curl -fsS "$READ/items/" 2>/dev/null | grep -q "$name" && return 0; sleep 1
  done
  echo "item $name never appeared on the read side"; return 1
}

rows=(); rc=0
for p in $PAIRINGS; do
  w=${p%/*}; r=${p#*/}; res=PASS; note=""
  for side in "$w:$RUST_COMMAND_IMAGE" "$r:$RUST_VIEWS_IMAGE"; do
    if [ "${side%%:*}" = rust ] && ! docker image inspect "${side#*:}" >/dev/null 2>&1; then
      res=SKIP; note="image ${side#*:} not built"
    fi
  done
  if [ $res = PASS ]; then
    echo "=== $p ==="
    dc --profile "$w-command" --profile "$r-views" down -v >/dev/null 2>&1
    if ! dc --profile "$w-command" --profile "$r-views" up -d --build; then res=FAIL; note="compose up"
    elif ! wait_http "$WRITE/InventoryCommand/IsAvailable" || ! wait_http "$READ/items/"; then res=FAIL; note="services not healthy"
    else
      # Order matters: the views suite asserts an exactly-one-item list, so it runs first on clean state.
      # when_create_event_then_its_in_list_view asserts the list is exactly one item, but the detail test
      # also publishes one, and xUnit's order is not fixed. So run the list test alone first, then the rest.
      V="$CSHARP_SAMPLE_DIR/redis/inventory-views/tst/SimpleCQRS.Views.IntegrationTests"
      { SvcHttpUrl="$READ/" dotnet test "$V" --filter "FullyQualifiedName~its_in_list_view" &&
        SvcHttpUrl="$READ/" dotnet test "$V" --no-build --filter "FullyQualifiedName!~its_in_list_view"; } \
        || { res=FAIL; note="$note views suite"; }
      SvcHttpUrl="$WRITE/InventoryCommand/" dotnet test "$CSHARP_SAMPLE_DIR/redis/inventory-logic/tst/SimpleCQRS.API.IntegrationTests" \
        || { res=FAIL; note="$note write suite"; }
      cross_check || { res=FAIL; note="write->read check"; }
    fi
    [ $res = FAIL ] && dc --profile "$w-command" --profile "$r-views" logs --tail 40
    dc --profile "$w-command" --profile "$r-views" down -v >/dev/null 2>&1
  fi
  [ $res = FAIL ] && rc=1
  rows+=("$(printf '| %-6s | %-6s | %-4s | %s |' "$w" "$r" "$res" "$note")")
done
echo; echo "| write  | read   | result | note |"; echo "|--------|--------|------|------|"
printf '%s\n' "${rows[@]}"
[ -n "${GITHUB_STEP_SUMMARY:-}" ] && { echo "| write | read | result | note |"; echo "|---|---|---|---|"; printf '%s\n' "${rows[@]}"; } >> "$GITHUB_STEP_SUMMARY"
exit $rc
