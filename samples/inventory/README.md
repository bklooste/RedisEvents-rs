# Inventory: a CQRS + event-sourcing sample

A Rust port of the Redis variant of
[cqrs-simple-micro-service](https://github.com/bklooste/cqrs-simple-micro-service/tree/main/redis):
two small services sharing one Redis.

```
             POST /InventoryCommand/*                      GET /items, /items/{id}
                       │                                            ▲
             ┌─────────▼──────────┐                       ┌─────────┴──────────┐
             │ inventory-command  │                       │  inventory-views   │
             │ (write side)       │                       │  (read side)       │
             │ load → decide →    │                       │ project events →   │
             │ save with expected │                       │ list + detail      │
             │ version            │                       │ views              │
             └─────────┬──────────┘                       └─────────▲──────────┘
                       │ XADD state stream + topic, one MULTI/EXEC  │ partition worker
                       ▼                                            │
          {inventory}:state:es:Inventory:<id>    ──►   s:{inventory}:<p>  ──────┘
```

- **Write side.** The `Inventory` aggregate's own Redis stream is its source of truth. A command
  loads the item, decides an event, and saves it with optimistic concurrency. The same transaction
  publishes the event to the `inventory` topic.
- **Read side.** A projector consumes the topic and maintains two views: an item list and a
  per-item detail view with a running stock count. Redeliveries are ignored by stream position.

## Crates

| Crate | What | Status |
| --- | --- | --- |
| [`inventory-domain`](inventory-domain) | events (wire-compatible with C#), aggregate rules, projections as pure functions | ✅ done, tested |
| `inventory-command` | axum service: the write side on `redisevents-eventsourcing` | plan WP E5b, after E3 |
| `inventory-views` | axum service: the read side on `redisevents-projections` | plan WP E5c, after E2 |

The domain has no Redis dependency. The services only wire it into the event store, the projector
and HTTP. See [`docs/PLAN.md`](../../docs/PLAN.md) §8.

## Interop with the C# sample

The Rust services are drop-in replacements for the C# ones, so any pairing works:

| Write side | Read side |
| --- | --- |
| C# `SimpleCQRS.Redis.API` | C# `SimpleCQRS.Redis.Views` |
| C# | **Rust** `inventory-views` |
| **Rust** `inventory-command` | C# |
| **Rust** | **Rust** |

The C# sample's own Scenarify HTTP suites (`SimpleCQRS.API.IntegrationTests`,
`SimpleCQRS.Views.IntegrationTests`) are the acceptance tests. They run unchanged against the
Rust services: point `SvcHttpUrl` at the Rust service instead of the C# one.

### The shared contract

| | Value |
| --- | --- |
| Topic | `inventory` |
| Aggregate name | `Inventory`, so the state stream is `{inventory}:state:es:Inventory:<id>` |
| Partition key | the item id (GUID, lowercase hyphenated) |
| Wire types | `inventory.created`, `inventory.renamed`, `inventory.checked-in`, `inventory.removed`, `inventory.deactivated` |
| Event JSON | **PascalCase**: `{"Id":"…","Count":7}` |
| HTTP JSON | **camelCase**: `{"id":"…","name":"…"}` (ASP.NET web defaults) |

**Watch the casing.** Event bodies and HTTP responses use different casings, as they do in the C#
sample. A C# consumer that receives camelCase event JSON does not fail: it reads an empty GUID and
a count of 0. The Rust domain rejects such bodies, and
[`tests/csharp_wire.rs`](inventory-domain/tests/csharp_wire.rs) pins the exact bytes C# writes.

### HTTP API (same routes as C#)

Write side, on port `54105` in the compose stack:

| Route | Query | Result |
| --- | --- | --- |
| `POST /InventoryCommand/Add` | `name`, `id?` | 204; 400 empty name; 409 conflict |
| `POST /InventoryCommand/ChangeName` | `id`, `name`, `version` | 204; 400; 404; 409 |
| `POST /InventoryCommand/Deactivate` | `id`, `version` | 204; 400 already deactivated; 404; 409 |
| `POST /InventoryCommand/CheckIn` | `id`, `number`, `version` | 204; 400 count ≤ 0; 404; 409 |
| `POST /InventoryCommand/Remove` | `id`, `number`, `version` | 204; 400 count ≤ 0; 404; 409 |
| `GET /InventoryCommand/IsAvailable` | — | 200 `true` |

Read side, on port `54106`:

| Route | Result |
| --- | --- |
| `GET /items/` | 200, `[{"id","name"}]` |
| `GET /items/{id}` | 200 `{"id","name","currentCount","streamPosition":{"ms","seq"}}` (C# adds a computed `streamPosition.timestamp`); 404 |

### One deliberate difference

The C# views service keeps the item list in memory (`InMemoryViewStore`) but its consumer
position in Redis. After a restart the list is empty, yet the projector does not replay old
events. The Rust views service keeps both views in Redis. The HTTP responses are unchanged.

## Running it

What runs today:

```bash
cargo test -p inventory-domain
```

Interop matrix (C# write/read x Rust write/read; the Rust slots SKIP until their images exist).
It needs Docker, .NET 10 and a checkout of the C# sample:

```bash
git clone https://github.com/bklooste/cqrs-simple-micro-service ../cqrs-simple-micro-service
CSHARP_SAMPLE_DIR=../cqrs-simple-micro-service samples/inventory/matrix.sh
# one pairing:  PAIRINGS=csharp/rust ...   Rust images: RUST_COMMAND_IMAGE / RUST_VIEWS_IMAGE
```

For each pairing it starts the pair from `compose.yml`, POSTs an item to the write side and polls the
read side for it, runs both C# Scenarify suites against the pair, and prints a result table. The CI job
`sample-matrix` runs it.

When E5b and E5c land:

```bash
docker compose -f samples/inventory/compose.yml up      # Redis on 6479, command on 54105, views on 54106
curl -X POST "localhost:54105/InventoryCommand/Add?name=widget"
curl localhost:54106/items/
```
