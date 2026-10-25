# RedisEvents-rs

[![CI](https://github.com/bklooste/RedisEvents-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/bklooste/RedisEvents-rs/actions/workflows/ci.yml)

A Rust port of [RedisEvents](https://github.com/bklooste/RedisEvents): Kafka-style messaging on
**Redis Streams**, with partitioned topics, per-key ordering, at-least-once delivery and consumer
positions stored in Redis.

It is **wire-compatible** with the C# library. A Rust producer can feed a C# consumer and the
reverse, on the same Redis, the same topics and the same consumer positions. The contract is
written down in [`wire-spec/WIRE.md`](wire-spec/WIRE.md) and enforced in CI.

> **Status: core library and event-sourcing crate merged.** The sample services are still in progress.
> The roadmap is in [`docs/PLAN.md`](docs/PLAN.md).

## Quick start

Start with the **[Inventory CQRS-ES sample](samples/inventory)**, a port of the Redis variant of
[cqrs-simple-micro-service](https://github.com/bklooste/cqrs-simple-micro-service/tree/main/redis).
It has two services:

- a **write side** that saves event-sourced inventory items with optimistic concurrency and
  publishes their events;
- a **read side** that projects those events into list and detail views.

Both sides speak the C# sample's wire format and HTTP API, so either can be swapped for its C#
counterpart.

The sample's domain crate (events, aggregate rules, projections, C# wire compatibility) is done:

```bash
cargo test -p inventory-domain
```

The two services follow as the library crates they use land (plan WPs E5b and E5c). The
[sample README](samples/inventory/README.md) shows the routes and how to run the full stack.

### Consuming typed messages

A consumer host is built from a builder: pick a topic, register a handler, and spawn. A typed
handler receives decoded messages (JSON by default; `typed_handler_with::<MessagePack, _, _>` in
`redisevents-msgpack` for MessagePack).

```rust
use redisevents::consumer::{HandleResult, PartitionContext, Streams, TypedHandler};
use redisevents::{Connection, Message, TypedMsg};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    order_id: String,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "Shop.OrderPlaced";
}

struct Shipping;

impl TypedHandler<OrderPlaced> for Shipping {
    async fn handle(&self, msg: &TypedMsg<OrderPlaced>, _ctx: &PartitionContext) -> HandleResult {
        println!("ship {}", msg.value.order_id);
        Ok(())
    }
}

async fn run() -> redisevents::Result<()> {
    let conn = Connection::connect("redis://localhost:6379").await?;
    let host = Streams::builder(conn)
        .topic("orders")
        .typed_handler::<OrderPlaced, _>(Shipping)
        .spawn()
        .await?;
    // ... until shutdown
    host.shutdown().await
}
```

## Crates

| Crate | Ports | Purpose |
| --- | --- | --- |
| `redisevents` | `RedisEvents`, `RedisEvents.Tracing` | core: wire codec, producers, consumers, positions, ownership, outbox, admin |
| `redisevents-msgpack` | `RedisEvents.MessagePack` | MessagePack typed publish/consume, LZ4 compatible with MessagePack-CSharp |
| `redisevents-projections` | `RedisEvents.Projections` | typed projections and view stores |
| `redisevents-eventsourcing` | `RedisEvents.EventSourcing` | event-sourced aggregates with optimistic concurrency |
| `redisevents-restate` | `RedisEvents.Restate` | batched Restate workflow starts |
| `redisevents-axum` | `RedisEvents.Web`, `RedisEvents.HealthChecks` | admin endpoints and health checks |

Samples live under [`samples/`](samples). They are not published.

## Interop rules worth knowing up front

- **Explicit type strings.** The C# typed API defaults the message type to the .NET full type name.
  Cross-language topics must use explicit type strings, which the Rust API always requires.
- **Same environment variables.** Keys are prefixed from `STREAMS_KEY_PREFIX`, or else from
  `ASPNETCORE_ENVIRONMENT` / `DOTNET_ENVIRONMENT`. The Rust side reads the same variables, so set
  them the same way for both services. The Rust side also reads `STREAMS_ENVIRONMENT` after those
  two (C# does not), so do not set it on a service that must interoperate.
- **Redis 7.4+** (for `HEXPIRE`), the same requirement as C#.

## Building

```bash
cargo build --workspace
cargo test --workspace                      # needs a C linker (build-essential, or `zig cc` as CARGO_TARGET_<triple>_LINKER) and, later, Docker or REDIS_URL
python3 scripts/check_wire_drift.py ../RedisEvents   # checks wire-spec/ against a C# checkout
```

MSRV: Rust 1.97 (stable − 2).

## License

[MIT](LICENSE)
