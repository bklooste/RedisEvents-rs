# RedisEvents-rs

[![CI](https://github.com/bklooste/RedisEvents-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/bklooste/RedisEvents-rs/actions/workflows/ci.yml)

A Rust port of [RedisEvents](https://github.com/bklooste/RedisEvents): Kafka-style messaging on
**Redis Streams**, with partitioned topics, per-key ordering, at-least-once delivery and consumer
positions stored in Redis.

It is **wire-compatible** with the C# library. A Rust producer can feed a C# consumer and the
reverse, on the same Redis, the same topics and the same consumer positions. The contract is
written down in [`wire-spec/WIRE.md`](wire-spec/WIRE.md) and enforced in CI.

> **Status: 0.2 release candidate.** All crates and both sample services are implemented and
> tested, and interop with the C# library runs in CI (RedisEvents NuGet >= 0.2.37, codec v1). The
> crates are prepared for crates.io; see [`CHANGELOG.md`](CHANGELOG.md). Only the work-queue
> delivery mode is deferred. The plan is in [`docs/PLAN.md`](docs/PLAN.md).

| Crate | Purpose |
| --- | --- |
| `redisevents` | core: wire, publishers, consumer host, positions, admin, health (features `otel`, `testing`, `tls-rustls`) |
| `redisevents-msgpack` | MessagePack payloads with MessagePack-CSharp compatible LZ4 |
| `redisevents-projections` | typed projections and view stores |
| `redisevents-eventsourcing` | aggregates and an optimistic-concurrency repository |
| `redisevents-restate` | batched Restate workflow starts from a stream |
| `redisevents-axum` | admin endpoints and a health check |

## Quick start

Start with the **[Inventory CQRS-ES sample](samples/inventory)**, a port of the Redis variant of
[cqrs-simple-micro-service](https://github.com/bklooste/cqrs-simple-micro-service/tree/main/redis).
It has two services:

- a **write side** that saves event-sourced inventory items with optimistic concurrency and
  publishes their events;
- a **read side** that projects those events into list and detail views.

Both sides speak the C# sample's wire format and HTTP API, so either can be swapped for its C#
counterpart.

The sample's domain crate (events, aggregate rules, projections, C# wire compatibility):

```bash
cargo test -p inventory-domain
```

Both services are implemented (plan WPs E5b and E5c). They read `REDIS_URL` (default
`redis://127.0.0.1:6379/0`). The [sample README](samples/inventory/README.md) shows the routes and how to run the full stack.

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

Samples live under [`samples/`](samples). They are not published. `redisevents-test-support` is a test helper and is not published either (as the C# test projects are not). Publishing: [docs/PUBLISHING.md](docs/PUBLISHING.md).

## Interop rules worth knowing up front

- **Explicit type strings.** The C# typed API defaults the message type to the .NET full type name.
  Cross-language topics must use explicit type strings, which the Rust API always requires.
- **Set the key prefix explicitly (C# and Rust defaults differ).** `STREAMS_KEY_PREFIX` is used
  verbatim by both. Without it C# uses `<ASPNETCORE_ENVIRONMENT|DOTNET_ENVIRONMENT>:re:`, defaulting to
  `production:re:`, while Rust uses `<STREAMS_ENVIRONMENT>:re:`, defaulting to `local:re:`, and never reads
  the .NET variables. With nothing set the two silently use different namespaces. Mixed deployments
  must set `STREAMS_KEY_PREFIX` on every service (or `STREAMS_ENVIRONMENT` on Rust equal to the C#
  environment name).
- **Redis 7.4+** (for `HEXPIRE`), the same requirement as C#.

## Performance

The numbers below come from this repo's own suites, which mirror the C# ones one to one:
`crates/redisevents/tests/perf.rs` (the C# `PerfBenchmarkTests`: same configurations, counts and
body sizes) and `crates/redisevents/benches/micro.rs` (criterion, the C# `StreamsMicroBenchmarks`:
same operations and inputs). They were run on the same dev machine the C# numbers were measured on
(13th Gen Intel Core i7-13700KF, WSL2, one un-tuned `redis:8-alpine` container) - a directional
baseline, not an SLA. Run-to-run variance on shared/virtualized hardware is normal (the Rust runs
were made with other builds competing for the CPU; the ranges are three runs); re-run the suites
on your own target infrastructure for numbers you would actually size capacity against.

**Throughput** - release build, against a local Redis:

```bash
docker run -d --name perf-redis -p 6379:6379 redis:8-alpine
REDIS_URL=redis://127.0.0.1:6379 cargo test --release -p redisevents --test perf -- \
    --ignored --nocapture --test-threads=1
```

| Path | Configuration | Rust | C# |
| --- | --- | --- | --- |
| Direct publish (`XADD`, unpipelined) | 128 B body | ~6,500-6,700 msg/s | ~6,200 msg/s |
| Direct publish (`XADD`, unpipelined) | 2048 B body | ~6,000-6,300 msg/s | ~6,300 msg/s |
| Pipelined `publish_batch` | batch of 100 | ~293,000-309,000 msg/s | ~225,000 msg/s |
| Pipelined `publish_batch` | batch of 500 | ~471,000-509,000 msg/s | ~137,000 msg/s |
| Pipelined `publish_batch` | batch of 2,000 | ~525,000-570,000 msg/s | ~159,000 msg/s |
| `BufferedStreamPublisher` | batch <=500, flush <=10ms | ~484,000-494,000 msg/s | ~389,000 msg/s |
| `BufferedStreamPublisher` | batch <=2,000, flush <=25ms | ~532,000-555,000 msg/s | ~422,000 msg/s |
| End-to-end consume | 1 partition, blocking reads | ~375,000-427,000 msg/s | ~278,000 msg/s |
| End-to-end consume | 4 partitions, blocking reads | ~416,000-420,000 msg/s | ~255,000 msg/s |
| End-to-end consume | 4 partitions, 4 reader threads | ~414,000-420,000 msg/s | ~343,000 msg/s |
| End-to-end consume | 4 partitions, polling reads | ~407,000-420,000 msg/s | ~310,000 msg/s |
| End-to-end consume | 4 partitions, backpressure disabled | ~414,000-421,000 msg/s | ~502,000 msg/s |

As in C#, a single unpipelined `XADD` is bound by the Redis round trip, so it sits around 6K msg/s
whatever the body size; pipelining or buffering takes that round trip off the hot path. The
consume rows are flat at ~420K msg/s because they are bound by the client's RESP parsing, not by
the library: the suite's extra `raw_xread_floor` test reads the same 100,000 entries with bare
`XREAD COUNT 500` calls on one connection and no consumer host, and gets ~375,000-392,000 msg/s.
A profile (`perf record`) of either is dominated by redis-rs's parser. That is also why extra
reader threads do not help a co-located topic: its partitions are read by one multi-stream
`XREAD` on one socket. The one row where C# is ahead (backpressure disabled) is that ceiling.

**Low-level latency** - criterion, release build, the per-call cost every message pays:

```bash
cargo bench -p redisevents --bench micro
```

| Operation | Rust | C# |
| --- | --- | --- |
| Encode (body + type + key + correlation id + headers), headers packed per call | ~216 ns | ~125 ns |
| Encode minimal (body + type + empty key) | ~82 ns | ~41 ns |
| Decode (headers left packed) | ~164 ns | ~786 ns |
| `partition_for_key`, power-of-two partition count | ~3.2 ns | ~2.0 ns |
| `partition_for_key`, non-power-of-two partition count | ~4.1 ns | ~2.3 ns |
| `partition_for_key`, single partition | ~1.5 ns (loop overhead) | effectively 0 ns |
| `RoundRobin::next` | ~5.8 ns | ~3.7 ns |
| `StreamId::parse` (Rust has one fallible parse for C# `Parse` / `TryParse`) | ~18-19 ns | ~12 ns |
| `StreamId::format_into` (into a caller buffer; C# `TryFormat`) | ~22 ns | ~7 ns |
| `StreamId::to_string` (allocates; C# `Format`) | ~40 ns | ~16 ns |

Rust's encode builds the whole `XADD` command (redis-rs `Cmd`, presized: one allocation per
buffer), where C# returns the field array that StackExchange.Redis serializes later, so the two
encode rows are not quite like for like. Decode is cheap because the body and the packed headers
move into `Bytes` without being copied. Routing allocates nothing.

## Building

```bash
cargo build --workspace
cargo test --workspace                      # needs a C linker (build-essential, or `zig cc` as CARGO_TARGET_<triple>_LINKER) and, later, Docker or REDIS_URL
python3 scripts/check_wire_drift.py ../RedisEvents   # checks wire-spec/ against a C# checkout
```

MSRV: Rust 1.97 (stable − 2).

## License

[MIT](LICENSE)
