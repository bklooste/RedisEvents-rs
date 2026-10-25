# RedisEvents-rs

[![CI](https://github.com/bklooste/RedisEvents-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/bklooste/RedisEvents-rs/actions/workflows/ci.yml)

A Rust port of [RedisEvents](https://github.com/bklooste/RedisEvents): Kafka-style messaging on
**Redis Streams**, with partitioned topics, per-key ordering, at-least-once delivery and consumer
positions stored in Redis.

It is **wire-compatible** with the C# library. A Rust producer can feed a C# consumer and the
reverse, on the same Redis, the same topics and the same consumer positions. The contract is
written down in [`wire-spec/WIRE.md`](wire-spec/WIRE.md) and enforced in CI.

> **Status: scaffolding.** Nothing is usable yet. The roadmap is in [`docs/PLAN.md`](docs/PLAN.md).

## Crates

| Crate | Ports | Purpose |
| --- | --- | --- |
| `redisevents` | `RedisEvents`, `RedisEvents.Tracing` | core: wire codec, producers, consumers, positions, ownership, outbox, admin |
| `redisevents-msgpack` | `RedisEvents.MessagePack` | MessagePack typed publish/consume, LZ4 compatible with MessagePack-CSharp |
| `redisevents-projections` | `RedisEvents.Projections` | typed projections and view stores |
| `redisevents-eventsourcing` | `RedisEvents.EventSourcing` | event-sourced aggregates with optimistic concurrency |
| `redisevents-restate` | `RedisEvents.Restate` | batched Restate workflow starts |
| `redisevents-axum` | `RedisEvents.Web`, `RedisEvents.HealthChecks` | admin endpoints and health checks |

## Interop rules worth knowing up front

- **Explicit type strings.** The C# typed API defaults the message type to the .NET full type name.
  Cross-language topics must use explicit type strings, which the Rust API always requires.
- **Same environment variables.** Keys are prefixed from `STREAMS_KEY_PREFIX`, or else from
  `ASPNETCORE_ENVIRONMENT` / `DOTNET_ENVIRONMENT`. The Rust side reads the same variables, so set
  them the same way for both services.
- **Redis 7.4+** (for `HEXPIRE`), the same requirement as C#.

## Building

```bash
cargo build --workspace
cargo test --workspace                      # needs a C linker (build-essential) and, later, Docker or REDIS_URL
python3 scripts/check_wire_drift.py ../RedisEvents   # checks wire-spec/ against a C# checkout
```

MSRV: Rust 1.97 (stable − 2).

## License

[MIT](LICENSE)
