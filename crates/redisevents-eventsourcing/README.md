# redisevents-eventsourcing

Event-sourced aggregates and an optimistic-concurrency repository on `redisevents`: `AggregateRoot`, `RedisEventRepository`, `CachedAggregate` and the execute/decide helpers. Conflicts surface as `ConcurrencyError`.

Part of [RedisEvents-rs](https://github.com/bklooste/RedisEvents-rs), the Rust port of the C# `RedisEvents` library. It is wire-compatible with the C# packages (RedisEvents NuGet >= 0.2.37, codec v1). See the [`redisevents`](https://crates.io/crates/redisevents) crate README for the error contract, configuration and interop rules, and this crate's API docs on [docs.rs](https://docs.rs/redisevents-eventsourcing).

License: MIT.
