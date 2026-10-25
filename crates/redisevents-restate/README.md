# redisevents-restate

Batched Restate workflow starts from a `redisevents` stream: a handler that turns each message into a Restate ingress start, with bounded batching and a blocking-error contract.

Part of [RedisEvents-rs](https://github.com/bklooste/RedisEvents-rs), the Rust port of the C# `RedisEvents` library. It is wire-compatible with the C# packages (RedisEvents NuGet >= 0.2.37, codec v1). See the [`redisevents`](https://crates.io/crates/redisevents) crate README for the error contract, configuration and interop rules, and this crate's API docs on [docs.rs](https://docs.rs/redisevents-restate).

License: MIT.
