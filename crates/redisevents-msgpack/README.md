# redisevents-msgpack

MessagePack typed publish/consume for `redisevents`, with MessagePack-CSharp compatible size-gated LZ4 block compression (array and map layouts). Use `typed_handler_with::<MessagePack, _, _>` to consume and the matching publish helpers to produce.

Part of [RedisEvents-rs](https://github.com/bklooste/RedisEvents-rs), the Rust port of the C# `RedisEvents` library. It is wire-compatible with the C# packages (RedisEvents NuGet >= 0.2.37, codec v1). See the [`redisevents`](https://crates.io/crates/redisevents) crate README for the error contract, configuration and interop rules, and this crate's API docs on [docs.rs](https://docs.rs/redisevents-msgpack).

License: MIT.
