# redisevents-axum

Admin endpoints (topic tail, reset preview and apply, ownership) and a health check for `redisevents`, as `axum` routers. `health_router` answers `GET /` with `{"status","rule","description","details"}` (200 for healthy and degraded, 503 for unhealthy). `stream_admin_router` takes an authorization layer, and its reset routes take `snake_case` query parameters (`max_count_scan`, `when_missing`).

Part of [RedisEvents-rs](https://github.com/bklooste/RedisEvents-rs), the Rust port of the C# `RedisEvents` library. It is wire-compatible with the C# packages (RedisEvents NuGet >= 0.2.37, codec v1). See the [`redisevents`](https://crates.io/crates/redisevents) crate README for the error contract, configuration and interop rules, and this crate's API docs on [docs.rs](https://docs.rs/redisevents-axum).

License: MIT.
