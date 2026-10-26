# Changelog

All notable changes are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/),
and the crates follow [Semantic Versioning](https://semver.org/).

## [0.2] - Unreleased

First release. Versions follow the C# repo's scheme (`version.json` holds `0.2`; CI appends the git height, see `docs/PUBLISHING.md`).

First release: a wire-compatible Rust port of the C# RedisEvents library.

### Compatibility

| Rust | C# | Wire |
| --- | --- | --- |
| `redisevents` 0.2 | RedisEvents NuGet >= 0.2.37 | codec v1 |

A Rust publisher can feed a C# consumer and the reverse, on the same Redis, topics and consumer
positions. CI runs the interop matrix against the NuGet pinned in `interop/dotnet/Versions.props`
(0.2.37) and against the latest published version. Minimum supported Rust version: stable minus 2
(1.97 at release).

### Added

- `redisevents`: wire codec (golden vectors generated from the C# code), configuration binding and
  validation (including the StackExchange.Redis connection-string translation), direct and buffered
  publishers, outbox, state store, idempotency and topic forwarder, Redis position store and reset
  markers, partition ownership (Lease and Static), consumer host with the C# error contract
  (`BestEffort`, `Block`), backpressure, background trimmer, stream admin (reset preview/apply,
  topic tail), health, lag and status reports, OpenTelemetry (`otel`) and in-memory test doubles
  (`testing`).
- `Connection::from_options` / `Connection::url_from_options`: build a connection from the bound
  `Streams` config so one config file serves both runtimes.
- Cargo feature `tls-rustls` for `rediss://` (C# `ssl=true`).
- `redisevents-msgpack`: MessagePack typed publish/consume with MessagePack-CSharp compatible LZ4.
- `redisevents-projections`: typed projections and view stores.
- `redisevents-eventsourcing`: aggregates and an optimistic-concurrency repository.
- `redisevents-restate`: batched Restate workflow starts from a stream.
- `redisevents-axum`: admin endpoints and a health check.
- `StreamHealthReport::new` (with `with_data`, `with_partition`) to build reports from outside the
  crate.

### Changed

- Report, status, health, preview and metadata structs, and the enums and error types that are
  likely to grow, are `#[non_exhaustive]`; match them with a wildcard arm. Option structs stay
  constructible with `..Default::default()`.

### Not included

- `Delivery: WorkQueue` (deferred; refused at startup), cluster and sentinel connections, and the
  outbox `conditions` parameter.

[0.2]: https://github.com/bklooste/RedisEvents-rs/releases
