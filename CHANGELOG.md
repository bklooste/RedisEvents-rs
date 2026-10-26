# Changelog

All notable changes are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/),
and the crates follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed (breaking): idiomatic Rust in the package crates (0.3)

The port was done too literally. Every byte in Redis and on the Restate wire is unchanged (wire
type strings, JSON/MessagePack bodies, LZ4 framing, view key `{topic}:view:<owner>:<view>`, state
stream `es:<Aggregate>:<id>`, version = `XLEN`, ingress request shape and status handling, metric
and span names); only the Rust API changed. Versions are now 0.3.

- `redisevents-projections`: `ProjectionBatch`, `BatchError` and `EventMeta::batch` are gone. A batch
  projection implements `BatchFlush` (an associated `Pending: Default` buffer and `flush(pending)`)
  and `BatchProjection<T>::buffer(event, &mut pending, ..)`; the projector owns the buffer, so there
  is no address-keyed state, lock or "closed" error. `ProjectorBuilder::projection` /
  `batch_projection` take the projection by value; `shared_projection` / `shared_batch_projection`
  take an `Arc`. `default_owner()` and `RedisViewStore::with_owner` are gone:
  `RedisViewStore::new(conn, keys, topic, owner, view_name)` takes the owner explicitly.
- `redisevents-eventsourcing`: `AggregateRoot::raise` -> `emit`, `uncommitted_changes` -> `pending`,
  `mark_changes_as_committed` -> `commit`, `load_from_history` -> `AggregateRoot::from_history(id, events)`.
  `CachedAggregate::run` takes `&self` (it owns its lock; no external `tokio::sync::Mutex`), and
  `evict` / `is_cached` are `async`. `ExecuteOptions::max_attempts` takes `NonZeroU32`.
  `Error::InvalidOperation` / `InvalidArgument` became `Error::RefusedAfterEmitting { .. }` /
  `Error::BlankId`. `AggregateDecision::done/refused/redo` helper fns are removed (use the variants).
- `redisevents-restate`: `RestateStartOptions { ingress }` is a required `Url` (no `Option`, no
  `Default`), `max_parallelism` / `max_attempts` are `NonZero*`, and `name` is removed.
  `RestateStats` and `RestateStartHandler::stats()` are removed. `RestateStart::with_input` returns
  `Result<Self, serde_json::Error>` (use `?` in `map`); a serialization failure now reads
  `Map failed: ..` and is labelled by stream id.
- `redisevents-msgpack`: `MessagePackMsgExt::decode_msgpack` is removed; use
  `redisevents_msgpack::decode::<T>(msg.body())`.
- Docs and log text no longer cite C# type names; "EventProjector:" log prefixes read "Projector:".

### Changed (behaviour)

- **Key prefix resolution (Rust only; C# unchanged).** `Keys::from_env` now resolves
  `STREAMS_KEY_PREFIX` (verbatim, unless blank), else `STREAMS_ENVIRONMENT` (lowercased) -> `<env>:re:`,
  else `local:re:`. Rust no longer reads `ASPNETCORE_ENVIRONMENT` or `DOTNET_ENVIRONMENT` and no longer
  defaults to `production`. The consumer host's validation environment name follows the same rule
  (`STREAMS_ENVIRONMENT`, else `local`). **A Rust service that relied on `ASPNETCORE_ENVIRONMENT`
  (or the `production` default) now uses `local:re:` unless `STREAMS_ENVIRONMENT` or
  `STREAMS_KEY_PREFIX` is set, and will no longer see its old keys.** C# still defaults to
  `production:re:`, so mixed C#/Rust deployments must set `STREAMS_KEY_PREFIX` on both services (or
  `STREAMS_ENVIRONMENT` on Rust equal to the C# environment name). No API change.
- Startup logs the resolved prefix at info, and says when it fell back to `local:re:`.

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
