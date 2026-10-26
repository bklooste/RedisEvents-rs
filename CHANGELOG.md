# Changelog

All notable changes are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/),
and the crates follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added: work-queue delivery (`delivery = "work_queue"`, C6)

Additive (0.4.x): no existing item changes shape.

- `delivery = "work_queue"` now runs instead of being refused at startup: competing consumers over a
  Redis consumer group per partition stream (group = consumer name, member = pod name, else the host
  name), `XACK` after each handled batch (success, `best_effort` skip, filtered; never on `Block`,
  `stop_partition` or `fail`), and `XAUTOCLAIM` recovery of entries another member left idle for
  30 s. Per-key order is not preserved in this mode. See the README, "Work-queue delivery".
- `config::WorkQueueOptions` (claim threshold, claim interval, member name) and
  `ConsumerBuilder::work_queue`. The defaults are the C# library's.
- Wire: `WIRE.md` §9 documents the consumer-group contract, so C# and Rust members can share one
  group (former §9–§12 are now §10–§13). The interop runner gains `work_queue_competing_consumers`
  and `work_queue_dead_member_claimed`; both hosts gain `consume --delivery work_queue
  [--batch-size N] [--delay-ms MS]`.
- Beyond C#, on the Rust side only (nothing extra on the wire): blocking `XREADGROUP` on the
  reader connection in `read_mode = block` (C# always polls), a restarted member re-reads its own
  pending entries at once, a claim sweep also runs under load, and a failed `XACK` is retried.

### Added: a stalled Lease owner hands its partitions on (port of C# RedisEvents#52)

Released as 0.4: `ConsumerOptions` gains a public field, which breaks exhaustive struct literals
built against 0.3.0 (use `..ConsumerOptions::default()`).

- `ConsumerOptions::unhealthy_stalled_seconds` (default 300, `0` off) and the health rule
  `StreamHealthRule::Stalled` (Unhealthy). Measured on the process's own clock: no read returned, no
  batch completed, a batch held too long, or position writes failing. `StreamPartitionStatus` gains
  `stalled_ms`, `unhealthy_stalled_seconds` and `stood_down_stalled`; `StreamHealthDetails` gains
  `stalled_partition` and `stalled_ms`.
- In Lease mode the host stands a stalled partition down, and a read loop that fails with an error no
  back-off fixes (or panics): it gives the lease up at once, quarantines the partition from re-claim
  for `max(2 x lease_ttl_seconds, unhealthy_stalled_seconds)`, withdraws its presence from the fair
  share, and rebuilds the read side on fresh reader connections. Static mode grades and logs only.
- Wire: the lease script is now sent with its optional trailing quarantine arguments (WIRE.md §8)
  while a quarantine lasts. Unchanged otherwise, so mixed C# / Rust groups hand over to each other.
- An `XREAD` reply the client cannot read (wrong shape, ids not after the cursor sent) is now a
  transport failure the read loop waits out on a fresh reader socket, instead of ending the loop.

### Changed (breaking): idiomatic Rust in the core, axum and test-support crates (0.3)

The port was done too literally. Nothing written to or read from Redis changes; metric and span names are unchanged.

- `Error::is_dont_ignore` is now `Error::is_blocking`. Descriptions and docs no longer say
  `DontIgnoreException` (the health description reads "blocked retrying a blocking error").
- `admin::health`: the static-class structs `StreamHealth`, `StreamStatus` and `StreamLag` are gone.
  Use the free functions `health::check`, `evaluate`, `grade`, `find_silent`, `partitions`, `any`,
  `estimate_entries_behind` and the constants `health::DEFAULT_SAMPLE_INTERVAL`,
  `DEFAULT_PUSH_INTERVAL`, `TAIL_STALE_AFTER`.
- `StreamHealthReport::data: BTreeMap<String, HealthValue>` (camelCase string keys) is replaced by
  `details: StreamHealthDetails`, a struct of typed `Option` fields (`partitions_blocked`,
  `worst_lag_ms`, ...). `HealthValue` and `with_data` are removed (`with_details` added). Status,
  rule and details implement `serde::Serialize` (snake_case).
- `redisevents-axum` health: the body is `{"status","rule","description","details"}` with lowercase
  snake_case values, not the ASP.NET `HealthReport` shape (`entries`, `tags`, "Healthy"). Status codes
  are unchanged (healthy and degraded 200, unhealthy 503). `CHECK_NAME` is removed.
- `redisevents-axum` admin: the preview query parameters are `max_count_scan` and `when_missing`
  (were `maxCountScan`, `whenMissing`). Operations and status codes are unchanged.
- Docs and messages no longer describe the C# library's types; cross-language rules stay in the
  Interop section.

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

### Changed (breaking): configuration and connection (0.3)

Configuration is not part of the wire; these changes make it idiomatic Rust and change no byte in
Redis (golden vectors, interop and the sample matrix are unchanged).

- **Option structs are plain serde.** `StreamOptions` and everything under it deserialize with
  `snake_case` field names and `snake_case` enum values (`async_batch`, `work_queue`, `lease`), from
  any serde source. The PascalCase keys, case-insensitive key/enum matching and the `Streams`
  section wrapper are gone, and so is `config::binder` (`bind_json`, `SECTION_NAME`,
  `resolve_topic`, `resolve_consumer_name`, `validate`, `advise`). Use
  `StreamOptions::{validate, advisories, topic, consumer_name}`. Error messages name fields by path
  (`consumers[0].batch_size`, not `Streams:Consumers[0]:BatchSize`).
- **`StreamsConfigurationAdvisor` is removed.** `options.advisories()` returns the lines;
  `Advisory::log` emits one.
- **Connection by URL.** `StreamOptions::connection_string` is replaced by `redis_url`
  (`redis://` / `rediss://`), default `redis://127.0.0.1:6379/0` (was the C# `redis-db.infra:6379`).
  `Connection::url_from_options` and `binder::to_redis_url` are removed; the StackExchange.Redis
  translator survives as the explicit `connection::stackexchange::to_redis_url`.
  `DEFAULT_CONNECTION_STRING`, `DEFAULT_SYNC_TIMEOUT_MS`, `resolve_sync_timeout_ms` and the
  `block_ms < syncTimeout` rule (a StackExchange.Redis client limit) are removed.
- **No environment names.** `StreamsBuilder::environment` is removed and `Trim = None` no longer
  depends on `Development`: set `StreamOptions::allow_untrimmed` (or `StreamsBuilder::allow_untrimmed(true)`).
  `STREAMS_ENVIRONMENT` now only feeds the key prefix.
- **Legacy C# key dropped.** `ConsumerOptions::use_consumer_group` is removed; `delivery` is a plain
  `DeliveryMode` (no `delivery()` accessor), and `InstanceOptions::mode` a plain `InstanceMode`
  (no `mode()` accessor; the "count with no mode" migration message is gone).
- **Instance identity.** `InstanceResolver` is replaced by `InstanceIdentity::{resolve, from_env,
  owned, describe_ownership}` and the free functions `config::instance::{owned, owner_of,
  pod_ordinal}` plus the `*_VARIABLE` constants. New `InstanceOptions::pod_name` is used before the
  `POD_NAME` variable. The `STREAMS_INSTANCE_COUNT` / `STREAMS_INSTANCE_INDEX` / `POD_NAME`
  fallbacks and the owner-value format in Redis are unchanged.
- Samples read `REDIS_URL` only (no `Streams__ConnectionString`).
- Version 0.3 (`version.json`, workspace `0.3.0`).

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

## [0.2] - 2026-10-26

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
