# Changelog

All notable changes are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/),
and the crates follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed (breaking)

Core, axum and test-support API made idiomatic Rust (version 0.3). Nothing written to or read from
Redis changes; metric and span names are unchanged.

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
