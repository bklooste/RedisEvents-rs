# Plan: RedisEvents for Rust (`bklooste/RedisEvents-rs`)

Status: draft, 2026-10-25. Source of truth for the port is this repo at `f34c43a`.

## 1. Goal

Ship a Rust implementation of RedisEvents that is **wire-compatible** with the C# library, so any of
these work against one Redis:

| Producer | Consumer | Notes |
| --- | --- | --- |
| C# | Rust | Rust consumer reads `s:{topic}:<p>`, stores positions in `p:{topic}:<consumer>` |
| Rust | C# | Rust publisher writes entries the C# `EntryCodec` decodes unchanged |
| C# + Rust | C# + Rust | Mixed publishers on one topic; same partition for the same key |
| — | C# **and** Rust under one `Consumer` name | Lease-mode ownership shared through `o:{topic}:<consumer>` (nice-to-have, tested but not a gate) |
| C# outbox / event sourcing | Rust projection | and the reverse |

"Compatible" means: same keys, same entry fields, same partition for a given key, same position and
lease encoding, and same admin markers. It does **not** mean the same internal design. Rust uses Rust
idioms wherever the bytes in Redis stay identical.

## 2. The wire contract (what has to match byte for byte)

Taken from the code. Phase 0 turns this list into `WIRE.md` plus golden vectors.

### 2.1 Key namespace (`Config/KeyNamespace.cs`)
- Prefix = `STREAMS_KEY_PREFIX` if set. Otherwise `"{env}:re:"`, where `env` is the lowercased
  `ASPNETCORE_ENVIRONMENT`, then `DOTNET_ENVIRONMENT`, defaulting to `production`.
- **Rust must read the same variables in the same order.** A Rust service that defaults to a
  different prefix silently sees an empty topic. Rust may also accept `STREAMS_ENVIRONMENT` as an
  alias, but it must rank below the two .NET variables.

### 2.2 Keys (`Wire/StreamKeys.cs`, `Outbox.StateKey`, `Idempotency`, `RedisViewStore`)
| Key | Shape |
| --- | --- |
| Partition stream | `<prefix>s:{topic}:<p>` (`s:topic:<p>` when co-location is off) |
| Positions | `<prefix>p:{topic}:<consumer>` |
| Positions meta | `<prefix>p:{topic}:<consumer>:meta` |
| Ownership / leases | `<prefix>o:{topic}:<consumer>` |
| Topic meta | `<prefix>m:{topic}` |
| State stream (outbox / ES) | `<prefix>{topic}:state:<name>` (ES name = `es:<Aggregate>:<id>`) |
| View store | `<prefix>{topic}:view:<owner>:<viewName>` |
| Dedupe claim | `<prefix>dedupe:{scope}:<id>` (scope must not contain braces) |

### 2.3 Entry fields (`Wire/EntryCodec.cs`, codec v1)
- `b` body (raw bytes, always present), `t` type (always), `k` partition key (always on a topic entry,
  may be empty, never on a state entry), `c` correlation id, `p` W3C traceparent, `h` packed headers.
- Optional fields are **omitted**, not written empty. Unknown fields are **ignored** on read.
- The codec version is stored once in `m:{topic}` field `v` (= `1`). A reader must refuse any
  other version.
- Enqueue time comes from the stream id's milliseconds. The entry has no timestamp field.

### 2.4 Header block (`Wire/HeaderBlock.cs`)
- `len(key) ':' key len(val) ':' val …`. Lengths are ASCII decimal **UTF-8 byte** counts.
- Caps: 64 headers and 8 KiB packed. The publisher errors when a cap is exceeded and never
  truncates. The reader errors on malformed input.

### 2.5 Partition routing (`Wire/PartitionRouter.cs`)
- Non-empty key: `xxh3_64(utf8(key), seed = 0)`. Fold to a partition with `hash & (n-1)` when `n`
  is a power of two, else `hash % n`. `n <= 1` gives partition 0.
- Rust: `xxhash_rust::xxh3::xxh3_64`. It matches `System.IO.Hashing.XxHash3.HashToUInt64` with the
  default seed. Golden vectors must cover ASCII, multi-byte UTF-8 and long keys.
- Empty key round-robins. The two sides need not agree here, because round-robin makes no ordering
  promise.

### 2.6 Topic meta (`Admin/StreamAdmin.cs`)
- `m:{topic}` holds `partitions`, `v`, `createdUtc` (ISO-8601 round-trip, `"O"`) and `maxLen`, each
  written with `HSETNX`.
- Partitions may grow (log a warning and overwrite) but never shrink (refuse at startup). Rust must
  enforce the same rule, because a C# and a Rust service disagreeing on `partitions` breaks key
  ordering.

### 2.7 Positions (`Positions/RedisPositionStore.cs`, `Admin/StreamAdmin.Reset.cs`)
- One field per partition. The value is `<streamId>|<instanceId>`, with the instance id as a GUID in
  `"D"` form. A value with no `|` is read as a bare id.
- Meta hash fields: `<p>:updatedUtc` (ISO-8601 `"O"`) and `<p>:instanceId`.
- The admin identity GUID `6f72616e-6765-4164-6d69-6e5265736574` marks an operator rewind. The
  flusher must treat it as "rewind", not as "a rival owns this partition".
- Reset marker: field `__reset:<p>`, value `<target id>|<issued time>`. A running worker applies it
  on its next flush and deletes it only after a read loop has taken it. On cold start, pending
  markers are applied before reading. Non-numeric fields are skipped when positions are read.

### 2.8 Ownership (`Ownership/OwnershipRegistry.cs`)
- Hash fields: `"<p>"` holds the owner value `<instanceId>|<POD_NAME>`, and `i:<instanceId>` is a
  presence field carrying the same value. Both carry a per-field TTL set with `HEXPIRE`, so
  **Redis 7.4 or later is required**.
- Lease mode runs the `LeaseScript` and `ReleaseScript` Lua. **Rust embeds both scripts byte for
  byte**, extracted into `wire-spec/lua/` so the two repos share one text. A mixed C#/Rust group
  then claims fair shares correctly, because the script alone decides who owns what.
- Static mode uses `STREAMS_INSTANCE_INDEX` / `STREAMS_INSTANCE_COUNT` or a StatefulSet ordinal, the
  same as C#.

### 2.9 Work-queue delivery
- `Delivery = WorkQueue` runs `XREADGROUP` with group name = consumer. Port the group creation, the
  claim/redelivery rules and the ack semantics from `Consumer/WorkQueueFetch.cs`.

### 2.10 Atomic writes (outbox, event sourcing)
- Outbox: state commands, then `XADD`, in one `MULTI`/`EXEC`.
- ES: optimistic concurrency checks the state stream's `XLEN` (the C# side uses WATCH via
  `Condition.StreamLengthEqual`), then runs `XADD` on the state stream and on the topic in one
  transaction. Rust may implement this as WATCH/MULTI or as one Lua script. Either is fine, because
  the data left in Redis is identical; only the observable result has to match.

### 2.11 Payloads (above the wire, but where interop actually breaks)
- **Type string.** The C# typed API defaults `t` to `typeof(T).FullName`, for example
  `MyCo.Orders.OrderPlaced`. Rust cannot derive that. Rule: **every cross-language message has an
  explicit, stable type string.** Rust's `trait Message { const TYPE: &'static str; }` makes it
  mandatory, and the README tells C# users to pass `type:` or use `EventTypeRegistry` names.
- **JSON casing.** By default System.Text.Json writes PascalCase and serde writes snake_case. Pick
  one contract per message, recommended `camelCase` on both sides (`JsonSerializerOptions` /
  `#[serde(rename_all = "camelCase")]`), and test it with vectors.
- **MessagePack.** MessagePack-CSharp writes `[Key(int)]` types as arrays and `[Key("x")]` types as
  maps. `rmp-serde` defaults to arrays (matching `Key(int)`) and needs `with_struct_map()` for the
  map form. LZ4 uses MessagePack-CSharp's **`Lz4BlockArray`** framing: an array whose first element
  is ext type **98** holding the uncompressed lengths, followed by `bin` LZ4 *block* chunks. No Rust
  crate implements this, so we write it with `lz4_flex::block`. Decode `Lz4Block` (ext 99) as well.
  The size gate defaults to 1024 bytes.

## 3. Repository & crates

New public repo **`bklooste/RedisEvents-rs`** (MIT), a Cargo workspace. The crate name
`redis-events` is taken on crates.io; the `redisevents` family is free (checked 2026-10-25).

```
RedisEvents-rs/
  wire-spec/
    WIRE.md                    language-neutral spec of §2, versioned with the codec (v1)
    vectors/*.json             golden vectors: entries, header blocks, routing, keys, positions, msgpack/LZ4
    lua/lease.lua release.lua  the ownership scripts, shared text
  crates/
    redisevents/               core  ← RedisEvents (wire, producer, consumer, positions, ownership, outbox, admin, trimming, diagnostics)
    redisevents-msgpack/       ← RedisEvents.MessagePack
    redisevents-projections/   ← RedisEvents.Projections
    redisevents-eventsourcing/ ← RedisEvents.EventSourcing (depends on projections, same as C#)
    redisevents-restate/       ← RedisEvents.Restate
    redisevents-axum/          ← RedisEvents.Web + HealthChecks (admin routes + health), axum feature-gated
  interop/
    dotnet/InteropHost/        small C# console app on the *published* RedisEvents NuGet: produce / consume / admin verbs
    rust-host/                 the same verbs in Rust
    scenarios/                 scripted scenarios run by the CI interop job
  examples/                    Inventory sample (command + view) in Rust, talking to the C# sample
  benches/                     criterion benches mirroring StreamsMicroBenchmarks
```

`RedisEvents.Tracing` becomes an `otel` cargo feature on the core crate. Rust has no DI container to
hook into, so a separate crate adds nothing.

### Stack
| Concern | Choice | Why |
| --- | --- | --- |
| Runtime | `tokio` + `tokio-util::CancellationToken` | standard; maps to `BackgroundService` / `CancellationToken` |
| Redis client | `redis` (redis-rs) with `tokio-comp`, `connection-manager`, `cluster-async` | `MultiplexedConnection` matches the SE.Redis multiplexer model; pipelines, `Script`, raw `cmd("HEXPIRE")` |
| Hash | `xxhash-rust` (`xxh3`) | bit-identical to `XxHash3` |
| Serialization | `serde`, `serde_json`, `rmp-serde`, `lz4_flex` | |
| Ids | `uuid` (v4, hyphenated = .NET `"D"`) | |
| Time | `time` or `chrono`, RFC 3339 with 7 fractional digits to match .NET `"O"` | |
| Observability | `tracing`, `opentelemetry` (metrics names identical: `streams.errors`, `streams.blocked`, `streams.clamp.released`, `streams.partitions.unowned`, `streams.batch.duration`) | dashboards work for both |
| Config | `serde` structs + the `config` crate, using the same section and key names as `StreamConfigBinder` | one config file per environment serves both |
| HTTP | `axum` (admin), `reqwest` (Restate ingress) | |
| Tests | `testcontainers` (`redis:8-alpine`), `proptest`, `criterion` | |

### API shape (C# → Rust)
| C# | Rust |
| --- | --- |
| `builder.AddStream<H>("orders")` | `Streams::builder(redis).topic("orders").handler(h).spawn()` returns a `StreamsHost` handle |
| `IMessageHandler` / `IBatchHandler` | `trait MessageHandler` / `trait BatchHandler` (native `async fn` in traits) |
| `IMessageHandler<T>` + `JsonTypeInfo<T>` | `TypedHandler<T: Message + DeserializeOwned>` with a codec type parameter (JSON / MessagePack) |
| Error contract: ordinary exception = skip, `DontIgnoreException` = block | `Result<(), HandleError>`, where `HandleError::Skip(e)` logs and skips and `HandleError::Block(e)` blocks and retries (1s→2→4→8→16→30s). A panic counts as `Skip`, mirroring "ordinary exception". |
| `IStreamPublisher`, `IStreamBufferedPublisher` | `Publisher`, `BufferedPublisher` (bounded mpsc channel + flush task; same loss-on-crash trade-off, documented) |
| `Outbox.WriteAndPublishAsync` | `Outbox::write_and_publish(\|pipe\| { ... })` taking a `redis::Pipeline` in atomic mode |
| `StreamAdmin` | `StreamAdmin` with the same operations (preview/reset, ownership, topic tail) |
| `Testing/*` | `redisevents::testing` (in-memory publisher, harness) behind a `testing` feature |

## 4. Phases

Each phase ends with green CI. From Phase 2 on, each phase also passes its rows of the interop
matrix in §5.

### Phase 0: Contract first (both repos)
1. Create `bklooste/RedisEvents-rs` (`gh repo create bklooste/RedisEvents-rs --public --license mit`),
   with the workspace skeleton, `rust-toolchain.toml` (stable), `rustfmt`/`clippy -D warnings`, and
   CI (build, test, clippy, a `redis:8-alpine` service).
2. Write `wire-spec/WIRE.md` from §2.
3. **In this repo:** add a small vector generator test that writes `vectors/*.json` from the real C#
   `EntryCodec`, `HeaderBlock`, `PartitionRouter`, `StreamKeys` and MessagePack paths. Add a
   conformance test that reads them back. Extract the two Lua scripts to embedded resource files so
   they can be copied verbatim. This is one PR here; the C# library's behaviour stays unchanged.
4. Copy the vectors and Lua scripts into the Rust repo. A CI check diffs them against the C# repo at
   a pinned tag, so drift fails the build.

Exit: the spec is reviewed, the vectors exist, and C# passes its own vectors.

### Phase 1: Core wire layer (pure, no Redis)
`StreamId`, `EntryCodec` (encode/decode, state variant), `HeaderBlock` (zero-copy reader over
`bytes::Bytes`), `PartitionRouter`, `StreamKeys`, `KeyNamespace`, `StreamMsg`.

Exit: every golden vector passes, and proptest round-trips headers and ids.

### Phase 2: Producer
Topic meta init/grow/refuse-shrink, direct `XADD`, pipelined batch publish, buffered publisher,
`MAXLEN ~` trimming plus the background trimmer (respecting the position interlock), the outbox,
idempotency (`SET NX PX`), and the topic forwarder.

Exit: **Rust → C#** interop scenarios pass (single, batch, buffered, headers, trace, outbox).

### Phase 3: Consumer
Partition workers (block fetch and poll fetch), batch dispatch, the error contract with backoff,
the position store and flusher (`<id>|<instance>` values, meta hash, foreign-writer cross-check),
reset markers, Lease ownership (shared Lua) and Static mode, the work-queue mode, backpressure,
start positions / replay-from-time, and graceful shutdown (flush positions, release leases).

Exit: **C# → Rust** interop passes.

Nice-to-have, not a gate: a C# and a Rust instance under the same `Consumer` name in Lease mode
split partitions fairly and fail over. The shared Lua should make this work for free, so it is
tested, but a failure is a bug to fix later, not a blocker. The `XREADGROUP` work-queue mode is
rarely used and is deferred to after Phase 5.

### Phase 4: Operability
Diagnostics (lag, health, status, partition status), OpenTelemetry metrics/spans with identical
names, `StreamAdmin`, the `redisevents-axum` admin routes and health check (same paths as
`RedisEvents.Web`), and the config binder with the same keys.

Exit: an admin reset issued from **C#** rewinds a running **Rust** consumer, and the reverse.

### Phase 5: Opt-in packages
1. `redisevents-msgpack`, including the `Lz4BlockArray` codec. Test cross-decoding both ways, above
   and below the 1024 B gate.
2. `redisevents-projections`: an event type registry with explicit wire names, projectors,
   `ViewStore` trait, Redis and in-memory stores (same view key).
3. `redisevents-eventsourcing`: aggregate root, repository with optimistic concurrency, and a cached
   aggregate with the concurrency-check switch from #47.
4. `redisevents-restate`: filter + map, bounded-parallel starts, block rather than skip.

Exit: the C# Inventory command API feeds the Rust view API (and the reverse), and an aggregate saved
from C# loads in Rust with the same version.

### Phase 6: Performance and release
Criterion benches that mirror `StreamsMicroBenchmarks`, an end-to-end throughput suite that mirrors
`PerfBenchmarkTests`, README with measured numbers, `cargo-semver-checks`, and publishing to
crates.io (optional, needs a token).

## 5. Interop test matrix (CI job `interop`)

The CI job starts `redis:8-alpine`, builds `interop/dotnet/InteropHost` against **(a)** the latest
published NuGet and **(b)** `bklooste/RedisEvents@main`, builds `rust-host`, then runs every scenario
in both directions:

| Scenario | Checks |
| --- | --- |
| Keyed publish, 1/4/7/16 partitions | same key lands on the same partition from both languages; per-key order preserved |
| Headers, correlation id, traceparent | round-trip exactly; trace continues across the language boundary |
| Unicode / empty / long keys | routing and `k` field match |
| Batch + buffered publish | count and order per key |
| Positions resume | stop the C# consumer, start a Rust consumer with the same consumer name; it resumes with no gap or replay beyond the last flush |
| Mixed Lease group (nice-to-have) | fair split, failover within one TTL, no dual ownership |
| Admin reset | marker written by one language, applied by the other |
| Partition grow | either side grows; the other refuses to shrink |
| Codec version guard | `v=2` in `m:{topic}` makes both sides refuse |
| Outbox / ES | state + event atomic; version check honoured across languages |
| JSON + MessagePack (+LZ4) payloads | typed decode both ways using explicit type strings |
| Work queue (deferred) | competing C#/Rust consumers in one `XREADGROUP` group; no message lost |

## 6. Interop hazards to design out

| Hazard | Mitigation |
| --- | --- |
| `t` defaults to C# `FullName` | Rust requires `Message::TYPE`; the C# docs recommend explicit types for cross-language topics |
| JSON property casing | per-message explicit casing + vectors |
| MessagePack array vs map | document the mapping; vectors for both forms |
| Different key prefix per runtime | identical env var resolution; startup log prints the resolved prefix |
| GUID / timestamp text formats | `uuid` hyphenated lowercase; timestamps with 7 fractional digits + `Z`/offset, and the parser accepts both |
| `HEXPIRE` availability | same startup probe (`__hexpire-probe:<id>`); refuse Lease mode without it, as C# does |
| Lua drift | one shared script text, plus a CI diff |
| Partition count disagreement | meta check on both sides, and the mixed-grow test |
| Cluster slots | same `{topic}` hash tags; the outbox and ES require co-location, as C# does |

## 7. Versioning rules
- Wire spec version = codec version (`v` in `m:{topic}`), currently 1. Any change to §2 is a spec
  PR with new vectors, landed in **both** repos before either releases.
- Crate versions are independent of NuGet versions. The README holds a compatibility table:
  "redisevents 0.x ↔ RedisEvents NuGet ≥ y, codec v1".
- The Rust repo's CI runs interop against C# `main` nightly, so a C# change that breaks Rust is
  caught before a release.

## 8. Rough sizing (relative)

| Phase | Size |
| --- | --- |
| 0 contract + scaffold | S |
| 1 wire | S |
| 2 producer | M |
| 3 consumer + ownership + positions | **L** (largest: `StreamConsumerHost`, `PositionFlusher`, `OwnershipRegistry`, `PartitionWorker.*` ≈ 8k C# lines) |
| 4 operability | M |
| 5 packages | M |
| 6 perf + release | S–M |

## 9. Decisions (agreed 2026-10-25)
1. **Repo**: separate `bklooste/RedisEvents-rs`.
2. **Redis client**: `redis-rs`.
3. **crates.io**: publish at Phase 6.
4. **Minimum supported Rust version**: latest stable − 2.
5. **Mixed C#/Rust instances under one `Consumer` name**: nice-to-have, not a gate. RedisEvents
   normally runs Kafka-style, so `XREADGROUP` parity is deferred.
