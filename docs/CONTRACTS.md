# Contracts (W0.2)

The public surface of the `redisevents` core crate, frozen by W0.2, and which work package (WP, see
`PLAN.md` §8.2) implements or consumes each part. Read this together with your row in §8.2, the
`WIRE.md` sections it touches and the C# files it names.

## 0. Rules

1. **Frozen means frozen.** Every `pub` item that exists after W0.2 (names, signatures, trait
   bounds, field lists of structs with public fields) changes only through a PR the lead (W0.2
   owner) reviews. Ask before working around a contract; do not change it in a feature PR.
2. **You may freely**: replace a `todo!("<your WP>")` body; add, rename or remove *private* fields of
   a struct whose fields are private; add `pub(crate)` items; add private modules inside the file(s)
   you own; add tests.
3. **Adding `pub` items** (new types, functions, methods) in a file you own is allowed but goes in
   the PR description under "New public API" so the lead can review it. New *trait methods* or new
   *enum variants* on a W0.2 type always need the lead.
4. **One WP, one file set.** Each stub file's header names its owner. Only edit files owned by your
   WP, plus `Cargo.toml` (dependencies) and tests. If you need something in another WP's file, ask.
5. **Never fake behaviour.** A body you cannot finish stays `todo!("<WP>")`.
6. **No `unsafe`** (`unsafe_code = "forbid"`). Clippy pedantic and `missing_docs` are errors in CI.
7. **Async**: tokio, multi-thread runtime. Every future the library hands to `tokio::spawn` must be
   `Send`. No cancellation-token parameters on async methods: cancelling = dropping the future
   (handlers get a token in their [`PartitionContext`]).
8. **Partitions are `u32`, everywhere.** Partition counts are `u32`. Stream ids are `StreamId`, never
   strings, inside the library.

## 1. Crate and module map

```
crates/redisevents/src/
  lib.rs                 W0.2   re-exports
  error.rs               W0.2   Error, Result, BoxError, ParseStreamIdError
  connection.rs          W0.2   Connection (complete); B6 adds tracing spans
  message.rs             W0.2   Message, PayloadCodec, Json (complete), CodecError
  wire/
    stream_id.rs         W0.2 type / A1 parse+format
    msg.rs               W0.2   StreamMsg, TypedMsg (complete)
    header_block.rs      W0.2 API / A1 bodies
    keys.rs              W0.2 API / A1 bodies (KeyNamespace + StreamKeys)
    codec.rs             W0.2 API / A1 bodies (EntryCodec)
    router.rs            W0.2 API / A1 bodies (PartitionRouter)
  config/
    mod.rs               W0.2   option structs + enums + defaults (complete)
    binder.rs            A2     binding, validation, connection-string translation
    instance.rs          A2     InstanceResolver
    advisor.rs           A2     StreamsConfigurationAdvisor
  producer/
    mod.rs               W0.2   Publisher, BufferedPublisher, PublisherExt, PublishOptions
    publisher.rs         B1     StreamPublisher
    buffered.rs          B2     BufferedStreamPublisher
    outbox.rs            B3     Outbox, StreamStore, Idempotency, TopicForwarder
  consumer/
    mod.rs               W0.2   re-exports
    handler.rs           W0.2 traits / C2 adapter bodies
    error.rs             W0.2   HandleError, BLOCK_BACKOFF, ResultExt (complete)
    context.rs           W0.2   PartitionContext (complete)
    fetch.rs             C1     BlockFetch, PollFetch, StreamReaderConnection, StreamBatch
    worker.rs            C2     PartitionWorker (crate-private)
    host.rs              C4     Streams / StreamsHost
    builder.rs           C4b    registration surface
    trimmer.rs           C5     BackgroundTrimmer
    work_queue.rs        C6     WorkQueueFetch (deferred)
  positions/
    mod.rs               W0.2   PositionStore, PositionRecord (B4 bodies), StartPosition, ADMIN_INSTANCE_ID
    redis_store.rs       B4     RedisPositionStore (NullPositionStore complete)
    reset_marker.rs      B4     ResetMarker (crate-private)
    start.rs             B4     StartPosition::resolve etc.
    flusher.rs           C3     PositionFlusher (crate-private)
  ownership/
    mod.rs               W0.2 types / B5 bodies   PartitionOwner, ContestedPartition, OwnershipMap, instance ids
    registry.rs          B5     OwnershipRegistry (crate-private)
  admin/
    meta.rs              B1     topic meta (ensure / grow / refuse shrink)
    stream_admin.rs      D1     StreamAdmin
    health.rs            D2     StreamHealth, StreamLag, StreamStatus
  diagnostics/mod.rs     W0.2 names / B6 everything else (+ `otel` feature)
  testing.rs             D4     in-memory doubles, behind feature `testing`
crates/redisevents-test-support/   W0.2   Redis fixture + RecordingHandler (dev-dependency only)
```

## 2. Shared types

### `wire::StreamId` (W0.2 type; A1 bodies)
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId { pub ms: u64, pub seq: u64 }
const MIN, MAX; const fn new(ms, seq); fn from_time(OffsetDateTime); fn timestamp(self) -> OffsetDateTime
fn parse(&str) -> Result<Self, ParseStreamIdError>;        // A1
fn parse_bytes(&[u8]) -> Result<Self, ParseStreamIdError>; // A1, for raw RESP ids (C1)
fn format_into(self, &mut [u8; 41]) -> &str;               // A1, allocation-free
impl Display (A1), FromStr
```
- Derived `Ord` is the Redis order (ms, then seq). Stored as `u64`, but **each part is
  `0..=i64::MAX`** (C# `long`): `StreamId::MAX_PART = i64::MAX`, `MAX = MAX_PART-MAX_PART`
  (= C# `StreamId.Max`), and parse **rejects** a part above `i64::MAX`.
- Parse rules = C# `StreamId.TryParse`: digits `-` digits, nothing else.
- Time type crate-wide is `time::OffsetDateTime`.

### `wire::StreamMsg` (W0.2, complete)
Private fields, accessor methods, so F1 can change the string representation:
`body() -> &Bytes`, `msg_type() -> &str`, `id() -> StreamId`, `partition() -> u32`,
`partition_key() -> &str`, `correlation_id() -> &str`, `trace_parent() -> Option<&str>`,
`headers() -> &HeaderBlock`, `enqueued_time()`, `into_body()`.
Build with `StreamMsg::new(id, partition, msg_type, body)` + `with_partition_key`,
`with_correlation_id`, `with_trace_parent`, `with_headers`.
- **Zero copy**: `body` is a `Bytes`. C1 builds it with `Bytes::from(Vec<u8>)` from the reply's
  bulk string (no copy), and passes it through `codec::decode`. Never copy a body in the library.
- Absent `t`/`k`/`c` decode as empty strings (C# behaviour); absent `p` is `None`.

`TypedMsg<T> { pub value: T, pub envelope: StreamMsg }` (C# `StreamMsg<T>`).

### `wire::HeaderBlock` (W0.2 API; A1 bodies)
`#[derive(Clone, Default, PartialEq, Eq, Hash)]`, wraps the packed `Bytes`, parsed lazily.
```rust
const MAX_COUNT: usize = 64; const MAX_BYTES: usize = 8192;
const fn empty(); const fn from_packed(Bytes); fn packed(&self) -> &Bytes; fn is_empty(&self) -> bool
fn pack<K: AsRef<str>, V: AsRef<str>>(&[(K, V)]) -> Result<Self>           // A1; cap → Error::InvalidArgument
fn get_bytes(&self, key: &[u8]) -> Result<Option<&[u8]>>                     // A1; first match
fn get(&self, key: &str) -> Result<Option<Cow<'_, str>>>                     // lossy UTF-8
fn iter(&self) -> HeaderIter<'_>   // Item = Result<Header<'_>>; yields the error once, then None (A1)
fn count(&self) -> Result<usize>; fn to_vec(&self) -> Result<Vec<(String, String)>>
pub struct Header<'a> { pub key_utf8: &'a [u8], pub value_utf8: &'a [u8] }  // key(), value() lossy
```
**Ownership of internals**: only `wire/header_block.rs` (A1) parses or builds the packed form.
Everyone else goes through this API. Malformed input → `Error::Transport`.

### `wire::Keys` (W0.2 API; A1 bodies)
The prefix is a **value**, not a global: `Keys::from_env()` (C# order: `STREAMS_KEY_PREFIX`,
`ASPNETCORE_ENVIRONMENT`, `DOTNET_ENVIRONMENT`, then Rust-only `STREAMS_ENVIRONMENT`, default
`production`), `Keys::from_lookup(fn)` for tests, `Keys::with_prefix(p)`.
Methods return `String`: `stream(topic, p, co_locate)`, `positions`, `positions_meta`, `ownership`,
`topic_meta`, `state(topic, name)`, `view(topic, owner, view)`, `dedupe(scope, id) -> Result<String>`,
`positions_pattern`, `stream_pattern(topic, co_locate)`.
Every component that touches Redis takes a `Keys` in its constructor (C4 resolves it once).

### `wire::codec` (W0.2 API; A1 bodies)
```rust
pub const CODEC_VERSION: u32 = 1;   pub mod fields { BODY="b", TYPE="t", PARTITION_KEY="k", CORRELATION_ID="c", TRACE_PARENT="p", HEADERS="h" }
pub struct EntryFields<'a> { body: &[u8], msg_type: &str, partition_key: Option<&str> /* None = state entry */,
                             correlation_id: Option<&str>, trace_parent: Option<&str>, headers: &HeaderBlock }
impl EntryFields { fn write_to(&self, cmd: &mut redis::Cmd) }   // appends pairs after "XADD key [trim] *"
pub fn decode<N: AsRef<[u8]>, I: IntoIterator<Item = (N, Bytes)>>(id, partition: u32, fields: I, codec_version: u32) -> Result<StreamMsg>
pub fn ensure_supported_version(u32) -> Result<()>
```
Headers are packed by the **publisher** once per call (`HeaderBlock::pack`), not by the codec.
Trim arguments (`TrimArgs.cs`) are A1's to add as `pub(crate)` in `wire/` (B1 and C5 use them).

### `wire::router` (W0.2 API; A1 bodies)
`partition_for_key(key: &[u8], partitions: u32) -> u32` and
`RoundRobin::next(&self, partitions) -> u32` (one per publisher). The split is the caller's: B1
checks for an empty key **before** routing and round-robins it, as C# `StreamPublisher` checks
`IsNullOrEmpty` before `PartitionRouter.ForKey`. `partition_for_key` is only called with a
non-empty key.

### `message` (W0.2, complete)
```rust
pub trait Message { const TYPE: &'static str; }
pub trait PayloadCodec: Send + Sync + 'static {
    const NAME: &'static str;
    fn encode<T: Serialize + ?Sized>(&T) -> Result<Bytes, CodecError>;
    fn decode<T: DeserializeOwned>(&[u8]) -> Result<T, CodecError>;
}
pub struct Json;   // serde_json
```
LZ4 MessagePack bodies use MessagePack-CSharp's `Lz4BlockArray` framing (ext 98), which C#
`Standard` options and plain MessagePack decoders cannot read; the MessagePack codec must decode
`Lz4BlockArray` (and `Lz4Block`, ext 99) itself.
E1 adds `MessagePack` (and LZ4 variants) in `redisevents-msgpack` implementing `PayloadCodec`;
A3 provides the pure framing it uses.

### `error` (W0.2)
```rust
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = Error> = std::result::Result<T, E>;
#[non_exhaustive] pub enum Error {
    Config(String),                                              // C# StreamConfigurationException
    Transport { message: String, source: Option<BoxError> },     // C# StreamTransportException
    Redis(redis::RedisError), InvalidArgument(String), Codec(CodecError), Closed(&'static str),
}
fn config(..), transport(..), transport_with(.., src), invalid_argument(..), is_dont_ignore(&self)
```
Which to use: wrong configuration or Redis state that contradicts it (partition shrink, circular
`StartFrom`) → `Config`. Unreadable data (malformed entry/header/id, unknown codec version) or a
failed connect → `Transport`. A raw command failure → `Redis` (via `?`). Bad caller input → `InvalidArgument`.
Use after shutdown → `Closed("<component>")`.

### `connection::Connection` (W0.2, complete)
```rust
#[derive(Clone)] pub struct Connection
async fn connect(url: &str) -> Result<Self>            // redis-rs URL; bad URL → Config, connect fail → Transport
async fn from_client(redis::Client) -> Result<Self>
fn shared(&self) -> redis::aio::ConnectionManager        // cheap clone, auto-reconnect, has a response timeout
async fn dedicated(&self) -> Result<MultiplexedConnection>       // new socket, NO response timeout
async fn dedicated_with_timeout(&self, Duration) -> Result<MultiplexedConnection>
async fn dedicated_with(&self, &AsyncConnectionConfig) -> Result<MultiplexedConnection>
fn client(&self) -> &redis::Client
```
- **Shared** for ordinary commands and pipelines (B1, B4, B5, C3, C5, D1, D2).
- **Dedicated** for `WATCH`/`MULTI`/`EXEC` (B3, E3; use `dedicated_with_timeout`) and blocking
  `XREAD BLOCK` (C1; `dedicated`, and C1 owns reconnecting it). Never put connection-state
  commands on `shared()`.
- Cluster (`cluster-async`) is not wired yet; see §5.

### `config` (W0.2 types and defaults; A2 the rest)
`StreamOptions`, `TopicOptions`, `ConsumerOptions`, `BackpressureOptions`, `InstanceOptions`,
`ProducerOptions`, enums `StartFrom`, `PersistMode`, `ErrorPolicy`, `ReadMode`, `InstanceMode`,
`DeliveryMode`, `TrimMode`. All `serde::Deserialize` with `rename_all = "PascalCase"` and
`default`, so the C# key names bind. Field = C# property in snake_case, same units (`_ms`,
`_seconds`), unsigned. "Set explicitly" flags become `Option`: `ConsumerOptions::delivery`
(accessor `delivery()`), `InstanceOptions::mode` (`mode()`), `TopicOptions::max_len` (`max_len()`,
`max_len_configured()`). `start_from_date` is RFC 3339.
A2 owns: the `config`-crate binder, case-insensitive enum values if C# tests need them, validation
messages, the `UseConsumerGroup` legacy mapping, the default consumer fallback, and
**translating the C# connection string** (`host:port,abortConnect=false,password=…,ssl=…`, default
`redis-db.infra:6379,abortConnect=false`) into a redis-rs URL for `Connection::connect`. A C# test
that asserts a negative-number validation message becomes a deserialize-error assertion.

## 3. Producer

```rust
#[non_exhaustive] #[derive(Default)] pub struct PublishOptions {
    pub correlation_id: Option<String>, pub headers: Vec<(String, String)>,
    pub partition: Option<u32>, pub trace_parent: Option<String>,   // None = current span (B6) or omit
}   // builder: new(), correlation_id(), header(), partition(), trace_parent()

pub trait Publisher: Send + Sync {
    fn publish(&self, partition_key: &str, body: Bytes, msg_type: &str, options: &PublishOptions)
        -> impl Future<Output = Result<StreamId>> + Send;
    fn publish_batch(&self, partition_key: &str, bodies: &[Bytes], msg_type: &str, options: &PublishOptions)
        -> impl Future<Output = Result<()>> + Send;
}
pub trait BufferedPublisher: Publisher {
    fn enqueue(&self, partition_key: &str, body: Bytes, msg_type: &str, options: &PublishOptions)
        -> impl Future<Output = Result<()>> + Send;
    fn flush(&self) -> impl Future<Output = Result<()>> + Send;
}
pub trait PublisherExt: Publisher {   // blanket for every Publisher, complete
    fn publish_message<C: PayloadCodec, M: Message + Serialize + Sync>(&self, key, &M, &PublishOptions) -> .. Result<StreamId>;
    fn publish_messages<C: PayloadCodec, M: Message + Serialize + Sync>(&self, key, &[M], &PublishOptions) -> .. Result<()>;
}
```
`impl Publisher for Arc<P>` and `impl BufferedPublisher for Arc<P>` exist.

- **B1** implements `StreamPublisher` (`new(conn, keys, topic, TopicOptions) -> Result<Self>`, which
  ensures topic meta via `admin/meta.rs`), routing (`router`), packing headers once, writing with
  `EntryFields::write_to`, trimming per `TopicOptions`. `publish_batch` = one pipeline. Explicit
  `partition >= partitions` → `InvalidArgument`.
- **B2** implements `BufferedStreamPublisher` (`new(StreamPublisher, ProducerOptions)`,
  `async shutdown(&self)`), queue full → wait, or drop oldest + `streams.buffer.dropped`.
  `publish`/`publish_batch` flush the queue first so per-key order holds. After shutdown → `Closed`.
- **B3** owns `outbox.rs`; may add public `Outbox`, `StreamStore`, `StateEvent`, `Idempotency`,
  `TopicForwarder` (new public API → lead review). Transactions on `Connection::dedicated_with_timeout`.
- **D4** implements in-memory `Publisher` / `BufferedPublisher` doubles against these traits.

## 4. Consumer

```rust
pub trait MessageHandler: Send + Sync + 'static {
    fn handle(&self, msg: &StreamMsg, ctx: &PartitionContext) -> impl Future<Output = HandleResult> + Send;
}
pub trait BatchHandler: Send + Sync + 'static {
    fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> impl Future<Output = HandleResult> + Send;
}
pub trait TypedHandler<T>: Send + Sync + 'static {
    fn handle(&self, msg: &TypedMsg<T>, ctx: &PartitionContext) -> impl Future<Output = HandleResult> + Send;
}
pub trait TypedBatchHandler<T>: Send + Sync + 'static {
    fn handle_batch(&self, batch: &[TypedMsg<T>], ctx: &PartitionContext) -> impl Future<Output = HandleResult> + Send;
}
pub struct PerMessage<H>(pub H);            // MessageHandler -> BatchHandler          (C2 body)
pub struct Typed<H, T, C = Json>;           // Typed[Batch]Handler -> [Batch]Handler   (C2 body)
impl MessageHandler for Arc<H>; impl BatchHandler for Arc<H>;

pub type HandleResult = Result<(), HandleError>;
pub enum HandleError { Skip(BoxError), Block(BoxError) }   // skip(e), block(e), is_block(), error(), into_error()
impl<E: std::error::Error + Send + Sync + 'static> From<E> for HandleError   // Skip, except Error::Config/Transport → Block
pub trait ResultExt<T> { fn or_block(self) -> Result<T, HandleError>; fn or_skip(self) -> Result<T, HandleError>; }
pub const BLOCK_BACKOFF: [Duration; 6] = [1s, 2s, 4s, 8s, 16s, 30s];   // last repeats

#[derive(Clone)] pub struct PartitionContext   // new(topic, consumer, partition, CancellationToken)
    topic() consumer() partition() cancellation() is_cancelled()
pub use tokio_util::sync::CancellationToken;  // as redisevents::consumer::CancellationToken
```

**Implementations write `async fn`** in their impl blocks; the compiler checks the future is `Send`.
The traits are not object-safe; C4 erases them internally (for example behind a private
boxed-future trait) and keeps the public builder generic.

**The error contract** (C2 implements it; every handler author relies on it):

| Handler outcome | Worker |
| --- | --- |
| `Ok(())` | advance past the batch |
| `Err(Skip(e))` | "ordinary exception": apply `ConsumerOptions::on_error`. `BestEffort` (default): log, count `streams.errors`, advance. `StopPartition`: stop this partition, do not advance. `Fail`: fail the host. |
| `Err(Block(e))` | `DontIgnoreException`: do not advance; retry the **same** batch after `BLOCK_BACKOFF[i]`, forever, while other partitions run; `streams.blocked` = 1 until it succeeds |
| panic | caught (`catch_unwind` on the handler future) and treated as `Skip` |

`PerMessage`: a `Skip` on one message applies the policy to that message and continues with the
next; a `Block` stops there, so the retry resumes at the blocking message (earlier ones count as
done). `Typed` as a message handler: decode failure = `Skip`. As a batch handler: failed bodies are
logged and dropped, the rest are delivered (never an empty call).

Owners:
- **C1** produces `Vec<StreamMsg>` per partition read via `codec::decode`, on dedicated connections.
- **C1 handoff (crate-private, `consumer::fetch`)**. All `pub(crate)`; no new `pub` items.
  ```rust
  ReaderConnection::new(&Connection, block_ms: u32) -> Result<Self>   // Clone = share one socket; block_ms 0 -> Config
  type StreamStart = (u32 /*partition*/, String /*stream key*/, StreamId /*read after*/);
  Fetch::block(ReaderConnection, Vec<StreamStart>, batch_size: u32, codec_version: u32) -> Result<Fetch>
  Fetch::poll(&Connection, Vec<StreamStart>, batch_size: u32, max_idle_delay_ms: u32, codec_version: u32) -> Result<Fetch>
  async fn Fetch::fetch(&mut self) -> Result<Vec<StreamBatch>>   // one per partition with entries; cancel-safe
  fn Fetch::seek(&mut self, partition: u32, after: StreamId) -> bool   // reset-marker protocol (C# ResetSeek)
  fn Fetch::position(&self, partition: u32) -> Option<StreamId>
  struct StreamBatch { partition: u32, msgs: Vec<StreamMsg>, last: StreamId }   // last = cursor after the batch
  ```
  One `Fetch` per read loop: a co-located group is one `Fetch` over all its partitions (one
  multi-stream `XREAD`); `CoLocatePartitions = false` is one `Fetch` per partition, all built from
  clones of one `ReaderConnection` per consumer (C# semantics). The loop never sleeps itself: block
  mode parks in Redis, poll mode applies the idle back-off inside `fetch`. Cursors advance only when
  `fetch` returns `Ok`; dropping the future (shutdown, revoke) loses nothing, and a dropped blocking
  read discards its socket so the next read is not queued behind a parked `BLOCK`. Errors:
  `Error::Redis` (the reader reconnects on the next read; C2 owns the outage back-off) and
  `Error::Transport` for a malformed reply. `last` covers entries C2's type filter drops.
- **C2** owns `worker.rs` and the `PerMessage` / `Typed` bodies. **C2 and C3 agree the reset hand-off
  and read-cursor interface (both crate-private) through the lead on day one.**
- **C4** owns `host.rs` (`Streams::builder(conn)…spawn() -> StreamsHost`), new public API reviewed by
  the lead; C4b and C4t start from its first-commit skeleton.
- **D4**, **E2**, **E4** and tests construct `PartitionContext::new(..)` and call handlers directly.
- `redisevents-test-support::RecordingHandler` implements both handler traits (C2t/C4t use it).

### C5 trimmer handle (`consumer/trimmer.rs`, crate-private)
```rust
BackgroundTrimmer::new(conn: Connection, keys: Keys, options: &StreamOptions) -> Self
BackgroundTrimmer::is_enabled(&TopicOptions) -> bool     // interval > 0 && retention > 0 && trim != None
Arc<BackgroundTrimmer>::start(&self) -> StartedTrimmer   // one tokio task per enabled topic; no-op if none
StartedTrimmer::stop(&self).await                         // cancels and joins; idempotent; Drop cancels
BackgroundTrimmer::trim_topic_once(&self, topic) -> Result<TopicTrimResult>
```
C4 builds one trimmer per host (also for publish-only pods), calls `start` after the flusher and
`stop` before the connection closes. Interlock: per partition, `MINID = min(cutoff, min over every
`p:{topic}:*` hash (SCAN, `:meta` skipped) of the stored position)`; a consumer hash lacking the
partition counts as `0-0`; no staleness timeout (as C#). Only the release valve overrides it
(`XLEN >= ClampReleaseThreshold * MaxLen`): `MINID cutoff`, then `MAXLEN` to a length-derived
target, one `streams.clamp.released` per transition into the released state. A failed scan or
command fails the sweep and trims nothing; the loop logs and retries next interval.

## 5. Positions, ownership, admin, diagnostics

```rust
pub trait PositionStore: Send + Sync + 'static {
    fn load(&self, topic: &str, consumer: &str) -> impl Future<Output = Result<BTreeMap<u32, StreamId>>> + Send;
    fn save(&self, topic: &str, consumer: &str, positions: &[(u32, StreamId)]) -> impl Future<Output = Result<()>> + Send;
    fn reset(&self, topic: &str, consumer: &str, to: StreamId, partition: Option<u32>) -> impl Future<Output = Result<()>> + Send;
}
pub struct PositionRecord { pub id: StreamId, pub instance_id: Uuid }  // format(), parse() — B4
pub enum StartPosition { After(StreamId), Now }   // BEGINNING, from_time, before, resolve, resolve_with — B4
pub const ADMIN_INSTANCE_ID: Uuid;                // 6f72616e-6765-4164-6d69-6e5265736574 (complete)
pub struct RedisPositionStore;                    // new(conn, keys, instance_id) — B4
pub struct NullPositionStore;                     // complete
```
- **B4**: store bodies (non-numeric fields skipped on load; meta fields `<p>:updatedUtc`,
  `<p>:instanceId`), `PositionRecord`, `StartPosition`, reset markers (crate-private).
- **C3**: flusher; the foreign-writer check treats `ADMIN_INSTANCE_ID` as a rewind.
- **B5**: `ownership::{PartitionOwner::format/parse, instance_id_for, stable_instance_id}` bodies
  and the crate-private registry. `instance_id_for` must equal C# `OwnershipRegistry.InstanceIdFor`
  (vector). `OwnershipMap`, `ContestedPartition` are complete data types.
  **B5 handoff (crate-private, `ownership::registry`)** for C3 / C4 / D1 / D2:
  - `OwnershipRegistryOptions::new(topic, consumer, partitions)` (C# defaults: Lease, TTL 30,
    renew 10, pod `unknown`, `stable_instance_id()`); set `mode`, `owned_partitions` (Static only),
    `pod_name`, `ttl_seconds`, `renew_seconds`, `instance_id`. `OwnershipRegistry::new(&conn, &keys,
    options) -> Result` validates (`Config` / `InvalidArgument`, C# messages).
  - `start().await -> Result`: `HEXPIRE` probe (Lease always, Static when it claims anything;
    `Error::Config` if the server rejects it, a warning if Redis is just unreachable), one claim
    (failure logged, not returned), then the renew loop on a `tokio` task. Idempotent.
  - **Ownership changes:** `subscribe() -> watch::Receiver<Arc<[u32]>>` and `held()`: the sorted
    partitions held. Lease mode updates it right after each lease script returns, before the rest
    of the cycle, so a partition lost mid-batch is visible within one renew interval
    (`changed()` / `borrow()`); C4 starts/stops workers from it, C3 checks it before saving.
    Static mode: the pre-assigned set, never changes. Release on stop sets it to `[]`; so does a
    whole TTL without a successful renew (Rust-only safety, C# keeps the stale set).
  - `stop().await`: cancels the loop (an in-flight cycle completes), then the compare-and-delete
    release (failure logged at debug). **Await it before dropping the `Connection`.** Dropping the
    registry only cancels the loop (claims lapse at the TTL, like a SIGKILL). `dispose().await` =
    `stop` + zero the `streams.partitions.unowned/contested` gauges + drop the snapshot.
  - `refresh().await -> Result<OwnershipMap>`, `latest()`, `instance_id()`, statics
    `any_degraded()` / `snapshot(topic, consumer)` (C# `Observed`, for D2 health), helpers
    `presence_field(id)` (`i:<id>`, for C3's liveness cross-check), `read_owners`, `count_members`,
    `hgetall`, `describe_map`.
- **B1**: `admin/meta.rs` (`ensure_topic_meta`), used by `StreamPublisher::new` and C4 startup.
- **D1** / **D2**: `admin/stream_admin.rs` / `admin/health.rs`, new public API reviewed by the lead;
  D3 consumes it.
- **B6**: everything in `diagnostics/` except the frozen name constants (`METER_NAME = "RedisEvents"`,
  `metrics::*`, `spans::*`), `traceparent` parsing/injection (used by B1 when
  `PublishOptions::trace_parent` is `None`, and by C2 per batch), and an `otel` feature.

## 6. Tests

- **Unit tests** live next to the code. **Integration tests** that need Redis dev-depend on
  `redisevents-test-support` and start with
  `let redis = redisevents_test_support::require_redis!();`, which skips cleanly when there is no
  Redis. `REDIS_URL` wins (CI: `redis://localhost:6379`, `redis:8-alpine` service); otherwise a
  reused testcontainers `redis:8-alpine` named `redisevents-test-redis`.
  `REDISEVENTS_REQUIRE_REDIS=1` turns a skip into a failure.
- Per test: `redis.connect().await` (fresh `Connection` on the test's runtime; never share one
  across tests), `redis.keys()` (prefix `test:re:`), `redis.new_topic(hint)`,
  `redis.new_consumer(hint)`. Do not `flush_all` in tests that may run in parallel.
- `wait_until(cond, timeout, because)` / `wait_until_async` for polling.
- Name tests after the C# test they port (`PLAN.md` §8.6 rule 3).

## 7. Golden vectors (`wire-spec/vectors/`, from W0.1)

Wire WPs load these in tests and must pass every case (`PLAN.md` §8.6 rule 6: extend them through
W0.1's generator, never by hand).

| File | Shape | Pins | WP |
| --- | --- | --- | --- |
| `routing.json` | object `{routes: [{key, partitions, partition}], xxh3: [{key, hash_u64_decimal_string}]}` | `partition_for_key`, raw xxh3 | A1 |
| `entries.json` | array `[{name, input: {body_b64, type, partitionKey, correlationId, traceParent, headers, state}, fields: [[name, value]…]}]`; `state: true` = state entry (`partition_key: None`) | `EntryFields::write_to` field order/omission, `decode` | A1 |
| `headers.json` | `{valid: [{name, headers: [[k, v]…], packed_b64}], invalid: [{name, packed_b64}], pack_rejects: [{name, headers}]}` | `HeaderBlock::pack` / iter / error cases | A1 |
| `keys.json` | `{prefix, keys: [{name, key}], prefix_resolution: [{name, env: {ASPNETCORE_ENVIRONMENT, DOTNET_ENVIRONMENT, STREAMS_KEY_PREFIX}, prefix}]}` | `Keys` (use `Keys::from_lookup` for `env`) | A1 |
| `formats.json` | `{stream_id_format, stream_id_parse: [{input, valid, ms, seq}], position_value, reset_marker, reset_marker_parse, owner_value, admin_instance_id, datetime_offset_o}`; numbers as decimal strings | `StreamId` (A1); `PositionRecord`, reset marker (B4); `PartitionOwner` (B5); .NET `"O"` time format (B4, B1 `createdUtc`) | A1, B4, B5 |
| `messagepack.json` | `{threshold_bytes, cases: [{name, type, compression, uncompressed_length, description, body_b64}]}` | `Lz4BlockArray` / `Lz4Block` framing, array vs map layout | A3, E1 |

## 8. Deliberately open

- **Cluster support** (`cluster-async`): `Connection` wraps a single-node `Client` only. Adding a
  cluster variant is a lead-reviewed change to `connection.rs` when a WP needs it.
- **Client names** (`CLIENT SETNAME streams:{service}:{pod}`, C# `StreamNames`): B6 decides
  whether `Connection` sets them.
- **Closure handlers** for C4b: the shape (`handler_fn` taking owned vs borrowed messages) is C4b's
  proposal, reviewed by the lead.
- **Projection dispatch** across many event types (E2) without reflection: E2's design; it builds
  on `BatchHandler` and `PayloadCodec`.
- **`StreamMsg` string storage** may move from `String` to `Bytes`-backed strings in F1; accessors
  keep that invisible.
