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

> **F3 (first release, 0.2) API hygiene.** Report/status/health/preview/metadata structs and the enums and
> error types likely to grow are `#[non_exhaustive]` (external code matches with `_` and builds them
> through constructors, e.g. `StreamHealthReport::new(..).with_data(..)`). Option structs stay
> literal-constructible (`..Default::default()`). The `tls-rustls` cargo feature enables `rediss://`.

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
    mod.rs               W0.2   option structs + enums + defaults; StreamOptions::{validate, advisories}
    validate.rs          A2     validation rules and advisory lines
    instance.rs          A2     InstanceIdentity, owned / owner_of / pod_ordinal
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
    health.rs            D2     free functions `check`, `evaluate`, `grade`, `find_silent`, `partitions`, `any`, `estimate_entries_behind`
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
The prefix is a **value**, not a global: `Keys::from_env()` (Rust rule:
`STREAMS_KEY_PREFIX`, else `STREAMS_ENVIRONMENT` -> `<env>:re:`, else `local:re:`; the .NET variables are
never read), `Keys::from_lookup(fn)` for tests, `Keys::with_prefix(p)`.
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
LZ4 MessagePack bodies use MessagePack-CSharp's `Lz4BlockArray` framing (ext 98), which the .NET
`Standard` options and plain MessagePack decoders cannot read; the MessagePack codec must decode
`Lz4BlockArray` (and `Lz4Block`, ext 99) itself.
E1 (`redisevents-msgpack`, done) adds `MessagePack` and `MessagePackMap` implementing `PayloadCodec`
(A3 provides the pure framing), plus:
```rust
pub type MessagePackTyped<H, T> = Typed<H, T, MessagePack>;      // consume; LZ4 auto-detected
pub type MessagePackMapTyped<H, T> = Typed<H, T, MessagePackMap>;
pub trait MessagePackPublisherExt: Publisher {                   // blanket for every Publisher
    fn publish_msgpack<M: Message + Serialize + Sync>(&self, key, &M, &Options, &PublishOptions) -> Result<StreamId>;
    fn publish_msgpack_batch<M: ...>(&self, key, &[M], &Options, &PublishOptions) -> Result<()>;
}
```
Decode one `StreamMsg` by hand with `redisevents_msgpack::decode::<T>(msg.body())`.
`Options` (layout, `Compression` threshold/disable) is passed per call; core's
`PublisherExt::publish_message::<MessagePack, _>` covers the defaults.

### `error` (W0.2)
```rust
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T, E = Error> = std::result::Result<T, E>;
#[non_exhaustive] pub enum Error {
    Config(String),                                              // C# StreamConfigurationException
    Transport { message: String, source: Option<BoxError> },     // C# StreamTransportException
    Redis(redis::RedisError), InvalidArgument(String), Codec(CodecError), Closed(&'static str),
}
fn config(..), transport(..), transport_with(.., src), invalid_argument(..), is_blocking(&self)
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
async fn from_options(&StreamOptions) -> Result<Connection>    // StreamOptions::redis_url() -> connect
// connection::stackexchange::to_redis_url(&str) -> Result<String>: opt-in C# connection-string bridge
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
`DeliveryMode`, `TrimMode`. Plain `serde::Deserialize` with `snake_case` fields, `#[serde(default)]`
and `snake_case` enum values, so any serde source works; configuration is not part of the wire
contract (`wire-spec/WIRE.md` is Redis only). Units in the field name (`_ms`, `_seconds`), counts
unsigned. `TopicOptions::max_len` stays an `Option` (`max_len()`, `max_len_configured()`) because
the retention rule needs "set explicitly". `start_from_date` is RFC 3339.
`StreamOptions::redis_url()` (default `redis://127.0.0.1:6379/0`), `topic(name)`,
`consumer_name(&ConsumerOptions)`, `validate()` (errors name the field path, e.g.
`consumers[0].batch_size`) and `advisories()`. `allow_untrimmed` is the explicit opt-in for
`trim = none`; there is no environment-name semantics. Instance identity:
`InstanceIdentity::{resolve, from_env}` take explicit `InstanceOptions` first, then
`STREAMS_INSTANCE_COUNT` / `STREAMS_INSTANCE_INDEX` / `POD_NAME` (shared with C#).

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
- **B3** (`producer/outbox.rs`, re-exported from `producer`). Public API, for E3 and services:
  ```rust
  pub struct OutboxPublish<'a> { pub topic: &'a str, pub partition_key: &'a str, pub body: &'a [u8],
      pub msg_type: &'a str, pub options: &'a PublishOptions, pub topic_options: &'a TopicOptions }
  impl Outbox {   // MULTI/EXEC on one cached `dedicated_with_timeout` connection (default 5 s)
      fn new(Connection, Keys) -> Self; fn with_response_timeout(self, Duration) -> Self;
      fn state_key(&Keys, topic, name) -> Result<String>;                 // <prefix>{topic}:state:<name>
      fn ensure_same_slot(&Keys, topic, state_keys: &[&str], co_locate: bool, partition: u32) -> Result<()>;
      async fn write_and_publish(&self, FnOnce(&mut redis::Pipeline), &OutboxPublish, state_keys: &[&str]) -> Result<StreamId>;
      async fn write_and_publish_many(&self, FnOnce(&mut redis::Pipeline), &[OutboxPublish], state_keys: &[&str]) -> Result<Vec<StreamId>>;
  }
  pub struct StateEvent { pub body: Bytes, pub msg_type: String, pub options: PublishOptions } // new(), with_options()
  impl StreamStore {   // E3's event store
      fn new(Connection, Keys, topic, TopicOptions) -> Result<Self>;    // Config error unless co-located
      fn require_co_located_partitions(topic, &TopicOptions) -> Result<()>;
      fn topic(&self) -> &str; fn options(&self) -> &TopicOptions; fn state_key(&self, name) -> Result<String>;
      async fn read(&self, name, after: StreamId /* exclusive */, max: usize) -> Result<Vec<StreamMsg>>;
      async fn append_and_publish(&self, name, expected_len: u64, partition_key, &[StateEvent])
          -> Result<Option<Vec<StreamId>>>;   // None = XLEN != expected_len, nothing written
      async fn append_and_publish_unchecked(&self, name, partition_key, &[StateEvent]) -> Result<Vec<StreamId>>;
  }
  impl Idempotency { fn key(&Keys, scope, id) -> Result<String>;
      async fn try_begin(&Connection, &Keys, scope, id, ttl: Duration) -> Result<bool>; } // SET NX PX
  pub struct ForwardedMessage { pub partition_key: String, pub body: Bytes, pub msg_type: String }
  impl TopicForwarder { fn new(Connection, Keys, target, TopicOptions) -> Result<Self>;
      fn with_name(self, &str) -> Result<Self>; fn with_marker_ttl(self, Duration) -> Self;
      async fn forward(&self, source, dedupe_id, position: StreamId, &[ForwardedMessage], correlation_id: Option<&str>) -> Result<bool>; }
  ```
  - Version = `XLEN` of the state stream; returned ids are the **state** entries' ids (E3's
    versions). State entries are `b t` (+ `c p h` with `state_metadata`), never `k`, never `MAXLEN`.
  - The conditional writes (store, forwarder) run as **one Lua script** (`EVALSHA`, `EVAL` on
    `NOSCRIPT`) on `shared()`, not `WATCH`/`MULTI`: same entries on the wire, one round trip, no
    socket per save, no false conflicts, every key in `KEYS` and in one slot. The plain `Outbox`
    keeps `MULTI`/`EXEC` on a dedicated connection because its state commands are arbitrary.
  - Errors: blank names, empty event lists, a missing type, a partition out of range →
    `InvalidArgument`; slot / co-location / brace / topic-option problems → `Config`; a failed
    transaction or script → `Transport` (it ran completely or not at all; no rollback inside).
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
- **C2** owns `worker.rs`, `shared.rs` and the `PerMessage` / `Typed` bodies. **C2 handoff
  (crate-private, `consumer::worker`)**, what C4 wires and C2t tests against:
  ```rust
  WorkerConfig { topic: Arc<str>, consumer: Arc<str>, filter: Option<Vec<String>>, on_error: ErrorPolicy,
                 persist: PersistMode, channel_capacity: Option<usize> /* None = inline */ }
  WorkerConfig::new(topic, consumer)                      // BestEffort, AsyncBatch, pooled(4), no filter
  WorkerConfig::from_options(&ConsumerOptions, consumer)  // Backpressure.Enabled=false -> inline
  PartitionSlot::new(partition: u32, stop: PartitionStop)
  trait FetchSource: Send + 'static { async fn fetch(&mut self) -> Result<Vec<StreamBatch>>; fn seek(&mut self, u32, StreamId) -> bool }
  impl FetchSource for Fetch                               // C1
  Worker::new(WorkerConfig, Vec<PartitionSlot>, F: FetchSource, Arc<H: BatchHandler>,
              Arc<dyn PositionSink>, Option<Arc<ResetSignal>>) -> Result<Worker<H, F>>
  async fn Worker::run(self) -> Result<(), WorkerError>    // Send; Ok = every partition stopped
  enum WorkerError { Handler { partition, error: BoxError } /* ErrorPolicy::Fail */, Fetch(Error) /* non-transient read */ }
  TypeFilter::new(Option<&[String]>), matches(&str); block_delay(attempt) -> Duration; is_transient(&Error) -> bool
  // #[cfg(test)] worker::testing: ScriptedFetch (Step::Batches / Step::Fail, then parks; FetchLog of seeks/reads),
  //   RecordingSink (SinkEvent::Record/Rewind/Flush; failing_flushes(n)), msg(), batch(), unique(),
  //   measurements(topic) -> Vec<Measurement::{Filtered, Consumed, Error, Batch}> (the MeterListener rig)
  ```
  Semantics: one `Worker` per read loop (one `Fetch`; a co-located group is one worker over all its
  partitions). Pooled: a bounded channel (capacity in batches) + one processor task per partition;
  a stopped partition is retired (the group reads on and discards for it). Inline: the loop calls the
  handler before the next fetch; `StopPartition` in an inline group stops the whole loop (C#). The
  type filter runs on decoded messages (C1 decodes); `streams.filtered` counts the drops, `last`
  still checkpoints them. Fetch errors: `is_transient` (dropped/refused/timeout, LOADING, BUSY, …,
  or a failed reconnect wrapped in `Transport`) are waited out on `BLOCK_BACKOFF` while the loop
  keeps its cursor; anything else ends the worker with `WorkerError::Fetch`. A handler `Err` after
  the partition's stop token fired is a shutdown (no log, metric or advance). `PerMessage` reports
  progress through a task-local scope, so a block or a stop at message *i* records `..i`. The
  worker does not clear per-partition gauges on exit (`instruments::clear_partition`): C4 does that
  when a partition is released.
- **C2↔C3 interface** (crate-private, `consumer/shared.rs`, owner C2, agreed with C3):
  ```rust
  // Reset hand-off (C# ResetSignal). One per consumer, indexed by partition 0..partition_count.
  ResetSignal::new(partition_count: u32) -> Self                 // shared as Arc<ResetSignal>
  fn has_pending(&self) -> bool                                  // read loop, once per round
  fn request(&self, ResetMarker) -> bool                         // flusher tick; true = newly pending (log it)
  fn try_take(&self, partition) -> Option<ResetMarker>           // read loop, before the next fetch
  fn is_taken(&self, partition) -> bool
  fn try_clear_taken(&self, partition) -> Option<ResetMarker>    // flusher: Some = rewound, delete the field now
  fn forget(&self, partition)                                    // host (C4): partition no longer read here
  // slot states: Empty -> Pending(m) -> Taken(m) -> Empty; a different marker replaces Pending/Taken

  // Positions (C# PositionRecorder + PositionFlush + PositionFlusher.Rewind). Arc<dyn PositionSink>.
  trait PositionSink: Send + Sync + 'static {
      fn record(&self, partition: u32, id: StreamId);   // after handling only; never blocks
      fn rewind(&self, partition: u32);                  // read loop, at take time, BEFORE Fetch::seek
      fn flush(&self) -> BoxFuture<'_, Result<()>>;      // Sync* modes: resolves when the write landed
      // Health hooks, default no-ops (C4's MonitoredSink forwards them to the partition monitor):
      fn blocked(&self, partition: u32, blocked: bool);  // handler Block back-off entered / left
      fn read_failing(&self, partition: u32);            // transport outage: blocked, stall clock keeps running
      fn fetched(&self, partition: u32);                 // a read covering it returned (stall-clock progress)
      fn busy(&self, partition: u32, busy: bool);        // a batch handed to / finished by the handler
      fn stopped(&self, partition: u32, reason: &str);
  }
  struct NoPositions;                                    // PersistMode::None, work queue, tests

  // Per-partition stop input (C4 drives it; C3's contested callback and B5's lease loss end in revoke()).
  PartitionStop::new(&host_token) -> Self   // child token; Clone; its token is the handler's ctx token
  fn shutdown(&self)   // graceful: in-flight batch finishes and is recorded; queued batches drain
  fn revoke(&self)     // lost: stop; nothing queued is handled; NOTHING more is recorded
  fn is_stopped(&self) -> bool; fn is_revoked(&self) -> bool; fn token(&self) -> &CancellationToken
  ```
  Rules: (1) **positions are recorded when processed, never when read**: `record` follows a
  successful batch (or message, `SyncMessage`), a `BestEffort` skip, or a batch the type filter
  emptied; `last` covers filtered entries. (2) Under `PersistMode::None` the worker calls neither
  `record` nor `flush`. (3) Reset order in the read loop: `try_take` → `rewind` → `Fetch::seek`, all
  before the next fetch; the flusher deletes `__reset:<p>` only after `try_clear_taken` returns it.
  (4) The flusher only `request`s markers for partitions this instance owns. (5) After `revoke` the
  worker stops reporting that partition; the flusher keeps its own stood-down guard as well.
  (6) A fetch error carrying `fetch::UnreadableReply` (an `XREAD` reply that does not answer the read
  sent: bad shape, an id not after the cursor) is transient like an outage; the blocking fetch has
  already dropped that reader socket. Decode and other errors still end the loop.
- **C4** owns `host.rs` (`Streams::builder(conn)…spawn() -> StreamsHost`), new public API reviewed by
  the lead; C4b and C4t start from its first-commit skeleton.
- **C4 API** (`consumer::host`, re-exported from `consumer`; new public API, lead review):
  ```rust
  Streams::builder(conn: Connection) -> StreamsBuilder
  StreamsBuilder::keys(Keys) / options(StreamOptions) / allow_untrimmed(bool) / topic_options(topic, TopicOptions) -> Self
  StreamsBuilder::stream_options(&self) -> &StreamOptions
  StreamsBuilder::topic(topic) -> ConsumerBuilder        // seeded from the first `consumers` entry for it
  async StreamsBuilder::spawn(self) -> Result<StreamsHost> // startup checks fail here (Config / Redis / Transport)
  ConsumerBuilder::consumer(name) / options(ConsumerOptions) / configure(|&mut ConsumerOptions|) -> Self
  ConsumerBuilder::consumer_options(&self) -> &ConsumerOptions
  ConsumerBuilder::position_store<S: PositionStore>(S) -> Self   // C# IPositionStore override
  ConsumerBuilder::instance_id(Uuid) -> Self                       // default stable_instance_id(); tests run two instances
  ConsumerBuilder::handler<H: BatchHandler>(H) -> StreamsBuilder  // THE extension point
  StreamsHost: connection(), consumers(), consumers_started(), is_running(),
               owned_partitions(topic, consumer) -> Vec<u32>, async health() -> StreamHealthReport,
               async closed() -> Result<()> /* Err = a consumer failed the host */,
               async shutdown(self) -> Result<()>
  ```
  **C4b** adds every other registration form (message handlers via `PerMessage`, typed handlers
  via `Typed`, closures, `AddStream`-style sugar) as additional inherent `impl ConsumerBuilder` /
  `impl StreamsBuilder` blocks in `consumer/builder.rs`, each ending in `self.handler(adapter)`;
  it never edits `host.rs`. **C4t** drives tests through this surface only.
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

### D4 testing doubles (`testing.rs`, feature `testing`, public)
In-memory doubles for code that publishes or handles messages, with no Redis. **E2 and E4 tests
build on these.** Owner D4; the module is `redisevents::testing` and needs `features = ["testing"]`.
- `InMemoryPublisher::new(partitions: u32) -> Result<Self>` implements `Publisher`. Routing is the
  wire router (`partition_for_key`, a per-instance `RoundRobin` for empty keys), so partitions match
  production. An explicit partition wins and is range-checked. `published()`, `count()`,
  `for_partition(p)`, `partition_for(key)` (advances the round-robin for an empty key), `clear()`.
  Each `PublishedMessage` keeps `partition` (routed) apart from `requested_partition` (asked for).
  Ids are per instance from `1-0`; `clear()` does not reset them.
- `InMemoryBufferedPublisher::new(partitions)` implements `BufferedPublisher`. `enqueue` buffers and
  nothing is published until `flush`, so a test that forgets to flush fails. Direct publishes bypass
  the buffer. `pending()` / `pending_count()` inspect the buffer. `arm_flush_failure(Error)` makes the
  next flush fail and keep the batch (one-shot); `disarm_flush_failure()` cancels it. There is no
  background pump, batch trigger, bounded queue or backpressure: those are tested against Redis.
- `MemoryPositionStore::new()` implements `PositionStore` with the real load / save / reset rules
  (single-partition reset creates it; all-partition reset touches only stored positions).
- `StreamTestHarness::new(topic)` (plus `with_consumer`, `with_cancellation`) drives handlers the way
  the worker does, with no host:
  - `context(partition) -> PartitionContext` for direct calls.
  - `build_raw(msg_type, body, &PublishOptions) -> Result<StreamMsg>` and `build::<C, M>(&msg, &opts)`
    (type from `M::TYPE`, body from codec `C`) make the `StreamMsg` a handler receives. Options are
    stamped on it (partition, correlation id, headers). Ids are unique process-wide.
  - `deliver(&handler, &msg)` for a `MessageHandler`; `deliver_batch(&handler, &[StreamMsg])` for a
    `BatchHandler` (one call, partition from the first message, an empty batch still called).
  - `deliver_typed::<C, M, _>(&handler, &value, &opts)` and `deliver_typed_batch::<C, M, _>(..)` for
    `TypedHandler` / `TypedBatchHandler`, round-tripping through the codec as `Typed` does.
  - Each returns the handler's `HandleResult` unchanged, so `Ok`, `Err(Skip)` and `Err(Block)` are
    asserted directly. A handler panic becomes `Err(Skip)` (the worker's `catch_unwind` rule).
    **Not modelled:** `on_error` policy, `Block` retry and back-off, positions, the type filter,
    and the batch's pooled memory. Those are the worker's and are tested there.

```rust
use redisevents::testing::{InMemoryBufferedPublisher, StreamTestHarness};
use redisevents::producer::{BufferedPublisher, PublishOptions, Publisher};

let publisher = InMemoryBufferedPublisher::new(8)?;
publisher.enqueue("order-42", body, "OrderPlaced", &PublishOptions::new()).await?;
assert!(publisher.published().is_empty());          // buffered, not yet on the stream
publisher.flush().await?;
let sent = publisher.published();                   // sent[0].partition == publisher.partition_for("order-42")

let harness = StreamTestHarness::new("orders");
let msg = StreamTestHarness::build::<Json, OrderPlaced>(&event, &PublishOptions::new())?;
let outcome = harness.deliver(&my_handler, &msg).await;   // HandleResult, as the worker would see it
assert!(outcome.is_ok());
```

### E2 projections (`redisevents-projections`, public)
Typed event projections over any topic; E3 (event sourcing) and the Inventory view service (E5c)
build on it. A `Projector` is a core `BatchHandler`, so positions, replay and the `Skip` / `Block`
contract are core's.
```rust
// registry: explicit wire strings, never Rust type names
EventTypeRegistry::new()
    .register_json::<T>("wire.type")?                    // serde + Json (T: Serialize + DeserializeOwned)
    .register::<T, C: PayloadCodec>("wire.type")?        // any codec
    .register_with::<T>("wire.type", encode_fn, decode_fn)?   // hand-rolled pair 
    .register_family::<E, _>(&["w1", "w2"], |wire, body| -> Result<Option<E>, _>)?  // many wires -> one enum, decode-only
// encode::<T>(&T) -> (&str, Bytes); decode::<T>(wire, body) -> Result<Option<T>, BoxError>; try_decode -> Decoded
// duplicate wire / same type under two wires / blank wire -> Error::Config; encode of unregistered -> InvalidArgument

pub trait Projection<T>: Send + Sync + 'static {
    fn handle(&self, event: &T, meta: &EventMeta, ctx: &PartitionContext) -> impl Future<Output = HandleResult> + Send;
}
pub trait BatchFlush: Send + Sync + 'static {            // write once per batch
    type Pending: Default + Send + 'static;              // the batch's buffer, one per batch per projection
    fn flush(&self, pending: Self::Pending, ctx: &PartitionContext) -> impl Future<Output = HandleResult> + Send;
}
pub trait BatchProjection<T>: BatchFlush {              // buffer one event type into Pending
    fn buffer(&self, event: &T, pending: &mut Self::Pending, meta: &EventMeta, ctx: &PartitionContext) -> impl Future<Output = HandleResult> + Send;
}
Projector::builder(topic, registry)                      // registry: EventTypeRegistry or Arc<..>
    .projection::<T, _>(P: Projection<T>)                // .shared_projection::<T, _>(Arc<P>) to bind one instance for several T
    .batch_projection::<T, _>(P: BatchProjection<T>)     // .shared_batch_projection::<T, _>(Arc<P>)
    .build()? -> Projector                               // Config error if T is not decodable by the registry
// Projector: BatchHandler. Register it with the consumer like any batch handler.

pub struct EventMeta { partition_key, id: StreamId, correlation_id, partition: u32 }
pub trait ViewStore<T> { get(&str) -> Option<T>; set(&str, &T); delete(&str); list() -> Vec<T> }   // all async, Result<_, redisevents::Error>
InMemoryViewStore::<T>::new()                            // T: Clone
RedisViewStore::<T>::new(&Connection, &Keys, topic, owner, view_name)?     // owner is explicit (it is in the key)
```
- **Dispatch (no reflection).** Rust cannot enumerate a type's trait impls, so binding is explicit:
  `.projection::<T, _>(p)` once per event type. Decoding yields a type-erased value tagged with its
  `TypeId`; the projector looks the bound projections up by that id and a generic shim downcasts and
  calls `Projection<T>::handle`. Two shapes fit: one `T` per wire type, or **one enum `E` for
  the topic** via `register_family` (the inventory sample: `InventoryEvent::decode` plugs in as is and
  the projection `match`es the enum).
- **Errors:** unknown wire type skipped (warn once per type, then debug); known
  type without a bound projection skipped; a **malformed entry is skipped per entry** (error log with
  id and type, `redisevents.projections.events.malformed` counter, `malformed_events_total()`) and the
  rest of the batch is still projected; a projection error ends the batch and is returned unchanged.
  Batch projections are flushed once after the last event, only if handed an event; on an ordinary
  event error the events before it are still flushed and the original error returned (a `Block` from
  that flush wins); on `Block` or shutdown (`ctx.is_cancelled()`) nothing is flushed; several failing
  flushes all run and the first error is returned.
- **Batch buffer.** A batch projection's buffer is its `BatchFlush::Pending` value, created per
  `handle_batch` call, lent `&mut` to `buffer` for every event and moved into `flush`. The projector
  owns it, so there is nothing to look up, lock or close (no owner-address keying).
- **Diagnostics.** `tracing` spans `projections.project` (one per dispatched event) and
  `projections.flush`, with the display name in `otel.name` and failures as `otel.status_code = "ERROR"`.
  Feature `otel` exports the malformed counter on meter `RedisEvents.Projections`.
- **View key** `<prefix>{<topic>}:view:<owner>:<view_name>` (`Keys::view`), a hash of id to JSON.
  The owner is an explicit argument (a .NET service uses its entry assembly name unless
  configured, so a Rust reader of its views passes that name). `list` is an `HSCAN` loop.
- **JSON casing.** `Json` keeps Rust field names. .NET events are PascalCase, so types shared with
  a .NET service normally need `#[serde(rename_all = "PascalCase")]` (a mismatch is silent on the .NET side); per-type
  naming is plain serde attributes. A wrong-cased body fails to decode and is skipped as malformed.

```rust
// Inventory view service (E5c): both read models from one projection over the domain's event enum.
let registry = EventTypeRegistry::new().register_family::<InventoryEvent, _>(
    &[wire_type::CREATED, wire_type::RENAMED, wire_type::CHECKED_IN, wire_type::REMOVED, wire_type::DEACTIVATED],
    InventoryEvent::decode)?;

struct InventoryViews { list: Arc<RedisViewStore<InventoryItemListDto>>, detail: Arc<RedisViewStore<InventoryItemDetailsDto>> }
impl Projection<InventoryEvent> for InventoryViews {
    async fn handle(&self, event: &InventoryEvent, meta: &EventMeta, _: &PartitionContext) -> HandleResult {
        let id = event.id().to_string();
        let pos = StreamPosition { ms: meta.id.ms, seq: meta.id.seq };
        let current = self.detail.get(&id).await.map_err(HandleError::block)?;
        match project_detail(current.as_ref(), event, pos) {
            ViewChange::Set(row) => self.detail.set(&id, &row).await,
            ViewChange::Delete => self.detail.delete(&id).await,
            ViewChange::Unchanged => Ok(()),
        }.map_err(HandleError::block)
        // ... same for project_list
    }
}
let projector = Projector::builder(TOPIC, registry)
    .projection::<InventoryEvent, _>(views)
    .build()?;      // hand to the consumer as a BatchHandler
```
The full, tested version is `crates/redisevents-projections/tests/inventory.rs`. Tests use D4's
`StreamTestHarness::deliver_batch`.

### E3 event sourcing (`redisevents-eventsourcing`, public)
Wire-compatible with the .NET package: state stream
`<prefix>{<topic>}:state:es:<Aggregate>:<id>` (entries `b` + `t` only), version = its `XLEN`, every
event also published to the topic with the aggregate id as partition key. Tested both ways against
the .NET golden bodies (`tests/inventory.rs`).

**Design.** `Aggregate` is implemented on a **marker type** that names a `State`, an `Event`, the
wire `NAME` and a pure `apply(&mut State, &Event)`. `AggregateRoot<A>` is the instance: id, version,
state, pending events (`emit` = apply + queue, `pending`, `commit`, `from_history`, `version`),
and it **derefs to the state**. Why: a
pure decide/apply domain such as `inventory_domain::InventoryItem` needs no change and no wrapper
struct; the marker is local to the service crate, so the orphan rule is no obstacle (a trait impl
on `InventoryItem` from a third crate would be); id and version live outside the domain state, as
the repository owns them; one total `apply` replaces per-event handler tables.
```rust
pub trait Aggregate: Send + Sync + 'static {
    type State: Default + Send + Sync + 'static;  type Event: Send + Sync + 'static;
    const NAME: &'static str;
    fn apply(state: &mut Self::State, event: &Self::Event);
    // default: registry.encode(event). Override for an enum Event over several wire types.
    fn encode(registry: &EventTypeRegistry, event: &Self::Event) -> Result<(String, Bytes)>;
}
AggregateRoot::<A>::new(id)? ; .id() .version() -> u64 .state() .emit(e) .pending() .commit() ; AggregateRoot::from_history(id, events)?

pub trait EventRepository {   // load / save / save_unchecked
    async fn load<A>(&self, id) -> Result<Option<AggregateRoot<A>>>;     // pages of 1000, registry decode
    async fn save<A>(&self, &mut AggregateRoot<A>, expected_version: Option<u64>, &PublishOptions) -> Result<u64>;
    async fn save_unchecked<A>(&self, &mut AggregateRoot<A>, &PublishOptions) -> Result<u64>;
}
RedisEventRepository::new(StreamStore, EventTypeRegistry /* or Arc */)   // generic over `EventStore` for fakes
```
- `save` with nothing pending writes nothing and returns the version. A lost check is
  `Error::Concurrency(ConcurrencyError { aggregate_name, id, expected_version })`; nothing was
  written and the root keeps its pending events.
- `Error` = `Concurrency | Store(redisevents::Error) | History(unknown or undecodable wire type in
  an aggregate's own history: never skipped) | RefusedAfterEmitting | BlankId`.
- Runners:
```rust
execute(&repo, id, &ExecuteOptions, async move |root: &mut AggregateRoot<A>| -> Result<AggregateDecision<T>, E>)
    -> Result<CommandResult<T> /* {value, version} */, ExecuteError<E> /* Decide(E) | Repository(Error) */>
CachedAggregate::<A>::new().with_expected_version(fn).run(&repo, id, &ExecuteOptions, async |root, from_cache| ..)
    -> Result<T, ExecuteError<E>>        // &self: owns its lock, so share it directly; async evict(), is_cached()
ExecuteOptions::default().max_attempts(NonZeroU32) /*default 1*/ .concurrency_check(bool) /*false = save_unchecked*/
    .publish_options(PublishOptions) .on_conflict(|attempt, ConcurrencyError| async {..})
AggregateDecision::{Done(T), Refused(T) /* must emit nothing */, Redo /* reload, uncounted */}
```
  Nothing emitted: no save. Retry only on `Concurrency`, up to `max_attempts`;
  `on_conflict` runs before each retry, not after the final loss. `ExecuteError::is_concurrency()`
  is the 409 test. `Decide(E)` carries the domain error (400 in E5b) and saves nothing.
- Spans (`tracing`; display name in `otel.name`, `otel.kind=client`): `eventsourcing.save <Name>`
  (`eventsourcing.aggregate`, `.aggregate_id`, `.expected_version`, `.version`, `.event_count`),
  `eventsourcing.load <Name>` (`.found`, `.version`); a lost check sets `otel.status_code=ERROR` with a
  "Concurrency conflict" description. Counters `eventsourcing.saves|save.conflicts|loads` on meter
  `RedisEvents.EventSourcing` under feature `otel`; `diagnostics::{saves,save_conflicts,loads}_total()` always.
- **Send gotcha.** The futures are `Send` (a handler can be `tokio::spawn`ed), but a closure that
  *borrows* a local (`async |root| tick(root, n)`) trips rustc's "Send is not general enough";
  write `async move |root| ..` when the closure captures anything.
- **Not provided**: benchmarks, `BindId`, a per-event handler table, an `Id` override (the id is the root's).

```rust
// E5b command service: the pure InventoryItem on the abstraction.
struct Inventory;                                   // marker, local to the service crate
impl Aggregate for Inventory {
    type State = InventoryItem;  type Event = InventoryEvent;  const NAME: &'static str = AGGREGATE_NAME; // "Inventory"
    fn apply(s: &mut InventoryItem, e: &InventoryEvent) { s.apply(e) }
    fn encode(_: &EventTypeRegistry, e: &InventoryEvent) -> Result<(String, Bytes)> {
        Ok((e.wire_type().to_owned(), Bytes::from(e.encode())))
    }
}
let registry = EventTypeRegistry::new()
    .register_family::<InventoryEvent, _>(&[CREATED, RENAMED, CHECKED_IN, REMOVED, DEACTIVATED], InventoryEvent::decode)?;
let repo = RedisEventRepository::new(StreamStore::new(conn, keys, TOPIC, topic_options)?, registry);

// POST check-in: domain error -> 400, conflict -> 409 (max_attempts retries first), missing item -> 404.
let out = execute(&repo, &id.to_string(), &ExecuteOptions::default().max_attempts(3),
    async move |item: &mut AggregateRoot<Inventory>| {
        if item.version() == 0 { return Ok(AggregateDecision::Refused(NotFound)); }
        let e = item.check_in(count, price)?;       // &InventoryItem via Deref; DomainError via `?`
        item.emit(e);
        Ok::<_, DomainError>(AggregateDecision::Done(Ok))
    }).await   // Err(ExecuteError::Decide(DomainError)) | Err(e) if e.is_concurrency()
// create: item.emit(InventoryItem::create(id, &name)?)
```

### E4 Restate starts (`redisevents-restate`, public)
```rust
RestateStartOptions::new(ingress: Url)          // pub fields: handler ("run"), max_parallelism: NonZeroUsize (8),
                                                // max_attempts: NonZeroU32 (5), initial/max_retry_delay, attempt_timeout
RestateStart::new(workflow, key)                // .with_input(&T) -> Result<Self, serde_json::Error>
RestateStartHandler::new(Client, options, keep: Fn(&TypedMsg<T>) -> bool,
                         map: Fn(&TypedMsg<T>) -> Result<RestateStart, BoxError>) -> Result<_, RestateConfigError>
// a TypedBatchHandler<T>: wrap in `Typed`. Ok only when every kept start was accepted, else
// Err(Block(RestateStartBlocked)); never Skip.
```
The ingress request is unchanged: `POST {ingress}/{workflow}/{key}/{handler}/send`, JSON body,
`traceparent`, no `Idempotency-Key`; 2xx accepted, timeout / transport / 408 / 425 / 429 / 5xx
retried in place with doubling delay, anything else fails the item and blocks the batch. There are
no per-handler counters: observe through the `restate.start` debug span and the ingress itself.

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
  **C3 handoff (crate-private, `positions::flusher`)** for C4:
  ```rust
  FlusherOptions::new(topic, consumer, partitions, interval /*persist_interval_ms*/, instance_id)
      // then set: redis: Some((conn, keys))          second-writer check + reset polling (Redis store only)
      //           owned: Some(registry.subscribe())  only held partitions are written / have markers requested
      //           resets: Some(Arc<ResetSignal>)     the same signal the read loops take from
      //           reset_poll_interval, exclusive_claims (= InstanceMode::Lease),
      //           contested_grace (contested_grace_seconds), recheck_interval (contested_recheck_seconds),
      //           on_contested: Arc<dyn Fn(p, mine, theirs)>, on_recovered: Arc<dyn Fn(p, theirs)>
  PositionFlusher::new(store: S /*PositionStore*/, FlusherOptions) -> Result<Self>  // wrap in Arc
  impl PositionSink for PositionFlusher<S>      // Arc<PositionFlusher<S>> -> Arc<dyn PositionSink> for workers
  fn start(&self)                               // spawns the timer; idempotent
  async fn stop(&self)                          // stops the timer, then one final flush
  async fn dispose(&self)                       // stop + zero the contention gauge
  fn attach(&self, p, PartitionStop); fn detach(&self, p)   // the stop revoked on a stand-down
  async fn flush_now / recheck / poll_resets    // one round now (tests, diagnostics)
  dirty_count, flushes, failures, contested_count, contention_count, recoveries,
  resets_applied, backwards_records, is_stood_down(p), reported(p)   // counters for health/tests
  ```
  Rules for C4: one flusher per consumer; `PersistMode::None` uses `NoPositions` instead. Attach
  each worker's `PartitionStop` when starting it (again after `on_recovered`). On a stand-down the
  flusher revokes the attached stop and then calls `on_contested`; C4 calls `ResetSignal::forget`
  and must not restart the partition until `on_recovered`. **Shutdown order**: stop workers →
  `flusher.stop()` (final flush) → `registry.stop()` (a released registry holds nothing, and
  positions of partitions not held are dropped, not written). Cold start: `reset_marker::take_all`
  before any worker or the flusher starts.
  Behaviour: a timer tick writes all dirty partitions in one `save` (nothing when idle), then polls
  markers, then re-arbitrates stood-down partitions. A failed write is re-queued, logged at warn
  and counted on `streams.positions.flush_failures`; `PositionSink::flush` also returns the error
  (the worker applies `HandleError::from`). A dropped flush re-queues what it drained. While a
  reset is pending for a partition the flusher does not write it (keeps the admin target, and the
  trim clamp, in place) until the read loop's `rewind`; the marker field is then removed with a
  compare-and-delete, so a newer marker survives.
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
  - **Quarantine** (C4's stall stand-down): `quarantine(&[u32], Duration)` and `quarantined()`.
    Until it ends (tokio clock), every lease cycle passes the quarantined partitions as the lease
    script's trailing `ARGV` (WIRE.md §8): held ones are released, none are claimed, and the
    presence field is deleted, so the instance leaves the fair share. It ends on its own. Static
    mode ignores it. C4 never starts a read side on a quarantined partition, even one still held.
- **B1**: `admin/meta.rs` (`ensure_topic_meta`), used by `StreamPublisher::new` and C4 startup.
- **D1** / **D2**: `admin/stream_admin.rs` / `admin/health.rs`, new public API reviewed by the lead;
  D3 consumes it.
- **D1 (landed)**, `admin::stream_admin`, public API for D3 and the G3 interop host:
  - `StreamAdmin::new(conn: Connection, keys: Keys)` (cheap to clone). All methods are `async`, take
    `topic` / `consumer` as `&str`, and return `Result`.
  - `reset_position(topic, consumer, ResetTarget, &ResetOptions) -> Vec<(u32, StreamId)>` and
    `preview_reset(..) -> ResetPreview` (same arguments, changes nothing). `ResetTarget` is
    `Time(OffsetDateTime)` (id just before `<ms>-0`, so an entry on that millisecond is delivered),
    `Id`, `Start` (`0-0`) or `End` (each tail; `Error::Config` if a stream does not exist).
    `ResetOptions { partition: Option<u32>, topic: Option<TopicOptions>, max_count_scan, start_from_when_missing }`
    (`Default`: all partitions, count/layout read from `m:{topic}`, cap `DEFAULT_MAX_COUNT_SCAN` =
    250_000, `Beginning`). A partition past the known count, or an unknowable count for a
    whole-topic reset, is `Error::Config`.
  - A reset writes `<p>` = `<id>|<ADMIN_INSTANCE_ID>` and `__reset:<p>` = `<id>|<issuedUtc>` (one
    `HSET`, via B4's `RedisPositionStore::reset_targets`, crate-private) plus the `:meta` fields.
  - `ResetPreview { topic, consumer, requested_from, partitions: Vec<PartitionResetPreview> }` with
    `total_to_reprocess()`, `total_to_skip()`, `any_trimmed_away()`, `describe()`;
    `PartitionResetPreview { partition, current, target, entries, entries_exact (false = floor),
    oldest, newest, length, direction: ResetDirection, trimmed_away, assumed_start }`.
  - `get_ownership(topic, consumer, partitions /* 0 = from meta */) -> OwnershipMap` (`unowned`
    filled, `contested` always empty), `read_topic_tail(topic, &TopicOptions) -> Vec<StreamId>`,
    `read_topic_meta(topic) -> BTreeMap<String, String>`, `read_topic_layout(topic) -> TopicOptions`,
    `StreamAdmin::reset_marker_field(p)`; consts `RESET_MARKER_PREFIX`, `DEFAULT_MAX_COUNT_SCAN`.
  - A2's `ResetOnStart` refusal names `StreamAdmin::reset_position`; that is this method.
  **D2 public API (`admin/health.rs`, added by D2, lead review):**
  - Rule `Stalled` (Unhealthy, graded first): `stalled_ms >= unhealthy_stalled_seconds` or
    `stood_down_stalled`. The stall clock runs on this process's own clock: no read returned, no
    batch completed, a batch held too long, or position writes failing. A handler `Block` and a
    policy stop suspend it; a transport block does not. In Lease mode C4 checks it every second and
    stands the partition down: monitor marked stalled-out and kept registered until the partition
    is read again or the host stops, stop revoked, `quarantine(max(2 x TTL, threshold))`, reader
    sockets recycled, a lease cycle run at once, read side rebuilt. A non-fatal read-loop failure
    (or panic) is stood down the same way at once. Static mode only logs (once) and grades.
  - `health::check(connection: Option<&Connection>, consumers: u32, consumers_started: u32) -> StreamHealthReport`
    (async): the readiness answer. Reads `health::partitions()`, a `PING` (1 s bound) when a
    connection is given, and `OwnershipRegistry::any_degraded()`. `consumers` / `consumers_started`
    are the consumer host's counts (`0, 0` when none); C4 supplies them.
  - `health::evaluate(&[StreamPartitionStatus], &StreamHealthContext) -> StreamHealthReport`
    (pure), `health::grade(&StreamPartitionStatus) -> StreamPartitionHealth`,
    `health::find_silent(&[..]) -> Option<StreamSilentConsumer>`.
  - Types: `StreamHealthStatus` and `StreamHealthRule` (`#[non_exhaustive]`, `Copy`),
    `StreamPartitionRunState`, `StreamPartitionHealth { status, rule }`, `StreamHealthContext`
    (pub fields, `Default`), `StreamHealthReport { status, rule, description, details:
    StreamHealthDetails, partition }` (`StreamHealthDetails`: typed `Option` fields, `Serialize`),
    `StreamSilentConsumer`, `StreamPartitionStatus` (pub fields: one snapshot per partition, C# names
    in snake_case, `StreamId` positions, `f64` milliseconds, `-1.0` for "never sampled").
  - `health::any()`, `health::partitions()`: snapshots of the process-wide monitors.
  - `health::{DEFAULT_SAMPLE_INTERVAL (15 s), DEFAULT_PUSH_INTERVAL (1 s), TAIL_STALE_AFTER (60 s)}`,
    `health::estimate_entries_behind(length, first, last, processed) -> i64`.
  - Crate-private (C2/C4 wire these, `admin/health/monitor.rs`): `PartitionMonitor` (created with
    `monitor::track(..)`, fed by `observe`, `set_tail`, `mark_blocked/unblocked/stopped/running`,
    `set_lag_entries`, and the stall clock: `mark_fetched`, `mark_busy/idle`,
    `mark_transport_blocked`, `set_persist_failing`, `mark_stalled_out`; `monitor::forget` removes
    only that monitor, never a newer one registered under the same key), `StreamLagSampler::start(conn, sample, push)` (publishes `streams.lag.ms`,
    samples `XINFO STREAM` into `streams.lag.entries` and `streams.stream.length`).
  - Gauges: `streams.lag.entries`, `streams.lag.ms`, `streams.stream.length` (existing B6 helpers),
    plus `streams.blocked` / block duration from the monitor.
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
- **Projection dispatch** across many event types (E2) without reflection: decided, see "E2
  projections" in §4 (explicit `TypeId` bindings, or one enum per topic via `register_family`).
- **`StreamMsg` string storage** may move from `String` to `Bytes`-backed strings in F1; accessors
  keep that invisible.
