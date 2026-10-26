# redisevents

Partitioned topics on Redis Streams, with per-key ordering, at-least-once delivery and consumer
positions stored in Redis. This is the core crate. It is the Rust port of the C# `RedisEvents`
core package, and it writes and reads the same bytes: a publisher in one language feeds a consumer
in the other, on the same Redis, the same topics and the same positions.

Read the error contract below **before** writing a handler. It is the part of this library that is
not discoverable from the API: the compiler accepts a handler that silently drops money.

> Every Rust code block on this page is compiled as a doctest (`cargo test --doc -p redisevents`).
> Blocks marked `no_run` compile but do not execute, because they need a live Redis. If you change
> a sample, the build is what checks it.

---

## The error contract

Four rules, and they are the whole thing:

1. **An ordinary error means the batch is logged and skipped.** A handler returns `Err` with a
   `HandleError::Skip`, which is what `?` produces for any error that is not the crate's own
   `Config` or `Transport`. The position advances past the batch and the partition keeps moving.
   This is `ErrorPolicy::BestEffort`, and it is the default. There is no dead-letter queue.
2. **The library does no retrying.** A transient failure is retried by the handler, if at all. The
   library cannot know whether your work is idempotent or whether a second attempt would help.
3. **A message that must not be skipped returns `HandleError::Block`.** The partition then stops
   advancing, retries the **same batch** on a back-off (1s, 2, 4, 8, 16, then 30s, forever) and
   other partitions keep running. `streams.blocked` is 1 while it lasts.
4. **Blocking is not free.** Lag grows for as long as the block lasts. A long enough block plus
   `MaxLen` trimming means the upstream end of the stream is thrown away while you are stuck. Block
   for "Redis is down" and "the ledger is unreachable". Do not block for "this one message is
   malformed", which will still be malformed in six hours.

### The decision table

| Situation | What to do |
|---|---|
| Malformed message, will never succeed | Return the error with `?`: log and skip is correct |
| Transient downstream failure, retry will fix it | Retry inside the handler, with a bounded number of attempts |
| Downstream is down and skipping loses money | Return `HandleError::block(e)` (or `.or_block()?`) |
| Message is important but not urgent | Park it somewhere durable, then return `Ok(())` |

**Row 1: malformed.** Do nothing special. The `?` on a decode error skips the message.

```rust
use redisevents::consumer::{BatchHandler, HandleResult, PartitionContext};
use redisevents::StreamMsg;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: String,
}

struct MalformedRow;

impl BatchHandler for MalformedRow {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        for msg in batch {
            // A body that will never parse: serde_json::Error is an ordinary error, so this is a
            // Skip. The batch is logged at error level and the partition moves on.
            let order: Order = serde_json::from_slice(msg.body())?;
            println!("order {}", order.id);
        }
        Ok(())
    }
}
```

**Row 2: transient.** Retry in the handler, and bound the attempts. An unbounded in-handler retry is
a block with none of its observability: `streams.blocked` reads zero while the partition goes
nowhere. After the last attempt, hand the failure to `or_block()` or `or_skip()` explicitly.

```rust
use std::time::Duration;

use redisevents::consumer::{BatchHandler, HandleResult, PartitionContext, ResultExt};
use redisevents::StreamMsg;

struct Pricing;

impl Pricing {
    async fn price(&self, _body: &[u8]) -> Result<(), std::io::Error> {
        Err(std::io::Error::other("pricing service is busy"))
    }
}

impl BatchHandler for Pricing {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        for msg in batch {
            let mut attempt = 0;
            loop {
                match self.price(msg.body()).await {
                    Ok(()) => break,
                    Err(_) if attempt < 3 => {
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                    }
                    // The retries are exhausted. Blocking retries the whole batch; use `or_skip()`
                    // instead if losing this message is acceptable.
                    Err(e) => return Err(e).or_block(),
                }
            }
        }
        Ok(())
    }
}
```

**Row 3: must not skip.** Return `HandleError::block`. The type of the error does not matter, only
which constructor you call.

```rust
use redisevents::consumer::{BatchHandler, HandleError, HandleResult, PartitionContext};
use redisevents::StreamMsg;

struct Ledger;

impl Ledger {
    async fn post(&self, _body: &[u8]) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl BatchHandler for Ledger {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        for msg in batch {
            if let Err(e) = self.post(msg.body()).await {
                // Skipping this loses money, so block. Lag grows while the ledger is down, which
                // is the deliberate trade. The partition retries this batch from its first entry.
                return Err(HandleError::block(e));
            }
        }
        Ok(())
    }
}
```

**Row 4: important but not urgent.** Park the message and return normally. The partition keeps
moving and nothing is lost; the message is now someone else's problem, on purpose. Copy what you
keep. `StreamMsg` owns its data, so `clone()` is cheap (the body is a reference-counted `Bytes`).

```rust
use redisevents::consumer::{MessageHandler, HandleResult, PartitionContext};
use redisevents::StreamMsg;

struct ParkingLot;

impl ParkingLot {
    async fn park(&self, _msg: StreamMsg) -> Result<(), std::io::Error> {
        Ok(())
    }
}

struct Parking(ParkingLot);

impl MessageHandler for Parking {
    async fn handle(&self, msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
        if let Err(e) = self.0.park(msg.clone()).await {
            // Parking itself failed: that is a must-not-skip failure.
            return Err(redisevents::HandleError::block(e));
        }
        Ok(())
    }
}
```

### `ErrorPolicy`: what an ordinary error does

`Streams:Consumers[n]:OnError` (`ConsumerOptions::on_error`) picks what a `Skip` does. A `Block`
ignores this setting: it always blocks.

| `ErrorPolicy` | Behaviour | When |
|---|---|---|
| `BestEffort` (default) | Log at error level, count `streams.errors`, advance past the batch | Almost always. A poison message must not wedge a partition forever |
| `StopPartition` | Log, do **not** advance, stop this partition; the others keep running | Rare. Prefer `Block`, which retries instead of standing still |
| `Fail` | Log, fail the host. `StreamsHost::closed()` returns the error and the host shuts itself down gracefully | Only for a consumer that cannot meaningfully run degraded. Let the orchestrator restart the process |

A panic inside a handler is caught by the worker and treated as a `Skip`.

`Error::Config` and `Error::Transport` convert to `Block` with `?`: they are the crate's own
"Redis or the configuration is wrong" errors, and a skip would lose messages during an outage. A raw
`redis::RedisError` converts to `Skip`. If a handler calls Redis directly, use `.or_block()` on
that call.

---

## Quickstart

A consumer host is built from a builder: pick a topic, register a handler, and spawn.

```rust
use redisevents::consumer::{BatchHandler, HandleResult, PartitionContext, Streams};
use redisevents::{Connection, StreamMsg};

struct Orders;

impl BatchHandler for Orders {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        for msg in batch {
            println!("{} {}", msg.id(), msg.msg_type());
        }
        Ok(())
    }
}

async fn run() -> redisevents::Result<()> {
    let conn = Connection::connect("redis://redis-db.infra:6379").await?;
    let host = Streams::builder(conn)
        .topic("orders")
        .handler(Orders)
        .spawn()
        .await?;
    // ... until shutdown
    host.shutdown().await
}
```

The handler is called once per **batch**. A single-message handler and a closure are the same
machinery underneath, with the same positions and the same error contract:

```rust
use redisevents::consumer::{HandleResult, MessageHandler, PartitionContext, Streams};
use redisevents::{Connection, StreamMsg};

struct OneOrder;

impl MessageHandler for OneOrder {
    async fn handle(&self, msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
        println!("{}", msg.id());
        Ok(())
    }
}

async fn run() -> redisevents::Result<()> {
    let conn = Connection::connect("redis://redis-db.infra:6379").await?;
    let host = Streams::builder(conn)
        .topic("orders")
        .message_handler(OneOrder)
        .spawn()
        .await?;
    host.shutdown().await
}
```

For a consumer too small to justify a type, take a closure over the batch (the closure receives the
messages by value):

```rust
use redisevents::consumer::Streams;
use redisevents::Connection;

async fn run() -> redisevents::Result<()> {
    let conn = Connection::connect("redis://redis-db.infra:6379").await?;
    let host = Streams::builder(conn)
        .topic("orders")
        .batch_fn(|batch, _ctx| async move {
            for msg in &batch {
                println!("{} bytes", msg.body().len());
            }
            Ok(())
        })
        .spawn()
        .await?;
    host.shutdown().await
}
```

There is no envelope. `StreamMsg::body()` is exactly the bytes the publisher passed in, and
decoding is yours. That keeps this crate free of any dependency on your message model.

---

## Configuration

The `Streams` section has the same keys in C# and Rust. The Rust binder reads the JSON form of the
section with `redisevents::config::binder::bind_json`. Key names are case-insensitive, and so are
enum values (`lease` and `Lease` are the same).

```rust
use redisevents::config::binder::bind_json;

fn configured() -> redisevents::Result<redisevents::config::StreamOptions> {
    bind_json(
        r#"{
            "Streams": {
                "Consumer": "bet-processing",
                "Topics": {
                    "orders": { "Partitions": 4, "MaxLen": 100000, "Trim": "Approx" }
                },
                "Consumers": [
                    { "Topic": "orders", "BatchSize": 100, "Persist": "AsyncBatch", "OnError": "BestEffort" }
                ]
            }
        }"#,
    )
}

# fn main() -> redisevents::Result<()> {
let options = configured()?;
assert_eq!(options.topics["orders"].partitions, 4);
assert_eq!(options.consumers[0].batch_size, 100);
# Ok(())
# }
```

Hand the options to the builder with `.options(options)`, then pick the registration by topic or by
index. `consumer_at(i)` takes `Streams:Consumers[i]`, and `sole_consumer()` takes the only entry:

```rust
use redisevents::config::StreamOptions;
use redisevents::consumer::{BatchHandler, HandleResult, PartitionContext, Streams};
use redisevents::{Connection, StreamMsg};

struct Orders;

impl BatchHandler for Orders {
    async fn handle_batch(&self, _batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        Ok(())
    }
}

async fn run(options: StreamOptions) -> redisevents::Result<()> {
    let conn = Connection::connect("redis://redis-db.infra:6379").await?;
    let host = Streams::builder(conn)
        .options(options)
        .consumer_at(0)?
        .handler(Orders)
        .spawn()
        .await?;
    host.shutdown().await
}
```

Settings can also be changed in code, after the topic is chosen:

```rust
use redisevents::config::PersistMode;
use redisevents::consumer::{BatchHandler, HandleResult, PartitionContext, Streams};
use redisevents::{Connection, StreamMsg};

struct Orders;

impl BatchHandler for Orders {
    async fn handle_batch(&self, _batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        Ok(())
    }
}

async fn run() -> redisevents::Result<()> {
    let conn = Connection::connect("redis://redis-db.infra:6379").await?;
    let host = Streams::builder(conn)
        .topic("orders")
        .configure(|c| {
            c.batch_size = 250;
            c.persist = PersistMode::SyncBatch;
        })
        .handler(Orders)
        .spawn()
        .await?;
    host.shutdown().await
}
```

### The keys

| Key | Default | What goes wrong |
|---|---|---|
| `ConnectionString` | `redis-db.infra:6379,abortConnect=false` | Build the connection from it with `Connection::from_options(&options)` (see [Connecting from the config](#connecting-from-the-config)); the host itself takes a `Connection` and only uses the string for the `BlockMs` check. Point it at `redis-cache` and you lose everything on restart |
| `Consumer` | executable file name | Two services sharing a name share a position hash and each see half the messages |
| `Topics:<t>:Partitions` | `1` | Can be increased, never decreased. Caps parallelism. See [Interop](#interop-rules) for the C# README's `2` |
| `Topics:<t>:MaxLen` | `10000` | Below worst-case consumer lag, trimming eats unprocessed messages |
| `Topics:<t>:Trim` | `Approx` | `None` is refused outside a `Development` environment |
| `Topics:<t>:StateMetadata` | `false` | State-store entries hold only body and type; `true` also keeps correlation id, trace and headers |
| `Consumers[n]:BatchSize` | `100` | Bigger batches amortise round trips and widen the redelivery window |
| `Consumers[n]:Persist` | `AsyncBatch` | `SyncBatch` and `SyncMessage` cost a Redis round trip. `None` never stores a position and is refused with `StartFrom: Stored` |
| `Consumers[n]:PersistIntervalMs` | `1000` | This is your duplicate window on a crash |
| `Consumers[n]:OnError` | `BestEffort` | See the table above |
| `Consumers[n]:BlockMs` | `1000` | Must stay under the connection's sync timeout (`5000` ms default), or it is refused at startup |
| `Consumers[n]:StartFrom` | `Stored` | `Stored` falls back to `StartFromWhenMissing`, then `Beginning`. `Date` needs `StartFromDate` |
| `Consumers[n]:ResetOnStart` | `false` | Refused at startup. Use `StreamAdmin::reset_position` instead |
| `Consumers[n]:Backpressure:Capacity` | `4` | Batches in flight per partition |
| `Consumers[n]:Delivery` | `Ordered` | `WorkQueue` is refused at startup in this version (see below) |
| `Consumers[n]:Instances:Mode` | `Lease` | `Static` needs an index and count, see [Ownership](#ownership-lease-and-static) |
| `Consumers[n]:UnhealthyBehindSeconds` | `0` (off) | Health: the only threshold safe to restart on |

Misconfiguration returns `Error::Config` naming the offending key. The config errors are returned by
`bind_json`, and by `spawn()` for the rules that need the whole picture.

Two things differ from the C# host, and both are easy to miss:

- **The host starts only the consumers you register.** `Streams:Consumers` entries seed the builder
  (the options for `topic("orders")` come from the first entry for that topic), but an entry with no
  handler is not started.
- **The host does not open the connection.** Build a `Connection` yourself (`Connection::connect`
  takes a redis-rs URL, `Connection::from_options` the bound config) and make sure it points at
  `redis-db`.

### Connecting from the config

`Connection::from_options` reads `Streams:ConnectionString` from the bound `StreamOptions` and
translates the StackExchange.Redis string (`host:6379,password=...,ssl=true,defaultDatabase=2`)
into a redis-rs URL, so the same config file serves the C# and the Rust runtime. An unset string
means the C# default (`redis-db.infra:6379`). `Connection::url_from_options` returns the URL
without connecting.

```rust,no_run
use redisevents::Connection;
use redisevents::config::binder::bind_json;

# async fn run() -> redisevents::Result<()> {
let options = bind_json(r#"{"ConnectionString": "redis-db.infra:6379,password=secret"}"#)?;
let conn = Connection::from_options(&options).await?;
# let _ = conn;
# Ok(())
# }
```

A malformed string is an `Error::Config` naming `Streams:ConnectionString`; the message never
echoes the password.

### TLS

`ssl=true` produces a `rediss://` URL. Enabling TLS needs the `tls-rustls` cargo feature
(`redisevents = { version = "0.1", features = ["tls-rustls"] }`), which turns on redis-rs's rustls
support with the bundled webpki root certificates. Without it, connecting to a `rediss://` URL
fails with a redis error saying TLS is not compiled in.

---

## Publishing

A publisher writes one entry per message. The partition key decides the partition: equal keys land
on the same partition and are ordered against each other. An empty key round-robins, which is right
for something with no per-entity ordering and wrong for anything with a lifecycle.

```rust
use bytes::Bytes;
use redisevents::config::TopicOptions;
use redisevents::producer::{PublishOptions, Publisher, StreamPublisher};
use redisevents::wire::Keys;
use redisevents::{Connection, StreamId};

async fn place_order(order_id: &str, body: Bytes) -> redisevents::Result<StreamId> {
    let conn = Connection::connect("redis://redis-db.infra:6379").await?;
    let publisher = StreamPublisher::new(
        conn,
        Keys::from_env(),
        "orders",
        TopicOptions {
            partitions: 4,
            ..TopicOptions::default()
        },
    )
    .await?;
    publisher
        .publish(order_id, body, "OrderPlaced", &PublishOptions::new())
        .await
}
```

`StreamPublisher::new` ensures the topic's metadata first. It refuses a partition count below the
recorded one (see [Sharp edges](#sharp-edges)). Build one publisher per topic and share it: it is
cheap to clone through `Arc`, and `publish` is `&self`.

Options that a message carries go on `PublishOptions`: a correlation id, headers, an explicit
partition (checked against the count), and a trace parent.

```rust
use redisevents::producer::PublishOptions;

let options = PublishOptions::new()
    .correlation_id("req-7f3a")
    .header("tenant", "acme")
    .partition(2);
# let _ = options;
```

### Buffered publishing

`BufferedStreamPublisher` acknowledges into an in-memory queue and flushes in the background, in
batches. It is the right trade for telemetry and the wrong one for money.

```rust
use bytes::Bytes;
use redisevents::config::{ProducerOptions, TopicOptions};
use redisevents::producer::{
    BufferedPublisher, BufferedStreamPublisher, PublishOptions, StreamPublisher,
};
use redisevents::wire::Keys;
use redisevents::Connection;

async fn telemetry(conn: Connection) -> redisevents::Result<()> {
    let inner = StreamPublisher::new(
        conn,
        Keys::from_env(),
        "telemetry",
        TopicOptions::default(),
    )
    .await?;
    let buffered = BufferedStreamPublisher::new(inner, ProducerOptions::default());
    buffered
        .enqueue("sensor-1", Bytes::from_static(b"{}"), "Reading", &PublishOptions::new())
        .await?;
    // A flush waits for the queued entries that it covers and reports their first error.
    buffered.flush().await?;
    buffered.shutdown().await
}
```

**Buffered publishes are lost on a crash.** An enqueued message that has not been flushed is lost
if the process dies. It is not delayed or retried on restart: it is gone, with no record that it
existed. `flush()` is the only way to make a buffered publish durable. Use the outbox, below, where
the publish must survive the process. `shutdown()` drains with a 5-second bound and logs what it
abandons.

---

## Typed messages

The calls above take bytes and a type string. When a topic carries one Rust type, implement
`Message` on it and let the typed API serialise it:

```rust
use redisevents::consumer::{HandleResult, PartitionContext, Streams, TypedHandler};
use redisevents::{Connection, Message, TypedMsg};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    order_id: String,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "Shop.OrderPlaced";
}

struct Shipping;

impl TypedHandler<OrderPlaced> for Shipping {
    async fn handle(&self, msg: &TypedMsg<OrderPlaced>, _ctx: &PartitionContext) -> HandleResult {
        println!("ship {}", msg.value.order_id);
        Ok(())
    }
}

async fn run() -> redisevents::Result<()> {
    let conn = Connection::connect("redis://redis-db.infra:6379").await?;
    let host = Streams::builder(conn)
        .topic("orders")
        .typed_handler::<OrderPlaced, _>(Shipping)
        .spawn()
        .await?;
    host.shutdown().await
}
```

Publishing the same type uses the codec-aware extension trait, which defaults to JSON:

```rust
use redisevents::producer::{PublishOptions, PublisherExt, StreamPublisher};
use redisevents::{Json, Message};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    order_id: String,
}

impl Message for OrderPlaced {
    const TYPE: &'static str = "Shop.OrderPlaced";
}

async fn place(publisher: &StreamPublisher, order: &OrderPlaced) -> redisevents::Result<()> {
    publisher
        .publish_message::<Json, _>("order-1", order, &PublishOptions::new())
        .await?;
    Ok(())
}
```

The type string is always explicit (`Message::TYPE`). It is not the Rust type name, and there is
no default derived from it. See [Interop](#interop-rules).

Bodies are JSON with the field names as written. C# events are PascalCase, so a type shared with C#
needs `#[serde(rename_all = "PascalCase")]`. The C# default is different and it is silent (see
Interop).

---

## The outbox

The problem the outbox solves is the two-write gap: a service writes its state, then publishes the
event that announces it, and dies in between. The state says the order is placed; the stream says
nothing happened.

Here the state store and the broker are the same Redis, so the classic outbox table and relay
collapse into one `MULTI`/`EXEC`. The state write is queued into a pipeline; it must not run it.

```rust
use redisevents::config::TopicOptions;
use redisevents::producer::{Outbox, OutboxPublish, PublishOptions};
use redisevents::wire::Keys;
use redisevents::{Connection, StreamId};

async fn place_order(conn: Connection, order_id: &str, body: &[u8]) -> redisevents::Result<StreamId> {
    let keys = Keys::from_env();
    let outbox = Outbox::new(conn, keys.clone());
    let state_key = Outbox::state_key(&keys, "orders", &format!("order:{order_id}"))?;
    let options = PublishOptions::new();
    let topic_options = TopicOptions::default();
    let publish = OutboxPublish {
        topic: "orders",
        partition_key: order_id,
        body,
        msg_type: "OrderPlaced",
        options: &options,
        topic_options: &topic_options,
    };
    outbox
        .write_and_publish(
            |tx| {
                tx.hset(&state_key, "status", "placed").ignore();
            },
            &publish,
            &[&state_key],
        )
        .await
}
```

Four things about it are load-bearing:

1. **Queue, do not run.** The closure only adds commands to the pipeline. Its commands run inside
   the `EXEC`, after the closure returns.
2. **`MULTI`/`EXEC` is isolation, not rollback.** No other client sees a half-applied state, and
   neither write is lost to a crash between them. It is not "both or neither": a command that fails
   at run time does not stop the others. Handlers still have to be idempotent.
3. **One hash slot.** Stream keys carry the topic's hash tag, so a state key must carry the same
   tag. `Outbox::state_key(&keys, topic, name)` builds one, and passing the state keys gets the tag
   checked before anything is sent. Otherwise a cluster would fail later with `CROSSSLOT`. (Cluster
   deployments are not supported yet, see below.)
4. **Dedicated connection, not the shared one.** The outbox runs on its own connection, because
   `WATCH`-style connection state must not be shared. Pass the `Connection`; the outbox takes care of
   the rest.

Optimistic concurrency is different in this crate. The outbox has no `conditions` argument. For
"append only if the stream is still at version N", use `StreamStore`, which is the event-sourcing
primitive (`append_and_publish` returns `None` when the version moved, and nothing is written):

```rust
use redisevents::config::TopicOptions;
use redisevents::producer::{StateEvent, StreamStore};
use redisevents::wire::Keys;
use redisevents::Connection;

async fn append(conn: Connection, expected: u64) -> redisevents::Result<()> {
    let store = StreamStore::new(conn, Keys::from_env(), "orders", TopicOptions::default())?;
    let written = store
        .append_and_publish(
            "order-42",
            expected,
            "order-42",
            &[StateEvent::new(b"{\"status\":\"placed\"}".to_vec(), "OrderPlaced")],
        )
        .await?;
    if written.is_none() {
        // Someone else appended first. Nothing was written: reload and decide again.
    }
    Ok(())
}
```

`StreamStore` requires co-located partitions (the default), because its state streams share the
topic's hash tag.

### Forwarding one topic onto another

A service that consumes one topic and announces what it saw on another cannot join the source's
transaction, and its read is at-least-once. `TopicForwarder` makes forwarding exactly-once in
effect, with a durable high-water mark per source and a dedupe marker, both written in the same
`EVALSHA` as the publish:

```rust
use std::time::Duration;

use redisevents::config::TopicOptions;
use redisevents::producer::{ForwardedMessage, TopicForwarder};
use redisevents::wire::Keys;
use redisevents::{Connection, StreamId};

async fn forward(conn: Connection, position: StreamId, body: Vec<u8>) -> redisevents::Result<bool> {
    let forwarder = TopicForwarder::new(conn, Keys::from_env(), "transactions", TopicOptions::default())?
        .with_marker_ttl(Duration::from_secs(7 * 24 * 3600));
    forwarder
        .forward(
            "customer_wallet",
            "customer-7:tx-100",
            position,
            &[ForwardedMessage::new("customer-7", body, "WalletTransaction")],
            None,
        )
        .await
}
```

It returns `Ok(false)` when the event was forwarded already, or when a concurrent forward from the
same source moved the mark first. Nothing is written in that case. A source must deliver in order,
because the mark only moves forward: one partition of a topic is one source.

### Idempotency

`Idempotency::try_begin` is one `SET NX PX` round trip. It returns `false` when the id was seen
within the TTL. Size the TTL above the replay window, or the claim dedupes nothing:

```rust
use std::time::Duration;

use redisevents::producer::Idempotency;
use redisevents::wire::Keys;
use redisevents::Connection;

async fn first_time(conn: &Connection, entry_id: &str) -> redisevents::Result<bool> {
    Idempotency::try_begin(conn, &Keys::from_env(), "orders", entry_id, Duration::from_secs(86_400)).await
}
```

It is a *claim*, not a transaction. If the process dies between the claim and the work, the message
is neither processed nor redelivered until the TTL expires. Where that matters, claim in the same
write that does the work, which is what the outbox is for.

---

## Positions and replay

A consumer's position is the id it has read up to, per partition, stored in one hash
at `<prefix>p:{topic}:{consumer}`. The consumer resumes from it.

- A crash loses up to `PersistIntervalMs` of position writes, so everything processed in that window
  is redelivered. A blocking retry re-runs the same batch from its first entry.
- `PersistMode::SyncBatch` writes after each batch. `SyncMessage` writes after each message. Neither
  gives exactly-once: the handler can succeed and the process die before the write lands.
- `StartFrom` picks the start when there is no stored position (`Stored` uses the stored one and
  falls back to `StartFromWhenMissing`). `Now` reads only new entries.

**Replay is an operator action, and it is previewed first.** A preview reports how much would be
reprocessed per partition and whether the target has already been trimmed away. A reset to a time
earlier than retention cannot bring back entries that no longer exist, and it will not say so
afterwards.

```rust
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::wire::Keys;
use redisevents::Connection;
use time::{Duration, OffsetDateTime};

async fn replay_last_day(conn: Connection) -> redisevents::Result<()> {
    let admin = StreamAdmin::new(conn, Keys::from_env());
    let from = OffsetDateTime::now_utc() - Duration::days(1);
    let preview = admin
        .preview_reset("orders", "billing", ResetTarget::Time(from), &ResetOptions::default())
        .await?;
    println!("{}", preview.describe());
    if preview.any_trimmed_away() {
        // Part of the range is gone from the stream. Decide whether that is acceptable first.
    }
    Ok(())
}
```

Then **stop the consumer**, reset, and start it again. A reset under a running consumer races the
position that consumer is writing, and only a stopped consumer (or one that picks the marker up
through its flush, see [the admin protocol](#the-admin-and-reset-protocol)) is safe to reason about:

```rust
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::wire::Keys;
use redisevents::Connection;

async fn rewind_to_start(conn: Connection) -> redisevents::Result<()> {
    let admin = StreamAdmin::new(conn, Keys::from_env());
    let written = admin
        .reset_position("orders", "billing", ResetTarget::Start, &ResetOptions::default())
        .await?;
    println!("{} partitions rewound", written.len());
    Ok(())
}
```

Handlers must be idempotent under replay: a reset re-delivers every entry after the target.

### Who owns which partition

`StreamAdmin::get_ownership` reads which instance holds each partition. An unowned partition is one
nobody is reading, and from the outside it looks exactly like a quiet topic. The
`streams.partitions.unowned` gauge and the ownership map are the two ways to tell the difference.

```rust
use redisevents::admin::stream_admin::StreamAdmin;
use redisevents::wire::Keys;
use redisevents::Connection;

async fn unowned(conn: Connection) -> redisevents::Result<Vec<u32>> {
    let admin = StreamAdmin::new(conn, Keys::from_env());
    let map = admin.get_ownership("orders", "billing", 0).await?;
    Ok(map.unowned)
}
```

---

## Ownership: Lease and Static

`Instances:Mode` decides how partitions are split between processes.

- **`Lease` (the default).** Each instance claims partitions from the ownership hash with a lease,
  renewed on an interval (`LeaseTtlSeconds` 30, `LeaseRenewSeconds` 10). A plain `Deployment`
  scales, and no instance count is maintained. Lease mode needs **Redis 7.4 or later**: the host
  probes `HEXPIRE` at start and refuses to run without it.
- **`Static`.** The instance is given an index and a count, and owns a fixed contiguous range of
  partitions. Index and count come from `Instances:Index` and `Instances:Count`, else from
  `STREAMS_INSTANCE_INDEX` and `STREAMS_INSTANCE_COUNT`, else from the `StatefulSet` ordinal of
  `POD_NAME`. Use it only from a `StatefulSet`.

Under `Static`, every pod with no ordinal resolves to index 0, so every pod owns every partition and
every message is processed once per pod. `STREAMS_INSTANCE_COUNT` must track the replica count:
scale up without it and the new pods own nothing; scale down without it and the departed pods'
partitions are read by nobody, silently. Change both together and watch `streams.partitions.unowned`
return to 0.

`Delivery: WorkQueue` (competing consumers over `XREADGROUP`) is refused at startup in this version.
The default, `Ordered`, is one owner per partition, in order.

---

## The admin and reset protocol

These are the bytes a reset writes, for anyone reading the positions hash directly or running a
mixed-language fleet:

- `StreamAdmin::reset_position` writes the partition's position as `<id>|<ADMIN_INSTANCE_ID>` and a
  marker field `__reset:<p>` = `<id>|<issuedUtc>`, in one `HSET`. `ADMIN_INSTANCE_ID` is the fixed
  nil-like id `6f72616e-6765-4164-6d69-6e5265736574`.
- A running consumer of either language sees the marker on its next flush, rewinds the partition to
  the target and clears the marker with a compare-and-delete, so a newer marker survives.
- A stopped consumer applies the marker when it starts.
- `preview_reset` changes nothing. It reports the same distance, the trimmed-away flag and the
  direction (`Rewind`, `Skip` or `None`).

A time target resolves to the id immediately before `<ms>-0`, so an entry written on the requested
millisecond is delivered, not skipped.

---

## Health

`StreamStatus::partitions()` is a snapshot of every partition this process reads, and
`StreamHealth::grade` gives each one a verdict: `Healthy`, `Degraded` or `Unhealthy`, with the
rule that fired. The host's own `health()` method runs the same rules with the connection and the
consumer counts included.

```rust
use redisevents::admin::health::{StreamHealth, StreamHealthStatus, StreamStatus};

fn any_unhealthy() -> bool {
    StreamStatus::partitions()
        .iter()
        .any(|p| StreamHealth::grade(p).status == StreamHealthStatus::Unhealthy)
}

assert!(!any_unhealthy());
```

The rules, in short. **Unhealthy**: the connection is down, a partition has been blocked past
`UnhealthyBlockSeconds`, or it has been stopped past `UnhealthyStoppedSeconds` with entries waiting,
or it is **behind the tail with a frozen position** for `UnhealthyBehindSeconds` (off by default).
**Degraded**: a partition is blocked or stopped inside its threshold, lag is over
`UnhealthyLagMs`, or the consumer has been silent for `DegradedSilenceSeconds` while caught up.

Lag never makes a partition Unhealthy, and silence never does either. Lag is the age of the last
processed entry, so it is largest exactly when an idle consumer is caught up. A restart would
drop in-flight batches and fix nothing. `UnhealthyBehindSeconds` is the one rule that is safe to
restart on: there is provably an entry the consumer has not read, and its position has not moved for
N seconds. Set N longer than the slowest batch.

Poll it from a health check or a watchdog, never from a message path: it allocates per partition.

### Metrics worth alerting on

| Metric | Means |
|---|---|
| `streams.errors` | Handler failures, tagged by policy and error type. A `best_effort` count is messages **skipped** |
| `streams.blocked` | A partition is blocked on a `Block` and going nowhere |
| `streams.clamp.released` | Trimming overran a lagging consumer. Data has been lost |
| `streams.partitions.unowned` | Partitions nobody is reading. Usually `STREAMS_INSTANCE_COUNT` drift under `Static` |
| `streams.batch.duration` | Handler latency. The first thing to check when lag climbs |
| `streams.buffer.dropped` | Buffered publishes shed under `DropOldest` |

Metric and span names are the C# ones, so one dashboard covers both languages. The `otel` feature
exports them through OpenTelemetry; without it they are `tracing` spans and no-op metrics.

---

## Interop rules

These are the rules that keep a Rust service and a C# service talking to each other.

- **Explicit type strings.** The C# typed API defaults the type to the .NET full name
  (`Shop.OrderPlaced` in C# is `Namespace.OrderPlaced` in .NET). Cross-language topics must use
  explicit type strings. The Rust API always requires them: `Message::TYPE` is a required constant.
- **PascalCase JSON.** C# serialises properties as PascalCase by default, and serde in Rust keeps
  field names. A type shared with C# needs `#[serde(rename_all = "PascalCase")]`. The mismatch is
  silent on the C# side: a wrongly cased body fails to decode there, and in Rust it is skipped as
  malformed, so nothing is raised.
- **Set `STREAMS_KEY_PREFIX` on every service: the C# and Rust defaults differ.** Stream keys are
  `<prefix>s:{topic}:<p>` (the braces are the hash tag, present by default). Rust resolves the prefix as
  `STREAMS_KEY_PREFIX` verbatim, else `<STREAMS_ENVIRONMENT lowercased>:re:`, else `local:re:`. Rust never
  reads `ASPNETCORE_ENVIRONMENT` or `DOTNET_ENVIRONMENT` and never defaults to `production`. C# uses
  `STREAMS_KEY_PREFIX`, else `ASPNETCORE_ENVIRONMENT` / `DOTNET_ENVIRONMENT`, defaulting to `production:re:`.
  With nothing configured the two use different namespaces (`production:re:` vs `local:re:`) and do
  not see each other's topics. Set `STREAMS_KEY_PREFIX` on both, or `STREAMS_ENVIRONMENT` on Rust equal to
  the C# environment name. Rust logs the resolved prefix at startup (info), and says so when it fell
  back to `local:re:`.
- **Redis 7.4 or later** in Lease mode (for `HEXPIRE`), the same requirement as C#.
- **Stream ids and positions are the same bytes.** A position written by one language is read by the
  other. A Rust consumer resumes at a C# consumer's position and the reverse.

---

## Sharp edges

Each of these has cost somebody a day. The reason is attached because the reason is what you will
remember.

### Streams live on `redis-db`, never `redis-cache`

`redis-cache` runs without persistence, so a pod restart loses every stream and every stored
position: messages never consumed, and the record of what was. The connection is yours to build in
this crate, so this is the one place it can go wrong without a warning. Build it from the same
connection string the C# services use, `redis-db`.

### Partition count goes up and never comes down

A shrink is refused: partitions past the new count would be orphaned with no way to notice.
Increasing is supported, but a key that hashed to one partition may hash to another, so its old
backlog and its new messages are consumed concurrently until the backlog drains. **Drain to near-zero
lag before increasing.**

`StreamPublisher::new` and the host compare the configured count with the topic's recorded count
and refuse a shrink with `Error::Config`. Configure the count the topic already has, as read from
its metadata, before starting a consumer against a topic a C# service created.

### `MaxLen` must exceed worst-case consumer lag

Trimming does not know what has been consumed. A consumer 20 000 messages behind on a topic with
`MaxLen: 10000` loses messages it has not read. The trim clamp holds trimming back for a lagging
consumer, but it is released above 80 % of `MaxLen`, to protect Redis itself. That release fires
`streams.clamp.released`. Treat it as data loss that has already happened.

### `?` on a Redis error skips

`?` turns an ordinary error into `Skip`. A raw `redis::RedisError` from a call the handler makes is an
ordinary error, so a Redis outage inside a handler, propagated with `?`, skips the batch and loses
it. Use `.or_block()` on any Redis or downstream call that must succeed. `Error::Transport` and
`Error::Config` already block.

### Buffered publishes are lost on a crash

See the buffered section above. `flush()` is the only way to make a buffered publish durable.

### Cancelling a publish

Dropping a publish future before it resolves leaves it unknown whether the entry was written. As
with a cancelled C# call, there is no retry.

---

## Not in this version

- **Cluster.** `Connection` wraps a single node. Hash tags are written so that a cluster port needs
  no key changes, but the transport is not there yet.
- **`Delivery: WorkQueue`.** Refused at startup. It is planned, behind the `XREADGROUP` work.
- **The outbox's `conditions`.** The C# outbox takes conditions and returns `null` on a failed one.
  The Rust `Outbox` does not have them. Use `StreamStore::append_and_publish` for version checks.
- **Host-managed DI.** Build the `Connection` yourself (`Connection::from_options` does the
  connection-string translation). Register consumers in code; the `Streams` section is bound from
  JSON explicitly.

## Module layout

| Module | Contents |
| --- | --- |
| `wire` | stream ids, the message type, the header block, key layout, the entry codec, partition routing |
| `message` | the typed-message trait and the payload codec seam (JSON here; MessagePack in `redisevents-msgpack`) |
| `config` | option structs mirroring the C# `Streams` section, the binder and the advisor |
| `connection` | the redis-rs connection wrapper |
| `producer` | publisher traits, the direct and buffered publishers, the outbox, stream store, idempotency and forwarder |
| `consumer` | handler traits, the error contract, partition context, the host and its builder |
| `positions` | the position store trait, records and reset markers |
| `ownership` | partition ownership (Lease and Static) |
| `admin` | topic metadata, reset and preview, ownership and tail reads, health and lag |
| `diagnostics` | metric and span names and tracing |
| `error` | the crate error type |
| `testing` | in-memory doubles (feature `testing`) |
