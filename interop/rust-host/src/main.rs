//! Rust twin of `interop/dotnet/InteropHost`: same verbs, same flags, same JSON-lines output.
//!
//! Every verb is real, over `redisevents`: `consume` runs the consumer host (WP C4) exactly as the
//! C# host runs `AddStream` (persist interval 100 ms, positions flushed on stop). Nothing here
//! fakes behaviour; a verb that cannot run would exit with code 3, which the runner skips.
//!
//! Exit codes: 0 ok, 1 usage or unexpected failure, 2 timeout, 3 not implemented, 4 library refused.

use std::future::Future;
use std::io::Write as _;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use redisevents::Connection;
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::config::{
    ConsumerOptions, DeliveryMode, InstanceMode, InstanceOptions, ProducerOptions, StreamOptions,
    TopicOptions,
};
use redisevents::consumer::Streams;
use redisevents::producer::{
    BufferedPublisher, BufferedStreamPublisher, Condition, Outbox, OutboxPublish, PublishOptions,
    Publisher, StreamPublisher,
};
use redisevents::wire::{Keys, StreamId};
use redisevents::{
    BatchHandler, HandleResult, Json, Message, PartitionContext, PublisherExt, StreamMsg,
    TypedHandler, TypedMsg,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Parser)]
#[command(
    name = "interop-rust-host",
    about = "RedisEvents interop harness (Rust side)"
)]
struct Cli {
    /// Redis URL; defaults to `REDIS_URL`, then `redis://localhost:6379`.
    #[arg(long, global = true)]
    redis: Option<String>,
    #[command(subcommand)]
    verb: Verb,
}

#[derive(clap::Args)]
#[allow(clippy::struct_excessive_bools)] // one field per CLI flag
struct PublishArgs {
    #[arg(long)]
    topic: String,
    #[arg(long, default_value_t = 1)]
    partitions: u32,
    /// Partition key; repeat for several keys (messages interleave per round).
    #[arg(long, required = true)]
    key: Vec<String>,
    #[arg(long = "type")]
    type_: String,
    /// Base64 body (empty when omitted).
    #[arg(long, default_value = "")]
    body_b64: String,
    /// `k=v`; repeatable.
    #[arg(long)]
    header: Vec<String>,
    #[arg(long)]
    correlation: Option<String>,
    #[arg(long)]
    traceparent: Option<String>,
    #[arg(long, default_value_t = 1)]
    count: u32,
    /// Append `|key|i` to each body so order can be asserted.
    #[arg(long)]
    seq: bool,
    /// Publish each key's messages with one batch call (takes precedence over `--buffered`).
    #[arg(long)]
    batch: bool,
    /// Publish through the buffered publisher.
    #[arg(long)]
    buffered: bool,
    /// Accepted for parity with the C# host. The Rust publisher always reconciles the topic meta
    /// before its first write, so the cold-start reorder cannot occur here either way.
    #[arg(long)]
    no_warmup: bool,
}

/// The stalled-owner scenario's knobs on `consume`.
#[derive(clap::Args, Clone, Copy, Default)]
struct StallArgs {
    /// Lease mode with this lease TTL (seconds).
    #[arg(long)]
    lease_ttl: Option<u32>,
    /// The lease renew interval (seconds), with `--lease-ttl`.
    #[arg(long, default_value_t = 1)]
    lease_renew: u32,
    /// `unhealthy_stalled_seconds`; also shortens the shutdown budget to 1 s.
    #[arg(long)]
    stall_seconds: Option<u32>,
    /// The handler never returns (and ignores its token). Prints `{"event":"wedged"}` per batch.
    #[arg(long)]
    wedge: bool,
}

/// The work-queue scenarios' knobs on `consume`.
#[derive(clap::Args, Clone, Copy, Default)]
struct DeliveryArgs {
    /// `ordered` (default) or `work_queue` (`XREADGROUP`; also selects static ownership so every
    /// instance competes on every partition). The member name is `POD_NAME`, as in C#.
    #[arg(long, value_enum, default_value_t = Delivery::Ordered)]
    delivery: Delivery,
    /// `batch_size`.
    #[arg(long)]
    batch_size: Option<u32>,
    /// The handler sleeps this long per batch, after printing it (simulated work).
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
}

#[derive(clap::ValueEnum, Clone, Copy, Default, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
enum Delivery {
    #[default]
    Ordered,
    WorkQueue,
}

#[derive(Subcommand)]
enum Verb {
    /// Publish messages to a topic.
    Publish(PublishArgs),
    /// Consume a topic, printing one JSON line per message.
    Consume {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        consumer: String,
        #[arg(long, default_value_t = 1)]
        partitions: u32,
        #[arg(long, default_value_t = 1)]
        expect: u32,
        /// Seconds to wait for `--expect` messages.
        #[arg(long, default_value_t = 30)]
        timeout: u32,
        #[command(flatten)]
        stall: StallArgs,
        #[command(flatten)]
        delivery: DeliveryArgs,
    },
    /// Print the stored positions of a consumer.
    Positions {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        consumer: String,
    },
    /// Rewind a consumer through the admin reset protocol.
    AdminReset {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        consumer: String,
        #[arg(long)]
        partition: Option<u32>,
        #[arg(long)]
        to_id: String,
    },
    /// Print the `m:{topic}` meta hash.
    Meta {
        #[arg(long)]
        topic: String,
    },
    /// Publish the two typed JSON messages of gate G4.
    PublishTyped {
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 1)]
        partitions: u32,
        #[arg(long)]
        key: String,
    },
    /// Consume typed JSON messages, printing one `typed` line with the decoded fields.
    ConsumeTyped {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        consumer: String,
        #[arg(long, default_value_t = 1)]
        partitions: u32,
        #[arg(long, default_value_t = 1)]
        expect: u32,
        #[arg(long, default_value_t = 30)]
        timeout: u32,
    },
    /// One outbox call: set a state hash field and publish, guarded by the `--if-*` conditions.
    Outbox(OutboxArgs),
    /// Move the state field `version` up by one, `--count` times, each guarded on the value read.
    OutboxRace {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        key: String,
        #[arg(long)]
        state: String,
        #[arg(long, default_value_t = 1)]
        count: u32,
        /// Start at this Unix time in milliseconds, so two writers really race.
        #[arg(long)]
        start_at_ms: u64,
    },
}

#[derive(clap::Args)]
struct OutboxArgs {
    #[arg(long)]
    topic: String,
    #[arg(long, default_value_t = 1)]
    partitions: u32,
    #[arg(long)]
    key: String,
    #[arg(long = "type")]
    type_: String,
    #[arg(long, default_value = "")]
    body_b64: String,
    #[arg(long)]
    correlation: Option<String>,
    /// The state name; the key is `Outbox::state_key(topic, name)`.
    #[arg(long)]
    state: String,
    /// `field=value`, the hash field to write.
    #[arg(long)]
    set: String,
    /// `field=value`; repeatable.
    #[arg(long)]
    if_equal: Vec<String>,
    /// Repeatable.
    #[arg(long)]
    if_field_absent: Vec<String>,
    #[arg(long)]
    if_key_absent: bool,
}

/// The typed payload of gate G4: twin of `TypedOrder.cs`, the default `System.Text.Json` shape.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TypedOrder {
    order_id: String,
    quantity: i64,
    price: f64,
    status: u8,
    placed_at: String,
    tags: Vec<String>,
    customer: TypedCustomer,
    note: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct TypedCustomer {
    name: String,
}

impl Message for TypedOrder {
    /// C#'s default type string for this type, its CLR full name.
    const TYPE: &'static str = "Interop.TypedOrder";
}

/// Twin of `Orders()` in the C# host.
fn orders() -> [TypedOrder; 2] {
    [
        TypedOrder {
            order_id: "order-Zoë-注文".to_owned(),
            quantity: 9_007_199_254_740_993,
            price: 12.5,
            status: 2,
            placed_at: "2026-11-02T03:04:05+00:00".to_owned(),
            tags: vec!["a".to_owned(), "b".to_owned()],
            customer: TypedCustomer {
                name: "Ann".to_owned(),
            },
            note: None,
        },
        TypedOrder {
            order_id: "order-2".to_owned(),
            quantity: 0,
            price: 0.25,
            status: 0,
            placed_at: "2026-11-02T03:04:06+00:00".to_owned(),
            tags: Vec::new(),
            customer: TypedCustomer {
                name: String::new(),
            },
            note: Some("with a note".to_owned()),
        },
    ]
}

struct TypedEmitter {
    seen: AtomicU32,
    expect: u32,
    reached: Notify,
}

impl TypedHandler<TypedOrder> for Arc<TypedEmitter> {
    #[allow(clippy::unused_async_trait_impl)] // the trait's shape
    async fn handle(&self, msg: &TypedMsg<TypedOrder>, _ctx: &PartitionContext) -> HandleResult {
        let order = &msg.value;
        let placed_at = order.placed_at.replace("+00:00", "Z");
        println!(
            "{}",
            serde_json::json!({
                "event": "typed",
                "key": msg.envelope.partition_key(),
                "type": msg.envelope.msg_type(),
                "order_id": order.order_id,
                "quantity": order.quantity,
                "price": order.price,
                "status": order.status,
                "placed_at": placed_at,
                "tags": order.tags,
                "customer": order.customer.name,
                "note": order.note,
            })
        );
        let _ = std::io::stdout().flush();
        if self.seen.fetch_add(1, Ordering::SeqCst) + 1 >= self.expect {
            self.reached.notify_one();
        }
        Ok(())
    }
}

fn run_publish_typed(redis: Option<&str>, topic: &str, partitions: u32, key: &str) -> ExitCode {
    let url = redis_url(redis);
    let result = run(async {
        let conn = Connection::connect(&url).await?;
        let options = TopicOptions {
            partitions,
            ..TopicOptions::default()
        };
        let publisher = StreamPublisher::new(conn, Keys::from_env(), topic, options).await?;
        let all = orders();
        for order in &all {
            let id = publisher
                .publish_message::<Json, _>(key, order, &PublishOptions::new())
                .await?;
            println!(
                "{}",
                serde_json::json!({"event": "published", "key": key, "id": id.to_string()})
            );
        }
        Ok(all.len())
    });
    match result {
        Ok(published) => {
            println!(
                "{}",
                serde_json::json!({"event": "done", "published": published})
            );
            ExitCode::SUCCESS
        }
        Err(code) => code,
    }
}

fn run_consume_typed(
    redis: Option<&str>,
    topic: &str,
    consumer: &str,
    partitions: u32,
    (expect, timeout): (u32, u32),
) -> ExitCode {
    let url = redis_url(redis);
    let emitter = Arc::new(TypedEmitter {
        seen: AtomicU32::new(0),
        expect,
        reached: Notify::new(),
    });
    let handler = emitter.clone();
    let result = run(async {
        let conn = Connection::connect(&url).await?;
        let mut options = StreamOptions::default();
        options.topics.insert(
            topic.to_owned(),
            TopicOptions {
                partitions,
                ..TopicOptions::default()
            },
        );
        options.consumers.push(ConsumerOptions {
            topic: topic.to_owned(),
            consumer: Some(consumer.to_owned()),
            persist_interval_ms: 100,
            ..ConsumerOptions::default()
        });
        let host = Streams::builder(conn)
            .options(options)
            .topic(topic)
            .typed_handler::<TypedOrder, _>(handler)
            .spawn()
            .await?;
        let reached = tokio::time::timeout(
            Duration::from_secs(u64::from(timeout)),
            emitter.reached.notified(),
        )
        .await
        .is_ok()
            || emitter.seen.load(Ordering::SeqCst) >= expect;
        host.shutdown().await?;
        Ok(reached)
    });
    match result {
        Ok(reached) => {
            let seen = emitter.seen.load(Ordering::SeqCst);
            println!(
                "{}",
                serde_json::json!({"event": "done", "consumed": seen, "timed_out": !reached})
            );
            if reached {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            }
        }
        Err(code) => code,
    }
}

fn run_outbox(redis: Option<&str>, args: &OutboxArgs) -> ExitCode {
    let body = match STANDARD.decode(&args.body_b64) {
        Ok(body) => body,
        Err(e) => return fail("FormatException", &e.to_string(), 1),
    };
    let Some((field, value)) = args.set.split_once('=') else {
        return usage(&format!("--set needs field=value, got '{}'", args.set));
    };
    let url = redis_url(redis);
    let result = run(async {
        let keys = Keys::from_env();
        let state = Outbox::state_key(&keys, &args.topic, &args.state)?;
        let mut conditions = Vec::new();
        for raw in &args.if_equal {
            let (f, v) = raw.split_once('=').ok_or_else(|| {
                redisevents::Error::invalid_argument(format!(
                    "--if-equal needs field=value, got '{raw}'"
                ))
            })?;
            conditions.push(Condition::hash_equal(state.as_str(), f, v));
        }
        for f in &args.if_field_absent {
            conditions.push(Condition::hash_not_exists(state.as_str(), f.as_str()));
        }
        if args.if_key_absent {
            conditions.push(Condition::key_not_exists(state.as_str()));
        }
        let mut options = PublishOptions::new();
        if let Some(c) = &args.correlation {
            options = options.correlation_id(c.clone());
        }
        let topic_options = TopicOptions {
            partitions: args.partitions,
            ..TopicOptions::default()
        };
        let outbox = Outbox::new(Connection::connect(&url).await?, keys);
        outbox
            .write_and_publish(
                |pipe| {
                    pipe.cmd("HSET").arg(&state).arg(field).arg(value);
                },
                &OutboxPublish {
                    topic: &args.topic,
                    partition_key: &args.key,
                    body: &body,
                    msg_type: &args.type_,
                    options: &options,
                    topic_options: &topic_options,
                },
                &conditions,
                &[&state],
            )
            .await
    });
    match result {
        Ok(id) => {
            println!(
                "{}",
                serde_json::json!({
                    "event": "outbox",
                    "applied": id.is_some(),
                    "id": id.map(|i| i.to_string()),
                })
            );
            ExitCode::SUCCESS
        }
        Err(code) => code,
    }
}

fn run_outbox_race(
    redis: Option<&str>,
    topic: &str,
    key: &str,
    state_name: &str,
    count: u32,
    start_at_ms: u64,
) -> ExitCode {
    let url = redis_url(redis);
    let result = run(async {
        let keys = Keys::from_env();
        let state = Outbox::state_key(&keys, topic, state_name)?;
        let conn = Connection::connect(&url).await?;
        let mut shared = conn.shared();
        let outbox = Outbox::new(conn, keys);
        let options = PublishOptions::new();
        let topic_options = TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        };
        let (mut wins, mut losses) = (0_u32, 0_u32);
        let start_at = UNIX_EPOCH + Duration::from_millis(start_at_ms);
        if let Ok(wait) = start_at.duration_since(SystemTime::now()) {
            tokio::time::sleep(wait).await;
        }
        while wins < count {
            let current: Option<String> = redis::cmd("HGET")
                .arg(&state)
                .arg("version")
                .query_async(&mut shared)
                .await?;
            let next = current
                .as_deref()
                .map_or(Ok(0), str::parse::<u64>)
                .map_err(|e| redisevents::Error::invalid_argument(e.to_string()))?
                + 1;
            let guard = match current {
                None => Condition::hash_not_exists(state.as_str(), "version"),
                Some(v) => Condition::hash_equal(state.as_str(), "version", v),
            };
            let body = next.to_string();
            let id = outbox
                .write_and_publish(
                    |pipe| {
                        pipe.cmd("HSET").arg(&state).arg("version").arg(next);
                    },
                    &OutboxPublish {
                        topic,
                        partition_key: key,
                        body: body.as_bytes(),
                        msg_type: "Interop.Versioned",
                        options: &options,
                        topic_options: &topic_options,
                    },
                    &[guard],
                    &[&state],
                )
                .await?;
            if id.is_some() {
                wins += 1;
            } else {
                losses += 1;
            }
        }
        Ok((wins, losses))
    });
    match result {
        Ok((wins, losses)) => {
            println!(
                "{}",
                serde_json::json!({"event": "done", "wins": wins, "losses": losses})
            );
            ExitCode::SUCCESS
        }
        Err(code) => code,
    }
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) if e.use_stderr() => return usage(&e.to_string()),
        Err(e) => {
            print!("{e}");
            return ExitCode::SUCCESS;
        }
    };
    let redis = cli.redis.as_deref();
    match &cli.verb {
        Verb::Publish(args) => run_publish(redis, args),
        Verb::Meta { topic } => run_meta(redis, topic),
        Verb::AdminReset {
            topic,
            consumer,
            partition,
            to_id,
        } => run_admin_reset(redis, topic, consumer, *partition, to_id),
        Verb::Consume {
            topic,
            consumer,
            partitions,
            expect,
            timeout,
            stall,
            delivery,
        } => run_consume(
            redis,
            topic,
            consumer,
            *partitions,
            (*expect, *timeout),
            *stall,
            *delivery,
        ),
        Verb::Positions { topic, consumer } => run_positions(redis, topic, consumer),
        Verb::PublishTyped {
            topic,
            partitions,
            key,
        } => run_publish_typed(redis, topic, *partitions, key),
        Verb::ConsumeTyped {
            topic,
            consumer,
            partitions,
            expect,
            timeout,
        } => run_consume_typed(redis, topic, consumer, *partitions, (*expect, *timeout)),
        Verb::Outbox(args) => run_outbox(redis, args),
        Verb::OutboxRace {
            topic,
            key,
            state,
            count,
            start_at_ms,
        } => run_outbox_race(redis, topic, key, state, *count, *start_at_ms),
    }
}

fn redis_url(redis: Option<&str>) -> String {
    redis
        .map(str::to_owned)
        .or_else(|| std::env::var("REDIS_URL").ok())
        .unwrap_or_else(|| "redis://localhost:6379".to_owned())
}

/// Runs `future` on a fresh current-thread runtime. A library refusal (`Error::Config`) is
/// `{"event":"refused",...}` with exit 4; anything else `{"event":"error",...}` with exit 1.
fn run<T>(future: impl Future<Output = redisevents::Result<T>>) -> Result<T, ExitCode> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| fail("Runtime", &e.to_string(), 1))?;
    runtime.block_on(future).map_err(report)
}

fn report(error: redisevents::Error) -> ExitCode {
    match error {
        redisevents::Error::Config(message) => {
            println!(
                "{}",
                serde_json::json!({"event": "refused", "message": message})
            );
            ExitCode::from(4)
        }
        e => fail("Error", &e.to_string(), 1),
    }
}

/// `publish`: one `published` line per message (direct, non-batch), then `done`. Batch and buffered
/// runs print only `done`, as the C# host does.
fn run_publish(redis: Option<&str>, args: &PublishArgs) -> ExitCode {
    let body = match STANDARD.decode(&args.body_b64) {
        Ok(body) => Bytes::from(body),
        Err(e) => return fail("FormatException", &e.to_string(), 1),
    };
    let mut opts = PublishOptions::new();
    if let Some(correlation) = &args.correlation {
        opts = opts.correlation_id(correlation.clone());
    }
    for raw in &args.header {
        let Some((key, value)) = raw.split_once('=') else {
            return usage(&format!("--header needs k=v, got '{raw}'"));
        };
        opts = opts.header(key, value);
    }
    if let Some(traceparent) = &args.traceparent {
        opts = opts.trace_parent(traceparent.clone());
    }
    let url = redis_url(redis);
    match run(publish_all(&url, args, body, opts)) {
        Ok(published) => {
            println!(
                "{}",
                serde_json::json!({"event": "done", "published": published})
            );
            ExitCode::SUCCESS
        }
        Err(code) => code,
    }
}

async fn publish_all(
    url: &str,
    args: &PublishArgs,
    body: Bytes,
    opts: PublishOptions,
) -> redisevents::Result<u32> {
    let conn = Connection::connect(url).await?;
    let topic_options = TopicOptions {
        partitions: args.partitions,
        ..TopicOptions::default()
    };
    // Reconciles the topic meta first: a partition shrink is refused here (`Error::Config`).
    let direct =
        StreamPublisher::new(conn, Keys::from_env(), args.topic.as_str(), topic_options).await?;
    if args.buffered {
        let options = ProducerOptions {
            topic: args.topic.clone(),
            buffered: true,
            ..ProducerOptions::default()
        };
        let buffered = BufferedStreamPublisher::new(direct, options);
        let published = if args.batch {
            publish_batches(&buffered, args, &body, &opts).await?
        } else {
            enqueue_each(&buffered, args, &body, &opts).await?
        };
        buffered.flush().await?;
        buffered.shutdown().await?;
        Ok(published)
    } else if args.batch {
        publish_batches(&direct, args, &body, &opts).await
    } else {
        publish_each(&direct, args, &body, &opts).await
    }
}

/// The body for message `i` of `key`: the shared body, plus `|key|i` under `--seq`.
fn body_for(args: &PublishArgs, body: &Bytes, key: &str, i: u32) -> Bytes {
    if args.seq {
        let mut out = body.to_vec();
        out.extend_from_slice(format!("|{key}|{i}").as_bytes());
        Bytes::from(out)
    } else {
        body.clone()
    }
}

async fn publish_each(
    target: &StreamPublisher,
    args: &PublishArgs,
    body: &Bytes,
    opts: &PublishOptions,
) -> redisevents::Result<u32> {
    let mut published = 0;
    for i in 0..args.count {
        for key in &args.key {
            let id = target
                .publish(key, body_for(args, body, key, i), &args.type_, opts)
                .await?;
            println!(
                "{}",
                serde_json::json!({"event": "published", "key": key, "id": id.to_string()})
            );
            published += 1;
        }
    }
    Ok(published)
}

async fn enqueue_each(
    target: &BufferedStreamPublisher,
    args: &PublishArgs,
    body: &Bytes,
    opts: &PublishOptions,
) -> redisevents::Result<u32> {
    let mut published = 0;
    for i in 0..args.count {
        for key in &args.key {
            target
                .enqueue(key, body_for(args, body, key, i), &args.type_, opts)
                .await?;
            published += 1;
        }
    }
    Ok(published)
}

async fn publish_batches<P: Publisher>(
    target: &P,
    args: &PublishArgs,
    body: &Bytes,
    opts: &PublishOptions,
) -> redisevents::Result<u32> {
    let mut published = 0;
    for key in &args.key {
        let bodies: Vec<Bytes> = (0..args.count)
            .map(|i| body_for(args, body, key, i))
            .collect();
        target
            .publish_batch(key, &bodies, &args.type_, opts)
            .await?;
        published += args.count;
    }
    Ok(published)
}

/// `meta`: the `m:{topic}` hash, `{"event":"meta","fields":{...}}`.
fn run_meta(redis: Option<&str>, topic: &str) -> ExitCode {
    let url = redis_url(redis);
    let result = run(async {
        let conn = Connection::connect(&url).await?;
        let admin = StreamAdmin::new(conn, Keys::from_env());
        admin.read_topic_meta(topic).await
    });
    match result {
        Ok(fields) => {
            println!("{}", serde_json::json!({"event": "meta", "fields": fields}));
            ExitCode::SUCCESS
        }
        Err(code) => code,
    }
}

/// `admin-reset`: `StreamAdmin::reset_position` to an exact id, prints
/// `{"event":"reset","targets":[{"partition":P,"id":"ms-seq"}]}`.
fn run_admin_reset(
    redis: Option<&str>,
    topic: &str,
    consumer: &str,
    partition: Option<u32>,
    to_id: &str,
) -> ExitCode {
    let url = redis_url(redis);
    let result = run(async {
        let to = StreamId::parse(to_id).map_err(redisevents::Error::from)?;
        let conn = Connection::connect(&url).await?;
        let admin = StreamAdmin::new(conn, Keys::from_env());
        let options = ResetOptions {
            partition,
            ..ResetOptions::default()
        };
        admin
            .reset_position(topic, consumer, ResetTarget::Id(to), &options)
            .await
    });
    match result {
        Ok(targets) => {
            let targets: Vec<_> = targets
                .into_iter()
                .map(|(partition, id)| serde_json::json!({"partition": partition, "id": id.to_string()}))
                .collect();
            println!(
                "{}",
                serde_json::json!({"event": "reset", "targets": targets})
            );
            ExitCode::SUCCESS
        }
        Err(code) => code,
    }
}

/// Prints one JSON line per delivered message, and signals when `expect` were seen.
struct Emitter {
    wedge: bool,
    delay: Duration,
    seen: AtomicU32,
    expect: u32,
    reached: Notify,
}

impl BatchHandler for Emitter {
    #[allow(clippy::unused_async_trait_impl)] // the trait's shape
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        if self.wedge {
            if let Some(first) = batch.first() {
                println!(
                    "{}",
                    serde_json::json!({"event": "wedged", "partition": first.partition()})
                );
            }
            std::future::pending::<()>().await;
        }
        self.emit(batch);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(())
    }
}

impl Emitter {
    fn emit(&self, batch: &[StreamMsg]) {
        let mut out = std::io::stdout().lock();
        for m in batch {
            let headers: Vec<[String; 2]> = m
                .headers()
                .to_vec()
                .unwrap_or_default()
                .into_iter()
                .map(|(k, v)| [k, v])
                .collect();
            let line = serde_json::json!({
                "event": "message",
                "partition": m.partition(),
                "id": m.id().to_string(),
                "key": m.partition_key(),
                "type": m.msg_type(),
                "body_b64": STANDARD.encode(m.body()),
                "headers": headers,
                "correlation": m.correlation_id(),
                "traceparent": m.trace_parent(),
            });
            let _ = writeln!(out, "{line}");
            let seen = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
            if self.expect > 0 && seen >= self.expect {
                self.reached.notify_one();
            }
        }
        let _ = out.flush();
    }
}

/// `consume`: runs the consumer host until `expect` messages arrived or `timeout` seconds passed,
/// stops it (which flushes positions), then prints `done`. Exit 2 on a timeout.
fn run_consume(
    redis: Option<&str>,
    topic: &str,
    consumer: &str,
    partitions: u32,
    (expect, timeout): (u32, u32),
    stall: StallArgs,
    delivery: DeliveryArgs,
) -> ExitCode {
    let url = redis_url(redis);
    let emitter = Arc::new(Emitter {
        wedge: stall.wedge,
        delay: Duration::from_millis(delivery.delay_ms),
        seen: AtomicU32::new(0),
        expect,
        reached: Notify::new(),
    });
    let handler = emitter.clone();
    let counter = emitter.clone();
    let result = run(async move {
        let conn = Connection::connect(&url).await?;
        let mut options = StreamOptions::default();
        options.topics.insert(
            topic.to_owned(),
            TopicOptions {
                partitions,
                ..TopicOptions::default()
            },
        );
        let mut consumer_options = ConsumerOptions {
            topic: topic.to_owned(),
            consumer: Some(consumer.to_owned()),
            persist_interval_ms: 100,
            ..ConsumerOptions::default()
        };
        if let Some(ttl) = stall.lease_ttl {
            consumer_options.instances = Some(InstanceOptions {
                mode: InstanceMode::Lease,
                lease_ttl_seconds: ttl,
                lease_renew_seconds: stall.lease_renew,
                ..InstanceOptions::default()
            });
        }
        if let Some(seconds) = stall.stall_seconds {
            consumer_options.unhealthy_stalled_seconds = seconds;
            consumer_options.shutdown_timeout_seconds = 1;
        }
        if let Some(n) = delivery.batch_size {
            consumer_options.batch_size = n;
        }
        if delivery.delivery == Delivery::WorkQueue {
            consumer_options.delivery = DeliveryMode::WorkQueue;
            consumer_options.instances = Some(InstanceOptions {
                mode: InstanceMode::Static,
                ..InstanceOptions::default()
            });
        }
        options.consumers.push(consumer_options);
        let host = Streams::builder(conn)
            .options(options)
            .topic(topic)
            .handler(handler)
            .spawn()
            .await?;
        let reached = expect == 0
            || tokio::time::timeout(
                Duration::from_secs(u64::from(timeout)),
                emitter.reached.notified(),
            )
            .await
            .is_ok()
            || emitter.seen.load(Ordering::SeqCst) >= expect;
        host.shutdown().await?;
        Ok(reached)
    });
    match result {
        Ok(reached) => {
            let timed_out = !reached && expect > 0;
            let seen = counter.seen.load(Ordering::SeqCst);
            println!(
                "{}",
                serde_json::json!({"event": "done", "consumed": seen, "timed_out": timed_out})
            );
            if timed_out {
                ExitCode::from(2)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(code) => code,
    }
}

/// `positions`: the consumer's positions hash, `{"event":"positions","positions":{"p":"id"},"markers":{..}}`.
fn run_positions(redis: Option<&str>, topic: &str, consumer: &str) -> ExitCode {
    let url = redis_url(redis);
    let result = run(async {
        let conn = Connection::connect(&url).await?;
        let key = Keys::from_env().positions(topic, consumer);
        let entries: Vec<(String, String)> = redis::cmd("HGETALL")
            .arg(&key)
            .query_async(&mut conn.shared())
            .await?;
        Ok(entries)
    });
    match result {
        Ok(entries) => {
            let mut positions = serde_json::Map::new();
            let mut markers = serde_json::Map::new();
            for (name, value) in entries {
                if !name.is_empty()
                    && name.bytes().all(|b| b.is_ascii_digit())
                    && let Ok(p) = name.parse::<u32>()
                {
                    let id = value.split('|').next().unwrap_or_default().to_owned();
                    positions.insert(p.to_string(), id.into());
                    continue;
                }
                markers.insert(name, value.into());
            }
            println!(
                "{}",
                serde_json::json!({"event": "positions", "positions": positions, "markers": markers})
            );
            ExitCode::SUCCESS
        }
        Err(code) => code,
    }
}

fn fail(kind: &str, message: &str, code: u8) -> ExitCode {
    println!(
        "{}",
        serde_json::json!({"event": "error", "kind": kind, "message": message})
    );
    ExitCode::from(code)
}

/// A usage error: `{"event":"error","kind":"usage",...}` with exit 1, as the C# host.
fn usage(message: &str) -> ExitCode {
    fail("usage", message.trim_end(), 1)
}
