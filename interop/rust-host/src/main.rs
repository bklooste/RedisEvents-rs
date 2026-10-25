//! Rust twin of `interop/dotnet/InteropHost`: same verbs, same flags, same JSON-lines output.
//!
//! `publish`, `meta` and `admin-reset` are real, over `redisevents`. `consume` and `positions` still
//! exit with code 3 ("not implemented"), which the scenario runner treats as a skip (they need the
//! consumer host, WP C4). Nothing here fakes behaviour.
//!
//! Exit codes: 0 ok, 1 usage or unexpected failure, 2 timeout, 3 not implemented, 4 library refused.

use std::future::Future;
use std::process::ExitCode;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use redisevents::Connection;
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::config::{ProducerOptions, TopicOptions};
use redisevents::producer::{
    BufferedPublisher, BufferedStreamPublisher, PublishOptions, Publisher, StreamPublisher,
};
use redisevents::wire::{Keys, StreamId};

/// Exit code meaning "this verb is not implemented yet"; the runner skips on it.
const EXIT_NOT_IMPLEMENTED: u8 = 3;

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
}

impl Verb {
    /// For a verb that is still a stub: its CLI name and the work package that implements it.
    fn not_implemented(&self) -> Option<(&'static str, &'static str)> {
        match self {
            Verb::Consume { .. } => Some(("consume", "WP C4 (consumer host)")),
            Verb::Positions { .. } => Some(("positions", "WP B4 (position store) and C4")),
            _ => None,
        }
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
        verb => {
            let (name, needs) = verb.not_implemented().unwrap_or(("unknown", "no verb"));
            println!(
                "{}",
                serde_json::json!({"event": "not_implemented", "verb": name, "needs": needs})
            );
            eprintln!("not implemented yet: {name} needs {needs}");
            ExitCode::from(EXIT_NOT_IMPLEMENTED)
        }
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
