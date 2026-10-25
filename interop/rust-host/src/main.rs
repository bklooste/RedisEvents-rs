//! Rust twin of `interop/dotnet/InteropHost`: same verbs, same flags, same JSON-lines output.
//!
//! `admin-reset` is real (D1, over `redisevents::admin::stream_admin::StreamAdmin`). Every other verb
//! still exits with code 3 ("not implemented"), which the scenario runner treats as a skip. Nothing
//! here fakes behaviour: a verb gets a real body in the work package that makes it possible (see the
//! table in `interop/README.md`).
//!
//! Exit codes: 0 ok, 1 usage or unexpected failure, 2 timeout, 3 not implemented, 4 library refused.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use redisevents::Connection;
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
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

#[derive(Subcommand)]
enum Verb {
    /// Publish messages to a topic.
    Publish {
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 1)]
        partitions: u32,
        /// Partition key; repeat for several keys (messages interleave per round).
        #[arg(long, required = true)]
        key: Vec<String>,
        #[arg(long = "type")]
        type_: String,
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
        /// Publish each key's messages with one batch call.
        #[arg(long, conflicts_with = "buffered")]
        batch: bool,
        /// Publish through the buffered publisher.
        #[arg(long)]
        buffered: bool,
        /// Skip ensuring the topic first (exposes cold-start behaviour of the buffered publisher).
        #[arg(long)]
        no_warmup: bool,
    },
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
    /// The verb's CLI name and the work packages that implement it.
    fn describe(&self) -> (&'static str, &'static str) {
        match self {
            Verb::Publish { .. } => ("publish", "WP B1 (wire layer) and the publisher"),
            Verb::Consume { .. } => ("consume", "WP C4 (consumer host)"),
            Verb::Positions { .. } => ("positions", "WP B1 (position store)"),
            Verb::AdminReset { .. } => ("admin-reset", "WP D1 (admin reset)"), // real: see run_admin_reset
            Verb::Meta { .. } => ("meta", "WP B1 (topic meta)"),
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Verb::AdminReset {
        topic,
        consumer,
        partition,
        to_id,
    } = &cli.verb
    {
        return run_admin_reset(cli.redis.as_deref(), topic, consumer, *partition, to_id);
    }
    let (name, needs) = cli.verb.describe();
    let line = json_line(name, needs);
    println!("{line}");
    eprintln!("not implemented yet: {name} needs {needs}");
    ExitCode::from(EXIT_NOT_IMPLEMENTED)
}

/// `admin-reset`: `StreamAdmin::reset_position` to an exact id, prints
/// `{"event":"reset","targets":[{"partition":P,"id":"ms-seq"}]}`. A library refusal (`Error::Config`)
/// is `{"event":"refused",...}` with exit 4; anything else is `{"event":"error",...}` with exit 1.
fn run_admin_reset(
    redis: Option<&str>,
    topic: &str,
    consumer: &str,
    partition: Option<u32>,
    to_id: &str,
) -> ExitCode {
    let url = redis
        .map(str::to_owned)
        .or_else(|| std::env::var("REDIS_URL").ok())
        .unwrap_or_else(|| "redis://localhost:6379".to_owned());
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => return fail("Runtime", &e.to_string(), 1),
    };
    let result = runtime.block_on(async {
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
        Err(redisevents::Error::Config(message)) => {
            println!(
                "{}",
                serde_json::json!({"event": "refused", "message": message})
            );
            ExitCode::from(4)
        }
        Err(e) => fail("Error", &e.to_string(), 1),
    }
}

fn fail(kind: &str, message: &str, code: u8) -> ExitCode {
    println!(
        "{}",
        serde_json::json!({"event": "error", "kind": kind, "message": message})
    );
    ExitCode::from(code)
}

/// One JSON line for the runner; hand-built so this crate needs no JSON dependency.
fn json_line(verb: &str, needs: &str) -> String {
    format!(r#"{{"event":"not_implemented","verb":"{verb}","needs":"{needs}"}}"#)
}
