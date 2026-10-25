//! Connection isolation and idle survival through the consumer host against real Redis. Port of the
//! C# `ReaderIsolationTests` (S21 and S21f), named after the C# test in `snake_case`.
//!
//! The shared multiplexer under test is the host's own ([`StreamsHost::connection`]), so the latency
//! loop measures what the library uses for writes and positions. Every measurement has a positive
//! control: a blocking `XREAD` issued on the timed connection itself must stall it.
//!
//! Not ported: the two S21f starvation tests. They saturate the thread pool of the C# runtime to prove the reader's
//! socket survives it; Rust has no equivalent pool to saturate through the public API, so they are
//! listed as unexpressible in the PR. Skips without Redis. Needs the C4 host: ignored until it lands.

#![allow(
    clippy::items_after_statements,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::too_many_lines,
    clippy::redundant_closure_for_method_calls
)]

mod host_common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use host_common::*;
use redisevents::Publisher;
use redisevents::config::{ConsumerOptions, PersistMode, StartFrom};
use redisevents::consumer::Streams;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until_async};

/// The `BLOCK` used by the latency test: large enough that a shared connection is unmissable.
const LATENCY_BLOCK_MS: u32 = 2_000;
/// How long each latency window runs: spans several `BLOCK` intervals.
const LATENCY_WINDOW: Duration = Duration::from_secs(4);
/// The p99 ceiling for the shared connection while a reader is parked.
const ISOLATED_P99_CEILING: Duration = Duration::from_millis(250);

/// One reader connection as `CLIENT LIST` reports it.
struct ReaderClient {
    id: i64,
    age_seconds: i64,
}

/// The dedicated reader connections of `consumer` (named `streams-rd:<service>:<consumer>:i<n>:<pod>`, C# `StreamNames.ReaderClientName`).
async fn reader_clients(
    redis: &redisevents_test_support::RedisFixture,
    consumer: &str,
) -> Vec<ReaderClient> {
    let mut conn = raw(redis).await;
    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut conn)
        .await
        .expect("CLIENT LIST");
    let infix = format!(":{consumer}:i");
    list.lines()
        .filter_map(|line| {
            let field = |name: &str| {
                line.split(' ')
                    .find_map(|kv| kv.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
            };
            let named = field("name")?;
            if !named.starts_with("streams-rd:") || !named.contains(&infix) {
                return None;
            }
            Some(ReaderClient {
                id: field("id")?.parse().ok()?,
                age_seconds: field("age")?.parse().ok()?,
            })
        })
        .collect()
}

/// Times `GET key` on `conn` back to back for `window`, in milliseconds.
async fn sample_get_latency(
    conn: &mut redis::aio::ConnectionManager,
    key: &str,
    window: Duration,
) -> Vec<f64> {
    let mut samples = Vec::new();
    let end = Instant::now() + window;
    while Instant::now() < end {
        let started = Instant::now();
        // A GET that times out on a stalled connection is the slowest sample there is, not a test
        // error: its elapsed time (the response timeout) is what the control measures.
        let _: redis::RedisResult<Option<String>> =
            redis::cmd("GET").arg(key).query_async(conn).await;
        samples.push(started.elapsed().as_secs_f64() * 1_000.0);
    }
    samples
}

fn p99(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    if sorted.is_empty() {
        return f64::INFINITY;
    }
    let index = ((sorted.len() as f64) * 0.99).ceil() as usize;
    sorted[index.clamp(1, sorted.len()) - 1]
}

/// S21. A consumer parked in `XREAD BLOCK 2000` on an idle topic does not move the p99 of a tight
/// `GET` loop on the shared connection; a blocking read issued on that same connection blows it out.
#[tokio::test]
async fn s21_a_parked_blocking_read_does_not_delay_the_shared_multiplexer() {
    let redis = require_redis!();
    let topic = redis.new_topic("s21");
    let consumer = redis.new_consumer("s21");
    let probe_key = format!("isolation-probe:{topic}");
    let host_conn = redis.connect().await;
    let host = Streams::builder(host_conn)
        .keys(redis.keys())
        .environment("Development")
        .topic_options(&topic, volume(2))
        .topic(&topic)
        .consumer(&consumer)
        .configure(|o: &mut ConsumerOptions| {
            o.block_ms = LATENCY_BLOCK_MS;
            o.batch_size = 16;
            o.persist = PersistMode::AsyncBatch;
            o.start_from = StartFrom::Beginning;
        })
        .handler(Arc::new(RecordingHandler::new()))
        .spawn()
        .await
        .expect("spawn");
    let mut shared = host.connection().shared();
    let _: () = redis::cmd("SET")
        .arg(&probe_key)
        .arg("v")
        .query_async(&mut shared)
        .await
        .expect("SET");

    let baseline = sample_get_latency(&mut shared, &probe_key, LATENCY_WINDOW).await;
    wait_until_async(
        || async { !reader_clients(redis, &consumer).await.is_empty() },
        Some(Duration::from_secs(30)),
        "the dedicated reader connection to appear in CLIENT LIST",
    )
    .await;
    let isolated = sample_get_latency(&mut shared, &probe_key, LATENCY_WINDOW).await;
    let readers = reader_clients(redis, &consumer).await;
    host.shutdown().await.expect("shutdown");

    // CONTROL: a blocking read on the connection being timed must stall it.
    let mut control = redis.connect().await.shared();
    let _: () = redis::cmd("SET")
        .arg(&probe_key)
        .arg("v")
        .query_async(&mut control)
        .await
        .expect("SET");
    let blocking_key = redis.keys().stream(&topic, 0, true);
    let mut blocker_conn = control.clone();
    let blocker = tokio::spawn(async move {
        let _: redis::RedisResult<redis::Value> = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(LATENCY_BLOCK_MS)
            .arg("STREAMS")
            .arg(&blocking_key)
            .arg("$")
            .query_async(&mut blocker_conn)
            .await;
    });
    let stalled = sample_get_latency(&mut control, &probe_key, LATENCY_WINDOW).await;
    blocker.abort();

    let baseline_p99 = p99(&baseline);
    let isolated_p99 = p99(&isolated);
    // The control's worst GET, not its p99: redis-rs gives up on the stalled connection after its
    // 500 ms response timeout and reconnects, so only a few of thousands of samples are stalled.
    let stalled_p99 = stalled.iter().copied().fold(0.0, f64::max);
    eprintln!(
        "GET p99 ms: baseline {baseline_p99:.2}, isolated {isolated_p99:.2}, control worst {stalled_p99:.2}"
    );
    assert!(
        baseline.len() > 100,
        "the baseline window needs enough samples for a p99"
    );
    assert!(
        isolated.len() > 100,
        "the measured window needs enough samples for a p99"
    );
    assert!(
        !readers.is_empty(),
        "ReadMode.Block must open a dedicated reader connection"
    );
    assert!(
        isolated_p99 < ISOLATED_P99_CEILING.as_secs_f64() * 1_000.0,
        "a blocking XREAD on the reader's own connection must not show in the shared connection's latency"
    );
    assert!(
        isolated_p99 < baseline_p99 + ISOLATED_P99_CEILING.as_secs_f64() * 1_000.0,
        "the p99 must stay flat relative to the same loop with no reader running"
    );
    assert!(
        stalled.len() >= 2,
        "CONTROL: the stalled window must have produced measurements"
    );
    assert!(
        stalled_p99 > 500.0,
        "CONTROL: a blocking XREAD on the timed connection must stall it"
    );
    assert!(
        stalled_p99 > isolated_p99 * 5.0,
        "CONTROL: sharing the connection must cost far more than not sharing it"
    );
}

/// S21. The reader connection survives a 65-second idle block without being torn down and rebuilt,
/// and the first message published after the idle window is delivered at once.
#[tokio::test]
async fn s21_reader_connection_survives_a_long_idle_and_resumes_immediately() {
    let redis = require_redis!();
    // 65 s, not less: the keep-alive and configuration check both default to 60 s, and the failure
    // this catches (a PING queued behind a long block) only shows past that.
    const IDLE: Duration = Duration::from_secs(65);
    let topic = redis.new_topic("s21-idle");
    let consumer = redis.new_consumer("s21-idle");
    let publisher = publisher(redis, &topic, volume(1)).await;
    let recorder = Arc::new(RecordingHandler::new());
    let host = Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .environment("Development")
        .topic_options(&topic, volume(1))
        .topic(&topic)
        .consumer(&consumer)
        .configure(|o: &mut ConsumerOptions| {
            // The shipped default, not one tuned to pass.
            o.block_ms = 1_000;
            o.batch_size = 16;
            o.persist = PersistMode::AsyncBatch;
            o.persist_interval_ms = 500;
            o.start_from = StartFrom::Beginning;
        })
        .handler(recorder.clone())
        .spawn()
        .await
        .expect("spawn");

    wait_until_async(
        || async { !reader_clients(redis, &consumer).await.is_empty() },
        Some(Duration::from_secs(30)),
        "the dedicated reader connection to appear in CLIENT LIST",
    )
    .await;
    let initial: Vec<i64> = {
        let mut ids: Vec<i64> = reader_clients(redis, &consumer)
            .await
            .iter()
            .map(|c| c.id)
            .collect();
        ids.sort_unstable();
        ids
    };
    tokio::time::sleep(IDLE).await;
    let after = reader_clients(redis, &consumer).await;
    let mut after_ids: Vec<i64> = after.iter().map(|c| c.id).collect();
    after_ids.sort_unstable();
    let oldest = after.iter().map(|c| c.age_seconds).max().unwrap_or(0);

    assert_eq!(
        recorder.count(),
        0,
        "nothing was published, so nothing may have been delivered"
    );
    assert_eq!(
        after_ids, initial,
        "the reader connection must survive a 65 s idle block without a rebuild"
    );
    assert!(
        oldest >= IDLE.as_secs() as i64 - 5,
        "the surviving connection must be the original one, aged by the whole window"
    );

    let wake = Instant::now();
    publisher
        .publish(
            "wake",
            text("wake"),
            MESSAGE_TYPE,
            &redisevents::producer::PublishOptions::new(),
        )
        .await
        .expect("publish");
    wait_for_count(&recorder, 1, "the first message after the idle window").await;
    assert!(
        wake.elapsed() < Duration::from_secs(2),
        "a parked XREAD wakes the instant an entry is added; a re-established connection would not"
    );
    let final_ids: Vec<i64> = {
        let mut ids: Vec<i64> = reader_clients(redis, &consumer)
            .await
            .iter()
            .map(|c| c.id)
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        final_ids, initial,
        "delivering after the idle must not have needed a new connection"
    );
    host.shutdown().await.expect("shutdown");
}
