//! Throughput benchmarks against a real Redis, a port of the C# `PerfBenchmarkTests`
//! (`tst/RedisEvents.Tests/PerfBenchmarkTests.cs`) with the same configurations and counts.
//!
//! Not part of the correctness suite: every test is `#[ignore]`d, so `cargo test` skips them. Run
//! them explicitly, in release, one at a time so they do not compete for the CPU:
//!
//! ```text
//! REDIS_URL=redis://127.0.0.1:6379 cargo test --release -p redisevents --test perf -- \
//!     --ignored --nocapture --test-threads=1
//! ```
//!
//! Results are printed as messages/second, to be compared across runs and machines rather than
//! asserted on (a CI box's absolute throughput is not a contract).

#![allow(
    clippy::cast_precision_loss,
    clippy::unnecessary_wraps,
    clippy::similar_names,
    clippy::unused_async_trait_impl
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use redisevents::config::{
    BackpressureOptions, ConsumerOptions, PersistMode, ProducerOptions, ReadMode, StartFrom,
    StreamOptions, TopicOptions, TrimMode,
};
use redisevents::consumer::Streams;
use redisevents::producer::{BufferedStreamPublisher, PublishOptions, Publisher, StreamPublisher};
use redisevents::{BatchHandler, BufferedPublisher, HandleResult, PartitionContext, StreamMsg};
use redisevents_test_support::{RedisFixture, require_redis, wait_until};

const MESSAGE_TYPE: &str = "perf";

fn report(label: &str, count: usize, elapsed: Duration) {
    let per_second = count as f64 / elapsed.as_secs_f64().max(0.0001);
    println!(
        "{label}: {count} msgs in {} ms = {per_second:.0} msg/s",
        elapsed.as_millis()
    );
}

/// C# `NoTrim`: no trimming, so the run measures the write path only.
fn no_trim(partitions: u32) -> TopicOptions {
    TopicOptions {
        partitions,
        trim: TrimMode::None,
        max_len: Some(i64::MAX.unsigned_abs()),
        background_trim_interval_seconds: 0,
        ..TopicOptions::default()
    }
}

async fn publisher(redis: &RedisFixture, topic: &str, options: TopicOptions) -> StreamPublisher {
    StreamPublisher::new(redis.connect().await, redis.keys(), topic, options)
        .await
        .expect("publisher")
}

async fn direct_single_publish(body_bytes: usize) {
    const TOTAL: usize = 5_000;
    let redis = require_redis!();
    let topic = redis.new_topic("perf-direct");
    let publisher = publisher(redis, &topic, no_trim(4)).await;
    let body = Bytes::from(vec![0u8; body_bytes]);
    let options = PublishOptions::default();

    let sw = Instant::now();
    for _ in 0..TOTAL {
        publisher
            .publish("key", body.clone(), MESSAGE_TYPE, &options)
            .await
            .expect("publish");
    }
    report(
        &format!("Direct single XADD (body={body_bytes}B)"),
        TOTAL,
        sw.elapsed(),
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn direct_single_publish_128() {
    direct_single_publish(128).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn direct_single_publish_2048() {
    direct_single_publish(2048).await;
}

async fn batch_publish(batch_size: usize) {
    const TOTAL: usize = 100_000;
    let redis = require_redis!();
    let topic = redis.new_topic("perf-batch");
    let publisher = publisher(redis, &topic, no_trim(4)).await;
    let bodies: Vec<Bytes> = (0..batch_size)
        .map(|_| Bytes::from(vec![0u8; 128]))
        .collect();
    let options = PublishOptions::default();

    let sw = Instant::now();
    let mut sent = 0;
    while sent < TOTAL {
        let count = batch_size.min(TOTAL - sent);
        publisher
            .publish_batch("key", &bodies[..count], MESSAGE_TYPE, &options)
            .await
            .expect("publish_batch");
        sent += count;
    }
    report(
        &format!("Pipelined publish_batch (batch={batch_size})"),
        TOTAL,
        sw.elapsed(),
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn batch_publish_100() {
    batch_publish(100).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn batch_publish_500() {
    batch_publish(500).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn batch_publish_2000() {
    batch_publish(2_000).await;
}

async fn buffered_publish(max_batch: u32, max_wait_ms: u32) {
    const TOTAL: usize = 100_000;
    let redis = require_redis!();
    let topic = redis.new_topic("perf-buffered");
    let direct = publisher(redis, &topic, no_trim(4)).await;
    let buffered = BufferedStreamPublisher::new(
        direct,
        ProducerOptions {
            topic: topic.clone(),
            buffered: true,
            max_batch,
            max_wait_ms,
            max_queue: 50_000,
            ..ProducerOptions::default()
        },
    );
    let body = Bytes::from(vec![0u8; 128]);
    let options = PublishOptions::default();

    let sw = Instant::now();
    for _ in 0..TOTAL {
        buffered
            .enqueue("key", body.clone(), MESSAGE_TYPE, &options)
            .await
            .expect("enqueue");
    }
    buffered.flush().await.expect("flush");
    let elapsed = sw.elapsed();
    report(
        &format!("BufferedStreamPublisher (max_batch={max_batch} max_wait_ms={max_wait_ms})"),
        TOTAL,
        elapsed,
    );
    buffered.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn buffered_publish_500_10() {
    buffered_publish(500, 10).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn buffered_publish_2000_25() {
    buffered_publish(2_000, 25).await;
}

/// Counts what it is handed and does nothing else, as the C# benchmark's handler.
struct Count(Arc<AtomicUsize>);

impl BatchHandler for Count {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        self.0.fetch_add(batch.len(), Ordering::Relaxed);
        Ok(())
    }
}

/// C# `PublishSpreadAsync`: `count` messages over `keys` keys, each key's share published
/// concurrently in pipelined chunks of 1000 with 64-byte bodies.
async fn publish_spread(publisher: Arc<StreamPublisher>, count: usize, keys: usize) {
    let mut tasks = Vec::new();
    for k in 0..keys {
        let n = count / keys + usize::from(k < count % keys);
        let publisher = Arc::clone(&publisher);
        tasks.push(tokio::spawn(async move {
            let key = format!("key-{k}");
            let bodies: Vec<Bytes> = (0..n.min(1_000))
                .map(|_| Bytes::from(vec![0u8; 64]))
                .collect();
            let options = PublishOptions::default();
            let mut offset = 0;
            while offset < n {
                let chunk = bodies.len().min(n - offset);
                publisher
                    .publish_batch(&key, &bodies[..chunk], MESSAGE_TYPE, &options)
                    .await
                    .expect("publish_batch");
                offset += chunk;
            }
        }));
    }
    for t in tasks {
        t.await.expect("publish task");
    }
}

async fn end_to_end_consume(
    partitions: u32,
    backpressure: bool,
    read_mode: ReadMode,
    reader_threads: u32,
) {
    const TOTAL: usize = 100_000;
    let redis = require_redis!();
    let topic = redis.new_topic("perf-e2e");
    let topic_options = no_trim(partitions);
    let publisher = Arc::new(publisher(redis, &topic, topic_options.clone()).await);
    publish_spread(publisher, TOTAL, partitions as usize * 4).await;

    let consumed = Arc::new(AtomicUsize::new(0));
    let mut root = StreamOptions::default();
    root.topics.insert(topic.clone(), topic_options);
    let consumer = ConsumerOptions {
        topic: topic.clone(),
        consumer: Some(redis.new_consumer("perf-consumer")),
        batch_size: 500,
        block_ms: 100,
        read_mode,
        reader_threads,
        persist: PersistMode::AsyncBatch,
        persist_interval_ms: 250,
        start_from: StartFrom::Beginning,
        backpressure: BackpressureOptions {
            enabled: backpressure,
            capacity: 8,
        },
        ..ConsumerOptions::default()
    };

    let sw = Instant::now();
    let host = Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(root)
        // trim = none (the C# benchmark's NoTrim) needs an explicit opt-in.
        .allow_untrimmed(true)
        .topic(&topic)
        .options(consumer)
        .handler(Count(Arc::clone(&consumed)))
        .spawn()
        .await
        .expect("spawn");
    wait_until(
        || consumed.load(Ordering::Relaxed) >= TOTAL,
        Some(Duration::from_secs(60)),
        "all messages to be consumed",
    )
    .await;
    let elapsed = sw.elapsed();
    host.shutdown().await.expect("shutdown");
    report(
        &format!(
            "End-to-end consume (partitions={partitions} backpressure={backpressure} read_mode={read_mode:?} reader_threads={reader_threads})"
        ),
        TOTAL,
        elapsed,
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn consume_1_partition_block() {
    end_to_end_consume(1, true, ReadMode::Block, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn consume_4_partitions_block() {
    end_to_end_consume(4, true, ReadMode::Block, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn consume_4_partitions_backpressure_off() {
    end_to_end_consume(4, false, ReadMode::Block, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn consume_4_partitions_multi_reader() {
    end_to_end_consume(4, true, ReadMode::Block, 4).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn consume_4_partitions_poll() {
    end_to_end_consume(4, true, ReadMode::Poll, 1).await;
}

/// Not a C# benchmark: the floor under the end-to-end consume numbers. Reads the same 100 000
/// entries (4 partitions) with bare `XREAD COUNT 500` calls on one connection and redis-rs's own
/// `StreamReadReply` conversion, with no consumer host. On the reference machine the host consumes
/// within ~10% of this, and a profile of either is dominated by redis-rs's RESP parser, so that,
/// not the library, is what bounds consume throughput.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf: run with --ignored --release"]
async fn raw_xread_floor() {
    const TOTAL: usize = 100_000;
    let redis = require_redis!();
    let topic = redis.new_topic("perf-raw");
    let topic_options = no_trim(4);
    let streams: Vec<String> = (0..4)
        .map(|p| {
            redis
                .keys()
                .stream(&topic, p, topic_options.co_locate_partitions)
        })
        .collect();
    let publisher = Arc::new(publisher(redis, &topic, topic_options).await);
    publish_spread(publisher, TOTAL, 16).await;

    let mut conn = redis
        .client()
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let mut after = vec!["0-0".to_owned(); streams.len()];
    let mut read = 0;
    let sw = Instant::now();
    while read < TOTAL {
        let reply: redis::streams::StreamReadReply = redis::cmd("XREAD")
            .arg("COUNT")
            .arg(500)
            .arg("STREAMS")
            .arg(&streams)
            .arg(&after)
            .query_async(&mut conn)
            .await
            .expect("XREAD");
        for key in reply.keys {
            let p = streams.iter().position(|s| *s == key.key).expect("stream");
            read += key.ids.len();
            if let Some(last) = key.ids.last() {
                after[p].clone_from(&last.id);
            }
        }
    }
    report(
        "Raw XREAD floor (partitions=4, no host)",
        read,
        sw.elapsed(),
    );
}
