//! Focused tests of the consumer host (C4): lifecycle, ownership change, shutdown order, reset
//! while running, ErrorPolicy.Fail, the shutdown budget and startup refusals. The bulk port of the
//! C# consumer suites is C4t's. Skips without Redis.

#![allow(
    clippy::cast_possible_truncation,
    clippy::items_after_statements,
    clippy::unnecessary_wraps
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::config::{
    ConsumerOptions, ErrorPolicy, InstanceMode, InstanceOptions, PersistMode, StreamOptions,
    TopicOptions,
};
use redisevents::consumer::{HandleError, Streams, StreamsHost};
use redisevents::producer::{PublishOptions, Publisher, StreamPublisher};
use redisevents::wire::StreamId;
use redisevents::{BatchHandler, Error, HandleResult, PartitionContext, StreamMsg};
use redisevents_test_support::{RecordingHandler, RedisFixture, require_redis, wait_until};

const KEYS16: [&str; 16] = [
    "k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10", "k11", "k12", "k13", "k14",
    "k15",
];
const WAIT: Option<Duration> = Some(Duration::from_secs(20));

fn options(topic: &str, partitions: u32) -> StreamOptions {
    let mut o = StreamOptions::default();
    o.topics.insert(
        topic.to_owned(),
        TopicOptions {
            partitions,
            ..TopicOptions::default()
        },
    );
    o
}

fn consumer_options(topic: &str, consumer: &str) -> ConsumerOptions {
    ConsumerOptions {
        topic: topic.to_owned(),
        consumer: Some(consumer.to_owned()),
        persist_interval_ms: 50,
        block_ms: 200,
        ..ConsumerOptions::default()
    }
}

fn lease(ttl: u32, renew: u32) -> Option<InstanceOptions> {
    Some(InstanceOptions {
        mode: Some(InstanceMode::Lease),
        lease_ttl_seconds: ttl,
        lease_renew_seconds: renew,
        ..InstanceOptions::default()
    })
}

async fn spawn<H: BatchHandler>(
    redis: &RedisFixture,
    topic: &str,
    partitions: u32,
    consumer: ConsumerOptions,
    handler: H,
) -> redisevents::Result<StreamsHost> {
    Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(options(topic, partitions))
        .topic(topic)
        .options(consumer)
        .handler(handler)
        .spawn()
        .await
}

async fn publish(redis: &RedisFixture, topic: &str, partitions: u32, keys: &[&str], per_key: u32) {
    let publisher = StreamPublisher::new(
        redis.connect().await,
        redis.keys(),
        topic,
        TopicOptions {
            partitions,
            ..TopicOptions::default()
        },
    )
    .await
    .expect("publisher");
    for i in 0..per_key {
        for key in keys {
            publisher
                .publish(
                    key,
                    Bytes::from(format!("{key}|{i}")),
                    "t",
                    &PublishOptions::default(),
                )
                .await
                .expect("publish");
        }
    }
}

async fn hgetall(redis: &RedisFixture, key: &str) -> HashMap<String, String> {
    redis::cmd("HGETALL")
        .arg(key)
        .query_async(&mut redis.connect().await.shared())
        .await
        .expect("HGETALL")
}

/// Stored positions (numeric fields only), id part.
async fn positions(redis: &RedisFixture, topic: &str, consumer: &str) -> HashMap<u32, StreamId> {
    hgetall(redis, &redis.keys().positions(topic, consumer))
        .await
        .into_iter()
        .filter_map(|(k, v)| {
            let p = k.parse::<u32>().ok()?;
            Some((p, StreamId::parse(v.split('|').next()?).ok()?))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn host_consumes_flushes_on_shutdown_and_resumes_without_replay() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-lifecycle");
    let consumer = redis.new_consumer("c");
    publish(redis, &topic, 4, &KEYS16, 1).await;

    let first = Arc::new(RecordingHandler::new());
    let host = spawn(
        redis,
        &topic,
        4,
        consumer_options(&topic, &consumer),
        first.clone(),
    )
    .await
    .expect("spawn");
    assert_eq!(host.consumers(), 1);
    assert_eq!(host.consumers_started(), 1);
    assert!(host.is_running());
    first.wait_for_exactly(16, None, WAIT).await;
    let owned = host.owned_partitions(&topic, &consumer);
    assert_eq!(owned, vec![0, 1, 2, 3]);
    host.shutdown().await.expect("clean shutdown");

    // Shutdown order: final flush wrote every partition; ownership released (no partition fields).
    let stored = positions(redis, &topic, &consumer).await;
    assert_eq!(stored.len(), 4, "every partition flushed: {stored:?}");
    let claims = hgetall(redis, &redis.keys().ownership(&topic, &consumer)).await;
    assert!(
        !claims.keys().any(|k| k.parse::<u32>().is_ok()),
        "claims released on shutdown: {claims:?}"
    );

    // Same consumer again: resumes after the flushed positions, nothing replays.
    publish(redis, &topic, 4, &["a"], 1).await;
    let second = Arc::new(RecordingHandler::new());
    let host = spawn(
        redis,
        &topic,
        4,
        consumer_options(&topic, &consumer),
        second.clone(),
    )
    .await
    .expect("respawn");
    second.wait_for_exactly(1, None, WAIT).await;
    assert_eq!(second.bodies(), vec!["a|0".to_owned()]);
    host.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_moves_to_the_survivor_when_the_holder_shuts_down() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-lease");
    let consumer = redis.new_consumer("c");
    let mut opts = consumer_options(&topic, &consumer);
    opts.instances = lease(3, 1);

    let a = Arc::new(RecordingHandler::new());
    let host_a = spawn(redis, &topic, 4, opts.clone(), a.clone())
        .await
        .expect("a");
    assert_eq!(host_a.owned_partitions(&topic, &consumer), vec![0, 1, 2, 3]);

    // A second instance (its own instance id; one process would otherwise share one).
    let b = Arc::new(RecordingHandler::new());
    let host_b = spawn_with_pod(redis, &topic, &opts, b.clone(), "pod-b").await;
    assert!(
        host_b.owned_partitions(&topic, &consumer).is_empty(),
        "every partition is held by a"
    );

    host_a.shutdown().await.expect("a shutdown");
    wait_until(
        || host_b.owned_partitions(&topic, &consumer) == vec![0, 1, 2, 3],
        WAIT,
        "b picks up every released partition",
    )
    .await;
    publish(redis, &topic, 4, &["k1", "k2", "k3", "k4"], 2).await;
    b.wait_for(8, WAIT).await;
    host_b.shutdown().await.expect("b shutdown");
}

/// A second host whose registry has another instance id (same process otherwise shares one).
async fn spawn_with_pod(
    redis: &RedisFixture,
    topic: &str,
    opts: &ConsumerOptions,
    handler: Arc<RecordingHandler>,
    pod: &str,
) -> StreamsHost {
    Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(options(topic, 4))
        .topic(topic)
        .options(opts.clone())
        .instance_id(redisevents::ownership::instance_id_for(Some(pod)))
        .handler(handler)
        .spawn()
        .await
        .expect("spawn b")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_reset_rewinds_a_running_consumer() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-reset");
    let consumer = redis.new_consumer("c");
    publish(redis, &topic, 1, &["k"], 5).await;
    let handler = Arc::new(RecordingHandler::new());
    let host = spawn(
        redis,
        &topic,
        1,
        consumer_options(&topic, &consumer),
        handler.clone(),
    )
    .await
    .expect("spawn");
    handler.wait_for(5, WAIT).await;
    // Let the position land, then rewind to the start while running.
    wait_until_positions(redis, &topic, &consumer, 1).await;
    StreamAdmin::new(redis.connect().await, redis.keys())
        .reset_position(
            &topic,
            &consumer,
            ResetTarget::Start,
            &ResetOptions::default(),
        )
        .await
        .expect("reset");
    handler.wait_for(10, WAIT).await;
    let bodies = handler.bodies();
    assert_eq!(&bodies[5..10], &bodies[0..5], "replayed from the start");
    // The flusher removes the marker once the read loop has taken it.
    let key = redis.keys().positions(&topic, &consumer);
    let deadline = Instant::now() + Duration::from_secs(20);
    while hgetall(redis, &key)
        .await
        .keys()
        .any(|k| k.starts_with("__reset"))
    {
        assert!(Instant::now() < deadline, "marker never consumed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    host.shutdown().await.expect("shutdown");
}

async fn wait_until_positions(redis: &RedisFixture, topic: &str, consumer: &str, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while positions(redis, topic, consumer).await.len() < n {
        assert!(Instant::now() < deadline, "positions never flushed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

struct Failing;

impl BatchHandler for Failing {
    #[allow(clippy::unused_async_trait_impl)]
    async fn handle_batch(&self, _batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        Err(HandleError::skip(std::io::Error::other("boom")))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_policy_fail_fails_the_host() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-fail");
    let consumer = redis.new_consumer("c");
    publish(redis, &topic, 1, &["k"], 1).await;
    let mut opts = consumer_options(&topic, &consumer);
    opts.on_error = ErrorPolicy::Fail;
    let host = spawn(redis, &topic, 1, opts, Failing).await.expect("spawn");
    let closed = tokio::time::timeout(Duration::from_secs(20), host.closed())
        .await
        .expect("host stops itself");
    let err = closed.expect_err("ErrorPolicy.Fail surfaces");
    assert!(err.to_string().contains("boom"), "{err}");
    assert!(!host.is_running());
    assert!(host.shutdown().await.is_err());
}

struct Stubborn(Arc<AtomicUsize>);

impl BatchHandler for Stubborn {
    async fn handle_batch(&self, _batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        // Ignores its cancellation token.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_drops_a_handler_that_outlives_the_budget() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-budget");
    let consumer = redis.new_consumer("c");
    publish(redis, &topic, 1, &["k"], 1).await;
    let mut opts = consumer_options(&topic, &consumer);
    opts.shutdown_timeout_seconds = 1;
    let calls = Arc::new(AtomicUsize::new(0));
    let host = spawn(redis, &topic, 1, opts, Stubborn(calls.clone()))
        .await
        .expect("spawn");
    wait_until(|| calls.load(Ordering::SeqCst) > 0, WAIT, "handler entered").await;
    let started = Instant::now();
    host.shutdown().await.expect("shutdown");
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "shutdown bounded by the budget, took {:?}",
        started.elapsed()
    );
    // Nothing was recorded for the unfinished batch.
    assert!(positions(redis, &topic, &consumer).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_refuses_an_unknown_codec_version() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-codec");
    let consumer = redis.new_consumer("c");
    let meta = redis.keys().topic_meta(&topic);
    redis::cmd("HSET")
        .arg(&meta)
        .arg("partitions")
        .arg(1)
        .arg("v")
        .arg(2)
        .query_async::<()>(&mut redis.connect().await.shared())
        .await
        .expect("HSET");
    let err = spawn(
        redis,
        &topic,
        1,
        consumer_options(&topic, &consumer),
        RecordingHandler::new(),
    )
    .await
    .expect_err("refused");
    assert!(matches!(err, Error::Config(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_persistence_over_a_null_store_is_refused() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-sync-null");
    let consumer = redis.new_consumer("c");
    let mut opts = consumer_options(&topic, &consumer);
    opts.persist = PersistMode::SyncBatch;
    let err = Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(options(&topic, 1))
        .topic(&topic)
        .options(opts)
        .position_store(redisevents::positions::NullPositionStore)
        .handler(RecordingHandler::new())
        .spawn()
        .await
        .expect_err("refused");
    assert!(matches!(err, Error::Config(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persist_none_consumes_and_writes_no_positions() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-none");
    let consumer = redis.new_consumer("c");
    publish(redis, &topic, 2, &["a", "b"], 3).await;
    let mut opts = consumer_options(&topic, &consumer);
    opts.persist = PersistMode::None;
    opts.start_from = redisevents::config::StartFrom::Beginning;
    let handler = Arc::new(RecordingHandler::new());
    let host = spawn(redis, &topic, 2, opts, handler.clone())
        .await
        .expect("spawn");
    handler.wait_for_exactly(6, None, WAIT).await;
    host.shutdown().await.expect("shutdown");
    assert!(positions(redis, &topic, &consumer).await.is_empty());
}
