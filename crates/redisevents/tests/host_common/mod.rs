//! Shared helpers for the consumer-host integration tests (C4t). Included per test file with
//! `mod host_common;`, so not every helper is used by every file.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use redisevents::admin::health::{HealthValue, StreamHealthReport};
use redisevents::config::{ConsumerOptions, TopicOptions, TrimMode};
use redisevents::consumer::{Streams, StreamsHost};
use redisevents::positions::{PositionStore, RedisPositionStore};
use redisevents::producer::{PublishOptions, Publisher, StreamPublisher};
use redisevents::wire::{StreamId, StreamMsg};
use redisevents_test_support::{RecordingHandler, RedisFixture, wait_until};
use uuid::Uuid;

/// Body size in bytes: a four-byte big-endian index plus filler.
pub const BODY_BYTES: usize = 64;

/// The message type every test publishes.
pub const MESSAGE_TYPE: &str = "test.event";

/// A body whose first four bytes are the big-endian global `index`.
pub fn indexed_body(index: u32) -> Bytes {
    let mut body = vec![0_u8; BODY_BYTES];
    body[..4].copy_from_slice(&index.to_be_bytes());
    Bytes::from(body)
}

/// The global index a body built by [`indexed_body`] carries.
pub fn index_of(msg: &StreamMsg) -> u32 {
    let body = msg.body();
    u32::from_be_bytes([body[0], body[1], body[2], body[3]])
}

/// A plain UTF-8 body.
pub fn text(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// Topic options for volume tests: no trimming, so every entry stays on its stream.
pub fn volume(partitions: u32) -> TopicOptions {
    TopicOptions {
        partitions,
        trim: TrimMode::None,
        max_len: Some(u64::MAX),
        background_trim_interval_seconds: 0,
        ..TopicOptions::default()
    }
}

/// A publisher for `topic` on `redis`.
pub async fn publisher(
    redis: &RedisFixture,
    topic: &str,
    options: TopicOptions,
) -> StreamPublisher {
    StreamPublisher::new(redis.connect().await, redis.keys(), topic, options)
        .await
        .expect("publisher")
}

/// Publishes the global indexes `from..from + count`, spread over `keys` partition keys
/// (`key-<i % keys>`), each key's messages in ascending order.
pub async fn publish_spread(publisher: &StreamPublisher, from: u32, count: u32, keys: u32) {
    for k in 0..keys {
        let indexes: Vec<u32> = (from..from + count).filter(|i| i % keys == k).collect();
        for chunk in indexes.chunks(500) {
            let bodies: Vec<Bytes> = chunk.iter().map(|i| indexed_body(*i)).collect();
            publisher
                .publish_batch(&key_name(k), &bodies, MESSAGE_TYPE, &PublishOptions::new())
                .await
                .expect("publish_batch");
        }
    }
}

/// The partition key used for key slot `k`.
pub fn key_name(k: u32) -> String {
    format!("key-{k}")
}

/// Waits until `handler` has recorded at least `count` messages.
pub async fn wait_for_count(handler: &RecordingHandler, count: usize, what: &str) {
    wait_until(
        || handler.count() >= count,
        Some(Duration::from_secs(60)),
        &format!("{count} messages: {what}"),
    )
    .await;
}

/// Waits until `handler` has recorded every index up to `max_index` and at least `count` messages.
pub async fn wait_for_max_index(
    handler: &RecordingHandler,
    count: usize,
    max_index: u32,
    what: &str,
) {
    wait_until(
        || {
            handler.count() >= count
                && handler
                    .messages()
                    .iter()
                    .map(index_of)
                    .max()
                    .is_some_and(|m| m >= max_index)
        },
        Some(Duration::from_secs(60)),
        what,
    )
    .await;
}

/// Counts of the indexes a recording saw: `(missing, duplicated)` over `0..total`.
pub fn audit(messages: &[StreamMsg], total: u32) -> (usize, usize) {
    let mut seen = vec![0_usize; total as usize];
    for msg in messages {
        let i = index_of(msg);
        if i < total {
            seen[i as usize] += 1;
        }
    }
    let missing = seen.iter().filter(|c| **c == 0).count();
    let duplicated = seen.iter().map(|c| c.saturating_sub(1)).sum();
    (missing, duplicated)
}

/// The raw redis-rs connection for assertions that go around the library.
pub async fn raw(redis: &RedisFixture) -> redis::aio::ConnectionManager {
    redis.connect().await.shared()
}

/// Length of the stream key of `(topic, partition)`.
pub async fn xlen(redis: &RedisFixture, topic: &str, partition: u32) -> i64 {
    let mut conn = raw(redis).await;
    redis::cmd("XLEN")
        .arg(redis.keys().stream(topic, partition, true))
        .query_async(&mut conn)
        .await
        .expect("XLEN")
}

/// Builds and starts one consumer on `topic` through the host, with `configure` applied to its
/// options (`consumer` is its name).
pub async fn start_host(
    redis: &RedisFixture,
    topic: &str,
    topic_options: TopicOptions,
    consumer: &str,
    configure: impl FnOnce(&mut ConsumerOptions),
    handler: Arc<RecordingHandler>,
) -> StreamsHost {
    Streams::builder(redis.connect().await)
        .keys(redis.keys())
        // trim = none is refused without an explicit opt-in.
        .allow_untrimmed(true)
        .topic_options(topic, topic_options)
        .topic(topic)
        .consumer(consumer)
        .configure(configure)
        .handler(handler)
        .spawn()
        .await
        .expect("spawn the consumer host")
}

/// The stored positions of `(topic, consumer)`, read through the Redis position store.
pub async fn positions(
    redis: &RedisFixture,
    topic: &str,
    consumer: &str,
) -> BTreeMap<u32, StreamId> {
    RedisPositionStore::new(redis.connect().await, redis.keys(), Uuid::nil())
        .load(topic, consumer)
        .await
        .expect("load positions")
}

/// An integer field of a health report's data, or `None`.
pub fn health_int(report: &StreamHealthReport, key: &str) -> Option<i64> {
    match report.data.get(key)? {
        HealthValue::Int(v) => Some(*v),
        _ => None,
    }
}

/// Writes `count` entries to `partition` of `topic`, one per `step_ms`, from `first_ms`, with explicit
/// ids, and returns those ids.
pub async fn seed_entries(
    redis: &RedisFixture,
    topic: &str,
    partition: u32,
    first_ms: u64,
    step_ms: u64,
    count: u32,
) -> Vec<StreamId> {
    let mut conn = redis.connect().await.shared();
    let key = redis.keys().stream(topic, partition, true);
    let mut ids = Vec::new();
    for i in 0..count {
        let id = StreamId::new(first_ms + u64::from(i) * step_ms, 0);
        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg(id.to_string())
            .arg("b")
            .arg(format!("msg-{i:03}"))
            .arg("t")
            .arg("replay-test")
            .arg("k")
            .arg("")
            .query_async(&mut conn)
            .await
            .expect("XADD");
        ids.push(id);
    }
    ids
}
