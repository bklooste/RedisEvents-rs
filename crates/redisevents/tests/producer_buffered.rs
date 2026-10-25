//! Integration tests for the buffered publisher (`BufferedStreamPublisher`) against a real Redis.
//! Port of the buffered parts of `ProducerServiceTests.cs` (named after the C# test in
//! `snake_case`) plus the cold-first-flush ordering regression. Skips without Redis
//! (`require_redis!`).

use bytes::Bytes;
use redis::aio::ConnectionManager;
use redisevents::config::{ProducerOptions, TopicOptions, TrimMode};
use redisevents::producer::{BufferedStreamPublisher, PublishOptions, StreamPublisher};
use redisevents::wire::partition_for_key;
use redisevents::{BufferedPublisher, Error};
use redisevents_test_support::RedisFixture;

fn topic_options(partitions: u32) -> TopicOptions {
    TopicOptions {
        partitions,
        trim: TrimMode::None,
        ..TopicOptions::default()
    }
}

async fn buffered(
    redis: &RedisFixture,
    topic: &str,
    topic_options: TopicOptions,
    producer: ProducerOptions,
) -> BufferedStreamPublisher {
    let inner = StreamPublisher::new(redis.connect().await, redis.keys(), topic, topic_options)
        .await
        .expect("publisher");
    BufferedStreamPublisher::new(inner, producer)
}

fn producer(topic: &str, max_batch: u32, max_wait_ms: u32, max_queue: u32) -> ProducerOptions {
    ProducerOptions {
        topic: topic.to_owned(),
        buffered: true,
        max_wait_ms,
        max_batch,
        max_queue,
        drop_oldest: false,
    }
}

/// The `b` (body) and `t` (type) fields of every entry of one stream, in stream order. Paged, so a
/// large stream does not run into the connection's response timeout.
async fn bodies(conn: &mut ConnectionManager, key: &str) -> Vec<(Vec<u8>, String)> {
    let mut out = Vec::new();
    let mut start = "-".to_owned();
    loop {
        let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(key)
            .arg(&start)
            .arg("+")
            .arg("COUNT")
            .arg(1_000)
            .query_async(conn)
            .await
            .expect("XRANGE");
        let Some(last) = reply.ids.last().map(|e| e.id.clone()) else {
            return out;
        };
        for e in reply.ids {
            let b: Vec<u8> = e.get("b").expect("b");
            let t: String = e.get("t").expect("t");
            out.push((b, t));
        }
        start = format!("({last}");
    }
}

/// C# `S16_buffered_producer_delivers_every_enqueue_after_flush`: 100 000 enqueues on four
/// partitions all land, exactly once each, once `flush` returns.
#[tokio::test]
async fn s16_buffered_producer_delivers_every_enqueue_after_flush() {
    const COUNT: u32 = 100_000;
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("s16-buffered");
    let b = buffered(
        redis,
        &topic,
        topic_options(4),
        producer(&topic, 500, 20, COUNT),
    )
    .await;

    for i in 0..COUNT {
        b.enqueue(
            &i.to_string(),
            Bytes::copy_from_slice(&i.to_be_bytes()),
            "S16.Buffered",
            &PublishOptions::new(),
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(60), b.flush())
        .await
        .expect("flush in time")
        .unwrap();

    let mut conn = redis.connect().await.shared();
    let mut seen = vec![false; COUNT as usize];
    let mut total = 0;
    for p in 0..4 {
        for (body, t) in bodies(&mut conn, &redis.keys().stream(&topic, p, true)).await {
            assert_eq!(t, "S16.Buffered");
            let i = u32::from_be_bytes(body.try_into().unwrap());
            assert_eq!(partition_for_key(i.to_string().as_bytes(), 4), p);
            assert!(!seen[i as usize], "message {i} published twice");
            seen[i as usize] = true;
            total += 1;
        }
    }
    assert_eq!(total, COUNT);
    b.shutdown().await.unwrap();
}

/// Regression for the C# cold-publisher bug: a brand-new topic and publisher, many messages on one
/// key, several batches; the first flush must not reorder them.
#[tokio::test]
async fn cold_publisher_preserves_per_key_order_on_first_flush() {
    const COUNT: u32 = 2_000;
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("cold-order");
    let b = buffered(
        redis,
        &topic,
        topic_options(4),
        producer(&topic, 500, 60_000, COUNT),
    )
    .await;

    for i in 0..COUNT {
        b.enqueue(
            "one-key",
            Bytes::from(i.to_string()),
            "T",
            &PublishOptions::new(),
        )
        .await
        .unwrap();
    }
    b.flush().await.unwrap();

    let mut conn = redis.connect().await.shared();
    let p = partition_for_key(b"one-key", 4);
    let got: Vec<u32> = bodies(&mut conn, &redis.keys().stream(&topic, p, true))
        .await
        .into_iter()
        .map(|(b, _)| String::from_utf8(b).unwrap().parse().unwrap())
        .collect();
    assert_eq!(got, (0..COUNT).collect::<Vec<_>>());
}

/// Graceful shutdown flushes what is queued even when no trigger has fired, then rejects.
#[tokio::test]
async fn shutdown_flushes_everything_queued() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("shutdown");
    let b = buffered(
        redis,
        &topic,
        topic_options(1),
        producer(&topic, 1_000, 60_000, 1_000),
    )
    .await;
    for i in 0..100_u32 {
        b.enqueue("k", Bytes::from(i.to_string()), "T", &PublishOptions::new())
            .await
            .unwrap();
    }
    b.shutdown().await.unwrap();

    let mut conn = redis.connect().await.shared();
    assert_eq!(
        bodies(&mut conn, &redis.keys().stream(&topic, 0, true))
            .await
            .len(),
        100
    );
    assert!(matches!(
        b.enqueue("k", Bytes::from_static(b"x"), "T", &PublishOptions::new())
            .await,
        Err(Error::Closed(_))
    ));
}
