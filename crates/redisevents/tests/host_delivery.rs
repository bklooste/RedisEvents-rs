//! Delivery and ordering through the consumer host against real Redis. Port of the C#
//! `DeliveryTests` (S4, S5, S7, S8, S11), named after the C# test in `snake_case`.
//!
//! Every message body carries its global index (big-endian, first four bytes), so "exactly once" is
//! a count over a bitmap and "in publish order" is an ascending check. No topic is shared and no test
//! flushes the server. Skips without Redis. Needs the C4 host: ignored until it lands.
//!
//! Not ported: S11's heap-growth bound (`GC.GetTotalMemory`) has no Rust counterpart observable
//! through the public API; the rest of S11 (in-flight bound, backlog left in Redis) is asserted.

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
use std::time::Duration;

use host_common::*;
use redisevents::Publisher;
use redisevents::config::{
    BackpressureOptions, ConsumerOptions, InstanceMode, InstanceOptions, PersistMode, StartFrom,
};
use redisevents::wire::StreamMsg;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until};

/// S4. Ten thousand messages published under one partition key are handed to the handler in publish
/// order, with nothing lost, duplicated or reordered.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s4_ten_thousand_messages_on_one_partition_key_arrive_in_publish_order() {
    let redis = require_redis!();
    const TOTAL: u32 = 10_000;
    const PARTITIONS: u32 = 4;
    let topic = redis.new_topic("s4");
    let publisher = publisher(redis, &topic, volume(PARTITIONS)).await;
    let bodies: Vec<_> = (0..TOTAL).map(indexed_body).collect();
    for chunk in bodies.chunks(500) {
        publisher
            .publish_batch(
                "one-key",
                chunk,
                MESSAGE_TYPE,
                &redisevents::producer::PublishOptions::new(),
            )
            .await
            .expect("publish_batch");
    }

    let recorder = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &redis.new_consumer("s4"),
        |o| {
            o.batch_size = 250;
            o.block_ms = 250;
            o.persist = PersistMode::AsyncBatch;
            o.persist_interval_ms = 100;
            o.start_from = StartFrom::Beginning;
        },
        recorder.clone(),
    )
    .await;
    wait_for_count(&recorder, TOTAL as usize, "all delivered").await;
    host.shutdown().await.expect("shutdown");

    assert_eq!(
        recorder.count(),
        TOTAL as usize,
        "one key on one partition must not be redelivered"
    );
    let messages = recorder.messages();
    let partitions: std::collections::BTreeSet<u32> =
        messages.iter().map(StreamMsg::partition).collect();
    assert_eq!(
        partitions.len(),
        1,
        "a single partition key hashes to exactly one partition"
    );
    let delivered: Vec<u32> = messages.iter().map(index_of).collect();
    assert!(
        delivered.windows(2).all(|w| w[0] < w[1]),
        "publish order is delivery order"
    );
    assert_eq!(delivered.first(), Some(&0));
    assert_eq!(delivered.last(), Some(&(TOTAL - 1)));
}

/// S5. Eighty thousand messages across eight partitions and thirty-two keys, consumed by two
/// instances that split the partitions 0-3 / 4-7: every message is delivered exactly once, no partition
/// is consumed by both instances, and each key's messages stay in publish order.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s5_eighty_thousand_messages_across_eight_partitions_are_delivered_exactly_once_in_key_order()
 {
    let redis = require_redis!();
    const PARTITIONS: u32 = 8;
    const KEYS: u32 = 32;
    const PER_KEY: u32 = 2_500;
    const TOTAL: u32 = KEYS * PER_KEY;
    let topic = redis.new_topic("s5");
    let consumer = redis.new_consumer("s5");
    let publisher = publisher(redis, &topic, volume(PARTITIONS)).await;
    publish_spread(&publisher, 0, TOTAL, KEYS).await;

    let recorder = Arc::new(RecordingHandler::new());
    let instance = |index: u32| {
        move |o: &mut ConsumerOptions| {
            o.batch_size = 250;
            // Bounds the tail of this test: a long block parks the shared reader connection.
            o.block_ms = 100;
            o.persist = PersistMode::AsyncBatch;
            o.persist_interval_ms = 100;
            o.start_from = StartFrom::Beginning;
            o.instances = Some(InstanceOptions {
                mode: Some(InstanceMode::Static),
                count: Some(2),
                index: Some(index),
                ..InstanceOptions::default()
            });
        }
    };
    let first = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &consumer,
        instance(0),
        recorder.clone(),
    )
    .await;
    let second = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &consumer,
        instance(1),
        recorder.clone(),
    )
    .await;
    wait_for_count(
        &recorder,
        TOTAL as usize,
        "all delivered across both instances",
    )
    .await;
    let owned_first = first.owned_partitions(&topic, &consumer);
    let owned_second = second.owned_partitions(&topic, &consumer);
    first.shutdown().await.expect("shutdown first");
    second.shutdown().await.expect("shutdown second");

    assert_eq!(owned_first, vec![0, 1, 2, 3]);
    assert_eq!(owned_second, vec![4, 5, 6, 7]);
    let (missing, duplicated) = audit(&recorder.messages(), TOTAL);
    assert_eq!(missing, 0, "no message may be lost");
    assert_eq!(
        duplicated, 0,
        "nothing failed and nothing restarted, so nothing may be redelivered"
    );
    assert_eq!(recorder.count(), TOTAL as usize);
    let partitions: std::collections::BTreeSet<u32> = recorder
        .messages()
        .iter()
        .map(StreamMsg::partition)
        .collect();
    assert_eq!(
        partitions.len(),
        PARTITIONS as usize,
        "32 keys over 8 partitions reach all of them"
    );

    // Per-key order: a key lives on one partition, so its indexes ascend on that partition.
    let mut per_key: std::collections::HashMap<String, Vec<u32>> = std::collections::HashMap::new();
    for msg in recorder.messages() {
        per_key
            .entry(msg.partition_key().to_owned())
            .or_default()
            .push(index_of(&msg));
    }
    assert_eq!(per_key.len(), KEYS as usize);
    for (key, indexes) in per_key {
        assert_eq!(
            indexes.len(),
            PER_KEY as usize,
            "key {key} was published {PER_KEY} times"
        );
        assert!(
            indexes.windows(2).all(|w| w[0] < w[1]),
            "messages under key {key} keep publish order"
        );
    }
}

/// S7. Consume 5 000, stop, publish 5 000 more, restart: the second run sees the new messages and at
/// most one flush interval's worth of the old ones.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s7_a_restarted_consumer_resumes_after_its_stored_position_rather_than_replaying() {
    let redis = require_redis!();
    const FIRST: u32 = 5_000;
    const SECOND: u32 = 5_000;
    const TOTAL: u32 = FIRST + SECOND;
    const PARTITIONS: u32 = 2;
    const BATCH: u32 = 100;
    const KEYS: u32 = 8;
    let topic = redis.new_topic("s7");
    let consumer = redis.new_consumer("s7");
    let publisher = publisher(redis, &topic, volume(PARTITIONS)).await;
    let configure = |o: &mut ConsumerOptions| {
        o.batch_size = BATCH;
        o.block_ms = 200;
        o.persist = PersistMode::AsyncBatch;
        o.persist_interval_ms = 100;
        o.start_from = StartFrom::Stored;
        o.start_from_when_missing = StartFrom::Beginning;
    };
    publish_spread(&publisher, 0, FIRST, KEYS).await;

    let run_one = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &consumer,
        configure,
        run_one.clone(),
    )
    .await;
    wait_for_count(
        &run_one,
        FIRST as usize,
        "the first run to deliver everything",
    )
    .await;
    host.shutdown().await.expect("shutdown run one");

    publish_spread(&publisher, FIRST, SECOND, KEYS).await;
    let run_two = Arc::new(RecordingHandler::new());
    let restarted = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &consumer,
        configure,
        run_two.clone(),
    )
    .await;
    wait_for_max_index(
        &run_two,
        SECOND as usize,
        TOTAL - 1,
        "the second run to reach the new tail",
    )
    .await;
    restarted.shutdown().await.expect("shutdown run two");

    let replayed = run_two.count() as i64 - i64::from(SECOND);
    let tolerance = i64::from(BATCH * PARTITIONS);
    assert!(
        replayed >= 0,
        "the restart must deliver at least the new messages"
    );
    assert!(
        replayed <= tolerance,
        "a restart may redeliver at most the unflushed positions, not the stream"
    );
    let (_, duplicated) = audit(&run_two.messages(), TOTAL);
    assert_eq!(duplicated, 0, "within one run nothing is delivered twice");

    let mut seen = vec![false; TOTAL as usize];
    for msg in run_one.messages().iter().chain(run_two.messages().iter()) {
        seen[index_of(msg) as usize] = true;
    }
    assert!(
        seen.iter().all(|s| *s),
        "every published message must be delivered to one of the two runs"
    );
    let mut after_stop = vec![false; SECOND as usize];
    for msg in run_two.messages() {
        let i = index_of(&msg);
        if i >= FIRST {
            after_stop[(i - FIRST) as usize] = true;
        }
    }
    assert!(
        after_stop.iter().all(|s| *s),
        "every message published while stopped is delivered after the restart"
    );
}

/// S8. `Persist = None` with `StartFrom = Now` skips the backlog and everything published while the
/// consumer was down, and never writes a position hash.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s8_persist_none_with_start_from_now_skips_the_backlog_and_survives_a_restart() {
    let redis = require_redis!();
    const BACKLOG: u32 = 1_000;
    const LIVE: u32 = 500;
    const GAP: u32 = 500;
    const AFTER: u32 = 300;
    const PARTITIONS: u32 = 2;
    const KEYS: u32 = 8;
    let topic = redis.new_topic("s8");
    let consumer = redis.new_consumer("s8");
    let publisher = publisher(redis, &topic, volume(PARTITIONS)).await;
    let configure = |o: &mut ConsumerOptions| {
        o.batch_size = 100;
        o.block_ms = 200;
        o.persist = PersistMode::None;
        o.start_from = StartFrom::Now;
    };
    publish_spread(&publisher, 0, BACKLOG, KEYS).await;

    let run_one = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &consumer,
        configure,
        run_one.clone(),
    )
    .await;
    next_millisecond().await;
    publish_spread(&publisher, BACKLOG, LIVE, KEYS).await;
    wait_for_max_index(
        &run_one,
        LIVE as usize,
        BACKLOG + LIVE - 1,
        "the live messages",
    )
    .await;
    host.shutdown().await.expect("shutdown run one");

    assert_eq!(
        run_one.count(),
        LIVE as usize,
        "StartFrom.Now must skip the backlog entirely"
    );
    assert!(
        run_one.messages().iter().all(|m| index_of(m) >= BACKLOG),
        "no backlog entry may be delivered"
    );

    publish_spread(&publisher, BACKLOG + LIVE, GAP, KEYS).await;
    let mut conn = raw(redis).await;
    let positions: usize = redis::cmd("HLEN")
        .arg(redis.keys().positions(&topic, &consumer))
        .query_async(&mut conn)
        .await
        .expect("HLEN");
    assert_eq!(
        positions, 0,
        "Persist = None must never write a position hash"
    );

    let run_two = Arc::new(RecordingHandler::new());
    let restarted = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &consumer,
        configure,
        run_two.clone(),
    )
    .await;
    next_millisecond().await;
    publish_spread(&publisher, BACKLOG + LIVE + GAP, AFTER, KEYS).await;
    wait_for_max_index(
        &run_two,
        AFTER as usize,
        BACKLOG + LIVE + GAP + AFTER - 1,
        "the post-restart messages",
    )
    .await;
    restarted.shutdown().await.expect("shutdown run two");

    assert_eq!(
        run_two.count(),
        AFTER as usize,
        "a restart with Persist = None and Now re-reads neither backlog nor gap"
    );
    assert!(
        run_two
            .messages()
            .iter()
            .all(|m| index_of(m) >= BACKLOG + LIVE + GAP),
        "only messages published after the restart may be delivered"
    );
}

/// S11. A slow handler bounds in-flight messages and leaves the backlog in Redis.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s11_a_slow_handler_bounds_in_flight_messages_and_leaves_the_backlog_in_redis() {
    let redis = require_redis!();
    const TOTAL: u32 = 200_000;
    const PARTITIONS: u32 = 2;
    const BATCH: u32 = 100;
    const CAPACITY: u32 = 4;
    const KEYS: u32 = 16;
    let topic = redis.new_topic("s11");
    let consumer = redis.new_consumer("s11");
    let publisher = publisher(redis, &topic, volume(PARTITIONS)).await;

    let recorder = Arc::new(RecordingHandler::new().with_delay(Duration::from_millis(50)));
    let host = start_host(
        redis,
        &topic,
        volume(PARTITIONS),
        &consumer,
        |o| {
            o.batch_size = BATCH;
            o.block_ms = 200;
            o.persist = PersistMode::AsyncBatch;
            o.persist_interval_ms = 250;
            o.start_from = StartFrom::Beginning;
            o.backpressure = BackpressureOptions {
                enabled: true,
                capacity: CAPACITY,
            };
        },
        recorder.clone(),
    )
    .await;
    publish_spread(&publisher, 0, BATCH * PARTITIONS * 2, KEYS).await;
    wait_until(
        || recorder.count() > 0,
        Some(Duration::from_secs(30)),
        "the consumer to start delivering",
    )
    .await;
    publish_spread(
        &publisher,
        BATCH * PARTITIONS * 2,
        TOTAL - BATCH * PARTITIONS * 2,
        KEYS,
    )
    .await;
    let consumed_when_publish_finished = recorder.count() as u64;
    host.shutdown().await.expect("shutdown");

    assert!(
        recorder.peak_in_flight() <= (CAPACITY * BATCH) as usize,
        "the reader awaits the bounded channel, so the handler never holds more than Capacity x BatchSize"
    );
    let outstanding = u64::from(TOTAL) - consumed_when_publish_finished;
    assert!(
        outstanding > u64::from(CAPACITY * BATCH * PARTITIONS * 10),
        "a 50 ms-per-batch handler cannot keep up with the publisher, so the backlog must be large"
    );
    let mut stored = 0_i64;
    for p in 0..PARTITIONS {
        stored += xlen(redis, &topic, p).await;
    }
    assert_eq!(
        stored,
        i64::from(TOTAL),
        "trimming is off, so every published entry is still on a stream"
    );
}

/// Waits for the wall clock to leave the current millisecond, so a `StartFrom::Now` consumer's cut-off
/// is strictly before the messages published after it.
async fn next_millisecond() {
    let start = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    wait_until(
        || time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000 > start,
        Some(Duration::from_secs(5)),
        "the clock to leave the current millisecond",
    )
    .await;
}
