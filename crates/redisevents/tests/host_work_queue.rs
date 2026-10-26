//! `delivery = work_queue` through the consumer host against real Redis (C6). S18a is the port of
//! the C# `S18_Consumer_group_mode_splits_the_load_between_competing_consumers`; S18b and the
//! `ConsumerGroupServiceTests` ports run at the fetch seam (`src/consumer/work_queue/tests.rs`),
//! as in C#. The rest pin the ack rules of the error contract in this mode. Skips without Redis.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::items_after_statements,
    clippy::struct_field_names
)]

mod host_common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use host_common::*;
use redisevents::Publisher;
use redisevents::config::{
    ConsumerOptions, DeliveryMode, ErrorPolicy, InstanceMode, InstanceOptions, ReadMode,
    TopicOptions, WorkQueueOptions,
};
use redisevents::consumer::{
    BatchHandler, HandleError, HandleResult, PartitionContext, Streams, StreamsHost,
};
use redisevents::producer::PublishOptions;
use redisevents::wire::StreamMsg;
use redisevents_test_support::{RecordingHandler, RedisFixture, require_redis, wait_until_async};
use tokio::sync::Notify;

/// Static ownership, so every instance reads every partition and the members compete (under the
/// lease default exactly one instance would hold each partition, as in C#).
fn work_queue(o: &mut ConsumerOptions) {
    o.delivery = DeliveryMode::WorkQueue;
    o.instances = Some(InstanceOptions {
        mode: InstanceMode::Static,
        ..InstanceOptions::default()
    });
    o.shutdown_timeout_seconds = 1;
}

async fn start_member<H: BatchHandler>(
    redis: &RedisFixture,
    topic: &str,
    topic_options: TopicOptions,
    consumer: &str,
    member: &str,
    claim_min_idle: Duration,
    configure: impl FnOnce(&mut ConsumerOptions),
    handler: H,
) -> StreamsHost {
    Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .allow_untrimmed(true)
        .topic_options(topic, topic_options)
        .topic(topic)
        .consumer(consumer)
        .configure(|o| {
            work_queue(o);
            configure(o);
        })
        .instance_id(uuid::Uuid::new_v4())
        .work_queue(
            WorkQueueOptions::new()
                .member(member)
                .claim_min_idle(claim_min_idle),
        )
        .handler(handler)
        .spawn()
        .await
        .expect("spawn the work-queue host")
}

/// `XPENDING <key> <group>` total (0 when the group does not exist yet).
async fn pending_count(redis: &RedisFixture, topic: &str, group: &str, partition: u32) -> u64 {
    let mut conn = raw(redis).await;
    let summary: redis::RedisResult<redis::Value> = redis::cmd("XPENDING")
        .arg(redis.keys().stream(topic, partition, true))
        .arg(group)
        .query_async(&mut conn)
        .await;
    match summary {
        Ok(redis::Value::Array(items)) => match items.first() {
            Some(redis::Value::Int(n)) => *n as u64,
            _ => 0,
        },
        _ => 0,
    }
}

/// `XINFO CONSUMERS` names of the group on one partition.
async fn members(
    redis: &RedisFixture,
    topic: &str,
    group: &str,
    partition: u32,
) -> BTreeSet<String> {
    let mut conn = raw(redis).await;
    let rows: Vec<redis::Value> = redis::cmd("XINFO")
        .arg("CONSUMERS")
        .arg(redis.keys().stream(topic, partition, true))
        .arg(group)
        .query_async(&mut conn)
        .await
        .expect("XINFO CONSUMERS");
    rows.into_iter()
        .filter_map(|row| {
            let fields: Vec<(String, redis::Value)> = match row {
                redis::Value::Map(m) => m
                    .into_iter()
                    .filter_map(|(k, v)| redis::from_redis_value::<String>(k).ok().map(|k| (k, v)))
                    .collect(),
                redis::Value::Array(a) => a
                    .chunks(2)
                    .filter_map(|c| {
                        redis::from_redis_value::<String>(c[0].clone())
                            .ok()
                            .map(|k| (k, c[1].clone()))
                    })
                    .collect(),
                _ => return None,
            };
            fields
                .into_iter()
                .find(|(k, _)| k == "name")
                .and_then(|(_, v)| redis::from_redis_value::<String>(v).ok())
        })
        .collect()
}

/// The group names on one partition (`XINFO GROUPS`).
async fn groups(redis: &RedisFixture, topic: &str, partition: u32) -> Vec<String> {
    let mut conn = raw(redis).await;
    let info: redis::streams::StreamInfoGroupsReply = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(redis.keys().stream(topic, partition, true))
        .query_async(&mut conn)
        .await
        .expect("XINFO GROUPS");
    info.groups.into_iter().map(|g| g.name).collect()
}

/// A handler whose first call parks until `gate` is notified (C# S18a's gate).
struct Gated {
    inner: Arc<RecordingHandler>,
    gate: Arc<Notify>,
    gated: AtomicBool,
}

impl BatchHandler for Gated {
    async fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        if !self.gated.swap(true, Ordering::SeqCst) {
            let _ = tokio::time::timeout(Duration::from_secs(60), self.gate.notified()).await;
        }
        self.inner.handle_batch(batch, ctx).await
    }
}

/// A handler that never returns (a pod wedged or dying mid-batch).
struct Wedged {
    seen: Arc<AtomicUsize>,
}

impl BatchHandler for Wedged {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        self.seen.fetch_add(batch.len(), Ordering::SeqCst);
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// S18a: two instances of one consumer split the load, and between them see every message once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s18_consumer_group_mode_splits_the_load_between_competing_consumers() {
    let redis = require_redis!();
    let topic = redis.new_topic("s18a");
    let consumer = redis.new_consumer("s18a");
    let topic_options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    const TOTAL: u32 = 30;
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    for i in 0..TOTAL {
        publisher
            .publish(
                "k",
                text(&format!("m{i}")),
                MESSAGE_TYPE,
                &PublishOptions::new().partition(0),
            )
            .await
            .unwrap();
    }
    let gate = Arc::new(Notify::new());
    let a = Arc::new(RecordingHandler::new());
    let b = Arc::new(RecordingHandler::new());
    let configure = |o: &mut ConsumerOptions| {
        o.batch_size = 1;
        o.backpressure.capacity = 1;
    };
    let host_a = start_member(
        redis,
        &topic,
        topic_options.clone(),
        &consumer,
        "s18a",
        Duration::from_secs(30),
        configure,
        Gated {
            inner: a.clone(),
            gate: gate.clone(),
            gated: AtomicBool::new(false),
        },
    )
    .await;
    // A is now parked inside its first batch, so B is guaranteed to take work off it.
    wait_until_async(
        || async { pending_count(redis, &topic, &consumer, 0).await >= 1 },
        Some(Duration::from_secs(30)),
        "A to take its first entry",
    )
    .await;
    let host_b = start_member(
        redis,
        &topic,
        topic_options,
        &consumer,
        "s18b",
        Duration::from_secs(30),
        configure,
        b.clone(),
    )
    .await;
    wait_for_count(&b, 5, "B takes work while A is stalled").await;
    gate.notify_one();
    wait_until_async(
        || async { a.count() + b.count() >= TOTAL as usize },
        Some(Duration::from_secs(60)),
        "both members between them to consume every message",
    )
    .await;
    assert!(
        a.count() > 0 && b.count() > 0,
        "a: {}, b: {}",
        a.count(),
        b.count()
    );
    let mut seen = [a.bodies(), b.bodies()].concat();
    let total = seen.len();
    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        total,
        "a consumer group delivers each entry to one member only"
    );
    let mut want: Vec<String> = (0..TOTAL).map(|i| format!("m{i}")).collect();
    want.sort();
    assert_eq!(seen, want);
    wait_until_async(
        || async { pending_count(redis, &topic, &consumer, 0).await == 0 },
        Some(Duration::from_secs(30)),
        "the pending-entries list to drain as batches are acknowledged",
    )
    .await;
    // The wire contract: group = consumer name; members = the instances' names.
    assert_eq!(
        groups(redis, &topic, 0).await,
        std::slice::from_ref(&consumer)
    );
    assert_eq!(
        members(redis, &topic, &consumer, 0).await,
        BTreeSet::from(["s18a".to_owned(), "s18b".to_owned()])
    );
    host_a.shutdown().await.unwrap();
    host_b.shutdown().await.unwrap();
}

/// Competing consumers over several co-located partitions, blocking reads: nothing lost, nothing
/// processed twice, every entry acknowledged, and no position hash written.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_consumers_split_the_load_with_no_message_lost_or_processed_twice() {
    let redis = require_redis!();
    let topic = redis.new_topic("wq-split");
    let consumer = redis.new_consumer("wq-split");
    const TOTAL: u32 = 2_000;
    let options = volume(4);
    let configure = |o: &mut ConsumerOptions| {
        o.batch_size = 10;
        o.read_mode = ReadMode::Block;
        o.block_ms = 200;
    };
    let a = Arc::new(RecordingHandler::new().with_delay(Duration::from_millis(1)));
    let b = Arc::new(RecordingHandler::new().with_delay(Duration::from_millis(1)));
    let wait = Duration::from_secs(30);
    let host_a = start_member(
        redis,
        &topic,
        options.clone(),
        &consumer,
        "pod-a",
        wait,
        configure,
        a.clone(),
    )
    .await;
    let host_b = start_member(
        redis,
        &topic,
        options.clone(),
        &consumer,
        "pod-b",
        wait,
        configure,
        b.clone(),
    )
    .await;
    let publisher = publisher(redis, &topic, options).await;
    publish_spread(&publisher, 0, TOTAL, 16).await;
    wait_until_async(
        || async { a.count() + b.count() >= TOTAL as usize },
        Some(Duration::from_secs(60)),
        "every message",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let all = [a.messages(), b.messages()].concat();
    assert_eq!(audit(&all, TOTAL), (0, 0), "no message lost or duplicated");
    assert_eq!(all.len(), TOTAL as usize);
    assert!(
        a.count() > 0 && b.count() > 0,
        "a: {}, b: {}",
        a.count(),
        b.count()
    );
    for p in 0..4 {
        wait_until_async(
            || async { pending_count(redis, &topic, &consumer, p).await == 0 },
            Some(Duration::from_secs(10)),
            "acks",
        )
        .await;
        assert_eq!(
            groups(redis, &topic, p).await,
            std::slice::from_ref(&consumer)
        );
    }
    assert!(
        positions(redis, &topic, &consumer).await.is_empty(),
        "no position hash in this mode"
    );
    host_a.shutdown().await.unwrap();
    host_b.shutdown().await.unwrap();
}

/// A member that dies with work pending: a survivor claims its entries once they are idle past the
/// threshold, and processes each exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_members_pending_entries_are_claimed_by_a_survivor_after_the_idle_threshold() {
    let redis = require_redis!();
    let topic = redis.new_topic("wq-claim");
    let consumer = redis.new_consumer("wq-claim");
    let options = TopicOptions {
        partitions: 2,
        ..TopicOptions::default()
    };
    const TOTAL: u32 = 20;
    let publisher = publisher(redis, &topic, options.clone()).await;
    publish_spread(&publisher, 0, TOTAL, 8).await;
    let seen = Arc::new(AtomicUsize::new(0));
    let threshold = Duration::from_secs(1);
    let configure = |o: &mut ConsumerOptions| o.batch_size = 100;
    let dying = start_member(
        redis,
        &topic,
        options.clone(),
        &consumer,
        "doomed",
        threshold,
        configure,
        Wedged { seen: seen.clone() },
    )
    .await;
    wait_until_async(
        || async {
            pending_count(redis, &topic, &consumer, 0).await
                + pending_count(redis, &topic, &consumer, 1).await
                == u64::from(TOTAL)
        },
        Some(Duration::from_secs(30)),
        "the doomed member to hold everything",
    )
    .await;
    // "Dies": its handler never returns, so the shutdown budget runs out and nothing is acked.
    dying.shutdown().await.unwrap();
    assert!(seen.load(Ordering::SeqCst) > 0);

    let survivor = Arc::new(RecordingHandler::new());
    let host = start_member(
        redis,
        &topic,
        options,
        &consumer,
        "survivor",
        threshold,
        configure,
        survivor.clone(),
    )
    .await;
    wait_for_count(
        &survivor,
        TOTAL as usize,
        "the survivor claims the dead member's entries",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(audit(&survivor.messages(), TOTAL), (0, 0));
    for p in 0..2 {
        wait_until_async(
            || async { pending_count(redis, &topic, &consumer, p).await == 0 },
            Some(Duration::from_secs(10)),
            "the survivor's acks",
        )
        .await;
    }
    host.shutdown().await.unwrap();
}

/// A live member's in-flight entries are not stolen before the threshold: a slow handler keeps
/// its work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_member_is_not_stolen_from_within_the_threshold() {
    let redis = require_redis!();
    let topic = redis.new_topic("wq-slow");
    let consumer = redis.new_consumer("wq-slow");
    let options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    let publisher = publisher(redis, &topic, options.clone()).await;
    publish_spread(&publisher, 0, 5, 1).await;
    let slow = Arc::new(RecordingHandler::new().with_delay(Duration::from_secs(2)));
    let configure = |o: &mut ConsumerOptions| o.batch_size = 100;
    let host_slow = start_member(
        redis,
        &topic,
        options.clone(),
        &consumer,
        "slow",
        Duration::from_secs(30),
        configure,
        slow.clone(),
    )
    .await;
    wait_until_async(
        || async { pending_count(redis, &topic, &consumer, 0).await == 5 },
        Some(Duration::from_secs(10)),
        "the slow member to take the batch",
    )
    .await;
    let other = Arc::new(RecordingHandler::new());
    let host_other = start_member(
        redis,
        &topic,
        options,
        &consumer,
        "other",
        Duration::from_secs(30),
        configure,
        other.clone(),
    )
    .await;
    wait_for_count(&slow, 5, "the slow member finishes").await;
    wait_until_async(
        || async { pending_count(redis, &topic, &consumer, 0).await == 0 },
        Some(Duration::from_secs(10)),
        "acks",
    )
    .await;
    assert_eq!(other.count(), 0, "nothing stolen from a live member");
    host_slow.shutdown().await.unwrap();
    host_other.shutdown().await.unwrap();
}

/// `Block` keeps the entry pending (never acked while failing) and the same member retries it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_keeps_the_entry_pending_and_retries() {
    let redis = require_redis!();
    let topic = redis.new_topic("wq-block");
    let consumer = redis.new_consumer("wq-block");
    let options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    let publisher = publisher(redis, &topic, options.clone()).await;
    publish_spread(&publisher, 0, 1, 1).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let handler = Arc::new(RecordingHandler::new().with_hook(move |_, _| {
        if c.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(HandleError::block(std::io::Error::other("downstream down")))
        } else {
            Ok(())
        }
    }));
    let host = start_member(
        redis,
        &topic,
        options,
        &consumer,
        "blocker",
        Duration::from_secs(30),
        |_| {},
        handler.clone(),
    )
    .await;
    wait_until_async(
        || async { calls.load(Ordering::SeqCst) >= 1 },
        Some(Duration::from_secs(10)),
        "the first attempt",
    )
    .await;
    assert_eq!(
        pending_count(redis, &topic, &consumer, 0).await,
        1,
        "pending while blocked"
    );
    wait_until_async(
        || async { calls.load(Ordering::SeqCst) >= 2 },
        Some(Duration::from_secs(10)),
        "the retry",
    )
    .await;
    wait_until_async(
        || async { pending_count(redis, &topic, &consumer, 0).await == 0 },
        Some(Duration::from_secs(10)),
        "the ack after the retry",
    )
    .await;
    assert_eq!(
        handler.count(),
        2,
        "one entry, two attempts by the same member"
    );
    host.shutdown().await.unwrap();
}

/// The ack rules of the error contract: a `BestEffort` skip and a filtered-out entry are acked;
/// `StopPartition` leaves the entry pending for redelivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skips_and_filtered_entries_are_acked_stop_partition_is_not() {
    let redis = require_redis!();
    let options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    let skip = |_: &[StreamMsg], _: &PartitionContext| -> HandleResult {
        Err(HandleError::skip(std::io::Error::other("bad message")))
    };
    let mut outcome = BTreeMap::new();
    for (name, policy, filter) in [
        ("best-effort", ErrorPolicy::BestEffort, None),
        ("stop", ErrorPolicy::StopPartition, None),
        (
            "filtered",
            ErrorPolicy::StopPartition,
            Some(vec!["other.type".to_owned()]),
        ),
    ] {
        let topic = redis.new_topic(name);
        let consumer = redis.new_consumer(name);
        let publisher = publisher(redis, &topic, options.clone()).await;
        publish_spread(&publisher, 0, 3, 1).await;
        let handler = Arc::new(RecordingHandler::new().with_hook(skip));
        let host = start_member(
            redis,
            &topic,
            options.clone(),
            &consumer,
            "m",
            Duration::from_secs(30),
            |o| {
                o.on_error = policy;
                o.filter.clone_from(&filter);
            },
            handler.clone(),
        )
        .await;
        // Wait for the group to have delivered the batch, then for the outcome to settle.
        wait_until_async(
            || async { handler.count() >= 3 || filter.is_some() },
            Some(Duration::from_secs(10)),
            "delivery",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(700)).await;
        outcome.insert(name, pending_count(redis, &topic, &consumer, 0).await);
        host.shutdown().await.unwrap();
    }
    assert_eq!(
        outcome["best-effort"], 0,
        "a BestEffort skip advances, so it acks"
    );
    assert_eq!(
        outcome["filtered"], 0,
        "filtered entries are acked with their batch"
    );
    assert_eq!(
        outcome["stop"], 3,
        "StopPartition does not advance, so it leaves them pending"
    );
}

/// `start_from = now` creates the group at `$`; a group that already exists keeps its cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_group_is_created_once_at_the_start_position() {
    let redis = require_redis!();
    let topic = redis.new_topic("wq-now");
    let consumer = redis.new_consumer("wq-now");
    let options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    let publisher = publisher(redis, &topic, options.clone()).await;
    publish_spread(&publisher, 0, 3, 1).await;
    let h = Arc::new(RecordingHandler::new());
    let host = start_member(
        redis,
        &topic,
        options.clone(),
        &consumer,
        "m",
        Duration::from_secs(30),
        |o| {
            o.start_from = redisevents::config::StartFrom::Now;
        },
        h.clone(),
    )
    .await;
    publish_spread(&publisher, 3, 2, 1).await;
    wait_for_count(&h, 2, "the entries after $").await;
    host.shutdown().await.unwrap();
    assert_eq!(
        audit(&h.messages(), 5),
        (3, 0),
        "only the two published after the group"
    );
    // Restart with start_from = beginning: BUSYGROUP, the cursor stays; nothing is replayed.
    let h2 = Arc::new(RecordingHandler::new());
    let host = start_member(
        redis,
        &topic,
        options,
        &consumer,
        "m",
        Duration::from_secs(30),
        |o| {
            o.start_from = redisevents::config::StartFrom::Beginning;
        },
        h2.clone(),
    )
    .await;
    publish_spread(&publisher, 5, 1, 1).await;
    wait_for_count(&h2, 1, "the new entry").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(h2.count(), 1, "an existing group's cursor is authoritative");
    host.shutdown().await.unwrap();
}
