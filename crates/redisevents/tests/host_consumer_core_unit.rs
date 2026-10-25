//! Host state and startup refusal, through the host against real Redis. Port of the C2t deferrals:
//! `ConsumerCoreUnitTests.The_loop_marks_its_partitions_running_and_caught_up` (the partition monitors
//! are host state, so the assertion is made on the host) and the startup refusal of a `Sync*` persist
//! mode without a real flush (`Worker::new` takes a `PositionSink`; the refusal is the host's job).
//! Skips without Redis. Needs the C4 host: ignored until it lands.

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
use redisevents::Error;
use redisevents::admin::health::StreamHealthStatus;
use redisevents::config::{ConsumerOptions, PersistMode, StartFrom, TopicOptions};
use redisevents::consumer::Streams;
use redisevents::positions::NullPositionStore;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until_async};

/// The loop over two partitions marks both as running and, once it has read to the tail, caught up:
/// the host reports both owned, running, and not behind.
#[tokio::test]
async fn the_loop_marks_its_partitions_running_and_caught_up() {
    let redis = require_redis!();
    let topic = redis.new_topic("loop-running");
    let consumer = redis.new_consumer("loop-running");
    let topic_options = TopicOptions {
        partitions: 2,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        topic_options,
        &consumer,
        |o: &mut ConsumerOptions| {
            o.batch_size = 1;
            o.start_from = StartFrom::Beginning;
            o.persist = PersistMode::AsyncBatch;
        },
        handler,
    )
    .await;
    assert!(host.is_running());
    assert_eq!(host.consumers(), 1);
    assert_eq!(host.consumers_started(), 1);
    wait_until_async(
        || async { host.owned_partitions(&topic, &consumer) == vec![0, 1] },
        Some(Duration::from_secs(30)),
        "the host to own both partitions",
    )
    .await;
    wait_until_async(
        || async { host.health().await.status == StreamHealthStatus::Healthy },
        Some(Duration::from_secs(30)),
        "both partitions to be read and caught up",
    )
    .await;
    host.shutdown().await.expect("shutdown");
}

/// A `Sync*` persist mode needs a real flush: a host given a position store that discards writes is
/// refused at startup, not allowed to run and lose positions.
#[tokio::test]
async fn sync_persist_mode_without_a_real_flush_is_refused_at_startup() {
    let redis = require_redis!();
    let topic = redis.new_topic("sync-refused");
    let consumer = redis.new_consumer("sync-refused");
    let result = Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .topic_options(
            &topic,
            TopicOptions {
                partitions: 1,
                ..Default::default()
            },
        )
        .topic(&topic)
        .consumer(&consumer)
        .configure(|o: &mut ConsumerOptions| o.persist = PersistMode::SyncBatch)
        .position_store(NullPositionStore)
        .handler(Arc::new(RecordingHandler::new()))
        .spawn()
        .await;
    assert!(
        matches!(result, Err(Error::Config(_))),
        "a Sync persist mode without a flush must be refused"
    );
}
