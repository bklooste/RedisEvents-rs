//! Error policies through the consumer host against real Redis. Port of the C# `ErrorServiceTests`
//! (S12, S12b, S13, S14, S18 x2, S20), named after the C# test in `snake_case`.
//!
//! The C# log assertions (`CapturingLogger`) have no counterpart in the public API, so the log
//! checks are dropped and the health report, positions and delivered messages are asserted instead.
//! The Rust worker uses the built-in [`BLOCK_BACKOFF`] ladder (no test override), so the block test
//! waits through real back-off steps.
//!
//! S18 (both, `Delivery = WorkQueue`) is in `host_work_queue.rs`. Not ported here, see the PR: S20
//! needs to pause the container, which the fixture does not expose. Skips without Redis. Needs the C4
//! host: ignored until it lands.

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

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use host_common::*;
use redisevents::Publisher;
use redisevents::admin::health::StreamHealthStatus;
use redisevents::config::{ConsumerOptions, ErrorPolicy, PersistMode};
use redisevents::consumer::{HandleError, PartitionContext};
use redisevents::producer::PublishOptions;
use redisevents::wire::StreamMsg;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until, wait_until_async};

/// Health is process-wide (every host's partition monitors share one registry), so a blocked or
/// stopped partition from a sibling test would show in another test's report: run these one at a
/// time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The bodies of a batch, as text.
fn bodies_of(batch: &[StreamMsg]) -> Vec<String> {
    batch
        .iter()
        .map(|m| String::from_utf8_lossy(m.body()).into_owned())
        .collect()
}

/// S12. A poison message is advanced past and never retried: the handler owns retry, not the library.
#[tokio::test]
async fn s12_a_poison_message_is_logged_once_advanced_past_and_never_retried() {
    let _serial = SERIAL.lock().await;
    let redis = require_redis!();
    let topic = redis.new_topic("s12");
    let consumer = redis.new_consumer("s12");
    let topic_options = redisevents::config::TopicOptions {
        partitions: 1,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new().with_hook(
        |batch: &[StreamMsg], _: &PartitionContext| {
            if bodies_of(batch).iter().any(|b| b == "poison") {
                return Err(HandleError::skip(io::Error::other(
                    "this message can never be deserialised",
                )));
            }
            Ok(())
        },
    ));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    let mut last = None;
    for body in ["one", "two", "poison", "four", "five"] {
        last = Some(
            publisher
                .publish(
                    "k",
                    text(body),
                    MESSAGE_TYPE,
                    &PublishOptions::new().partition(0),
                )
                .await
                .expect("publish"),
        );
    }
    let host = start_host(
        redis,
        &topic,
        topic_options,
        &consumer,
        |o: &mut ConsumerOptions| {
            o.batch_size = 1;
            o.persist = PersistMode::SyncBatch;
            o.on_error = ErrorPolicy::BestEffort;
        },
        handler.clone(),
    )
    .await;
    wait_for_count(&handler, 5, "every message, poison included").await;
    host.shutdown().await.expect("shutdown");

    assert_eq!(handler.bodies(), ["one", "two", "poison", "four", "five"]);
    assert_eq!(
        handler.bodies().iter().filter(|b| *b == "poison").count(),
        1,
        "the library does not retry an ordinary error"
    );
    let stored = positions(redis, &topic, &consumer).await;
    assert_eq!(
        stored.get(&0),
        last.as_ref(),
        "the position advanced past every message, poison included"
    );
}

/// S12b. A `Block` error blocks its partition, the sibling keeps going, health degrades and then fails
/// readiness, and the partition recovers and resumes once the downstream is back.
#[tokio::test]
async fn s12b_a_custom_blocking_error_blocks_its_partition_and_recovers() {
    let _serial = SERIAL.lock().await;
    let redis = require_redis!();
    let topic = redis.new_topic("s12b");
    let consumer = redis.new_consumer("s12b");
    let topic_options = redisevents::config::TopicOptions {
        partitions: 2,
        ..Default::default()
    };
    let downstream_down = Arc::new(AtomicBool::new(true));
    let down = downstream_down.clone();
    let handler = Arc::new(RecordingHandler::new().with_hook(
        move |batch: &[StreamMsg], _: &PartitionContext| {
            if down.load(Ordering::Acquire) && bodies_of(batch).iter().any(|b| b.starts_with("p0"))
            {
                return Err(HandleError::block(io::Error::other(
                    "the mandatory downstream is unreachable",
                )));
            }
            Ok(())
        },
    ));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    let blocked = publisher
        .publish(
            "k",
            text("p0-a"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(0),
        )
        .await
        .expect("publish");
    publisher
        .publish(
            "k",
            text("p1-a"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(1),
        )
        .await
        .expect("publish");
    publisher
        .publish(
            "k",
            text("p1-b"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(1),
        )
        .await
        .expect("publish");

    let host = start_host(
        redis,
        &topic,
        topic_options,
        &consumer,
        |o: &mut ConsumerOptions| {
            o.batch_size = 1;
            o.persist = PersistMode::SyncBatch;
            o.on_error = ErrorPolicy::BestEffort;
            o.unhealthy_block_seconds = 3;
        },
        handler.clone(),
    )
    .await;
    wait_until(
        || {
            let b = handler.bodies();
            b.contains(&"p1-a".to_owned()) && b.contains(&"p1-b".to_owned())
        },
        Some(Duration::from_secs(30)),
        "the other partition to keep consuming while partition 0 is blocked",
    )
    .await;
    wait_until(
        || handler.bodies().iter().filter(|b| *b == "p0-a").count() >= 2,
        Some(Duration::from_secs(30)),
        "the blocked batch to be retried",
    )
    .await;
    let degraded = host.health().await;
    assert_eq!(degraded.status, StreamHealthStatus::Degraded);
    assert_eq!(health_count(&degraded, "partitions_blocked"), Some(1));
    assert!(
        degraded
            .description
            .contains("blocked retrying a blocking error"),
        "{}",
        degraded.description
    );
    assert!(
        !positions(redis, &topic, &consumer).await.contains_key(&0),
        "a blocked partition never advances"
    );
    assert!(
        positions(redis, &topic, &consumer).await.contains_key(&1),
        "the sibling checkpoints normally"
    );

    wait_until_async(
        || async { host.health().await.status == StreamHealthStatus::Unhealthy },
        Some(Duration::from_secs(30)),
        "health to fail readiness once the block passes UnhealthyBlockSeconds",
    )
    .await;
    downstream_down.store(false, Ordering::Release);
    wait_until_async(
        || async { positions(redis, &topic, &consumer).await.contains_key(&0) },
        Some(Duration::from_secs(30)),
        "the recovered batch to advance the position it was holding",
    )
    .await;
    assert_eq!(
        positions(redis, &topic, &consumer).await.get(&0),
        Some(&blocked),
        "the blocked batch was processed"
    );

    let next = publisher
        .publish(
            "k",
            text("p0-b"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(0),
        )
        .await
        .expect("publish");
    wait_until(
        || handler.bodies().contains(&"p0-b".to_owned()),
        Some(Duration::from_secs(30)),
        "the partition to keep consuming after recovery",
    )
    .await;
    wait_until_async(
        || async { positions(redis, &topic, &consumer).await.get(&0) == Some(&next) },
        Some(Duration::from_secs(30)),
        "the position to follow the recovered partition forward",
    )
    .await;
    let healthy = host.health().await;
    assert_eq!(health_count(&healthy, "partitions_blocked"), Some(0));
    assert_eq!(health_count(&healthy, "partitions_stopped"), Some(0));
    assert_ne!(healthy.status, StreamHealthStatus::Unhealthy);
    host.shutdown().await.expect("shutdown");
}

/// S13. `StopPartition` stands one partition down and leaves the others running.
#[tokio::test]
async fn s13_stop_partition_stands_one_partition_down_and_leaves_the_others_running() {
    let _serial = SERIAL.lock().await;
    let redis = require_redis!();
    let topic = redis.new_topic("s13");
    let consumer = redis.new_consumer("s13");
    let topic_options = redisevents::config::TopicOptions {
        partitions: 4,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new().with_hook(
        |batch: &[StreamMsg], _: &PartitionContext| {
            if bodies_of(batch).iter().any(|b| b.starts_with("p0")) {
                return Err(HandleError::skip(io::Error::other("partition 0 is wedged")));
            }
            Ok(())
        },
    ));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    publisher
        .publish(
            "k",
            text("p0-poison"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(0),
        )
        .await
        .expect("publish");
    publisher
        .publish(
            "k",
            text("p0-after"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(0),
        )
        .await
        .expect("publish");
    for partition in 1..4 {
        for i in 0..3 {
            publisher
                .publish(
                    "k",
                    text(&format!("p{partition}-{i}")),
                    MESSAGE_TYPE,
                    &PublishOptions::new().partition(partition),
                )
                .await
                .expect("publish");
        }
    }
    let host = start_host(
        redis,
        &topic,
        topic_options,
        &consumer,
        |o: &mut ConsumerOptions| {
            o.batch_size = 1;
            o.persist = PersistMode::SyncBatch;
            o.on_error = ErrorPolicy::StopPartition;
        },
        handler.clone(),
    )
    .await;
    wait_for_count(&handler, 10, "the nine healthy messages and the poison").await;
    // A stopped partition must not process its already-read successor; give it time to try.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let bodies = handler.bodies();
    assert!(bodies.contains(&"p0-poison".to_owned()));
    assert!(
        !bodies.contains(&"p0-after".to_owned()),
        "a stopped partition processes nothing further"
    );
    for partition in 1..4 {
        for i in 0..3 {
            assert!(bodies.contains(&format!("p{partition}-{i}")));
        }
    }
    let stored = positions(redis, &topic, &consumer).await;
    assert!(
        !stored.contains_key(&0),
        "the stopped partition did not advance past the failing batch"
    );
    assert_eq!(stored.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    let health = host.health().await;
    assert_eq!(health.status, StreamHealthStatus::Degraded);
    assert_eq!(health_count(&health, "partitions_stopped"), Some(1));
    assert_eq!(health_count(&health, "partitions_blocked"), Some(0));
    assert!(
        health.description.contains("stopped"),
        "{}",
        health.description
    );
    host.shutdown()
        .await
        .expect("shutdown: a stood-down partition is not a fault");
}

/// S14. `ErrorPolicy::Fail` faults the host and does not advance past the batch it failed on.
#[tokio::test]
async fn s14_error_policy_fail_faults_the_host_and_does_not_advance() {
    let _serial = SERIAL.lock().await;
    let redis = require_redis!();
    let topic = redis.new_topic("s14");
    let consumer = redis.new_consumer("s14");
    let topic_options = redisevents::config::TopicOptions {
        partitions: 1,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new().with_hook(
        |batch: &[StreamMsg], _: &PartitionContext| {
            if bodies_of(batch).iter().any(|b| b == "boom") {
                return Err(HandleError::skip(io::Error::other(
                    "the whole downstream is gone",
                )));
            }
            Ok(())
        },
    ));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    publisher
        .publish(
            "k",
            text("boom"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(0),
        )
        .await
        .expect("publish");
    publisher
        .publish(
            "k",
            text("never"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(0),
        )
        .await
        .expect("publish");
    let host = start_host(
        redis,
        &topic,
        topic_options,
        &consumer,
        |o: &mut ConsumerOptions| {
            o.batch_size = 1;
            o.persist = PersistMode::SyncBatch;
            o.on_error = ErrorPolicy::Fail;
        },
        handler.clone(),
    )
    .await;
    let failure = tokio::time::timeout(Duration::from_secs(30), host.closed())
        .await
        .expect("the host to stop within 30 s");
    assert!(
        failure.is_err(),
        "ErrorPolicy.Fail must fail the host, not stop it cleanly"
    );
    assert!(
        !positions(redis, &topic, &consumer).await.contains_key(&0),
        "Fail must not advance past the failed batch"
    );
    assert!(
        !handler.bodies().contains(&"never".to_owned()),
        "nothing after the failure is processed"
    );
    let health = host.health().await;
    assert_eq!(health.status, StreamHealthStatus::Unhealthy);
    assert!(
        health.description.contains("stopped"),
        "{}",
        health.description
    );
    assert!(
        host.shutdown().await.is_err(),
        "shutdown reports the failure that already stopped the host"
    );
}
