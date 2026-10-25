//! The consumer core through the host against real Redis. Port of the C# `ConsumerCoreTests` (R-03,
//! R-08 and the stop paths), named after the C# test in `snake_case`.
//!
//! Not ported, see the PR: `Lag_gauges_are_populated_through_the_host` (the lag gauges are OpenTelemetry
//! metrics, observable only with the `otel` feature and an exporter, and the default build has none),
//! and `Overlapping_stops_and_dispose_do_not_throw` (`StreamsHost::shutdown` takes `self`, so an
//! overlapping second stop cannot be written; the type system already rules it out).
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

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use host_common::*;
use redisevents::Publisher;
use redisevents::config::{
    BackpressureOptions, ConsumerOptions, ErrorPolicy, PersistMode, TopicOptions,
};
use redisevents::consumer::{HandleError, HandleResult, PartitionContext};
use redisevents::producer::PublishOptions;
use redisevents::wire::StreamMsg;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until};

/// A hook that fails every batch containing a body that starts with `prefix`, as a skip.
fn fail_on(
    prefix: &'static str,
) -> impl Fn(&[StreamMsg], &PartitionContext) -> HandleResult + Send + Sync + 'static {
    move |batch: &[StreamMsg], _: &PartitionContext| {
        if batch
            .iter()
            .any(|m| m.body().starts_with(prefix.as_bytes()))
        {
            return Err(HandleError::skip(io::Error::other(
                "this partition is done",
            )));
        }
        Ok(())
    }
}

/// S14b. `ErrorPolicy::Fail` on a multi-partition consumer stops the host, even when the fault comes
/// from a partition other than the first.
#[tokio::test]
async fn s14b_error_policy_fail_on_a_multi_partition_consumer_stops_the_application() {
    let redis = require_redis!();
    let topic = redis.new_topic("s14b");
    let consumer = redis.new_consumer("s14b");
    let topic_options = TopicOptions {
        partitions: 4,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new().with_hook(fail_on("boom")));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    publisher
        .publish(
            "k",
            text("boom"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(2),
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
        handler,
    )
    .await;
    let outcome = tokio::time::timeout(Duration::from_secs(30), host.closed())
        .await
        .expect("the host to stop within 30 s");
    assert!(
        outcome.is_err(),
        "ErrorPolicy.Fail must stop the application from any partition"
    );
}

/// S14c. The same, with `CoLocatePartitions = false`, where the fault arrives from a per-partition
/// worker rather than the shared group.
#[tokio::test]
async fn s14c_error_policy_fail_stops_the_application_on_the_non_colocated_path() {
    let redis = require_redis!();
    let topic = redis.new_topic("s14c");
    let consumer = redis.new_consumer("s14c");
    let topic_options = TopicOptions {
        partitions: 4,
        co_locate_partitions: false,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new().with_hook(fail_on("boom")));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    publisher
        .publish(
            "k",
            text("boom"),
            MESSAGE_TYPE,
            &PublishOptions::new().partition(3),
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
        handler,
    )
    .await;
    let outcome = tokio::time::timeout(Duration::from_secs(30), host.closed())
        .await
        .expect("the host to stop within 30 s");
    assert!(
        outcome.is_err(),
        "ErrorPolicy.Fail must stop the application from a per-partition worker"
    );
}

/// A policy other than `Fail` never stops the application: `StopPartition` stands one partition down
/// and the host keeps serving.
#[tokio::test]
async fn stop_partition_does_not_stop_the_application() {
    let redis = require_redis!();
    let topic = redis.new_topic("stop-partition");
    let consumer = redis.new_consumer("stop-partition");
    let topic_options = TopicOptions {
        partitions: 2,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new().with_hook(fail_on("one")));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    publisher
        .publish(
            "k",
            text("one"),
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
            o.on_error = ErrorPolicy::StopPartition;
        },
        handler.clone(),
    )
    .await;
    wait_for_count(&handler, 1, "the failing batch").await;
    // A moment for a stop that should not come.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        host.is_running(),
        "StopPartition stands one partition down and leaves the host serving"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), host.closed())
            .await
            .is_err(),
        "the host must not report a stop"
    );
    host.shutdown().await.expect("shutdown");
}

/// On the non-co-located path a stood-down partition ends its readers, so shutdown does not wait out
/// `ShutdownTimeoutSeconds` for it.
#[tokio::test]
async fn stop_partition_on_the_non_colocated_path_stops_the_reader_and_drains_fast() {
    let redis = require_redis!();
    let topic = redis.new_topic("stop-fast");
    let consumer = redis.new_consumer("stop-fast");
    let topic_options = TopicOptions {
        partitions: 2,
        co_locate_partitions: false,
        ..Default::default()
    };
    let handler = Arc::new(RecordingHandler::new().with_hook(fail_on("m")));
    let publisher = publisher(redis, &topic, topic_options.clone()).await;
    for i in 0..40 {
        publisher
            .publish(
                "k",
                text(&format!("m{i}")),
                MESSAGE_TYPE,
                &PublishOptions::new().partition(0),
            )
            .await
            .expect("publish");
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
            o.shutdown_timeout_seconds = 10;
            o.backpressure = BackpressureOptions {
                enabled: true,
                capacity: 1,
            };
        },
        handler.clone(),
    )
    .await;
    wait_until(
        || handler.count() >= 1,
        Some(Duration::from_secs(30)),
        "the stood-down partition to fail its first batch",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let stopping = Instant::now();
    host.shutdown().await.expect("shutdown");
    assert!(
        stopping.elapsed() < Duration::from_secs(8),
        "a stood-down partition must not make every shutdown burn the whole ShutdownTimeoutSeconds"
    );
}
