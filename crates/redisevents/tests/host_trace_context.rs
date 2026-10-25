//! Trace context through the consumer host against real Redis. Port of the C# `TraceContextTests`
//! (S21e), named after the C# test in `snake_case`.
//!
//! What the default (no `otel` feature) build can observe: the per-message `traceparent` and
//! correlation id a handler receives from the entry. Span identity, parents and links are only
//! recorded by the `otel` feature (remote parents and links need it, see
//! `diagnostics/activity.rs`), and this crate has no in-memory span exporter in its dev-dependencies,
//! so the span assertions of the C# tests are not ported. See the PR for the list.
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
use redisevents::Publisher;
use redisevents::config::{BackpressureOptions, ConsumerOptions, PersistMode, StartFrom};
use redisevents::producer::PublishOptions;
use redisevents_test_support::{RecordingHandler, require_redis};

const KEY: &str = "trace-key";
const TOTAL: u32 = 4;

/// A W3C `traceparent` for trace `t` and span `s` (both small integers, hex-encoded).
fn traceparent(t: u32, s: u32) -> String {
    format!("00-{t:032x}-{s:016x}-01")
}

/// Starts a consumer on `topic` with batches of `batch_size`, with or without backpressure.
async fn consumer_for(
    redis: &redisevents_test_support::RedisFixture,
    topic: &str,
    consumer: &str,
    batch_size: u32,
    backpressure: bool,
    handler: Arc<RecordingHandler>,
) -> redisevents::consumer::StreamsHost {
    start_host(
        redis,
        topic,
        redisevents::config::TopicOptions {
            partitions: 1,
            ..Default::default()
        },
        consumer,
        move |o: &mut ConsumerOptions| {
            o.batch_size = batch_size;
            o.persist = PersistMode::SyncBatch;
            o.start_from = StartFrom::Beginning;
            o.backpressure = BackpressureOptions {
                enabled: backpressure,
                ..BackpressureOptions::default()
            };
        },
        handler,
    )
    .await
}

/// S21e. Each message's handler sees the trace context its own producer stamped on the entry, so the
/// context travels per message, not per consumer batch. (The C# span-link assertions for a batch are
/// not ported: they need the `otel` feature.)
async fn each_message_carries_its_producer_trace_parent(backpressure: bool) {
    let redis = require_redis!();
    let topic = redis.new_topic("trace-parent");
    let consumer = redis.new_consumer("trace-parent");
    let publisher = publisher(
        redis,
        &topic,
        redisevents::config::TopicOptions {
            partitions: 1,
            ..Default::default()
        },
    )
    .await;
    let mut sent = Vec::new();
    for i in 0..TOTAL {
        let parent = traceparent(i + 1, 100 + i);
        publisher
            .publish(
                KEY,
                text(&format!("m{i}")),
                MESSAGE_TYPE,
                &PublishOptions::new().trace_parent(parent.clone()),
            )
            .await
            .expect("publish");
        sent.push(parent);
    }
    let handler = Arc::new(RecordingHandler::new());
    let host = consumer_for(redis, &topic, &consumer, 1, backpressure, handler.clone()).await;
    wait_for_count(&handler, TOTAL as usize, "every message").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    host.shutdown().await.expect("shutdown");

    let delivered = handler.messages();
    assert_eq!(delivered.len(), TOTAL as usize);
    let parents: Vec<String> = delivered
        .iter()
        .map(|m| m.trace_parent().unwrap_or_default().to_owned())
        .collect();
    assert_eq!(
        parents, sent,
        "the 'p' field is where per-message trace context lives"
    );
    let unique: std::collections::HashSet<&String> = parents.iter().collect();
    assert_eq!(
        unique.len(),
        TOTAL as usize,
        "each message came from its own trace"
    );
}

/// S21e. The correlation id reaches the handler on every message. (The C# log-scope assertion is not
/// ported: the public API has no log capture.)
async fn the_correlation_id_reaches_the_handler(backpressure: bool) {
    let redis = require_redis!();
    let topic = redis.new_topic("trace-corr");
    let consumer = redis.new_consumer("trace-corr");
    let publisher = publisher(
        redis,
        &topic,
        redisevents::config::TopicOptions {
            partitions: 1,
            ..Default::default()
        },
    )
    .await;
    for i in 0..TOTAL {
        publisher
            .publish(
                KEY,
                text(&format!("m{i}")),
                MESSAGE_TYPE,
                &PublishOptions::new().correlation_id(format!("corr-{i}")),
            )
            .await
            .expect("publish");
    }
    let handler = Arc::new(RecordingHandler::new());
    let host = consumer_for(redis, &topic, &consumer, 2, backpressure, handler.clone()).await;
    wait_for_count(&handler, TOTAL as usize, "every message").await;
    host.shutdown().await.expect("shutdown");
    let ids: Vec<String> = handler
        .messages()
        .iter()
        .map(|m| m.correlation_id().to_owned())
        .collect();
    assert_eq!(
        ids,
        (0..TOTAL).map(|i| format!("corr-{i}")).collect::<Vec<_>>()
    );
}

/// S21e (inline). See [`each_message_carries_its_producer_trace_parent`].
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s21e_a_single_message_span_parents_off_the_producer_that_published_it_inline() {
    each_message_carries_its_producer_trace_parent(false).await;
}

/// S21e (backpressure). See [`each_message_carries_its_producer_trace_parent`].
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s21e_a_single_message_span_parents_off_the_producer_that_published_it_backpressure() {
    each_message_carries_its_producer_trace_parent(true).await;
}

/// S21e (inline). See [`the_correlation_id_reaches_the_handler`].
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s21e_the_correlation_id_reaches_the_handler_and_the_log_scope_inline() {
    the_correlation_id_reaches_the_handler(false).await;
}

/// S21e (backpressure). See [`the_correlation_id_reaches_the_handler`].
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s21e_the_correlation_id_reaches_the_handler_and_the_log_scope_backpressure() {
    the_correlation_id_reaches_the_handler(true).await;
}
