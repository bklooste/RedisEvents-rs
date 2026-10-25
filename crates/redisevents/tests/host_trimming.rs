//! Trimming against real Redis, with and without the consumer host. Port of the C# `TrimmingTests`
//! cases the C5 WP deferred to C4t: the inline approximate `MAXLEN ~` bound on publish (no host, so it
//! runs without the ignore), the background trimmer started by the generic host, and the live-consumer
//! phase of S15b (a blocked partition holds its position). S15c (`maxmemory` under `noeviction`) is not
//! ported: it needs `CONFIG SET maxmemory` on the shared test server, which would disturb every test
//! running against it. The S15b clamp-release sweep is not ported either: `BackgroundTrimmer` is
//! crate-private, so the sweep is only reachable through the host. Skips without Redis.

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
use std::time::Duration;

use host_common::*;
use redisevents::Publisher;
use redisevents::config::{ConsumerOptions, PersistMode, StartFrom, TopicOptions, TrimMode};
use redisevents::consumer::{HandleError, PartitionContext, Streams};
use redisevents::wire::StreamMsg;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until_async};

/// S15 (inline). Publishing 5 000 entries against `MaxLen 1000` with approximate trimming keeps at
/// least `MaxLen` entries and stays near the bound, with no consumer and no host involved.
#[tokio::test]
async fn s15_inline_approximate_trim_bounds_the_stream_at_maxlen() {
    let redis = require_redis!();
    let topic = redis.new_topic("s15-inline");
    let options = TopicOptions {
        partitions: 1,
        max_len: Some(1_000),
        trim: TrimMode::Approx,
        ..Default::default()
    };
    let publisher = publisher(redis, &topic, options).await;
    for i in 0..5_000 {
        publisher
            .publish(
                "k",
                text(&format!("m{i}")),
                MESSAGE_TYPE,
                &redisevents::producer::PublishOptions::new(),
            )
            .await
            .expect("publish");
    }
    let length = xlen(redis, &topic, 0).await;
    assert!(
        length >= 1_000,
        "MAXLEN ~ keeps at least MaxLen entries; got {length}"
    );
    assert!(
        length <= 1_300,
        "approximate trimming overshoots by about one macro node; got {length}"
    );
}

/// S15. A background trimmer started by the generic host (a publish-only host, no consumers) applies
/// the topic's retention window to a stream of hour-old entries.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s15_background_trimmer_runs_when_the_generic_host_starts_it() {
    let redis = require_redis!();
    let topic = redis.new_topic("s15-host");
    let options = TopicOptions {
        partitions: 1,
        max_len: Some(100_000),
        trim: TrimMode::Exact,
        retention_seconds: Some(1_800),
        background_trim_interval_seconds: 1,
        ..Default::default()
    };
    let hour_ago = (time::OffsetDateTime::now_utc().unix_timestamp() as u64 - 3_600) * 1_000;
    seed_entries(redis, &topic, 0, hour_ago, 1_000, 100).await;
    assert_eq!(xlen(redis, &topic, 0).await, 100);

    let host = Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .topic_options(&topic, options)
        .spawn()
        .await
        .expect("spawn a publish-only host");
    wait_until_async(
        || async { xlen(redis, &topic, 0).await == 0 },
        Some(Duration::from_secs(20)),
        "the host-started trimmer to apply the configured 30-minute retention window",
    )
    .await;
    host.shutdown().await.expect("shutdown");
}

/// S15b (live consumer phase). A consumer blocked on one entry does not advance past it, keeps
/// retrying the same batch, and handles nothing past it. (The clamp release itself is not ported, see
/// the module docs.)
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s15b_clamp_release_frees_memory_when_every_entry_is_newer_than_the_cutoff() {
    let redis = require_redis!();
    const BLOCK_AT: u32 = 10;
    let topic = redis.new_topic("s15b");
    let consumer = redis.new_consumer("s15b");
    let options = TopicOptions {
        partitions: 1,
        max_len: Some(1_000),
        trim: TrimMode::Exact,
        retention_seconds: Some(2 * 60 * 60),
        clamp_release_threshold: 0.8,
        ..Default::default()
    };
    let publisher = publisher(redis, &topic, options.clone()).await;
    let mut ids = Vec::new();
    for i in 0..100 {
        ids.push(
            publisher
                .publish(
                    "k",
                    indexed_body(i),
                    MESSAGE_TYPE,
                    &redisevents::producer::PublishOptions::new(),
                )
                .await
                .expect("publish"),
        );
    }

    let handler = Arc::new(RecordingHandler::new().with_hook(
        |batch: &[StreamMsg], _: &PartitionContext| {
            if batch.iter().any(|m| index_of(m) == BLOCK_AT) {
                return Err(HandleError::block(io::Error::other("the blocked batch")));
            }
            Ok(())
        },
    ));
    let host = start_host(
        redis,
        &topic,
        options,
        &consumer,
        |o: &mut ConsumerOptions| {
            o.batch_size = 1;
            o.persist = PersistMode::AsyncBatch;
            o.persist_interval_ms = 50;
            o.start_from = StartFrom::Beginning;
            o.shutdown_timeout_seconds = 5;
        },
        handler.clone(),
    )
    .await;
    wait_until_async(
        || {
            let handler = handler.clone();
            async move {
                handler
                    .messages()
                    .iter()
                    .filter(|m| index_of(m) == BLOCK_AT)
                    .count()
                    > 1
            }
        },
        Some(Duration::from_secs(30)),
        "the blocked partition to retry the same batch",
    )
    .await;
    wait_until_async(
        || async {
            positions(redis, &topic, &consumer).await.get(&0) == Some(&ids[(BLOCK_AT - 1) as usize])
        },
        Some(Duration::from_secs(30)),
        "the blocked consumer to flush the position of the last batch it did handle",
    )
    .await;
    assert_eq!(
        positions(redis, &topic, &consumer).await.get(&0),
        Some(&ids[(BLOCK_AT - 1) as usize]),
        "the position must not advance past a batch the handler never completed"
    );
    let seen: Vec<u32> = handler.messages().iter().map(index_of).collect();
    assert!(
        seen.iter().all(|i| *i <= BLOCK_AT),
        "nothing past the blocked batch may be handled"
    );
    assert!(
        host.is_running(),
        "a blocked partition does not stop the host"
    );
    host.shutdown().await.expect("shutdown");
}
