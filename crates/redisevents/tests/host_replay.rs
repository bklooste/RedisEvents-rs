//! Replay through the consumer host against real Redis. Port of the consumer-driven parts of the C#
//! `ReplayTests` (S9 reset, S10 live reset). The preview-only S9 cases (the warning on a trimmed-away
//! start) are already ported, without the host, in `stream_admin.rs`.
//!
//! Entries are seeded by hand with explicit ids spaced a minute apart (the C# `SeedAsync`), in the
//! codec's field layout (`b` body, `t` type, `k` partition key), so the host decodes them.
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
use std::time::{Duration, Instant};

use host_common::*;
use redisevents::Publisher;
use redisevents::admin::meta::ensure_topic_meta;
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::config::{ConsumerOptions, PersistMode, StartFrom};
use redisevents::wire::StreamId;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until_async};
use time::OffsetDateTime;

fn body(index: u32) -> String {
    format!("msg-{index:03}")
}

fn bodies(from: u32, to: u32) -> Vec<String> {
    (from..to).map(body).collect()
}

fn at(ms: u64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).expect("in range")
}

fn replay_consumer(
    configure_extra: impl FnOnce(&mut ConsumerOptions),
) -> impl FnOnce(&mut ConsumerOptions) {
    move |o: &mut ConsumerOptions| {
        o.batch_size = 10;
        o.start_from = StartFrom::Stored;
        o.start_from_when_missing = StartFrom::Beginning;
        o.persist = PersistMode::AsyncBatch;
        o.persist_interval_ms = 100;
        o.block_ms = 200;
        configure_extra(o);
    }
}

/// S9. A reset to a date reprocesses exactly the count the preview predicted, starting on the entry
/// written at the requested millisecond.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s9_reset_to_a_date_reprocesses_exactly_the_count_the_preview_predicted() {
    let redis = require_redis!();
    let topic = redis.new_topic("s9-host");
    let consumer = redis.new_consumer("s9-host");
    let options = volume(1);
    const TOTAL: u32 = 60;
    const MIDPOINT: u32 = 30;
    let first_ms =
        (time::OffsetDateTime::now_utc().unix_timestamp() as u64 - u64::from(TOTAL) * 60) * 1_000;
    ensure_topic_meta(&redis.connect().await, &redis.keys(), &topic, &options)
        .await
        .expect("topic meta");
    let ids = seed_entries(redis, &topic, 0, first_ms, 60_000, TOTAL).await;

    let first = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        options.clone(),
        &consumer,
        replay_consumer(|_| {}),
        first.clone(),
    )
    .await;
    wait_for_count(
        &first,
        TOTAL as usize,
        "the first pass to read the whole stream",
    )
    .await;
    host.shutdown().await.expect("shutdown first pass");
    assert_eq!(
        first.bodies(),
        bodies(0, TOTAL),
        "the first pass reads the whole stream"
    );
    assert_eq!(
        positions(redis, &topic, &consumer).await.get(&0),
        ids.last(),
        "a clean stop flushes the final position, so the consumer is parked on the newest entry"
    );

    let admin = StreamAdmin::new(redis.connect().await, redis.keys());
    let midpoint = at(ids[MIDPOINT as usize].ms);
    let scope = ResetOptions {
        topic: Some(options.clone()),
        ..ResetOptions::default()
    };
    let preview = admin
        .preview_reset(&topic, &consumer, ResetTarget::Time(midpoint), &scope)
        .await
        .expect("preview");
    assert_eq!(preview.total_to_reprocess(), u64::from(TOTAL - MIDPOINT));
    let targets = admin
        .reset_position(&topic, &consumer, ResetTarget::Time(midpoint), &scope)
        .await
        .expect("reset");
    assert_eq!(targets.len(), 1);

    let replayed = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        options,
        &consumer,
        replay_consumer(|_| {}),
        replayed.clone(),
    )
    .await;
    let expected = preview.total_to_reprocess() as usize;
    wait_for_count(&replayed, expected, "the replay the preview promised").await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    host.shutdown().await.expect("shutdown replay");
    assert_eq!(
        replayed.count(),
        expected,
        "the number reprocessed must equal the number the preview promised"
    );
    let got: Vec<StreamId> = replayed.messages().iter().map(|m| m.id()).collect();
    assert_eq!(
        got,
        ids[MIDPOINT as usize..].to_vec(),
        "the replay starts on the entry at the requested millisecond"
    );
    assert_eq!(replayed.bodies(), bodies(MIDPOINT, TOTAL));
}

/// S10. A running consumer picks up an admin reset marker and rewinds on a flush tick, without a
/// restart, and the marker is cleared once taken.
#[tokio::test]
#[ignore = "needs C4 implementation"]
async fn s10_a_running_consumer_picks_up_the_reset_marker_and_rewinds_on_a_flush_tick() {
    let redis = require_redis!();
    let topic = redis.new_topic("s10");
    let consumer = redis.new_consumer("s10");
    let options = volume(1);
    const TOTAL: u32 = 20;
    const REWIND_TO: u32 = 9;
    ensure_topic_meta(&redis.connect().await, &redis.keys(), &topic, &options)
        .await
        .expect("topic meta");

    let handler = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        options.clone(),
        &consumer,
        replay_consumer(|o| {
            o.persist_interval_ms = 200;
            o.block_ms = 200;
        }),
        handler.clone(),
    )
    .await;
    assert_eq!(host.owned_partitions(&topic, &consumer), vec![0]);

    let publisher = publisher(redis, &topic, options.clone()).await;
    let mut ids = Vec::new();
    for i in 0..TOTAL {
        ids.push(
            publisher
                .publish(
                    "replay",
                    text(&body(i)),
                    MESSAGE_TYPE,
                    &redisevents::producer::PublishOptions::new(),
                )
                .await
                .expect("publish"),
        );
    }
    wait_for_count(&handler, TOTAL as usize, "the first pass").await;
    assert_eq!(handler.bodies(), bodies(0, TOTAL));
    wait_until_async(
        || async { positions(redis, &topic, &consumer).await.get(&0) == ids.last() },
        Some(Duration::from_secs(15)),
        "the running consumer to flush its position at the tail",
    )
    .await;

    let admin = StreamAdmin::new(redis.connect().await, redis.keys());
    let clock = Instant::now();
    let targets = admin
        .reset_position(
            &topic,
            &consumer,
            ResetTarget::Id(ids[REWIND_TO as usize]),
            &ResetOptions::default(),
        )
        .await
        .expect("reset");
    assert_eq!(targets, vec![(0, ids[REWIND_TO as usize])]);

    let expected_total = (TOTAL + (TOTAL - 1 - REWIND_TO)) as usize;
    wait_for_count(&handler, expected_total, "the rewound replay").await;
    let rewound = clock.elapsed();
    assert!(
        host.is_running(),
        "the rewind happened live, not across a restart"
    );
    assert_eq!(
        handler.bodies()[TOTAL as usize..].to_vec(),
        bodies(REWIND_TO + 1, TOTAL),
        "everything after the target is delivered a second time, and nothing before it is"
    );
    assert!(
        rewound < Duration::from_secs(3),
        "a live reset is picked up on the next flush tick (about 400 ms here), took {rewound:?}"
    );

    let marker = StreamAdmin::reset_marker_field(0);
    let positions_key = redis.keys().positions(&topic, &consumer);
    let conn = raw(redis).await;
    wait_until_async(
        || {
            let mut conn = conn.clone();
            let positions_key = positions_key.clone();
            let marker = marker.clone();
            async move {
                let present: bool = redis::cmd("HEXISTS")
                    .arg(&positions_key)
                    .arg(&marker)
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(true);
                !present
            }
        },
        Some(Duration::from_secs(15)),
        "the reset marker to be cleared once the reader had taken it",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(
        handler.count(),
        expected_total,
        "the replay delivers exactly the rewound range, once"
    );
    wait_until_async(
        || async { positions(redis, &topic, &consumer).await.get(&0) == ids.last() },
        Some(Duration::from_secs(15)),
        "the position to return to the tail once the replay had drained",
    )
    .await;
    host.shutdown().await.expect("shutdown");
}
