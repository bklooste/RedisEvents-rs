//! A reset written by another process, against a running host. Port of the C3 deferral
//! `WriterIdentityTests.A_reset_issued_from_another_process_rewinds_instead_of_standing_the_partition_down`:
//! the consumer keeps running, the rewind lands live, and the partition is not stood down. The "other
//! process" is a second connection through the public `StreamAdmin` (the admin instance id).
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
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::config::{ConsumerOptions, PersistMode, StartFrom};
use redisevents::positions::ADMIN_INSTANCE_ID;
use redisevents::producer::PublishOptions;
use redisevents_test_support::{RecordingHandler, require_redis, wait_until_async};

/// The rewind lands and the tail is delivered again; until the consumer has flushed its own position
/// over the admin's, the second-writer check has not run, so the test nudges one message at a time until
/// the stamp changes; then the partition is still consumed.
#[tokio::test]
async fn a_reset_issued_from_another_process_rewinds_instead_of_standing_the_partition_down() {
    let redis = require_redis!();
    const TOTAL: u32 = 20;
    const REWIND_TO: usize = 9;
    let topic = redis.new_topic("oop-reset");
    let consumer = redis.new_consumer("oop-reset");
    let options = volume(1);
    let publisher = publisher(redis, &topic, options.clone()).await;

    let handler = Arc::new(RecordingHandler::new());
    let host = start_host(
        redis,
        &topic,
        options,
        &consumer,
        |o: &mut ConsumerOptions| {
            o.batch_size = 10;
            o.start_from = StartFrom::Stored;
            o.start_from_when_missing = StartFrom::Beginning;
            o.persist = PersistMode::AsyncBatch;
            o.persist_interval_ms = 100;
            o.block_ms = 200;
        },
        handler.clone(),
    )
    .await;

    let mut ids = Vec::new();
    for i in 0..TOTAL {
        ids.push(
            publisher
                .publish(
                    "identity",
                    text(&format!("msg-{i:03}")),
                    MESSAGE_TYPE,
                    &PublishOptions::new(),
                )
                .await
                .expect("publish"),
        );
    }
    wait_for_count(&handler, TOTAL as usize, "the first pass").await;
    wait_until_async(
        || async { positions(redis, &topic, &consumer).await.get(&0) == ids.last() },
        Some(Duration::from_secs(15)),
        "the running consumer to flush its position at the tail",
    )
    .await;

    // The "other process": its own connection and the admin instance id, through the public admin API.
    let admin = StreamAdmin::new(redis.connect().await, redis.keys());
    let targets = admin
        .reset_position(
            &topic,
            &consumer,
            ResetTarget::Id(ids[REWIND_TO]),
            &ResetOptions::default(),
        )
        .await
        .expect("reset");
    assert_eq!(targets, vec![(0, ids[REWIND_TO])]);

    let expected = TOTAL as usize + (TOTAL as usize - 1 - REWIND_TO);
    wait_for_count(&handler, expected, "the rewound tail to be delivered again").await;

    // Until the consumer flushes its own position over the admin's, the second-writer check has not
    // run. A reset drops the replay's first pending position, so nudge one message at a time.
    let mut nudged = false;
    for attempt in 0..40 {
        publisher
            .publish(
                "identity",
                text(&format!("nudge-{attempt}")),
                MESSAGE_TYPE,
                &PublishOptions::new(),
            )
            .await
            .expect("publish");
        tokio::time::sleep(Duration::from_millis(250)).await;
        let records = redisevents::positions::RedisPositionStore::new(
            redis.connect().await,
            redis.keys(),
            ADMIN_INSTANCE_ID,
        )
        .load_records(&topic, &consumer)
        .await
        .expect("load records");
        if records
            .get(&0)
            .is_some_and(|r| r.instance_id != ADMIN_INSTANCE_ID)
        {
            nudged = true;
            break;
        }
    }
    assert!(
        nudged,
        "the consumer must flush its own position over the one the admin wrote"
    );

    // The part that fails without the fix: the partition is still consumed after that flush.
    publisher
        .publish(
            "identity",
            text("msg-last"),
            MESSAGE_TYPE,
            &PublishOptions::new(),
        )
        .await
        .expect("publish");
    wait_until_async(
        || {
            let handler = handler.clone();
            async move { handler.bodies().iter().any(|b| b == "msg-last") }
        },
        Some(Duration::from_secs(30)),
        "a message published after the reset to be delivered",
    )
    .await;
    assert!(
        host.is_running(),
        "a reset from another process must not stand the partition down"
    );
    host.shutdown().await.expect("shutdown");
}
