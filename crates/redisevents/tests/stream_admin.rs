//! Integration tests for `StreamAdmin` against a real Redis. Ports of the C# `TopicTailTests`, the
//! preview / reset halves of `ReplayTests` (S9) and the admin-written parts of `PositionTests`
//! and `WriterIdentityTests`, in `snake_case`.
//!
//! Skipped (needs the consumer host, C4, or a running flusher, C3): `ReplayTests.S10_a_running_consumer_picks_up_the_reset_marker_and_rewinds_on_a_flush_tick`;
//! the consumer-driven halves of S9 (a first and a second consumer pass) are replaced by writing the
//! stored position directly and reading the stream after the target; `WriterIdentityTests` S6e and
//! the static / lease stand-down cases (flusher behaviour, not admin). Skips without Redis.

#![allow(
    clippy::items_after_statements,
    clippy::cast_possible_truncation,
    clippy::assertions_on_result_states,
    clippy::assert_is_empty
)]

use std::collections::HashMap;

use redisevents::Error;
use redisevents::admin::meta::ensure_topic_meta;
use redisevents::admin::stream_admin::{ResetDirection, ResetOptions, ResetTarget, StreamAdmin};
use redisevents::config::{StartFrom, TopicOptions};
use redisevents::ownership::PartitionOwner;
use redisevents::positions::{ADMIN_INSTANCE_ID, PositionStore, RedisPositionStore};
use redisevents::wire::StreamId;
use redisevents_test_support::{RedisFixture, require_redis};
use time::OffsetDateTime;
use uuid::Uuid;

async fn admin(redis: &RedisFixture) -> StreamAdmin {
    StreamAdmin::new(redis.connect().await, redis.keys())
}

async fn xadd(redis: &RedisFixture, key: &str, id: &str) {
    let mut conn = redis.connect().await.shared();
    let _: String = redis::cmd("XADD")
        .arg(key)
        .arg(id)
        .arg("f")
        .arg("v")
        .query_async(&mut conn)
        .await
        .expect("XADD");
}

fn key(redis: &RedisFixture, topic: &str, p: u32) -> String {
    redis.keys().stream(topic, p, true)
}

fn opts(partitions: u32) -> TopicOptions {
    TopicOptions {
        partitions,
        ..TopicOptions::default()
    }
}

fn id(ms: u64, seq: u64) -> StreamId {
    StreamId::new(ms, seq)
}

fn at(ms: u64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).unwrap()
}

/// One partition seeded with `count` entries, one per `step_ms`, from `first_ms`.
async fn seed(
    redis: &RedisFixture,
    topic: &str,
    first_ms: u64,
    step_ms: u64,
    count: u64,
) -> Vec<StreamId> {
    let mut ids = Vec::new();
    for i in 0..count {
        let sid = id(first_ms + i * step_ms, 0);
        xadd(redis, &key(redis, topic, 0), &sid.to_string()).await;
        ids.push(sid);
    }
    ids
}

async fn hgetall(redis: &RedisFixture, key: &str) -> HashMap<String, String> {
    let mut conn = redis.connect().await.shared();
    redis::cmd("HGETALL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .expect("HGETALL")
}

// ---- TopicTailTests ----

#[tokio::test]
async fn a_topic_that_was_never_written_reports_0_0_on_every_partition() {
    let redis = require_redis!();
    let topic = redis.new_topic("tail-none");
    let tails = admin(redis)
        .await
        .read_topic_tail(&topic, &opts(3))
        .await
        .unwrap();
    assert_eq!(tails, vec![StreamId::MIN; 3]);
}

#[tokio::test]
async fn an_ensured_but_empty_topic_reports_0_0() {
    let redis = require_redis!();
    let topic = redis.new_topic("tail-ensured");
    let options = opts(2);
    ensure_topic_meta(&redis.connect().await, &redis.keys(), &topic, &options)
        .await
        .unwrap();
    let tails = admin(redis)
        .await
        .read_topic_tail(&topic, &options)
        .await
        .unwrap();
    assert_eq!(tails, vec![StreamId::MIN; 2]);
}

#[tokio::test]
async fn each_partition_reports_its_own_last_id() {
    let redis = require_redis!();
    let topic = redis.new_topic("tail-each");
    xadd(redis, &key(redis, &topic, 0), "5-1").await;
    xadd(redis, &key(redis, &topic, 0), "7-0").await;
    xadd(redis, &key(redis, &topic, 2), "9-3").await;
    let tails = admin(redis)
        .await
        .read_topic_tail(&topic, &opts(3))
        .await
        .unwrap();
    assert_eq!(tails, vec![id(7, 0), StreamId::MIN, id(9, 3)]);
}

#[tokio::test]
async fn a_trimmed_stream_still_reports_the_last_id_ever_added() {
    let redis = require_redis!();
    let topic = redis.new_topic("tail-trimmed");
    let k = key(redis, &topic, 0);
    xadd(redis, &k, "10-0").await;
    xadd(redis, &k, "11-0").await;
    let mut conn = redis.connect().await.shared();
    let _: i64 = redis::cmd("XTRIM")
        .arg(&k)
        .arg("MAXLEN")
        .arg(0)
        .query_async(&mut conn)
        .await
        .unwrap();
    let tails = admin(redis)
        .await
        .read_topic_tail(&topic, &opts(1))
        .await
        .unwrap();
    assert_eq!(tails, vec![id(11, 0)]);
}

#[tokio::test]
async fn keys_carry_the_namespace_prefix() {
    let redis = require_redis!();
    let topic = redis.new_topic("tail-prefix");
    assert!(!redis.keys().prefix().is_empty());
    // The same shape without the prefix: another environment's stream, which must not be read.
    xadd(redis, &format!("s:{{{topic}}}:0"), "99-0").await;
    xadd(redis, &key(redis, &topic, 0), "3-0").await;
    let tails = admin(redis)
        .await
        .read_topic_tail(&topic, &opts(1))
        .await
        .unwrap();
    assert_eq!(tails, vec![id(3, 0)]);
}

#[tokio::test]
async fn a_spread_topic_reads_the_untagged_keys() {
    let redis = require_redis!();
    let topic = redis.new_topic("tail-spread");
    xadd(redis, &redis.keys().stream(&topic, 1, false), "4-2").await;
    let options = TopicOptions {
        partitions: 2,
        co_locate_partitions: false,
        ..TopicOptions::default()
    };
    let tails = admin(redis)
        .await
        .read_topic_tail(&topic, &options)
        .await
        .unwrap();
    assert_eq!(tails, vec![StreamId::MIN, id(4, 2)]);
}

#[tokio::test]
async fn fewer_than_one_partition_is_refused() {
    let redis = require_redis!();
    let err = admin(redis)
        .await
        .read_topic_tail("t", &opts(0))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
}

// ---- ReplayTests S9 ----

#[tokio::test]
async fn s9_reset_to_a_date_reprocesses_exactly_the_count_the_preview_predicted() {
    let redis = require_redis!();
    let topic = redis.new_topic("s9");
    let consumer = redis.new_consumer("s9");
    let options = opts(1);
    const TOTAL: u64 = 60;
    const MIDPOINT: usize = 30;
    let first_ms = 1_700_000_000_000;
    let ids = seed(redis, &topic, first_ms, 60_000, TOTAL).await;
    ensure_topic_meta(&redis.connect().await, &redis.keys(), &topic, &options)
        .await
        .unwrap();

    // The state after a first pass: parked on the newest entry.
    let store = RedisPositionStore::new(redis.connect().await, redis.keys(), Uuid::new_v4());
    store
        .save(&topic, &consumer, &[(0, *ids.last().unwrap())])
        .await
        .unwrap();

    // The midpoint is exactly the instant entry 30 was written at: the boundary case.
    let midpoint = at(ids[MIDPOINT].ms);
    let admin = admin(redis).await;
    let scope = ResetOptions {
        topic: Some(options.clone()),
        ..ResetOptions::default()
    };
    let preview = admin
        .preview_reset(&topic, &consumer, ResetTarget::Time(midpoint), &scope)
        .await
        .unwrap();

    assert_eq!(preview.partitions.len(), 1);
    let p = &preview.partitions[0];
    assert_eq!(p.direction, ResetDirection::Rewind);
    assert_eq!(p.current, ids.last().copied());
    assert_eq!(
        p.target,
        id(ids[MIDPOINT].ms - 1, StreamId::MAX_PART),
        "the target sits immediately before <ms>-0 so the entry on that millisecond is replayed"
    );
    assert_eq!(p.oldest, Some(ids[0]));
    assert_eq!(p.newest, ids.last().copied());
    assert_eq!(p.length, TOTAL);
    assert!(!p.trimmed_away);
    assert!(p.entries_exact);
    assert_eq!(preview.total_to_reprocess(), TOTAL - MIDPOINT as u64);
    assert_eq!(preview.total_to_skip(), 0);
    assert!(!preview.any_trimmed_away());

    let targets = admin
        .reset_position(&topic, &consumer, ResetTarget::Time(midpoint), &scope)
        .await
        .unwrap();
    assert_eq!(targets, vec![(0, p.target)]);

    // What a consumer starting from the stored position would now read: XRANGE strictly after it.
    let stored = store.load(&topic, &consumer).await.unwrap();
    assert_eq!(stored[&0], p.target);
    let mut conn = redis.connect().await.shared();
    let after: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(key(redis, &topic, 0))
        .arg(format!("({}", p.target))
        .arg("+")
        .query_async(&mut conn)
        .await
        .unwrap();
    let read: Vec<StreamId> = after
        .ids
        .iter()
        .map(|e| StreamId::parse(&e.id).unwrap())
        .collect();
    assert_eq!(read, ids[MIDPOINT..].to_vec());
}

#[tokio::test]
async fn s9_preview_warns_when_the_requested_date_precedes_the_oldest_surviving_entry() {
    let redis = require_redis!();
    let topic = redis.new_topic("s9-trim");
    let consumer = redis.new_consumer("s9-trim");
    let options = opts(1);
    const TOTAL: u64 = 40;
    const SURVIVORS: u64 = 10;
    let ids = seed(redis, &topic, 1_700_000_000_000, 60_000, TOTAL).await;
    RedisPositionStore::new(redis.connect().await, redis.keys(), Uuid::new_v4())
        .save(&topic, &consumer, &[(0, *ids.last().unwrap())])
        .await
        .unwrap();
    let mut conn = redis.connect().await.shared();
    let _: i64 = redis::cmd("XTRIM")
        .arg(key(redis, &topic, 0))
        .arg("MAXLEN")
        .arg("=")
        .arg(SURVIVORS)
        .query_async(&mut conn)
        .await
        .unwrap();

    let admin = admin(redis).await;
    let scope = ResetOptions {
        topic: Some(options),
        ..ResetOptions::default()
    };
    let too_old = admin
        .preview_reset(&topic, &consumer, ResetTarget::Time(at(ids[0].ms)), &scope)
        .await
        .unwrap();
    let gone = &too_old.partitions[0];
    assert!(gone.trimmed_away);
    assert!(too_old.any_trimmed_away());
    assert_eq!(gone.oldest, Some(ids[(TOTAL - SURVIVORS) as usize]));
    assert_eq!(gone.length, SURVIVORS);
    assert_eq!(gone.entries, SURVIVORS);
    assert!(too_old.describe().contains("that data is gone"));

    let in_window = admin
        .preview_reset(
            &topic,
            &consumer,
            ResetTarget::Time(at(ids[(TOTAL - SURVIVORS) as usize].ms)),
            &scope,
        )
        .await
        .unwrap();
    assert!(!in_window.any_trimmed_away());
    assert!(!in_window.partitions[0].trimmed_away);
    assert_eq!(in_window.total_to_reprocess(), SURVIVORS);
    assert!(!in_window.describe().contains("that data is gone"));
}

// ---- Reset behaviour (StreamAdmin.Reset.cs) ----

#[tokio::test]
async fn reset_writes_the_position_under_the_admin_id_with_a_marker() {
    let redis = require_redis!();
    let topic = redis.new_topic("reset-bytes");
    let consumer = redis.new_consumer("reset-bytes");
    let admin = admin(redis).await;
    let scope = ResetOptions {
        topic: Some(opts(3)),
        ..ResetOptions::default()
    };
    let targets = admin
        .reset_position(&topic, &consumer, ResetTarget::Id(id(12, 3)), &scope)
        .await
        .unwrap();
    assert_eq!(
        targets,
        vec![(0, id(12, 3)), (1, id(12, 3)), (2, id(12, 3))]
    );

    let hash = hgetall(redis, &redis.keys().positions(&topic, &consumer)).await;
    for p in 0..3 {
        assert_eq!(
            hash[&p.to_string()],
            format!("12-3|{}", ADMIN_INSTANCE_ID.hyphenated())
        );
        let marker = &hash[&StreamAdmin::reset_marker_field(p)];
        let (target, issued) = marker.split_once('|').expect("marker has a time");
        assert_eq!(target, "12-3");
        assert!(
            issued.ends_with("+00:00") && issued.contains('T'),
            "{issued}"
        );
    }
    let meta = hgetall(redis, &redis.keys().positions_meta(&topic, &consumer)).await;
    assert_eq!(
        meta["0:instanceId"],
        ADMIN_INSTANCE_ID.hyphenated().to_string()
    );
}

#[tokio::test]
async fn reset_one_partition_touches_only_that_partition() {
    let redis = require_redis!();
    let topic = redis.new_topic("reset-one");
    let consumer = redis.new_consumer("reset-one");
    let scope = ResetOptions {
        partition: Some(1),
        topic: Some(opts(3)),
        ..ResetOptions::default()
    };
    let targets = admin(redis)
        .await
        .reset_position(&topic, &consumer, ResetTarget::Start, &scope)
        .await
        .unwrap();
    assert_eq!(targets, vec![(1, StreamId::MIN)]);
    let hash = hgetall(redis, &redis.keys().positions(&topic, &consumer)).await;
    assert!(hash.contains_key("1") && hash.contains_key("__reset:1"));
    assert!(!hash.contains_key("0") && !hash.contains_key("2"));
}

#[tokio::test]
async fn reset_of_a_partition_that_does_not_exist_is_refused() {
    let redis = require_redis!();
    let topic = redis.new_topic("reset-range");
    let consumer = redis.new_consumer("reset-range");
    let scope = ResetOptions {
        partition: Some(3),
        topic: Some(opts(3)),
        ..ResetOptions::default()
    };
    let err = admin(redis)
        .await
        .reset_position(&topic, &consumer, ResetTarget::Start, &scope)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err}");
    assert!(
        hgetall(redis, &redis.keys().positions(&topic, &consumer))
            .await
            .is_empty(),
        "nothing written"
    );
}

#[tokio::test]
async fn whole_topic_reset_without_a_known_partition_count_is_refused() {
    let redis = require_redis!();
    let topic = redis.new_topic("reset-nocount");
    let err = admin(redis)
        .await
        .reset_position(&topic, "c", ResetTarget::Start, &ResetOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err}");
}

#[tokio::test]
async fn the_partition_count_and_layout_are_read_from_meta_when_not_given() {
    let redis = require_redis!();
    let topic = redis.new_topic("reset-meta");
    let consumer = redis.new_consumer("reset-meta");
    ensure_topic_meta(&redis.connect().await, &redis.keys(), &topic, &opts(2))
        .await
        .unwrap();
    let admin = admin(redis).await;
    let layout = admin.read_topic_layout(&topic).await.unwrap();
    assert_eq!(layout.partitions, 2);
    assert!(layout.co_locate_partitions);
    let meta = admin.read_topic_meta(&topic).await.unwrap();
    assert_eq!(meta["partitions"], "2");
    let targets = admin
        .reset_position(
            &topic,
            &consumer,
            ResetTarget::Id(id(5, 0)),
            &ResetOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(targets.len(), 2);
}

#[tokio::test]
async fn reset_to_the_end_moves_to_each_tail_and_refuses_a_missing_stream() {
    let redis = require_redis!();
    let topic = redis.new_topic("reset-end");
    let consumer = redis.new_consumer("reset-end");
    xadd(redis, &key(redis, &topic, 0), "8-1").await;
    let admin = admin(redis).await;
    let scope = ResetOptions {
        topic: Some(opts(1)),
        ..ResetOptions::default()
    };
    let targets = admin
        .reset_position(&topic, &consumer, ResetTarget::End, &scope)
        .await
        .unwrap();
    assert_eq!(targets, vec![(0, id(8, 1))]);

    let missing = redis.new_topic("reset-end-missing");
    let err = admin
        .reset_position(&missing, &consumer, ResetTarget::End, &scope)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err}");
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[tokio::test]
async fn preview_with_no_stored_position_measures_from_the_assumed_start() {
    let redis = require_redis!();
    let topic = redis.new_topic("preview-missing");
    let consumer = redis.new_consumer("preview-missing");
    let ids = seed(redis, &topic, 1_000, 1_000, 5).await;
    let admin = admin(redis).await;
    let mut scope = ResetOptions {
        topic: Some(opts(1)),
        ..ResetOptions::default()
    };

    let from_start = admin
        .preview_reset(&topic, &consumer, ResetTarget::Id(ids[2]), &scope)
        .await
        .unwrap();
    let p = &from_start.partitions[0];
    assert_eq!((p.current, p.assumed_start), (None, Some(StreamId::MIN)));
    assert_eq!((p.direction, p.entries), (ResetDirection::Skip, 3));

    scope.start_from_when_missing = StartFrom::Now;
    let from_now = admin
        .preview_reset(&topic, &consumer, ResetTarget::Id(ids[2]), &scope)
        .await
        .unwrap();
    let p = &from_now.partitions[0];
    assert_eq!(p.assumed_start, Some(ids[4]));
    assert_eq!((p.direction, p.entries), (ResetDirection::Rewind, 2));
    assert!(from_now.describe().contains("(none, assuming"));
}

#[tokio::test]
async fn preview_counting_stops_at_the_scan_cap_and_reports_a_floor() {
    let redis = require_redis!();
    let topic = redis.new_topic("preview-cap");
    let consumer = redis.new_consumer("preview-cap");
    let k = key(redis, &topic, 0);
    let mut conn = redis.connect().await.shared();
    let mut pipe = redis::pipe();
    for i in 1..=2_500u64 {
        pipe.cmd("XADD")
            .arg(&k)
            .arg(format!("{i}-0"))
            .arg("f")
            .arg("v")
            .ignore();
    }
    pipe.query_async::<()>(&mut conn).await.unwrap();

    let scope = ResetOptions {
        topic: Some(opts(1)),
        max_count_scan: 1_500,
        ..ResetOptions::default()
    };
    let admin = admin(redis).await;
    let preview = admin
        .preview_reset(&topic, &consumer, ResetTarget::End, &scope)
        .await
        .unwrap();
    let p = &preview.partitions[0];
    assert_eq!(p.direction, ResetDirection::Skip);
    assert!(!p.entries_exact);
    assert!(p.entries >= 1_500 && p.entries < 2_500, "{}", p.entries);

    let exact = admin
        .preview_reset(
            &topic,
            &consumer,
            ResetTarget::End,
            &ResetOptions {
                max_count_scan: 0,
                ..scope
            },
        )
        .await
        .unwrap();
    assert!(exact.partitions[0].entries_exact);
    assert_eq!(exact.partitions[0].entries, 2_500);
}

// ---- StreamAdmin.Ownership.cs ----

#[tokio::test]
async fn get_ownership_reads_owners_skips_junk_and_reports_gaps() {
    let redis = require_redis!();
    let topic = redis.new_topic("own");
    let consumer = redis.new_consumer("own");
    let instance = Uuid::new_v4();
    let owner = PartitionOwner::parse(0, &format!("{instance}|pod-a")).unwrap_or_else(|_| {
        panic!("owner value parses");
    });
    let mut conn = redis.connect().await.shared();
    let _: () = redis::cmd("HSET")
        .arg(redis.keys().ownership(&topic, &consumer))
        .arg("0")
        .arg(owner.format())
        .arg("2")
        .arg(owner.format())
        .arg("i:whatever")
        .arg("x")
        .arg("notanumber")
        .arg("y")
        .query_async(&mut conn)
        .await
        .unwrap();

    let admin = admin(redis).await;
    let map = admin.get_ownership(&topic, &consumer, 4).await.unwrap();
    assert_eq!(map.owners.keys().copied().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(map.unowned, vec![1, 3]);
    assert!(map.contested.is_empty());

    // Unknown count, no meta: inferred from the highest claim.
    let inferred = admin.get_ownership(&topic, &consumer, 0).await.unwrap();
    assert_eq!(inferred.unowned, vec![1]);

    // Count from meta.
    ensure_topic_meta(&redis.connect().await, &redis.keys(), &topic, &opts(5))
        .await
        .unwrap();
    let from_meta = admin.get_ownership(&topic, &consumer, 0).await.unwrap();
    assert_eq!(from_meta.unowned, vec![1, 3, 4]);
}
