//! Port of the trimmer parts of `TrimmingTests.cs` plus the interlock safety tests.
//!
//! Not ported here (see the PR): `S15_inline_*` (inline `XADD MAXLEN`, already covered by B1's
//! `producer_publisher.rs` exact/none trim tests), `A_trimming_topic_never_trims_its_state_streams`
//! needs `StreamStore` (B3, deferred) so only its trimmer half is ported, `S15_background_trimmer_runs_when_the_generic_host_starts_it`
//! and the live-consumer phase of S15b need the consumer host (defer to C4), and
//! `S15c_at_maxmemory_*` needs the host plus a publisher under `maxmemory` (defer to C4).

use std::sync::Arc;
use std::time::Duration;

use redisevents_test_support::{RedisFixture, require_redis, wait_until_async};

use super::*;
use crate::config::StreamOptions;
use crate::positions::{PositionStore, redis_store::RedisPositionStore};

fn options_for(topic: &str, topic_options: &TopicOptions) -> StreamOptions {
    let mut options = StreamOptions::default();
    options
        .topics
        .insert(topic.to_owned(), topic_options.clone());
    options
}

fn topic_options(max_len: u64, trim: TrimMode, retention: u32) -> TopicOptions {
    TopicOptions {
        partitions: 1,
        max_len: Some(max_len),
        trim,
        retention_seconds: Some(retention),
        ..TopicOptions::default()
    }
}

struct Rig {
    conn: Connection,
    keys: Keys,
    trimmer: Arc<BackgroundTrimmer>,
    store: RedisPositionStore,
}

async fn rig(redis: &RedisFixture, topic: &str, options: &TopicOptions) -> Rig {
    // The fixture's own `Connection`/`Keys` come from the dev-dependency copy of this crate, so
    // rebuild them here from its URL and prefix.
    let conn = Connection::connect(redis.url()).await.expect("connect");
    let keys = Keys::with_prefix(redis.keys().prefix());
    Rig {
        trimmer: Arc::new(BackgroundTrimmer::new(
            conn.clone(),
            keys.clone(),
            &options_for(topic, options),
        )),
        store: RedisPositionStore::new(conn.clone(), keys.clone(), uuid::Uuid::new_v4()),
        conn,
        keys,
    }
}

fn hours_ago_ms(hours: i64) -> u64 {
    let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    u64::try_from(now).unwrap() - u64::try_from(hours * 3_600_000).unwrap()
}

/// `count` entries with explicit ids `base_ms + i`-0, so the retention cutoff is a fixed point.
async fn write_dated(conn: &Connection, key: &str, base_ms: u64, count: u64) -> Vec<StreamId> {
    let mut shared = conn.shared();
    let mut ids = Vec::new();
    for i in 0..count {
        let id = StreamId::new(base_ms + i, 0);
        let _: String = redis::cmd("XADD")
            .arg(key)
            .arg(id.to_string())
            .arg("d")
            .arg("x")
            .query_async(&mut shared)
            .await
            .expect("XADD");
        ids.push(id);
    }
    ids
}

async fn xadd_auto(conn: &Connection, key: &str, count: u64) {
    let mut shared = conn.shared();
    for _ in 0..count {
        let _: String = redis::cmd("XADD")
            .arg(key)
            .arg("*")
            .arg("d")
            .arg("x")
            .query_async(&mut shared)
            .await
            .expect("XADD");
    }
}

async fn len(conn: &Connection, key: &str) -> i64 {
    redis::cmd("XLEN")
        .arg(key)
        .query_async(&mut conn.shared())
        .await
        .expect("XLEN")
}

async fn first_id(conn: &Connection, key: &str) -> Option<StreamId> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .arg("COUNT")
        .arg(1)
        .query_async(&mut conn.shared())
        .await
        .expect("XRANGE");
    reply.ids.first().map(|e| StreamId::parse(&e.id).unwrap())
}

async fn del(conn: &Connection, key: &str) {
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut conn.shared())
        .await
        .expect("DEL");
}

#[tokio::test]
async fn s15_background_time_trim_clamps_at_the_slowest_stored_position() {
    let redis = require_redis!();
    let topic = redis.new_topic("trim-clamp");
    let options = topic_options(100_000, TrimMode::Exact, 1_800);
    let r = rig(redis, &topic, &options).await;
    let key = r.keys.stream(&topic, 0, true);

    // An hour old: "now - 30 minutes" is after every entry.
    let ids = write_dated(&r.conn, &key, hours_ago_ms(1), 200).await;
    let slow = redis.new_consumer("slow");
    let fast = redis.new_consumer("fast");
    r.store.save(&topic, &slow, &[(0, ids[50])]).await.unwrap();
    r.store.save(&topic, &fast, &[(0, ids[150])]).await.unwrap();

    let held = r.trimmer.trim_topic_once(&topic).await.unwrap();
    let p = &held.partitions[0];
    assert_eq!(p.held_by.as_deref(), Some(slow.as_str()));
    assert_eq!(p.clamp, ids[50]);
    assert_eq!(p.min_id, ids[50], "never a MINID past the slowest consumer");
    assert!(!p.clamp_released);
    assert!(p.cutoff > ids[199]);
    assert_eq!(len(&r.conn, &key).await, 150);
    assert_eq!(first_id(&r.conn, &key).await, Some(ids[50]));

    del(&r.conn, &r.keys.positions(&topic, &slow)).await;
    let moved = r.trimmer.trim_topic_once(&topic).await.unwrap();
    assert_eq!(moved.partitions[0].held_by.as_deref(), Some(fast.as_str()));
    assert_eq!(moved.partitions[0].min_id, ids[150]);
    assert_eq!(len(&r.conn, &key).await, 50);

    del(&r.conn, &r.keys.positions(&topic, &fast)).await;
    let free = r.trimmer.trim_topic_once(&topic).await.unwrap();
    assert_eq!(free.partitions[0].held_by, None);
    assert_eq!(free.partitions[0].min_id, free.partitions[0].cutoff);
    assert_eq!(len(&r.conn, &key).await, 0);
}

#[tokio::test]
async fn s15_background_trimmer_sweeps_on_its_own_timer() {
    let redis = require_redis!();
    let topic = redis.new_topic("trim-timer");
    let mut options = topic_options(100_000, TrimMode::Exact, 1_800);
    options.background_trim_interval_seconds = 1;
    assert!(BackgroundTrimmer::is_enabled(&options));
    let r = rig(redis, &topic, &options).await;
    let key = r.keys.stream(&topic, 0, true);

    write_dated(&r.conn, &key, hours_ago_ms(1), 100).await;
    assert_eq!(len(&r.conn, &key).await, 100);
    assert_eq!(r.trimmer.topics(), vec![topic.clone()]);

    let started = r.trimmer.start();
    assert!(started.is_running());
    wait_until_async(
        || async { len(&r.conn, &key).await == 0 },
        Some(Duration::from_secs(20)),
        "the timer loop to sweep the topic at its 1s interval",
    )
    .await;
    started.stop().await;
    assert!(!started.is_running());
    started.stop().await; // idempotent
}

#[tokio::test]
async fn a_trimming_topic_never_trims_its_state_streams() {
    let redis = require_redis!();
    let topic = redis.new_topic("trim-state");
    let options = topic_options(2, TrimMode::Exact, 1_800);
    let r = rig(redis, &topic, &options).await;
    let state = r.keys.state(&topic, "es:Wallet:w-1");
    write_dated(&r.conn, &state, hours_ago_ms(1), 10).await;

    r.trimmer.trim_topic_once(&topic).await.unwrap();

    assert_eq!(
        len(&r.conn, &state).await,
        10,
        "the sweep only touches partition streams"
    );
}

#[tokio::test]
async fn s15b_clamp_release_frees_memory_when_every_entry_is_newer_than_the_cutoff() {
    const MAX_LEN: u64 = 1_000;
    const RELEASE_AT: i64 = 800;
    let redis = require_redis!();
    let topic = redis.new_topic("trim-release");
    let consumer = redis.new_consumer("stuck");
    let options = topic_options(MAX_LEN, TrimMode::Exact, 2 * 60 * 60);
    let r = rig(redis, &topic, &options).await;
    let key = r.keys.stream(&topic, 0, true);

    xadd_auto(&r.conn, &key, 100).await;
    let first = first_id(&r.conn, &key).await.unwrap();

    // Stuck since yesterday (the live-host phase of the C# test is deferred to C4).
    let stuck_since = StreamId::from_time(OffsetDateTime::now_utc() - time::Duration::days(1));
    r.store
        .save(&topic, &consumer, &[(0, stuck_since)])
        .await
        .unwrap();

    // Below the threshold: honoured, deletes nothing.
    let honoured = r.trimmer.trim_topic_once(&topic).await.unwrap();
    let h = &honoured.partitions[0];
    assert!(!h.clamp_released);
    assert_eq!(h.held_by.as_deref(), Some(consumer.as_str()));
    assert_eq!(h.min_id, stuck_since);
    assert_eq!(h.removed, 0);
    assert_eq!(len(&r.conn, &key).await, 100);
    assert_eq!(first_id(&r.conn, &key).await, Some(first));
    assert_eq!(r.trimmer.released_total(), 0);

    // Over the release point but under MaxLen.
    xadd_auto(&r.conn, &key, 800).await;
    let at_threshold = len(&r.conn, &key).await;
    assert!((RELEASE_AT..i64::try_from(MAX_LEN).unwrap()).contains(&at_threshold));

    let sacrificed = r.trimmer.trim_topic_once(&topic).await.unwrap();
    let s = &sacrificed.partitions[0];
    assert!(s.clamp_released);
    assert_eq!(s.held_by.as_deref(), Some(consumer.as_str()));
    assert_eq!(s.clamp, stuck_since);
    assert_eq!(s.length, at_threshold);
    assert!(
        s.cutoff < first,
        "MINID alone deletes nothing in this state"
    );

    let after = len(&r.conn, &key).await;
    assert!(after < at_threshold);
    assert!(after <= RELEASE_AT);
    assert_eq!(s.removed, u64::try_from(at_threshold - after).unwrap());
    let head = first_id(&r.conn, &key).await.unwrap();
    assert!(
        head > first,
        "the stuck consumer's unprocessed backlog is gone"
    );
    assert_eq!(s.min_id, head);
    assert_eq!(r.trimmer.released_total(), 1, "once per release");

    // Still released next sweep: one incident, not one per sweep.
    xadd_auto(&r.conn, &key, 200).await;
    let still = r.trimmer.trim_topic_once(&topic).await.unwrap();
    assert!(still.partitions[0].clamp_released);
    assert_eq!(r.trimmer.released_total(), 1);

    // Back under the line, then over again: a second incident.
    let recovered = r.trimmer.trim_topic_once(&topic).await.unwrap();
    assert!(!recovered.partitions[0].clamp_released);
    xadd_auto(&r.conn, &key, 200).await;
    r.trimmer.trim_topic_once(&topic).await.unwrap();
    assert_eq!(r.trimmer.released_total(), 2);
}

#[tokio::test]
async fn a_lagging_consumers_unread_entries_survive_trimming() {
    let redis = require_redis!();
    let topic = redis.new_topic("trim-lag");
    for mode in [TrimMode::Exact, TrimMode::Approx] {
        let options = topic_options(100_000, mode, 60);
        let r = rig(redis, &format!("{topic}{mode:?}"), &options).await;
        let t = format!("{topic}{mode:?}");
        let key = r.keys.stream(&t, 0, true);
        let ids = write_dated(&r.conn, &key, hours_ago_ms(2), 500).await;
        let laggard = redis.new_consumer("lag");
        let current = redis.new_consumer("cur");
        r.store.save(&t, &laggard, &[(0, ids[100])]).await.unwrap();
        r.store.save(&t, &current, &[(0, ids[499])]).await.unwrap();
        // Positions meta and an unrelated key must not count as consumers.
        assert!(r.keys.positions_meta(&t, &laggard).ends_with(":meta"));

        r.trimmer.trim_topic_once(&t).await.unwrap();
        r.trimmer.trim_topic_once(&t).await.unwrap();

        let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(&key)
            .arg(ids[100].to_string())
            .arg("+")
            .query_async(&mut r.conn.shared())
            .await
            .unwrap();
        assert_eq!(
            reply.ids.len(),
            400,
            "{mode:?}: everything from the laggard's position on survives"
        );
    }
}

#[tokio::test]
async fn a_consumer_with_no_position_for_the_partition_clamps_to_the_floor() {
    let redis = require_redis!();
    let topic = redis.new_topic("trim-nofield");
    let mut options = topic_options(100_000, TrimMode::Exact, 60);
    options.partitions = 2;
    let r = rig(redis, &topic, &options).await;
    let k0 = r.keys.stream(&topic, 0, true);
    let k1 = r.keys.stream(&topic, 1, true);
    let ids0 = write_dated(&r.conn, &k0, hours_ago_ms(2), 50).await;
    write_dated(&r.conn, &k1, hours_ago_ms(2), 50).await;
    let c = redis.new_consumer("partial");
    r.store.save(&topic, &c, &[(0, ids0[49])]).await.unwrap();

    let result = r.trimmer.trim_topic_once(&topic).await.unwrap();
    assert_eq!(result.partitions[0].clamp, ids0[49]);
    assert_eq!(result.partitions[1].clamp, StreamId::MIN);
    assert_eq!(result.partitions[1].removed, 0);
    assert_eq!(
        len(&r.conn, &k1).await,
        50,
        "nothing processed on partition 1, nothing trimmed"
    );
    assert_eq!(len(&r.conn, &k0).await, 1);
}

#[tokio::test]
async fn trim_mode_none_and_missing_retention_delete_nothing() {
    let redis = require_redis!();
    let topic = redis.new_topic("trim-none");
    let r = rig(redis, &topic, &topic_options(100, TrimMode::None, 60)).await;
    let key = r.keys.stream(&topic, 0, true);
    write_dated(&r.conn, &key, hours_ago_ms(2), 20).await;
    let result = r.trimmer.trim_topic_once(&topic).await.unwrap();
    assert_eq!(result.removed(), 0);
    assert_eq!(len(&r.conn, &key).await, 20);

    let no_retention = TopicOptions::default();
    let r2 = rig(redis, &topic, &no_retention).await;
    assert!(matches!(
        r2.trimmer.trim_topic_once(&topic).await,
        Err(Error::Config(_))
    ));
    assert_eq!(len(&r2.conn, &key).await, 20);
}

#[test]
fn release_arithmetic_matches_csharp() {
    let mut o = TopicOptions {
        max_len: Some(1_000),
        ..TopicOptions::default()
    };
    assert_eq!(release_at(&o), 800);
    o.clamp_release_threshold = 2.0;
    assert_eq!(release_at(&o), 1_000);
    o.clamp_release_threshold = 0.0;
    assert_eq!(release_at(&o), 0);
    o.clamp_release_threshold = f64::NAN;
    assert_eq!(release_at(&o), 0);
    assert_eq!(release_target(1_000, 800, 900), 700);
    assert_eq!(release_target(1_000, 1_000, 1_000), 999);
    assert_eq!(release_target(10, 8, 1), 1);
}

#[test]
fn clamp_is_the_minimum_over_consumers() {
    let rec = |ms| PositionRecord {
        id: StreamId::new(ms, 0),
        instance_id: uuid::Uuid::nil(),
    };
    let a = ConsumerPositions {
        consumer: "a".into(),
        positions: BTreeMap::from([(0, rec(50))]),
    };
    let b = ConsumerPositions {
        consumer: "b".into(),
        positions: BTreeMap::from([(0, rec(20))]),
    };
    assert_eq!(
        clamp_for(&[a, b], 0),
        (StreamId::new(20, 0), Some("b".to_owned()))
    );
    assert_eq!(clamp_for(&[], 0), (StreamId::MIN, None));
}
