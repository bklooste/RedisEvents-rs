//! Integration tests for the outbox, state store, idempotency and forwarder (B3) against a real
//! Redis. Ports of `StreamStoreTests.cs`, `TopicForwarderTests.cs`, the Redis-facing parts of
//! `OutboxTransactionTests.cs` and `IdempotencyTests.cs`, named after the C# test in `snake_case`.
//! Skips without Redis (`require_redis!`).

#![allow(
    clippy::assert_is_empty,
    clippy::similar_names,
    reason = "test assertions read better this way"
)]

use std::time::Duration;

use bytes::Bytes;
use redis::aio::ConnectionManager;
use redisevents::config::{TopicOptions, TrimMode};
use redisevents::producer::{
    ForwardedMessage, Idempotency, Outbox, OutboxPublish, PublishOptions, StateEvent, StreamStore,
    TopicForwarder,
};
use redisevents::wire::codec::{CODEC_VERSION, decode};
use redisevents::wire::{StreamId, StreamMsg};
use redisevents::{Error, Publisher};
use redisevents_test_support::RedisFixture;

const NAME: &str = "es:Inventory:42";
const AGGREGATE_ID: &str = "42";
const TRACE: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn options() -> TopicOptions {
    TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    }
}

fn event(body: &str, msg_type: &str) -> StateEvent {
    StateEvent::new(Bytes::copy_from_slice(body.as_bytes()), msg_type)
}

fn with_metadata() -> PublishOptions {
    PublishOptions::new()
        .correlation_id("corr-0f4a1c")
        .header("es-version", "1")
        .trace_parent(TRACE)
}

async fn raw(redis: &RedisFixture) -> ConnectionManager {
    redis.connect().await.shared()
}

async fn xlen(conn: &mut ConnectionManager, key: &str) -> u64 {
    redis::cmd("XLEN")
        .arg(key)
        .query_async(conn)
        .await
        .expect("XLEN")
}

/// Every entry of `key`: decoded, plus the raw field names.
async fn entries(conn: &mut ConnectionManager, key: &str) -> Vec<(StreamMsg, Vec<String>)> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .query_async(conn)
        .await
        .expect("XRANGE");
    reply
        .ids
        .into_iter()
        .map(|e| {
            let id = StreamId::parse(&e.id).expect("id");
            let mut names: Vec<String> = Vec::new();
            let mut pairs = Vec::new();
            for (k, v) in e.map {
                names.push(k.clone());
                pairs.push((
                    k,
                    Bytes::from(redis::from_redis_value::<Vec<u8>>(v).expect("bytes")),
                ));
            }
            // XRANGE preserves field order; the map does not, so sort for stable assertions.
            names.sort();
            (decode(id, 0, pairs, CODEC_VERSION).expect("decode"), names)
        })
        .collect()
}

fn store(
    redis: &RedisFixture,
    conn: redisevents::Connection,
    topic: &str,
    o: TopicOptions,
) -> StreamStore {
    StreamStore::new(conn, redis.keys(), topic, o).expect("store")
}

fn text(m: &StreamMsg) -> String {
    String::from_utf8(m.body().to_vec()).expect("utf8")
}

// ------------------------------------------------------------------------------------------------
// StreamStoreTests
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn append_then_read_round_trips_in_order() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let s = store(redis, redis.connect().await, &topic, options());
    let events = [
        event("created", "inventory.created"),
        event("renamed — ünicode, and a \0 NUL byte", "inventory.renamed"),
        event("deactivated", "inventory.deactivated"),
    ];
    let ids = s
        .append_and_publish(NAME, 0, AGGREGATE_ID, &events)
        .await
        .unwrap()
        .expect("appended");
    assert_eq!(ids.len(), 3);
    assert!(ids.windows(2).all(|w| w[0] < w[1]));

    let read = s.read(NAME, StreamId::MIN, 100).await.unwrap();
    let types: Vec<&str> = read.iter().map(StreamMsg::msg_type).collect();
    assert_eq!(
        types,
        [
            "inventory.created",
            "inventory.renamed",
            "inventory.deactivated"
        ]
    );
    let bodies: Vec<String> = read.iter().map(text).collect();
    assert_eq!(
        bodies,
        [
            "created",
            "renamed — ünicode, and a \0 NUL byte",
            "deactivated"
        ]
    );
    assert!(read.iter().all(|m| m.partition_key().is_empty()));
    assert_eq!(read.iter().map(StreamMsg::id).collect::<Vec<_>>(), ids);

    let mut conn = raw(redis).await;
    assert_eq!(xlen(&mut conn, &redis.keys().state(&topic, NAME)).await, 3);
}

#[tokio::test]
async fn appended_events_are_published_to_the_topic_in_order() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let s = store(redis, redis.connect().await, &topic, options());
    s.append_and_publish(
        NAME,
        0,
        AGGREGATE_ID,
        &[event("one", "t.one"), event("two", "t.two")],
    )
    .await
    .unwrap()
    .expect("appended");

    let mut conn = raw(redis).await;
    let published = entries(&mut conn, &redis.keys().stream(&topic, 0, true)).await;
    assert_eq!(published.len(), 2);
    let types: Vec<&str> = published.iter().map(|(m, _)| m.msg_type()).collect();
    assert_eq!(types, ["t.one", "t.two"]);
    let bodies: Vec<String> = published.iter().map(|(m, _)| text(m)).collect();
    assert_eq!(bodies, ["one", "two"]);
    assert!(
        published
            .iter()
            .all(|(m, _)| m.partition_key() == AGGREGATE_ID)
    );
}

#[tokio::test]
async fn length_mismatch_returns_null_and_writes_nothing() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let s = store(redis, redis.connect().await, &topic, options());
    s.append_and_publish(NAME, 0, AGGREGATE_ID, &[event("first", "t.first")])
        .await
        .unwrap()
        .expect("appended");

    let stale = s
        .append_and_publish(
            NAME,
            5,
            AGGREGATE_ID,
            &[event("stale", "t.stale"), event("also-stale", "t.stale")],
        )
        .await
        .unwrap();
    assert!(stale.is_none(), "XLEN is 1, not 5");

    let mut conn = raw(redis).await;
    assert_eq!(xlen(&mut conn, &redis.keys().state(&topic, NAME)).await, 1);
    assert_eq!(
        xlen(&mut conn, &redis.keys().stream(&topic, 0, true)).await,
        1
    );
    let state = s.read(NAME, StreamId::MIN, 10).await.unwrap();
    assert_eq!(state.len(), 1);
    assert_eq!(state[0].msg_type(), "t.first");
}

#[tokio::test]
async fn expected_length_zero_creates_once() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let s = store(redis, redis.connect().await, &topic, options());
    let created = s
        .append_and_publish(NAME, 0, AGGREGATE_ID, &[event("a", "t.a")])
        .await
        .unwrap();
    let again = s
        .append_and_publish(NAME, 0, AGGREGATE_ID, &[event("b", "t.b")])
        .await
        .unwrap();
    assert!(created.is_some());
    assert!(again.is_none());
    let mut conn = raw(redis).await;
    assert_eq!(xlen(&mut conn, &redis.keys().state(&topic, NAME)).await, 1);
}

/// Two concurrent saves at the same expected version, on separate connections: exactly one wins,
/// and the loser writes nothing to either the state stream or the topic. Repeated to give the race
/// a real chance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_appends_at_the_same_version_leave_exactly_one_winner() {
    let redis = redisevents_test_support::require_redis!();
    for round in 0..20 {
        let topic = redis.new_topic("race");
        let seeder = store(redis, redis.connect().await, &topic, options());
        seeder
            .append_and_publish(NAME, 0, AGGREGATE_ID, &[event("seed", "t.seed")])
            .await
            .unwrap()
            .expect("seeded");

        let first = store(redis, redis.connect().await, &topic, options());
        let second = store(redis, redis.connect().await, &topic, options());
        let one_events = [event("one", "t.one"), event("one-b", "t.one")];
        let two_events = [event("two", "t.two"), event("two-b", "t.two")];
        let (one, two) = tokio::join!(
            first.append_and_publish(NAME, 1, AGGREGATE_ID, &one_events),
            second.append_and_publish(NAME, 1, AGGREGATE_ID, &two_events),
        );
        let (one, two) = (one.unwrap(), two.unwrap());
        assert_eq!(
            usize::from(one.is_some()) + usize::from(two.is_some()),
            1,
            "round {round}: one writer must lose the length check"
        );
        let winner = if one.is_some() { "t.one" } else { "t.two" };

        let mut conn = raw(redis).await;
        let state_key = redis.keys().state(&topic, NAME);
        let topic_key = redis.keys().stream(&topic, 0, true);
        assert_eq!(xlen(&mut conn, &state_key).await, 3);
        assert_eq!(xlen(&mut conn, &topic_key).await, 3);
        for key in [state_key, topic_key] {
            let types: Vec<String> = entries(&mut conn, &key)
                .await
                .iter()
                .map(|(m, _)| m.msg_type().to_owned())
                .collect();
            assert_eq!(types, ["t.seed", winner, winner], "round {round}: {key}");
        }
    }
}

#[tokio::test]
async fn read_pages_exclusively_and_a_missing_stream_is_empty() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let s = store(redis, redis.connect().await, &topic, options());
    assert!(s.read(NAME, StreamId::MIN, 10).await.unwrap().is_empty());

    let events: Vec<StateEvent> = (0..5).map(|i| event(&format!("e{i}"), "t.e")).collect();
    let ids = s
        .append_and_publish(NAME, 0, AGGREGATE_ID, &events)
        .await
        .unwrap()
        .unwrap();

    let page = s.read(NAME, StreamId::MIN, 2).await.unwrap();
    assert_eq!(page.iter().map(StreamMsg::id).collect::<Vec<_>>(), ids[..2]);
    let next = s.read(NAME, page[1].id(), 2).await.unwrap();
    assert_eq!(
        next.iter().map(StreamMsg::id).collect::<Vec<_>>(),
        ids[2..4]
    );
    let last = s.read(NAME, next[1].id(), 100).await.unwrap();
    assert_eq!(last.iter().map(StreamMsg::id).collect::<Vec<_>>(), ids[4..]);
    assert!(s.read(NAME, ids[4], 100).await.unwrap().is_empty());
}

#[tokio::test]
async fn state_entries_are_body_and_type_only_while_the_topic_entry_keeps_its_metadata() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let s = store(redis, redis.connect().await, &topic, options());
    s.append_and_publish(
        NAME,
        0,
        AGGREGATE_ID,
        &[event("body", "t.one").with_options(with_metadata())],
    )
    .await
    .unwrap()
    .unwrap();

    let mut conn = raw(redis).await;
    let state = entries(&mut conn, &redis.keys().state(&topic, NAME)).await;
    assert_eq!(state.len(), 1);
    assert_eq!(state[0].1, ["b", "t"]);
    let read = s.read(NAME, StreamId::MIN, 10).await.unwrap();
    assert!(read[0].partition_key().is_empty());
    assert!(read[0].correlation_id().is_empty());
    assert!(read[0].trace_parent().is_none());
    assert!(read[0].headers().is_empty());

    let published = entries(&mut conn, &redis.keys().stream(&topic, 0, true)).await;
    let m = &published[0].0;
    assert_eq!(m.partition_key(), AGGREGATE_ID);
    assert_eq!(m.correlation_id(), "corr-0f4a1c");
    assert_eq!(m.trace_parent(), Some(TRACE));
    assert_eq!(m.headers().get("es-version").unwrap().as_deref(), Some("1"));
}

#[tokio::test]
async fn state_metadata_keeps_everything_but_the_partition_key_on_the_state_entry() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let o = TopicOptions {
        state_metadata: true,
        ..options()
    };
    let s = store(redis, redis.connect().await, &topic, o);
    s.append_and_publish_unchecked(
        NAME,
        AGGREGATE_ID,
        &[event("body", "t.one").with_options(with_metadata())],
    )
    .await
    .unwrap();

    let mut conn = raw(redis).await;
    let state = entries(&mut conn, &redis.keys().state(&topic, NAME)).await;
    assert!(!state[0].1.contains(&"k".to_owned()));
    let read = s.read(NAME, StreamId::MIN, 10).await.unwrap();
    let published = entries(&mut conn, &redis.keys().stream(&topic, 0, true)).await;
    assert!(read[0].partition_key().is_empty());
    assert_eq!(published[0].0.partition_key(), AGGREGATE_ID);
    for m in [&read[0], &published[0].0] {
        assert_eq!(m.correlation_id(), "corr-0f4a1c");
        assert_eq!(m.trace_parent(), Some(TRACE));
        assert_eq!(m.headers().get("es-version").unwrap().as_deref(), Some("1"));
    }
}

#[tokio::test]
async fn a_stream_holding_old_full_entries_and_new_lean_entries_reads_and_versions_as_one() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let s = store(redis, redis.connect().await, &topic, options());
    let key = redis.keys().state(&topic, NAME);
    let mut conn = raw(redis).await;
    let _: String = redis::cmd("XADD")
        .arg(&key)
        .arg("*")
        .arg(&["b", "old", "t", "t.old", "k", AGGREGATE_ID, "c", "corr-old"])
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        s.append_and_publish(NAME, 1, AGGREGATE_ID, &[event("new", "t.new")])
            .await
            .unwrap()
            .is_some()
    );
    let read = s.read(NAME, StreamId::MIN, 10).await.unwrap();
    assert_eq!(
        read.iter().map(StreamMsg::msg_type).collect::<Vec<_>>(),
        ["t.old", "t.new"]
    );
    assert_eq!(read.iter().map(text).collect::<Vec<_>>(), ["old", "new"]);
    assert_eq!(read[0].partition_key(), AGGREGATE_ID);
    assert_eq!(read[0].correlation_id(), "corr-old");
    assert!(read[1].partition_key().is_empty());
    assert!(
        s.append_and_publish(NAME, 1, AGGREGATE_ID, &[event("stale", "t.new")])
            .await
            .unwrap()
            .is_none(),
        "the stream is two long, whatever shape its entries are"
    );
}

#[tokio::test]
async fn append_of_no_events_is_refused() {
    let redis = redisevents_test_support::require_redis!();
    let s = store(
        redis,
        redis.connect().await,
        &redis.new_topic("store"),
        options(),
    );
    assert!(matches!(
        s.append_and_publish(NAME, 0, AGGREGATE_ID, &[]).await,
        Err(Error::InvalidArgument(_))
    ));
}

/// C# `StoreRegistrationTests.AddStreamStore_withoutCoLocatedPartitions_throwsAtRegistration`: the
/// refusal at construction.
#[tokio::test]
async fn a_store_on_a_topic_without_co_located_partitions_is_refused() {
    let redis = redisevents_test_support::require_redis!();
    let o = TopicOptions {
        partitions: 4,
        co_locate_partitions: false,
        ..TopicOptions::default()
    };
    let err = StreamStore::new(redis.connect().await, redis.keys(), "spread", o).unwrap_err();
    assert!(
        matches!(err, Error::Config(ref m) if m.contains("CoLocatePartitions is false")),
        "{err}"
    );
}

/// An outbox entry and a plain publish of one key land in one partition, and both decode the same.
#[tokio::test]
async fn a_store_append_routes_the_key_exactly_as_the_publisher_does() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("store");
    let o = TopicOptions {
        partitions: 4,
        trim: TrimMode::None,
        ..TopicOptions::default()
    };
    let s = store(redis, redis.connect().await, &topic, o.clone());
    s.append_and_publish(NAME, 0, "customer-7", &[event("a", "T")])
        .await
        .unwrap()
        .unwrap();
    let p =
        redisevents::producer::StreamPublisher::new(redis.connect().await, redis.keys(), &topic, o)
            .await
            .unwrap();
    p.publish(
        "customer-7",
        Bytes::from_static(b"b"),
        "T",
        &PublishOptions::new(),
    )
    .await
    .unwrap();
    let partition = redisevents::wire::partition_for_key(b"customer-7", 4);
    let mut conn = raw(redis).await;
    assert_eq!(
        xlen(&mut conn, &redis.keys().stream(&topic, partition, true)).await,
        2
    );
}

// ------------------------------------------------------------------------------------------------
// Outbox (Redis-facing parts of OutboxTransactionTests)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn write_and_publish_many_returns_one_id_per_publish_in_order() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("outbox");
    let keys = redis.keys();
    let outbox = Outbox::new(redis.connect().await, keys.clone());
    let o = TopicOptions {
        partitions: 4,
        ..TopicOptions::default()
    };
    let none = PublishOptions::new();
    let mk = |key: &'static str, body: &'static [u8], t: &'static str| OutboxPublish {
        topic: &topic,
        partition_key: key,
        body,
        msg_type: t,
        options: &none,
        topic_options: &o,
    };
    let publishes = [
        mk("customer-1", b"a", "BetPlaced"),
        mk("customer-2", b"b", "BetMatched"),
        mk("customer-3", b"c", "BetSettled"),
    ];
    let state_key = Outbox::state_key(&keys, &topic, "1").unwrap();
    let mut calls = 0;
    let ids = outbox
        .write_and_publish_many(
            |pipe| {
                calls += 1;
                pipe.cmd("SET").arg(&state_key).arg("v").ignore();
            },
            &publishes,
            &[&state_key],
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    assert_eq!(
        calls, 1,
        "the state closure is queued once, ahead of the publishes"
    );

    let mut conn = raw(redis).await;
    let v: String = redis::cmd("GET")
        .arg(&state_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(v, "v");
    for (id, p) in ids.iter().zip(&publishes) {
        let partition = redisevents::wire::partition_for_key(p.partition_key.as_bytes(), 4);
        let got = entries(&mut conn, &keys.stream(&topic, partition, true)).await;
        let m = got
            .iter()
            .find(|(m, _)| m.id() == *id)
            .expect("entry with the returned id");
        assert_eq!(m.0.msg_type(), p.msg_type);
        assert_eq!(m.0.partition_key(), p.partition_key);
    }
}

#[tokio::test]
async fn write_and_publish_reuses_its_dedicated_connection_across_calls() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("outbox");
    let keys = redis.keys();
    let outbox = Outbox::new(redis.connect().await, keys.clone());
    let o = options();
    let none = PublishOptions::new();
    let counter = Outbox::state_key(&keys, &topic, "count").unwrap();
    for _ in 0..3 {
        outbox
            .write_and_publish(
                |pipe| {
                    pipe.cmd("INCR").arg(&counter);
                },
                &OutboxPublish {
                    topic: &topic,
                    partition_key: "k",
                    body: b"x",
                    msg_type: "T",
                    options: &none,
                    topic_options: &o,
                },
                &[&counter],
            )
            .await
            .unwrap();
    }
    let mut conn = raw(redis).await;
    let n: u64 = redis::cmd("GET")
        .arg(&counter)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(n, 3);
    assert_eq!(xlen(&mut conn, &keys.stream(&topic, 0, true)).await, 3);
}

/// C# `A_failed_exec_is_wrapped_as_a_transport_failure_that_names_the_batch`: a queue-time error
/// (wrong arity) aborts the whole `EXEC`, so nothing is applied and the error names the batch.
#[tokio::test]
async fn a_failed_exec_is_wrapped_as_a_transport_failure_that_names_the_batch() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("outbox");
    let keys = redis.keys();
    let outbox = Outbox::new(redis.connect().await, keys.clone());
    let o = options();
    let none = PublishOptions::new();
    let err = outbox
        .write_and_publish(
            |pipe| {
                pipe.cmd("SET"); // no key, no value: rejected at queue time
            },
            &OutboxPublish {
                topic: &topic,
                partition_key: "k",
                body: b"x",
                msg_type: "T",
                options: &none,
                topic_options: &o,
            },
            &[],
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Transport { ref message, .. } if message.contains("1 message(s)") && message.contains(&topic)),
        "{err}"
    );
    let mut conn = raw(redis).await;
    assert_eq!(xlen(&mut conn, &keys.stream(&topic, 0, true)).await, 0);
}

// ------------------------------------------------------------------------------------------------
// IdempotencyTests (the Redis-facing ones)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn second_sighting_returns_false() {
    let redis = redisevents_test_support::require_redis!();
    let conn = redis.connect().await;
    let scope = redis.new_topic("dedupe");
    let ttl = Duration::from_hours(24);
    assert!(
        Idempotency::try_begin(&conn, &redis.keys(), &scope, "bet-123", ttl)
            .await
            .unwrap()
    );
    assert!(
        !Idempotency::try_begin(&conn, &redis.keys(), &scope, "bet-123", ttl)
            .await
            .unwrap()
    );

    let mut raw = conn.shared();
    let key = Idempotency::key(&redis.keys(), &scope, "bet-123").unwrap();
    let pttl: i64 = redis::cmd("PTTL")
        .arg(&key)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert!(pttl > 0 && pttl <= 24 * 3600 * 1000, "{pttl}");
}

// ------------------------------------------------------------------------------------------------
// TopicForwarderTests
// ------------------------------------------------------------------------------------------------

fn message(text: &str) -> ForwardedMessage {
    ForwardedMessage::new(
        "customer-1",
        Bytes::copy_from_slice(text.as_bytes()),
        "Forwarded",
    )
}

async fn forwarder(redis: &RedisFixture, ttl: Option<Duration>) -> (TopicForwarder, String) {
    let topic = redis.new_topic("forward");
    let o = TopicOptions {
        partitions: 1,
        trim: TrimMode::None,
        ..TopicOptions::default()
    };
    let mut f = TopicForwarder::new(redis.connect().await, redis.keys(), &topic, o).unwrap();
    if let Some(ttl) = ttl {
        f = f.with_marker_ttl(ttl);
    }
    (f, redis.keys().stream(&topic, 0, true))
}

#[tokio::test]
async fn a_forward_publishes_and_the_same_position_again_does_not() {
    let redis = redisevents_test_support::require_redis!();
    let (f, key) = forwarder(redis, None).await;
    assert!(
        f.forward("src", "tx-1", StreamId::new(5, 0), &[message("a")], None)
            .await
            .unwrap()
    );
    assert!(
        !f.forward("src", "tx-1", StreamId::new(5, 0), &[message("a")], None)
            .await
            .unwrap()
    );
    assert_eq!(xlen(&mut raw(redis).await, &key).await, 1);
}

#[tokio::test]
async fn a_replay_below_the_high_water_mark_is_skipped_even_after_its_marker_expired() {
    let redis = redisevents_test_support::require_redis!();
    let (f, key) = forwarder(redis, Some(Duration::from_millis(50))).await;
    f.forward("src", "tx-1", StreamId::new(5, 0), &[message("a")], None)
        .await
        .unwrap();
    f.forward("src", "tx-2", StreamId::new(6, 0), &[message("b")], None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !f.forward("src", "tx-1", StreamId::new(5, 0), &[message("a")], None)
            .await
            .unwrap()
    );
    assert!(
        !f.forward("src", "tx-2", StreamId::new(6, 0), &[message("b")], None)
            .await
            .unwrap()
    );
    assert_eq!(xlen(&mut raw(redis).await, &key).await, 2);
}

#[tokio::test]
async fn positions_compare_as_stream_ids_not_as_text() {
    let redis = redisevents_test_support::require_redis!();
    let (f, key) = forwarder(redis, None).await;
    f.forward("src", "tx-9", StreamId::new(9, 0), &[message("a")], None)
        .await
        .unwrap();
    assert!(
        f.forward("src", "tx-10", StreamId::new(10, 0), &[message("b")], None)
            .await
            .unwrap()
    );
    assert_eq!(xlen(&mut raw(redis).await, &key).await, 2);
}

#[tokio::test]
async fn a_marked_dedupe_id_is_skipped_at_a_new_position() {
    let redis = redisevents_test_support::require_redis!();
    let (f, key) = forwarder(redis, None).await;
    f.forward("src", "tx-1", StreamId::new(5, 0), &[message("a")], None)
        .await
        .unwrap();
    assert!(
        !f.forward("src", "tx-1", StreamId::new(7, 0), &[message("a")], None)
            .await
            .unwrap()
    );
    assert_eq!(xlen(&mut raw(redis).await, &key).await, 1);
}

#[tokio::test]
async fn each_source_keeps_its_own_high_water_mark() {
    let redis = redisevents_test_support::require_redis!();
    let (f, key) = forwarder(redis, None).await;
    f.forward(
        "wallet",
        "w:tx-1",
        StreamId::new(100, 0),
        &[message("a")],
        None,
    )
    .await
    .unwrap();
    assert!(
        f.forward(
            "payments",
            "p:tx-1",
            StreamId::new(1, 0),
            &[message("b")],
            None
        )
        .await
        .unwrap(),
        "a low position on another source is not below the wallet's mark"
    );
    assert_eq!(xlen(&mut raw(redis).await, &key).await, 2);
}

#[tokio::test]
async fn every_message_of_one_forward_is_published_in_order() {
    let redis = redisevents_test_support::require_redis!();
    let (f, key) = forwarder(redis, None).await;
    assert!(
        f.forward(
            "src",
            "tx-1",
            StreamId::new(1, 0),
            &[message("a"), message("b"), message("c")],
            Some("corr-1"),
        )
        .await
        .unwrap()
    );
    let mut conn = raw(redis).await;
    let got = entries(&mut conn, &key).await;
    assert_eq!(
        got.iter().map(|(m, _)| text(m)).collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    assert!(got.iter().all(|(m, _)| m.correlation_id() == "corr-1"));
}

#[tokio::test]
async fn an_empty_forward_is_rejected() {
    let redis = redisevents_test_support::require_redis!();
    let (f, _) = forwarder(redis, None).await;
    assert!(matches!(
        f.forward("src", "tx-1", StreamId::new(1, 0), &[], None)
            .await,
        Err(Error::InvalidArgument(_))
    ));
}
