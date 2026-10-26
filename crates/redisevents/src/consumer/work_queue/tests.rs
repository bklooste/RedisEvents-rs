//! Tests of the work-queue fetch.
//!
//! Unit tests: ports of `ConsumerGroupTests.cs` (the `PendingAcks` rule) plus reply parsing and the
//! member-name rule. Redis tests (driven at the fetch seam, as C# does, because the claim threshold
//! is 30 s through the host): ports of `ConsumerGroupServiceTests.cs` and S18b
//! (`S18_XAutoClaim_recovers_a_partition_abandoned_mid_batch`), plus the Rust additions (own pending
//! re-read on restart, deleted pending entries, blocking multi-stream reads). S18a and the
//! host-level cases live in `tests/host_work_queue.rs`.
//!
//! Not ported: the log assertion of `A_destroyed_group_is_recreated…` (`CapturingLog`; the warning
//! is emitted through `tracing` and the behaviour it reports is asserted instead).

#![allow(clippy::assert_is_empty)]

use std::sync::Arc;
use std::time::Duration;

use redis::Value;

use super::*;
use crate::wire::{CODEC_VERSION, EntryFields, HeaderBlock, Keys};

fn id(ms: u64, seq: u64) -> StreamId {
    StreamId::new(ms, seq)
}

/// `count` consecutive ids starting at `first_ms-0` (C# `Batch`).
fn batch_ids(first_ms: u64, count: u64) -> Vec<StreamId> {
    (0..count).map(|i| id(first_ms + i, 0)).collect()
}

fn with_batches(batches: &[&Vec<StreamId>]) -> PendingAcks {
    let pending = PendingAcks::default();
    for b in batches {
        assert_eq!(pending.add((*b).clone()), 0);
    }
    pending
}

// ---------------------------------------------------------------------------------- PendingAcks

#[test]
fn a_claimed_batch_with_older_ids_is_not_acknowledged_by_the_live_batch_ahead_of_it() {
    let live = batch_ids(500, 3);
    let claimed = batch_ids(5, 2);
    let pending = with_batches(&[&live, &claimed]);
    assert_eq!(pending.take(*live.last().unwrap()), Some(live.clone()));
    assert_eq!(pending.len(), 1, "the claimed batch waits for its own turn");
    assert_eq!(pending.take(*claimed.last().unwrap()), Some(claimed));
    assert_eq!(pending.len(), 0);
}

#[test]
fn one_batch_is_acknowledged_by_its_own_last_id() {
    let only = batch_ids(10, 4);
    let pending = with_batches(&[&only]);
    assert_eq!(pending.take(*only.last().unwrap()), Some(only));
    assert_eq!(pending.len(), 0);
}

#[test]
fn an_id_inside_an_unfinished_batch_acknowledges_only_the_batches_ahead_of_it() {
    let first = batch_ids(100, 2);
    let second = batch_ids(200, 3);
    let pending = with_batches(&[&first, &second]);
    assert_eq!(pending.take(id(201, 0)), Some(first));
    assert_eq!(
        pending.len(),
        1,
        "the batch still being processed keeps its ids"
    );
}

#[test]
fn an_unknown_id_acknowledges_nothing() {
    let batch = batch_ids(100, 2);
    let pending = with_batches(&[&batch]);
    assert_eq!(pending.take(id(99, 0)), None, "older than anything held");
    assert_eq!(pending.take(id(4_000, 0)), None, "never fetched here");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending.take(*batch.last().unwrap()), Some(batch));
}

#[test]
fn an_unacknowledged_batch_ahead_is_joined_into_the_next_acknowledgement() {
    let first = batch_ids(100, 2);
    let second = batch_ids(200, 2);
    let third = batch_ids(300, 1);
    let pending = with_batches(&[&first, &second, &third]);
    let acked = pending.take(*second.last().unwrap()).unwrap();
    assert_eq!(acked, [first[0], first[1], second[0], second[1]]);
    assert_eq!(pending.len(), 1);
}

#[test]
fn clear_forgets_every_pending_batch() {
    let batch = batch_ids(100, 2);
    let pending = with_batches(&[&batch]);
    pending.clear();
    assert_eq!(pending.len(), 0);
    assert_eq!(pending.take(*batch.last().unwrap()), None);
}

#[test]
fn the_queue_is_bounded_and_reports_what_it_dropped() {
    let pending = PendingAcks::default();
    let mut dropped = 0;
    for i in 0..PendingAcks::MAX_PENDING_BATCHES + 3 {
        dropped += pending.add(batch_ids((i as u64 + 1) * 10, 1));
    }
    assert_eq!(dropped, 3);
    assert_eq!(pending.len(), PendingAcks::MAX_PENDING_BATCHES);
    assert_eq!(
        pending.take(id(10, 0)),
        None,
        "the oldest were dropped, not acknowledged"
    );
}

#[test]
fn the_reader_and_the_processor_may_use_the_queue_at_the_same_time() {
    const BATCHES: u64 = 10_000;
    const IN_FLIGHT_CAP: usize = 32;
    let pending = Arc::new(PendingAcks::default());
    let reader = {
        let pending = pending.clone();
        std::thread::spawn(move || {
            let mut dropped = 0;
            for i in 0..BATCHES {
                while pending.len() >= IN_FLIGHT_CAP {
                    std::thread::yield_now();
                }
                dropped += pending.add(batch_ids((i + 1) * 10, 2));
            }
            dropped
        })
    };
    let processor = {
        let pending = pending.clone();
        std::thread::spawn(move || {
            let mut acked = Vec::new();
            for i in 0..BATCHES {
                let last = id((i + 1) * 10 + 1, 0);
                loop {
                    if let Some(ids) = pending.take(last) {
                        acked.extend(ids);
                        break;
                    }
                    std::thread::yield_now();
                }
            }
            acked
        })
    };
    assert_eq!(reader.join().unwrap(), 0);
    let acked = processor.join().unwrap();
    assert_eq!(acked.len() as u64, BATCHES * 2);
    let unique: std::collections::HashSet<_> = acked.iter().collect();
    assert_eq!(
        unique.len(),
        acked.len(),
        "every id acknowledged exactly once"
    );
    assert_eq!(pending.len(), 0);
}

#[test]
fn a_failed_ack_puts_its_batches_back_in_order() {
    let first = batch_ids(100, 2);
    let second = batch_ids(200, 2);
    let third = batch_ids(300, 1);
    let pending = with_batches(&[&first, &second, &third]);
    let taken = pending.take_segments(*second.last().unwrap());
    assert_eq!(taken.len(), 2);
    pending.restore(taken);
    assert_eq!(pending.len(), 3);
    assert_eq!(pending.take(*first.last().unwrap()), Some(first));
    assert_eq!(
        pending.take(*third.last().unwrap()),
        Some([second, third].concat())
    );
}

#[test]
fn contains_sees_every_held_id() {
    let pending = with_batches(&[&batch_ids(100, 3)]);
    assert!(pending.contains(id(101, 0)));
    assert!(!pending.contains(id(103, 0)));
}

// ---------------------------------------------------------------------------------- parsing

fn bulk(s: &str) -> Value {
    Value::BulkString(s.as_bytes().to_vec())
}

fn entry(eid: &str, fields: &[(&str, &str)]) -> Value {
    Value::Array(vec![
        bulk(eid),
        Value::Array(
            fields
                .iter()
                .flat_map(|(k, v)| [bulk(k), bulk(v)])
                .collect(),
        ),
    ])
}

#[test]
fn parse_claim_reads_cursor_entries_and_ignores_the_deleted_list() {
    let reply = Value::Array(vec![
        bulk("1700-3"),
        Value::Array(vec![
            entry("5-0", &[("b", "x")]),
            entry("6-1", &[("b", "y")]),
        ]),
        Value::Array(vec![bulk("4-0")]),
    ]);
    let c = parse_claim(reply).unwrap();
    assert_eq!(c.cursor, "1700-3");
    assert_eq!(
        c.entries.iter().map(|e| e.id).collect::<Vec<_>>(),
        [id(5, 0), id(6, 1)]
    );
}

#[test]
fn parse_claim_accepts_the_redis_6_2_shape_with_nil_entries() {
    let reply = Value::Array(vec![
        bulk("0-0"),
        Value::Array(vec![Value::Nil, Value::Nil]),
    ]);
    let c = parse_claim(reply).unwrap();
    assert_eq!(c.cursor, "0-0");
    assert!(c.entries.is_empty());
}

#[test]
fn parse_claim_refuses_other_shapes() {
    assert!(parse_claim(Value::Nil).is_err());
    assert!(parse_claim(Value::Array(vec![bulk("0-0")])).is_err());
    assert!(parse_claim(Value::Array(vec![bulk("nope"), Value::Array(vec![])])).is_err());
}

#[test]
fn pending_consumers_reads_the_xpending_summary() {
    let summary = Value::Array(vec![
        Value::Int(3),
        bulk("1-0"),
        bulk("3-0"),
        Value::Array(vec![
            Value::Array(vec![bulk("pod-a"), bulk("2")]),
            Value::Array(vec![bulk("pod-b"), bulk("1")]),
        ]),
    ]);
    assert_eq!(pending_consumers(summary), ["pod-a", "pod-b"]);
    let empty = Value::Array(vec![Value::Int(0), Value::Nil, Value::Nil, Value::Nil]);
    assert!(pending_consumers(empty).is_empty());
}

#[test]
fn the_member_name_is_explicit_then_pod_then_host() {
    assert_eq!(member_name(Some("m1"), Some("pod-0")), "m1");
    assert_eq!(member_name(Some("  "), Some("pod-0")), "pod-0");
    assert_eq!(member_name(None, None), machine_name());
    assert!(!machine_name().is_empty());
    assert_eq!(
        short_host("web-1.cluster.local\n").as_deref(),
        Some("web-1")
    );
    assert_eq!(short_host(" \n"), None);
}

#[test]
fn the_group_is_created_at_dollar_only_for_now() {
    assert_eq!(start_arg(StartPosition::Now), "$");
    assert_eq!(start_arg(StartPosition::BEGINNING), "0-0");
    assert_eq!(start_arg(StartPosition::After(id(12, 3))), "12-3");
}

#[test]
fn claim_interval_zero_is_the_idle_cap() {
    let s = GroupSettings::new(
        "g".into(),
        "m".into(),
        StartPosition::BEGINNING,
        0,
        CODEC_VERSION,
        50,
        &WorkQueueOptions::default(),
    );
    assert_eq!(s.claim_interval, Duration::from_millis(50));
    assert_eq!(s.claim_min_idle, Duration::from_secs(30));
    assert_eq!(s.batch_size, 1);
}

// ---------------------------------------------------------------------------------- Redis

const CLAIM_MIN_IDLE: Duration = Duration::from_millis(300);

fn keys() -> Keys {
    Keys::with_prefix("test:wq:")
}

async fn connect(fixture: &redisevents_test_support::RedisFixture) -> Connection {
    Connection::connect(fixture.url()).await.expect("connect")
}

async fn xadd(conn: &Connection, key: &str, body: &str) -> StreamId {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(key).arg("*");
    EntryFields {
        body: body.as_bytes(),
        msg_type: "test.event",
        partition_key: Some("k"),
        correlation_id: None,
        trace_parent: None,
        headers: &HeaderBlock::empty(),
    }
    .write_to(&mut cmd);
    let id: String = cmd.query_async(&mut conn.shared()).await.expect("XADD");
    StreamId::parse(&id).unwrap()
}

fn settings(group: &str, member: &str) -> GroupSettings {
    GroupSettings::new(
        group.to_owned(),
        member.to_owned(),
        StartPosition::BEGINNING,
        10,
        CODEC_VERSION,
        10,
        &WorkQueueOptions::new()
            .claim_min_idle(CLAIM_MIN_IDLE)
            .claim_interval(Duration::from_millis(10)),
    )
}

/// A polling member over one stream (partition 0), its group ensured.
async fn member(conn: &Connection, key: &str, group: &str, name: &str) -> WorkQueueFetch {
    let mut f = WorkQueueFetch::poll(conn, vec![(0, key.to_owned())], settings(group, name))
        .unwrap()
        .without_history();
    f.ensure_groups().await.unwrap();
    f
}

async fn fetch_until_some(f: &mut WorkQueueFetch, what: &str) -> StreamBatch {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut got = f.fetch().await.unwrap();
        if let Some(b) = got.pop() {
            return b;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
    }
}

fn ids_of(b: &StreamBatch) -> Vec<StreamId> {
    b.msgs.iter().map(StreamMsg::id).collect()
}

async fn ack(f: &WorkQueueFetch, b: &StreamBatch) {
    let acker = f.acker();
    acker.record(b.partition, b.last);
    acker.flush().await.unwrap();
}

/// `[(id, consumer)]` of the group's PEL.
async fn pel(conn: &Connection, key: &str, group: &str) -> Vec<(StreamId, String)> {
    let rows: Vec<(String, String, u64, u64)> = redis::cmd("XPENDING")
        .arg(key)
        .arg(group)
        .arg("-")
        .arg("+")
        .arg(1000)
        .query_async(&mut conn.shared())
        .await
        .unwrap();
    rows.into_iter()
        .map(|(i, c, _, _)| (StreamId::parse(&i).unwrap(), c))
        .collect()
}

use crate::wire::StreamMsg;

/// S18b: `XAUTOCLAIM` recovers entries an instance was delivered and then abandoned mid-batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s18_xautoclaim_recovers_a_partition_abandoned_mid_batch() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("s18b"), 0, false);
    let group = fixture.new_consumer("s18b");
    let mut dying = member(&conn, &key, &group, "instance-a").await;
    let mut published = Vec::new();
    for i in 0..5 {
        published.push(xadd(&conn, &key, &format!("m{i}")).await);
    }
    let delivered = fetch_until_some(&mut dying, "the first delivery").await;
    assert_eq!(
        delivered.msgs.len(),
        5,
        "the first instance was handed the whole batch"
    );
    // ...and it dies without acknowledging any of it.
    assert_eq!(pel(&conn, &key, &group).await.len(), 5);
    drop(dying);

    let mut survivor = member(&conn, &key, &group, "instance-b").await;
    let claimed = fetch_until_some(&mut survivor, "the survivor's claim").await;
    assert_eq!(
        ids_of(&claimed),
        published,
        "moved unchanged into the survivor's PEL"
    );
    let after = pel(&conn, &key, &group).await;
    assert!(
        after.iter().all(|(_, c)| c == "instance-b") && after.len() == 5,
        "{after:?}"
    );

    ack(&survivor, &claimed).await;
    assert!(pel(&conn, &key, &group).await.is_empty());
}

/// R-07(b) end to end: acknowledging a live batch leaves a later, older claimed batch pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledging_a_live_batch_leaves_a_later_claimed_batch_pending() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("r07b"), 0, false);
    let group = fixture.new_consumer("r07b");
    let mut dying = member(&conn, &key, &group, "instance-a").await;
    let old = [
        xadd(&conn, &key, "old-0").await,
        xadd(&conn, &key, "old-1").await,
    ];
    assert_eq!(fetch_until_some(&mut dying, "old").await.msgs.len(), 2);
    let fresh = [
        xadd(&conn, &key, "new-0").await,
        xadd(&conn, &key, "new-1").await,
    ];

    let mut survivor = member(&conn, &key, &group, "instance-b").await;
    let live = fetch_until_some(&mut survivor, "live").await;
    assert_eq!(ids_of(&live), fresh);
    let pending = survivor.pending(0).unwrap();
    assert_eq!(pending.len(), 1);

    let claimed = fetch_until_some(&mut survivor, "the claim").await;
    // Rust leaves out the survivor's own in-flight live entries (they are already queued).
    assert_eq!(
        ids_of(&claimed),
        old,
        "the sweep recovers the abandoned entries only"
    );
    assert_eq!(
        pending.len(),
        2,
        "the claimed batch is queued behind the live one"
    );

    ack(&survivor, &live).await;
    let after: Vec<StreamId> = pel(&conn, &key, &group)
        .await
        .into_iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(
        after, old,
        "only the batch the handler processed may be acknowledged"
    );
    assert_eq!(pending.len(), 1);

    ack(&survivor, &claimed).await;
    assert!(pel(&conn, &key, &group).await.is_empty());
    assert_eq!(pending.len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_destroyed_group_is_recreated_and_the_stale_pending_batches_are_dropped() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("nogroup"), 0, false);
    let group = fixture.new_consumer("nogroup");
    let mut m = member(&conn, &key, &group, "instance-a").await;
    xadd(&conn, &key, "m0").await;
    xadd(&conn, &key, "m1").await;
    assert_eq!(fetch_until_some(&mut m, "first").await.msgs.len(), 2);
    let pending = m.pending(0).unwrap();
    assert_eq!(pending.len(), 1);

    let destroyed: i64 = redis::cmd("XGROUP")
        .arg("DESTROY")
        .arg(&key)
        .arg(&group)
        .query_async(&mut conn.shared())
        .await
        .unwrap();
    assert_eq!(destroyed, 1);
    xadd(&conn, &key, "m2").await;

    let redelivered = fetch_until_some(&mut m, "the recreated group's redelivery").await;
    assert_eq!(
        redelivered.msgs.len(),
        3,
        "recreated at the configured start"
    );
    assert_eq!(
        pending.len(),
        1,
        "the batch held for the destroyed group was dropped"
    );
    ack(&m, &redelivered).await;
    assert!(pel(&conn, &key, &group).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resetting_a_group_rewinds_it_and_purges_the_pending_entries() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("reset"), 0, false);
    let group = fixture.new_consumer("reset");
    let mut m = member(&conn, &key, &group, "instance-a").await;
    let mut ids = Vec::new();
    for i in 0..5 {
        ids.push(xadd(&conn, &key, &format!("m{i}")).await);
    }
    assert_eq!(fetch_until_some(&mut m, "first").await.msgs.len(), 5);
    assert_eq!(pel(&conn, &key, &group).await.len(), 5);

    m.reset_to(&conn, StreamId::MIN, true).await.unwrap();
    assert!(
        pel(&conn, &key, &group).await.is_empty(),
        "consumers deleted with their entries"
    );
    assert_eq!(m.pending(0).unwrap().len(), 0);

    let replayed = fetch_until_some(&mut m, "replay").await;
    assert_eq!(
        ids_of(&replayed),
        ids,
        "a rewind to 0-0 redelivers the retained stream"
    );
    ack(&m, &replayed).await;

    m.reset_to(&conn, *ids.last().unwrap(), true).await.unwrap();
    assert!(
        m.fetch().await.unwrap().is_empty(),
        "reading resumes after the id set"
    );
    let after = xadd(&conn, &key, "after-the-reset").await;
    let tail = fetch_until_some(&mut m, "tail").await;
    assert_eq!(ids_of(&tail), [after]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resetting_a_group_that_does_not_exist_creates_it_at_the_target() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("reset-new"), 0, false);
    let group = fixture.new_consumer("reset-new");
    xadd(&conn, &key, "before").await;
    set_group_position(&conn, &key, &group, StreamId::MIN, true)
        .await
        .unwrap();
    let mut m = WorkQueueFetch::poll(&conn, vec![(0, key.clone())], settings(&group, "a")).unwrap();
    assert_eq!(
        fetch_until_some(&mut m, "the existing entry")
            .await
            .msgs
            .len(),
        1
    );
}

/// Rust addition: a restarted member re-reads its own pending entries first, at once, without
/// waiting out the claim threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_member_redelivers_its_own_pending_entries_first() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("own"), 0, false);
    let group = fixture.new_consumer("own");
    let mut first = member(&conn, &key, &group, "pod-a").await;
    let mut ids = Vec::new();
    for i in 0..15 {
        ids.push(xadd(&conn, &key, &format!("m{i}")).await);
    }
    // Two batches of 10 + 5 delivered, none acknowledged: then a restart.
    assert_eq!(fetch_until_some(&mut first, "a").await.msgs.len(), 10);
    assert_eq!(fetch_until_some(&mut first, "b").await.msgs.len(), 5);
    drop(first);
    let late = xadd(&conn, &key, "late").await;

    let mut wq = WorkQueueOptions::new().claim_min_idle(Duration::from_secs(60));
    wq.claim_interval_ms = 10;
    let s = GroupSettings::new(
        group.clone(),
        "pod-a".into(),
        StartPosition::BEGINNING,
        10,
        CODEC_VERSION,
        10,
        &wq,
    );
    let mut again = WorkQueueFetch::poll(&conn, vec![(0, key.clone())], s).unwrap();
    let h1 = fetch_until_some(&mut again, "history page 1").await;
    let h2 = fetch_until_some(&mut again, "history page 2").await;
    assert_eq!(
        [ids_of(&h1), ids_of(&h2)].concat(),
        ids,
        "own PEL first, paged, in order"
    );
    let live = fetch_until_some(&mut again, "live").await;
    assert_eq!(ids_of(&live), [late]);
    for b in [&h1, &h2, &live] {
        ack(&again, b).await;
    }
    assert!(pel(&conn, &key, &group).await.is_empty());
}

/// A pending entry deleted from the stream (XDEL / trimming) cannot be handled: it is acked and
/// skipped, never handed to the decoder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_entry_deleted_from_the_stream_is_acked_and_skipped() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("gone"), 0, false);
    let group = fixture.new_consumer("gone");
    let mut first = member(&conn, &key, &group, "pod-a").await;
    let a = xadd(&conn, &key, "a").await;
    let b = xadd(&conn, &key, "b").await;
    assert_eq!(fetch_until_some(&mut first, "both").await.msgs.len(), 2);
    drop(first);
    let _: i64 = redis::cmd("XDEL")
        .arg(&key)
        .arg(a.to_string())
        .query_async(&mut conn.shared())
        .await
        .unwrap();
    let mut again =
        WorkQueueFetch::poll(&conn, vec![(0, key.clone())], settings(&group, "pod-a")).unwrap();
    let h = fetch_until_some(&mut again, "history").await;
    assert_eq!(ids_of(&h), [b]);
    let left: Vec<StreamId> = pel(&conn, &key, &group)
        .await
        .into_iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(left, [b], "the deleted entry was acked");
}

/// Block mode: one multi-stream `XREADGROUP BLOCK` over co-located partitions on the reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocking_member_reads_several_streams_in_one_call() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let topic = fixture.new_topic("block");
    let k0 = keys().stream(&topic, 0, true);
    let k1 = keys().stream(&topic, 1, true);
    let group = fixture.new_consumer("block");
    let reader = ReaderConnection::new(&conn, 200).unwrap();
    let mut m = WorkQueueFetch::block(
        &conn,
        reader,
        vec![(0, k0.clone()), (1, k1.clone())],
        settings(&group, "pod-a"),
    )
    .unwrap();
    m.ensure_groups().await.unwrap();
    assert!(
        m.fetch().await.unwrap().is_empty(),
        "the block expires with nothing"
    );
    let a = xadd(&conn, &k0, "a").await;
    let b = xadd(&conn, &k1, "b").await;
    let mut got = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while got.len() < 2 {
        for batch in m.fetch().await.unwrap() {
            got.push((batch.partition, ids_of(&batch)));
            ack(&m, &batch).await;
        }
        assert!(tokio::time::Instant::now() < deadline);
    }
    got.sort();
    assert_eq!(got, [(0, vec![a]), (1, vec![b])]);
    assert!(pel(&conn, &k0, &group).await.is_empty() && pel(&conn, &k1, &group).await.is_empty());
}

/// An id reported mid-batch (a `Block` at message i of a per-message handler) acks nothing; the
/// batch's own last id then acks all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_progress_acknowledges_nothing_until_the_batch_is_done() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("partial"), 0, false);
    let group = fixture.new_consumer("partial");
    let mut m = member(&conn, &key, &group, "pod-a").await;
    for i in 0..3 {
        xadd(&conn, &key, &format!("m{i}")).await;
    }
    let b = fetch_until_some(&mut m, "batch").await;
    let acker = m.acker();
    acker.record(0, b.msgs[1].id());
    acker.flush().await.unwrap();
    assert_eq!(pel(&conn, &key, &group).await.len(), 3);
    acker.record(0, b.last);
    acker.flush().await.unwrap();
    assert!(pel(&conn, &key, &group).await.is_empty());
}
