//! Flusher tests: a 1:1 port of the flusher half of C# `PositionTests` (`Flusher_*`,
//! `Reset_Live_*`, `SecondWriter_*`) and of `WriterIdentityTests`, plus the races C3 owns. Inside
//! the crate because the flusher is crate-private; Redis tests take only the URL from
//! `redisevents-test-support`. C# used an in-memory fake multiplexer; here the Redis side is real.
//!
//! Not ported, with reasons:
//! - `Persist_AsyncBatch/SyncBatch/SyncMessage/None_*`, `Persist_SyncModeWithoutFlush_*`: they
//!   drive `PartitionWorker.ProcessLoopAsync` (when the worker records and flushes), which is C2's
//!   worker loop (C2t). The flusher side is `persist_sync_flush_through_the_sink_writes_once_per_call`.
//! - `ResetSignal_*`: `ResetSignal` is C2's (`consumer/shared.rs`), tested there.
//!   `ResetSignal_WithNoOwnedPartitions_Throws` has no equivalent: the Rust signal covers every
//!   partition and the flusher filters by ownership.
//! - `Flusher_FlushFailureFromCancellation_*`: Rust cancels by dropping the future;
//!   `flusher_flush_dropped_mid_write_requeues` is the equivalent.
//! - Log-text assertions (`log.Entries…`): the lib test binary has one global subscriber, owned by
//!   the ownership tests; these tests assert the state each log line reports instead
//!   ([`Reported`], the counters).
//! - `A_reset_issued_from_another_process_*` runs a whole consumer host (C4/C4t); its flusher half
//!   is `a_reset_issued_from_another_process_rewinds_instead_of_standing_the_partition_down` below.
#![allow(
    clippy::too_many_lines,
    clippy::unused_async_trait_impl,
    clippy::type_complexity,
    clippy::assert_is_empty
)]

use std::sync::atomic::AtomicU32;

use tokio::sync::Notify;

use super::*;
use crate::config::InstanceMode;
use crate::ownership::instance_id_for;
use crate::ownership::registry::{OwnershipRegistry, OwnershipRegistryOptions};
use crate::positions::RedisPositionStore;

const TOPIC: &str = "orders";
const CONSUMER: &str = "svc";

/// Two fixed identities, so a tiebreak assertion is not a coin toss.
const LOWER: Uuid = Uuid::from_u128(0x1111_1111_1111_4111_8111_1111_1111_1111);
const HIGHER: Uuid = Uuid::from_u128(0x2222_2222_2222_4222_8222_2222_2222_2222);

fn id(ms: u64, seq: u64) -> StreamId {
    StreamId::new(ms, seq)
}

// ------------------------------------------------------------------ counting store

#[derive(Default)]
struct StoreState {
    saved: Vec<Vec<(u32, StreamId)>>,
    calls: u32,
    fail: bool,
}

/// A store that counts writes and remembers what it was given (C# `CountingStore`).
#[derive(Clone, Default)]
struct CountingStore {
    state: Arc<Mutex<StoreState>>,
    /// When set, every save parks until notified.
    stall: Option<Arc<Notify>>,
}

impl CountingStore {
    fn writes(&self) -> usize {
        lock(&self.state).saved.len()
    }
    fn calls(&self) -> u32 {
        lock(&self.state).calls
    }
    fn last(&self) -> Option<Vec<(u32, StreamId)>> {
        lock(&self.state).saved.last().cloned()
    }
    fn everything(&self) -> Vec<(u32, StreamId)> {
        lock(&self.state).saved.concat()
    }
    fn set_fail(&self, fail: bool) {
        lock(&self.state).fail = fail;
    }
}

impl PositionStore for CountingStore {
    async fn load(
        &self,
        _topic: &str,
        _consumer: &str,
    ) -> Result<std::collections::BTreeMap<u32, StreamId>> {
        Ok(std::collections::BTreeMap::new())
    }

    async fn save(
        &self,
        _topic: &str,
        _consumer: &str,
        positions: &[(u32, StreamId)],
    ) -> Result<()> {
        if let Some(stall) = &self.stall {
            stall.notified().await;
        }
        let mut state = lock(&self.state);
        state.calls += 1;
        if state.fail {
            return Err(Error::transport("redis is down"));
        }
        if !positions.is_empty() {
            state.saved.push(positions.to_vec());
        }
        Ok(())
    }

    async fn reset(
        &self,
        _topic: &str,
        _consumer: &str,
        _to: StreamId,
        _partition: Option<u32>,
    ) -> Result<()> {
        Ok(())
    }
}

fn options(partitions: u32, interval: Duration) -> FlusherOptions {
    FlusherOptions::new(TOPIC, CONSUMER, partitions, interval, Uuid::new_v4())
}

fn new_flusher(store: &CountingStore, partitions: u32) -> PositionFlusher<CountingStore> {
    PositionFlusher::new(store.clone(), options(partitions, Duration::from_secs(300))).unwrap()
}

async fn wait(mut until: impl FnMut() -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if until() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out after 5s waiting for: {what}");
}

// ------------------------------------------------------------------ flusher: coalescing

#[tokio::test]
async fn flusher_thousand_records_coalesce_to_one_write() {
    let store = CountingStore::default();
    let flusher = new_flusher(&store, 4);
    for i in 1..=1000 {
        flusher.record(0, id(i, 0));
    }
    assert_eq!(
        flusher.dirty_count(),
        1,
        "a thousand records on one partition are one dirty slot"
    );
    flusher.flush_now().await.unwrap();
    assert_eq!(store.writes(), 1);
    assert_eq!(store.last().unwrap(), vec![(0, id(1000, 0))]);
    assert_eq!(flusher.flushes(), 1);
    assert_eq!(flusher.dirty_count(), 0);
}

#[tokio::test]
async fn flusher_second_flush_with_nothing_new_writes_nothing() {
    let store = CountingStore::default();
    let flusher = new_flusher(&store, 4);
    flusher.record(0, id(10, 0));
    flusher.flush_now().await.unwrap();
    flusher.flush_now().await.unwrap();
    assert_eq!(store.writes(), 1, "the second flush found nothing dirty");
}

#[tokio::test]
async fn flusher_only_dirty_partitions_are_written() {
    let store = CountingStore::default();
    let flusher = new_flusher(&store, 8);
    flusher.record(1, id(100, 3));
    flusher.record(5, id(200, 7));
    assert_eq!(flusher.dirty_count(), 2);
    flusher.flush_now().await.unwrap();
    assert_eq!(store.writes(), 1, "every dirty partition rides one write");
    assert_eq!(
        store.last().unwrap(),
        vec![(1, id(100, 3)), (5, id(200, 7))]
    );
}

#[tokio::test]
async fn flusher_idle_writes_nothing_at_all() {
    let store = CountingStore::default();
    let flusher =
        PositionFlusher::new(store.clone(), options(4, Duration::from_millis(15))).unwrap();
    flusher.start();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        store.writes(),
        0,
        "an idle consumer generates no position traffic"
    );
    assert_eq!(store.calls(), 0, "not even an empty save is issued");
    assert_eq!(flusher.flushes(), 0);
    flusher.stop().await;
    assert_eq!(
        store.calls(),
        0,
        "the final flush of an idle flusher is also a no-op"
    );
}

#[tokio::test]
async fn flusher_running_timer_writes_roughly_once_per_interval() {
    let store = CountingStore::default();
    let flusher =
        PositionFlusher::new(store.clone(), options(2, Duration::from_millis(100))).unwrap();
    flusher.start();
    for i in 1..=500 {
        flusher.record(0, id(i, 0));
        flusher.record(1, id(i, 0));
    }
    tokio::time::sleep(Duration::from_millis(350)).await;
    flusher.stop().await;
    assert!(store.writes() > 0);
    assert!(
        store.writes() < 10,
        "500 records over ~3 intervals must not cost 500 writes"
    );
    let everything = store.everything();
    assert!(everything.contains(&(0, id(500, 0))));
    assert!(everything.contains(&(1, id(500, 0))));
}

#[tokio::test]
async fn flusher_stop_flushes_the_last_interval() {
    let store = CountingStore::default();
    let flusher = new_flusher(&store, 2);
    flusher.start();
    flusher.record(0, id(42, 0));
    flusher.stop().await;
    assert_eq!(
        store.writes(),
        1,
        "a clean shutdown must not redeliver the last interval"
    );
    assert_eq!(store.last().unwrap(), vec![(0, id(42, 0))]);
}

// ------------------------------------------------------------------ flusher: failure handling

#[tokio::test]
async fn flusher_flush_failure_does_not_throw_and_retries_next_time() {
    let store = CountingStore::default();
    store.set_fail(true);
    let flusher = new_flusher(&store, 4);
    flusher.record(2, id(7, 0));

    // The timer path swallows it; the on-demand path reports it to the Sync* worker.
    assert!(flusher.flush_now().await.is_err());
    assert_eq!(flusher.failures(), 1);
    assert_eq!(flusher.flushes(), 0);
    assert_eq!(
        flusher.dirty_count(),
        1,
        "the drained partition is re-queued"
    );

    store.set_fail(false);
    flusher.flush_now().await.unwrap();
    assert_eq!(flusher.flushes(), 1);
    assert_eq!(store.last().unwrap(), vec![(2, id(7, 0))]);
}

#[tokio::test]
async fn flusher_flush_dropped_mid_write_requeues() {
    let stall = Arc::new(Notify::new());
    let store = CountingStore {
        stall: Some(Arc::clone(&stall)),
        ..CountingStore::default()
    };
    let flusher = new_flusher(&store, 2);
    flusher.record(0, id(3, 0));
    let dropped = tokio::time::timeout(Duration::from_millis(50), flusher.flush_now()).await;
    assert!(dropped.is_err(), "the write was parked and then dropped");
    assert_eq!(flusher.dirty_count(), 1, "a dropped flush loses nothing");
    assert_eq!(flusher.flushes(), 0);

    let flush = flusher.flush_now();
    tokio::pin!(flush);
    tokio::select! {
        _ = &mut flush => panic!("the stalled store cannot have answered yet"),
        () = tokio::time::sleep(Duration::from_millis(20)) => {}
    }
    stall.notify_one();
    flush.await.unwrap();
    assert_eq!(store.last().unwrap(), vec![(0, id(3, 0))]);
}

#[tokio::test]
async fn flusher_sync_flush_waits_for_a_concurrent_write_to_land() {
    // Rust-only race: a Sync* worker's flush that finds nothing dirty because the timer's flush
    // already drained its record must still not return before that write landed.
    let stall = Arc::new(Notify::new());
    let store = CountingStore {
        stall: Some(Arc::clone(&stall)),
        ..CountingStore::default()
    };
    let flusher = Arc::new(new_flusher(&store, 1));
    flusher.record(0, id(1, 0));
    let first = tokio::spawn({
        let f = Arc::clone(&flusher);
        async move { f.flush_now().await }
    });
    wait(|| flusher.dirty_count() == 0, "the first flush drained").await;
    let second = tokio::spawn({
        let f = Arc::clone(&flusher);
        async move { f.flush_now().await }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !second.is_finished(),
        "nothing landed yet, so the sink flush must still wait"
    );
    stall.notify_one();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(store.writes(), 1);
}

// ------------------------------------------------------------------ flusher: monotonicity guard

#[tokio::test]
async fn flusher_backwards_id_trips_the_monotonicity_guard() {
    let store = CountingStore::default();
    let flusher = new_flusher(&store, 2);
    flusher.record(0, id(100, 5));
    flusher.record(0, id(100, 4));
    assert_eq!(
        flusher.backwards_records(),
        1,
        "positions are monotonic per partition"
    );
}

#[tokio::test]
async fn flusher_forward_ids_do_not_trip_the_guard() {
    let store = CountingStore::default();
    let flusher = new_flusher(&store, 2);
    flusher.record(0, id(100, 1));
    flusher.record(0, id(100, 2));
    flusher.record(0, id(101, 0));
    flusher.record(0, id(101, 0));
    assert_eq!(
        flusher.backwards_records(),
        0,
        "equal or increasing ids are legal"
    );
}

#[tokio::test]
async fn flusher_record_for_unknown_partition_is_ignored_not_thrown() {
    let store = CountingStore::default();
    let flusher = new_flusher(&store, 2);
    flusher.record(9, id(1, 0));
    assert_eq!(flusher.dirty_count(), 0);
    flusher.flush_now().await.unwrap();
    assert_eq!(store.writes(), 0);
}

#[tokio::test]
async fn flusher_new_rejects_impossible_options() {
    let store = CountingStore::default();
    assert!(PositionFlusher::new(store.clone(), options(0, Duration::from_secs(1))).is_err());
    assert!(PositionFlusher::new(store.clone(), options(1, Duration::ZERO)).is_err());
    let mut blank = options(1, Duration::from_secs(1));
    blank.topic = " ".into();
    assert!(PositionFlusher::new(store, blank).is_err());
}

#[tokio::test]
async fn flusher_drops_positions_of_partitions_no_longer_held() {
    let store = CountingStore::default();
    let (tx, rx) = watch::channel::<Held>(Arc::from([0u32, 1]));
    let mut o = options(2, Duration::from_secs(300));
    o.owned = Some(rx);
    let flusher = PositionFlusher::new(store.clone(), o).unwrap();
    flusher.record(0, id(5, 0));
    flusher.record(1, id(5, 0));
    tx.send(Arc::from([0u32])).unwrap();
    flusher.flush_now().await.unwrap();
    assert_eq!(
        store.last().unwrap(),
        vec![(0, id(5, 0))],
        "a lost partition's late write would only fight its new owner"
    );
    assert_eq!(flusher.dirty_count(), 0);
}

#[tokio::test]
async fn persist_sync_flush_through_the_sink_writes_once_per_call() {
    // The flusher half of Persist_SyncBatch / Persist_SyncMessage: the worker records then awaits
    // the sink's flush, and each such pair is one write.
    let store = CountingStore::default();
    let sink: Arc<dyn PositionSink> = Arc::new(new_flusher(&store, 1));
    for i in 1..=5 {
        sink.record(0, id(i, 2));
        sink.flush().await.unwrap();
    }
    assert_eq!(store.writes(), 5);
    assert_eq!(store.last().unwrap(), vec![(0, id(5, 2))]);
}

#[tokio::test]
async fn second_writer_without_redis_is_not_checked() {
    let store = CountingStore::default();
    let contested = Arc::new(AtomicU32::new(0));
    let mut o = options(2, Duration::from_secs(300));
    let c = Arc::clone(&contested);
    o.on_contested = Some(Arc::new(move |_, _, _| {
        c.fetch_add(1, Ordering::SeqCst);
    }));
    let flusher = PositionFlusher::new(store.clone(), o).unwrap();
    flusher.record(0, id(1, 0));
    flusher.flush_now().await.unwrap();
    assert_eq!(contested.load(Ordering::SeqCst), 0);
    assert_eq!(store.writes(), 1);
}

// ------------------------------------------------------------------ Redis fixture

async fn conn(redis: &redisevents_test_support::RedisFixture) -> Connection {
    Connection::connect(redis.url()).await.expect("connect")
}

fn keys() -> Keys {
    Keys::with_prefix("test:re:")
}

async fn hset(conn: &Connection, key: &str, field: &str, value: &str) {
    let mut c = conn.shared();
    redis::cmd("HSET")
        .arg(key)
        .arg(field)
        .arg(value)
        .query_async::<()>(&mut c)
        .await
        .unwrap();
}

async fn hget(conn: &Connection, key: &str, field: &str) -> Option<String> {
    let mut c = conn.shared();
    redis::cmd("HGET")
        .arg(key)
        .arg(field)
        .query_async(&mut c)
        .await
        .unwrap()
}

/// A foreign id counts as a rival only while it holds a live presence field.
async fn mark_live(conn: &Connection, topic: &str, consumer: &str, instance: Uuid) {
    hset(
        conn,
        &keys().ownership(topic, consumer),
        &presence_field(instance),
        "live",
    )
    .await;
}

async fn seed_position(
    conn: &Connection,
    topic: &str,
    consumer: &str,
    partition: u32,
    at: StreamId,
    by: Uuid,
) {
    let value = PositionRecord {
        id: at,
        instance_id: by,
    }
    .format();
    hset(
        conn,
        &keys().positions(topic, consumer),
        &partition.to_string(),
        &value,
    )
    .await;
}

async fn seed_marker(
    conn: &Connection,
    topic: &str,
    consumer: &str,
    partition: u32,
    target: StreamId,
) {
    hset(
        conn,
        &keys().positions(topic, consumer),
        &reset_marker::field(partition),
        &reset_marker::format(target, time::OffsetDateTime::now_utc()),
    )
    .await;
}

struct Rig {
    conn: Connection,
    topic: String,
    consumer: String,
}

impl Rig {
    async fn new(redis: &redisevents_test_support::RedisFixture, hint: &str) -> Self {
        Self {
            conn: conn(redis).await,
            topic: redis.new_topic(hint),
            consumer: redis.new_consumer(hint),
        }
    }

    fn options(&self, partitions: u32, interval: Duration, instance: Uuid) -> FlusherOptions {
        let mut o =
            FlusherOptions::new(&self.topic, &self.consumer, partitions, interval, instance);
        o.redis = Some((self.conn.clone(), keys()));
        o
    }

    fn store(&self, instance: Uuid) -> RedisPositionStore {
        RedisPositionStore::new(self.conn.clone(), keys(), instance)
    }

    /// A flusher on a real Redis store, with every stand-down recorded in `contested`.
    fn flusher(
        &self,
        partitions: u32,
        instance: Uuid,
        contested: &Arc<Mutex<Vec<(u32, Uuid, Uuid)>>>,
        tweak: impl FnOnce(&mut FlusherOptions),
    ) -> PositionFlusher<RedisPositionStore> {
        let mut o = self.options(partitions, Duration::from_secs(300), instance);
        let sink = Arc::clone(contested);
        o.on_contested = Some(Arc::new(move |p, m, t| lock(&sink).push((p, m, t))));
        tweak(&mut o);
        PositionFlusher::new(self.store(instance), o).unwrap()
    }

    async fn stored(&self) -> std::collections::BTreeMap<u32, PositionRecord> {
        self.store(Uuid::nil())
            .load_records(&self.topic, &self.consumer)
            .await
            .unwrap()
    }

    fn positions_key(&self) -> String {
        keys().positions(&self.topic, &self.consumer)
    }

    fn static_registry(&self, pod: &str, partitions: u32) -> OwnershipRegistry {
        let mut o = OwnershipRegistryOptions::new(&self.topic, &self.consumer, partitions);
        o.owned_partitions = (0..partitions).collect();
        o.pod_name = pod.to_owned();
        o.mode = InstanceMode::Static;
        o.instance_id = instance_id_for(Some(pod));
        OwnershipRegistry::new(&self.conn, &keys(), o).unwrap()
    }

    fn lease_registry(&self, pod: &str, partitions: u32) -> OwnershipRegistry {
        let mut o = OwnershipRegistryOptions::new(&self.topic, &self.consumer, partitions);
        o.pod_name = pod.to_owned();
        o.instance_id = instance_id_for(Some(pod));
        OwnershipRegistry::new(&self.conn, &keys(), o).unwrap()
    }
}

fn contested_log() -> Arc<Mutex<Vec<(u32, Uuid, Uuid)>>> {
    Arc::new(Mutex::new(Vec::new()))
}

// ------------------------------------------------------------------ second writer

#[tokio::test]
async fn second_writer_live_foreign_instance_id_counts_contested_and_stops_the_partition() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "sw-live").await;
    let (mine, theirs) = (HIGHER, LOWER);
    seed_position(&rig.conn, &rig.topic, &rig.consumer, 0, id(77, 1), theirs).await;
    mark_live(&rig.conn, &rig.topic, &rig.consumer, theirs).await;

    let contested = contested_log();
    let flusher = rig.flusher(2, mine, &contested, |_| {});
    let stop = PartitionStop::detached();
    flusher.attach(0, stop.clone());
    let other = PartitionStop::detached();
    flusher.attach(1, other.clone());
    flusher.detach(1);

    flusher.record(0, id(100, 0));
    flusher.record(1, id(100, 0));
    flusher.flush_now().await.unwrap();

    assert_eq!(*lock(&contested), vec![(0, mine, theirs)]);
    assert_eq!(flusher.contested_count(), 1);
    assert_eq!(flusher.reported(0), Reported::Contention);
    assert!(
        stop.is_revoked(),
        "the partition's worker is revoked: nothing more is recorded"
    );
    assert!(flusher.is_stood_down(0) && !flusher.is_stood_down(1));

    // The stood-down partition stops writing; the uncontested one carries on.
    flusher.record(0, id(200, 0));
    flusher.record(1, id(200, 0));
    assert_eq!(
        flusher.dirty_count(),
        1,
        "partition 0 is stood down, partition 1 is not"
    );
    flusher.flush_now().await.unwrap();
    let stored = rig.stored().await;
    assert_eq!(stored[&1].id, id(200, 0));
    assert_eq!(
        stored[&0].id,
        id(100, 0),
        "the first write landed before the verdict; none after"
    );

    // Latched: a second sighting neither calls back nor counts twice.
    assert_eq!(lock(&contested).len(), 1);
    assert_eq!(flusher.contested_count(), 1);
}

#[tokio::test]
async fn second_writer_our_own_or_unattributed_values_are_not_contention() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "sw-own").await;
    let mine = Uuid::new_v4();
    let key = rig.positions_key();
    seed_position(&rig.conn, &rig.topic, &rig.consumer, 0, id(1, 0), mine).await;
    hset(&rig.conn, &key, "1", "12-3").await; // legacy, unattributed
    hset(&rig.conn, &key, "2", "garbage").await; // unparseable

    let contested = contested_log();
    let flusher = rig.flusher(4, mine, &contested, |_| {});
    for p in 0..4 {
        flusher.record(p, id(100, 0)); // partition 3 has no previous value at all
    }
    flusher.flush_now().await.unwrap();
    assert!(lock(&contested).is_empty());
    assert_eq!(flusher.contested_count(), 0);
    assert_eq!(flusher.contention_count(), 0);
}

#[tokio::test]
async fn second_writer_foreign_id_with_no_live_claim_is_not_contention() {
    // R-01: a pod restart under a new id and an out-of-process reset both look like this.
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "sw-stale").await;
    seed_position(&rig.conn, &rig.topic, &rig.consumer, 0, id(77, 1), LOWER).await;

    let contested = contested_log();
    let flusher = rig.flusher(1, HIGHER, &contested, |_| {});
    flusher.record(0, id(100, 0));
    flusher.flush_now().await.unwrap();
    assert!(lock(&contested).is_empty());
    assert_eq!(flusher.contested_count(), 0);
    assert_eq!(flusher.contention_count(), 0);
    assert_eq!(flusher.reported(0), Reported::Stale);

    flusher.record(0, id(200, 0));
    flusher.flush_now().await.unwrap();
    assert_eq!(flusher.flushes(), 2, "and it keeps writing");
    assert_eq!(rig.stored().await[&0].id, id(200, 0));
}

#[tokio::test]
async fn second_writer_admin_reset_is_not_contention() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "sw-admin").await;
    seed_position(
        &rig.conn,
        &rig.topic,
        &rig.consumer,
        0,
        id(9, 0),
        ADMIN_INSTANCE_ID,
    )
    .await;
    mark_live(&rig.conn, &rig.topic, &rig.consumer, ADMIN_INSTANCE_ID).await;

    let contested = contested_log();
    let flusher = rig.flusher(1, Uuid::new_v4(), &contested, |_| {});
    flusher.record(0, id(100, 0));
    flusher.flush_now().await.unwrap();
    assert!(lock(&contested).is_empty());
    assert_eq!(flusher.contested_count(), 0);
    assert_eq!(flusher.reported(0), Reported::Nothing, "not even judged");
}

#[tokio::test]
async fn second_writer_lower_id_wins_the_tiebreak_and_keeps_the_partition() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "sw-tiebreak").await;
    seed_position(&rig.conn, &rig.topic, &rig.consumer, 0, id(77, 1), HIGHER).await;
    mark_live(&rig.conn, &rig.topic, &rig.consumer, HIGHER).await;

    let contested = contested_log();
    let flusher = rig.flusher(1, LOWER, &contested, |_| {});
    flusher.record(0, id(100, 0));
    flusher.flush_now().await.unwrap();
    assert!(
        lock(&contested).is_empty(),
        "the lower id keeps the partition"
    );
    assert_eq!(flusher.contested_count(), 0);
    assert_eq!(
        flusher.contention_count(),
        1,
        "the overlap is still on the gauge"
    );
    assert_eq!(flusher.reported(0), Reported::Contention);

    // The rival writes again; we still write, and count it once.
    seed_position(&rig.conn, &rig.topic, &rig.consumer, 0, id(150, 0), HIGHER).await;
    flusher.record(0, id(200, 0));
    flusher.flush_now().await.unwrap();
    assert_eq!(rig.stored().await[&0].id, id(200, 0));
    assert_eq!(flusher.contention_count(), 1);
}

#[test]
fn uuid_order_matches_dotnet_guid_compare_to() {
    // .NET compares Data1/2/3 unsigned, then the trailing bytes: string order of the "D" form,
    // which is `Uuid`'s byte order. The tiebreak must agree across the two implementations.
    let a = Uuid::parse_str("7fffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
    let b = Uuid::parse_str("80000000-0000-0000-0000-000000000000").unwrap();
    assert!(a < b, "Data1 is compared unsigned");
    let c = Uuid::parse_str("00000000-0000-0000-00ff-000000000000").unwrap();
    let d = Uuid::parse_str("00000000-0000-0000-0100-000000000000").unwrap();
    assert!(c < d);
    assert!(LOWER < HIGHER);
}

// ------------------------------------------------------------------ reset: live

fn with_resets(o: &mut FlusherOptions, signal: &Arc<ResetSignal>, interval: Duration) {
    o.resets = Some(Arc::clone(signal));
    o.interval = interval;
}

#[tokio::test]
async fn reset_live_is_noticed_on_a_flusher_tick_handed_over_and_cleared() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-live").await;
    let target = id(1_000, 4);
    seed_marker(&rig.conn, &rig.topic, &rig.consumer, 0, target).await;

    let store = CountingStore::default();
    let signal = Arc::new(ResetSignal::new(2));
    let mut o = rig.options(2, Duration::from_millis(15), Uuid::new_v4());
    with_resets(&mut o, &signal, Duration::from_millis(15));
    let flusher = PositionFlusher::new(store.clone(), o).unwrap();
    flusher.start();

    wait(|| signal.has_pending(), "the reset became pending").await;

    // Standing in for the read loop: take, rewind, (seek).
    let taken = signal.try_take(0).expect("pending");
    assert_eq!(taken.target, target);
    flusher.rewind(0);
    assert!(!signal.has_pending());

    wait(|| flusher.resets_applied() == 1, "the marker was cleared").await;
    assert_eq!(
        hget(&rig.conn, &rig.positions_key(), &reset_marker::field(0)).await,
        None,
        "the marker is deleted once acted on"
    );
    assert!(!signal.is_taken(0));
    assert_eq!(flusher.dirty_count(), 0);
    flusher.stop().await;
    assert_eq!(
        store.writes(),
        0,
        "a rewind writes nothing; Redis already holds the target"
    );
}

#[tokio::test]
async fn reset_live_marker_stays_in_redis_until_a_read_loop_takes_it() {
    // Also the "reset issued while a read is parked" race: the reader is in a blocking XREAD and
    // takes nothing for a while; the marker waits.
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-parked").await;
    seed_marker(&rig.conn, &rig.topic, &rig.consumer, 0, id(9, 0)).await;

    let signal = Arc::new(ResetSignal::new(1));
    let mut o = rig.options(1, Duration::from_millis(10), Uuid::new_v4());
    with_resets(&mut o, &signal, Duration::from_millis(10));
    let flusher = PositionFlusher::new(CountingStore::default(), o).unwrap();
    flusher.start();
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        hget(&rig.conn, &rig.positions_key(), &reset_marker::field(0))
            .await
            .is_some(),
        "a reader parked in a blocking XREAD must not lose the reset"
    );
    assert_eq!(flusher.resets_applied(), 0);
    assert!(signal.has_pending());
    flusher.stop().await;
}

#[tokio::test]
async fn reset_live_after_rewind_a_backwards_record_is_forgiven_exactly_once() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-forgive").await;
    seed_marker(&rig.conn, &rig.topic, &rig.consumer, 0, id(10, 0)).await;

    let signal = Arc::new(ResetSignal::new(1));
    let mut o = rig.options(1, Duration::from_millis(10), Uuid::new_v4());
    with_resets(&mut o, &signal, Duration::from_millis(10));
    let flusher = PositionFlusher::new(CountingStore::default(), o).unwrap();
    flusher.record(0, id(9_000, 0));
    flusher.start();

    wait(|| signal.has_pending(), "the reset became pending").await;
    assert!(signal.try_take(0).is_some());
    flusher.rewind(0);
    wait(|| flusher.resets_applied() == 1, "the marker was cleared").await;
    flusher.stop().await;

    flusher.record(0, id(11, 0));
    assert_eq!(flusher.backwards_records(), 0);
    flusher.record(0, id(10, 5));
    assert_eq!(
        flusher.backwards_records(),
        0,
        "the reset's own replay must not trip the guard"
    );
    flusher.record(0, id(10, 4));
    assert_eq!(
        flusher.backwards_records(),
        1,
        "forgiveness is granted exactly once per reset"
    );
}

#[tokio::test]
async fn reset_live_pending_reset_holds_back_the_pre_reset_position() {
    // Race: admin reset vs flusher. The admin call stored the target as the position (admin id)
    // with the marker; the worker keeps recording pre-reset positions until its read loop takes
    // the reset. Writing those would overwrite the target (lost on a crash, and the trimmer would
    // clamp past the replay range), so the flusher holds the partition until the rewind.
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-hold").await;
    let mine = Uuid::new_v4();
    let target = id(10, 0);

    let signal = Arc::new(ResetSignal::new(2));
    let contested = contested_log();
    let flusher = rig.flusher(2, mine, &contested, |o| {
        o.resets = Some(Arc::clone(&signal));
    });

    flusher.record(0, id(500, 0));
    flusher.record(1, id(500, 0));
    flusher.flush_now().await.unwrap();

    rig.store(mine)
        .reset(&rig.topic, &rig.consumer, target, Some(0))
        .await
        .unwrap();
    flusher.poll_resets().await;
    assert!(signal.has_pending());

    // In-flight batches keep landing before the read loop gets to the reset.
    flusher.record(0, id(600, 0));
    flusher.record(1, id(600, 0));
    flusher.flush_now().await.unwrap();
    let stored = rig.stored().await;
    assert_eq!(
        stored[&0].id, target,
        "the admin's target is not overwritten"
    );
    assert_eq!(stored[&0].instance_id, ADMIN_INSTANCE_ID);
    assert_eq!(stored[&1].id, id(600, 0), "other partitions are unaffected");

    // The read loop takes it: try_take -> rewind -> seek; the replay records from the target.
    assert!(signal.try_take(0).is_some());
    flusher.rewind(0);
    flusher.record(0, id(11, 0));
    flusher.flush_now().await.unwrap();
    let stored = rig.stored().await;
    assert_eq!(stored[&0].id, id(11, 0));
    assert_eq!(stored[&0].instance_id, mine);
    assert!(
        lock(&contested).is_empty(),
        "the admin id is a rewind, never a rival"
    );
    assert_eq!(flusher.reported(0), Reported::Nothing);

    flusher.poll_resets().await;
    assert_eq!(flusher.resets_applied(), 1);
    assert_eq!(
        hget(&rig.conn, &rig.positions_key(), &reset_marker::field(0)).await,
        None
    );
}

#[tokio::test]
async fn reset_live_post_seek_record_survives_the_clearing_tick() {
    // Race: flusher vs read loop. The read loop rewinds and records a fresh post-seek position
    // before the flusher's tick notices the take; that tick only deletes the marker and must not
    // drop the new position (C# `Rewind` remarks).
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-postseek").await;
    seed_marker(&rig.conn, &rig.topic, &rig.consumer, 0, id(10, 0)).await;

    let store = CountingStore::default();
    let signal = Arc::new(ResetSignal::new(1));
    let mut o = rig.options(1, Duration::from_secs(300), Uuid::new_v4());
    o.resets = Some(Arc::clone(&signal));
    let flusher = PositionFlusher::new(store.clone(), o).unwrap();

    flusher.poll_resets().await;
    assert!(signal.try_take(0).is_some());
    flusher.rewind(0);
    flusher.record(0, id(12, 0));

    flusher.poll_resets().await;
    assert_eq!(flusher.resets_applied(), 1);
    assert_eq!(
        flusher.dirty_count(),
        1,
        "the post-seek position is still pending"
    );
    flusher.flush_now().await.unwrap();
    assert_eq!(store.last().unwrap(), vec![(0, id(12, 0))]);
}

#[tokio::test]
async fn reset_live_newer_marker_is_not_deleted_with_the_taken_one() {
    // The delete is compare-and-delete: a marker an operator writes between our read and our
    // delete survives (C# deleted it along with the old one).
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-cas").await;
    let key = rig.positions_key();
    seed_marker(&rig.conn, &rig.topic, &rig.consumer, 0, id(10, 0)).await;
    let old = hget(&rig.conn, &key, &reset_marker::field(0))
        .await
        .unwrap();
    seed_marker(&rig.conn, &rig.topic, &rig.consumer, 0, id(20, 0)).await;

    let mut c = rig.conn.shared();
    let deleted = clear_markers(&mut c, &key, &[(0, old)]).await.unwrap();
    assert_eq!(deleted, 0);
    let newer = hget(&rig.conn, &key, &reset_marker::field(0))
        .await
        .unwrap();
    assert!(newer.starts_with("20-0|"), "{newer}");

    let deleted = clear_markers(&mut c, &key, &[(0, newer)]).await.unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn reset_live_marker_of_a_partition_not_owned_is_left_for_its_owner() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-foreign").await;
    seed_marker(&rig.conn, &rig.topic, &rig.consumer, 1, id(10, 0)).await;

    let signal = Arc::new(ResetSignal::new(2));
    let (_tx, rx) = watch::channel::<Held>(Arc::from([0u32]));
    let mut o = rig.options(2, Duration::from_secs(300), Uuid::new_v4());
    o.resets = Some(Arc::clone(&signal));
    o.owned = Some(rx);
    let flusher = PositionFlusher::new(CountingStore::default(), o).unwrap();
    flusher.poll_resets().await;
    assert!(
        !signal.has_pending(),
        "only owned partitions' markers are requested"
    );
    assert!(
        hget(&rig.conn, &rig.positions_key(), &reset_marker::field(1))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn reset_cold_marker_written_while_not_running_is_applied_at_the_next_start() {
    // The cold path: nobody is running, so nothing polls; the marker waits for the next start,
    // where `take_all` (B4) applies and deletes it before any read loop or flusher exists.
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "reset-cold").await;
    let mine = Uuid::new_v4();
    rig.store(mine)
        .reset(&rig.topic, &rig.consumer, id(10, 0), Some(0))
        .await
        .unwrap();

    let markers = reset_marker::take_all(&rig.conn, &keys(), &rig.topic, &rig.consumer)
        .await
        .unwrap();
    assert_eq!(markers[&0].target, id(10, 0));

    // The new life's flusher then writes over the admin-stamped position without a stand-down.
    let signal = Arc::new(ResetSignal::new(1));
    let contested = contested_log();
    let flusher = rig.flusher(1, mine, &contested, |o| {
        o.resets = Some(Arc::clone(&signal));
    });
    flusher.poll_resets().await;
    assert!(
        !signal.has_pending(),
        "the marker is gone: nothing to apply twice"
    );
    flusher.record(0, id(11, 0));
    flusher.flush_now().await.unwrap();
    assert!(lock(&contested).is_empty());
    assert_eq!(rig.stored().await[&0].id, id(11, 0));
}

// ------------------------------------------------------------------ WriterIdentityTests

#[tokio::test]
async fn s6e_two_live_writers_stand_exactly_one_of_themselves_down() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "s6e").await;
    let own_a = rig.static_registry("svc-a-0", 1);
    let own_b = rig.static_registry("svc-b-0", 1);
    let (a, b) = (own_a.instance_id(), own_b.instance_id());
    assert_ne!(a, b);
    own_a.start().await.unwrap();
    own_b.start().await.unwrap();

    let (ca, cb) = (contested_log(), contested_log());
    let flush_a = rig.flusher(1, a, &ca, |_| {});
    let flush_b = rig.flusher(1, b, &cb, |_| {});

    flush_a.record(0, id(1000, 0));
    flush_a.flush_now().await.unwrap();
    flush_b.record(0, id(1001, 0));
    flush_b.flush_now().await.unwrap();
    flush_a.record(0, id(1002, 0));
    flush_a.flush_now().await.unwrap();

    let a_loses = a > b;
    let (loser, winner) = if a_loses {
        (&flush_a, &flush_b)
    } else {
        (&flush_b, &flush_a)
    };
    let (loser_calls, winner_calls) = if a_loses { (&ca, &cb) } else { (&cb, &ca) };
    let winner_id = if a_loses { b } else { a };

    assert_eq!(loser.contested_count(), 1, "the higher id stands down");
    assert_eq!(
        lock(loser_calls).iter().map(|c| c.0).collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(
        winner.contested_count(),
        0,
        "both standing down would leave nobody"
    );
    assert!(lock(winner_calls).is_empty());
    assert_eq!(loser.contention_count(), 1);
    assert_eq!(winner.contention_count(), 1);

    winner.record(0, id(2000, 0));
    loser.record(0, id(3000, 0));
    winner.flush_now().await.unwrap();
    loser.flush_now().await.unwrap();
    let stored = rig.stored().await;
    assert_eq!(
        stored[&0].id,
        id(2000, 0),
        "only the winner is still writing"
    );
    assert_eq!(stored[&0].instance_id, winner_id);

    own_a.dispose().await;
    own_b.dispose().await;
}

#[tokio::test]
async fn a_static_rolling_deploy_overlap_inside_the_grace_window_stands_nobody_down() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "static-rollover").await;
    let own_out = rig.static_registry("svc-6b9c4f-oldpd", 1);
    let own_in = rig.static_registry("svc-7d4f8b-newpd", 1);
    own_out.start().await.unwrap();
    own_in.start().await.unwrap();

    let grace = |o: &mut FlusherOptions| o.contested_grace = Duration::from_secs(300);
    let (c_out, c_in) = (contested_log(), contested_log());
    let flush_out = rig.flusher(1, own_out.instance_id(), &c_out, grace);
    let flush_in = rig.flusher(1, own_in.instance_id(), &c_in, grace);

    for (n, f) in [&flush_out, &flush_in, &flush_out, &flush_in]
        .iter()
        .enumerate()
    {
        f.record(0, id(1000 + n as u64, 0));
        f.flush_now().await.unwrap();
    }
    assert_eq!(
        flush_in.contested_count(),
        0,
        "a handover until proven otherwise"
    );
    assert_eq!(
        flush_out.contested_count(),
        0,
        "nor may the outgoing pod's drain be cut short"
    );
    assert!(lock(&c_in).is_empty() && lock(&c_out).is_empty());

    own_out.stop().await;
    flush_out.dispose().await;
    flush_in.record(0, id(2000, 0));
    flush_in.flush_now().await.unwrap();
    assert_eq!(flush_in.contested_count(), 0);
    let stored = rig.stored().await;
    assert_eq!(stored[&0].id, id(2000, 0));
    assert_eq!(stored[&0].instance_id, own_in.instance_id());
    own_in.dispose().await;
}

#[tokio::test]
async fn a_static_stand_down_resumes_once_its_contender_has_left_the_hash() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "static-resume").await;
    let own_a = rig.static_registry("svc-a-0", 1);
    let own_b = rig.static_registry("svc-b-0", 1);
    own_a.start().await.unwrap();
    own_b.start().await.unwrap();

    let recovered: Arc<Mutex<Vec<(u32, Uuid)>>> = Arc::new(Mutex::new(Vec::new()));
    let on_recovered = |o: &mut FlusherOptions| {
        let r = Arc::clone(&recovered);
        o.on_recovered = Some(Arc::new(move |p, t| lock(&r).push((p, t))));
        o.recheck_interval = Some(Duration::from_secs(30));
    };
    let (ca, cb) = (contested_log(), contested_log());
    let flush_a = rig.flusher(1, own_a.instance_id(), &ca, on_recovered);
    let flush_b = rig.flusher(1, own_b.instance_id(), &cb, on_recovered);

    flush_a.record(0, id(1000, 0));
    flush_a.flush_now().await.unwrap();
    flush_b.record(0, id(1001, 0));
    flush_b.flush_now().await.unwrap();
    flush_a.record(0, id(1002, 0));
    flush_a.flush_now().await.unwrap();

    let loser_is_a = own_a.instance_id() > own_b.instance_id();
    let (loser, winner) = if loser_is_a {
        (&flush_a, &flush_b)
    } else {
        (&flush_b, &flush_a)
    };
    let (loser_reg, winner_reg) = if loser_is_a {
        (&own_a, &own_b)
    } else {
        (&own_b, &own_a)
    };
    assert_eq!(loser.contested_count(), 1);

    loser.recheck().await;
    assert_eq!(loser.contested_count(), 1, "the contender is still live");
    assert_eq!(loser.recoveries(), 0);
    assert!(lock(&recovered).is_empty());

    winner_reg.stop().await;
    winner.dispose().await;
    loser.recheck().await;
    assert_eq!(loser.contested_count(), 0);
    assert_eq!(loser.recoveries(), 1);
    assert_eq!(
        lock(&recovered).iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![0],
        "the host is asked to read the partition again, exactly once"
    );
    assert_eq!(loser.contention_count(), 0);

    loser.record(0, id(5000, 0));
    loser.flush_now().await.unwrap();
    let stored = rig.stored().await;
    assert_eq!(stored[&0].id, id(5000, 0), "the survivor is writing again");
    assert_eq!(stored[&0].instance_id, loser.instance_id());
    loser_reg.dispose().await;
}

#[tokio::test]
async fn a_lease_handoff_does_not_falsely_stand_down_the_new_owner() {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "lease-handoff").await;
    let own_a = rig.lease_registry("svc-7d4f8b-aaaaa", 1);
    own_a.start().await.unwrap();
    assert_eq!(&*own_a.held(), &[0]);

    let lease = |o: &mut FlusherOptions| o.exclusive_claims = true;
    let (ca, cb) = (contested_log(), contested_log());
    let a = own_a.instance_id();
    let b = instance_id_for(Some("svc-7d4f8b-bbbbb"));
    let flush_a = rig.flusher(1, a, &ca, lease);
    let flush_b = rig.flusher(1, b, &cb, lease);

    flush_a.record(0, id(1000, 0));
    flush_a.flush_now().await.unwrap();
    own_a.stop().await;

    let own_b = rig.lease_registry("svc-7d4f8b-bbbbb", 1);
    own_b.start().await.unwrap();
    assert_eq!(&*own_b.held(), &[0]);

    flush_a.record(0, id(1001, 0));
    flush_a.flush_now().await.unwrap();
    assert_eq!(
        rig.stored().await[&0].instance_id,
        a,
        "the late write really landed"
    );

    flush_b.record(0, id(2000, 0));
    flush_b.flush_now().await.unwrap();
    assert_eq!(flush_b.contested_count(), 0);
    assert!(lock(&cb).is_empty());
    let stored = rig.stored().await;
    assert_eq!(stored[&0].id, id(2000, 0));
    assert_eq!(stored[&0].instance_id, b);
    own_b.dispose().await;
}

#[tokio::test]
async fn a_late_flush_from_a_live_peer_that_gave_the_partition_up_does_not_stand_the_new_owner_down()
 {
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "lease-late-flush").await;
    let own_a = rig.lease_registry("svc-7d4f8b-aaaaa", 2);
    let first = own_a.refresh().await.unwrap();
    assert_eq!(first.owners.keys().copied().collect::<Vec<_>>(), vec![0, 1]);

    let lease = |o: &mut FlusherOptions| o.exclusive_claims = true;
    let (ca, cb) = (contested_log(), contested_log());
    let a = own_a.instance_id();
    let b = instance_id_for(Some("svc-7d4f8b-bbbbb"));
    let flush_a = rig.flusher(2, a, &ca, lease);
    let flush_b = rig.flusher(2, b, &cb, lease);

    flush_a.record(1, id(1000, 0));
    flush_a.flush_now().await.unwrap();

    let own_b = rig.lease_registry("svc-7d4f8b-bbbbb", 2);
    own_b.refresh().await.unwrap();
    own_a.refresh().await.unwrap();
    own_b.refresh().await.unwrap();
    assert_eq!(&*own_a.held(), &[0], "A gave up its surplus partition");
    assert_eq!(&*own_b.held(), &[1], "B claimed it");

    flush_a.record(1, id(1001, 0));
    flush_a.flush_now().await.unwrap();
    assert_eq!(
        rig.stored().await[&1].instance_id,
        a,
        "A's late write landed after B's claim"
    );

    flush_b.record(1, id(2000, 0));
    flush_b.flush_now().await.unwrap();
    assert_eq!(flush_b.contested_count(), 0, "a late flush, not a rival");
    assert!(lock(&cb).is_empty());
    assert_eq!(flush_b.contention_count(), 0);
    assert_eq!(flush_b.reported(1), Reported::LateWrite);

    flush_b.record(1, id(2001, 0));
    flush_a.record(0, id(500, 0));
    flush_b.flush_now().await.unwrap();
    flush_a.flush_now().await.unwrap();
    let stored = rig.stored().await;
    assert_eq!((stored[&1].id, stored[&1].instance_id), (id(2001, 0), b));
    assert_eq!((stored[&0].id, stored[&0].instance_id), (id(500, 0), a));
    assert!(lock(&ca).is_empty());

    own_b.dispose().await;
    own_a.dispose().await;
}

#[tokio::test]
async fn a_restarted_pod_does_not_contest_its_own_predecessors_position() {
    // "Our own previous life's id is not a rival": the id is derived from the pod name.
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "restart").await;
    let life1 = rig.static_registry("svc-1", 1);
    let life1_id = life1.instance_id();
    life1.refresh().await.unwrap(); // claims; never released: a SIGKILL

    let c1 = contested_log();
    let flush1 = rig.flusher(1, life1_id, &c1, |_| {});
    flush1.record(0, id(1000, 0));
    flush1.flush_now().await.unwrap();
    drop(flush1);
    drop(life1);

    let life2 = rig.static_registry("svc-1", 1);
    assert_eq!(
        life2.instance_id(),
        life1_id,
        "a restart is the same logical writer"
    );
    life2.start().await.unwrap();
    assert!(life2.latest().unwrap().contested.is_empty());

    let c2 = contested_log();
    let flush2 = rig.flusher(1, life1_id, &c2, |_| {});
    flush2.record(0, id(2000, 0));
    flush2.flush_now().await.unwrap();
    assert_eq!(flush2.contested_count(), 0);
    assert!(lock(&c2).is_empty());
    assert_eq!(rig.stored().await[&0].id, id(2000, 0));
    life2.dispose().await;
}

#[tokio::test]
async fn a_reset_issued_from_another_process_rewinds_instead_of_standing_the_partition_down() {
    // The flusher half of the C# host test: a running consumer (live presence claim), a reset
    // written by an admin process (admin identity, its own connection), the read loop rewinds, and
    // the flusher writes over the admin's value without standing down.
    let redis = redisevents_test_support::require_redis!();
    let rig = Rig::new(redis, "oop-reset").await;
    let own = rig.static_registry("svc-0", 1);
    own.start().await.unwrap();
    let mine = own.instance_id();
    mark_live(&rig.conn, &rig.topic, &rig.consumer, ADMIN_INSTANCE_ID).await; // worst case

    let signal = Arc::new(ResetSignal::new(1));
    let contested = contested_log();
    let flusher = rig.flusher(1, mine, &contested, |o| {
        o.resets = Some(Arc::clone(&signal));
        o.owned = Some(own.subscribe());
        o.interval = Duration::from_millis(20);
    });
    let stop = PartitionStop::detached();
    flusher.attach(0, stop.clone());
    flusher.start();

    flusher.record(0, id(19, 0));
    wait(|| flusher.flushes() >= 1, "the tail position flushed").await;

    let admin = conn(redis).await;
    RedisPositionStore::new(admin, keys(), Uuid::new_v4())
        .reset(&rig.topic, &rig.consumer, id(9, 0), Some(0))
        .await
        .unwrap();

    wait(|| signal.has_pending(), "the reset reached the read loop").await;
    assert!(signal.try_take(0).is_some());
    flusher.rewind(0);
    for ms in 10..=19 {
        flusher.record(0, id(ms, 0));
    }
    wait(|| flusher.resets_applied() == 1, "the marker was cleared").await;
    flusher.record(0, id(20, 0));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let stored = rig.stored().await;
        if stored[&0].instance_id == mine && stored[&0].id == id(20, 0) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the consumer flushed over the admin's position"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(lock(&contested).is_empty());
    assert!(!stop.is_stopped(), "the partition is still being consumed");
    assert_eq!(flusher.backwards_records(), 0);
    flusher.stop().await;
    own.dispose().await;
}
