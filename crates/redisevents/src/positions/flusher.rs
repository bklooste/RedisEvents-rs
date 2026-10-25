//! The position flusher. **Owner: C3** (port of `PositionFlusher.cs`): batches positions, cross-
//! checks foreign writers against the ownership hash, applies reset markers through the hand-off
//! agreed with C2, and keeps the stored position a safe trim clamp for the trimmer (C5).
//! Crate-private. See `docs/CONTRACTS.md`, "C3".
//!
//! # Shape
//!
//! One [`PositionFlusher`] per consumer (topic, consumer pair), shared as `Arc`. It implements
//! [`PositionSink`], so the host hands it to every partition worker as `Arc<dyn PositionSink>`:
//!
//! - [`record`](PositionSink::record) stores the last processed id in the partition's slot and
//!   marks it dirty. A short uncontended lock; no await, no Redis.
//! - the timer ([`start`](PositionFlusher::start)) writes every dirty partition in one
//!   [`PositionStore::save`] per `interval`, and writes nothing at all when nothing is dirty.
//! - [`flush`](PositionSink::flush) is the same write on demand (`SyncBatch` / `SyncMessage`); it
//!   resolves once everything recorded before the call has landed, or with the write's error.
//! - [`stop`](PositionFlusher::stop) stops the timer and does a final flush.
//!
//! `PersistMode::None` uses [`NoPositions`](crate::consumer::shared::NoPositions) instead of a
//! flusher; `AsyncBatch` uses the timer only; the two `Sync*` modes use the timer plus the worker's
//! awaited `flush`. The flusher itself does not need to know the mode.
//!
//! # Second-writer check (R-01 option B)
//!
//! With [`FlusherOptions::redis`] set, each write is pipelined with an `HMGET` of the same fields
//! (sent first on the same multiplexed connection, so it reads the values **before** our write).
//! A value stamped with another instance id is a question, not a verdict:
//!
//! - nil (legacy) or our own id (this instance, or a previous life of it: the id is stable per pod)
//!   → nothing to do;
//! - [`ADMIN_INSTANCE_ID`] → an operator rewind; never stood down for (the reset hand-off applies it);
//! - otherwise one `HMGET` on the ownership hash asks whether that id still has a live presence
//!   field (`i:<id>`) and who holds the partition's claim. No presence → stale (a previous life,
//!   an admin tool, a dead pod): keep writing. Lease mode and the claim names us → a late flush
//!   from the previous owner: keep writing. Static mode inside `contested_grace` → wait. Otherwise
//!   the lower id keeps the partition (gauge + error log) and the higher one **stands down**: it
//!   stops writing that partition, revokes the attached [`PartitionStop`] and calls
//!   [`FlusherOptions::on_contested`]. A failed probe stands nothing down.
//! - every `recheck_interval`, a stood-down partition re-probes its contender's presence and
//!   resumes ([`FlusherOptions::on_recovered`]) once it is gone.
//!
//! # Reset hand-off (`WIRE.md` §7)
//!
//! With [`FlusherOptions::resets`] and `redis` set, every `reset_poll_interval` the timer reads the
//! `__reset:<p>` markers of the partitions this instance owns ([`FlusherOptions::owned`]) and
//! [`ResetSignal::request`]s them. While a reset is pending for a partition, the flusher **does not
//! write that partition**: the admin call stored the target as the position, and writing a
//! pre-reset position over it would both lose the reset on a crash and let the trimmer clamp past
//! the replay range. The read loop takes the reset and calls [`rewind`](PositionSink::rewind)
//! (dropping the pre-reset pending position and releasing the hold) before it seeks; a later tick
//! sees the slot taken and deletes the field with a compare-and-delete (only if it still holds the
//! value we read, so a newer marker written in between survives), then clears the slot. Every
//! flush (the tick's, a `Sync*` worker's and the final one at [`stop`](PositionFlusher::stop))
//! first deletes taken markers the same way, and a taken partition's post-rewind position is
//! written only after its marker is gone: a crash leaves either the marker next to the reset
//! target (the reset applies once more) or no marker, never a marker next to a newer position
//! (which would replay twice). C#'s `StopAsync` only flushes and leaves a taken marker behind.
//!
//! # Trim interlock (C5)
//!
//! The trimmer clamps its `MINID` to the lowest stored position. The flusher keeps that clamp safe:
//! it writes only positions that were **processed** (the worker records after handling), never a
//! value ahead of one (a slot holds one whole `StreamId` under a lock, so there is no torn pair);
//! it never overwrites a pending reset target; a stood-down partition stops writing, so its stored
//! position stays where it was; and `stop` flushes last so a clean shutdown leaves the clamp at the
//! true position.

// C4 (host) is the caller; until it lands only the tests use this module.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use redis::Script;
use redis::aio::ConnectionManager;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::reset_marker::{self, ResetMarker};
use super::{ADMIN_INSTANCE_ID, PositionRecord, PositionStore};
use crate::connection::Connection;
use crate::consumer::shared::{BoxFuture, PartitionStop, PositionSink, ResetSignal};
use crate::diagnostics::instruments;
use crate::error::{Error, Result};
use crate::ownership::PartitionOwner;
use crate::ownership::registry::{Held, presence_field};
use crate::wire::{Keys, StreamId};

/// Deletes each `ARGV[i]` field of `KEYS[1]` only while it still holds `ARGV[i + 1]`.
const CLEAR_MARKERS_SCRIPT: &str = r"local n = 0
for i = 1, #ARGV, 2 do
  if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
    n = n + redis.call('HDEL', KEYS[1], ARGV[i])
  end
end
return n
";

static CLEAR_MARKERS: LazyLock<Script> = LazyLock::new(|| Script::new(CLEAR_MARKERS_SCRIPT));

/// Called when this instance stands a partition down: `(partition, mine, theirs)`.
pub(crate) type ContestedCallback = Arc<dyn Fn(u32, Uuid, Uuid) + Send + Sync>;

/// Called when a stood-down partition may be read again: `(partition, theirs)`.
pub(crate) type RecoveredCallback = Arc<dyn Fn(u32, Uuid) + Send + Sync>;

/// How the flusher is built (C# `PositionFlusher` constructor parameters).
#[derive(Clone)]
pub(crate) struct FlusherOptions {
    /// Topic name.
    pub(crate) topic: String,
    /// Consumer name (owner of the positions hash).
    pub(crate) consumer: String,
    /// Partitions on the topic; slots exist for all of them.
    pub(crate) partitions: u32,
    /// Timer period (`ConsumerOptions::persist_interval_ms`).
    pub(crate) interval: Duration,
    /// This instance's id (the store's id for a `RedisPositionStore`).
    pub(crate) instance_id: Uuid,
    /// The connection and keys for the second-writer check and reset polling. `None` (non-Redis
    /// store, tests) skips both: the field layout is not ours to read.
    pub(crate) redis: Option<(Connection, Keys)>,
    /// The partitions this instance holds (`OwnershipRegistry::subscribe`). Dirty positions of a
    /// partition no longer held are dropped, not written, and only held partitions' reset markers
    /// are requested. `None` = every partition counts as held.
    pub(crate) owned: Option<watch::Receiver<Held>>,
    /// The live reset hand-off. `None` = no marker polling (resets then apply at the next start).
    pub(crate) resets: Option<Arc<ResetSignal>>,
    /// How often markers are polled, rounded to whole ticks; `None` = every tick.
    pub(crate) reset_poll_interval: Option<Duration>,
    /// `true` under `InstanceMode::Lease`: a partition's claim names its one holder.
    pub(crate) exclusive_claims: bool,
    /// Static mode: how long an overlap must last before acting on it (`contested_grace_seconds`).
    pub(crate) contested_grace: Duration,
    /// How often a stood-down partition re-probes its contender (`contested_recheck_seconds`);
    /// `None` or zero disables recovery.
    pub(crate) recheck_interval: Option<Duration>,
    /// Stand-down notification (after the attached [`PartitionStop`] was revoked).
    pub(crate) on_contested: Option<ContestedCallback>,
    /// Recovery notification: the host restarts the partition's worker.
    pub(crate) on_recovered: Option<RecoveredCallback>,
}

impl FlusherOptions {
    /// Options with no Redis side, no resets, no grace and no recovery.
    pub(crate) fn new(
        topic: impl Into<String>,
        consumer: impl Into<String>,
        partitions: u32,
        interval: Duration,
        instance_id: Uuid,
    ) -> Self {
        Self {
            topic: topic.into(),
            consumer: consumer.into(),
            partitions,
            interval,
            instance_id,
            redis: None,
            owned: None,
            resets: None,
            reset_poll_interval: None,
            exclusive_claims: false,
            contested_grace: Duration::ZERO,
            recheck_interval: None,
            on_contested: None,
            on_recovered: None,
        }
    }
}

/// What has been said about a partition's foreign writer (C# `Reported*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Reported {
    /// Nothing yet.
    #[default]
    Nothing,
    /// A foreign id with nobody live behind it.
    Stale,
    /// A live second writer. Outranks `Stale`.
    Contention,
    /// A live peer's late write on a partition whose claim we hold.
    LateWrite,
}

/// One partition's state.
#[derive(Debug, Default)]
#[allow(clippy::struct_excessive_bools)]
struct Slot {
    /// The last recorded (processed) id.
    pending: Option<StreamId>,
    /// `pending` not yet written.
    dirty: bool,
    /// Stood down for a live rival: records are ignored.
    stopped: bool,
    /// A reset rewound this partition: the first backwards record is the replay, not a bug.
    rewound: bool,
    /// A reset is pending: do not write this partition until the read loop rewinds.
    reset_hold: bool,
    reported: Reported,
    /// Counted on the contention gauge.
    counted: bool,
    /// First sight of an overlap the claim could not settle (Static grace).
    contested_since: Option<Instant>,
    /// The id a stood-down partition waits on.
    contender: Option<Uuid>,
}

struct RedisSide {
    conn: ConnectionManager,
    positions_key: String,
    ownership_key: String,
}

struct Inner<S> {
    store: S,
    topic: String,
    consumer: String,
    instance_id: Uuid,
    interval: Duration,
    metric_key: String,
    redis: Option<RedisSide>,
    owned: Option<watch::Receiver<Held>>,
    resets: Option<Arc<ResetSignal>>,
    reset_poll_ticks: u32,
    exclusive_claims: bool,
    contested_grace: Duration,
    recheck_ticks: u32,
    on_contested: Option<ContestedCallback>,
    on_recovered: Option<RecoveredCallback>,
    stops: Mutex<HashMap<u32, PartitionStop>>,
    /// Raw marker values last read per partition, for the compare-and-delete.
    marker_values: Mutex<HashMap<u32, String>>,
    slots: Box<[Mutex<Slot>]>,
    gate: tokio::sync::Mutex<()>,
    dirty_count: AtomicUsize,
    contested_count: AtomicUsize,
    contention_count: AtomicI64,
    flushes: AtomicU64,
    failures: AtomicU64,
    resets_applied: AtomicU64,
    recoveries: AtomicU64,
    backwards: AtomicU64,
}

struct Running {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

/// The position flusher for one consumer. See the module docs.
pub(crate) struct PositionFlusher<S: PositionStore> {
    inner: Arc<Inner<S>>,
    running: Mutex<Option<Running>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Whole ticks of `interval` in `every`, at least 1.
fn ticks_of(every: Duration, interval: Duration) -> u32 {
    if every <= interval {
        return 1;
    }
    let ratio = (every.as_secs_f64() / interval.as_secs_f64()).round();
    if ratio >= f64::from(u32::MAX) {
        u32::MAX
    } else {
        // In range and non-negative: checked above.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let ticks = ratio as u32;
        ticks.max(1)
    }
}

impl<S: PositionStore> PositionFlusher<S> {
    /// A flusher writing through `store`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic or consumer, [`Error::Config`] for zero
    /// partitions or a zero interval.
    pub(crate) fn new(store: S, options: FlusherOptions) -> Result<Self> {
        if options.topic.trim().is_empty() {
            return Err(Error::invalid_argument(
                "position flusher topic must not be blank",
            ));
        }
        if options.consumer.trim().is_empty() {
            return Err(Error::invalid_argument(
                "position flusher consumer must not be blank",
            ));
        }
        if options.partitions < 1 {
            return Err(Error::config(format!(
                "position flusher for topic '{}' was given 0 partitions; it must be at least 1",
                options.topic
            )));
        }
        if options.interval.is_zero() {
            return Err(Error::config(format!(
                "position flusher for topic '{}' needs a flush interval above zero (PersistIntervalMs)",
                options.topic
            )));
        }

        let redis = options.redis.as_ref().map(|(conn, keys)| RedisSide {
            conn: conn.shared(),
            positions_key: keys.positions(&options.topic, &options.consumer),
            ownership_key: keys.ownership(&options.topic, &options.consumer),
        });
        // Marker polling needs the Redis side: the hash is ours to read only then.
        let resets = if redis.is_some() {
            options.resets
        } else {
            None
        };
        let recheck_ticks = match options.recheck_interval {
            Some(every) if !every.is_zero() => ticks_of(every, options.interval),
            _ => 0,
        };
        let reset_poll_ticks = ticks_of(
            options.reset_poll_interval.unwrap_or(options.interval),
            options.interval,
        );

        let inner = Inner {
            store,
            metric_key: format!("{}:{}:positions", options.topic, options.consumer),
            topic: options.topic,
            consumer: options.consumer,
            instance_id: options.instance_id,
            interval: options.interval,
            redis,
            owned: options.owned,
            resets,
            reset_poll_ticks,
            exclusive_claims: options.exclusive_claims,
            contested_grace: options.contested_grace,
            recheck_ticks,
            on_contested: options.on_contested,
            on_recovered: options.on_recovered,
            stops: Mutex::new(HashMap::new()),
            marker_values: Mutex::new(HashMap::new()),
            slots: (0..options.partitions)
                .map(|_| Mutex::new(Slot::default()))
                .collect(),
            gate: tokio::sync::Mutex::new(()),
            dirty_count: AtomicUsize::new(0),
            contested_count: AtomicUsize::new(0),
            contention_count: AtomicI64::new(0),
            flushes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            resets_applied: AtomicU64::new(0),
            recoveries: AtomicU64::new(0),
            backwards: AtomicU64::new(0),
        };
        Ok(Self {
            inner: Arc::new(inner),
            running: Mutex::new(None),
        })
    }

    /// Starts the timer on a `tokio` task. Idempotent.
    pub(crate) fn start(&self) {
        let mut running = lock(&self.running);
        if running.is_some() {
            return;
        }
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(Arc::clone(&self.inner), cancel.clone()));
        *running = Some(Running { cancel, task });
    }

    /// Stops the timer (a tick in progress is dropped; whatever it drained is re-queued) and does
    /// one final flush so a clean shutdown does not redeliver the last interval. Call it after the
    /// workers stopped and **before** the ownership registry releases (a released registry holds
    /// nothing, and positions of partitions not held are dropped).
    pub(crate) async fn stop(&self) {
        let running = lock(&self.running).take();
        if let Some(running) = running {
            running.cancel.cancel();
            let _ = running.task.await;
        }
        // Logged and counted inside; nothing more to do with the error at shutdown.
        let _ = self.inner.flush().await;
    }

    /// [`stop`](Self::stop), then zeroes the contention gauge (C# `DisposeAsync`).
    pub(crate) async fn dispose(&self) {
        self.stop().await;
        instruments::set_contested_partitions(&self.inner.metric_key, 0);
    }

    /// Registers the stop the flusher revokes when it stands `partition` down. The host attaches
    /// each worker's [`PartitionStop`] when it starts it (again after a recovery).
    pub(crate) fn attach(&self, partition: u32, stop: PartitionStop) {
        lock(&self.inner.stops).insert(partition, stop);
    }

    /// Forgets `partition`'s stop (the host stopped the worker itself).
    pub(crate) fn detach(&self, partition: u32) {
        lock(&self.inner.stops).remove(&partition);
    }

    /// One flush now; `Err` is the write's error (already logged, counted and re-queued).
    pub(crate) async fn flush_now(&self) -> Result<()> {
        self.inner.flush().await
    }

    /// One re-arbitration of the stood-down partitions now (the timer does this every
    /// `recheck_interval`).
    pub(crate) async fn recheck(&self) {
        self.inner.recheck().await;
    }

    /// One reset-marker poll now (the timer does this every `reset_poll_interval`).
    pub(crate) async fn poll_resets(&self) {
        self.inner.poll_resets_logged().await;
    }

    /// This instance's id.
    pub(crate) fn instance_id(&self) -> Uuid {
        self.inner.instance_id
    }

    /// Partitions with a recorded position not yet written.
    pub(crate) fn dirty_count(&self) -> usize {
        self.inner.dirty_count.load(Ordering::Acquire)
    }

    /// Writes that reached the store with at least one partition.
    pub(crate) fn flushes(&self) -> u64 {
        self.inner.flushes.load(Ordering::Acquire)
    }

    /// Writes that failed and were re-queued (`streams.positions.flush_failures`).
    pub(crate) fn failures(&self) -> u64 {
        self.inner.failures.load(Ordering::Acquire)
    }

    /// Partitions this instance stood down.
    pub(crate) fn contested_count(&self) -> usize {
        self.inner.contested_count.load(Ordering::Acquire)
    }

    /// Partitions in contention with a live second writer, kept or not (the gauge).
    pub(crate) fn contention_count(&self) -> i64 {
        self.inner.contention_count.load(Ordering::Acquire)
    }

    /// Stood-down partitions taken back after their contender left.
    pub(crate) fn recoveries(&self) -> u64 {
        self.inner.recoveries.load(Ordering::Acquire)
    }

    /// Reset markers handed to a read loop and then cleared.
    pub(crate) fn resets_applied(&self) -> u64 {
        self.inner.resets_applied.load(Ordering::Acquire)
    }

    /// Records that moved a partition backwards outside a reset (C# debug assert; a wiring bug).
    pub(crate) fn backwards_records(&self) -> u64 {
        self.inner.backwards.load(Ordering::Acquire)
    }

    /// Whether `partition` is stood down.
    pub(crate) fn is_stood_down(&self, partition: u32) -> bool {
        self.inner.slot(partition).is_some_and(|s| s.stopped)
    }

    /// What was last reported about `partition`'s foreign writer.
    pub(crate) fn reported(&self, partition: u32) -> Reported {
        self.inner
            .slot(partition)
            .map_or(Reported::Nothing, |s| s.reported)
    }
}

impl<S: PositionStore> Drop for PositionFlusher<S> {
    fn drop(&mut self) {
        if let Some(running) = self.running.get_mut().ok().and_then(Option::take) {
            running.cancel.cancel();
        }
    }
}

impl<S: PositionStore> PositionSink for PositionFlusher<S> {
    fn record(&self, partition: u32, id: StreamId) {
        self.inner.record(partition, id);
    }

    fn rewind(&self, partition: u32) {
        self.inner.rewind(partition);
    }

    fn flush(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.inner.flush())
    }
}

/// Re-marks drained partitions dirty unless the write completed (a dropped or failed flush).
struct Drained<'a, S: PositionStore> {
    inner: &'a Inner<S>,
    items: Vec<(u32, StreamId)>,
    done: bool,
}

impl<S: PositionStore> Drop for Drained<'_, S> {
    fn drop(&mut self) {
        if !self.done {
            for &(partition, _) in &self.items {
                self.inner.mark_dirty(partition);
            }
        }
    }
}

/// The timer loop. Each tick: flush when dirty, poll resets, re-arbitrate stood-down partitions.
async fn run<S: PositionStore>(inner: Arc<Inner<S>>, cancel: CancellationToken) {
    let mut ticker =
        tokio::time::interval_at(tokio::time::Instant::now() + inner.interval, inner.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut poll_ticks = 0u32;
    let mut recheck_countdown = inner.recheck_ticks;

    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let body = async {
            if inner.dirty_count.load(Ordering::Acquire) != 0 {
                let _ = inner.flush().await;
            }
            // Outside the dirty check: an idle consumer is the one most likely to be reset.
            if inner.resets.is_some() {
                poll_ticks += 1;
                if poll_ticks >= inner.reset_poll_ticks {
                    poll_ticks = 0;
                    inner.poll_resets_logged().await;
                }
            }
            // A stood-down partition records nothing, so it is never dirty: also outside.
            if inner.recheck_ticks > 0 && inner.contested_count.load(Ordering::Acquire) != 0 {
                recheck_countdown = recheck_countdown.saturating_sub(1);
                if recheck_countdown == 0 {
                    recheck_countdown = inner.recheck_ticks;
                    inner.recheck().await;
                }
            }
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            () = body => {}
        }
    }
}

impl<S: PositionStore> Inner<S> {
    fn slot(&self, partition: u32) -> Option<MutexGuard<'_, Slot>> {
        let slot = self.slots.get(usize::try_from(partition).ok()?)?;
        Some(lock(slot))
    }

    fn set_dirty(&self, slot: &mut Slot) {
        if !slot.dirty {
            slot.dirty = true;
            self.dirty_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn clear_dirty(&self, slot: &mut Slot) {
        if slot.dirty {
            slot.dirty = false;
            self.dirty_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    fn mark_dirty(&self, partition: u32) {
        if let Some(mut slot) = self.slot(partition) {
            self.set_dirty(&mut slot);
        }
    }

    fn record(&self, partition: u32, id: StreamId) {
        let Some(mut slot) = self.slot(partition) else {
            // A wiring bug; losing the position costs redelivery, so the processing path goes on.
            tracing::error!(
                topic = %self.topic,
                consumer = %self.consumer,
                partition,
                "Streams: position recorded for a partition outside the topic's range; ignored"
            );
            return;
        };
        if slot.stopped {
            return;
        }
        if let Some(previous) = slot.pending
            && id < previous
        {
            if slot.rewound {
                // The replay a reset asked for: forgiven once.
                slot.rewound = false;
            } else {
                self.backwards.fetch_add(1, Ordering::AcqRel);
                tracing::error!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    partition,
                    previous = %previous,
                    id = %id,
                    "Streams: a partition's recorded position moved backwards; positions are monotonic per partition"
                );
            }
        }
        slot.pending = Some(id);
        self.set_dirty(&mut slot);
    }

    fn rewind(&self, partition: u32) {
        let Some(mut slot) = self.slot(partition) else {
            return;
        };
        self.clear_dirty(&mut slot);
        slot.pending = None;
        slot.rewound = true;
        slot.reset_hold = false;
    }

    fn is_held(held: Option<&Held>, partition: u32) -> bool {
        held.is_none_or(|h| h.binary_search(&partition).is_ok())
    }

    /// Takes every writable dirty partition, clearing its flag first so a concurrent record
    /// re-marks itself for the next flush.
    fn drain(&self) -> Vec<(u32, StreamId)> {
        let held = self.owned.as_ref().map(|rx| rx.borrow().clone());
        let mut out = Vec::new();
        for (index, slot) in self.slots.iter().enumerate() {
            let mut slot = lock(slot);
            if !slot.dirty || slot.reset_hold {
                // A held reset stays dirty: written once the read loop rewinds.
                continue;
            }
            let partition = u32::try_from(index).expect("slots are indexed by u32 partitions");
            if self.resets.as_ref().is_some_and(|r| r.is_taken(partition)) {
                // Rewound, but its `__reset:<p>` field is not deleted yet: a post-rewind position
                // stored next to a live marker would replay twice after a crash. Stays dirty;
                // written once `clear_taken` deleted the field.
                continue;
            }
            self.clear_dirty(&mut slot);
            if slot.stopped {
                continue;
            }
            if !Self::is_held(held.as_ref(), partition) {
                // No longer ours: a late write would only fight the new owner.
                slot.pending = None;
                continue;
            }
            if let Some(id) = slot.pending {
                out.push((partition, id));
            }
        }
        out
    }

    async fn flush(&self) -> Result<()> {
        // The gate first, then the dirty check: a flush that finds nothing dirty because a
        // concurrent one drained it must still wait for that write to land.
        let _gate = self.gate.lock().await;
        // Taken resets first: their markers are deleted before any post-rewind position is
        // written (see `clear_taken`), including on the final flush at stop.
        if let Err(e) = self.clear_taken().await {
            self.warn_reset_failure(&e);
        }
        if self.dirty_count.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        let mut drained = Drained {
            inner: self,
            items: self.drain(),
            done: false,
        };
        if drained.items.is_empty() {
            drained.done = true;
            return Ok(());
        }
        match self.write(&drained.items).await {
            Ok(before) => {
                drained.done = true;
                self.flushes.fetch_add(1, Ordering::AcqRel);
                if let Some(before) = before {
                    self.inspect(&drained.items, &before).await;
                }
                Ok(())
            }
            Err(e) => {
                let count = drained.items.len();
                drop(drained);
                self.failures.fetch_add(1, Ordering::AcqRel);
                instruments::record_position_flush_failure(&self.topic, &self.consumer);
                tracing::warn!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    partitions = count,
                    error = %e,
                    "Streams: position flush failed for topic {} consumer {} ({count} partitions); retrying in {}ms. Messages already processed may be redelivered if the process stops before a flush succeeds.",
                    self.topic,
                    self.consumer,
                    self.interval.as_millis()
                );
                Err(e)
            }
        }
    }

    /// The store write, with the second-writer `HMGET` sent ahead of it on the same connection.
    /// `Ok(None)` when there is no Redis side or the read failed (the next flush asks again).
    async fn write(&self, items: &[(u32, StreamId)]) -> Result<Option<Vec<Option<String>>>> {
        let Some(redis) = &self.redis else {
            self.store.save(&self.topic, &self.consumer, items).await?;
            return Ok(None);
        };
        let fields: Vec<String> = items.iter().map(|(p, _)| p.to_string()).collect();
        let mut conn = redis.conn.clone();
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(&redis.positions_key).arg(&fields);
        // `join!` polls the read first, which queues it on the multiplexed connection ahead of
        // the store's HSET: the values come back as they were before our write.
        let (before, saved) = tokio::join!(
            cmd.query_async::<Vec<Option<String>>>(&mut conn),
            self.store.save(&self.topic, &self.consumer, items)
        );
        saved?;
        match before {
            Ok(values) => Ok(Some(values)),
            Err(e) => {
                tracing::debug!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    error = %e,
                    "Streams: second-writer read failed; checking again on the next flush"
                );
                Ok(None)
            }
        }
    }

    /// Picks out the foreign writers in the pre-write values and judges them.
    async fn inspect(&self, items: &[(u32, StreamId)], before: &[Option<String>]) {
        let mut candidates: Vec<(u32, PositionRecord)> = Vec::new();
        for (&(partition, _), raw) in items.iter().zip(before) {
            let Some(record) = raw.as_deref().and_then(PositionRecord::parse) else {
                // No previous value (first write) or not a position: nobody to contend with.
                continue;
            };
            if record.instance_id == self.instance_id {
                // Our own id came back: whatever overlap the grace clock was timing has ended.
                if let Some(mut slot) = self.slot(partition) {
                    slot.contested_since = None;
                }
                continue;
            }
            if record.instance_id.is_nil() || record.instance_id == ADMIN_INSTANCE_ID {
                // Unattributed (legacy) or an operator rewind: not a rival.
                continue;
            }
            candidates.push((partition, record));
        }
        if !candidates.is_empty() {
            self.judge(&candidates).await;
        }
    }

    /// Asks the ownership hash whether each foreign writer is alive and who holds the claim.
    async fn judge(&self, candidates: &[(u32, PositionRecord)]) {
        let Some(redis) = &self.redis else {
            return;
        };
        let mut fields: Vec<String> = candidates
            .iter()
            .map(|(_, r)| presence_field(r.instance_id))
            .collect();
        fields.extend(candidates.iter().map(|(p, _)| p.to_string()));
        let mut conn = redis.conn.clone();
        let live: Vec<Option<String>> = match redis::cmd("HMGET")
            .arg(&redis.ownership_key)
            .arg(&fields)
            .query_async(&mut conn)
            .await
        {
            Ok(values) => values,
            Err(e) => {
                // Fail safe: an unanswerable question is not a second writer.
                tracing::warn!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    instance = %self.instance_id,
                    error = %e,
                    "Streams: could not check whether another instance writing topic {} consumer {} is still live; leaving the partition running and re-checking on the next flush.",
                    self.topic,
                    self.consumer
                );
                return;
            }
        };
        let n = candidates.len();
        for (i, &(partition, other)) in candidates.iter().enumerate() {
            if live.get(i).is_none_or(Option::is_none) {
                self.note_stale(partition, other);
                continue;
            }
            if self.exclusive_claims
                && let Some(Some(claim)) = live.get(n + i)
                && PartitionOwner::parse_lenient(partition, claim).instance_id == self.instance_id
            {
                self.note_late_write(partition, other);
                continue;
            }
            if !self.exclusive_claims && self.within_grace(partition, other) {
                continue;
            }
            if self.instance_id < other.instance_id {
                self.hold(partition, other);
            } else {
                self.contest(partition, other);
            }
        }
    }

    /// Static mode: whether an overlap is still inside its grace window.
    fn within_grace(&self, partition: u32, other: PositionRecord) -> bool {
        if self.contested_grace.is_zero() {
            return false;
        }
        let Some(mut slot) = self.slot(partition) else {
            return false;
        };
        let now = Instant::now();
        let Some(since) = slot.contested_since else {
            slot.contested_since = Some(now);
            drop(slot);
            tracing::warn!(
                topic = %self.topic,
                consumer = %self.consumer,
                instance = %self.instance_id,
                partition,
                other = %other.instance_id,
                "Streams: partition {partition} of topic {} consumer {} carried a position written by live instance {}. On a rolling deploy that is the outgoing pod and it ends when the pod does, so this instance keeps the partition for up to {}s before deciding. If it is still there after that, the instance count really is wrong.",
                self.topic,
                self.consumer,
                other.instance_id,
                self.contested_grace.as_secs()
            );
            return true;
        };
        now.duration_since(since) < self.contested_grace
    }

    fn note_stale(&self, partition: u32, other: PositionRecord) {
        let Some(mut slot) = self.slot(partition) else {
            return;
        };
        // Only from Nothing: a real contention must not be downgraded.
        if slot.reported != Reported::Nothing {
            return;
        }
        slot.reported = Reported::Stale;
        drop(slot);
        tracing::info!(
            topic = %self.topic,
            consumer = %self.consumer,
            instance = %self.instance_id,
            partition,
            other = %other.instance_id,
            "Streams: partition {partition} of topic {} consumer {} carried a position written by instance {}, which holds no live ownership claim (a previous life of this consumer, or a position written by an admin tool). Continuing to write it.",
            self.topic,
            self.consumer,
            other.instance_id
        );
    }

    fn note_late_write(&self, partition: u32, other: PositionRecord) {
        let Some(mut slot) = self.slot(partition) else {
            return;
        };
        if slot.reported != Reported::Nothing {
            return;
        }
        slot.reported = Reported::LateWrite;
        drop(slot);
        tracing::info!(
            topic = %self.topic,
            consumer = %self.consumer,
            instance = %self.instance_id,
            partition,
            other = %other.instance_id,
            "Streams: partition {partition} of topic {} consumer {} carried a position written by live instance {}, which no longer holds that partition's claim; this instance does. A late flush from before the handoff, not a second writer. Continuing to write it.",
            self.topic,
            self.consumer,
            other.instance_id
        );
    }

    fn count_contention(&self, slot: &mut Slot) {
        if slot.counted {
            return;
        }
        slot.counted = true;
        let count = self.contention_count.fetch_add(1, Ordering::AcqRel) + 1;
        instruments::set_contested_partitions(&self.metric_key, count);
    }

    /// Keeps a contested partition: this instance won the tiebreak.
    fn hold(&self, partition: u32, other: PositionRecord) {
        let Some(mut slot) = self.slot(partition) else {
            return;
        };
        self.count_contention(&mut slot);
        if slot.reported == Reported::Contention {
            return;
        }
        slot.reported = Reported::Contention;
        drop(slot);
        tracing::error!(
            topic = %self.topic,
            consumer = %self.consumer,
            instance = %self.instance_id,
            partition,
            other = %other.instance_id,
            "Streams: partition {partition} of topic {} consumer {} is also being written by live instance {}, so its messages are being processed twice. This instance ({}) has the lower id and keeps the partition; the other stands down. A partitioned consumer must run as a StatefulSet, not a Deployment; check STREAMS_INSTANCE_COUNT and the pod ordinals.",
            self.topic,
            self.consumer,
            other.instance_id,
            self.instance_id
        );
    }

    /// Stands a partition down: stop writing it, revoke its worker, tell the host.
    fn contest(&self, partition: u32, other: PositionRecord) {
        let Some(mut slot) = self.slot(partition) else {
            return;
        };
        if slot.stopped {
            return;
        }
        slot.stopped = true;
        self.clear_dirty(&mut slot);
        slot.contender = Some(other.instance_id);
        self.count_contention(&mut slot);
        slot.reported = Reported::Contention;
        drop(slot);
        self.contested_count.fetch_add(1, Ordering::AcqRel);

        tracing::error!(
            topic = %self.topic,
            consumer = %self.consumer,
            instance = %self.instance_id,
            partition,
            other = %other.instance_id,
            "Streams: partition {partition} of topic {} consumer {} is being written by instance {} as well as this instance {} (their last position {}). Two writers move the stored position backwards and replay messages, so this instance is standing that partition down rather than fighting for it. A partitioned consumer must run as a StatefulSet, not a Deployment; check STREAMS_INSTANCE_COUNT and the pod ordinals.",
            self.topic,
            self.consumer,
            other.instance_id,
            self.instance_id,
            other.id
        );

        let stop = lock(&self.stops).remove(&partition);
        if let Some(stop) = stop {
            stop.revoke();
        }
        if let Some(callback) = &self.on_contested {
            callback(partition, self.instance_id, other.instance_id);
        }
    }

    /// Re-probes the contenders of stood-down partitions; resumes those whose contender is gone.
    /// Fails closed: a probe error resumes nothing.
    async fn recheck(&self) {
        let Some(redis) = &self.redis else {
            return;
        };
        let waiting: Vec<(u32, Uuid)> = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| {
                let slot = lock(slot);
                let contender = slot.contender.filter(|_| slot.stopped)?;
                Some((u32::try_from(index).ok()?, contender))
            })
            .collect();
        if waiting.is_empty() {
            return;
        }
        let fields: Vec<String> = waiting.iter().map(|(_, id)| presence_field(*id)).collect();
        let mut conn = redis.conn.clone();
        let live: Vec<Option<String>> = match redis::cmd("HMGET")
            .arg(&redis.ownership_key)
            .arg(&fields)
            .query_async(&mut conn)
            .await
        {
            Ok(values) => values,
            Err(e) => {
                tracing::warn!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    instance = %self.instance_id,
                    error = %e,
                    "Streams: could not re-check whether the instance that stood a partition of topic {} consumer {} down is still live; the partition stays stopped and the next tick asks again.",
                    self.topic,
                    self.consumer
                );
                return;
            }
        };
        for (i, &(partition, contender)) in waiting.iter().enumerate() {
            match live.get(i) {
                Some(None) => self.resume(partition, contender),
                Some(Some(_)) => tracing::warn!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    instance = %self.instance_id,
                    partition,
                    other = %contender,
                    "Streams: partition {partition} of topic {} consumer {} is still stood down; instance {contender} holds a live presence claim and is presumed to be writing it. This instance resumes the partition as soon as that instance is gone.",
                    self.topic,
                    self.consumer
                ),
                None => {}
            }
        }
    }

    fn resume(&self, partition: u32, contender: Uuid) {
        let Some(mut slot) = self.slot(partition) else {
            return;
        };
        if !slot.stopped {
            return;
        }
        slot.stopped = false;
        slot.contender = None;
        slot.contested_since = None;
        slot.reported = Reported::Nothing;
        // Whatever was pending before the stand-down is stale now; the new worker records afresh.
        slot.pending = None;
        let was_counted = std::mem::take(&mut slot.counted);
        drop(slot);
        self.contested_count.fetch_sub(1, Ordering::AcqRel);
        self.recoveries.fetch_add(1, Ordering::AcqRel);
        if was_counted {
            let count = self.contention_count.fetch_sub(1, Ordering::AcqRel) - 1;
            instruments::set_contested_partitions(&self.metric_key, count);
        }
        tracing::warn!(
            topic = %self.topic,
            consumer = %self.consumer,
            instance = %self.instance_id,
            partition,
            other = %contender,
            "Streams: partition {partition} of topic {} consumer {} is resuming; instance {contender}, which it stood down for, no longer holds a presence claim, so there is no second writer left to fight. This is the normal end of a rolling deploy that overlapped two pods of one ordinal.",
            self.topic,
            self.consumer
        );
        if let Some(callback) = &self.on_recovered {
            callback(partition, contender);
        }
    }

    async fn poll_resets_logged(&self) {
        if let Err(e) = self.poll_resets().await {
            self.warn_reset_failure(&e);
        }
    }

    fn warn_reset_failure(&self, e: &Error) {
        tracing::warn!(
            topic = %self.topic,
            consumer = %self.consumer,
            error = %e,
            "Streams: reset-marker poll failed for topic {} consumer {}; retrying in {}ms. A pending reset is delayed, not lost: the marker stays in Redis until a worker acts on it.",
            self.topic,
            self.consumer,
            self.interval.as_millis() * u128::from(self.reset_poll_ticks)
        );
    }

    /// The reset tick: notice markers of owned partitions, hand them to the read loops, delete the
    /// ones a read loop has taken. A failure leaves every slot as it was.
    async fn poll_resets(&self) -> Result<()> {
        let (Some(redis), Some(signal)) = (&self.redis, &self.resets) else {
            return Ok(());
        };
        // Serialised with `flush`, which also clears taken markers: a reset requested here while
        // a flush is between its delete and its slot clear must not have its slot cleared there.
        let _gate = self.gate.lock().await;
        let owned = self.owned_partitions();
        // A hold on a partition no longer owned would never be released by a rewind.
        for (index, slot) in self.slots.iter().enumerate() {
            let partition = u32::try_from(index).unwrap_or(u32::MAX);
            if owned.binary_search(&partition).is_err() {
                lock(slot).reset_hold = false;
            }
        }
        if owned.is_empty() {
            return Ok(());
        }

        let fields: Vec<String> = owned.iter().map(|&p| reset_marker::field(p)).collect();
        let mut conn = redis.conn.clone();
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(&redis.positions_key)
            .arg(&fields)
            .query_async(&mut conn)
            .await?;

        for (&partition, raw) in owned.iter().zip(values) {
            let Some(raw) = raw else { continue };
            let Some((target, issued_utc)) = reset_marker::parse(&raw) else {
                continue;
            };
            let marker = ResetMarker {
                partition,
                target,
                issued_utc,
            };
            if signal.request(marker) {
                if let Some(mut slot) = self.slot(partition) {
                    slot.reset_hold = true;
                }
                tracing::warn!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    partition,
                    target = %target,
                    "Streams: reset marker found for topic {} consumer {} partition {partition}; the running worker will restart its reads after {target} (reset issued {}). Entries after that id are delivered again, so handlers must be idempotent. The safe path for a planned replay is still scale to zero, reset, scale up.",
                    self.topic,
                    self.consumer,
                    super::redis_store::format_o(issued_utc)
                );
            }
            lock(&self.marker_values).insert(partition, raw);
        }

        self.clear_taken().await
    }

    fn owned_partitions(&self) -> Vec<u32> {
        let partitions = u32::try_from(self.slots.len()).unwrap_or(u32::MAX);
        match &self.owned {
            Some(rx) => rx
                .borrow()
                .iter()
                .copied()
                .filter(|&p| p < partitions)
                .collect(),
            None => (0..partitions).collect(),
        }
    }

    /// Deletes the `__reset:<p>` field of every owned partition whose reset a read loop has taken
    /// (compare-and-delete: a newer marker survives), then clears those slots. Called under the
    /// gate, by the reset tick and at the start of every flush (so the final flush at stop does
    /// it too), always **before** the partition's post-rewind position is written: `drain` skips a
    /// taken partition. A crash at any point therefore leaves either the marker next to the reset
    /// target (the reset applies once more, replaying nothing persisted past it) or no marker,
    /// never a marker next to a newer position.
    async fn clear_taken(&self) -> Result<()> {
        let (Some(redis), Some(signal)) = (&self.redis, &self.resets) else {
            return Ok(());
        };
        let clear: Vec<(u32, ResetMarker, String)> = {
            let values = lock(&self.marker_values);
            self.owned_partitions()
                .into_iter()
                .filter_map(|p| Some((p, signal.taken(p)?, values.get(&p)?.clone())))
                .collect()
        };
        if clear.is_empty() {
            return Ok(());
        }
        let fields: Vec<(u32, String)> = clear.iter().map(|(p, _, v)| (*p, v.clone())).collect();
        let mut conn = redis.conn.clone();
        clear_markers(&mut conn, &redis.positions_key, &fields).await?;

        // Only now that the field is gone: clearing the slot first would let the next tick see the
        // same marker again and rewind the reader a second time.
        for (partition, marker, _) in clear {
            if signal.clear_taken_if(partition, marker) {
                lock(&self.marker_values).remove(&partition);
                self.resets_applied.fetch_add(1, Ordering::AcqRel);
                tracing::info!(
                    topic = %self.topic,
                    consumer = %self.consumer,
                    partition,
                    target = %marker.target,
                    "Streams: reset marker cleared for topic {} consumer {} partition {partition}; reads restarted after {}.",
                    self.topic,
                    self.consumer,
                    marker.target
                );
            }
        }
        Ok(())
    }
}

/// Deletes each `(partition, value)` marker field only while it still holds `value`. Returns how
/// many fields were deleted.
pub(crate) async fn clear_markers(
    conn: &mut ConnectionManager,
    positions_key: &str,
    markers: &[(u32, String)],
) -> Result<u64> {
    let mut invocation = CLEAR_MARKERS.key(positions_key);
    for (partition, value) in markers {
        invocation.arg(reset_marker::field(*partition)).arg(value);
    }
    Ok(invocation.invoke_async(conn).await?)
}

#[cfg(test)]
#[path = "flusher_tests.rs"]
mod tests;
