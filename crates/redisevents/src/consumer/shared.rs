//! The crate-private interface between the partition worker (C2) and the position flusher (C3),
//! and the per-partition stop input the host (C4) drives. **Owner: C2**; agreed with C3. See
//! `docs/CONTRACTS.md`, "C2↔C3 interface".
//!
//! Three pieces:
//!
//! - [`ResetSignal`]: the live reset hand-off. The flusher publishes a pending
//!   reset per partition ([`ResetSignal::request`]); the read loop takes it ([`ResetSignal::try_take`])
//!   right before its next fetch, calls [`PositionSink::rewind`] and then `Fetch::seek`; the flusher
//!   deletes the Redis marker only once [`ResetSignal::try_clear_taken`] says the take happened.
//! - [`PositionSink`]: how the worker reports processed positions. Positions are recorded **when processed, never when
//!   read**.
//! - [`PartitionStop`]: how a partition is stopped from outside: gracefully (shutdown) or because it
//!   was lost (lease lost, rebalanced away, contested). After a revoke the worker records nothing
//!   more for that partition.

// Wired by C3 (flusher) and C4 (host); until then only the worker and tests use parts of it.
#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::positions::reset_marker::ResetMarker;
use crate::wire::StreamId;

// ------------------------------------------------------------------------------------------------
// ResetSignal
// ------------------------------------------------------------------------------------------------

/// One partition's slot: the three states of the hand-off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// Nothing pending.
    Empty,
    /// The flusher published this marker; no read loop has rewound to it yet.
    Pending(ResetMarker),
    /// A read loop rewound to this marker; the flusher may now delete its Redis field.
    Taken(ResetMarker),
}

/// The live reset hand-off between the flusher tick and the read loops.
///
/// One per consumer (host), indexed by partition `0..partition_count`. Shared as `Arc<ResetSignal>`.
///
/// Protocol (`WIRE.md` §7):
/// 1. **Flusher** reads `__reset:<p>` markers each tick and calls [`request`](Self::request) for the
///    partitions **this instance currently owns** (only those: a marker for another instance's
///    partition must stay in Redis for its owner). Re-requesting the same marker is a no-op, so the
///    flusher may call it every tick until it gets to delete the field.
/// 2. **Read loop** checks [`has_pending`](Self::has_pending) (one atomic load) before each fetch;
///    if set, for each partition it reads, [`try_take`](Self::try_take) → [`PositionSink::rewind`] →
///    `Fetch::seek(p, marker.target)`, in that order, before the next fetch.
/// 3. **Flusher** calls [`try_clear_taken`](Self::try_clear_taken) each tick; when it returns the
///    marker, the rewind has happened and the flusher deletes `__reset:<p>` (only if the field still
///    holds that marker's value). A newer marker landing in between is not cleared.
///
/// When a partition is lost, the host (C4) calls [`forget`](Self::forget): a pending reset nobody
/// here will take is dropped (the marker stays in Redis for the next owner, or the next start).
#[derive(Debug)]
pub(crate) struct ResetSignal {
    slots: Box<[Mutex<Slot>]>,
    pending: AtomicUsize,
}

impl ResetSignal {
    /// A signal covering partitions `0..partition_count`.
    pub(crate) fn new(partition_count: u32) -> Self {
        Self {
            slots: (0..partition_count)
                .map(|_| Mutex::new(Slot::Empty))
                .collect(),
            pending: AtomicUsize::new(0),
        }
    }

    fn slot(&self, partition: u32) -> Option<std::sync::MutexGuard<'_, Slot>> {
        let slot = self.slots.get(usize::try_from(partition).ok()?)?;
        Some(slot.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Whether any partition has a reset waiting to be taken. The read loop's per-round check.
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire) > 0
    }

    /// Flusher: publishes a marker read from Redis. Returns `true` when this made a new reset
    /// pending (the cue to log it); `false` when the same marker was already pending or taken, or
    /// the partition is out of range. A different marker replaces whatever was there, taken or not:
    /// the newest instruction wins.
    pub(crate) fn request(&self, marker: ResetMarker) -> bool {
        let Some(mut slot) = self.slot(marker.partition) else {
            return false;
        };
        match *slot {
            Slot::Pending(m) | Slot::Taken(m) if m == marker => false,
            Slot::Pending(_) => {
                *slot = Slot::Pending(marker);
                true
            }
            Slot::Empty | Slot::Taken(_) => {
                *slot = Slot::Pending(marker);
                self.pending.fetch_add(1, Ordering::AcqRel);
                true
            }
        }
    }

    /// Read loop: takes `partition`'s pending reset, if any. The caller must then rewind the
    /// flusher ([`PositionSink::rewind`]) and seek its fetch before issuing the next read.
    pub(crate) fn try_take(&self, partition: u32) -> Option<ResetMarker> {
        let mut slot = self.slot(partition)?;
        let Slot::Pending(marker) = *slot else {
            return None;
        };
        *slot = Slot::Taken(marker);
        self.pending.fetch_sub(1, Ordering::AcqRel);
        Some(marker)
    }

    /// Whether `partition`'s reset was taken and now waits for its marker to be deleted.
    pub(crate) fn is_taken(&self, partition: u32) -> bool {
        self.slot(partition)
            .is_some_and(|s| matches!(*s, Slot::Taken(_)))
    }

    /// Flusher: clears a taken slot and returns its marker, which is now safe to delete from Redis.
    /// `None` when the slot is empty or still pending.
    pub(crate) fn try_clear_taken(&self, partition: u32) -> Option<ResetMarker> {
        let mut slot = self.slot(partition)?;
        let Slot::Taken(marker) = *slot else {
            return None;
        };
        *slot = Slot::Empty;
        Some(marker)
    }

    /// Flusher: [`try_clear_taken`](Self::try_clear_taken), but only while the slot is still taken
    /// for exactly `marker` (the one whose field was just deleted). A reset re-requested and taken
    /// again in between keeps its slot, so its own marker is deleted before the slot clears.
    pub(crate) fn clear_taken_if(&self, partition: u32, marker: ResetMarker) -> bool {
        let Some(mut slot) = self.slot(partition) else {
            return false;
        };
        if *slot != Slot::Taken(marker) {
            return false;
        }
        *slot = Slot::Empty;
        true
    }

    /// The marker `partition`'s reset was taken for, if it waits for its field to be deleted.
    pub(crate) fn taken(&self, partition: u32) -> Option<ResetMarker> {
        match *self.slot(partition)? {
            Slot::Taken(marker) => Some(marker),
            _ => None,
        }
    }

    /// Host: `partition` is no longer read here. Drops a pending reset (its marker stays in Redis)
    /// and a taken one (the flusher no longer clears it; the next owner's cold start applies it again,
    /// which only replays).
    pub(crate) fn forget(&self, partition: u32) {
        let Some(mut slot) = self.slot(partition) else {
            return;
        };
        if matches!(*slot, Slot::Pending(_)) {
            self.pending.fetch_sub(1, Ordering::AcqRel);
        }
        *slot = Slot::Empty;
    }
}

// ------------------------------------------------------------------------------------------------
// PositionSink
// ------------------------------------------------------------------------------------------------

/// A boxed `Send` future, for the object-safe [`PositionSink::flush`].
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where the worker reports positions (C3's `PositionFlusher` implements it). Shared as `Arc<dyn PositionSink>`.
///
/// Rules the worker keeps:
/// - [`record`](Self::record) is called **only after** a batch (or, under `SyncMessage`, a message)
///   was handled successfully, or skipped under `ErrorPolicy::BestEffort`, or filtered down to
///   nothing; never at read time. Ids per partition are non-decreasing except right after a
///   [`rewind`](Self::rewind).
/// - Under `PersistMode::None` the worker never calls `record` or `flush`.
/// - Under `SyncBatch` / `SyncMessage` it awaits [`flush`](Self::flush) after each `record`. A
///   flush error is treated like a handler error (`HandleError::from`: `Config`/`Transport` block,
///   anything else skips).
/// - After a partition's [`PartitionStop::revoke`], the worker records nothing more for it.
///   The flusher keeps its own guard too (a partition it stood down as contested ignores `record`).
pub(crate) trait PositionSink: Send + Sync + 'static {
    /// Records `id` as `partition`'s last processed entry. Must not block or await.
    fn record(&self, partition: u32, id: StreamId);

    /// The read loop took a reset for `partition` and is about to seek: drop any pending,
    /// un-flushed position recorded before it. Called synchronously before the seek, so a record
    /// after this call is always for an entry read from the new position.
    fn rewind(&self, partition: u32);

    /// Persists everything recorded so far and resolves when the write has landed.
    fn flush(&self) -> BoxFuture<'_, Result<()>>;

    /// The worker entered (`true`) or left (`false`) the `HandleError::Block` back-off on
    /// `partition`. Health hook: the host forwards it to the partition's monitor. Nothing by default.
    fn blocked(&self, _partition: u32, _blocked: bool) {}

    /// `partition` stood down on a handler failure (`StopPartition` / `Fail`). Health hook, called at the failure so the state is visible before the worker
    /// exits. Nothing by default.
    fn stopped(&self, _partition: u32, _reason: &str) {}
}

/// A sink that keeps nothing: `PersistMode::None`, work-queue mode, and tests.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct NoPositions;

impl PositionSink for NoPositions {
    fn record(&self, _partition: u32, _id: StreamId) {}

    fn rewind(&self, _partition: u32) {}

    fn flush(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

// ------------------------------------------------------------------------------------------------
// PartitionStop
// ------------------------------------------------------------------------------------------------

/// Stops one partition's work from outside the worker. Cheap to clone; the host keeps one per
/// owned partition and hands a clone to the worker. Its token is the one handlers see in
/// [`PartitionContext::cancellation`](super::PartitionContext::cancellation).
///
/// - [`shutdown`](Self::shutdown): graceful. The read loop stops; a batch already being handled
///   finishes and is recorded if it succeeds; batches already queued (pooled mode) are still
///   handed to the handler with a cancelled token (C# drain). A handler error after the token
///   fired is a shutdown, not a failure: no log, no metric, no advance.
/// - [`revoke`](Self::revoke): the partition is no longer ours (lease lost, rebalanced away,
///   contested). Same as shutdown, except nothing queued is handled and **nothing more is
///   recorded**, not even the result of the batch in flight. The backlog replays for the new owner.
#[derive(Debug, Clone)]
pub(crate) struct PartitionStop {
    token: CancellationToken,
    revoked: Arc<AtomicBool>,
}

impl PartitionStop {
    /// A stop whose token is a child of `parent` (the host token: shutting the host down stops
    /// every partition gracefully).
    pub(crate) fn new(parent: &CancellationToken) -> Self {
        Self {
            token: parent.child_token(),
            revoked: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A stop with no parent (tests, harnesses).
    pub(crate) fn detached() -> Self {
        Self::new(&CancellationToken::new())
    }

    /// The token handlers see.
    pub(crate) fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Graceful stop.
    pub(crate) fn shutdown(&self) {
        self.token.cancel();
    }

    /// The partition was lost: stop and record nothing more.
    pub(crate) fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
        self.token.cancel();
    }

    /// Whether the token fired (shutdown or revoke).
    pub(crate) fn is_stopped(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Whether [`revoke`](Self::revoke) was called.
    pub(crate) fn is_revoked(&self) -> bool {
        self.revoked.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    fn marker(partition: u32, ms: u64) -> ResetMarker {
        ResetMarker {
            partition,
            target: StreamId::new(ms, 0),
            issued_utc: OffsetDateTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn request_take_clear_walks_the_three_states() {
        let s = ResetSignal::new(4);
        assert!(!s.has_pending());
        assert!(s.request(marker(2, 5)));
        assert!(
            !s.request(marker(2, 5)),
            "re-requesting the same marker is a no-op"
        );
        assert!(s.has_pending());
        assert_eq!(
            s.try_clear_taken(2),
            None,
            "not taken yet: the marker must stay"
        );
        assert_eq!(s.try_take(1), None);
        assert_eq!(s.try_take(2), Some(marker(2, 5)));
        assert!(!s.has_pending());
        assert!(s.is_taken(2));
        assert!(
            !s.request(marker(2, 5)),
            "a taken marker re-read before deletion stays taken"
        );
        assert_eq!(s.try_take(2), None);
        assert_eq!(s.try_clear_taken(2), Some(marker(2, 5)));
        assert!(!s.is_taken(2));
    }

    #[test]
    fn a_newer_marker_replaces_a_taken_one_and_blocks_its_clear() {
        let s = ResetSignal::new(1);
        s.request(marker(0, 1));
        s.try_take(0);
        assert!(s.request(marker(0, 2)));
        assert!(s.has_pending());
        assert_eq!(s.try_clear_taken(0), None);
        assert_eq!(s.try_take(0), Some(marker(0, 2)));
    }

    #[test]
    fn a_newer_pending_marker_replaces_without_double_counting() {
        let s = ResetSignal::new(1);
        s.request(marker(0, 1));
        s.request(marker(0, 2));
        assert_eq!(s.try_take(0), Some(marker(0, 2)));
        assert!(!s.has_pending());
    }

    #[test]
    fn out_of_range_and_forget() {
        let s = ResetSignal::new(2);
        assert!(!s.request(marker(9, 1)));
        assert_eq!(s.try_take(9), None);
        s.request(marker(1, 1));
        s.forget(1);
        assert!(!s.has_pending());
        assert_eq!(s.try_take(1), None);
    }

    #[test]
    fn revoke_is_a_stop_that_remembers_why() {
        let host = CancellationToken::new();
        let a = PartitionStop::new(&host);
        let b = PartitionStop::new(&host);
        a.revoke();
        assert!(a.is_stopped() && a.is_revoked());
        assert!(!b.is_stopped());
        host.cancel();
        assert!(b.is_stopped() && !b.is_revoked());
    }
}
