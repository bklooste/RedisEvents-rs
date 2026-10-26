//! `delivery = work_queue`: competing consumers over a Redis consumer group. **Owner: C6** (port of
//! `WorkQueueFetch.cs`).
//!
//! # The wire contract (shared with C#, `wire-spec/WIRE.md` §9)
//!
//! A C# and a Rust member can sit in one group on one stream, so these match the C# library
//! exactly:
//!
//! - **Group** = the consumer name, one group per partition stream `s:{topic}:<p>`, created with
//!   `XGROUP CREATE <key> <consumer> <id|$> MKSTREAM` at the resolved start position (`$` for
//!   `start_from = now`, else the exclusive id). `BUSYGROUP` is the normal case and the existing
//!   group's cursor is kept. `NOGROUP` on a read or claim recreates it the same way.
//! - **Member** = this instance's name within the group: the pod name (`POD_NAME`), else the host
//!   name (C# `Environment.MachineName`). Stable across a restart, so a bounced pod finds its own
//!   pending entries again.
//! - **Reads**: `XREADGROUP GROUP <consumer> <member> COUNT <batch_size> STREAMS <key> >`.
//! - **Acks**: one `XACK` per handled batch, after it was processed — exactly where an ordered
//!   consumer would record its position: on success, on a `BestEffort` skip and on a batch the
//!   type filter emptied (filtered entries are acked with their batch). `Block`, `StopPartition`,
//!   `Fail`, a revoked partition and a shutdown that drops a read leave the entries pending.
//! - **Claims**: `XAUTOCLAIM <key> <consumer> <member> <min-idle> <cursor> COUNT 100`, with
//!   min-idle 30 000 ms, cursor walked across sweeps and restarted at `0-0` when Redis wraps it.
//!   No delivery-count limit and no dead-lettering: an entry is redelivered until someone acks it.
//! - **No position hash** and no reset markers: Redis owns the cursor (the group's last-delivered
//!   id) and the pending-entries list (PEL).
//!
//! # Where Rust differs (none of it visible on the wire beyond the commands above)
//!
//! - `read_mode = block` issues `XREADGROUP BLOCK` on the consumer's dedicated reader connection
//!   (C# always polls on the shared connection), and co-located partitions share one multi-stream
//!   `XREADGROUP`. `read_mode = poll` is the C# shape, with the same idle back-off.
//! - On start each member first re-reads its **own** pending entries (`XREADGROUP … STREAMS <key>
//!   <id>`, paging from `0-0`) instead of waiting for them to go idle and claiming them back.
//! - A claim sweep also runs under load once per `claim_min_idle`, not only on the idle path, so a
//!   dead sibling's entries are recovered even while new work keeps arriving.
//! - Entries a sweep hands back that this instance already holds in flight are not delivered a
//!   second time (the claim still resets their idle time, which is harmless).
//! - A failed `XACK` puts its ids back so the retry acks them (C# drops them and leaves the
//!   entries to the claim sweep).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use redis::Value;
use redis::aio::ConnectionManager;

use crate::config::WorkQueueOptions;
use crate::connection::Connection;
use crate::consumer::fetch::{
    self, RawEntry, ReaderConnection, SliceKey, StreamBatch, backoff_next,
};
use crate::consumer::shared::{BoxFuture, PositionSink};
use crate::error::{Error, Result};
use crate::positions::StartPosition;
use crate::wire::{StreamId, codec};

#[cfg(test)]
mod tests;

/// How many pending entries one `XAUTOCLAIM` sweep takes at most (C# `ClaimBatchLimit`).
pub(crate) const CLAIM_BATCH_LIMIT: u32 = 100;

/// Where an `XAUTOCLAIM` scan of the PEL starts, and restarts once Redis wraps it.
const CLAIM_START: &str = "0-0";

// ------------------------------------------------------------------------------------------------
// Member name
// ------------------------------------------------------------------------------------------------

/// This instance's name within the group: `explicit`, else `pod_name`, else the host name.
pub(crate) fn member_name(explicit: Option<&str>, pod_name: Option<&str>) -> String {
    [explicit, pod_name]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map_or_else(machine_name, str::to_owned)
}

/// The host name the way .NET's `Environment.MachineName` reports it on the platforms C# runs on:
/// the kernel host name, cut at the first `.`.
pub(crate) fn machine_name() -> String {
    let raw = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_default();
    short_host(&raw).unwrap_or_else(|| "unknown".to_owned())
}

fn short_host(raw: &str) -> Option<String> {
    let name = raw.trim().split('.').next().unwrap_or("").trim();
    (!name.is_empty()).then(|| name.to_owned())
}

// ------------------------------------------------------------------------------------------------
// PendingAcks
// ------------------------------------------------------------------------------------------------

/// One fetch's ids, waiting for the handler to finish that batch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Segment {
    ids: Vec<StreamId>,
    first: StreamId,
    last: StreamId,
}

/// The fetched-but-unacknowledged batches of one partition, in fetch order (C# `PendingAcks`).
///
/// Written by the read loop and drained by the processor, so it owns a lock (taken once per batch).
/// **Matching is by fetch order and range, never "everything at or below"**: a claim sweep returns
/// entries older than the live batch fetched before it, so the batch just handled is the first one
/// whose `[first, last]` contains the reported id, and only it and the batches ahead of it are
/// acknowledged — and only once the reported id reaches its last entry.
#[derive(Debug, Default)]
pub(crate) struct PendingAcks {
    queue: Mutex<VecDeque<Segment>>,
}

impl PendingAcks {
    /// The most batches one partition holds unacknowledged before the oldest are dropped (their
    /// entries stay in the PEL and come back through the claim sweep or on restart).
    pub(crate) const MAX_PENDING_BATCHES: usize = 1024;

    fn lock(&self) -> MutexGuard<'_, VecDeque<Segment>> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many batches wait to be acknowledged.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// Records one fetch's ids at the tail. Returns how many old batches were dropped to stay
    /// under [`MAX_PENDING_BATCHES`](Self::MAX_PENDING_BATCHES); normally 0.
    pub(crate) fn add(&self, ids: Vec<StreamId>) -> usize {
        let (Some(&first), Some(&last)) = (ids.first(), ids.last()) else {
            return 0;
        };
        let mut queue = self.lock();
        queue.push_back(Segment { ids, first, last });
        let mut dropped = 0;
        while queue.len() > Self::MAX_PENDING_BATCHES {
            queue.pop_front();
            dropped += 1;
        }
        dropped
    }

    /// Whether `id` is held by any pending batch.
    pub(crate) fn contains(&self, id: StreamId) -> bool {
        self.lock().iter().any(|s| s.ids.contains(&id))
    }

    /// Removes and returns the ids a handler reporting `through` has finished, or `None`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn take(&self, through: StreamId) -> Option<Vec<StreamId>> {
        let segments = self.take_segments(through);
        (!segments.is_empty()).then(|| segments.into_iter().flat_map(|s| s.ids).collect())
    }

    fn take_segments(&self, through: StreamId) -> Vec<Segment> {
        let mut queue = self.lock();
        let mut take = 0;
        for (index, s) in queue.iter().enumerate() {
            if s.first <= through && through <= s.last {
                take = if through == s.last { index + 1 } else { index };
                break;
            }
        }
        queue.drain(..take).collect()
    }

    /// Puts segments whose `XACK` failed back at the head, in their order.
    fn restore(&self, segments: Vec<Segment>) {
        let mut queue = self.lock();
        for s in segments.into_iter().rev() {
            queue.push_front(s);
        }
    }

    /// Forgets every pending batch: the group they belong to is gone or was reset.
    pub(crate) fn clear(&self) {
        self.lock().clear();
    }
}

// ------------------------------------------------------------------------------------------------
// Group commands
// ------------------------------------------------------------------------------------------------

fn is_code(e: &redis::RedisError, code: &str) -> bool {
    e.code() == Some(code)
}

/// Whether `error` is Redis answering `NOGROUP` (the group or the key went away).
pub(crate) fn is_no_group(error: &Error) -> bool {
    matches!(error, Error::Redis(e) if is_code(e, "NOGROUP"))
}

fn start_arg(start: StartPosition) -> String {
    match start {
        StartPosition::After(id) => id.to_string(),
        StartPosition::Now => "$".to_owned(),
    }
}

/// `XGROUP CREATE <key> <group> <id|$> MKSTREAM`, tolerating `BUSYGROUP` (the group's cursor is
/// then kept: rewinding it under a live sibling would redeliver its work). `true` = created here.
///
/// # Errors
/// [`Error::Transport`] when Redis refuses for any other reason.
pub(crate) async fn create_group(
    shared: &mut ConnectionManager,
    key: &str,
    group: &str,
    start: StartPosition,
) -> Result<bool> {
    let position = start_arg(start);
    let reply: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(key)
        .arg(group)
        .arg(&position)
        .arg("MKSTREAM")
        .query_async(shared)
        .await;
    match reply {
        Ok(()) => {
            tracing::info!(group, key, position, "Streams: created consumer group.");
            Ok(true)
        }
        Err(e) if is_code(&e, "BUSYGROUP") => {
            tracing::debug!(
                group,
                key,
                "Streams: consumer group already exists; keeping its current position."
            );
            Ok(false)
        }
        Err(e) => Err(Error::transport_with(
            format!(
                "Failed to create consumer group '{group}' on stream '{key}' at position '{position}'."
            ),
            e,
        )),
    }
}

/// Group mode's reset (C# `SetGroupPositionAsync`): `XGROUP SETID` to `last_delivered`, creating
/// the group there when it does not exist, then (with `purge_pending`) deletes every consumer of
/// the group and with them their pending entries. Run it with the consumers stopped. Returns how
/// many consumers were deleted.
///
/// Not wired into `admin` yet, as in C# (where nothing calls it either): it is the seam the
/// reset tests drive.
///
/// # Errors
/// [`Error::Transport`] when Redis refuses the move. A failed purge returns 0.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn set_group_position(
    conn: &Connection,
    key: &str,
    group: &str,
    last_delivered: StreamId,
    purge_pending: bool,
) -> Result<usize> {
    let mut shared = conn.shared();
    let position = last_delivered.to_string();
    let moved: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("SETID")
        .arg(key)
        .arg(group)
        .arg(&position)
        .query_async(&mut shared)
        .await;
    match moved {
        Ok(()) => {}
        Err(e) if is_code(&e, "NOGROUP") => {
            create_group(
                &mut shared,
                key,
                group,
                StartPosition::After(last_delivered),
            )
            .await?;
        }
        Err(e) => {
            return Err(Error::transport_with(
                format!(
                    "Failed to move consumer group '{group}' on stream '{key}' to position '{position}'."
                ),
                e,
            ));
        }
    }
    let purged = if purge_pending {
        purge_consumers(&mut shared, key, group).await
    } else {
        0
    };
    tracing::warn!(
        group,
        key,
        position,
        purged,
        "Streams: consumer group was reset; consumers deleted with their pending entries. Entries after that position \
         will be redelivered."
    );
    Ok(purged)
}

#[cfg_attr(not(test), allow(dead_code))]
async fn purge_consumers(shared: &mut ConnectionManager, key: &str, group: &str) -> usize {
    let summary: redis::RedisResult<Value> = redis::cmd("XPENDING")
        .arg(key)
        .arg(group)
        .query_async(shared)
        .await;
    let Ok(summary) = summary else {
        return 0;
    };
    let mut purged = 0;
    for name in pending_consumers(summary) {
        let deleted: redis::RedisResult<i64> = redis::cmd("XGROUP")
            .arg("DELCONSUMER")
            .arg(key)
            .arg(group)
            .arg(&name)
            .query_async(shared)
            .await;
        if deleted.is_ok() {
            purged += 1;
        }
    }
    purged
}

/// The consumer names of an `XPENDING <key> <group>` summary `[count, min, max, [[name, n], …]]`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn pending_consumers(summary: Value) -> Vec<String> {
    let Value::Array(items) = fetch::unwrap(summary) else {
        return Vec::new();
    };
    let Some(consumers) = items.into_iter().nth(3) else {
        return Vec::new();
    };
    let Value::Array(consumers) = fetch::unwrap(consumers) else {
        return Vec::new();
    };
    consumers
        .into_iter()
        .filter_map(|c| match fetch::unwrap(c) {
            Value::Array(pair) => pair.into_iter().next().and_then(fetch::into_bytes),
            _ => None,
        })
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .collect()
}

/// One parsed `XAUTOCLAIM` reply: the next scan cursor and the claimed entries.
#[derive(Debug)]
pub(crate) struct Claimed {
    pub(crate) cursor: String,
    pub(crate) entries: Vec<RawEntry>,
}

/// Parses `XAUTOCLAIM`'s `[next-cursor, [[id, fields], …], [deleted-id, …]]` (Redis 7; 6.2 has no
/// third element and reports deleted entries as nil, which are skipped).
///
/// # Errors
/// [`Error::Transport`] for any other shape.
pub(crate) fn parse_claim(reply: Value) -> Result<Claimed> {
    let Value::Array(items) = fetch::unwrap(reply) else {
        return Err(fetch::malformed(
            "the XAUTOCLAIM reply was not an array of '[cursor, entries, deleted]'",
        ));
    };
    let mut it = items.into_iter();
    let (Some(cursor), Some(entries)) = (it.next(), it.next()) else {
        return Err(fetch::malformed(
            "the XAUTOCLAIM reply held fewer than two elements",
        ));
    };
    let cursor = fetch::into_bytes(fetch::unwrap(cursor))
        .ok_or_else(|| fetch::malformed("the XAUTOCLAIM cursor was not a string"))?;
    let cursor = String::from_utf8_lossy(&cursor).into_owned();
    StreamId::parse(&cursor).map_err(|e| {
        fetch::malformed(&format!(
            "the XAUTOCLAIM cursor '{cursor}' did not parse: {e}"
        ))
    })?;
    Ok(Claimed {
        cursor,
        entries: fetch::parse_entry_list(entries, true)?,
    })
}

fn xack_cmd(key: &str, group: &str, ids: &[StreamId]) -> redis::Cmd {
    let mut cmd = redis::cmd("XACK");
    cmd.arg(key).arg(group);
    let mut buf = [0u8; 41];
    for id in ids {
        cmd.arg(id.format_into(&mut buf));
    }
    cmd
}

// ------------------------------------------------------------------------------------------------
// The fetch
// ------------------------------------------------------------------------------------------------

/// What a work-queue fetch needs besides its connections and streams.
#[derive(Debug, Clone)]
pub(crate) struct GroupSettings {
    /// The group name: the consumer name.
    pub(crate) group: String,
    /// This instance's name within the group.
    pub(crate) member: String,
    /// Where the group is created (and recreated).
    pub(crate) start: StartPosition,
    pub(crate) batch_size: u32,
    pub(crate) codec_version: u32,
    pub(crate) max_idle_delay_ms: u32,
    pub(crate) claim_min_idle: Duration,
    /// `ZERO` = the idle back-off cap (C#).
    pub(crate) claim_interval: Duration,
}

impl GroupSettings {
    /// Settings from the work-queue options (`claim_interval_ms = 0` resolves here).
    pub(crate) fn new(
        group: String,
        member: String,
        start: StartPosition,
        batch_size: u32,
        codec_version: u32,
        max_idle_delay_ms: u32,
        wq: &WorkQueueOptions,
    ) -> Self {
        let claim_interval = if wq.claim_interval_ms > 0 {
            Duration::from_millis(wq.claim_interval_ms)
        } else {
            Duration::from_millis(u64::from(max_idle_delay_ms.max(1)))
        };
        Self {
            group,
            member,
            start,
            batch_size: batch_size.max(1),
            codec_version,
            max_idle_delay_ms,
            claim_min_idle: Duration::from_millis(wq.claim_min_idle_ms),
            claim_interval,
        }
    }
}

/// One partition stream of a work-queue fetch.
struct GroupStream {
    partition: u32,
    key: String,
    pending: Arc<PendingAcks>,
    /// `Some(after)` while this member's own PEL is still being re-read at startup.
    history: Option<StreamId>,
    claim_cursor: String,
    last_claim: Option<Instant>,
}

enum Live {
    /// `XREADGROUP BLOCK` on the consumer's dedicated reader connection.
    Block(ReaderConnection),
    /// `XREADGROUP` on the shared connection with the idle back-off (the C# shape).
    Poll { idle_delay_ms: u32 },
}

/// One read loop's group fetch over one or more partitions (C# `WorkQueueFetch`).
pub(crate) struct WorkQueueFetch {
    shared: ConnectionManager,
    live: Live,
    settings: GroupSettings,
    streams: Vec<GroupStream>,
}

impl std::fmt::Debug for WorkQueueFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkQueueFetch")
            .field("group", &self.settings.group)
            .field("member", &self.settings.member)
            .field(
                "partitions",
                &self.streams.iter().map(|s| s.partition).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl WorkQueueFetch {
    /// A blocking group fetch: live reads park in Redis for the reader's `block_ms`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for no streams or a duplicate partition; [`Error::Transport`]
    /// for an unsupported codec version.
    pub(crate) fn block(
        conn: &Connection,
        reader: ReaderConnection,
        streams: Vec<(u32, String)>,
        settings: GroupSettings,
    ) -> Result<Self> {
        Self::build(conn, Live::Block(reader), streams, settings)
    }

    /// A polling group fetch on the shared connection (the C# shape).
    ///
    /// # Errors
    /// As [`block`](Self::block).
    pub(crate) fn poll(
        conn: &Connection,
        streams: Vec<(u32, String)>,
        settings: GroupSettings,
    ) -> Result<Self> {
        Self::build(conn, Live::Poll { idle_delay_ms: 0 }, streams, settings)
    }

    fn build(
        conn: &Connection,
        live: Live,
        streams: Vec<(u32, String)>,
        settings: GroupSettings,
    ) -> Result<Self> {
        codec::ensure_supported_version(settings.codec_version)?;
        if streams.is_empty() {
            return Err(Error::invalid_argument(
                "A fetch needs at least one stream key.",
            ));
        }
        for (i, (p, _)) in streams.iter().enumerate() {
            if streams[..i].iter().any(|(o, _)| o == p) {
                return Err(Error::invalid_argument(format!(
                    "Partition {p} appears twice in one fetch."
                )));
            }
        }
        Ok(Self {
            shared: conn.shared(),
            live,
            settings,
            streams: streams
                .into_iter()
                .map(|(partition, key)| GroupStream {
                    partition,
                    key,
                    pending: Arc::new(PendingAcks::default()),
                    history: Some(StreamId::MIN),
                    claim_cursor: CLAIM_START.to_owned(),
                    last_claim: None,
                })
                .collect(),
        })
    }

    /// Skips re-reading this member's own pending entries at startup (the C# behaviour: they come
    /// back through the claim sweep once idle). Tests use it to drive the sweep directly.
    #[cfg(test)]
    pub(crate) fn without_history(mut self) -> Self {
        for s in &mut self.streams {
            s.history = None;
        }
        self
    }

    /// The pending queue of `partition`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn pending(&self, partition: u32) -> Option<Arc<PendingAcks>> {
        self.streams
            .iter()
            .find(|s| s.partition == partition)
            .map(|s| s.pending.clone())
    }

    /// The acknowledger the worker records into: `XACK` on flush.
    pub(crate) fn acker(&self) -> Arc<GroupAcker> {
        Arc::new(GroupAcker {
            shared: self.shared.clone(),
            group: self.settings.group.clone(),
            streams: self
                .streams
                .iter()
                .map(|s| (s.partition, (s.key.clone(), s.pending.clone())))
                .collect(),
            recorded: Mutex::new(BTreeMap::new()),
        })
    }

    /// Creates every stream's group at the start position, tolerating existing ones.
    ///
    /// # Errors
    /// As [`create_group`].
    pub(crate) async fn ensure_groups(&mut self) -> Result<()> {
        for s in &self.streams {
            create_group(
                &mut self.shared,
                &s.key,
                &self.settings.group,
                self.settings.start,
            )
            .await?;
        }
        Ok(())
    }

    /// Moves every stream's group to `last_delivered` (C# `ResetToAsync`) and forgets what was
    /// held unacknowledged, since those ids belong to the cursor just replaced.
    ///
    /// # Errors
    /// As [`set_group_position`].
    #[cfg(test)]
    pub(crate) async fn reset_to(
        &mut self,
        conn: &Connection,
        last_delivered: StreamId,
        purge_pending: bool,
    ) -> Result<()> {
        for s in &mut self.streams {
            set_group_position(
                conn,
                &s.key,
                &self.settings.group,
                last_delivered,
                purge_pending,
            )
            .await?;
            s.pending.clear();
            s.history = None;
        }
        Ok(())
    }

    /// One read: at most one batch per partition. Order of business: this member's own pending
    /// entries (startup only), an overdue claim sweep, the live `XREADGROUP … >`, and on an empty
    /// live read a due claim sweep, then (poll mode) the idle back-off. Cancel-safe in the sense
    /// of C1: dropping it loses nothing — whatever Redis delivered stays in this member's PEL.
    ///
    /// # Errors
    /// [`Error::Redis`] for a failed read (the worker waits transient ones out) and
    /// [`Error::Transport`] for an unreadable reply or a failed group re-create.
    pub(crate) async fn fetch(&mut self) -> Result<Vec<StreamBatch>> {
        if let Some(batch) = self.read_history().await? {
            return Ok(vec![batch]);
        }
        let overdue = self
            .settings
            .claim_min_idle
            .max(self.settings.claim_interval);
        let claimed = self.sweep(overdue).await?;
        if !claimed.is_empty() {
            return Ok(claimed);
        }
        let batches = match self.read_live().await {
            Ok(b) => b,
            Err(e) if is_no_group(&e) => {
                self.on_no_group().await?;
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        if !batches.is_empty() {
            if let Live::Poll { idle_delay_ms } = &mut self.live {
                *idle_delay_ms = 0;
            }
            return Ok(batches);
        }
        let idle_at_cap = match self.live {
            Live::Block(_) => true,
            Live::Poll { idle_delay_ms } => idle_delay_ms >= self.settings.max_idle_delay_ms,
        };
        if idle_at_cap {
            let claimed = self.sweep(self.settings.claim_interval).await?;
            if !claimed.is_empty() {
                if let Live::Poll { idle_delay_ms } = &mut self.live {
                    *idle_delay_ms = 0;
                }
                return Ok(claimed);
            }
        }
        if let Live::Poll { idle_delay_ms } = &mut self.live {
            let delay = *idle_delay_ms;
            *idle_delay_ms = backoff_next(delay, self.settings.max_idle_delay_ms);
            if delay == 0 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(Duration::from_millis(u64::from(delay))).await;
            }
        }
        Ok(Vec::new())
    }

    /// The group (or the key) went away underneath the reader: recreate it at the start position
    /// and drop what was held for the old PEL.
    async fn on_no_group(&mut self) -> Result<()> {
        tracing::warn!(
            group = %self.settings.group,
            "Streams: consumer group is missing on the stream; recreating it at the configured start position."
        );
        self.ensure_groups().await?;
        for s in &mut self.streams {
            s.pending.clear();
            s.history = None;
            CLAIM_START.clone_into(&mut s.claim_cursor);
        }
        Ok(())
    }

    /// Pages through this member's own PEL (`XREADGROUP … STREAMS <key> <after>`) until it is
    /// exhausted, one batch per call.
    async fn read_history(&mut self) -> Result<Option<StreamBatch>> {
        for i in 0..self.streams.len() {
            while let Some(after) = self.streams[i].history {
                let mut cmd = redis::cmd("XREADGROUP");
                cmd.arg("GROUP")
                    .arg(&self.settings.group)
                    .arg(&self.settings.member)
                    .arg("COUNT")
                    .arg(self.settings.batch_size)
                    .arg("STREAMS")
                    .arg(&self.streams[i].key)
                    .arg(after.to_string());
                let reply = match cmd.query_async::<Value>(&mut self.shared).await {
                    Ok(v) => v,
                    Err(e) if is_code(&e, "NOGROUP") => {
                        self.on_no_group().await?;
                        return Ok(None);
                    }
                    Err(e) => return Err(Error::Redis(e)),
                };
                let entries = fetch::parse(reply, self.streams[i].key.as_bytes())?;
                let Some(last) = entries.last().map(|e| e.id) else {
                    self.streams[i].history = None;
                    break;
                };
                if last <= after {
                    return Err(fetch::malformed(&format!(
                        "a pending-history read after {after} returned {last}"
                    )));
                }
                self.streams[i].history = Some(last);
                if let Some(batch) = self.accept(i, entries).await? {
                    tracing::info!(
                        group = %self.settings.group, member = %self.settings.member,
                        key = %self.streams[i].key, count = batch.msgs.len(),
                        "Streams: redelivering this member's own pending entries from before its restart."
                    );
                    return Ok(Some(batch));
                }
            }
        }
        Ok(None)
    }

    fn live_cmd(&self, block_ms: Option<u32>) -> redis::Cmd {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(&self.settings.group)
            .arg(&self.settings.member);
        if let Some(ms) = block_ms {
            cmd.arg("BLOCK").arg(ms);
        }
        cmd.arg("COUNT")
            .arg(self.settings.batch_size)
            .arg("STREAMS");
        for s in &self.streams {
            cmd.arg(&s.key);
        }
        for _ in &self.streams {
            cmd.arg(">");
        }
        cmd
    }

    async fn read_live(&mut self) -> Result<Vec<StreamBatch>> {
        let reply = match &self.live {
            Live::Block(reader) => {
                let reader = reader.clone();
                let cmd = self.live_cmd(Some(reader.block_ms()));
                reader.xread(&cmd).await?.0
            }
            Live::Poll { .. } => {
                let cmd = self.live_cmd(None);
                cmd.query_async::<Value>(&mut self.shared).await?
            }
        };
        let keys: Vec<&[u8]> = self.streams.iter().map(|s| s.key.as_bytes()).collect();
        let slices = fetch::parse_all(reply, &keys)?;
        let mut batches = Vec::with_capacity(slices.len());
        for slice in slices {
            let SliceKey::Requested(slot) = slice.key else {
                continue;
            };
            if let Some(batch) = self.accept(slot, slice.entries).await? {
                batches.push(batch);
            }
        }
        Ok(batches)
    }

    /// Claim sweeps for every stream whose last sweep is at least `every` ago.
    async fn sweep(&mut self, every: Duration) -> Result<Vec<StreamBatch>> {
        let mut out = Vec::new();
        for i in 0..self.streams.len() {
            let due = self.streams[i]
                .last_claim
                .is_none_or(|at| at.elapsed() >= every);
            if !due {
                continue;
            }
            if let Some(batch) = self.claim(i).await? {
                out.push(batch);
            }
        }
        Ok(out)
    }

    /// One `XAUTOCLAIM` sweep of stream `i`. A failed sweep is logged and swallowed: recovery is
    /// opportunistic, and tearing down a healthy reader over it would turn a blip into an outage.
    async fn claim(&mut self, i: usize) -> Result<Option<StreamBatch>> {
        self.streams[i].last_claim = Some(Instant::now());
        let min_idle = u64::try_from(self.settings.claim_min_idle.as_millis()).unwrap_or(u64::MAX);
        let reply = redis::cmd("XAUTOCLAIM")
            .arg(&self.streams[i].key)
            .arg(&self.settings.group)
            .arg(&self.settings.member)
            .arg(min_idle)
            .arg(&self.streams[i].claim_cursor)
            .arg("COUNT")
            .arg(CLAIM_BATCH_LIMIT)
            .query_async::<Value>(&mut self.shared)
            .await;
        let claimed = match reply.map_err(Error::Redis).and_then(parse_claim) {
            Ok(c) => c,
            Err(e) if is_no_group(&e) => {
                self.on_no_group().await?;
                return Ok(None);
            }
            Err(e) => {
                tracing::warn!(
                    group = %self.settings.group, key = %self.streams[i].key, error = %e,
                    "Streams: XAUTOCLAIM sweep failed; entries stay pending and the next sweep retries."
                );
                return Ok(None);
            }
        };
        self.streams[i].claim_cursor = claimed.cursor;
        // Entries this instance already holds in flight (idle because their handler is slow or
        // blocked) were just re-claimed to ourselves; delivering them again would process them
        // twice in one process.
        let pending = self.streams[i].pending.clone();
        let fresh: Vec<RawEntry> = claimed
            .entries
            .into_iter()
            .filter(|e| !pending.contains(e.id))
            .collect();
        if fresh.is_empty() {
            return Ok(None);
        }
        tracing::info!(
            group = %self.settings.group, key = %self.streams[i].key, count = fresh.len(), min_idle_ms = min_idle,
            "Streams: claimed entries idle past the threshold from another member."
        );
        self.accept(i, fresh).await
    }

    /// Decodes a group read's entries for stream `i` and records their ids for the ack. Entries
    /// with no fields (deleted or trimmed while pending) cannot be handled; they are acked at once.
    async fn accept(&mut self, i: usize, entries: Vec<RawEntry>) -> Result<Option<StreamBatch>> {
        let (entries, gone): (Vec<RawEntry>, Vec<RawEntry>) =
            entries.into_iter().partition(|e| !e.fields.is_empty());
        if !gone.is_empty() {
            let ids: Vec<StreamId> = gone.iter().map(|e| e.id).collect();
            let key = self.streams[i].key.clone();
            if let Err(e) = xack_cmd(&key, &self.settings.group, &ids)
                .query_async::<i64>(&mut self.shared)
                .await
            {
                tracing::warn!(key = %key, error = %e, "Streams: failed to acknowledge pending entries that no longer exist.");
            }
        }
        let Some(last) = entries.last().map(|e| e.id) else {
            return Ok(None);
        };
        let partition = self.streams[i].partition;
        let ids: Vec<StreamId> = entries.iter().map(|e| e.id).collect();
        let mut msgs = Vec::with_capacity(entries.len());
        for e in entries {
            msgs.push(codec::decode(
                e.id,
                partition,
                e.fields,
                self.settings.codec_version,
            )?);
        }
        let dropped = self.streams[i].pending.add(ids);
        if dropped > 0 {
            tracing::warn!(
                group = %self.settings.group, key = %self.streams[i].key, dropped,
                limit = PendingAcks::MAX_PENDING_BATCHES,
                "Streams: holding too many unacknowledged batches; dropped the oldest from the ack queue. Those entries stay \
                 in this member's PEL and come back through the claim sweep or on restart."
            );
        }
        Ok(Some(StreamBatch {
            partition,
            msgs,
            last,
        }))
    }
}

// ------------------------------------------------------------------------------------------------
// The acknowledger
// ------------------------------------------------------------------------------------------------

/// Group mode's position sink (C# `GroupAcknowledger`): `record` remembers the last handled id per
/// partition, `flush` acknowledges the batches it finished with one `XACK` per partition.
pub(crate) struct GroupAcker {
    shared: ConnectionManager,
    group: String,
    streams: BTreeMap<u32, (String, Arc<PendingAcks>)>,
    recorded: Mutex<BTreeMap<u32, StreamId>>,
}

impl GroupAcker {
    fn lock(&self) -> MutexGuard<'_, BTreeMap<u32, StreamId>> {
        self.recorded.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn ack(&self) -> Result<()> {
        let work = std::mem::take(&mut *self.lock());
        for (partition, through) in work {
            let Some((key, pending)) = self.streams.get(&partition) else {
                continue;
            };
            let segments = pending.take_segments(through);
            if segments.is_empty() {
                continue;
            }
            let ids: Vec<StreamId> = segments
                .iter()
                .flat_map(|s| s.ids.iter().copied())
                .collect();
            let mut shared = self.shared.clone();
            if let Err(e) = xack_cmd(key, &self.group, &ids)
                .query_async::<i64>(&mut shared)
                .await
            {
                pending.restore(segments);
                return Err(Error::transport_with(
                    format!(
                        "Failed to acknowledge {} entries for consumer group '{}' on stream '{key}'.",
                        ids.len(),
                        self.group
                    ),
                    e,
                ));
            }
        }
        Ok(())
    }
}

impl PositionSink for GroupAcker {
    fn record(&self, partition: u32, id: StreamId) {
        self.lock().insert(partition, id);
    }

    fn rewind(&self, _partition: u32) {}

    fn flush(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(self.ack())
    }
}
