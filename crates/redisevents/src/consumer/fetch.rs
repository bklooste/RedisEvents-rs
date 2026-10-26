//! The fetch layer: blocking `XREAD` on dedicated reader connections
//! ([`Connection::dedicated`](crate::Connection::dedicated)), polling reads, parsing the raw reply
//! into `StreamMsg`s through `wire::codec::decode`
//! without copying bodies, and cancelling a parked read. **Owner: C1** (port of `BlockFetch.cs`,
//! `PollFetch.cs`, `StreamReaderConnection.cs`, `StreamBatch.cs`). Items here are `pub(crate)`
//! unless the lead agrees otherwise.
//!
//! # Shape
//!
//! - `ReaderConnection`: the dedicated, read-only socket a `ReadMode::Block` consumer reads on
//!   Cheap to clone; clones share one socket, which is how the
//!   per-partition read loops of a consumer with `co_locate_partitions = false` share one reader
//!   connection. It can only issue `XREAD`, and it reconnects on its own.
//! - `Fetch`: one read loop's fetch, over one or more partitions (the co-located group is one
//!   multi-stream `XREAD`, the single-partition form is a group of one). It owns the read cursor of
//!   every partition it covers, advances each to the last id it returned, and moves one on
//!   `Fetch::seek` (C# `SeekTo` / `ResetSeek`, for the reset-marker protocol). Built with
//!   `Fetch::block` or `Fetch::poll` (on the shared
//!   connection, with the idle back-off folded in).
//! - `StreamBatch`: what one fetch hands back per partition: decoded messages plus the last id
//!   read. No pooling: the batch owns a `Vec`, and every body is a `Bytes` that took over the
//!   reply's bulk-string buffer (no copy), so a handler may keep messages as long as it likes and the
//!   C# `StreamBatch.Rent`/`Return`/`Copy` lifetime contract has no Rust counterpart.
//!
//! # Cancellation
//!
//! Cancelling = dropping the `Fetch::fetch` future. That is safe at every await point:
//!
//! - **Cursors** move only after a reply has been fully parsed, so a dropped read loses nothing;
//!   the entries it would have returned are read again by the next fetch.
//! - **No stale reply**: redis-rs matches replies to requests in order inside the multiplexed
//!   connection's driver, and the reply to a dropped request is discarded there, never handed to
//!   the next caller. On top of that, a `ReaderConnection` whose read is dropped mid-flight
//!   **discards its socket**: the next read opens a fresh one instead of queueing behind a `BLOCK`
//!   still parked server-side, and the old socket closes once the last clone using it is gone (Redis
//!   then drops the parked read). Reads already in flight on the old socket from other clones finish
//!   normally.
//!
//! Unlike C#, which cannot abort a command on the wire and so waits out at most one `BlockMs` at
//! shutdown, a Rust read loop can stop at once.
//!
//! # Handoff to C2
//!
//! C2 builds one `Fetch` per read loop and calls `Fetch::fetch` in a loop (it never sleeps on
//! its own: both modes own their idling), `Fetch::seek` to apply a reset before the next fetch, and
//! `Fetch::position` to read a cursor. See `docs/CONTRACTS.md` §4, "C1 handoff".

// Consumed by C2 (worker.rs); until it lands, only the tests here use these items.
#![cfg_attr(not(test), allow(dead_code))]

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use redis::Value;
use redis::aio::MultiplexedConnection;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::wire::codec;
use crate::wire::{StreamId, StreamMsg};

#[cfg(test)]
mod parse_tests;
#[cfg(test)]
mod redis_tests;

// ------------------------------------------------------------------------------------------------
// StreamBatch
// ------------------------------------------------------------------------------------------------

/// One partition's share of one fetch: the decoded messages, in stream order, and the id of the
/// last entry read. Never empty when returned by [`Fetch::fetch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamBatch {
    /// The partition the messages were read from.
    pub(crate) partition: u32,
    /// The decoded messages. A type filter (C2) runs over these; [`StreamBatch::last`] still
    /// covers anything it drops, so a batch filtered down to nothing still checkpoints.
    pub(crate) msgs: Vec<StreamMsg>,
    /// The id of the last entry read: the partition's cursor after this batch.
    pub(crate) last: StreamId,
}

// ------------------------------------------------------------------------------------------------
// ReaderConnection
// ------------------------------------------------------------------------------------------------

/// The dedicated, read-only connection a blocking consumer reads on.
///
/// - Opened through [`Connection::dedicated`], so it has **no response timeout**: a `BLOCK` may
///   wait as long as its argument says. (C# raises `syncTimeout` above `N × BlockMs` for N loops
///   queued on one connection; with no client-side timeout there is nothing to raise.)
/// - **Read-only by construction**: the only command it can send is the `XREAD` that [`Fetch`]
///   builds; it hands out neither the socket nor a way to send anything else. A write queued here
///   would wait behind a parked `BLOCK`, and a `MULTI`/`EXEC` cannot span two connections.
/// - **Lazy and self-healing** (C1 owns reconnecting it): the socket opens on the first read, and is
///   discarded and reopened on the next read after an I/O failure or a dropped in-flight read.
/// - **Shared by cloning**. C# opens one per consumer and lets every read loop of that consumer use
///   it; clones here share one socket the same way. Reads from several clones pipeline on it, and
///   Redis serves a blocked client's commands in order, so N loops parked on one connection queue
///   (the reason co-located groups use one multi-stream read instead).
#[derive(Clone)]
pub(crate) struct ReaderConnection {
    inner: Arc<ReaderInner>,
}

struct ReaderInner {
    conn: Connection,
    block_ms: u32,
    /// `CLIENT SETNAME` for every socket this reader opens;
    /// `None` leaves it unnamed.
    client_name: Option<String>,
    /// The live socket and its generation; `None` until the first read and after a discard.
    slot: Mutex<Slot>,
    /// Serialises opening a socket so concurrent first reads open one, not several.
    connecting: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct Slot {
    socket: Option<MultiplexedConnection>,
    generation: u64,
}

impl std::fmt::Debug for ReaderConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderConnection")
            .field("block_ms", &self.inner.block_ms)
            .finish_non_exhaustive()
    }
}

impl ReaderConnection {
    /// A reader over `conn`'s endpoint that issues `XREAD BLOCK block_ms`. Opens nothing yet.
    ///
    /// # Errors
    /// [`Error::Config`] when `block_ms` is 0: `BLOCK 0` (block forever) is never issued, as in C#.
    pub(crate) fn new(conn: &Connection, block_ms: u32) -> Result<Self> {
        Self::named(conn, block_ms, None)
    }

    /// [`new`](Self::new), naming each socket it opens with `CLIENT SETNAME client_name`.
    ///
    /// # Errors
    /// As [`new`](Self::new).
    pub(crate) fn named(
        conn: &Connection,
        block_ms: u32,
        client_name: Option<String>,
    ) -> Result<Self> {
        if block_ms == 0 {
            return Err(Error::config(
                "Streams: BlockMs must be positive (it is 0). BLOCK 0 — block forever — is deliberately never issued: a bounded block keeps the reader's wake-up cadence predictable.",
            ));
        }
        Ok(Self {
            inner: Arc::new(ReaderInner {
                conn: conn.clone(),
                block_ms,
                client_name,
                slot: Mutex::new(Slot::default()),
                connecting: tokio::sync::Mutex::new(()),
            }),
        })
    }

    /// The `BLOCK` timeout, in milliseconds.
    pub(crate) fn block_ms(&self) -> u32 {
        self.inner.block_ms
    }

    fn lock_slot(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.inner
            .slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn current(&self) -> Option<(MultiplexedConnection, u64)> {
        let slot = self.lock_slot();
        slot.socket.clone().map(|s| (s, slot.generation))
    }

    /// The live socket, opening one if there is none.
    async fn socket(&self) -> Result<(MultiplexedConnection, u64)> {
        if let Some(found) = self.current() {
            return Ok(found);
        }
        let _connecting = self.inner.connecting.lock().await;
        if let Some(found) = self.current() {
            return Ok(found);
        }
        let socket = self.inner.conn.dedicated().await.map_err(|e| {
            Error::transport_with(
                "Streams: failed to open the dedicated reader connection.",
                e,
            )
        })?;
        let mut socket = socket;
        if let Some(name) = &self.inner.client_name {
            // A name is diagnostics only: a server that refuses it still serves reads.
            if let Err(e) = redis::cmd("CLIENT")
                .arg("SETNAME")
                .arg(name)
                .query_async::<()>(&mut socket)
                .await
            {
                tracing::debug!(client_name = %name, error = %e, "Streams: CLIENT SETNAME failed on the reader connection.");
            }
        }
        let mut slot = self.lock_slot();
        slot.generation += 1;
        slot.socket = Some(socket.clone());
        Ok((socket, slot.generation))
    }

    /// Drops the socket of `generation` if it is still the live one.
    fn discard(&self, generation: u64) {
        let mut slot = self.lock_slot();
        if slot.generation == generation {
            slot.socket = None;
        }
    }

    /// Drops the live socket, so the next read opens a fresh one. Used when the host stands a
    /// stalled partition down: whatever wedged may be the connection. Reads in flight on the old
    /// socket finish normally.
    pub(crate) fn recycle(&self) {
        self.lock_slot().socket = None;
    }

    /// Sends one `XREAD` (the only command this type can send) and returns the raw reply with the
    /// generation of the socket it came over.
    async fn xread(&self, cmd: &redis::Cmd) -> Result<(Value, u64)> {
        let (mut socket, generation) = self.socket().await?;
        // Discards the socket if this future is dropped before the reply arrives (see the module
        // docs, "Cancellation").
        let mut guard = DiscardOnDrop {
            reader: self,
            generation,
            armed: true,
        };
        let reply = cmd.query_async::<Value>(&mut socket).await;
        guard.armed = false;
        match reply {
            Ok(v) => Ok((v, generation)),
            Err(e) => {
                if e.is_io_error() || e.is_connection_dropped() || e.is_unrecoverable_error() {
                    self.discard(generation);
                }
                Err(Error::Redis(e))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn live_generation(&self) -> Option<u64> {
        let slot = self.lock_slot();
        slot.socket.as_ref().map(|_| slot.generation)
    }

    /// Test-only: the server-side client id of the live socket.
    #[cfg(test)]
    pub(crate) async fn client_id(&self) -> Result<i64> {
        let (mut socket, _) = self.socket().await?;
        Ok(redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut socket)
            .await?)
    }
}

struct DiscardOnDrop<'a> {
    reader: &'a ReaderConnection,
    generation: u64,
    armed: bool,
}

impl Drop for DiscardOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.reader.discard(self.generation);
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Fetch
// ------------------------------------------------------------------------------------------------

/// One stream a [`Fetch`] covers: partition, key, and the exclusive id to read after.
#[derive(Debug, Clone)]
struct Cursor {
    partition: u32,
    key: String,
    after: StreamId,
}

enum Source {
    /// `XREAD BLOCK` on a dedicated reader connection.
    Block(ReaderConnection),
    /// Plain `XREAD` on the shared connection, with an idle back-off.
    Poll {
        shared: redis::aio::ConnectionManager,
        max_idle_delay_ms: u32,
        idle_delay_ms: u32,
    },
}

/// One read loop's fetch over one or more partitions of a topic (C# `BlockFetch` / `PollFetch`,
/// single-key and co-located forms in one type). Not shared: one per read loop.
pub(crate) struct Fetch {
    source: Source,
    cursors: Vec<Cursor>,
    count: u32,
    codec_version: u32,
    #[cfg(test)]
    reads_issued: u64,
}

impl std::fmt::Debug for Fetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetch")
            .field("cursors", &self.cursors)
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

/// `(partition, stream key, start after)` for each stream a fetch covers, in request order.
pub(crate) type StreamStart = (u32, String, StreamId);

impl Fetch {
    /// A blocking fetch: `XREAD BLOCK <block_ms> COUNT <batch_size> STREAMS k1…kN id1…idN` on
    /// `reader`. Returns as soon as any stream has an entry, otherwise after `block_ms` with nothing.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for no streams, a duplicate partition or `batch_size` 0;
    /// [`Error::Transport`] for an unsupported `codec_version`.
    pub(crate) fn block(
        reader: ReaderConnection,
        streams: Vec<StreamStart>,
        batch_size: u32,
        codec_version: u32,
    ) -> Result<Self> {
        Self::build(Source::Block(reader), streams, batch_size, codec_version)
    }

    /// A polling fetch: `XREAD COUNT <batch_size> STREAMS …` on the shared connection, no `BLOCK`.
    /// An empty read waits out the idle back-off (0, 1, 2, 4… ms capped at `max_idle_delay_ms`, the
    /// first step a yield) before returning; a non-empty read resets it.
    ///
    /// # Errors
    /// As [`Fetch::block`].
    pub(crate) fn poll(
        conn: &Connection,
        streams: Vec<StreamStart>,
        batch_size: u32,
        max_idle_delay_ms: u32,
        codec_version: u32,
    ) -> Result<Self> {
        Self::build(
            Source::Poll {
                shared: conn.shared(),
                max_idle_delay_ms,
                idle_delay_ms: 0,
            },
            streams,
            batch_size,
            codec_version,
        )
    }

    fn build(
        source: Source,
        streams: Vec<StreamStart>,
        batch_size: u32,
        codec_version: u32,
    ) -> Result<Self> {
        codec::ensure_supported_version(codec_version)?;
        if streams.is_empty() {
            return Err(Error::invalid_argument(
                "A fetch needs at least one stream key.",
            ));
        }
        if batch_size == 0 {
            return Err(Error::invalid_argument("BatchSize must be at least 1."));
        }
        let cursors: Vec<Cursor> = streams
            .into_iter()
            .map(|(partition, key, after)| Cursor {
                partition,
                key,
                after,
            })
            .collect();
        for (i, c) in cursors.iter().enumerate() {
            if cursors[..i].iter().any(|o| o.partition == c.partition) {
                return Err(Error::invalid_argument(format!(
                    "Partition {} appears twice in one fetch.",
                    c.partition
                )));
            }
        }
        Ok(Self {
            source,
            cursors,
            count: batch_size,
            codec_version,
            #[cfg(test)]
            reads_issued: 0,
        })
    }

    /// The id the next read of `partition` starts after; `None` if this fetch does not cover it.
    pub(crate) fn position(&self, partition: u32) -> Option<StreamId> {
        self.cursors
            .iter()
            .find(|c| c.partition == partition)
            .map(|c| c.after)
    }

    /// Moves `partition`'s cursor so the next read returns entries after `after` (the reset-marker
    /// protocol; C# `SeekTo` / `ResetSeek`). Takes effect on the next [`Fetch::fetch`]; other
    /// partitions' cursors are untouched. In poll mode the idle back-off is reset too, so the new
    /// position is read at once. Returns `false` if this fetch does not cover `partition`.
    pub(crate) fn seek(&mut self, partition: u32, after: StreamId) -> bool {
        let Some(c) = self.cursors.iter_mut().find(|c| c.partition == partition) else {
            return false;
        };
        c.after = after;
        if let Source::Poll { idle_delay_ms, .. } = &mut self.source {
            *idle_delay_ms = 0;
        }
        true
    }

    /// Poll mode: the delay the next consecutive empty read waits, in ms. Block mode: 0.
    pub(crate) fn idle_delay_ms(&self) -> u32 {
        match &self.source {
            Source::Poll { idle_delay_ms, .. } => *idle_delay_ms,
            Source::Block(_) => 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn reads_issued(&self) -> u64 {
        self.reads_issued
    }

    fn command(&self, block_ms: Option<u32>) -> redis::Cmd {
        let mut cmd = redis::cmd("XREAD");
        if let Some(ms) = block_ms {
            cmd.arg("BLOCK").arg(ms);
        }
        cmd.arg("COUNT").arg(self.count).arg("STREAMS");
        for c in &self.cursors {
            cmd.arg(c.key.as_str());
        }
        let mut buf = [0u8; 41];
        for c in &self.cursors {
            cmd.arg(c.after.format_into(&mut buf));
        }
        cmd
    }

    /// One read. Returns one [`StreamBatch`] per partition that had entries, in reply order; an
    /// empty `Vec` when the block expired (block mode) or after the idle wait (poll mode). Each
    /// returned partition's cursor has moved to its batch's `last`.
    ///
    /// Cancel-safe: dropping the future loses nothing (see the module docs).
    ///
    /// # Errors
    /// [`Error::Redis`] when the command fails (the reader reconnects on its next read);
    /// [`Error::Transport`] when the reply is not in the documented `XREAD` shape — never skipped,
    /// because a mis-parsed reply would leave the cursor unmoved and spin the loop.
    pub(crate) async fn fetch(&mut self) -> Result<Vec<StreamBatch>> {
        #[cfg(test)]
        {
            self.reads_issued += 1;
        }
        let (reply, socket) = match &self.source {
            Source::Block(reader) => {
                let reader = reader.clone();
                let cmd = self.command(Some(reader.block_ms()));
                let (reply, generation) = reader.xread(&cmd).await?;
                (reply, Some((reader, generation)))
            }
            Source::Poll { shared, .. } => {
                let mut shared = shared.clone();
                let cmd = self.command(None);
                (cmd.query_async::<Value>(&mut shared).await?, None)
            }
        };
        let batches = match self.accept(reply) {
            Ok(batches) => batches,
            Err(e) => {
                // A reply that does not answer the read sent is evidence the connection's replies
                // have slipped against its requests: retrying on the same socket would only fetch
                // more replies meant for other reads, so the next read opens a fresh one.
                if is_unreadable_reply(&e)
                    && let Some((reader, generation)) = socket
                {
                    reader.discard(generation);
                }
                return Err(e);
            }
        };
        if let Source::Poll {
            max_idle_delay_ms,
            idle_delay_ms,
            ..
        } = &mut self.source
        {
            if batches.is_empty() {
                let delay = *idle_delay_ms;
                *idle_delay_ms = backoff_next(delay, *max_idle_delay_ms);
                if delay == 0 {
                    tokio::task::yield_now().await;
                } else {
                    tokio::time::sleep(Duration::from_millis(u64::from(delay))).await;
                }
            } else {
                *idle_delay_ms = 0;
            }
        }
        Ok(batches)
    }

    /// Parses, validates and decodes `reply`, then advances the cursors. Nothing moves on an
    /// error. Every slice's ids must be strictly after the cursor sent and ascending, or the reply
    /// is not the answer to this read.
    fn accept(&mut self, reply: Value) -> Result<Vec<StreamBatch>> {
        let keys: Vec<&[u8]> = self.cursors.iter().map(|c| c.key.as_bytes()).collect();
        let slices = parse_all(reply, &keys)?;
        for slice in &slices {
            let SliceKey::Requested(slot) = slice.key else {
                continue;
            };
            let mut previous = self.cursors[slot].after;
            for entry in &slice.entries {
                if entry.id <= previous {
                    return Err(malformed(&format!(
                        "entry {} of stream '{}' is not after {previous}, the read's cursor or the entry before it",
                        entry.id, self.cursors[slot].key
                    )));
                }
                previous = entry.id;
            }
        }
        let mut batches = Vec::with_capacity(slices.len());
        let mut foreign = 0usize;
        for slice in slices {
            let SliceKey::Requested(slot) = slice.key else {
                foreign += 1;
                if let SliceKey::Foreign(key) = &slice.key {
                    tracing::warn!(
                        key = %String::from_utf8_lossy(key),
                        entries = slice.entries.len(),
                        "Streams: the XREAD reply carried a stream that was not requested; its entries are ignored."
                    );
                }
                continue;
            };
            let partition = self.cursors[slot].partition;
            let last = slice.entries.last().map_or(StreamId::MIN, |e| e.id);
            let mut msgs = Vec::with_capacity(slice.entries.len());
            for e in slice.entries {
                msgs.push(codec::decode(
                    e.id,
                    partition,
                    e.fields,
                    self.codec_version,
                )?);
            }
            batches.push((
                slot,
                StreamBatch {
                    partition,
                    msgs,
                    last,
                },
            ));
        }
        if batches.is_empty() && foreign > 0 {
            return Err(malformed(&format!(
                "the reply held {foreign} streams, none of them one that was requested"
            )));
        }
        Ok(batches
            .into_iter()
            .map(|(slot, b)| {
                self.cursors[slot].after = b.last;
                b
            })
            .collect())
    }
}

/// The idle back-off ladder: 0 → 1 → 2 → 4 … capped at `max`; `max` 0 pins 0.
pub(crate) fn backoff_next(current: u32, max: u32) -> u32 {
    if max == 0 {
        return 0;
    }
    let next = if current == 0 {
        1
    } else {
        current.saturating_mul(2)
    };
    next.min(max)
}

// ------------------------------------------------------------------------------------------------
// Reply parsing (C# BlockFetch.Parse / ParseAll)
// ------------------------------------------------------------------------------------------------

/// One raw entry: its id and its field/value pairs. Values are `Bytes` that took over the reply's
/// buffers, so nothing is copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawEntry {
    pub(crate) id: StreamId,
    pub(crate) fields: Vec<(Bytes, Bytes)>,
}

/// Which requested key a slice belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SliceKey {
    /// Index into the requested keys.
    Requested(usize),
    /// A key that was not requested, exactly as it arrived.
    Foreign(Vec<u8>),
}

/// One stream's entries out of a multi-stream reply. Never empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Slice {
    pub(crate) key: SliceKey,
    pub(crate) entries: Vec<RawEntry>,
}

/// The marker an [`Error::Transport`] carries when an `XREAD` reply could not be read: the
/// signature of a garbled or desynchronised connection, not of anything in the stream. The read
/// loop waits it out like an outage (C# #52), and a blocking fetch opens a fresh socket.
#[derive(Debug)]
pub(crate) struct UnreadableReply;

impl std::fmt::Display for UnreadableReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the XREAD reply did not answer the read that was sent")
    }
}

impl std::error::Error for UnreadableReply {}

/// Whether `error` is a reply this client could not read (see [`UnreadableReply`]).
pub(crate) fn is_unreadable_reply(error: &Error) -> bool {
    matches!(error, Error::Transport { source: Some(source), .. } if source.is::<UnreadableReply>())
}

fn malformed(detail: &str) -> Error {
    Error::transport_with(
        format!(
            "Streams: the XREAD reply was not in the expected shape — {detail}. Expected '[[streamKey, [[id, [field, value, ...]], ...]], ...]' (RESP2) or the equivalent map (RESP3). Nothing is skipped: the cursor stays put and the read is retried, on a fresh reader connection in block mode."
        ),
        UnreadableReply,
    )
}

/// Strips a RESP3 attribute wrapper.
fn unwrap(v: Value) -> Value {
    match v {
        Value::Attribute { data, .. } => unwrap(*data),
        other => other,
    }
}

/// The reply's elements, or `None` for a non-array (nil, `+OK`, …) reply. A RESP3 map comes back
/// as its pairs.
enum Top {
    Flat(Vec<Value>),
    Pairs(Vec<(Value, Value)>),
}

fn top(reply: Value) -> Option<Top> {
    match unwrap(reply) {
        Value::Array(items) | Value::Set(items) if !items.is_empty() => Some(Top::Flat(items)),
        Value::Map(pairs) if !pairs.is_empty() => Some(Top::Pairs(pairs)),
        _ => None,
    }
}

/// Walks a reply's streams: RESP2 `[key, entries]` pairs and the flattened RESP3 map
/// (`key, entries, key, entries…`) are told apart by shape, since a stream key is never an array;
/// a redis-rs RESP3 `Map` is already pairs.
fn walk(top: Top) -> Result<Vec<(Value, Value)>> {
    match top {
        Top::Pairs(pairs) => Ok(pairs),
        Top::Flat(items) => {
            let mut out = Vec::with_capacity(items.len());
            let mut it = items.into_iter().map(unwrap);
            while let Some(item) = it.next() {
                if let Value::Array(pair) = item {
                    let n = pair.len();
                    let Ok::<[Value; 2], _>([k, e]) = pair.try_into() else {
                        return Err(malformed(&format!(
                            "a stream element held {n} items where '[streamKey, entries]' was expected"
                        )));
                    };
                    out.push((k, e));
                } else {
                    let Some(entries) = it.next() else {
                        return Err(malformed(
                            "a stream key arrived with no entries element after it",
                        ));
                    };
                    out.push((item, entries));
                }
            }
            Ok(out)
        }
    }
}

fn key_bytes(v: &Value) -> Option<&[u8]> {
    match v {
        Value::BulkString(b) => Some(b),
        Value::SimpleString(s) => Some(s.as_bytes()),
        _ => None,
    }
}

fn into_bytes(v: Value) -> Option<Bytes> {
    match unwrap(v) {
        Value::BulkString(b) => Some(Bytes::from(b)),
        Value::SimpleString(s) => Some(Bytes::from(s.into_bytes())),
        Value::Okay => Some(Bytes::from_static(b"OK")),
        _ => None,
    }
}

/// Parses one stream's entries array `[[id, [field, value, …]], …]`.
fn parse_entries(entries: Value) -> Result<Vec<RawEntry>> {
    let Value::Array(items) = unwrap(entries) else {
        return Ok(Vec::new());
    };
    if items.is_empty() {
        return Ok(Vec::new());
    }
    let total = items.len();
    let mut out = Vec::with_capacity(total);
    for entry in items {
        let entry = match unwrap(entry) {
            // Trimmed or deleted between the read and the reply: nothing to hand on.
            Value::Nil => continue,
            Value::Array(e) => e,
            _ => {
                return Err(malformed(
                    "an entry was not an array where '[id, fields]' was expected",
                ));
            }
        };
        let n = entry.len();
        let Ok::<[Value; 2], _>([id, fields]) = entry.try_into() else {
            return Err(malformed(&format!(
                "an entry held {n} items where '[id, fields]' was expected"
            )));
        };
        let id = match unwrap(id) {
            Value::Nil => {
                return Err(malformed(
                    "an entry arrived with no id, so its position could not be recorded",
                ));
            }
            other => {
                let raw =
                    key_bytes(&other).ok_or_else(|| malformed("an entry id was not a string"))?;
                StreamId::parse_bytes(raw).map_err(|e| {
                    malformed(&format!(
                        "an entry id '{}' did not parse: {e}",
                        String::from_utf8_lossy(raw)
                    ))
                })?
            }
        };
        out.push(RawEntry {
            id,
            fields: parse_fields(fields)?,
        });
    }
    if out.is_empty() {
        return Err(malformed(&format!(
            "all {total} entries in the reply were nil, leaving the read cursor with nowhere to advance to"
        )));
    }
    Ok(out)
}

/// Parses one entry's flat `[field, value, …]` list (or a RESP3 map of them).
fn parse_fields(fields: Value) -> Result<Vec<(Bytes, Bytes)>> {
    let pairs: Vec<(Value, Value)> = match unwrap(fields) {
        Value::Array(items) => {
            if items.len() % 2 != 0 {
                return Err(malformed(&format!(
                    "an entry held {} field items, which is not an even field/value list",
                    items.len()
                )));
            }
            let mut out = Vec::with_capacity(items.len() / 2);
            let mut it = items.into_iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                out.push((k, v));
            }
            out
        }
        Value::Map(pairs) => pairs,
        _ => return Ok(Vec::new()),
    };
    pairs
        .into_iter()
        .map(|(k, v)| match (into_bytes(k), into_bytes(v)) {
            (Some(k), Some(v)) => Ok((k, v)),
            _ => Err(malformed("an entry field or value was not a string")),
        })
        .collect()
}

/// Parses a single-key reply: the stream whose key is `expected`, or the
/// only stream if there is just one (a key prefix on the connection would rewrite its text).
///
/// # Errors
/// [`Error::Transport`] for a malformed reply, or several streams none of which is `expected`.
pub(crate) fn parse(reply: Value, expected: &[u8]) -> Result<Vec<RawEntry>> {
    let Some(top) = top(reply) else {
        return Ok(Vec::new());
    };
    let streams = walk(top)?;
    let count = streams.len();
    let mut first = None;
    let mut matched = None;
    for (key, entries) in streams {
        let is_match = matched.is_none()
            && !expected.is_empty()
            && key_bytes(&unwrap(key)).is_some_and(|k| k == expected);
        if is_match {
            matched = Some(entries);
        } else if first.is_none() {
            first = Some(entries);
        }
    }
    let entries = match (matched, count) {
        (Some(e), _) => e,
        (None, 1) => first.unwrap_or_default(),
        (None, n) => {
            return Err(malformed(&format!(
                "the reply held {n} streams, none of them the one that was requested"
            )));
        }
    };
    parse_entries(entries)
}

/// Parses a multi-stream reply into one [`Slice`] per stream that had
/// entries, in reply order, each matched to a requested key on raw bytes. A key that was not
/// requested is carried through as [`SliceKey::Foreign`], except that a single-key request whose
/// reply holds exactly one (unmatched) stream attributes it to that key.
///
/// # Errors
/// [`Error::Transport`] when any stream in the reply is malformed.
pub(crate) fn parse_all(reply: Value, keys: &[&[u8]]) -> Result<Vec<Slice>> {
    let Some(top) = top(reply) else {
        return Ok(Vec::new());
    };
    let streams = walk(top)?;
    let mut slices = Vec::with_capacity(streams.len());
    let mut hint = 0usize;
    let mut unmatched = 0usize;
    for (key, entries) in streams {
        let entries = parse_entries(entries)?;
        if entries.is_empty() {
            continue;
        }
        let key = unwrap(key);
        let raw = key_bytes(&key).unwrap_or_default();
        let slot = (hint..keys.len()).chain(0..hint).find(|&i| keys[i] == raw);
        if let Some(slot) = slot {
            hint = if slot + 1 == keys.len() { 0 } else { slot + 1 };
            slices.push(Slice {
                key: SliceKey::Requested(slot),
                entries,
            });
        } else {
            unmatched += 1;
            slices.push(Slice {
                key: SliceKey::Foreign(raw.to_vec()),
                entries,
            });
        }
    }
    // The single-key rescue, decided once the stream count is known (see C# ParseAll).
    if keys.len() == 1 && slices.len() == 1 && unmatched == 1 {
        slices[0].key = SliceKey::Requested(0);
    }
    Ok(slices)
}
