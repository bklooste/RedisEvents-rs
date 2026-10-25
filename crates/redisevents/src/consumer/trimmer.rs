//! The background trimmer, with the position interlock (trim only below the minimum position of
//! every consumer). **Owner: C5** (port of `BackgroundTrimmer.cs`). Crate-private: C4 wires it.
//!
//! # The interlock
//! Each sweep, per topic, `SCAN`s `p:{topic}:*` (never `KEYS`) for every consumer's positions hash
//! (`:meta` hashes excluded) and loads each one. For each partition the **clamp** is the minimum
//! over all those consumers of the stored position; a consumer with a hash but no field for the
//! partition counts as [`StreamId::MIN`] (it has processed nothing). The `MINID` sent is
//! `min(clamp, cutoff)` where `cutoff = now - RetentionSeconds`, so entries a registered consumer has
//! not yet processed are never trimmed. There is no staleness timeout: C# never treats an old
//! position as abandoned. The only override is the release valve: when the partition holds at least
//! `ClampReleaseThreshold * MaxLen` entries the clamp is dropped, `MINID <cutoff>` is applied, then a
//! length-derived `MAXLEN` trim frees memory, and `streams.clamp.released` fires once per release.
//! If the scan fails the sweep fails and nothing is trimmed.

#![allow(
    dead_code,
    reason = "crate-private surface consumed by the C4 host; exercised by this module's tests"
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::{StreamOptions, TopicOptions, TrimMode};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::positions::PositionRecord;
use crate::positions::redis_store::hgetall;
use crate::wire::codec::{max_len_args, min_id_args};
use crate::wire::{Keys, StreamId};

const SCAN_PAGE_SIZE: usize = 100;
const META_SUFFIX: &str = ":meta";
/// `Length` when no clamp was in force and `XLEN` was never read.
pub(crate) const LENGTH_NOT_READ: i64 = -1;

/// What one sweep did to one partition (C# `PartitionTrimResult`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionTrimResult {
    pub partition: u32,
    /// `XLEN` when the clamp was evaluated, or [`LENGTH_NOT_READ`].
    pub length: i64,
    /// The id retention alone asked for.
    pub cutoff: StreamId,
    /// Lowest recorded position of any consumer, [`StreamId::MIN`] if one has none.
    pub clamp: StreamId,
    /// The `MINID` actually sent; under a release, the id of the oldest surviving entry.
    pub min_id: StreamId,
    /// Entries `XTRIM` reported deleted.
    pub removed: u64,
    /// The consumer that set the clamp, if any.
    pub held_by: Option<String>,
    pub clamp_released: bool,
}

/// What one sweep did to one topic (C# `TopicTrimResult`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicTrimResult {
    pub topic: String,
    pub cutoff_utc: OffsetDateTime,
    pub partitions: Vec<PartitionTrimResult>,
}

impl TopicTrimResult {
    pub(crate) fn removed(&self) -> u64 {
        self.partitions.iter().map(|p| p.removed).sum()
    }

    pub(crate) fn any_clamp_released(&self) -> bool {
        self.partitions.iter().any(|p| p.clamp_released)
    }
}

/// Periodic time-based trim of every enabled topic's partition streams.
///
/// `start`/`stop` are the handle C4 uses; both are idempotent. `stop` cancels the loops and waits
/// for them; a sweep in flight finishes its current command.
pub(crate) struct BackgroundTrimmer {
    conn: Connection,
    keys: Keys,
    topics: HashMap<String, TopicOptions>,
    /// Partitions currently in the released state (the metric counts transitions into it).
    released: Mutex<HashSet<(String, u32)>>,
    released_total: AtomicU64,
    running: Option<Running>,
}

struct Running {
    cancel: CancellationToken,
    loops: Vec<JoinHandle<()>>,
}

impl BackgroundTrimmer {
    pub(crate) fn new(conn: Connection, keys: Keys, options: &StreamOptions) -> Self {
        Self {
            conn,
            keys,
            topics: options.topics.clone(),
            released: Mutex::new(HashSet::new()),
            released_total: AtomicU64::new(0),
            running: None,
        }
    }

    /// Whether the trimmer does anything for `topic`: an interval, a retention window and a trim
    /// mode that deletes.
    pub(crate) fn is_enabled(topic: &TopicOptions) -> bool {
        topic.background_trim_interval_seconds > 0
            && topic.retention_seconds.is_some_and(|r| r > 0)
            && topic.trim != TrimMode::None
    }

    /// Enabled topic names, sorted.
    pub(crate) fn topics(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .topics
            .iter()
            .filter(|(_, t)| Self::is_enabled(t))
            .map(|(n, _)| n.clone())
            .collect();
        names.sort();
        names
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.is_some()
    }

    /// Number of clamp releases reported so far (mirrors `streams.clamp.released`).
    pub(crate) fn released_total(&self) -> u64 {
        self.released_total.load(Ordering::Relaxed)
    }

    /// Starts one loop per enabled topic. A no-op when running or when no topic is enabled.
    pub(crate) fn start(self: &std::sync::Arc<Self>) -> StartedTrimmer {
        StartedTrimmer {
            trimmer: std::sync::Arc::clone(self),
            running: Mutex::new(self.spawn_loops()),
        }
    }

    fn spawn_loops(self: &std::sync::Arc<Self>) -> Option<Running> {
        self.warn_about_ineffective_configuration();
        let enabled = self.topics();
        if enabled.is_empty() {
            return None;
        }
        let cancel = CancellationToken::new();
        let mut loops = Vec::with_capacity(enabled.len());
        for topic in enabled {
            let options = self.topics[&topic].clone();
            info!(
                topic = %topic,
                retention_seconds = ?options.retention_seconds,
                interval_seconds = options.background_trim_interval_seconds,
                max_len = options.max_len(),
                trim = ?options.trim,
                clamp_release_threshold = options.clamp_release_threshold,
                release_at = release_at(&options),
                "Streams: background trimmer started; time-based retention is clamped to the slowest stored consumer position."
            );
            let this = std::sync::Arc::clone(self);
            let token = cancel.clone();
            loops.push(tokio::spawn(async move {
                this.run_loop(topic, options, token).await;
            }));
        }
        Some(Running { cancel, loops })
    }

    async fn run_loop(&self, topic: String, options: TopicOptions, cancel: CancellationToken) {
        let period = Duration::from_secs(u64::from(options.background_trim_interval_seconds));
        // First sweep after one period, like C# `PeriodicTimer`.
        let mut ticker = tokio::time::interval_at(Instant::now() + period, period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = ticker.tick() => {}
            }
            match self.trim(&topic, &options, &cancel).await {
                Ok(_) => {}
                Err(_) if cancel.is_cancelled() => return,
                Err(e) => error!(
                    topic = %topic,
                    interval_seconds = options.background_trim_interval_seconds,
                    error = %e,
                    "Streams: background trim sweep failed; retrying next interval. Time-based retention is not being applied to this topic until it succeeds, so the stream is held by MaxLen alone."
                ),
            }
        }
    }

    /// One sweep of `topic` now (C# `TrimTopicOnceAsync`).
    ///
    /// # Errors
    /// [`Error::Config`] without a retention window (the cutoff would be "now", trimming
    /// everything); [`Error::Redis`] when a command fails.
    pub(crate) async fn trim_topic_once(&self, topic: &str) -> Result<TopicTrimResult> {
        if topic.trim().is_empty() {
            return Err(Error::invalid_argument("topic must not be empty"));
        }
        let options = self.topics.get(topic).cloned().unwrap_or_default();
        if options.retention_seconds.is_none_or(|r| r == 0) {
            return Err(Error::config(format!(
                "Streams:Topics:{topic}:RetentionSeconds is not set, so there is no time window to trim to. The background trimmer applies time-based retention only; length-based trimming rides on every XADD."
            )));
        }
        self.trim(topic, &options, &CancellationToken::new()).await
    }

    async fn trim(
        &self,
        topic: &str,
        options: &TopicOptions,
        cancel: &CancellationToken,
    ) -> Result<TopicTrimResult> {
        let cutoff_utc = cutoff_for(options, OffsetDateTime::now_utc());
        let cutoff = StreamId::from_time(cutoff_utc);
        let positions = self.read_positions(topic).await?;

        let mut partitions = Vec::with_capacity(options.partitions as usize);
        for partition in 0..options.partitions {
            if cancel.is_cancelled() {
                return Err(Error::invalid_argument("trim sweep cancelled"));
            }
            partitions.push(
                self.trim_partition(topic, options, partition, cutoff, &positions)
                    .await?,
            );
        }
        Ok(TopicTrimResult {
            topic: topic.to_owned(),
            cutoff_utc,
            partitions,
        })
    }

    async fn trim_partition(
        &self,
        topic: &str,
        options: &TopicOptions,
        partition: u32,
        cutoff: StreamId,
        positions: &[ConsumerPositions],
    ) -> Result<PartitionTrimResult> {
        let key = self
            .keys
            .stream(topic, partition, options.co_locate_partitions);
        let (clamp, holder) = clamp_for(positions, partition);

        // The clamp only matters when it is behind what retention asked for.
        let held = holder.is_some() && clamp < cutoff;
        let mut min_id = if held { clamp } else { cutoff };
        let mut length = LENGTH_NOT_READ;
        let mut released = false;
        let mut release_to = 0_i64;

        if held {
            let holder_name = holder.as_deref().unwrap_or_default();
            length = self.xlen(&key).await?;
            let release_at = release_at(options);
            if length >= release_at {
                // THE VALVE: one stuck consumer must not hold the stream above the memory ceiling.
                // This deletes that consumer's unprocessed backlog on purpose.
                min_id = cutoff;
                released = true;
                release_to = release_target(options.max_len(), release_at, length);
                self.report_release(
                    topic,
                    options,
                    partition,
                    holder_name,
                    clamp,
                    cutoff,
                    length,
                    release_at,
                    release_to,
                );
            } else {
                self.report_release_ended(topic, partition);
                if length > 0 {
                    warn!(
                        topic = %topic, partition, consumer = %holder_name, position = %clamp,
                        cutoff = %cutoff, length, release_at,
                        "Streams: trim is held back by a consumer; retention alone would trim further. The clamp is released at {release_at} entries, above which the backlog is dropped to protect memory."
                    );
                }
            }
        } else {
            self.report_release_ended(topic, partition);
        }

        let mut removed = if min_id > StreamId::MIN {
            self.xtrim(&key, &min_id_args(options.trim, min_id)).await?
        } else {
            0
        };

        if released {
            // The half that actually frees memory: MINID <cutoff> removes nothing when every entry
            // is newer than the cutoff.
            let target = u64::try_from(release_to).unwrap_or(1);
            removed += self
                .xtrim(&key, &max_len_args(options.trim, target))
                .await?;
            if let Some(head) = self.head_id(&key).await? {
                min_id = head;
            }
        }

        Ok(PartitionTrimResult {
            partition,
            length,
            cutoff,
            clamp,
            min_id,
            removed,
            held_by: holder,
            clamp_released: released,
        })
    }

    async fn xlen(&self, key: &str) -> Result<i64> {
        let mut conn = self.conn.shared();
        Ok(redis::cmd("XLEN").arg(key).query_async(&mut conn).await?)
    }

    async fn xtrim(&self, key: &str, args: &[String]) -> Result<u64> {
        if args.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn.shared();
        let removed: Option<u64> = redis::cmd("XTRIM")
            .arg(key)
            .arg(args)
            .query_async(&mut conn)
            .await?;
        Ok(removed.unwrap_or(0))
    }

    async fn head_id(&self, key: &str) -> Result<Option<StreamId>> {
        let mut conn = self.conn.shared();
        let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut conn)
            .await?;
        Ok(reply.ids.first().and_then(|e| StreamId::parse(&e.id).ok()))
    }

    #[allow(clippy::too_many_arguments, reason = "mirrors the C# report signature")]
    fn report_release(
        &self,
        topic: &str,
        options: &TopicOptions,
        partition: u32,
        holder: &str,
        clamp: StreamId,
        cutoff: StreamId,
        length: i64,
        release_at: i64,
        release_to: i64,
    ) {
        let first = self
            .released
            .lock()
            .expect("released set")
            .insert((topic.to_owned(), partition));
        if !first {
            debug!(
                topic = %topic, partition, consumer = %holder, position = %clamp, release_to,
                "Streams: trim clamp is still released; the consumer is still stuck."
            );
            return;
        }
        self.released_total.fetch_add(1, Ordering::Relaxed);
        crate::diagnostics::instruments::record_clamp_released(topic, partition, holder);
        error!(
            topic = %topic, partition, consumer = %holder, position = %clamp, length, release_at,
            clamp_release_threshold = options.clamp_release_threshold, max_len = options.max_len(),
            cutoff = %cutoff, release_to,
            "Streams: trim clamp RELEASED - consumer {holder} is stuck and the partition holds at least the release point; its unprocessed backlog on this partition is being deleted deliberately, because holding it would run Redis out of memory. Fix or restart the consumer, or raise MaxLen. Logged once per release, not once per sweep."
        );
    }

    fn report_release_ended(&self, topic: &str, partition: u32) {
        if self
            .released
            .lock()
            .expect("released set")
            .remove(&(topic.to_owned(), partition))
        {
            info!(
                topic = %topic, partition,
                "Streams: trim clamp release has ended; the clamp is being honoured again."
            );
        }
    }

    /// Every consumer's positions for `topic`, found with `SCAN` (never `KEYS`).
    async fn read_positions(&self, topic: &str) -> Result<Vec<ConsumerPositions>> {
        let pattern = self.keys.positions_pattern(topic);
        let prefix = pattern.trim_end_matches('*').to_owned();
        let mut conn = self.conn.shared();

        let mut names: HashSet<String> = HashSet::new();
        let mut cursor: u64 = 0;
        loop {
            let (next, page): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_PAGE_SIZE)
                .query_async(&mut conn)
                .await?;
            for raw in page {
                let text = String::from_utf8_lossy(&raw);
                if text.len() <= prefix.len() || text.ends_with(META_SUFFIX) {
                    continue;
                }
                names.insert(text[prefix.len()..].to_owned());
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        let mut out = Vec::with_capacity(names.len());
        for consumer in names {
            let entries = hgetall(&self.conn, &self.keys.positions(topic, &consumer)).await?;
            out.push(ConsumerPositions {
                consumer,
                positions: crate::positions::redis_store::read_positions(&entries),
            });
        }
        Ok(out)
    }

    fn warn_about_ineffective_configuration(&self) {
        for (name, topic) in &self.topics {
            if topic.background_trim_interval_seconds == 0 || Self::is_enabled(topic) {
                continue;
            }
            if topic.retention_seconds.is_none_or(|r| r == 0) {
                warn!(
                    topic = %name,
                    "Streams:Topics:{name}:BackgroundTrimIntervalSeconds is set but RetentionSeconds is not, so the background trimmer has no time window and does nothing for this topic. Set RetentionSeconds, or remove the interval and let the inline MAXLEN {} window do the trimming.",
                    topic.max_len()
                );
            } else {
                warn!(
                    topic = %name,
                    "Streams:Topics:{name}:BackgroundTrimIntervalSeconds is set but Trim is None, so nothing is ever deleted and the stream grows without bound. Trim=None is a Development-only setting."
                );
            }
        }
    }
}

/// The running handle C4 holds: `stop` cancels and joins the loops. Dropping it without `stop`
/// cancels the loops but does not wait.
pub(crate) struct StartedTrimmer {
    trimmer: std::sync::Arc<BackgroundTrimmer>,
    running: Mutex<Option<Running>>,
}

impl StartedTrimmer {
    pub(crate) fn trimmer(&self) -> &std::sync::Arc<BackgroundTrimmer> {
        &self.trimmer
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.lock().expect("running").is_some()
    }

    /// Cancels the loops and waits for them. Idempotent.
    pub(crate) async fn stop(&self) {
        let running = self.running.lock().expect("running").take();
        if let Some(running) = running {
            running.cancel.cancel();
            for handle in running.loops {
                let _ = handle.await;
            }
        }
    }
}

impl Drop for StartedTrimmer {
    fn drop(&mut self) {
        if let Some(running) = self.running.lock().expect("running").take() {
            running.cancel.cancel();
        }
    }
}

struct ConsumerPositions {
    consumer: String,
    positions: BTreeMap<u32, PositionRecord>,
}

/// The slowest position for `partition` and who holds it. A consumer with no field for the
/// partition clamps to [`StreamId::MIN`]. No consumers: `(MIN, None)`.
fn clamp_for(positions: &[ConsumerPositions], partition: u32) -> (StreamId, Option<String>) {
    let mut clamp = StreamId::MAX;
    let mut holder: Option<&str> = None;
    for consumer in positions {
        let recorded = consumer
            .positions
            .get(&partition)
            .map_or(StreamId::MIN, |r| r.id);
        if recorded < clamp {
            clamp = recorded;
            holder = Some(&consumer.consumer);
        }
    }
    match holder {
        Some(h) => (clamp, Some(h.to_owned())),
        None => (StreamId::MIN, None),
    }
}

/// Entry count at which the clamp yields: `MaxLen * min(threshold, 1)`, at least 1; `0` when the
/// threshold is not positive (the clamp then always yields).
fn release_at(options: &TopicOptions) -> i64 {
    let threshold = options.clamp_release_threshold;
    if threshold.is_nan() || threshold <= 0.0 {
        return 0;
    }
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        reason = "C# (long)(MaxLen * threshold) truncation"
    )]
    let at = (options.max_len() as f64 * threshold.min(1.0)) as i64;
    at.max(1)
}

fn release_target(max_len: u64, release_at: i64, length: i64) -> i64 {
    let max_len = i64::try_from(max_len).unwrap_or(i64::MAX);
    let slice = (max_len - release_at).max(1);
    (length - slice).min(release_at).max(1)
}

fn cutoff_for(options: &TopicOptions, now: OffsetDateTime) -> OffsetDateTime {
    let retention = i64::from(options.retention_seconds.unwrap_or(0));
    let cutoff = now - time::Duration::seconds(retention);
    if cutoff.unix_timestamp_nanos() < 0 {
        OffsetDateTime::UNIX_EPOCH
    } else {
        cutoff
    }
}

#[cfg(test)]
mod tests;
