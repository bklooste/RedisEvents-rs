//! [`StreamAdmin`]: operator operations on a topic. **Owner: D1** (port of `StreamAdmin.Reset.cs`,
//! `.Ownership.cs`, `.Tail.cs`).
//!
//! - **Reset** a consumer's positions to a time, an id, the start or the end, for one partition or
//!   all ([`StreamAdmin::reset_position`]), and **preview** what that would do first
//!   ([`StreamAdmin::preview_reset`]). A reset writes the position under
//!   `ADMIN_INSTANCE_ID` together with a
//!   `__reset:<p>` = `<targetId>|<issuedUtc>` marker (`WIRE.md` §7), the same bytes as C#, so a running
//!   consumer of either language rewinds on its next flush and a stopped one applies the marker at
//!   startup. Handlers must be idempotent: entries after the target are delivered again.
//! - **Ownership**: [`StreamAdmin::get_ownership`] reads `o:{topic}:<consumer>`.
//! - **Tail**: [`StreamAdmin::read_topic_tail`], the last id ever added per partition.
//! - **Meta**: [`StreamAdmin::read_topic_meta`] and [`StreamAdmin::read_topic_layout`].
//!
//! A time target resolves to the id immediately *before* `<ms>-0` (`StartPosition::from_time`), so an
//! entry written exactly on the requested millisecond is delivered, not skipped.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use time::OffsetDateTime;

use crate::config::{StartFrom, TopicOptions};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::ownership::OwnershipMap;
use crate::ownership::registry::{hgetall, read_owners};
use crate::positions::redis_store::format_o;
use crate::positions::{ADMIN_INSTANCE_ID, RedisPositionStore, StartPosition};
use crate::wire::{Keys, StreamId};

/// The reset marker field prefix; the field for partition `p` is `__reset:<p>`.
pub const RESET_MARKER_PREFIX: &str = "__reset";

/// The default cap on entries a preview counts per partition before it reports a floor.
pub const DEFAULT_MAX_COUNT_SCAN: u64 = 250_000;

/// Entries fetched per `XRANGE` page while counting.
const COUNT_PAGE_SIZE: usize = 1000;

/// Which way a reset moves a partition's position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ResetDirection {
    /// The target equals the current position: nothing is reprocessed and nothing skipped.
    None,
    /// The target is behind the current position: entries are reprocessed.
    Rewind,
    /// The target is ahead of the current position: entries are skipped, never processed.
    Skip,
}

impl ResetDirection {
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Rewind => "rewind",
            Self::Skip => "skip",
        }
    }
}

/// Where a reset moves positions to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ResetTarget {
    /// Replay from this instant: the entry written exactly on its millisecond is delivered.
    Time(OffsetDateTime),
    /// Resume strictly after this id.
    Id(StreamId),
    /// The beginning of each stream (`0-0`): replay everything that survives.
    Start,
    /// The tail of each stream: skip everything currently in it. Refused when a partition stream
    /// does not exist (it would write `0-0`, the opposite of the request).
    End,
}

/// Scope and tuning for [`StreamAdmin::preview_reset`] and [`StreamAdmin::reset_position`].
#[derive(Debug, Clone)]
pub struct ResetOptions {
    /// One partition, or `None` for every partition of the topic.
    pub partition: Option<u32>,
    /// The topic's configured partition count and key layout. `None` reads the count from
    /// `m:{topic}` and detects co-location from the keys that exist.
    pub topic: Option<TopicOptions>,
    /// Preview only: stop counting a partition at this many entries and report a floor. Zero means
    /// [`DEFAULT_MAX_COUNT_SCAN`].
    pub max_count_scan: u64,
    /// Preview only: the start a consumer with no stored position would use, so the distance is
    /// measured from where it would really begin.
    pub start_from_when_missing: StartFrom,
}

impl Default for ResetOptions {
    fn default() -> Self {
        Self {
            partition: None,
            topic: None,
            max_count_scan: DEFAULT_MAX_COUNT_SCAN,
            start_from_when_missing: StartFrom::Beginning,
        }
    }
}

/// What a reset would do to one partition, computed without changing anything.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PartitionResetPreview {
    /// The partition.
    pub partition: u32,
    /// The consumer's recorded position, or `None` when it has none and would resolve from
    /// `StartFromWhenMissing` instead.
    pub current: Option<StreamId>,
    /// The position that would be written; the next read starts strictly after it.
    pub target: StreamId,
    /// Entries between the two positions: reprocessed on a rewind, skipped on a skip.
    pub entries: u64,
    /// `false` when counting hit the scan cap and [`entries`](Self::entries) is a floor.
    pub entries_exact: bool,
    /// The oldest id still in the stream, `None` when empty.
    pub oldest: Option<StreamId>,
    /// The newest id in the stream, `None` when empty.
    pub newest: Option<StreamId>,
    /// The stream's `XLEN`.
    pub length: u64,
    /// Whether the reset rewinds, skips, or does nothing.
    pub direction: ResetDirection,
    /// `true` when the first entry the consumer would see predates the oldest surviving entry: the
    /// requested data is gone.
    pub trimmed_away: bool,
    /// With no stored position, the id the distance was measured from.
    pub assumed_start: Option<StreamId>,
}

/// What a reset would do to a consumer, per partition.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResetPreview {
    /// The topic.
    pub topic: String,
    /// The consumer.
    pub consumer: String,
    /// The instant asked for, when the target was a time.
    pub requested_from: Option<OffsetDateTime>,
    /// One entry per partition in scope.
    pub partitions: Vec<PartitionResetPreview>,
}

impl ResetPreview {
    /// Entries that would be reprocessed, summed over rewinding partitions.
    #[must_use]
    pub fn total_to_reprocess(&self) -> u64 {
        self.total(ResetDirection::Rewind)
    }

    /// Entries that would be skipped, summed over skipping partitions.
    #[must_use]
    pub fn total_to_skip(&self) -> u64 {
        self.total(ResetDirection::Skip)
    }

    /// `true` when any partition's requested data has been trimmed away.
    #[must_use]
    pub fn any_trimmed_away(&self) -> bool {
        self.partitions.iter().any(|p| p.trimmed_away)
    }

    fn total(&self, direction: ResetDirection) -> u64 {
        self.partitions
            .iter()
            .filter(|p| p.direction == direction)
            .map(|p| p.entries)
            .sum()
    }

    /// A multi-line operator summary, one line per partition.
    #[must_use]
    pub fn describe(&self) -> String {
        let mut text = format!("reset {} / {}", self.topic, self.consumer);
        if let Some(from) = self.requested_from {
            let _ = write!(text, "  from {}", format_o(from));
        }
        text.push('\n');
        for p in &self.partitions {
            let current = match (p.current, p.assumed_start) {
                (Some(id), _) => id.to_string(),
                (None, Some(assumed)) => format!("(none, assuming {assumed})"),
                (None, None) => "(none)".to_owned(),
            };
            let _ = write!(
                text,
                "  p{}  current {current} -> {}  {} {}{} of {}  oldest {}",
                p.partition,
                p.target,
                p.direction.label(),
                if p.entries_exact { "" } else { "\u{2265}" },
                p.entries,
                p.length,
                p.oldest
                    .map_or_else(|| "(empty)".to_owned(), |id| id.to_string()),
            );
            if p.trimmed_away {
                text.push_str(
                    "  ** requested start is older than the oldest surviving entry: that data is gone **",
                );
            }
            text.push('\n');
        }
        text
    }
}

/// Operator helpers over a topic's Redis keys. Cheap to clone; holds a [`Connection`] and the
/// key namespace.
#[derive(Debug, Clone)]
pub struct StreamAdmin {
    conn: Connection,
    keys: Keys,
}

impl StreamAdmin {
    /// An admin over `conn`, using `keys` (the key prefix) to address streams, positions and meta.
    #[must_use]
    pub fn new(conn: Connection, keys: Keys) -> Self {
        Self { conn, keys }
    }

    /// The marker field for one partition: `__reset:<partition>`.
    #[must_use]
    pub fn reset_marker_field(partition: u32) -> String {
        crate::positions::reset_marker::field(partition)
    }

    /// Reads the live ownership map for a topic and consumer from `o:{topic}:<consumer>`. Fields
    /// that are not a partition number are skipped.
    ///
    /// `partitions` is the total count when the caller knows it; `0` reads it from `m:{topic}`,
    /// and failing that infers it from the highest claim (which can only under-report a gap).
    /// [`OwnershipMap::unowned`] is populated; `contested` is always empty here, since an overlap
    /// is only detectable by an instance comparing a claim to its own id.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic or consumer; [`Error::Redis`] when Redis fails.
    pub async fn get_ownership(
        &self,
        topic: &str,
        consumer: &str,
        partitions: u32,
    ) -> Result<OwnershipMap> {
        check_name("topic", topic)?;
        check_name("consumer", consumer)?;
        let mut shared = self.conn.shared();
        let entries = hgetall(&mut shared, &self.keys.ownership(topic, consumer)).await?;
        let owners = read_owners(&entries);

        let mut total = partitions;
        if total < 1 {
            total = self.read_partition_count(topic).await?;
        }
        if total < 1 {
            total = owners.keys().map(|p| p + 1).max().unwrap_or(0);
        }
        let unowned = (0..total).filter(|p| !owners.contains_key(p)).collect();
        Ok(OwnershipMap {
            owners,
            unowned,
            contested: Vec::new(),
        })
    }

    /// Each partition's tail: the id of the last entry ever added (`XINFO STREAM`
    /// `last-generated-id`), index `p` for partition `p`. Exact for a trimmed stream; a stream that
    /// does not exist or was never written reports `0-0`. Keys honour the key prefix and
    /// [`TopicOptions::co_locate_partitions`]; the partition count is the caller's configured one,
    /// not looked up.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic or fewer than one partition; [`Error::Redis`].
    pub async fn read_topic_tail(
        &self,
        topic: &str,
        options: &TopicOptions,
    ) -> Result<Vec<StreamId>> {
        check_name("topic", topic)?;
        if options.partitions < 1 {
            return Err(Error::invalid_argument(
                "TopicOptions.partitions must be at least 1",
            ));
        }
        let mut shared = self.conn.shared();
        let mut tails = Vec::with_capacity(options.partitions as usize);
        for partition in 0..options.partitions {
            let key = self
                .keys
                .stream(topic, partition, options.co_locate_partitions);
            let info: redis::RedisResult<redis::streams::StreamInfoStreamReply> =
                redis::cmd("XINFO")
                    .arg("STREAM")
                    .arg(&key)
                    .query_async(&mut shared)
                    .await;
            tails.push(match info {
                Ok(info) => StreamId::parse(&info.last_generated_id).unwrap_or(StreamId::MIN),
                // Never written and never ensured: nothing published, so 0-0 is exact.
                Err(e) if e.to_string().to_ascii_lowercase().contains("no such key") => {
                    StreamId::MIN
                }
                Err(e) => return Err(e.into()),
            });
        }
        Ok(tails)
    }

    /// The `m:{topic}` hash as recorded (`v`, `partitions`, `createdUtc`, `maxLen`); empty when the
    /// topic has no meta.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic; [`Error::Redis`].
    pub async fn read_topic_meta(&self, topic: &str) -> Result<BTreeMap<String, String>> {
        check_name("topic", topic)?;
        let mut shared = self.conn.shared();
        Ok(hgetall(&mut shared, &self.keys.topic_meta(topic))
            .await?
            .into_iter()
            .collect())
    }

    /// The topic's partition count (from `m:{topic}`, `0` when unrecorded) and its detected key
    /// layout, as [`TopicOptions`].
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic; [`Error::Redis`].
    pub async fn read_topic_layout(&self, topic: &str) -> Result<TopicOptions> {
        check_name("topic", topic)?;
        Ok(TopicOptions {
            partitions: self.read_partition_count(topic).await?,
            co_locate_partitions: self.detect_co_location(topic).await?,
            ..TopicOptions::default()
        })
    }

    /// What resetting `consumer` to `target` would do, per partition, without changing anything.
    /// Counting is paged (`XRANGE`, 1000 per page) and stops at
    /// [`ResetOptions::max_count_scan`], reporting a floor.
    ///
    /// # Errors
    /// [`Error::Config`] for a partition that does not exist or an unknowable partition count;
    /// [`Error::InvalidArgument`] for blank names; [`Error::Redis`].
    pub async fn preview_reset(
        &self,
        topic: &str,
        consumer: &str,
        target: ResetTarget,
        options: &ResetOptions,
    ) -> Result<ResetPreview> {
        let partitions = self
            .resolve_partitions(topic, consumer, options.partition, options.topic.as_ref())
            .await?;
        let co_locate = self
            .resolve_co_locate(topic, options.topic.as_ref())
            .await?;
        let store = self.admin_store();
        let stored = store.load_records(topic, consumer).await?;
        let fixed = match target {
            ResetTarget::Time(when) => Some(time_target(when)),
            ResetTarget::Id(id) => Some(id),
            ResetTarget::Start => Some(StreamId::MIN),
            ResetTarget::End => None,
        };
        let requested_from = match target {
            ResetTarget::Time(when) => Some(when),
            _ => None,
        };
        let cap = if options.max_count_scan > 0 {
            options.max_count_scan
        } else {
            DEFAULT_MAX_COUNT_SCAN
        };

        let mut shared = self.conn.shared();
        let mut previews = Vec::with_capacity(partitions.len());
        for p in partitions {
            let key = self.keys.stream(topic, p, co_locate);
            let current = stored.get(&p).map(|record| record.id);
            let length: u64 = redis::cmd("XLEN")
                .arg(&key)
                .query_async(&mut shared)
                .await?;
            let (oldest, newest) = self.read_bounds(&key).await?;

            // `End` has no single id: each partition's own tail.
            let partition_target = fixed.unwrap_or_else(|| newest.unwrap_or(StreamId::MIN));

            // With no stored position the fallback the consumer would use stands in for it;
            // assuming 0-0 under `Now` would report a skip of the whole stream for a no-op move.
            let assumed = if current.is_none() {
                Some(if options.start_from_when_missing == StartFrom::Now {
                    newest.unwrap_or(StreamId::MIN)
                } else {
                    StreamId::MIN
                })
            } else {
                None
            };
            let baseline = current.or(assumed).unwrap_or(StreamId::MIN);

            let direction = match partition_target.cmp(&baseline) {
                std::cmp::Ordering::Equal => ResetDirection::None,
                std::cmp::Ordering::Less => ResetDirection::Rewind,
                std::cmp::Ordering::Greater => ResetDirection::Skip,
            };
            let (from, to) = if direction == ResetDirection::Rewind {
                (partition_target, baseline)
            } else {
                (baseline, partition_target)
            };
            let (entries, entries_exact) = if direction == ResetDirection::None {
                (0, true)
            } else {
                self.count_between(&key, from, to, cap).await?
            };

            // The first entry the consumer would see; if that predates what survives, it is gone.
            let wanted = next_id(partition_target);
            let trimmed_away = oldest.is_some_and(|o| o > wanted);

            previews.push(PartitionResetPreview {
                partition: p,
                current,
                target: partition_target,
                entries,
                entries_exact,
                oldest,
                newest,
                length,
                direction,
                trimmed_away,
                assumed_start: assumed,
            });
        }
        Ok(ResetPreview {
            topic: topic.to_owned(),
            consumer: consumer.to_owned(),
            requested_from,
            partitions: previews,
        })
    }

    /// Moves `consumer`'s position to `target` for one partition or all, writing each position
    /// under the admin instance id with a `__reset:<p>` marker in one `HSET`. Returns the
    /// `(partition, target)` pairs written. Any running consumer rewinds on its next flush tick; a
    /// stopped one applies the marker at startup. Entries after the target are delivered again.
    ///
    /// # Errors
    /// [`Error::Config`] for a partition that does not exist, an unknowable partition count, or
    /// [`ResetTarget::End`] over a stream that does not exist; [`Error::InvalidArgument`] for blank
    /// names; [`Error::Redis`].
    pub async fn reset_position(
        &self,
        topic: &str,
        consumer: &str,
        target: ResetTarget,
        options: &ResetOptions,
    ) -> Result<Vec<(u32, StreamId)>> {
        let partitions = self
            .resolve_partitions(topic, consumer, options.partition, options.topic.as_ref())
            .await?;

        let targets: Vec<(u32, StreamId)> = match target {
            ResetTarget::Time(when) => {
                let id = time_target(when);
                partitions.into_iter().map(|p| (p, id)).collect()
            }
            ResetTarget::Id(id) => partitions.into_iter().map(|p| (p, id)).collect(),
            ResetTarget::Start => partitions.into_iter().map(|p| (p, StreamId::MIN)).collect(),
            ResetTarget::End => {
                let co_locate = self
                    .resolve_co_locate(topic, options.topic.as_ref())
                    .await?;
                let mut shared = self.conn.shared();
                let mut targets = Vec::with_capacity(partitions.len());
                for p in partitions {
                    let key = self.keys.stream(topic, p, co_locate);
                    let (_, newest) = self.read_bounds(&key).await?;
                    if newest.is_none() {
                        let exists: bool = redis::cmd("EXISTS")
                            .arg(&key)
                            .query_async(&mut shared)
                            .await?;
                        if !exists {
                            // Not "the stream is empty" but "this is the wrong key": wrong topic
                            // name or wrong co-location layout. 0-0 would be a rewind.
                            return Err(Error::config(format!(
                                "Partition stream '{key}' does not exist, so a reset to the end has no tail to move to and would write 0-0 \u{2014} a rewind to the beginning, the opposite of what was asked. Check the topic name and whether the topic is co-located (TopicOptions.co_locate_partitions)."
                            )));
                        }
                    }
                    // An empty partition has no tail to skip to; 0-0 is already "everything there is".
                    targets.push((p, newest.unwrap_or(StreamId::MIN)));
                }
                targets
            }
        };

        if targets.is_empty() {
            return Ok(targets);
        }
        self.admin_store()
            .reset_targets(topic, consumer, &targets)
            .await?;
        tracing::warn!(
            topic,
            consumer,
            partitions = targets.len(),
            first_target = %targets[0].1,
            "stream positions reset: any running worker rewinds on its next flush tick; entries after the target are delivered again, so handlers must be idempotent"
        );
        Ok(targets)
    }

    fn admin_store(&self) -> RedisPositionStore {
        RedisPositionStore::new(self.conn.clone(), self.keys.clone(), ADMIN_INSTANCE_ID)
    }

    /// The recorded partition count, `0` when absent or not a positive integer.
    async fn read_partition_count(&self, topic: &str) -> Result<u32> {
        let mut shared = self.conn.shared();
        let recorded: Option<String> = redis::cmd("HGET")
            .arg(self.keys.topic_meta(topic))
            .arg("partitions")
            .query_async(&mut shared)
            .await?;
        Ok(recorded.as_deref().map_or(0, parse_count))
    }

    async fn resolve_co_locate(&self, topic: &str, options: Option<&TopicOptions>) -> Result<bool> {
        match options {
            Some(o) => Ok(o.co_locate_partitions),
            None => self.detect_co_location(topic).await,
        }
    }

    async fn detect_co_location(&self, topic: &str) -> Result<bool> {
        let mut shared = self.conn.shared();
        let tagged: bool = redis::cmd("EXISTS")
            .arg(self.keys.stream(topic, 0, true))
            .query_async(&mut shared)
            .await?;
        let spread: bool = redis::cmd("EXISTS")
            .arg(self.keys.stream(topic, 0, false))
            .query_async(&mut shared)
            .await?;
        Ok(tagged || !spread)
    }

    async fn resolve_partitions(
        &self,
        topic: &str,
        consumer: &str,
        partition: Option<u32>,
        options: Option<&TopicOptions>,
    ) -> Result<Vec<u32>> {
        check_name("topic", topic)?;
        check_name("consumer", consumer)?;
        let configured = options.map_or(0, |o| o.partitions);

        if let Some(one) = partition {
            // An index past the end would write a position and marker no worker ever reads and
            // report success; refuse it whenever the count is knowable.
            let known = if configured < 1 {
                self.read_partition_count(topic).await?
            } else {
                configured
            };
            if known >= 1 && one >= known {
                return Err(Error::config(format!(
                    "Topic '{topic}' has {known} partition(s), numbered 0..{}, so partition {one} does not exist. Resetting it would leave an orphan position and reset marker in p:{{{topic}}}:{consumer} that no worker reads.",
                    known - 1
                )));
            }
            return Ok(vec![one]);
        }

        let total = if configured < 1 {
            self.read_partition_count(topic).await?
        } else {
            configured
        };
        if total < 1 {
            return Err(Error::config(format!(
                "Topic '{topic}' has no recorded partition count in m:{{{topic}}}, so a whole-topic reset cannot know how many partitions to move. Pass TopicOptions with the configured partition count, or reset one partition at a time."
            )));
        }
        Ok((0..total).collect())
    }

    /// `(oldest, newest)` ids in a stream, `None` when empty or missing.
    async fn read_bounds(&self, key: &str) -> Result<(Option<StreamId>, Option<StreamId>)> {
        let mut shared = self.conn.shared();
        let first: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(key)
            .arg("-")
            .arg("+")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut shared)
            .await?;
        let last: redis::streams::StreamRangeReply = redis::cmd("XREVRANGE")
            .arg(key)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut shared)
            .await?;
        Ok((bound(&first), bound(&last)))
    }

    /// Entries with ids in `(from, to]`, paged, stopping at `cap` with `exact = false`.
    async fn count_between(
        &self,
        key: &str,
        from: StreamId,
        to: StreamId,
        cap: u64,
    ) -> Result<(u64, bool)> {
        if to <= from {
            return Ok((0, true));
        }
        let mut shared = self.conn.shared();
        let upper = to.to_string();
        let mut cursor = next_id(from);
        let mut count: u64 = 0;
        loop {
            let page: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
                .arg(key)
                .arg(cursor.to_string())
                .arg(&upper)
                .arg("COUNT")
                .arg(COUNT_PAGE_SIZE)
                .query_async(&mut shared)
                .await?;
            let len = page.ids.len();
            if len == 0 {
                return Ok((count, true));
            }
            count += len as u64;
            if count >= cap {
                return Ok((count, false));
            }
            if len < COUNT_PAGE_SIZE {
                return Ok((count, true));
            }
            let Some(last) = bound_last(&page) else {
                return Ok((count, true));
            };
            if last >= to {
                return Ok((count, true));
            }
            cursor = next_id(last);
        }
    }
}

/// The position a time target writes: just before the first id at that millisecond.
fn time_target(when: OffsetDateTime) -> StreamId {
    match StartPosition::from_time(when) {
        StartPosition::After(id) => id,
        StartPosition::Now => unreachable!("from_time always yields After"),
    }
}

fn bound(reply: &redis::streams::StreamRangeReply) -> Option<StreamId> {
    reply
        .ids
        .first()
        .and_then(|entry| StreamId::parse(&entry.id).ok())
}

fn bound_last(reply: &redis::streams::StreamRangeReply) -> Option<StreamId> {
    reply
        .ids
        .last()
        .and_then(|entry| StreamId::parse(&entry.id).ok())
}

fn check_name(what: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::invalid_argument(format!("{what} must not be empty")));
    }
    Ok(())
}

/// The id right after `id` (C# `Next`).
fn next_id(id: StreamId) -> StreamId {
    if id.seq == StreamId::MAX_PART {
        StreamId::new(id.ms + 1, 0)
    } else {
        StreamId::new(id.ms, id.seq + 1)
    }
}

/// A positive decimal count (digits only, `int` range), else `0`.
fn parse_count(text: &str) -> u32 {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return 0;
    }
    text.parse::<i32>()
        .ok()
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(p: u32, direction: ResetDirection, entries: u64) -> PartitionResetPreview {
        PartitionResetPreview {
            partition: p,
            current: Some(StreamId::new(9, 0)),
            target: StreamId::new(3, 0),
            entries,
            entries_exact: true,
            oldest: Some(StreamId::new(1, 0)),
            newest: Some(StreamId::new(9, 0)),
            length: 9,
            direction,
            trimmed_away: false,
            assumed_start: None,
        }
    }

    #[test]
    fn totals_split_by_direction() {
        let preview = ResetPreview {
            topic: "t".into(),
            consumer: "c".into(),
            requested_from: None,
            partitions: vec![
                part(0, ResetDirection::Rewind, 5),
                part(1, ResetDirection::Skip, 7),
                part(2, ResetDirection::Rewind, 2),
                part(3, ResetDirection::None, 0),
            ],
        };
        assert_eq!(preview.total_to_reprocess(), 7);
        assert_eq!(preview.total_to_skip(), 7);
        assert!(!preview.any_trimmed_away());
    }

    #[test]
    fn describe_marks_floors_and_trimmed_data() {
        let mut p = part(0, ResetDirection::Rewind, 250_000);
        p.entries_exact = false;
        p.trimmed_away = true;
        let text = ResetPreview {
            topic: "t".into(),
            consumer: "c".into(),
            requested_from: None,
            partitions: vec![p],
        }
        .describe();
        assert!(text.contains("rewind \u{2265}250000 of 9"), "{text}");
        assert!(text.contains("that data is gone"), "{text}");
    }

    #[test]
    fn time_target_sits_immediately_before_the_millisecond() {
        let when = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        assert_eq!(
            time_target(when),
            StreamId::new(1_699_999_999_999, StreamId::MAX_PART)
        );
    }
}
