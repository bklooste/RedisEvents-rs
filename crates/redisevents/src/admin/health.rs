//! Health, lag and status. **Owner: D2** (port of `StreamHealth.cs`, `StreamLag.cs`,
//! `StreamStatus.cs`, `StreamPartitionStatus.cs`; the host-neutral part of `StreamsHealthCheck.cs`).
//!
//! Read-only: every number here comes from in-process monitors, the ownership registry, and a
//! `PING` (plus `XINFO STREAM` per partition, in the sampler). [`check`] is the entry
//! point for a health endpoint; `redisevents-axum` (D3) maps its [`StreamHealthStatus`] to HTTP.
//!
//! Monitors are created by the consumer host (C2/C4) through the crate-private registry in
//! `monitor.rs`; this module only grades their snapshots.

#[allow(
    dead_code,
    reason = "consumed by the consumer host (C2/C4) when it lands"
)]
pub(crate) mod monitor;
#[cfg(test)]
mod stall_tests;
#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::time::Duration;

use crate::connection::Connection;
use crate::ownership::registry::OwnershipRegistry;
use crate::wire::StreamId;

/// The verdict on stream consumption in this process.
///
/// Map it with a `match`, never a cast: the numeric order differs from other health vocabularies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StreamHealthStatus {
    /// Every partition is being read and nothing is past a threshold.
    Healthy,
    /// Something wants attention but a restart would not help, or would make it worse.
    Degraded,
    /// A partition is not being read and only a restart brings it back.
    Unhealthy,
}

/// Which rule produced a [`StreamHealthReport`] or a [`StreamPartitionHealth`].
///
/// New members may be added; treat an unknown value as its [`StreamHealthStatus`] alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StreamHealthRule {
    /// No rule fired: Healthy.
    #[default]
    None,
    /// Unhealthy: the shared Redis connection is down.
    ConnectionDown,
    /// Unhealthy: a partition has been blocked retrying past `unhealthy_block_seconds`.
    BlockedTooLong,
    /// Unhealthy: a partition whose stop escalates has been stopped past `unhealthy_stopped_seconds`
    /// with entries waiting.
    StoppedTooLong,
    /// Unhealthy: a partition has sat behind the tail with a position that does not change for
    /// `unhealthy_behind_seconds`.
    BehindTail,
    /// Unhealthy: consumers are registered but none has started.
    NoConsumerStarted,
    /// Unhealthy: every partition worker has stopped.
    AllPartitionsStopped,
    /// Degraded: no partition has started reading yet.
    NotReadingYet,
    /// Degraded: a partition is blocked retrying a `Block` error.
    Blocked,
    /// Degraded: a partition has stood down under an error policy.
    Stopped,
    /// Degraded: a partition's lag is past `unhealthy_lag_ms`.
    Lagging,
    /// Degraded: the ownership registry shows a gap or an overlap.
    OwnershipDegraded,
    /// Degraded: a caught-up consumer has received nothing past `degraded_silence_seconds`.
    Silent,
    /// Unhealthy: a partition this instance owns has made no progress (no read returned, no batch
    /// completed, or its position writes kept failing) past `unhealthy_stalled_seconds`, or the
    /// host already stood it down for that reason (and in Lease mode gave its lease up).
    Stalled,
}

/// The run state of a partition worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum StreamPartitionRunState {
    /// Registered, has not yet fetched.
    #[default]
    Starting,
    /// Reading.
    Running,
    /// Retrying a blocking failure.
    Blocked,
    /// Stood down.
    Stopped,
}

/// The grade of one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct StreamPartitionHealth {
    /// The status.
    pub status: StreamHealthStatus,
    /// The rule that fired.
    pub rule: StreamHealthRule,
}

/// A point-in-time snapshot of one partition.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
#[allow(
    clippy::struct_excessive_bools,
    reason = "a flat snapshot: each flag is an independent fact the grading reads"
)]
pub struct StreamPartitionStatus {
    /// Topic.
    pub topic: String,
    /// Consumer name.
    pub consumer: String,
    /// Partition index.
    pub partition: u32,
    /// Run state.
    pub state: StreamPartitionRunState,
    /// Why it stopped, when stopped.
    pub stop_reason: Option<String>,
    /// The last entry processed by this instance (`0-0` before any).
    pub last_processed: StreamId,
    /// Whether the last sample found nothing behind.
    pub is_caught_up: bool,
    /// Age of the last processed entry, in milliseconds.
    pub lag_ms: f64,
    /// Entries between the position and the tail; `-1` when never sampled.
    pub lag_entries: i64,
    /// Time blocked, in milliseconds (`0` when not blocked).
    pub blocked_ms: f64,
    /// Time stopped, in milliseconds (`0` when not stopped).
    pub stopped_ms: f64,
    /// Threshold: lag above this (ms) is Degraded.
    pub unhealthy_lag_ms: u32,
    /// Threshold: blocked longer than this (s) is Unhealthy.
    pub unhealthy_block_seconds: u32,
    /// Threshold: stopped (escalating) longer than this (s) is Unhealthy.
    pub unhealthy_stopped_seconds: u32,
    /// Threshold: behind the tail, frozen this long (s), is Unhealthy; `0` = off.
    pub unhealthy_behind_seconds: u32,
    /// Threshold: caught up and silent this long (s) is Degraded; `0` = off.
    pub degraded_silence_seconds: u32,
    /// Whether a stop escalates to Unhealthy.
    pub escalates: bool,
    /// The position: the last observed id, or the start position before any.
    pub position: StreamId,
    /// Milliseconds since the position last changed.
    pub position_unchanged_ms: f64,
    /// The last sampled tail (`0-0` for an empty stream), `None` when never sampled.
    pub tail_id: Option<StreamId>,
    /// Milliseconds since the tail was last sampled; `-1` when never.
    pub tail_sample_age_ms: f64,
    /// Milliseconds since the tail last gained an entry; `-1` when never sampled.
    pub tail_unchanged_ms: f64,
    /// Whether the position is behind a fresh tail sample.
    pub is_behind_tail: bool,
    /// Milliseconds the partition has been behind the tail (`0` when not behind).
    pub behind_ms: f64,
    /// Milliseconds without progress (`0` when the stall rule does not apply to the state).
    pub stalled_ms: f64,
    /// Threshold: no progress this long (s) is Unhealthy; `0` = off.
    pub unhealthy_stalled_seconds: u32,
    /// Whether the host stood this partition down because it stalled or its read loop faulted.
    pub stood_down_stalled: bool,
}

/// Process-level facts a grade depends on, beyond the partitions.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamHealthContext {
    /// Set when the Redis connection is down: the reason.
    pub connection_fault: Option<String>,
    /// Consumers registered in this process.
    pub consumers: u32,
    /// Consumers that have started.
    pub consumers_started: u32,
    /// Whether the ownership registry shows a gap or an overlap.
    pub ownership_degraded: bool,
}

/// The structured facts behind a [`StreamHealthReport`]. Fields that do not apply to the rule that
/// fired are `None`; absent fields are omitted from serialized output.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
#[non_exhaustive]
pub struct StreamHealthDetails {
    /// Consumers registered in this process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumers: Option<u32>,
    /// Consumers that have started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumers_started: Option<u32>,
    /// Partitions monitored in this process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<u32>,
    /// Partitions blocked retrying.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_blocked: Option<u32>,
    /// Partitions stopped.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_stopped: Option<u32>,
    /// Partitions still starting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_starting: Option<u32>,
    /// Partitions behind the tail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_behind: Option<u32>,
    /// The worst lag, in whole milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worst_lag_ms: Option<i64>,
    /// The partition with the worst lag, as `topic[partition]/consumer`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worst_lag_partition: Option<String>,
    /// Entries behind on that partition.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worst_lag_entries: Option<i64>,
    /// The Redis connection fault, when it is down.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    /// The partition that sat behind the tail, as `topic[partition]/consumer`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behind_partition: Option<String>,
    /// How long it has been behind, in whole milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behind_ms: Option<i64>,
    /// Whether the ownership registry shows a gap or an overlap.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ownership_degraded: Option<bool>,
    /// The silent consumer, as `topic/consumer`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silent_consumer: Option<String>,
    /// How long it has been silent, in whole milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silent_ms: Option<i64>,
    /// The stalled partition, as `topic[partition]/consumer`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stalled_partition: Option<String>,
    /// How long it has made no progress, in whole milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stalled_ms: Option<i64>,
}

/// A consumer that is caught up and receiving nothing.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct StreamSilentConsumer {
    /// Topic.
    pub topic: String,
    /// Consumer name.
    pub consumer: String,
    /// Partitions this process reads for it.
    pub partitions: u32,
    /// How long the shortest of them has not moved, in milliseconds.
    pub silent_ms: f64,
    /// The threshold it is graded against.
    pub degraded_silence_seconds: u32,
}

/// The verdict on stream consumption in this process.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct StreamHealthReport {
    /// The status.
    pub status: StreamHealthStatus,
    /// The rule that fired.
    pub rule: StreamHealthRule,
    /// A human-readable explanation, naming the partition where there is one.
    pub description: String,
    /// Structured facts behind the verdict; the fields that do not apply to the rule are `None`.
    pub details: StreamHealthDetails,
    /// The partition the rule is about, when one is.
    pub partition: Option<StreamPartitionStatus>,
}

impl StreamHealthReport {
    /// A report with no details and no partition; add them with [`with_details`](Self::with_details) and
    /// [`with_partition`](Self::with_partition).
    #[must_use]
    pub fn new(
        status: StreamHealthStatus,
        rule: StreamHealthRule,
        description: impl Into<String>,
    ) -> Self {
        Self {
            status,
            rule,
            description: description.into(),
            details: StreamHealthDetails::default(),
            partition: None,
        }
    }

    /// Sets the structured facts.
    #[must_use]
    pub fn with_details(mut self, details: StreamHealthDetails) -> Self {
        self.details = details;
        self
    }

    /// Sets the partition the rule is about.
    #[must_use]
    pub fn with_partition(mut self, partition: StreamPartitionStatus) -> Self {
        self.partition = Some(partition);
        self
    }
}

/// How often the sampler reads each stream.
pub const DEFAULT_SAMPLE_INTERVAL: Duration = Duration::from_secs(15);
/// How often lag is republished.
pub const DEFAULT_PUSH_INTERVAL: Duration = Duration::from_secs(1);
/// A tail sample older than this no longer counts (four sample intervals, C# `TailStaleAfter`).
pub const TAIL_STALE_AFTER: Duration = Duration::from_secs(60);

/// Estimated entries between `processed` and the tail.
///
/// `length` is `XLEN`, `first` and `last` are the first and last entries present (`0-0` when
/// empty), `last` for the interpolation is the last generated id. Interpolates by id time, so
/// it is an estimate: exact only when entries are evenly spread.
#[must_use]
pub fn estimate_entries_behind(
    length: i64,
    first: StreamId,
    last: StreamId,
    processed: StreamId,
) -> i64 {
    if length <= 0 || processed >= last {
        return 0;
    }
    if processed < first {
        // Our position predates the oldest entry still present: everything present is unread.
        return length;
    }
    let span = i128::from(last.ms) - i128::from(first.ms);
    if span <= 0 {
        // Every entry landed in one millisecond: nothing to interpolate, and we know we are behind.
        return length;
    }
    let behind_ms = i128::from(last.ms) - i128::from(processed.ms);
    let behind = i64::try_from(i128::from(length) * behind_ms / span).unwrap_or(length);
    // processed < last, so at least one entry, however the arithmetic rounds.
    behind.clamp(1, length)
}

/// `true` when any partition is being monitored.
#[must_use]
pub fn any() -> bool {
    !monitor::all().is_empty()
}

/// One snapshot per monitored partition, in key order.
#[must_use]
pub fn partitions() -> Vec<StreamPartitionStatus> {
    monitor::all().iter().map(|m| m.status()).collect()
}

/// Grades this process: reads the partitions, a `PING` on `connection` (when given), and the
/// ownership registry, then applies [`evaluate`]. `consumers` and
/// `consumers_started` are the consumer host's counts; pass `0, 0` when there is none.
pub async fn check(
    connection: Option<&Connection>,
    consumers: u32,
    consumers_started: u32,
) -> StreamHealthReport {
    let connection_fault = match connection {
        Some(conn) => connection_fault(conn).await,
        None => None,
    };
    let context = StreamHealthContext {
        connection_fault,
        consumers,
        consumers_started,
        ownership_degraded: OwnershipRegistry::any_degraded(),
    };
    evaluate(&partitions(), &context)
}

async fn connection_fault(connection: &Connection) -> Option<String> {
    let mut conn = connection.shared();
    let ping = redis::cmd("PING");
    match tokio::time::timeout(
        Duration::from_secs(1),
        ping.query_async::<String>(&mut conn),
    )
    .await
    {
        Ok(Ok(_)) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(_) => Some("PING timed out after 1s".to_owned()),
    }
}

/// Grades a process from its partition snapshots and context.
#[must_use]
#[allow(
    clippy::field_reassign_with_default,
    clippy::too_many_lines,
    reason = "one arm per rule, in priority order; details are filled in as rules are checked"
)]
pub fn evaluate(
    partitions: &[StreamPartitionStatus],
    context: &StreamHealthContext,
) -> StreamHealthReport {
    if context.consumers == 0 && partitions.is_empty() {
        // A producer-only service, or one whose consumers are not registered here: nothing to
        // report is not the same as something being wrong.
        return StreamHealthReport {
            status: StreamHealthStatus::Healthy,
            rule: StreamHealthRule::None,
            description: "Streams: no stream consumers are registered in this process.".to_owned(),
            details: StreamHealthDetails::default(),
            partition: None,
        };
    }

    let mut data = StreamHealthDetails::default();
    data.consumers = Some(context.consumers);
    data.partitions = Some(u32::try_from(partitions.len()).unwrap_or(u32::MAX));
    data.consumers_started = Some(context.consumers_started);

    let tally = Tally::of(partitions, &mut data);

    if let Some(fault) = &context.connection_fault {
        data.connection = Some(fault.clone());
        return unhealthy(
            StreamHealthRule::ConnectionDown,
            format!("Streams: the Redis connection is down ({fault}); nothing is being consumed."),
            data,
            None,
        );
    }

    if let Some(stuck) = tally.stalled {
        data.stalled_partition = Some(describe(stuck));
        data.stalled_ms = Some(ms_int(stuck.stalled_ms));
        let description = if stuck.stood_down_stalled {
            format!(
                "Streams: partition {} was stood down by this instance ({}), and in Lease mode its lease handed to another instance. Whatever stopped it is still in this process; a restart rebuilds its Redis connections.",
                describe(stuck),
                stuck.stop_reason.as_deref().unwrap_or("stalled")
            )
        } else {
            format!(
                "Streams: partition {} has made no progress for {}s (no read returned and no batch completed, or its position writes kept failing), past its UnhealthyStalledSeconds of {}. It holds the partition and is not reading it; a restart hands the partition on.",
                describe(stuck),
                seconds(stuck.stalled_ms),
                stuck.unhealthy_stalled_seconds
            )
        };
        return unhealthy(StreamHealthRule::Stalled, description, data, Some(stuck));
    }

    if let Some(wedged) = tally.blocked_too_long {
        return unhealthy(
            StreamHealthRule::BlockedTooLong,
            format!(
                "Streams: partition {} has been blocked for {}s retrying a blocking error, past its UnhealthyBlockSeconds of {}. Its position has not advanced and its backlog is at risk of being trimmed.",
                describe(wedged),
                seconds(wedged.blocked_ms),
                wedged.unhealthy_block_seconds
            ),
            data,
            Some(wedged),
        );
    }

    if let Some(abandoned) = tally.stopped_too_long {
        return unhealthy(
            StreamHealthRule::StoppedTooLong,
            format!(
                "Streams: partition {} stood down {}s ago ({}) and is {} entries behind a stream that is still being written, past its UnhealthyStoppedSeconds of {}. Nothing reads it again without a restart.",
                describe(abandoned),
                seconds(abandoned.stopped_ms),
                abandoned.stop_reason.as_deref().unwrap_or(""),
                abandoned.lag_entries,
                abandoned.unhealthy_stopped_seconds
            ),
            data,
            Some(abandoned),
        );
    }

    if let Some(frozen) = tally.behind_too_long {
        data.behind_partition = Some(describe(frozen));
        data.behind_ms = Some(ms_int(frozen.behind_ms));
        let not_started = if frozen.state == StreamPartitionRunState::Starting {
            "; it has not started reading"
        } else {
            ""
        };
        let tail = frozen.tail_id.unwrap_or(StreamId::MIN);
        return unhealthy(
            StreamHealthRule::BehindTail,
            format!(
                "Streams: partition {} is behind the tail of its stream (position {}, tail {}) and its position has not changed for {}s, past its UnhealthyBehindSeconds of {}{}. There is an entry it has not read and it is not reading it; a restart hands the partition on.",
                describe(frozen),
                frozen.position,
                tail,
                seconds(frozen.behind_ms),
                frozen.unhealthy_behind_seconds,
                not_started
            ),
            data,
            Some(frozen),
        );
    }

    if context.consumers > 0 && context.consumers_started == 0 {
        return unhealthy(
            StreamHealthRule::NoConsumerStarted,
            format!(
                "Streams: none of the {} registered stream consumers has started.",
                context.consumers
            ),
            data,
            None,
        );
    }

    if !partitions.is_empty() && tally.live == 0 {
        let reason = match tally.stop_reason {
            Some(reason) => format!("Last reason: {reason}"),
            None => String::new(),
        };
        return unhealthy(
            StreamHealthRule::AllPartitionsStopped,
            format!(
                "Streams: all {} partition workers have stopped; nothing is being consumed. {reason}",
                partitions.len()
            ),
            data,
            None,
        );
    }

    data.ownership_degraded = Some(context.ownership_degraded);

    // Starting is not Running: "nothing has read yet" is true for a second or two of every
    // normal startup and stays true for as long as the fault lasts. Degraded, not Unhealthy:
    // restarting a pod that is still starting is a loop.
    if tally.running == 0 && tally.starting > 0 {
        return StreamHealthReport {
            status: StreamHealthStatus::Degraded,
            rule: StreamHealthRule::NotReadingYet,
            description: format!(
                "Streams: none of the {} partition worker(s) has started reading yet ({} still starting).",
                partitions.len(),
                tally.starting
            ),
            details: data,
            partition: None,
        };
    }

    if tally.blocked > 0
        || tally.stopped > 0
        || tally.lagging.is_some()
        || context.ownership_degraded
    {
        let rule = if tally.blocked > 0 {
            StreamHealthRule::Blocked
        } else if tally.stopped > 0 {
            StreamHealthRule::Stopped
        } else if tally.lagging.is_some() {
            StreamHealthRule::Lagging
        } else {
            StreamHealthRule::OwnershipDegraded
        };
        let partition = if rule == StreamHealthRule::Lagging {
            tally.lagging.cloned()
        } else {
            None
        };
        return StreamHealthReport {
            status: StreamHealthStatus::Degraded,
            rule,
            description: explain(&tally, context.ownership_degraded),
            details: data,
            partition,
        };
    }

    // Last, so it never hides a rule that names a fault: this one is about a consumer that is
    // reading, and has nothing to read.
    if let Some(silent) = find_silent(partitions) {
        data.silent_consumer = Some(format!("{}/{}", silent.topic, silent.consumer));
        data.silent_ms = Some(ms_int(silent.silent_ms));
        return StreamHealthReport {
            status: StreamHealthStatus::Degraded,
            rule: StreamHealthRule::Silent,
            description: format!(
                "Streams: consumer {} on {} is caught up and receiving nothing: none of the {} partition(s) it reads here has moved for {}s, past its DegradedSilenceSeconds of {}. A quiet topic and a broken upstream look the same from here; a restart fixes neither.",
                silent.consumer,
                silent.topic,
                silent.partitions,
                seconds(silent.silent_ms),
                silent.degraded_silence_seconds
            ),
            details: data,
            partition: None,
        };
    }

    StreamHealthReport {
        status: StreamHealthStatus::Healthy,
        rule: StreamHealthRule::None,
        description: format!(
            "Streams: {} consumer(s), {} of {} partition(s) reading, worst lag {}s.",
            context.consumers_started,
            tally.running,
            partitions.len(),
            seconds(tally.worst_lag_ms)
        ),
        details: data,
        partition: None,
    }
}

/// Grades one partition.
#[must_use]
pub fn grade(partition: &StreamPartitionStatus) -> StreamPartitionHealth {
    // First: the one rule that needs nothing this process reads from Redis. `stalled_ms` is
    // already 0 for the states the rule does not apply to.
    if partition.stood_down_stalled
        || (partition.unhealthy_stalled_seconds > 0
            && partition.stalled_ms >= f64::from(partition.unhealthy_stalled_seconds) * 1000.0)
    {
        return StreamPartitionHealth {
            status: StreamHealthStatus::Unhealthy,
            rule: StreamHealthRule::Stalled,
        };
    }
    match partition.state {
        StreamPartitionRunState::Blocked => {
            return if partition.blocked_ms > f64::from(partition.unhealthy_block_seconds) * 1000.0 {
                StreamPartitionHealth {
                    status: StreamHealthStatus::Unhealthy,
                    rule: StreamHealthRule::BlockedTooLong,
                }
            } else {
                StreamPartitionHealth {
                    status: StreamHealthStatus::Degraded,
                    rule: StreamHealthRule::Blocked,
                }
            };
        }
        StreamPartitionRunState::Stopped => {
            // A stood-down partition nobody chose to stop, with entries piling up behind it.
            // -1 (never sampled) and 0 (at the tail) both mean there is nothing to escalate yet.
            return if partition.escalates
                && partition.lag_entries > 0
                && partition.stopped_ms > f64::from(partition.unhealthy_stopped_seconds) * 1000.0
            {
                StreamPartitionHealth {
                    status: StreamHealthStatus::Unhealthy,
                    rule: StreamHealthRule::StoppedTooLong,
                }
            } else {
                StreamPartitionHealth {
                    status: StreamHealthStatus::Degraded,
                    rule: StreamHealthRule::Stopped,
                }
            };
        }
        StreamPartitionRunState::Starting | StreamPartitionRunState::Running => {}
    }

    // IsBehindTail is checked as well, so a hand-built snapshot cannot be Unhealthy on a
    // duration alone.
    if partition.unhealthy_behind_seconds > 0
        && partition.is_behind_tail
        && partition.behind_ms >= f64::from(partition.unhealthy_behind_seconds) * 1000.0
    {
        return StreamPartitionHealth {
            status: StreamHealthStatus::Unhealthy,
            rule: StreamHealthRule::BehindTail,
        };
    }

    // Zero entries behind is the tail of the stream, where a large lag only means a quiet topic.
    if partition.lag_ms > f64::from(partition.unhealthy_lag_ms) && partition.lag_entries != 0 {
        StreamPartitionHealth {
            status: StreamHealthStatus::Degraded,
            rule: StreamHealthRule::Lagging,
        }
    } else {
        StreamPartitionHealth {
            status: StreamHealthStatus::Healthy,
            rule: StreamHealthRule::None,
        }
    }
}

/// The caught-up, silent consumer with the longest silence, if any.
#[must_use]
pub fn find_silent(partitions: &[StreamPartitionStatus]) -> Option<StreamSilentConsumer> {
    let mut worst: Option<StreamSilentConsumer> = None;
    let stale_after_ms = TAIL_STALE_AFTER.as_secs_f64() * 1000.0;
    for (i, first) in partitions.iter().enumerate() {
        if first.degraded_silence_seconds == 0 || first.state != StreamPartitionRunState::Running {
            continue;
        }
        // Grade a consumer once, at its first running partition in the snapshot.
        let seen = partitions[..i]
            .iter()
            .any(|p| p.state == StreamPartitionRunState::Running && same_consumer(p, first));
        if seen {
            continue;
        }
        let window_ms = f64::from(first.degraded_silence_seconds) * 1000.0;
        let mut owned = 0_u32;
        let mut shortest = f64::MAX;
        let mut all_silent = true;
        for p in &partitions[i..] {
            if !all_silent {
                break;
            }
            if p.state != StreamPartitionRunState::Running || !same_consumer(p, first) {
                continue;
            }
            owned += 1;
            all_silent = !p.is_behind_tail
                && p.tail_id.is_some()
                && p.tail_sample_age_ms >= 0.0
                && p.tail_sample_age_ms <= stale_after_ms
                && p.position_unchanged_ms >= window_ms;
            shortest = shortest.min(p.position_unchanged_ms);
        }
        if all_silent && worst.as_ref().is_none_or(|w| shortest > w.silent_ms) {
            worst = Some(StreamSilentConsumer {
                topic: first.topic.clone(),
                consumer: first.consumer.clone(),
                partitions: owned,
                silent_ms: shortest,
                degraded_silence_seconds: first.degraded_silence_seconds,
            });
        }
    }
    worst
}

fn same_consumer(a: &StreamPartitionStatus, b: &StreamPartitionStatus) -> bool {
    a.topic == b.topic && a.consumer == b.consumer
}

/// Counts and the partitions that name each rule.
#[derive(Default)]
struct Tally<'a> {
    live: u32,
    running: u32,
    starting: u32,
    blocked: u32,
    stopped: u32,
    behind: u32,
    worst_lag_ms: f64,
    worst: Option<&'a StreamPartitionStatus>,
    lagging: Option<&'a StreamPartitionStatus>,
    stopped_too_long: Option<&'a StreamPartitionStatus>,
    blocked_too_long: Option<&'a StreamPartitionStatus>,
    behind_too_long: Option<&'a StreamPartitionStatus>,
    stalled: Option<&'a StreamPartitionStatus>,
    stop_reason: Option<&'a str>,
}

impl<'a> Tally<'a> {
    fn of(partitions: &'a [StreamPartitionStatus], data: &mut StreamHealthDetails) -> Self {
        let mut counts = Self::default();
        for partition in partitions {
            match partition.state {
                StreamPartitionRunState::Blocked => counts.blocked += 1,
                StreamPartitionRunState::Stopped => {
                    counts.stopped += 1;
                    counts.stop_reason = counts.stop_reason.or(partition.stop_reason.as_deref());
                }
                StreamPartitionRunState::Starting => counts.starting += 1,
                StreamPartitionRunState::Running => counts.running += 1,
            }
            if partition.lag_ms > counts.worst_lag_ms {
                counts.worst_lag_ms = partition.lag_ms;
                counts.worst = Some(partition);
            }
            if partition.is_behind_tail {
                counts.behind += 1;
            }
            match grade(partition).rule {
                StreamHealthRule::Stalled => {
                    counts.stalled = counts.stalled.or(Some(partition));
                }
                StreamHealthRule::BlockedTooLong => {
                    counts.blocked_too_long = counts.blocked_too_long.or(Some(partition));
                }
                StreamHealthRule::StoppedTooLong => {
                    counts.stopped_too_long = counts.stopped_too_long.or(Some(partition));
                }
                StreamHealthRule::BehindTail => {
                    counts.behind_too_long = counts.behind_too_long.or(Some(partition));
                }
                _ => {}
            }
            // Asked of every partition, not taken from grade: a blocked or stopped partition's lag
            // still belongs in the Degraded explanation.
            if partition.lag_ms > f64::from(partition.unhealthy_lag_ms)
                && partition.lag_entries != 0
            {
                counts.lagging = counts.lagging.or(Some(partition));
            }
        }
        // Blocked partitions are live: they are retrying, not gone. Starting ones have not stood
        // down, but they are not reading either, and have their own counter.
        counts.live = counts.running + counts.blocked + counts.starting;
        data.partitions_blocked = Some(counts.blocked);
        data.partitions_stopped = Some(counts.stopped);
        data.partitions_starting = Some(counts.starting);
        data.partitions_behind = Some(counts.behind);
        data.worst_lag_ms = Some(ms_int(counts.worst_lag_ms));
        if let Some(worst) = counts.worst {
            data.worst_lag_partition = Some(describe(worst));
            data.worst_lag_entries = Some(worst.lag_entries);
        }
        counts
    }
}

fn explain(counts: &Tally<'_>, ownership_degraded: bool) -> String {
    let mut reasons: Vec<String> = Vec::with_capacity(4);
    if counts.blocked > 0 {
        reasons.push(format!(
            "{} partition(s) blocked retrying a blocking error; positions are not advancing",
            counts.blocked
        ));
    }
    if counts.stopped > 0 {
        reasons.push(match counts.stop_reason {
            None => format!("{} partition(s) stopped", counts.stopped),
            Some(reason) => format!("{} partition(s) stopped ({reason})", counts.stopped),
        });
    }
    if let Some(lagging) = counts.lagging {
        reasons.push(format!(
            "partition {} is {}s behind, past its UnhealthyLagMs of {}ms",
            describe(lagging),
            seconds(lagging.lag_ms),
            lagging.unhealthy_lag_ms
        ));
    }
    if ownership_degraded {
        reasons.push(
            "the ownership registry shows a gap or an overlap — check STREAMS_INSTANCE_COUNT against spec.replicas, and that a partitioned consumer runs as a StatefulSet".to_owned(),
        );
    }
    format!("Streams: {}.", reasons.join("; "))
}

fn unhealthy(
    rule: StreamHealthRule,
    description: String,
    data: StreamHealthDetails,
    partition: Option<&StreamPartitionStatus>,
) -> StreamHealthReport {
    StreamHealthReport {
        status: StreamHealthStatus::Unhealthy,
        rule,
        description,
        details: data,
        partition: partition.cloned(),
    }
}

fn describe(partition: &StreamPartitionStatus) -> String {
    format!(
        "{}[{}]/{}",
        partition.topic, partition.partition, partition.consumer
    )
}

/// Seconds with one decimal, as C# `F1` (`(ms / 1000).ToString("F1")`).
fn seconds(milliseconds: f64) -> String {
    let mut out = String::new();
    let _ = write!(out, "{:.1}", milliseconds / 1000.0);
    out
}

/// Milliseconds as a report integer, truncated toward zero.
#[allow(clippy::cast_possible_truncation)]
fn ms_int(milliseconds: f64) -> i64 {
    milliseconds as i64
}
