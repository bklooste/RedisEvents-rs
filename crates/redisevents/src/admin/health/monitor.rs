//! Per-partition monitors, the process-wide monitor registry, and the lag sampler (C# `StreamLag.cs`:
//! `StreamPartitionMonitor`, `StreamLag`, `StreamLagSampler`). Crate-private: the consumer host
//! (C2/C4) creates a monitor per partition it reads and feeds it; `health.rs` grades the snapshots.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use redis::Value;
use time::OffsetDateTime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{
    StreamPartitionRunState, StreamPartitionStatus, TAIL_STALE_AFTER, estimate_entries_behind,
};
use crate::connection::Connection;
use crate::diagnostics::instruments;
use crate::error::{Error, Result};
use crate::wire::StreamId;

/// Clock for monitors: monotonic time for elapsed durations, wall time for lag. Tests inject a
/// manual clock (C#: `TimeProvider`).
pub(crate) trait HealthClock: Send + Sync + 'static {
    /// Monotonic now.
    fn now(&self) -> Instant;
    /// Wall-clock now, in Unix milliseconds.
    fn unix_ms(&self) -> i64;
}

/// The real clock.
pub(crate) struct SystemClock;

impl HealthClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn unix_ms(&self) -> i64 {
        i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
            .unwrap_or(i64::MAX)
    }
}

/// The thresholds a monitor grades against (copied from `ConsumerOptions`).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Thresholds {
    pub(crate) unhealthy_lag_ms: u32,
    pub(crate) unhealthy_block_seconds: u32,
    pub(crate) unhealthy_stopped_seconds: u32,
    pub(crate) unhealthy_behind_seconds: u32,
    pub(crate) degraded_silence_seconds: u32,
}

/// Progress against the stream's tail.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TailProgress {
    pub(crate) position: StreamId,
    pub(crate) position_unchanged_ms: f64,
    pub(crate) tail: Option<StreamId>,
    pub(crate) tail_sample_age_ms: f64,
    pub(crate) tail_unchanged_ms: f64,
    pub(crate) behind_ms: f64,
    pub(crate) is_behind: bool,
}

struct Progress {
    noted: StreamId,
    position_changed_at: Instant,
    tail_sampled: bool,
    tail: StreamId,
    last_entry_seen: StreamId,
    tail_sampled_at: Instant,
    tail_changed_at: Instant,
    behind: bool,
    behind_since: Instant,
}

struct Inner {
    /// `None` until something is observed.
    last_processed: Option<StreamId>,
    caught_up: bool,
    lag_entries: i64,
    state: StreamPartitionRunState,
    blocked_since: Option<Instant>,
    stopped_since: Option<Instant>,
    escalates: bool,
    stop_reason: Option<String>,
    progress: Progress,
}

/// Tracks one partition this process reads.
pub(crate) struct PartitionMonitor {
    topic: String,
    consumer: String,
    partition: u32,
    stream_key: String,
    metric_key: String,
    stream_metric_key: String,
    thresholds: Thresholds,
    start_position: StreamId,
    clock: Arc<dyn HealthClock>,
    inner: Mutex<Inner>,
}

fn elapsed_ms(from: Instant, to: Instant) -> f64 {
    to.saturating_duration_since(from).as_secs_f64() * 1000.0
}

impl PartitionMonitor {
    /// A monitor whose frozen-position clock starts now (C#: at acquisition).
    pub(crate) fn new(
        topic: &str,
        consumer: &str,
        partition: u32,
        stream_key: &str,
        thresholds: Thresholds,
        start_position: StreamId,
        clock: Arc<dyn HealthClock>,
    ) -> Self {
        let now = clock.now();
        Self {
            topic: topic.to_owned(),
            consumer: consumer.to_owned(),
            partition,
            stream_key: stream_key.to_owned(),
            metric_key: format!("{topic}:{partition}:{consumer}"),
            stream_metric_key: format!("{topic}:{partition}"),
            thresholds,
            start_position,
            clock,
            inner: Mutex::new(Inner {
                last_processed: None,
                caught_up: false,
                lag_entries: -1,
                state: StreamPartitionRunState::Starting,
                blocked_since: None,
                stopped_since: None,
                escalates: false,
                stop_reason: None,
                progress: Progress {
                    noted: start_position,
                    position_changed_at: now,
                    tail_sampled: false,
                    tail: StreamId::MIN,
                    last_entry_seen: StreamId::MIN,
                    tail_sampled_at: now,
                    tail_changed_at: now,
                    behind: false,
                    behind_since: now,
                },
            }),
        }
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) fn consumer(&self) -> &str {
        &self.consumer
    }

    pub(crate) fn partition(&self) -> u32 {
        self.partition
    }

    pub(crate) fn stream_key(&self) -> &str {
        &self.stream_key
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The position: the last observed id, or the start position before any.
    pub(crate) fn position(&self) -> StreamId {
        self.lock().last_processed.unwrap_or(self.start_position)
    }

    /// Records a processed entry.
    pub(crate) fn observe(&self, id: StreamId) {
        let mut inner = self.lock();
        inner.last_processed = Some(id);
        inner.caught_up = false;
    }

    /// Marks the partition as at the tail.
    pub(crate) fn mark_caught_up(&self) {
        self.lock().caught_up = true;
    }

    /// Records the entries-behind estimate from the last sample.
    pub(crate) fn set_lag_entries(&self, entries: i64) {
        self.lock().lag_entries = entries;
    }

    /// Records a tail sample: the last entry that is actually in the stream, `MIN` when empty.
    pub(crate) fn set_tail(&self, last_entry: StreamId) {
        let mut inner = self.lock();
        let now = self.clock.now();
        let p = &mut inner.progress;
        // A trim that empties the stream is not the tail moving: only a new last entry is.
        if !p.tail_sampled || (last_entry != StreamId::MIN && last_entry != p.last_entry_seen) {
            p.tail_changed_at = now;
        }
        if last_entry != StreamId::MIN {
            p.last_entry_seen = last_entry;
        }
        p.tail = last_entry;
        p.tail_sampled = true;
        p.tail_sampled_at = now;
        Self::note_progress(&mut inner, now, self.start_position);
    }

    /// Lets the sampler tick notice a position change without a sample.
    pub(crate) fn refresh_progress(&self) {
        let mut inner = self.lock();
        let now = self.clock.now();
        Self::note_progress(&mut inner, now, self.start_position);
    }

    fn note_progress(inner: &mut Inner, now: Instant, start: StreamId) {
        let position = inner.last_processed.unwrap_or(start);
        let p = &mut inner.progress;
        if position != p.noted {
            p.noted = position;
            p.position_changed_at = now;
            p.behind = false;
        }
        if p.tail_sampled && p.tail > position {
            if !p.behind {
                p.behind = true;
                p.behind_since = now;
            }
        } else {
            p.behind = false;
        }
    }

    /// Progress against the tail.
    pub(crate) fn read_progress(&self) -> TailProgress {
        let mut inner = self.lock();
        let now = self.clock.now();
        Self::note_progress(&mut inner, now, self.start_position);
        let p = &inner.progress;
        let sample_age = if p.tail_sampled {
            elapsed_ms(p.tail_sampled_at, now)
        } else {
            -1.0
        };
        // A sample that has stopped arriving says nothing about now.
        let fresh = p.tail_sampled && sample_age <= TAIL_STALE_AFTER.as_secs_f64() * 1000.0;
        TailProgress {
            position: p.noted,
            position_unchanged_ms: elapsed_ms(p.position_changed_at, now),
            tail: p.tail_sampled.then_some(p.tail),
            tail_sample_age_ms: sample_age,
            tail_unchanged_ms: if p.tail_sampled {
                elapsed_ms(p.tail_changed_at, now)
            } else {
                -1.0
            },
            behind_ms: if p.behind && fresh {
                elapsed_ms(p.behind_since, now)
            } else {
                0.0
            },
            is_behind: p.behind && fresh,
        }
    }

    /// The partition is being read.
    pub(crate) fn mark_running(&self) {
        let mut inner = self.lock();
        inner.stop_reason = None;
        inner.state = StreamPartitionRunState::Running;
        inner.stopped_since = None;
        inner.escalates = false;
        self.clear_block(&mut inner);
    }

    /// The partition is retrying a blocking failure.
    pub(crate) fn mark_blocked(&self) {
        let mut inner = self.lock();
        inner.state = StreamPartitionRunState::Blocked;
        if inner.blocked_since.is_none() {
            inner.blocked_since = Some(self.clock.now());
            instruments::set_blocked(&self.metric_key, 1);
        }
        let blocked = Self::blocked_ms(&inner, self.clock.as_ref());
        instruments::set_block_duration_ms(&self.metric_key, blocked);
    }

    /// Whether the partition is in the blocked state.
    pub(crate) fn is_blocked(&self) -> bool {
        self.lock().state == StreamPartitionRunState::Blocked
    }

    /// Whether the partition is in the stopped state.
    pub(crate) fn is_stopped(&self) -> bool {
        self.lock().state == StreamPartitionRunState::Stopped
    }

    /// The blocking failure cleared.
    pub(crate) fn mark_unblocked(&self) {
        let mut inner = self.lock();
        self.clear_block(&mut inner);
        inner.state = StreamPartitionRunState::Running;
    }

    /// The partition stood down. `escalate` is kept from the first stop only.
    pub(crate) fn mark_stopped(&self, reason: &str, escalate: bool) {
        let mut inner = self.lock();
        inner.stop_reason = Some(reason.to_owned());
        if inner.stopped_since.is_none() {
            inner.stopped_since = Some(self.clock.now());
            inner.escalates = escalate;
        }
        inner.state = StreamPartitionRunState::Stopped;
        self.clear_block(&mut inner);
    }

    /// Publishes `streams.lag.ms` and the block duration.
    pub(crate) fn publish_lag_ms(&self) {
        self.refresh_progress();
        let lag = self.lag_ms();
        instruments::set_lag_ms(&self.metric_key, lag);
        let inner = self.lock();
        if inner.blocked_since.is_some() {
            instruments::set_block_duration_ms(
                &self.metric_key,
                Self::blocked_ms(&inner, self.clock.as_ref()),
            );
        }
    }

    /// Age of the last processed entry, in ms; `0` when caught up, unknown, or skewed.
    #[allow(clippy::cast_precision_loss)]
    pub(crate) fn lag_ms(&self) -> f64 {
        let inner = self.lock();
        if inner.caught_up {
            return 0.0;
        }
        match inner.last_processed {
            None => 0.0,
            Some(id) => {
                let lag = self.clock.unix_ms() - i64::try_from(id.ms).unwrap_or(i64::MAX);
                // Redis's clock and ours differ; a small negative reading is skew, not negative lag.
                lag.max(0) as f64
            }
        }
    }

    /// Snapshot for grading.
    pub(crate) fn status(&self) -> StreamPartitionStatus {
        let progress = self.read_progress();
        let lag_ms = self.lag_ms();
        let inner = self.lock();
        let state = inner.state;
        let blocked_ms = Self::blocked_ms(&inner, self.clock.as_ref());
        let stopped_ms = inner
            .stopped_since
            .map_or(0.0, |since| elapsed_ms(since, self.clock.now()));
        StreamPartitionStatus {
            topic: self.topic.clone(),
            consumer: self.consumer.clone(),
            partition: self.partition,
            state,
            stop_reason: inner.stop_reason.clone(),
            last_processed: inner.last_processed.unwrap_or(StreamId::MIN),
            is_caught_up: inner.caught_up,
            lag_ms,
            lag_entries: inner.lag_entries,
            blocked_ms,
            stopped_ms,
            unhealthy_lag_ms: self.thresholds.unhealthy_lag_ms,
            unhealthy_block_seconds: self.thresholds.unhealthy_block_seconds,
            unhealthy_stopped_seconds: self.thresholds.unhealthy_stopped_seconds,
            unhealthy_behind_seconds: self.thresholds.unhealthy_behind_seconds,
            degraded_silence_seconds: self.thresholds.degraded_silence_seconds,
            escalates: state == StreamPartitionRunState::Stopped && inner.escalates,
            position: progress.position,
            position_unchanged_ms: progress.position_unchanged_ms,
            tail_id: progress.tail,
            tail_sample_age_ms: progress.tail_sample_age_ms,
            tail_unchanged_ms: progress.tail_unchanged_ms,
            is_behind_tail: progress.is_behind,
            behind_ms: progress.behind_ms,
        }
    }

    fn blocked_ms(inner: &Inner, clock: &dyn HealthClock) -> f64 {
        inner
            .blocked_since
            .map_or(0.0, |since| elapsed_ms(since, clock.now()))
    }

    fn clear_block(&self, inner: &mut Inner) {
        if inner.blocked_since.take().is_some() {
            instruments::set_blocked(&self.metric_key, 0);
            instruments::set_block_duration_ms(&self.metric_key, 0.0);
        }
    }
}

type Registry = BTreeMap<String, Arc<PartitionMonitor>>;

/// Process-wide monitors, keyed `topic:partition:consumer`.
static MONITORS: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn monitors() -> MutexGuard<'static, Registry> {
    MONITORS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Creates and registers a monitor on the real clock; replaces one with the same key.
pub(crate) fn track(
    topic: &str,
    consumer: &str,
    partition: u32,
    stream_key: &str,
    thresholds: Thresholds,
    start_position: StreamId,
) -> Result<Arc<PartitionMonitor>> {
    track_with_clock(
        topic,
        consumer,
        partition,
        stream_key,
        thresholds,
        start_position,
        Arc::new(SystemClock),
    )
}

/// [`track`] with an injected clock.
pub(crate) fn track_with_clock(
    topic: &str,
    consumer: &str,
    partition: u32,
    stream_key: &str,
    thresholds: Thresholds,
    start_position: StreamId,
    clock: Arc<dyn HealthClock>,
) -> Result<Arc<PartitionMonitor>> {
    if topic.trim().is_empty() || consumer.trim().is_empty() {
        return Err(Error::invalid_argument(
            "topic and consumer must not be blank",
        ));
    }
    let monitor = Arc::new(PartitionMonitor::new(
        topic,
        consumer,
        partition,
        stream_key,
        thresholds,
        start_position,
        clock,
    ));
    monitors().insert(monitor.metric_key.clone(), Arc::clone(&monitor));
    Ok(monitor)
}

/// Every registered monitor.
pub(crate) fn all() -> Vec<Arc<PartitionMonitor>> {
    monitors().values().cloned().collect()
}

/// Unregisters a monitor and clears its gauge series.
pub(crate) fn forget(monitor: &Arc<PartitionMonitor>) {
    monitors().remove(&monitor.metric_key);
    instruments::clear_partition(&monitor.metric_key);
    instruments::clear_stream_length(&monitor.stream_metric_key);
}

/// Unregisters every monitor.
pub(crate) fn clear() {
    let drained: Vec<_> = monitors().values().cloned().collect();
    for monitor in &drained {
        forget(monitor);
    }
}

/// Publishes lag for every monitor.
pub(crate) fn publish_lag_ms() {
    for monitor in all() {
        monitor.publish_lag_ms();
    }
}

/// Samples the stream of every registered monitor once.
/// A failed sample keeps the previous values; a missing stream is an empty one.
pub(crate) async fn sample_once(connection: &Connection) {
    let mut conn = connection.shared();
    for monitor in all() {
        let info = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(monitor.stream_key())
            .query_async::<Value>(&mut conn)
            .await;
        match info {
            Ok(info) => {
                let length = info_field(&info, "length").and_then(int_value).unwrap_or(0);
                let last = info_field(&info, "last-generated-id")
                    .and_then(text_value)
                    .and_then(|s| StreamId::parse(s).ok())
                    .unwrap_or(StreamId::MIN);
                let first = info_field(&info, "first-entry")
                    .and_then(entry_id)
                    .unwrap_or(StreamId::MIN);
                // The tail is the last entry actually present, not the last id ever generated.
                let tail = info_field(&info, "last-entry")
                    .and_then(entry_id)
                    .unwrap_or(StreamId::MIN);
                monitor.set_tail(tail);
                let behind = estimate_entries_behind(length, first, last, monitor.position());
                monitor.set_lag_entries(behind);
                instruments::set_stream_length(&monitor.stream_metric_key, length);
                instruments::set_lag_entries(&monitor.metric_key, behind);
            }
            Err(error)
                if error
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("no such key") =>
            {
                // The stream has not been written to yet: nothing behind us, nothing in it.
                monitor.set_tail(StreamId::MIN);
                monitor.set_lag_entries(0);
                instruments::set_lag_entries(&monitor.metric_key, 0);
                instruments::set_stream_length(&monitor.stream_metric_key, 0);
            }
            Err(error) => {
                tracing::debug!(
                    %error,
                    topic = monitor.topic(),
                    partition = monitor.partition(),
                    consumer = monitor.consumer(),
                    "streams: lag sample failed; the previous values are kept until the next sample"
                );
            }
        }
    }
}

/// Reads one field of an `XINFO STREAM` reply, in RESP2 (flat array) or RESP3 (map) form.
fn info_field<'a>(info: &'a Value, name: &str) -> Option<&'a Value> {
    match info {
        Value::Map(pairs) => pairs
            .iter()
            .find(|(k, _)| text_value(k) == Some(name))
            .map(|(_, v)| v),
        Value::Array(items) => items
            .chunks(2)
            .find(|pair| pair.len() == 2 && text_value(&pair[0]) == Some(name))
            .map(|pair| &pair[1]),
        _ => None,
    }
}

fn text_value(value: &Value) -> Option<&str> {
    match value {
        Value::BulkString(bytes) => std::str::from_utf8(bytes).ok(),
        Value::SimpleString(text) => Some(text),
        _ => None,
    }
}

fn int_value(value: &Value) -> Option<i64> {
    match value {
        Value::Int(n) => Some(*n),
        _ => None,
    }
}

/// The id of an `[id, [field, value, ...]]` entry reply, or `None` for nil.
fn entry_id(value: &Value) -> Option<StreamId> {
    match value {
        Value::Array(items) => items
            .first()
            .and_then(text_value)
            .and_then(|s| StreamId::parse(s).ok()),
        _ => None,
    }
}

/// Runs [`publish_lag_ms`] every `push` and [`sample_once`] every `sample` until stopped (C#
/// `StreamLagSampler`).
pub(crate) struct StreamLagSampler {
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl StreamLagSampler {
    /// Starts the loop on the current runtime.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] when either interval is zero.
    pub(crate) fn start(connection: Connection, sample: Duration, push: Duration) -> Result<Self> {
        if sample.is_zero() || push.is_zero() {
            return Err(Error::invalid_argument(
                "streams: lag sampler intervals must be positive",
            ));
        }
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(push);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_sample = Instant::now();
            loop {
                tokio::select! {
                    () = token.cancelled() => break,
                    _ = ticker.tick() => {}
                }
                publish_lag_ms();
                if last_sample.elapsed() < sample {
                    continue;
                }
                last_sample = Instant::now();
                sample_once(&connection).await;
            }
        });
        Ok(Self {
            cancel,
            task: Some(task),
        })
    }

    /// Cancels the loop and waits for it to finish.
    pub(crate) async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for StreamLagSampler {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
