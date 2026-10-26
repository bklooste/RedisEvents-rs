//! Metric instruments, names and units identical to C# `StreamsDiagnostics`.
//!
//! Counters and histograms are recorded inline. Gauges are levels, so each is backed by a keyed
//! store (`"topic:partition:consumer"`, see the `set_*` functions) that the gauge callback reads at
//! scrape time. A series disappears only when its key is cleared, or, for the counts that are
//! meaningless at zero, when the count reaches zero.
//!
//! With the `otel` feature the instruments are created on the global meter named [`METER_NAME`]
//! (through the `opentelemetry` API). Without it the recording functions are no-ops, but the gauge
//! store still works, so the gauge semantics stay testable without a meter provider.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

#[cfg(feature = "otel")]
use std::sync::LazyLock;

#[cfg(feature = "otel")]
use opentelemetry::KeyValue;
#[cfg(feature = "otel")]
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};

#[cfg(feature = "otel")]
use super::METER_NAME;
use super::metrics;

/// Gauge (not in the frozen [`metrics`] module): entries waiting in a buffered publisher's queue.
pub(crate) const BUFFER_QUEUED: &str = "streams.buffer.queued";
/// Gauge (not in the frozen [`metrics`] module): entries held in a partition stream (`XLEN`).
pub(crate) const STREAM_LENGTH: &str = "streams.stream.length";

/// A gauge tag value.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TagValue {
    /// A string tag (topic, consumer, ...).
    Str(String),
    /// A numeric tag (partition index), as C# parses it.
    Int(i64),
}

/// A gauge level, integer or fractional.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum GaugeValue {
    /// Integer-valued gauge.
    Int(i64),
    /// Millisecond-valued gauge.
    Float(f64),
}

/// One reported series: its tags and current level.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Series {
    /// The tags, in C# order.
    pub(crate) tags: Vec<(&'static str, TagValue)>,
    /// The current level.
    pub(crate) value: GaugeValue,
}

/// How a series key maps onto tags.
#[derive(Clone, Copy)]
enum TagShape {
    Topic,
    TopicPartition,
    TopicConsumer,
    TopicPartitionConsumer,
}

type GaugeStore = BTreeMap<&'static str, BTreeMap<String, Series>>;

static GAUGES: Mutex<GaugeStore> = Mutex::new(BTreeMap::new());

fn gauges() -> MutexGuard<'static, GaugeStore> {
    GAUGES.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 1 while a partition is blocked retrying a blocking error, 0 once it is not. Reported at
/// 0 (never removed) so a recovery is visible and a dead pod is not mistaken for one.
pub(crate) fn set_blocked(key: &str, value: i64) {
    set_series(
        metrics::BLOCKED,
        key,
        GaugeValue::Int(value),
        TagShape::TopicPartitionConsumer,
        false,
    );
}

/// How long the blocked partition has been blocked, 0 once it is not.
pub(crate) fn set_block_duration_ms(key: &str, milliseconds: f64) {
    set_series(
        metrics::BLOCK_DURATION_MS,
        key,
        GaugeValue::Float(milliseconds),
        TagShape::TopicPartitionConsumer,
        false,
    );
}

/// Partitions nobody owns. Zero removes the series.
pub(crate) fn set_unowned_partitions(key: &str, count: i64) {
    set_series(
        metrics::PARTITIONS_UNOWNED,
        key,
        GaugeValue::Int(count),
        TagShape::TopicConsumer,
        true,
    );
}

/// Partitions claimed by more than one instance. Zero removes the series.
pub(crate) fn set_contested_partitions(key: &str, count: i64) {
    set_series(
        metrics::PARTITIONS_CONTESTED,
        key,
        GaugeValue::Int(count),
        TagShape::TopicConsumer,
        true,
    );
}

/// Entries between the consumer position and the tail. Zero is kept.
pub(crate) fn set_lag_entries(key: &str, entries: i64) {
    set_series(
        metrics::LAG_ENTRIES,
        key,
        GaugeValue::Int(entries),
        TagShape::TopicPartitionConsumer,
        false,
    );
}

/// Age of the oldest unprocessed entry, in milliseconds. Zero is kept.
pub(crate) fn set_lag_ms(key: &str, milliseconds: f64) {
    set_series(
        metrics::LAG_MS,
        key,
        GaugeValue::Float(milliseconds),
        TagShape::TopicPartitionConsumer,
        false,
    );
}

/// A buffered publisher's queue depth (`key` is the topic). Zero is kept.
pub(crate) fn set_buffer_queued(topic: &str, queued: i64) {
    set_series(
        BUFFER_QUEUED,
        topic,
        GaugeValue::Int(queued),
        TagShape::Topic,
        false,
    );
}

/// Entries in one partition's stream (`key` is `"topic:partition"`). Zero is kept.
pub(crate) fn set_stream_length(key: &str, length: i64) {
    set_series(
        STREAM_LENGTH,
        key,
        GaugeValue::Int(length),
        TagShape::TopicPartition,
        false,
    );
}

/// Stops reporting every per-partition consumer gauge for `key` (`"topic:partition:consumer"`).
pub(crate) fn clear_partition(key: &str) {
    let mut store = gauges();
    for name in [
        metrics::BLOCKED,
        metrics::BLOCK_DURATION_MS,
        metrics::LAG_ENTRIES,
        metrics::LAG_MS,
    ] {
        if let Some(series) = store.get_mut(name) {
            series.remove(key);
        }
    }
}

/// Stops reporting a buffered publisher's queue depth.
pub(crate) fn clear_buffer_queued(topic: &str) {
    remove_series(BUFFER_QUEUED, topic);
}

/// Stops reporting a stream's length.
pub(crate) fn clear_stream_length(key: &str) {
    remove_series(STREAM_LENGTH, key);
}

/// The current series of one gauge, for scrape callbacks and tests.
pub(crate) fn gauge_series(name: &str) -> Vec<Series> {
    gauges()
        .get(name)
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default()
}

fn set_series(
    name: &'static str,
    key: &str,
    value: GaugeValue,
    shape: TagShape,
    remove_at_zero: bool,
) {
    let at_or_below_zero = match value {
        GaugeValue::Int(v) => v <= 0,
        GaugeValue::Float(v) => v <= 0.0,
    };
    if remove_at_zero && at_or_below_zero {
        remove_series(name, key);
        return;
    }

    let mut store = gauges();
    let series = store.entry(name).or_default();
    match series.get_mut(key) {
        Some(existing) => existing.value = value,
        None => {
            series.insert(
                key.to_owned(),
                Series {
                    tags: tags_for(key, shape),
                    value,
                },
            );
        }
    }
}

fn remove_series(name: &'static str, key: &str) {
    if let Some(series) = gauges().get_mut(name) {
        series.remove(key);
    }
}

/// Splits a `":"`-joined key into tags. Extra segments are ignored; a missing one is `"unknown"`.
fn tags_for(key: &str, shape: TagShape) -> Vec<(&'static str, TagValue)> {
    let parts: Vec<&str> = key.split(':').collect();
    let part = |i: usize| parts.get(i).copied().unwrap_or("unknown").to_owned();
    let partition = |i: usize| {
        let text = part(i);
        text.parse::<i32>()
            .map_or(TagValue::Str(text), |n| TagValue::Int(i64::from(n)))
    };
    let topic = || ("topic", TagValue::Str(part(0)));

    match shape {
        TagShape::Topic => vec![topic()],
        TagShape::TopicPartition => vec![topic(), ("partition", partition(1))],
        TagShape::TopicConsumer => vec![topic(), ("consumer", TagValue::Str(part(1)))],
        TagShape::TopicPartitionConsumer => {
            vec![
                topic(),
                ("partition", partition(1)),
                ("consumer", TagValue::Str(part(2))),
            ]
        }
    }
}

// ---------------------------------------------------------------- recording (inline)

/// Counts messages published, tagged topic and partition.
pub(crate) fn record_published(topic: &str, partition: u32, count: u64) {
    #[cfg(feature = "otel")]
    instruments()
        .published
        .add(count, &[topic_kv(topic), partition_kv(partition)]);
    let _ = (topic, partition, count);
}

/// Counts messages consumed, tagged topic, partition and consumer.
pub(crate) fn record_consumed(topic: &str, partition: u32, consumer: &str, count: u64) {
    #[cfg(feature = "otel")]
    instruments().consumed.add(
        count,
        &[
            topic_kv(topic),
            partition_kv(partition),
            consumer_kv(consumer),
        ],
    );
    let _ = (topic, partition, consumer, count);
}

/// Counts messages the consumer's type filter dropped, tagged topic and consumer.
pub(crate) fn record_filtered(topic: &str, consumer: &str, count: u64) {
    #[cfg(feature = "otel")]
    instruments()
        .filtered
        .add(count, &[topic_kv(topic), consumer_kv(consumer)]);
    let _ = (topic, consumer, count);
}

/// Counts one processing error, tagged topic, consumer, policy and exception type.
pub(crate) fn record_error(topic: &str, consumer: &str, policy: &str, exception_type: &str) {
    #[cfg(feature = "otel")]
    instruments().errors.add(
        1,
        &[
            topic_kv(topic),
            consumer_kv(consumer),
            KeyValue::new("policy", policy.to_owned()),
            KeyValue::new("exception.type", exception_type.to_owned()),
        ],
    );
    let _ = (topic, consumer, policy, exception_type);
}

/// Counts one background-trimmer clamp release. Every increment means a consumer lost backlog.
pub(crate) fn record_clamp_released(topic: &str, partition: u32, consumer: &str) {
    #[cfg(feature = "otel")]
    instruments().clamp_released.add(
        1,
        &[
            topic_kv(topic),
            partition_kv(partition),
            consumer_kv(consumer),
        ],
    );
    let _ = (topic, partition, consumer);
}

/// Counts one failed position flush.
pub(crate) fn record_position_flush_failure(topic: &str, consumer: &str) {
    #[cfg(feature = "otel")]
    instruments()
        .positions_flush_failures
        .add(1, &[topic_kv(topic), consumer_kv(consumer)]);
    let _ = (topic, consumer);
}

/// Counts entries a full buffered publisher shed under `DropOldest`.
pub(crate) fn record_buffer_dropped(topic: &str) {
    #[cfg(feature = "otel")]
    instruments().buffer_dropped.add(1, &[topic_kv(topic)]);
    let _ = topic;
}

/// Records one batch's size and processing time (milliseconds), tagged topic and consumer.
pub(crate) fn record_batch(topic: &str, consumer: &str, size: u64, duration_ms: f64) {
    #[cfg(feature = "otel")]
    {
        let attrs = [topic_kv(topic), consumer_kv(consumer)];
        instruments().batch_size.record(size, &attrs);
        instruments().batch_duration.record(duration_ms, &attrs);
    }
    let _ = (topic, consumer, size, duration_ms);
}

/// Creates the instruments now, so the meter and its gauges exist before the first measurement.
/// Optional: every recording function creates them on first use.
pub(crate) fn ensure_initialized() {
    #[cfg(feature = "otel")]
    let _ = instruments();
}

// ---------------------------------------------------------------- otel instruments

#[cfg(feature = "otel")]
fn topic_kv(topic: &str) -> KeyValue {
    KeyValue::new("topic", topic.to_owned())
}

#[cfg(feature = "otel")]
fn consumer_kv(consumer: &str) -> KeyValue {
    KeyValue::new("consumer", consumer.to_owned())
}

#[cfg(feature = "otel")]
fn partition_kv(partition: u32) -> KeyValue {
    KeyValue::new("partition", i64::from(partition))
}

/// Every instrument, created once on the global meter. Gauge handles must stay alive for their
/// callbacks to keep reporting, so they are held here.
#[cfg(feature = "otel")]
struct Instruments {
    published: Counter<u64>,
    consumed: Counter<u64>,
    filtered: Counter<u64>,
    errors: Counter<u64>,
    clamp_released: Counter<u64>,
    positions_flush_failures: Counter<u64>,
    buffer_dropped: Counter<u64>,
    batch_duration: Histogram<f64>,
    batch_size: Histogram<u64>,
    blocked: ObservableGauge<i64>,
    block_duration_ms: ObservableGauge<f64>,
    unowned: ObservableGauge<i64>,
    contested: ObservableGauge<i64>,
    lag_entries: ObservableGauge<i64>,
    lag_ms: ObservableGauge<f64>,
    buffer_queued: ObservableGauge<i64>,
    stream_length: ObservableGauge<i64>,
}

#[cfg(feature = "otel")]
#[allow(clippy::too_many_lines)] // one declarative table of every instrument
fn instruments() -> &'static Instruments {
    static INSTRUMENTS: LazyLock<Instruments> = LazyLock::new(|| {
        let meter = opentelemetry::global::meter(METER_NAME);
        Instruments {
            published: meter
                .u64_counter("streams.published")
                .with_unit("messages")
                .with_description("Number of messages published")
                .build(),
            consumed: meter
                .u64_counter("streams.consumed")
                .with_unit("messages")
                .with_description("Number of messages consumed")
                .build(),
            filtered: meter
                .u64_counter("streams.filtered")
                .with_unit("messages")
                .with_description("Number of messages filtered")
                .build(),
            errors: meter
                .u64_counter("streams.errors")
                .with_unit("messages")
                .with_description("Number of processing errors")
                .build(),
            clamp_released: meter
                .u64_counter("streams.clamp.released")
                .with_unit("messages")
                .with_description("Number of clamp releases")
                .build(),
            positions_flush_failures: meter
                .u64_counter("streams.positions.flush_failures")
                .with_unit("failures")
                .with_description("Number of position flush failures")
                .build(),
            buffer_dropped: meter
                .u64_counter("streams.buffer.dropped")
                .with_unit("messages")
                .with_description(
                    "Entries dropped by a buffered publisher because its queue was full",
                )
                .build(),
            batch_duration: meter
                .f64_histogram("streams.batch.duration")
                .with_unit("ms")
                .with_description("Duration of batch processing")
                .build(),
            batch_size: meter
                .u64_histogram("streams.batch.size")
                .with_unit("messages")
                .with_description("Number of messages in a batch")
                .build(),
            blocked: observable_i64(
                &meter,
                metrics::BLOCKED,
                "status",
                "Whether a partition is blocked",
            ),
            block_duration_ms: observable_f64(
                &meter,
                metrics::BLOCK_DURATION_MS,
                "ms",
                "How long a partition has been blocked",
            ),
            unowned: observable_i64(
                &meter,
                metrics::PARTITIONS_UNOWNED,
                "partitions",
                "Number of unowned partitions",
            ),
            contested: observable_i64(
                &meter,
                metrics::PARTITIONS_CONTESTED,
                "partitions",
                "Number of contested partitions",
            ),
            lag_entries: observable_i64(
                &meter,
                metrics::LAG_ENTRIES,
                "entries",
                "Entries between the consumer position and the stream tail",
            ),
            lag_ms: observable_f64(
                &meter,
                metrics::LAG_MS,
                "ms",
                "Age of the oldest unprocessed entry",
            ),
            buffer_queued: observable_i64(
                &meter,
                BUFFER_QUEUED,
                "messages",
                "Entries waiting in a buffered publisher queue",
            ),
            stream_length: observable_i64(
                &meter,
                STREAM_LENGTH,
                "entries",
                "Entries currently held in a partition stream",
            ),
        }
    });
    &INSTRUMENTS
}

#[cfg(feature = "otel")]
fn observable_i64(
    meter: &opentelemetry::metrics::Meter,
    name: &'static str,
    unit: &'static str,
    description: &'static str,
) -> ObservableGauge<i64> {
    meter
        .i64_observable_gauge(name)
        .with_unit(unit)
        .with_description(description)
        .with_callback(move |observer| {
            for series in gauge_series(name) {
                if let GaugeValue::Int(value) = series.value {
                    observer.observe(value, &attributes(&series.tags));
                }
            }
        })
        .build()
}

#[cfg(feature = "otel")]
fn observable_f64(
    meter: &opentelemetry::metrics::Meter,
    name: &'static str,
    unit: &'static str,
    description: &'static str,
) -> ObservableGauge<f64> {
    meter
        .f64_observable_gauge(name)
        .with_unit(unit)
        .with_description(description)
        .with_callback(move |observer| {
            for series in gauge_series(name) {
                if let GaugeValue::Float(value) = series.value {
                    observer.observe(value, &attributes(&series.tags));
                }
            }
        })
        .build()
}

#[cfg(feature = "otel")]
fn attributes(tags: &[(&'static str, TagValue)]) -> Vec<KeyValue> {
    tags.iter()
        .map(|(key, value)| match value {
            TagValue::Str(s) => KeyValue::new(*key, s.clone()),
            TagValue::Int(i) => KeyValue::new(*key, *i),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    //! Ported from C# `MetricsTests`. Each test uses its own topic, because the gauge store is
    //! process-wide and tests run in parallel.

    use super::*;

    fn unique(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
    }

    fn series_for(name: &str, topic: &str) -> Vec<Series> {
        gauge_series(name)
            .into_iter()
            .filter(|s| s.tags.contains(&("topic", TagValue::Str(topic.to_owned()))))
            .collect()
    }

    fn values_for(name: &str, topic: &str) -> Vec<GaugeValue> {
        series_for(name, topic)
            .into_iter()
            .map(|s| s.value)
            .collect()
    }

    #[test]
    fn blocked_reports_one_then_zero_and_only_stops_when_the_partition_is_released() {
        let topic = unique("metrics-blocked");
        let key = format!("{topic}:0:reader");

        set_blocked(&key, 1);
        assert_eq!(values_for(metrics::BLOCKED, &topic), [GaugeValue::Int(1)]);

        set_blocked(&key, 0);
        assert_eq!(
            values_for(metrics::BLOCKED, &topic),
            [GaugeValue::Int(0)],
            "an unblocked partition reports 0; vanishing would be indistinguishable from a dead pod"
        );

        clear_partition(&key);
        assert!(
            series_for(metrics::BLOCKED, &topic).is_empty(),
            "a partition this process no longer consumes stops publishing, which is what absence means"
        );
    }

    #[test]
    fn block_duration_falls_back_to_zero_with_its_blocked_gauge() {
        let topic = unique("metrics-duration");
        let key = format!("{topic}:1:reader");

        set_block_duration_ms(&key, 4200.0);
        assert_eq!(
            values_for(metrics::BLOCK_DURATION_MS, &topic),
            [GaugeValue::Float(4200.0)]
        );

        set_block_duration_ms(&key, 0.0);
        assert_eq!(
            values_for(metrics::BLOCK_DURATION_MS, &topic),
            [GaugeValue::Float(0.0)],
            "the pair has to rise and fall together, or a dashboard shows a blocked partition with no duration"
        );

        clear_partition(&key);
        assert_eq!(series_for(metrics::BLOCK_DURATION_MS, &topic).len(), 0);
    }

    #[test]
    fn blocked_gauge_carries_topic_partition_and_consumer_tags() {
        let topic = unique("metrics-tags");
        let key = format!("{topic}:3:orders-reader");
        set_blocked(&key, 1);

        let series = series_for(metrics::BLOCKED, &topic);
        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0].tags,
            vec![
                ("topic", TagValue::Str(topic.clone())),
                ("partition", TagValue::Int(3)),
                ("consumer", TagValue::Str("orders-reader".to_owned())),
            ]
        );

        clear_partition(&key);
    }
}
