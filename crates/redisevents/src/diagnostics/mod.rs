//! Metrics and tracing. **Names: W0.2** (final, identical to C# so dashboards serve both).
//! **Everything else: B6** (port of `StreamActivity.cs`, `StreamSpans.cs`, `StreamsDiagnostics.cs`,
//! `StreamNames.cs`, the `traceparent` parsing, and an `otel` feature for OpenTelemetry export).

/// The meter / tracer name.
pub const METER_NAME: &str = "RedisEvents";

/// Metric names.
pub mod metrics {
    /// Counter, messages published.
    pub const PUBLISHED: &str = "streams.published";
    /// Counter, messages consumed.
    pub const CONSUMED: &str = "streams.consumed";
    /// Counter, messages filtered out by type.
    pub const FILTERED: &str = "streams.filtered";
    /// Counter, processing errors.
    pub const ERRORS: &str = "streams.errors";
    /// Counter, clamp releases.
    pub const CLAMP_RELEASED: &str = "streams.clamp.released";
    /// Counter, failed position flushes.
    pub const POSITION_FLUSH_FAILURES: &str = "streams.positions.flush_failures";
    /// Counter, messages dropped by a full buffered publisher.
    pub const BUFFER_DROPPED: &str = "streams.buffer.dropped";
    /// Histogram (ms), batch processing time.
    pub const BATCH_DURATION: &str = "streams.batch.duration";
    /// Histogram, messages per batch.
    pub const BATCH_SIZE: &str = "streams.batch.size";
    /// Gauge, 1 while a partition is blocked.
    pub const BLOCKED: &str = "streams.blocked";
    /// Gauge (ms), how long a partition has been blocked.
    pub const BLOCK_DURATION_MS: &str = "streams.block.duration_ms";
    /// Gauge, unowned partitions.
    pub const PARTITIONS_UNOWNED: &str = "streams.partitions.unowned";
    /// Gauge, contested partitions.
    pub const PARTITIONS_CONTESTED: &str = "streams.partitions.contested";
    /// Gauge, entries between position and tail.
    pub const LAG_ENTRIES: &str = "streams.lag.entries";
    /// Gauge (ms), age of the oldest unprocessed entry.
    pub const LAG_MS: &str = "streams.lag.ms";
}

/// Span names.
pub mod spans {
    /// Producer span.
    pub const PUBLISH: &str = "streams.publish";
    /// Consumer span, one per batch.
    pub const PROCESS: &str = "streams.process";
}
