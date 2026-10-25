//! Spans, logging helpers and the malformed-event counter (C# `ProjectionsDiagnostics` /
//! `ProjectionsSpans`).
//!
//! Spans are `tracing` spans named `projections.project` and `projections.flush`; the C# display
//! name (`projections.project <wire type>`) is in the `otel.name` field, which
//! `tracing-opentelemetry` uses. A failed span gets `otel.status_code = "ERROR"` and
//! `otel.status_description`.

use std::sync::atomic::{AtomicU64, Ordering};

use redisevents::StreamId;
use tracing::Span;

/// The instrumentation scope of the C# package, also the meter name under the `otel` feature.
pub const SOURCE_NAME: &str = "RedisEvents.Projections";

/// Counter name: events skipped because their body failed to decode.
pub const MALFORMED_EVENTS: &str = "redisevents.projections.events.malformed";

static MALFORMED: AtomicU64 = AtomicU64::new(0);

/// How many malformed events this process has skipped (all projectors). Mirrors the
/// [`MALFORMED_EVENTS`] counter so it is observable without an OpenTelemetry pipeline.
#[must_use]
pub fn malformed_events_total() -> u64 {
    MALFORMED.load(Ordering::Relaxed)
}

pub(crate) fn count_malformed(topic: &str, wire_type: &str) {
    MALFORMED.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "otel")]
    {
        use std::sync::LazyLock;

        use opentelemetry::KeyValue;
        use opentelemetry::metrics::Counter;
        static COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
            opentelemetry::global::meter(SOURCE_NAME)
                .u64_counter(MALFORMED_EVENTS)
                .with_unit("events")
                .with_description("Events skipped because their body failed to decode")
                .build()
        });
        COUNTER.add(
            1,
            &[
                KeyValue::new("topic", topic.to_owned()),
                KeyValue::new("wire_type", wire_type.to_owned()),
            ],
        );
    }
    #[cfg(not(feature = "otel"))]
    let _ = (topic, wire_type);
}

/// `projections.project <wire type>`, one per dispatched event.
pub(crate) fn project_span(wire_type: &str, partition_key: &str, id: StreamId) -> Span {
    tracing::info_span!(
        "projections.project",
        otel.name = %format!("projections.project {wire_type}"),
        otel.kind = "internal",
        "projections.wire_type" = wire_type,
        "projections.partition_key" = partition_key,
        "projections.stream_id" = %id,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

/// `projections.flush <projection>`, one per flushed batch projection.
pub(crate) fn flush_span(projection: &str, topic: &str, partition: u32) -> Span {
    tracing::info_span!(
        "projections.flush",
        otel.name = %format!("projections.flush {projection}"),
        otel.kind = "internal",
        "projections.topic" = topic,
        "projections.partition" = partition,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

pub(crate) fn failed(span: &Span, error: &dyn std::error::Error) {
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", error.to_string());
}
