//! Spans and counters (C# `EventSourcingSpans` / `EventSourcingDiagnostics`).
//!
//! Spans are `tracing` spans `eventsourcing.save` and `eventsourcing.load`; the C# display name
//! (`eventsourcing.save <Aggregate>`) is in `otel.name`, with `otel.kind = "client"`. Attribute
//! names are the C# tag names. A failed span gets `otel.status_code = "ERROR"` and
//! `otel.status_description`; a lost version check gets the description
//! `Concurrency conflict: ...`.

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::Span;

/// The instrumentation scope of the C# package, also the meter name under the `otel` feature.
pub const SOURCE_NAME: &str = "RedisEvents.EventSourcing";

/// Counter name: successful saves.
pub const SAVES: &str = "eventsourcing.saves";
/// Counter name: lost version checks.
pub const SAVE_CONFLICTS: &str = "eventsourcing.save.conflicts";
/// Counter name: loads.
pub const LOADS: &str = "eventsourcing.loads";

static SAVE_TOTAL: AtomicU64 = AtomicU64::new(0);
static CONFLICT_TOTAL: AtomicU64 = AtomicU64::new(0);
static LOAD_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Successful saves in this process; mirrors [`SAVES`] without an OpenTelemetry pipeline.
#[must_use]
pub fn saves_total() -> u64 {
    SAVE_TOTAL.load(Ordering::Relaxed)
}

/// Lost version checks in this process; mirrors [`SAVE_CONFLICTS`].
#[must_use]
pub fn save_conflicts_total() -> u64 {
    CONFLICT_TOTAL.load(Ordering::Relaxed)
}

/// Loads in this process; mirrors [`LOADS`].
#[must_use]
pub fn loads_total() -> u64 {
    LOAD_TOTAL.load(Ordering::Relaxed)
}

#[cfg(feature = "otel")]
fn otel_add(name: &'static str, unit: &'static str, description: &'static str) {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    use opentelemetry::metrics::Counter;
    static COUNTERS: LazyLock<Mutex<HashMap<&'static str, Counter<u64>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    if let Ok(mut counters) = COUNTERS.lock() {
        counters
            .entry(name)
            .or_insert_with(|| {
                opentelemetry::global::meter(SOURCE_NAME)
                    .u64_counter(name)
                    .with_unit(unit)
                    .with_description(description)
                    .build()
            })
            .add(1, &[]);
    }
}

pub(crate) fn save_span(aggregate: &str, id: &str, expected_version: u64) -> Span {
    tracing::info_span!(
        "eventsourcing.save",
        otel.name = %format!("eventsourcing.save {aggregate}"),
        otel.kind = "client",
        "eventsourcing.aggregate" = aggregate,
        "eventsourcing.aggregate_id" = id,
        "eventsourcing.expected_version" = expected_version,
        "eventsourcing.version" = tracing::field::Empty,
        "eventsourcing.event_count" = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

pub(crate) fn saved(span: &Span, new_version: u64, event_count: usize) {
    SAVE_TOTAL.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "otel")]
    otel_add(SAVES, "saves", "Number of successful aggregate saves");
    span.record("eventsourcing.version", new_version);
    span.record("eventsourcing.event_count", event_count as u64);
}

pub(crate) fn concurrency_conflict(span: &Span) {
    CONFLICT_TOTAL.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "otel")]
    otel_add(
        SAVE_CONFLICTS,
        "conflicts",
        "Number of optimistic concurrency conflicts on save",
    );
    span.record("otel.status_code", "ERROR");
    span.record(
        "otel.status_description",
        "Concurrency conflict: the aggregate's version has changed.",
    );
}

pub(crate) fn failed(span: &Span, error: &dyn std::error::Error) {
    span.record("otel.status_code", "ERROR");
    span.record("otel.status_description", error.to_string());
}

pub(crate) fn load_span(aggregate: &str, id: &str) -> Span {
    tracing::info_span!(
        "eventsourcing.load",
        otel.name = %format!("eventsourcing.load {aggregate}"),
        otel.kind = "client",
        "eventsourcing.aggregate" = aggregate,
        "eventsourcing.aggregate_id" = id,
        "eventsourcing.found" = tracing::field::Empty,
        "eventsourcing.version" = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
        otel.status_description = tracing::field::Empty,
    )
}

pub(crate) fn loaded(span: &Span, version: Option<u64>) {
    LOAD_TOTAL.fetch_add(1, Ordering::Relaxed);
    #[cfg(feature = "otel")]
    otel_add(LOADS, "loads", "Number of aggregate loads");
    span.record("eventsourcing.found", version.is_some());
    if let Some(v) = version {
        span.record("eventsourcing.version", v);
    }
}
