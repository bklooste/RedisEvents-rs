//! Spans: `streams.publish {topic}` (producer) and `streams.process {topic}` (consumer, one per
//! batch), with the attribute names C# `StreamSpans` / `StreamActivity` use.
//!
//! With the `otel` feature a span is an OpenTelemetry span on the global tracer named
//! [`METER_NAME`](super::METER_NAME), parented per [`plan_batch`](super::traceparent::plan_batch).
//! Without it the span is a `tracing` span carrying the same attributes as fields (`otel.name` and
//! `otel.kind` are set so a `tracing-opentelemetry` layer exports it under the same name and kind).
//! Remote parents and links need the `otel` feature; the `tracing` form records the parent
//! `traceparent` and the link count as fields instead.

use time::OffsetDateTime;

use super::spans;
#[cfg(feature = "otel")]
use super::traceparent::TraceParent;
use super::traceparent::plan_batch;
use crate::wire::{StreamId, StreamMsg};

/// The messaging system id (`messaging.system`).
pub(crate) const MESSAGING_SYSTEM: &str = "redis_streams";

/// Attribute: the messaging system.
pub(crate) const MESSAGING_SYSTEM_KEY: &str = "messaging.system";
/// Attribute: the topic.
pub(crate) const DESTINATION_KEY: &str = "messaging.destination";
/// Attribute: the partition index.
pub(crate) const PARTITION_KEY: &str = "messaging.partition";
/// Attribute: the message type.
pub(crate) const MESSAGE_TYPE_KEY: &str = "messaging.message.type";
/// Attribute: the Redis stream id.
pub(crate) const MESSAGE_ID_KEY: &str = "messaging.message_id";
/// Attribute: messages covered by a batch span.
pub(crate) const BATCH_COUNT_KEY: &str = "messaging.batch.message_count";
/// Attribute: the consumer name.
pub(crate) const CONSUMER_KEY: &str = "streams.consumer";
/// Attribute: milliseconds between acceptance and processing.
pub(crate) const LAG_MS_KEY: &str = "streams.lag_ms";

/// The three fields every consumer span is tagged with (C# `StreamSpanContext`).
#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamSpanContext<'a> {
    /// The topic the batch was read from.
    pub(crate) topic: &'a str,
    /// The partition the worker owns.
    pub(crate) partition: u32,
    /// The consumer name.
    pub(crate) consumer: &'a str,
}

/// A started span. Ends when dropped.
pub(crate) struct StreamSpan {
    inner: SpanInner,
}

#[cfg(feature = "otel")]
type SpanInner = opentelemetry::global::BoxedSpan;

#[cfg(not(feature = "otel"))]
type SpanInner = tracing::Span;

impl StreamSpan {
    /// Records the Redis id the write or read returned (`messaging.message_id`).
    pub(crate) fn set_message_id(&mut self, id: StreamId) {
        let text = id.to_string();
        #[cfg(feature = "otel")]
        {
            use opentelemetry::trace::Span as _;
            if self.inner.is_recording() {
                self.inner
                    .set_attribute(opentelemetry::KeyValue::new(MESSAGE_ID_KEY, text));
            }
        }
        #[cfg(not(feature = "otel"))]
        self.inner.record(MESSAGE_ID_KEY, text.as_str());
    }

    /// Marks the span as failed. The status is set even when the span is not recording attributes,
    /// because a tail sampler keys off it.
    pub(crate) fn set_error(&mut self, message: &str) {
        #[cfg(feature = "otel")]
        {
            use opentelemetry::trace::Span as _;
            self.inner
                .set_status(opentelemetry::trace::Status::error(message.to_owned()));
        }
        #[cfg(not(feature = "otel"))]
        {
            self.inner.record("otel.status_code", "ERROR");
            self.inner.record("otel.status_message", message);
        }
    }
}

#[cfg(feature = "otel")]
impl Drop for StreamSpan {
    fn drop(&mut self) {
        use opentelemetry::trace::Span as _;
        self.inner.end();
    }
}

/// Starts the producer span for one write (or one pipelined batch of `count` writes). `parent` is
/// the ambient context. The message id is unknown until the write returns; see
/// [`StreamSpan::set_message_id`].
pub(crate) fn start_publish(
    topic: &str,
    partition: u32,
    message_type: Option<&str>,
    count: u64,
) -> StreamSpan {
    #[cfg(feature = "otel")]
    {
        use opentelemetry::trace::SpanKind;
        let mut attrs = vec![
            opentelemetry::KeyValue::new(MESSAGING_SYSTEM_KEY, MESSAGING_SYSTEM),
            opentelemetry::KeyValue::new(DESTINATION_KEY, topic.to_owned()),
            opentelemetry::KeyValue::new(PARTITION_KEY, i64::from(partition)),
        ];
        if let Some(t) = message_type.filter(|t| !t.is_empty()) {
            attrs.push(opentelemetry::KeyValue::new(MESSAGE_TYPE_KEY, t.to_owned()));
        }
        if count > 1 {
            attrs.push(opentelemetry::KeyValue::new(
                BATCH_COUNT_KEY,
                i64::try_from(count).unwrap_or(i64::MAX),
            ));
        }
        let parent = opentelemetry::Context::current();
        StreamSpan {
            inner: otel_start(
                format!("{} {topic}", spans::PUBLISH),
                SpanKind::Producer,
                &parent,
                Vec::new(),
                attrs,
            ),
        }
    }
    #[cfg(not(feature = "otel"))]
    {
        let name = format!("{} {topic}", spans::PUBLISH);
        let span = tracing::info_span!(
            "streams.publish",
            "otel.name" = name.as_str(),
            "otel.kind" = "producer",
            "messaging.system" = MESSAGING_SYSTEM,
            "messaging.destination" = topic,
            "messaging.partition" = partition,
            "messaging.message.type" = tracing::field::Empty,
            "messaging.batch.message_count" = tracing::field::Empty,
            "messaging.message_id" = tracing::field::Empty,
            "otel.status_code" = tracing::field::Empty,
            "otel.status_message" = tracing::field::Empty,
        );
        if let Some(t) = message_type.filter(|t| !t.is_empty()) {
            span.record(MESSAGE_TYPE_KEY, t);
        }
        if count > 1 {
            span.record(BATCH_COUNT_KEY, count);
        }
        StreamSpan { inner: span }
    }
}

/// Starts the consumer span for a batch. The parent and links come from [`plan_batch`]; with no
/// context on any entry the span is a root with no links.
pub(crate) fn start_process(
    ctx: &StreamSpanContext<'_>,
    batch: &[StreamMsg],
    last: StreamId,
) -> StreamSpan {
    let plan = plan_batch(batch);
    let name = format!("{} {}", spans::PROCESS, ctx.topic);
    let batch_count = u64::try_from(batch.len()).unwrap_or(u64::MAX);

    let (message_type, lag_ms) = match batch.first() {
        Some(first) => (
            first.msg_type().to_owned(),
            batch.last().map(|m| lag_ms(m.enqueued_time())),
        ),
        None => (String::new(), None),
    };

    #[cfg(feature = "otel")]
    {
        use opentelemetry::trace::{Link, SpanKind};
        let parent = match plan.parent {
            Some(p) => remote_context(&p),
            None => opentelemetry::Context::new(),
        };
        let links: Vec<Link> = plan
            .links
            .iter()
            .map(|l| Link::with_context(remote_span_context(l)))
            .collect();
        let mut attrs = vec![
            opentelemetry::KeyValue::new(MESSAGING_SYSTEM_KEY, MESSAGING_SYSTEM),
            opentelemetry::KeyValue::new(DESTINATION_KEY, ctx.topic.to_owned()),
            opentelemetry::KeyValue::new(PARTITION_KEY, i64::from(ctx.partition)),
            opentelemetry::KeyValue::new(
                BATCH_COUNT_KEY,
                i64::try_from(batch_count).unwrap_or(i64::MAX),
            ),
            opentelemetry::KeyValue::new(CONSUMER_KEY, ctx.consumer.to_owned()),
        ];
        if !batch.is_empty() {
            attrs.push(opentelemetry::KeyValue::new(MESSAGE_TYPE_KEY, message_type));
            attrs.push(opentelemetry::KeyValue::new(
                MESSAGE_ID_KEY,
                last.to_string(),
            ));
            attrs.push(opentelemetry::KeyValue::new(
                LAG_MS_KEY,
                lag_ms.unwrap_or(0.0),
            ));
        }
        StreamSpan {
            inner: otel_start(name, SpanKind::Consumer, &parent, links, attrs),
        }
    }
    #[cfg(not(feature = "otel"))]
    {
        let parent_traceparent = plan.parent.map(|p| p.format());
        let span = tracing::info_span!(
            "streams.process",
            "otel.name" = name.as_str(),
            "otel.kind" = "consumer",
            "messaging.system" = MESSAGING_SYSTEM,
            "messaging.destination" = ctx.topic,
            "messaging.partition" = ctx.partition,
            "messaging.batch.message_count" = batch_count,
            "streams.consumer" = ctx.consumer,
            "messaging.message.type" = message_type.as_str(),
            "messaging.message_id" = last.to_string().as_str(),
            "streams.lag_ms" = lag_ms.unwrap_or(0.0),
            "streams.link_count" = u64::try_from(plan.links.len()).unwrap_or(u64::MAX),
            "streams.parent_traceparent" = parent_traceparent.as_deref().unwrap_or(""),
            "otel.status_code" = tracing::field::Empty,
            "otel.status_message" = tracing::field::Empty,
        );
        StreamSpan { inner: span }
    }
}

/// The ambient W3C `traceparent` to stamp on an outgoing entry, when there is an ambient span. With
/// the `otel` feature this is the current OpenTelemetry context; without it there is none.
pub(crate) fn current_traceparent() -> Option<String> {
    #[cfg(feature = "otel")]
    {
        use opentelemetry::trace::TraceContextExt as _;
        let cx = opentelemetry::Context::current();
        let sc = cx.span().span_context().clone();
        sc.is_valid().then(|| {
            TraceParent::new(
                sc.trace_id().to_bytes(),
                sc.span_id().to_bytes(),
                sc.trace_flags().to_u8(),
            )
            .format()
        })
    }
    #[cfg(not(feature = "otel"))]
    {
        None
    }
}

/// Milliseconds between an entry's enqueue time and now, floored at zero (clock skew between
/// Redis and this process is not negative lag).
pub(crate) fn lag_ms(enqueued: OffsetDateTime) -> f64 {
    let lag = (OffsetDateTime::now_utc() - enqueued).as_seconds_f64() * 1000.0;
    lag.max(0.0)
}

#[cfg(feature = "otel")]
fn otel_start(
    name: String,
    kind: opentelemetry::trace::SpanKind,
    parent: &opentelemetry::Context,
    links: Vec<opentelemetry::trace::Link>,
    attributes: Vec<opentelemetry::KeyValue>,
) -> opentelemetry::global::BoxedSpan {
    use opentelemetry::trace::{SpanBuilder, Tracer as _};
    let tracer = opentelemetry::global::tracer(super::METER_NAME);
    let builder = SpanBuilder::from_name(name)
        .with_kind(kind)
        .with_attributes(attributes)
        .with_links(links);
    tracer.build_with_context(builder, parent)
}

#[cfg(feature = "otel")]
fn remote_span_context(p: &TraceParent) -> opentelemetry::trace::SpanContext {
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
    SpanContext::new(
        TraceId::from_bytes(p.trace_id),
        SpanId::from_bytes(p.span_id),
        TraceFlags::new(p.flags),
        true,
        TraceState::NONE,
    )
}

#[cfg(feature = "otel")]
fn remote_context(p: &TraceParent) -> opentelemetry::Context {
    use opentelemetry::trace::TraceContextExt as _;
    opentelemetry::Context::new().with_remote_span_context(remote_span_context(p))
}
