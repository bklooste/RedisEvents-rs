//! The producer side. **Traits and [`PublishOptions`]: W0.2** (final). Implementations, one file
//! each:
//!
//! | File | WP | Contents |
//! | --- | --- | --- |
//! | `publisher.rs` | B1 | [`StreamPublisher`]: pipelined `XADD`, routing, topic meta on first use |
//! | `buffered.rs` | B2 | [`BufferedStreamPublisher`]: bounded queue + flush task |
//! | `outbox.rs` | B3 | `Outbox`, state store, idempotency, topic forwarder |
//!
//! Cancellation: there is no token parameter; drop the future. Whether a dropped publish was
//! written is unknown (as with a cancelled C# call); each implementation documents its
//! cancel-safety.

pub mod buffered;
pub mod outbox;
pub mod publisher;

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use serde::Serialize;

pub use buffered::BufferedStreamPublisher;
pub use outbox::{
    ForwardedMessage, Idempotency, Outbox, OutboxPublish, StateEvent, StreamStore, TopicForwarder,
};
pub use publisher::StreamPublisher;

use crate::error::Result;
use crate::message::{Message, PayloadCodec};
use crate::wire::StreamId;

/// Per-call publish options (C# `PublishOptions`). `Default` is "none of these".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PublishOptions {
    /// Correlation id (field `c`); `None` or empty is omitted.
    pub correlation_id: Option<String>,
    /// Custom headers, packed into field `h` (at most 64, 8 KiB packed; exceeding either is an
    /// error, never a truncation).
    pub headers: Vec<(String, String)>,
    /// Write to this partition instead of routing by key. Must be `< partitions`.
    pub partition: Option<u32>,
    /// W3C `traceparent` (field `p`). `None` = take it from the current tracing span when the `otel`
    /// feature is on (B6), else omit.
    pub trace_parent: Option<String>,
}

impl PublishOptions {
    /// No options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the correlation id.
    #[must_use]
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Appends a header.
    #[must_use]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Pins the partition.
    #[must_use]
    pub fn partition(mut self, partition: u32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Sets the `traceparent` explicitly.
    #[must_use]
    pub fn trace_parent(mut self, trace_parent: impl Into<String>) -> Self {
        self.trace_parent = Some(trace_parent.into());
        self
    }
}

/// Publishes to one topic (C# `IStreamPublisher`). Implementations are bound to a topic at
/// construction.
pub trait Publisher: Send + Sync {
    /// Publishes one message and returns its id once Redis has it.
    ///
    /// An empty `partition_key` round-robins (no ordering promise).
    fn publish(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<StreamId>> + Send;

    /// Publishes several messages with one key and type, in order, in one round trip. All go to
    /// one partition (the key's, or `options.partition`).
    fn publish_batch(
        &self,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Publishes through a buffer (C# `IStreamBufferedPublisher`). Enqueued messages not yet flushed
/// are lost if the process dies: the documented trade-off for throughput.
pub trait BufferedPublisher: Publisher {
    /// Queues one message. Returns once queued (waiting for room, or dropping the oldest, per
    /// `ProducerOptions::drop_oldest`), not once written.
    fn enqueue(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Writes everything queued before this call.
    fn flush(&self) -> impl Future<Output = Result<()>> + Send;
}

impl<P: Publisher> Publisher for Arc<P> {
    fn publish(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<StreamId>> + Send {
        (**self).publish(partition_key, body, msg_type, options)
    }

    fn publish_batch(
        &self,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        (**self).publish_batch(partition_key, bodies, msg_type, options)
    }
}

impl<P: BufferedPublisher> BufferedPublisher for Arc<P> {
    fn enqueue(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        (**self).enqueue(partition_key, body, msg_type, options)
    }

    fn flush(&self) -> impl Future<Output = Result<()>> + Send {
        (**self).flush()
    }
}

/// Typed publishing over any [`Publisher`] (C# `TypedPublishExtensions`): encodes with codec `C`
/// and uses [`Message::TYPE`] as the type string.
///
/// ```no_run
/// # use redisevents::{Json, Message, PublishOptions, PublisherExt};
/// # #[derive(serde::Serialize)] struct OrderPlaced;
/// # impl Message for OrderPlaced { const TYPE: &'static str = "OrderPlaced"; }
/// # async fn f(p: &redisevents::producer::StreamPublisher) -> redisevents::Result<()> {
/// p.publish_message::<Json, _>("order-1", &OrderPlaced, &PublishOptions::new()).await?;
/// # Ok(()) }
/// ```
pub trait PublisherExt: Publisher {
    /// Encodes `message` with `C` and publishes it under `M::TYPE`.
    fn publish_message<C: PayloadCodec, M: Message + Serialize + Sync>(
        &self,
        partition_key: &str,
        message: &M,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<StreamId>> + Send {
        let body = C::encode(message);
        async move { self.publish(partition_key, body?, M::TYPE, options).await }
    }

    /// Encodes each of `messages` with `C` and publishes them as one batch under `M::TYPE`.
    fn publish_messages<C: PayloadCodec, M: Message + Serialize + Sync>(
        &self,
        partition_key: &str,
        messages: &[M],
        options: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let bodies: Result<Vec<Bytes>, _> = messages.iter().map(|m| C::encode(m)).collect();
        async move {
            self.publish_batch(partition_key, &bodies?, M::TYPE, options)
                .await
        }
    }
}

impl<P: Publisher + ?Sized> PublisherExt for P {}
