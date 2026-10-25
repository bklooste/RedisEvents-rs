//! [`StreamMsg`], one decoded entry, and [`TypedMsg`]. **Owner: W0.2** (final).

use bytes::Bytes;
use time::OffsetDateTime;

use super::{HeaderBlock, StreamId};

/// One message as a handler sees it: the publisher's body and metadata, plus where it sits on the
/// wire.
///
/// The body is a [`Bytes`] sliced from the Redis reply, so it is not copied on the way to the
/// handler and cloning a `StreamMsg` is cheap for the body. A handler that keeps a message beyond
/// its call keeps that slice (and whatever it references) alive.
///
/// Fields are private so the representation of the string fields can change (for example to
/// `Bytes`-backed strings, F1) without breaking callers. Build one with [`StreamMsg::new`] and the
/// `with_*` methods; the entry codec (A1) and test doubles do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMsg {
    body: Bytes,
    msg_type: String,
    id: StreamId,
    partition: u32,
    partition_key: String,
    correlation_id: String,
    trace_parent: Option<String>,
    headers: HeaderBlock,
}

impl StreamMsg {
    /// A message with the given wire position, type and body; empty key, correlation id and
    /// headers, no trace parent.
    pub fn new(id: StreamId, partition: u32, msg_type: impl Into<String>, body: Bytes) -> Self {
        Self {
            body,
            msg_type: msg_type.into(),
            id,
            partition,
            partition_key: String::new(),
            correlation_id: String::new(),
            trace_parent: None,
            headers: HeaderBlock::empty(),
        }
    }

    /// Sets the partition key (field `k`).
    #[must_use]
    pub fn with_partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = key.into();
        self
    }

    /// Sets the correlation id (field `c`).
    #[must_use]
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = id.into();
        self
    }

    /// Sets the W3C `traceparent` (field `p`).
    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: Option<String>) -> Self {
        self.trace_parent = trace_parent;
        self
    }

    /// Sets the packed headers (field `h`).
    #[must_use]
    pub fn with_headers(mut self, headers: HeaderBlock) -> Self {
        self.headers = headers;
        self
    }

    /// The publisher's bytes, verbatim (field `b`).
    #[must_use]
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// The message type string (field `t`). Empty if the entry had none.
    #[must_use]
    pub fn msg_type(&self) -> &str {
        &self.msg_type
    }

    /// The entry id.
    #[must_use]
    pub fn id(&self) -> StreamId {
        self.id
    }

    /// The partition the entry was read from.
    #[must_use]
    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// The partition key (field `k`); empty when the publisher gave none or on a state entry.
    #[must_use]
    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    /// The correlation id (field `c`); empty when absent.
    #[must_use]
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }

    /// The W3C `traceparent` (field `p`), if any.
    #[must_use]
    pub fn trace_parent(&self) -> Option<&str> {
        self.trace_parent.as_deref()
    }

    /// The packed custom headers (field `h`).
    #[must_use]
    pub fn headers(&self) -> &HeaderBlock {
        &self.headers
    }

    /// When Redis accepted the entry: the millisecond part of [`StreamMsg::id`].
    #[must_use]
    pub fn enqueued_time(&self) -> OffsetDateTime {
        self.id.timestamp()
    }

    /// Consumes the message and returns its body.
    #[must_use]
    pub fn into_body(self) -> Bytes {
        self.body
    }
}

/// A message whose body has been decoded into a `T` (C# `StreamMsg<T>`).
#[derive(Debug, Clone, PartialEq)]
pub struct TypedMsg<T> {
    /// The decoded body.
    pub value: T,
    /// The raw message it was decoded from.
    pub envelope: StreamMsg,
}
