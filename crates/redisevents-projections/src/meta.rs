//! [`EventMeta`]: what the projector knows about an event besides its decoded value.

use redisevents::{StreamId, StreamMsg};

/// What the projector knows about an event besides its decoded value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EventMeta {
    /// The entry's partition key (the aggregate id for event-sourced topics).
    pub partition_key: String,
    /// The entry's stream position. Strictly increasing within a partition, so it orders
    /// redeliveries.
    pub id: StreamId,
    /// The entry's correlation id (empty when none).
    pub correlation_id: String,
    /// The partition the entry was read from.
    pub partition: u32,
}

impl EventMeta {
    /// A meta for partition 0.
    #[must_use]
    pub fn new(
        partition_key: impl Into<String>,
        id: StreamId,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            partition_key: partition_key.into(),
            id,
            correlation_id: correlation_id.into(),
            partition: 0,
        }
    }

    /// The meta for `msg`.
    #[must_use]
    pub fn from_msg(msg: &StreamMsg) -> Self {
        Self {
            partition_key: msg.partition_key().to_owned(),
            id: msg.id(),
            correlation_id: msg.correlation_id().to_owned(),
            partition: msg.partition(),
        }
    }
}
