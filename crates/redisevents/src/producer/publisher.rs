//! [`StreamPublisher`], the direct publisher. **Owner: B1** (port of `StreamPublisher.cs`; topic meta
//! via `admin::meta`). The struct and its trait impl are part of the frozen surface; the private
//! fields and the constructor's internals are B1's.

use bytes::Bytes;

use super::{PublishOptions, Publisher};
use crate::config::TopicOptions;
use crate::connection::Connection;
use crate::error::Result;
use crate::wire::{Keys, StreamId};

/// Publishes straight to Redis: one `XADD` per message (pipelined for a batch) on the shared
/// connection, routed by key. Ensures the topic meta (`m:{topic}`) on construction, refusing a
/// partition shrink.
#[derive(Debug)]
pub struct StreamPublisher {
    conn: Connection,
    keys: Keys,
    topic: String,
    options: TopicOptions,
}

impl StreamPublisher {
    /// A publisher for `topic`, ensuring its meta hash first.
    ///
    /// # Errors
    /// [`Error::Config`](crate::Error::Config) when the configured partition count is below the
    /// recorded one or the recorded codec version is unsupported; [`Error::Redis`](crate::Error::Redis)
    /// when Redis fails.
    pub async fn new(
        conn: Connection,
        keys: Keys,
        topic: impl Into<String>,
        options: TopicOptions,
    ) -> Result<Self> {
        let _ = (conn, keys, topic.into(), options);
        todo!("B1")
    }

    /// The topic this publisher writes to.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The topic options in effect.
    #[must_use]
    pub fn options(&self) -> &TopicOptions {
        &self.options
    }
}

impl Publisher for StreamPublisher {
    async fn publish(
        &self,
        _partition_key: &str,
        _body: Bytes,
        _msg_type: &str,
        _options: &PublishOptions,
    ) -> Result<StreamId> {
        let _ = (&self.conn, &self.keys);
        todo!("B1")
    }

    async fn publish_batch(
        &self,
        _partition_key: &str,
        _bodies: &[Bytes],
        _msg_type: &str,
        _options: &PublishOptions,
    ) -> Result<()> {
        todo!("B1")
    }
}
