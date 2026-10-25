//! [`BufferedStreamPublisher`]. **Owner: B2** (port of `BufferedStreamPublisher.cs`): bounded mpsc
//! queue, flush task on `max_wait_ms` / `max_batch`, backpressure or drop-oldest, drain on
//! [`BufferedStreamPublisher::shutdown`]. The struct, `new`, `shutdown` and the trait impls are
//! frozen; the private fields are B2's.

use bytes::Bytes;

use super::{BufferedPublisher, PublishOptions, Publisher, StreamPublisher};
use crate::config::ProducerOptions;
use crate::error::Result;
use crate::wire::StreamId;

/// A buffering publisher over a [`StreamPublisher`]. `publish`/`publish_batch` bypass the buffer
/// (after flushing what is queued, so order per key is kept).
#[derive(Debug)]
pub struct BufferedStreamPublisher {
    inner: std::sync::Arc<StreamPublisher>,
    options: ProducerOptions,
}

impl BufferedStreamPublisher {
    /// Starts the flush task on the current tokio runtime.
    #[must_use]
    pub fn new(inner: StreamPublisher, options: ProducerOptions) -> Self {
        let _ = (inner, options);
        todo!("B2")
    }

    /// Stops accepting, flushes everything queued, and stops the flush task.
    ///
    /// # Errors
    /// The error of the final flush.
    pub async fn shutdown(&self) -> Result<()> {
        let _ = (&self.inner, &self.options);
        todo!("B2")
    }
}

impl Publisher for BufferedStreamPublisher {
    async fn publish(
        &self,
        _partition_key: &str,
        _body: Bytes,
        _msg_type: &str,
        _options: &PublishOptions,
    ) -> Result<StreamId> {
        todo!("B2")
    }

    async fn publish_batch(
        &self,
        _partition_key: &str,
        _bodies: &[Bytes],
        _msg_type: &str,
        _options: &PublishOptions,
    ) -> Result<()> {
        todo!("B2")
    }
}

impl BufferedPublisher for BufferedStreamPublisher {
    async fn enqueue(
        &self,
        _partition_key: &str,
        _body: Bytes,
        _msg_type: &str,
        _options: &PublishOptions,
    ) -> Result<()> {
        todo!("B2")
    }

    async fn flush(&self) -> Result<()> {
        todo!("B2")
    }
}
