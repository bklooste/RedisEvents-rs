//! [`PartitionContext`]. **Owner: W0.2** (final).

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

/// What a handler knows about the partition it is serving. Cheap to clone.
///
/// The cancellation token fires when the partition is being taken away (shutdown, lost lease,
/// rebalance). A handler doing long work should watch it; the worker also drops the handler's
/// future once its shutdown budget runs out, so handlers must be cancel-safe at `.await` points
/// for anything they care about.
#[derive(Debug, Clone)]
pub struct PartitionContext {
    topic: Arc<str>,
    consumer: Arc<str>,
    partition: u32,
    cancellation: CancellationToken,
}

impl PartitionContext {
    /// A context for `topic`/`consumer`/`partition`. Public so test harnesses (D4) and other crates'
    /// tests can call handlers directly.
    pub fn new(
        topic: impl Into<Arc<str>>,
        consumer: impl Into<Arc<str>>,
        partition: u32,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            topic: topic.into(),
            consumer: consumer.into(),
            partition,
            cancellation,
        }
    }

    /// The topic name (without key prefix).
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The consumer name.
    #[must_use]
    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    /// The partition index.
    #[must_use]
    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// Fires when this partition's work should stop.
    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// `true` once [`PartitionContext::cancellation`] has fired.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
}
