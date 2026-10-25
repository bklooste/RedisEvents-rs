//! The projection traits (C# `IProjection<T>`, `IBatchProjection`).

use std::future::Future;

use redisevents::{HandleResult, PartitionContext};

use crate::{EventMeta, ProjectionBatch};

/// Handles one decoded event type `T`. A struct may implement this for several `T`.
///
/// Return `Ok(())` when the event is applied, `Err(HandleError::skip(..))` for an ordinary failure
/// (the consumer's `on_error` policy applies) and `Err(HandleError::block(..))` when the batch must
/// be retried (the view store is down). Delivery is at least once, so `handle` must be idempotent;
/// compare `meta.id` with a position stored in the view (see the inventory sample).
pub trait Projection<T>: Send + Sync + 'static {
    /// Applies `event`.
    fn handle(
        &self,
        event: &T,
        meta: &EventMeta,
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}

/// A projection that buffers writes during a batch and writes them once at the end.
///
/// While handling events it buffers into [`EventMeta::batch`] (keyed by `self`); after the last
/// event of the batch the projector calls `flush` once, only if this projection was handed at least
/// one event. The consumer position does not move until `flush` returns.
///
/// Failure rules: if an event fails with an ordinary error, the events before it are still flushed
/// and the original error is returned (core skips the batch). If an event or a flush fails with
/// `Block` (or the host is stopping) nothing more is flushed and the whole batch is redelivered. If
/// one flush fails ordinarily the others still run and the first failure is returned.
///
/// Implement `BatchProjection` and `Projection<T>` on the same type and bind it with
/// [`ProjectorBuilder::batch_projection`](crate::ProjectorBuilder::batch_projection).
pub trait BatchProjection: Send + Sync + 'static {
    /// Writes what the batch buffered.
    fn flush(
        &self,
        batch: &ProjectionBatch,
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}
