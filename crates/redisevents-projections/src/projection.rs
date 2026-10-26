//! The projection traits.

use std::future::Future;

use redisevents::{HandleResult, PartitionContext};

use crate::EventMeta;

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

/// The write side of a projection that buffers during a batch and writes once at the end.
///
/// The projector creates one [`Pending`](Self::Pending) (`Default`) per batch, hands `&mut` to it
/// with every event ([`BatchProjection::buffer`]), and passes it by value to `flush` after the last
/// event, only if this projection was handed at least one event. The consumer position does not
/// move until `flush` returns. The buffer is owned by the projector call, so there is nothing to
/// look up, lock or close.
///
/// Failure rules: if an event fails with an ordinary error, the events before it are still flushed
/// and the original error is returned (core skips the batch). If an event or a flush fails with
/// `Block` (or the host is stopping) nothing more is flushed and the whole batch is redelivered. If
/// one flush fails ordinarily the others still run and the first failure is returned.
pub trait BatchFlush: Send + Sync + 'static {
    /// What one batch buffers; shared by every event type this projection handles.
    type Pending: Default + Send + 'static;

    /// Writes what the batch buffered.
    fn flush(
        &self,
        pending: Self::Pending,
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}

/// Buffers one decoded event type `T` into the batch's [`Pending`](BatchFlush::Pending). A struct
/// may implement this for several `T`; bind it with
/// [`ProjectorBuilder::batch_projection`](crate::ProjectorBuilder::batch_projection).
pub trait BatchProjection<T>: BatchFlush {
    /// Buffers `event` (no I/O is expected; the write happens in [`BatchFlush::flush`]).
    fn buffer(
        &self,
        event: &T,
        pending: &mut Self::Pending,
        meta: &EventMeta,
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}
