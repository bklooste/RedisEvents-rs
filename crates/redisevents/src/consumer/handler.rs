//! The handler traits and the adapters between them. **Traits: W0.2** (final). **Adapter bodies:
//! C2**.
//!
//! # Shape
//!
//! Handlers use native `async fn` in traits. The trait methods are declared as returning
//! `impl Future<Output = HandleResult> + Send`, so an implementation simply writes `async fn` and
//! the compiler checks its future is `Send`:
//!
//! ```
//! use redisevents::{HandleResult, MessageHandler, PartitionContext, StreamMsg};
//!
//! struct Print;
//!
//! impl MessageHandler for Print {
//!     async fn handle(&self, msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
//!         println!("{} {}", msg.id(), msg.msg_type());
//!         Ok(())
//!     }
//! }
//! ```
//!
//! # Bounds, and why
//!
//! - `Send + Sync + 'static` on the handler: one handler value serves **every partition** of a
//!   topic, concurrently, from tasks spawned on a multi-thread tokio runtime (C# registers one
//!   handler instance per topic too). Use interior mutability (`Mutex`, atomics) for state.
//! - `+ Send` on the returned future: the worker `tokio::spawn`s it.
//! - The message and context are borrowed for the duration of the call. Calls on one partition
//!   are strictly sequential; calls on different partitions overlap.
//!
//! The traits are not object-safe (`impl Future` returns); the host stores handlers generically
//! and erases them internally (C4).
//!
//! # Batch vs message
//!
//! A [`BatchHandler`] gets every message of one read, in order, all from one partition. An error
//! applies to the whole batch (skip it, or block on it). A [`MessageHandler`] is adapted to a
//! batch handler by [`PerMessage`], which calls it once per message in order: a `Skip` skips that
//! one message and continues; a `Block` stops at that message so the retry starts there.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::{HandleError, HandleResult, PartitionContext, worker};
use crate::config::ErrorPolicy;
use crate::message::{Json, PayloadCodec};
use crate::wire::{StreamMsg, TypedMsg};

/// Handles one message at a time (C# `IMessageHandler`).
pub trait MessageHandler: Send + Sync + 'static {
    /// Handles `msg`. See [`HandleError`](super::HandleError) for what an error does.
    fn handle(
        &self,
        msg: &StreamMsg,
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}

/// Handles a batch of messages from one partition (C# `IBatchHandler`).
pub trait BatchHandler: Send + Sync + 'static {
    /// Handles `batch` (non-empty, in stream order, one partition).
    fn handle_batch(
        &self,
        batch: &[StreamMsg],
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}

/// Handles one decoded message (C# `IMessageHandler<T>`). Adapt with [`Typed`].
pub trait TypedHandler<T>: Send + Sync + 'static {
    /// Handles `msg`.
    fn handle(
        &self,
        msg: &TypedMsg<T>,
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}

/// Handles a batch of decoded messages (C# `IBatchHandler<T>`). Adapt with [`Typed`].
pub trait TypedBatchHandler<T>: Send + Sync + 'static {
    /// Handles `batch`. Entries whose body failed to decode have been logged and dropped, so the
    /// batch may be shorter than the read (and is never empty).
    fn handle_batch(
        &self,
        batch: &[TypedMsg<T>],
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send;
}

impl<H: MessageHandler> MessageHandler for Arc<H> {
    fn handle(
        &self,
        msg: &StreamMsg,
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send {
        (**self).handle(msg, ctx)
    }
}

impl<H: BatchHandler> BatchHandler for Arc<H> {
    fn handle_batch(
        &self,
        batch: &[StreamMsg],
        ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send {
        (**self).handle_batch(batch, ctx)
    }
}

/// Runs a [`MessageHandler`] as a [`BatchHandler`]: once per message, in order. A `Skip` skips that
/// message (subject to `on_error`); a `Block` stops the batch there. C2 implements it, because
/// which messages count as done must agree with the worker's position tracking.
#[derive(Debug, Clone, Default)]
pub struct PerMessage<H>(pub H);

///
/// Under `ErrorPolicy::BestEffort` a `Skip` (or a panic) on one message is logged, counted in
/// `streams.errors` and skipped, and the batch carries on. Under `StopPartition` / `Fail`, and for a
/// `Block`, it returns that error at once, after telling the worker how many leading messages were
/// done: those are recorded, and a block retry resumes at the failing message. Called outside a
/// worker (a test calling it directly) it behaves as under `BestEffort`.
impl<H: MessageHandler> BatchHandler for PerMessage<H> {
    async fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        let policy = worker::scope_policy().unwrap_or_default();
        for (i, msg) in batch.iter().enumerate() {
            let outcome = worker::catch_unwind(self.0.handle(msg, ctx))
                .await
                .unwrap_or_else(|panic| Err(worker::panic_to_skip(panic.as_ref())));
            let Err(error) = outcome else {
                continue;
            };
            if error.is_block() || policy != ErrorPolicy::BestEffort || ctx.is_cancelled() {
                worker::scope_report_done(i);
                return Err(error);
            }
            worker::skip_message(ctx, msg, &error);
        }
        Ok(())
    }
}

/// Decodes bodies with codec `C` (default [`Json`]) and runs a [`TypedHandler`] or
/// [`TypedBatchHandler`] (C# `TypedHandlerAdapter`).
///
/// As a [`MessageHandler`]: a body that fails to decode is a `Skip`. As a [`BatchHandler`]: a body
/// that fails to decode is logged and dropped from the batch, and the rest still run.
pub struct Typed<H, T, C = Json> {
    inner: H,
    _marker: PhantomData<fn() -> (T, C)>,
}

impl<H, T, C> Typed<H, T, C> {
    /// Wraps `inner`.
    pub const fn new(inner: H) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// The wrapped handler.
    pub const fn inner(&self) -> &H {
        &self.inner
    }
}

impl<H: std::fmt::Debug, T, C> std::fmt::Debug for Typed<H, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Typed").field(&self.inner).finish()
    }
}

impl<H, T, C> MessageHandler for Typed<H, T, C>
where
    H: TypedHandler<T>,
    T: DeserializeOwned + Send + Sync + 'static,
    C: PayloadCodec,
{
    async fn handle(&self, msg: &StreamMsg, ctx: &PartitionContext) -> HandleResult {
        let value = C::decode::<T>(msg.body()).map_err(HandleError::skip)?;
        let typed = TypedMsg {
            value,
            envelope: msg.clone(),
        };
        self.inner.handle(&typed, ctx).await
    }
}

impl<H, T, C> BatchHandler for Typed<H, T, C>
where
    H: TypedBatchHandler<T>,
    T: DeserializeOwned + Send + Sync + 'static,
    C: PayloadCodec,
{
    async fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        let mut typed = Vec::with_capacity(batch.len());
        for msg in batch {
            match C::decode::<T>(msg.body()) {
                Ok(value) => typed.push(TypedMsg {
                    value,
                    envelope: msg.clone(),
                }),
                Err(e) => tracing::error!(
                    topic = ctx.topic(),
                    partition = ctx.partition(),
                    consumer = ctx.consumer(),
                    id = %msg.id(),
                    msg_type = msg.msg_type(),
                    codec = C::NAME,
                    target = std::any::type_name::<T>(),
                    error = %e,
                    "Streams: skipping malformed message: its body failed to decode. The rest of the batch is still handled."
                ),
            }
        }
        if typed.is_empty() {
            return Ok(());
        }
        self.inner.handle_batch(&typed, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::CancellationToken;

    /// Compile-time check of the contract C2/C4 rely on: a handler known only through its trait
    /// bound can be driven from a spawned task on a multi-thread runtime.
    #[allow(dead_code)]
    fn spawnable<H: BatchHandler, M: MessageHandler>(batch: Arc<H>, single: Arc<M>) {
        drop(tokio::spawn(async move {
            let ctx = PartitionContext::new("t", "c", 0, CancellationToken::new());
            let msgs: Vec<StreamMsg> = Vec::new();
            let _ = batch.handle_batch(&msgs, &ctx).await;
            if let Some(m) = msgs.first() {
                let _ = single.handle(m, &ctx).await;
            }
        }));
    }

    #[allow(dead_code)]
    fn adapters_are_handlers<M: MessageHandler, T: TypedHandler<u32>>(m: M, t: T) {
        spawnable(Arc::new(PerMessage(m)), Arc::new(Typed::<T, u32>::new(t)));
    }
}
