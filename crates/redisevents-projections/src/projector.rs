//! [`Projector`]: the [`BatchHandler`] that decodes a batch and dispatches it to projections (C#
//! `EventProjector`).

use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use redisevents::{
    BatchHandler, Error, HandleError, HandleResult, PartitionContext, Result, StreamMsg,
};
use tracing::Instrument;

use crate::diagnostics;
use crate::{BatchProjection, EventMeta, EventTypeRegistry, Projection, ProjectionBatch};

type BoxFut<'a> = Pin<Box<dyn Future<Output = HandleResult> + Send + 'a>>;

/// A projection bound to one event type, type-erased.
trait Binding: Send + Sync {
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
        meta: &'a EventMeta,
        ctx: &'a PartitionContext,
    ) -> BoxFut<'a>;

    fn flusher(&self) -> Option<&Arc<dyn Flusher>>;
}

/// A batch projection's flush, type-erased. `id` is the projection's address, so one projection
/// bound for several event types is flushed once.
trait Flusher: Send + Sync {
    fn id(&self) -> usize;
    fn name(&self) -> &str;
    fn flush<'a>(&'a self, batch: &'a ProjectionBatch, ctx: &'a PartitionContext) -> BoxFut<'a>;
}

struct Bound<P, T> {
    projection: Arc<P>,
    flusher: Option<Arc<dyn Flusher>>,
    _event: PhantomData<fn() -> T>,
}

impl<P: Projection<T>, T: Send + Sync + 'static> Binding for Bound<P, T> {
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
        meta: &'a EventMeta,
        ctx: &'a PartitionContext,
    ) -> BoxFut<'a> {
        Box::pin(async move {
            let Some(event) = event.downcast_ref::<T>() else {
                return Err(HandleError::skip("decoded event is not the bound type"));
            };
            self.projection.handle(event, meta, ctx).await
        })
    }

    fn flusher(&self) -> Option<&Arc<dyn Flusher>> {
        self.flusher.as_ref()
    }
}

struct FlushOf<P>(Arc<P>);

impl<P: BatchProjection> Flusher for FlushOf<P> {
    fn id(&self) -> usize {
        Arc::as_ptr(&self.0).cast::<()>() as usize
    }

    fn name(&self) -> &str {
        let full = std::any::type_name::<P>();
        full.rsplit("::").next().unwrap_or(full)
    }

    fn flush<'a>(&'a self, batch: &'a ProjectionBatch, ctx: &'a PartitionContext) -> BoxFut<'a> {
        Box::pin(self.0.flush(batch, ctx))
    }
}

/// Builds a [`Projector`].
pub struct ProjectorBuilder {
    topic: String,
    registry: Arc<EventTypeRegistry>,
    bindings: HashMap<TypeId, Vec<Arc<dyn Binding>>>,
    unregistered: Vec<&'static str>,
}

impl ProjectorBuilder {
    /// Binds `projection` to events decoded as `T`. Several projections (or one projection for
    /// several `T`) may bind; for one event they run in binding order.
    #[must_use]
    pub fn projection<T, P>(self, projection: Arc<P>) -> Self
    where
        T: Send + Sync + 'static,
        P: Projection<T>,
    {
        self.bind::<T, P>(projection, None)
    }

    /// [`projection`](Self::projection) for a projection that is also a [`BatchProjection`]: after
    /// the batch's last event, `flush` runs once if it was handed any event. Bind it for each `T`
    /// it handles; it is still flushed once.
    #[must_use]
    pub fn batch_projection<T, P>(self, projection: Arc<P>) -> Self
    where
        T: Send + Sync + 'static,
        P: Projection<T> + BatchProjection,
    {
        let flusher: Arc<dyn Flusher> = Arc::new(FlushOf(Arc::clone(&projection)));
        self.bind::<T, P>(projection, Some(flusher))
    }

    fn bind<T, P>(mut self, projection: Arc<P>, flusher: Option<Arc<dyn Flusher>>) -> Self
    where
        T: Send + Sync + 'static,
        P: Projection<T>,
    {
        if !self.registry.decodes_to::<T>() {
            self.unregistered.push(std::any::type_name::<T>());
        }
        self.bindings
            .entry(TypeId::of::<T>())
            .or_default()
            .push(Arc::new(Bound {
                projection,
                flusher,
                _event: PhantomData::<fn() -> T>,
            }));
        self
    }

    /// Finishes the projector.
    ///
    /// # Errors
    /// [`Error::Config`] when a projection is bound to a type no registered wire type decodes to:
    /// it could never run.
    pub fn build(self) -> Result<Projector> {
        if !self.unregistered.is_empty() {
            return Err(Error::Config(format!(
                "projection bound to event type(s) not registered with the EventTypeRegistry: {}",
                self.unregistered.join(", ")
            )));
        }
        Ok(Projector {
            topic: self.topic,
            registry: self.registry,
            bindings: self.bindings,
            warned_unknown: Mutex::new(HashSet::new()),
        })
    }
}

/// Decodes each entry of a batch by wire type and runs the bound projections (C# `EventProjector`).
///
/// It is a [`BatchHandler`], so register it like any other: positions, replay and the
/// `Skip` / `Block` contract are core's.
///
/// * **Unknown wire type**: skipped; `warn` the first time the type is seen, `debug` after.
/// * **Known type, no bound projection**: skipped (`debug`).
/// * **Malformed entry** (the body does not decode): skipped **per entry**, logged with its id and
///   type and counted ([`MALFORMED_EVENTS`](crate::MALFORMED_EVENTS)); the rest of the batch is
///   still projected. Returning an error would make core skip the whole batch.
/// * **Projection error**: ends the batch and is returned unchanged, so later events of the batch
///   are not reached. Per-event projections have already written the events before it.
/// * **Batch projections** ([`BatchProjection`]): flushed once after the last event, in first-use
///   order. On an ordinary event error the flush still runs for the events before it and the
///   original error is returned; if that flush finds the store down (`Block`) the `Block` wins, so
///   the batch redelivers instead of skipping past unwritten rows. On a `Block` or a shutdown no
///   flush runs. If several flushes fail ordinarily, all run and the first error is returned.
///
/// One `projections.project <wire type>` span covers dispatch of one event to all its projections,
/// and one `projections.flush <projection>` span each flush.
pub struct Projector {
    topic: String,
    registry: Arc<EventTypeRegistry>,
    bindings: HashMap<TypeId, Vec<Arc<dyn Binding>>>,
    warned_unknown: Mutex<HashSet<String>>,
}

impl std::fmt::Debug for Projector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Projector")
            .field("topic", &self.topic)
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl Projector {
    /// Starts a projector for `topic` (used in logs, spans and the batch scope; it does not have
    /// to equal the consumer's topic, but should).
    #[must_use]
    pub fn builder(
        topic: impl Into<String>,
        registry: impl Into<Arc<EventTypeRegistry>>,
    ) -> ProjectorBuilder {
        ProjectorBuilder {
            topic: topic.into(),
            registry: registry.into(),
            bindings: HashMap::new(),
            unregistered: Vec::new(),
        }
    }

    fn first_time_unknown(&self, wire_type: &str) -> bool {
        self.warned_unknown
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(wire_type.to_owned())
    }

    async fn dispatch(
        &self,
        batch: &[StreamMsg],
        scope: &ProjectionBatch,
        to_flush: &mut Vec<Arc<dyn Flusher>>,
        ctx: &PartitionContext,
    ) -> HandleResult {
        for msg in batch {
            let decoded = match self.registry.try_decode(msg.msg_type(), msg.body()) {
                Ok(Some(decoded)) => decoded,
                Ok(None) => {
                    self.log_unknown(msg);
                    continue;
                }
                Err(error) => {
                    // A fault in this one entry, not in the batch: skip it and keep going.
                    diagnostics::count_malformed(&self.topic, msg.msg_type());
                    tracing::error!(
                        topic = %self.topic,
                        partition = msg.partition(),
                        id = %msg.id(),
                        partition_key = msg.partition_key(),
                        wire_type = msg.msg_type(),
                        %error,
                        "EventProjector: skipping malformed event of wire type '{}' on topic={} \
                         partition={} id={} partitionKey={}: its body failed to decode. The rest \
                         of the batch is still projected.",
                        msg.msg_type(),
                        self.topic,
                        msg.partition(),
                        msg.id(),
                        msg.partition_key(),
                    );
                    continue;
                }
            };

            let Some(bound) = self.bindings.get(&decoded.type_id()) else {
                tracing::debug!(
                    "EventProjector: skipping event of wire type '{}': known to the registry but \
                     no projection is bound for it.",
                    msg.msg_type()
                );
                continue;
            };

            let meta = EventMeta::from_msg(msg, scope);
            let span = diagnostics::project_span(msg.msg_type(), msg.partition_key(), msg.id());
            let outcome: HandleResult = async {
                for binding in bound {
                    // Recorded before the call, so a batch projection that buffered part of this
                    // event and then failed is still flushed on the skip path.
                    if let Some(flusher) = binding.flusher()
                        && !to_flush.iter().any(|f| f.id() == flusher.id())
                    {
                        to_flush.push(Arc::clone(flusher));
                    }
                    binding.call(decoded.as_any(), &meta, ctx).await?;
                }
                Ok(())
            }
            .instrument(span.clone())
            .await;
            if let Err(error) = &outcome {
                diagnostics::failed(&span, error.error());
            }
            outcome?;
        }
        Ok(())
    }

    fn log_unknown(&self, msg: &StreamMsg) {
        if self.first_time_unknown(msg.msg_type()) {
            tracing::warn!(
                "EventProjector: skipping events of unknown wire type '{}' on topic={}: no \
                 EventTypeRegistry registration for it. First seen at partition={} id={} \
                 partitionKey={}; further events of this type are skipped at debug.",
                msg.msg_type(),
                self.topic,
                msg.partition(),
                msg.id(),
                msg.partition_key(),
            );
        } else {
            tracing::debug!(
                "EventProjector: skipping event of unknown wire type '{}' on topic={} id={}",
                msg.msg_type(),
                self.topic,
                msg.id(),
            );
        }
    }

    async fn flush_all(
        &self,
        scope: &ProjectionBatch,
        to_flush: &[Arc<dyn Flusher>],
        ctx: &PartitionContext,
    ) -> HandleResult {
        let mut first: Option<HandleError> = None;
        for flusher in to_flush {
            let span = diagnostics::flush_span(flusher.name(), scope.topic(), scope.partition());
            let outcome = flusher.flush(scope, ctx).instrument(span.clone()).await;
            let Err(error) = outcome else { continue };
            diagnostics::failed(&span, error.error());
            if redelivers(&error, ctx) {
                return Err(error);
            }
            if first.is_none() {
                first = Some(error);
            } else {
                tracing::error!(
                    projection = flusher.name(),
                    topic = %self.topic,
                    partition = scope.partition(),
                    %error,
                    "EventProjector: a later flush also failed; the first flush failure in the \
                     batch is the one returned."
                );
            }
        }
        first.map_or(Ok(()), Err)
    }
}

/// A `Block`, or any error while the host is stopping, must redeliver the batch, not skip it.
fn redelivers(error: &HandleError, ctx: &PartitionContext) -> bool {
    error.is_block() || ctx.is_cancelled()
}

impl BatchHandler for Projector {
    async fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        let Some(first) = batch.first() else {
            return Ok(());
        };
        let scope = ProjectionBatch::new(self.topic.clone(), first.partition());
        let mut to_flush: Vec<Arc<dyn Flusher>> = Vec::new();

        let outcome = async {
            if let Err(error) = self.dispatch(batch, &scope, &mut to_flush, ctx).await {
                if to_flush.is_empty() || redelivers(&error, ctx) {
                    return Err(error);
                }
                // An ordinary error: core skips this batch and advances. Per-event projections
                // have written the events before the failing one; flush the batch projections so
                // they have too.
                if let Err(flush) = self.flush_all(&scope, &to_flush, ctx).await {
                    if redelivers(&flush, ctx) {
                        // The store behind the flush is down (or we are stopping): redeliver
                        // instead of skipping, or the held rows would be lost.
                        return Err(flush);
                    }
                    tracing::error!(
                        topic = %self.topic,
                        partition = scope.partition(),
                        %flush,
                        "EventProjector: flushing batch projections after an event failed also \
                         failed. The event's error is returned and the batch is skipped, so this \
                         batch's buffered work is lost."
                    );
                }
                return Err(error);
            }
            if to_flush.is_empty() {
                Ok(())
            } else {
                self.flush_all(&scope, &to_flush, ctx).await
            }
        }
        .await;

        scope.close();
        outcome
    }
}
