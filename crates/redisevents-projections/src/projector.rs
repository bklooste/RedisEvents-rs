//! [`Projector`]: the [`BatchHandler`] that decodes a batch and dispatches it to projections.

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
use crate::{BatchFlush, BatchProjection, EventMeta, EventTypeRegistry, Projection};

type BoxFut<'a> = Pin<Box<dyn Future<Output = HandleResult> + Send + 'a>>;
type Pending = Box<dyn Any + Send>;

/// A projection bound to one event type, type-erased.
trait Binding: Send + Sync {
    /// `pending` is this binding's batch buffer when it is a batch projection.
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
        pending: Option<&'a mut Pending>,
        meta: &'a EventMeta,
        ctx: &'a PartitionContext,
    ) -> BoxFut<'a>;

    /// The index of this binding's flusher in the projector, for a batch projection.
    fn flusher(&self) -> Option<usize>;
}

/// A batch projection's flush, type-erased. One per distinct projection instance, so a projection
/// bound for several event types is flushed once.
trait Flusher: Send + Sync {
    fn name(&self) -> &str;
    fn new_pending(&self) -> Pending;
    fn flush<'a>(&'a self, pending: Pending, ctx: &'a PartitionContext) -> BoxFut<'a>;
}

fn wrong_type() -> HandleError {
    HandleError::skip("decoded event is not the bound type")
}

struct Plain<P, T> {
    projection: Arc<P>,
    _event: PhantomData<fn() -> T>,
}

impl<P: Projection<T>, T: Send + Sync + 'static> Binding for Plain<P, T> {
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
        _pending: Option<&'a mut Pending>,
        meta: &'a EventMeta,
        ctx: &'a PartitionContext,
    ) -> BoxFut<'a> {
        Box::pin(async move {
            let Some(event) = event.downcast_ref::<T>() else {
                return Err(wrong_type());
            };
            self.projection.handle(event, meta, ctx).await
        })
    }

    fn flusher(&self) -> Option<usize> {
        None
    }
}

struct Batched<P, T> {
    projection: Arc<P>,
    flusher: usize,
    _event: PhantomData<fn() -> T>,
}

impl<P: BatchProjection<T>, T: Send + Sync + 'static> Binding for Batched<P, T> {
    fn call<'a>(
        &'a self,
        event: &'a (dyn Any + Send + Sync),
        pending: Option<&'a mut Pending>,
        meta: &'a EventMeta,
        ctx: &'a PartitionContext,
    ) -> BoxFut<'a> {
        Box::pin(async move {
            let (Some(event), Some(pending)) = (
                event.downcast_ref::<T>(),
                pending.and_then(|p| p.downcast_mut::<P::Pending>()),
            ) else {
                return Err(wrong_type());
            };
            self.projection.buffer(event, pending, meta, ctx).await
        })
    }

    fn flusher(&self) -> Option<usize> {
        Some(self.flusher)
    }
}

struct FlushOf<P>(Arc<P>);

impl<P: BatchFlush> Flusher for FlushOf<P> {
    fn name(&self) -> &str {
        let full = std::any::type_name::<P>();
        full.rsplit("::").next().unwrap_or(full)
    }

    fn new_pending(&self) -> Pending {
        Box::new(P::Pending::default())
    }

    fn flush<'a>(&'a self, pending: Pending, ctx: &'a PartitionContext) -> BoxFut<'a> {
        Box::pin(async move {
            match pending.downcast::<P::Pending>() {
                Ok(pending) => self.0.flush(*pending, ctx).await,
                Err(_) => Err(HandleError::skip(
                    "batch buffer is not the projection's type",
                )),
            }
        })
    }
}

/// Builds a [`Projector`].
pub struct ProjectorBuilder {
    topic: String,
    registry: Arc<EventTypeRegistry>,
    bindings: HashMap<TypeId, Vec<Arc<dyn Binding>>>,
    flushers: Vec<Arc<dyn Flusher>>,
    /// Address of each bound batch projection, to give one instance one flusher.
    flusher_of: HashMap<usize, usize>,
    unregistered: Vec<&'static str>,
}

impl ProjectorBuilder {
    /// Binds `projection` to events decoded as `T`. Several projections may bind; for one event
    /// they run in binding order. To bind one instance for several `T`, use
    /// [`shared_projection`](Self::shared_projection).
    #[must_use]
    pub fn projection<T, P>(self, projection: P) -> Self
    where
        T: Send + Sync + 'static,
        P: Projection<T>,
    {
        self.shared_projection::<T, P>(Arc::new(projection))
    }

    /// [`projection`](Self::projection) for an instance you keep a handle to (to bind it for
    /// several event types, or to inspect it afterwards).
    #[must_use]
    pub fn shared_projection<T, P>(mut self, projection: Arc<P>) -> Self
    where
        T: Send + Sync + 'static,
        P: Projection<T>,
    {
        self.note::<T>();
        self.push::<T>(Arc::new(Plain {
            projection,
            _event: PhantomData::<fn() -> T>,
        }));
        self
    }

    /// Binds a [`BatchProjection`]: after the batch's last event, its [`BatchFlush::flush`] runs
    /// once if it was handed any event. To bind it for several `T`, use
    /// [`shared_batch_projection`](Self::shared_batch_projection).
    #[must_use]
    pub fn batch_projection<T, P>(self, projection: P) -> Self
    where
        T: Send + Sync + 'static,
        P: BatchProjection<T>,
    {
        self.shared_batch_projection::<T, P>(Arc::new(projection))
    }

    /// [`batch_projection`](Self::batch_projection) for a shared instance. Bind the same `Arc` for
    /// each `T` it handles; it is still flushed once.
    #[must_use]
    pub fn shared_batch_projection<T, P>(mut self, projection: Arc<P>) -> Self
    where
        T: Send + Sync + 'static,
        P: BatchProjection<T>,
    {
        self.note::<T>();
        let address = Arc::as_ptr(&projection).cast::<()>() as usize;
        let flusher = *self.flusher_of.entry(address).or_insert_with(|| {
            self.flushers
                .push(Arc::new(FlushOf(Arc::clone(&projection))));
            self.flushers.len() - 1
        });
        self.push::<T>(Arc::new(Batched {
            projection,
            flusher,
            _event: PhantomData::<fn() -> T>,
        }));
        self
    }

    fn note<T: 'static>(&mut self) {
        if !self.registry.decodes_to::<T>() {
            self.unregistered.push(std::any::type_name::<T>());
        }
    }

    fn push<T: 'static>(&mut self, binding: Arc<dyn Binding>) {
        self.bindings
            .entry(TypeId::of::<T>())
            .or_default()
            .push(binding);
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
            flushers: self.flushers,
            warned_unknown: Mutex::new(HashSet::new()),
        })
    }
}

/// Decodes each entry of a batch by wire type and runs the bound projections.
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
/// * **Batch projections** ([`BatchProjection`](crate::BatchProjection)): flushed once after the last event, in first-use
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
    flushers: Vec<Arc<dyn Flusher>>,
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
    /// Starts a projector for `topic` (used in logs and spans; it does not have
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
            flushers: Vec::new(),
            flusher_of: HashMap::new(),
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
        buffers: &mut Buffers,
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
                        "Projector: skipping malformed event of wire type '{}' on topic={} \
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
                    "Projector: skipping event of wire type '{}': known to the registry but \
                     no projection is bound for it.",
                    msg.msg_type()
                );
                continue;
            };

            let meta = EventMeta::from_msg(msg);
            let span = diagnostics::project_span(msg.msg_type(), msg.partition_key(), msg.id());
            let outcome: HandleResult = async {
                for binding in bound {
                    // Recorded before the call, so a batch projection that buffered part of this
                    // event and then failed is still flushed on the skip path.
                    let pending = binding
                        .flusher()
                        .map(|i| buffers.slot(i, &self.flushers[i]));
                    binding.call(decoded.as_any(), pending, &meta, ctx).await?;
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
                "Projector: skipping events of unknown wire type '{}' on topic={}: no \
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
                "Projector: skipping event of unknown wire type '{}' on topic={} id={}",
                msg.msg_type(),
                self.topic,
                msg.id(),
            );
        }
    }

    async fn flush_all(&self, buffers: &mut Buffers, ctx: &PartitionContext) -> HandleResult {
        let mut first: Option<HandleError> = None;
        for index in std::mem::take(&mut buffers.order) {
            let flusher = &self.flushers[index];
            let Some(pending) = buffers.slots[index].take() else {
                continue;
            };
            let span = diagnostics::flush_span(flusher.name(), &self.topic, ctx.partition());
            let outcome = flusher.flush(pending, ctx).instrument(span.clone()).await;
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
                    partition = ctx.partition(),
                    %error,
                    "Projector: a later flush also failed; the first flush failure in the \
                     batch is the one returned."
                );
            }
        }
        first.map_or(Ok(()), Err)
    }
}

/// The batch buffers of one `handle_batch` call: one slot per batch projection, in first-use order.
struct Buffers {
    slots: Vec<Option<Pending>>,
    order: Vec<usize>,
}

impl Buffers {
    fn new(flushers: usize) -> Self {
        Self {
            slots: (0..flushers).map(|_| None).collect(),
            order: Vec::new(),
        }
    }

    /// The buffer of flusher `index`, created on first use. Recorded before the projection runs,
    /// so one that buffered part of an event and then failed is still flushed on the skip path.
    fn slot(&mut self, index: usize, flusher: &Arc<dyn Flusher>) -> &mut Pending {
        if self.slots[index].is_none() {
            self.order.push(index);
        }
        self.slots[index].get_or_insert_with(|| flusher.new_pending())
    }
}

/// A `Block`, or any error while the host is stopping, must redeliver the batch, not skip it.
fn redelivers(error: &HandleError, ctx: &PartitionContext) -> bool {
    error.is_block() || ctx.is_cancelled()
}

impl BatchHandler for Projector {
    async fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        if batch.is_empty() {
            return Ok(());
        }
        let mut buffers = Buffers::new(self.flushers.len());

        if let Err(error) = self.dispatch(batch, &mut buffers, ctx).await {
            if buffers.order.is_empty() || redelivers(&error, ctx) {
                return Err(error);
            }
            // An ordinary error: core skips this batch and advances. Per-event projections
            // have written the events before the failing one; flush the batch projections so
            // they have too.
            if let Err(flush) = self.flush_all(&mut buffers, ctx).await {
                if redelivers(&flush, ctx) {
                    // The store behind the flush is down (or we are stopping): redeliver
                    // instead of skipping, or the held rows would be lost.
                    return Err(flush);
                }
                tracing::error!(
                    topic = %self.topic,
                    partition = ctx.partition(),
                    %flush,
                    "Projector: flushing batch projections after an event failed also \
                     failed. The event's error is returned and the batch is skipped, so this \
                     batch's buffered work is lost."
                );
            }
            return Err(error);
        }
        if buffers.order.is_empty() {
            Ok(())
        } else {
            self.flush_all(&mut buffers, ctx).await
        }
    }
}
