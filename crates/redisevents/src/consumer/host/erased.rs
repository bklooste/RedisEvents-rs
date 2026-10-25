//! Type erasure for the host: the handler traits and `PositionStore` are not object-safe (their
//! methods return `impl Future`), so the host keeps them behind small boxed-future traits.

use std::any::TypeId;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use crate::consumer::fetch::Fetch;
use crate::consumer::handler::BatchHandler;
use crate::consumer::shared::{BoxFuture, PartitionStop, PositionSink, ResetSignal};
use crate::consumer::worker::{PartitionSlot, Worker, WorkerConfig, WorkerError};
use crate::error::Result;
use crate::positions::flusher::{FlusherOptions, PositionFlusher};
use crate::positions::{NullPositionStore, PositionStore, RedisPositionStore};
use crate::wire::StreamId;

/// The run future of one worker.
pub(crate) type WorkerRun = BoxFuture<'static, std::result::Result<(), WorkerError>>;

/// Builds workers for a handler type the host no longer names.
pub(crate) trait RunnerFactory: Send + Sync + 'static {
    /// `Worker::new(..)` + `run()`, boxed.
    fn worker(
        &self,
        cfg: WorkerConfig,
        slots: Vec<PartitionSlot>,
        fetch: Fetch,
        sink: Arc<dyn PositionSink>,
        resets: Option<Arc<ResetSignal>>,
    ) -> Result<WorkerRun>;
}

/// The one [`RunnerFactory`]: holds the handler, resolved once (C# resolves it at construction).
pub(crate) struct HandlerRunner<H> {
    handler: Arc<H>,
}

impl<H: BatchHandler> HandlerRunner<H> {
    pub(crate) fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }
}

impl<H: BatchHandler> RunnerFactory for HandlerRunner<H> {
    fn worker(
        &self,
        cfg: WorkerConfig,
        slots: Vec<PartitionSlot>,
        fetch: Fetch,
        sink: Arc<dyn PositionSink>,
        resets: Option<Arc<ResetSignal>>,
    ) -> Result<WorkerRun> {
        let worker = Worker::new(cfg, slots, fetch, self.handler.clone(), sink, resets)?;
        Ok(Box::pin(worker.run()))
    }
}

/// A position flusher of any store type.
pub(crate) trait FlusherDyn: PositionSink {
    fn start(&self);
    fn dispose(&self) -> BoxFuture<'_, ()>;
    fn attach(&self, partition: u32, stop: PartitionStop);
    fn detach(&self, partition: u32);
}

impl<S: PositionStore> FlusherDyn for PositionFlusher<S> {
    fn start(&self) {
        PositionFlusher::start(self);
    }

    fn dispose(&self) -> BoxFuture<'_, ()> {
        Box::pin(PositionFlusher::dispose(self))
    }

    fn attach(&self, partition: u32, stop: PartitionStop) {
        PositionFlusher::attach(self, partition, stop);
    }

    fn detach(&self, partition: u32) {
        PositionFlusher::detach(self, partition);
    }
}

/// A position store of any type: loads, and builds a flusher over itself.
pub(crate) trait StoreDyn: Send + Sync + 'static {
    fn load<'a>(
        &'a self,
        topic: &'a str,
        consumer: &'a str,
    ) -> BoxFuture<'a, Result<BTreeMap<u32, StreamId>>>;

    fn flusher(&self, options: FlusherOptions) -> Result<Arc<dyn FlusherDyn>>;

    /// `NullPositionStore`: keeps nothing, so `Sync*` persistence is refused over it.
    fn is_null(&self) -> bool;

    /// `RedisPositionStore`: the flusher may use Redis directly (foreign-writer check, reset polling).
    fn is_redis(&self) -> bool;
}

/// Shares one store between generations of flushers.
pub(crate) struct StoreHolder<S> {
    store: Arc<S>,
}

impl<S: PositionStore> StoreHolder<S> {
    pub(crate) fn new(store: S) -> Self {
        Self {
            store: Arc::new(store),
        }
    }
}

/// `PositionStore` over an `Arc<S>`.
struct Shared<S>(Arc<S>);

impl<S: PositionStore> PositionStore for Shared<S> {
    fn load(
        &self,
        topic: &str,
        consumer: &str,
    ) -> impl Future<Output = Result<BTreeMap<u32, StreamId>>> + Send {
        self.0.load(topic, consumer)
    }

    fn save(
        &self,
        topic: &str,
        consumer: &str,
        positions: &[(u32, StreamId)],
    ) -> impl Future<Output = Result<()>> + Send {
        self.0.save(topic, consumer, positions)
    }

    fn reset(
        &self,
        topic: &str,
        consumer: &str,
        to: StreamId,
        partition: Option<u32>,
    ) -> impl Future<Output = Result<()>> + Send {
        self.0.reset(topic, consumer, to, partition)
    }
}

impl<S: PositionStore> StoreDyn for StoreHolder<S> {
    fn load<'a>(
        &'a self,
        topic: &'a str,
        consumer: &'a str,
    ) -> BoxFuture<'a, Result<BTreeMap<u32, StreamId>>> {
        Box::pin(self.store.load(topic, consumer))
    }

    fn flusher(&self, options: FlusherOptions) -> Result<Arc<dyn FlusherDyn>> {
        let flusher = PositionFlusher::new(Shared(self.store.clone()), options)?;
        Ok(Arc::new(flusher))
    }

    fn is_null(&self) -> bool {
        TypeId::of::<S>() == TypeId::of::<NullPositionStore>()
    }

    fn is_redis(&self) -> bool {
        TypeId::of::<S>() == TypeId::of::<RedisPositionStore>()
    }
}
