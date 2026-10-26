//! The consumer host: `Streams::builder(conn).topic("orders").handler(h).spawn()` returning a
//! [`StreamsHost`] handle with graceful shutdown. Wires workers, ownership, flusher and trimmer;
//! startup checks (topic meta, codec version, partition shrink). **Owner: C4** (port of
//! `StreamConsumerHost.cs`).
//!
//! # Shape
//!
//! ```no_run
//! # use redisevents::{Connection, StreamMsg, PartitionContext, HandleResult, BatchHandler};
//! # use redisevents::consumer::host::Streams;
//! struct Print;
//! impl BatchHandler for Print {
//!     async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
//!         for m in batch { println!("{} {}", m.id(), m.msg_type()); }
//!         Ok(())
//!     }
//! }
//! # async fn run() -> redisevents::Result<()> {
//! let conn = Connection::connect("redis://localhost:6379").await?;
//! let host = Streams::builder(conn).topic("orders").handler(Print).spawn().await?;
//! // ... later
//! host.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! # Extension points (C4b)
//!
//! [`ConsumerBuilder::handler`] takes any [`BatchHandler`]; every other registration form (message
//! handlers, typed handlers, closures) is an adapter onto it. C4b adds those as further inherent
//! `impl ConsumerBuilder` / `impl StreamsBuilder` blocks in `consumer/builder.rs`, ending in
//! `self.handler(adapter)`; nothing in this file needs to change for that.

mod erased;
mod runtime;

use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::admin::health::{self, StreamHealthReport};
use crate::config::{ConsumerOptions, StreamOptions, TopicOptions};
use crate::connection::Connection;
use crate::consumer::handler::BatchHandler;
use crate::error::Result;
use crate::positions::PositionStore;
use crate::wire::Keys;

use erased::{HandlerRunner, RunnerFactory, StoreDyn, StoreHolder};
use runtime::{ConsumerState, Fault};

/// Entry point of the consumer host.
#[derive(Debug, Clone, Copy)]
pub struct Streams;

impl Streams {
    /// A builder over `conn`. Keys come from the environment ([`Keys::from_env`]) unless
    /// [`StreamsBuilder::keys`] sets them; options default to [`StreamOptions::default`].
    #[must_use]
    pub fn builder(conn: Connection) -> StreamsBuilder {
        StreamsBuilder {
            conn,
            keys: None,
            options: StreamOptions::default(),
            environment: None,
            registrations: Vec::new(),
        }
    }
}

/// Collects the configuration and the consumer registrations; [`StreamsBuilder::spawn`] starts them.
pub struct StreamsBuilder {
    conn: Connection,
    keys: Option<Keys>,
    options: StreamOptions,
    environment: Option<String>,
    registrations: Vec<Registration>,
}

/// One consumer: its options and the type-erased handler and store.
pub(crate) struct Registration {
    pub(crate) options: ConsumerOptions,
    pub(crate) store: Option<Arc<dyn StoreDyn>>,
    pub(crate) instance_id: Option<uuid::Uuid>,
    pub(crate) runner: Arc<dyn RunnerFactory>,
}

impl StreamsBuilder {
    /// The key namespace (default [`Keys::from_env`]).
    #[must_use]
    pub fn keys(mut self, keys: Keys) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Replaces the whole configuration (the `Streams` section): topics, consumer defaults, the
    /// root consumer name. Call it before [`StreamsBuilder::topic`], which reads consumer entries
    /// from it.
    #[must_use]
    pub fn options(mut self, options: StreamOptions) -> Self {
        self.options = options;
        self
    }

    /// The configured options so far.
    #[must_use]
    pub fn stream_options(&self) -> &StreamOptions {
        &self.options
    }

    /// The host environment name used by validation (`Development` relaxes `Trim = None`).
    /// Default: `STREAMS_ENVIRONMENT`, else `local`.
    #[must_use]
    pub fn environment(mut self, name: impl Into<String>) -> Self {
        self.environment = Some(name.into());
        self
    }

    /// Sets (replaces) the options of one topic.
    #[must_use]
    pub fn topic_options(mut self, topic: impl Into<String>, options: TopicOptions) -> Self {
        self.options.topics.insert(topic.into(), options);
        self
    }

    /// Starts a consumer registration on `topic`. Its options start from the first
    /// `Streams:Consumers` entry for that topic, else the defaults.
    #[must_use]
    pub fn topic(self, topic: impl Into<String>) -> ConsumerBuilder {
        let topic = topic.into();
        let options = self
            .options
            .consumers
            .iter()
            .find(|c| c.topic == topic)
            .cloned()
            .unwrap_or_else(|| ConsumerOptions {
                topic: topic.clone(),
                ..ConsumerOptions::default()
            });
        ConsumerBuilder {
            parent: self,
            options,
            store: None,
            instance_id: None,
        }
    }

    /// Runs the startup checks and starts every registered consumer.
    ///
    /// # Errors
    /// [`Error::Config`](crate::Error::Config) for invalid options, a partition shrink, an unknown
    /// codec version, a server without `HEXPIRE`, or `Delivery = WorkQueue` (not supported yet);
    /// [`Error::Redis`](crate::Error::Redis) / [`Error::Transport`](crate::Error::Transport) when
    /// Redis fails during startup.
    pub async fn spawn(self) -> Result<StreamsHost> {
        let keys = self.keys.unwrap_or_else(Keys::from_env);
        let environment = self
            .environment
            .unwrap_or_else(runtime::environment_from_env);
        runtime::spawn(
            self.conn,
            keys,
            self.options,
            &environment,
            self.registrations,
        )
        .await
    }
}

/// One consumer registration in progress; [`ConsumerBuilder::handler`] completes it.
pub struct ConsumerBuilder {
    parent: StreamsBuilder,
    options: ConsumerOptions,
    store: Option<Arc<dyn StoreDyn>>,
    instance_id: Option<uuid::Uuid>,
}

impl ConsumerBuilder {
    /// The consumer name (owner of the position hash). Default: the options' `Consumer`, the root
    /// `Streams:Consumer`, then the executable name.
    #[must_use]
    pub fn consumer(mut self, name: impl Into<String>) -> Self {
        self.options.consumer = Some(name.into());
        self
    }

    /// Replaces the consumer options; the topic stays the one given to
    /// [`StreamsBuilder::topic`].
    #[must_use]
    pub fn options(mut self, options: ConsumerOptions) -> Self {
        let topic = std::mem::take(&mut self.options.topic);
        self.options = ConsumerOptions { topic, ..options };
        self
    }

    /// Edits the consumer options in place.
    #[must_use]
    pub fn configure(mut self, f: impl FnOnce(&mut ConsumerOptions)) -> Self {
        let topic = self.options.topic.clone();
        f(&mut self.options);
        self.options.topic = topic;
        self
    }

    /// The consumer options so far.
    #[must_use]
    pub fn consumer_options(&self) -> &ConsumerOptions {
        &self.options
    }

    /// Uses `store` instead of the Redis position store. Only used
    /// when `Persist` is not `None`.
    #[must_use]
    pub fn position_store<S: PositionStore>(mut self, store: S) -> Self {
        self.store = Some(Arc::new(StoreHolder::new(store)));
        self
    }

    /// The instance id this consumer claims partitions and writes positions under. Default:
    /// [`stable_instance_id`](crate::ownership::stable_instance_id) (from `POD_NAME`, else one per
    /// process). Set it to run two instances of one consumer in one process (tests).
    #[must_use]
    pub fn instance_id(mut self, id: uuid::Uuid) -> Self {
        self.instance_id = Some(id);
        self
    }

    /// Completes the registration with a batch handler. Every other handler shape is an adapter
    /// onto this ([`PerMessage`](crate::consumer::PerMessage), [`Typed`](crate::consumer::Typed),
    /// C4b's closures).
    #[must_use]
    pub fn handler<H: BatchHandler>(self, handler: H) -> StreamsBuilder {
        let Self {
            mut parent,
            options,
            store,
            instance_id,
        } = self;
        parent.registrations.push(Registration {
            options,
            store,
            instance_id,
            runner: Arc::new(HandlerRunner::new(handler)),
        });
        parent
    }
}

/// A running consumer host. [`StreamsHost::shutdown`] stops it gracefully: workers drain (within
/// `ShutdownTimeoutSeconds`), positions are flushed, ownership is released. Dropping it without
/// shutdown cancels everything at once (claims then lapse at their TTL).
pub struct StreamsHost {
    conn: Connection,
    consumers: Vec<Arc<ConsumerState>>,
    /// Cancelled to stop the host (shutdown, drop, or a consumer's fatal fault).
    stop: CancellationToken,
    /// The first fatal fault, if any.
    fault: Arc<Mutex<Option<Fault>>>,
    /// `true` once the host task finished its orderly shutdown.
    done: watch::Receiver<bool>,
}

impl std::fmt::Debug for StreamsHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsHost")
            .field("consumers", &self.consumers.len())
            .field("running", &self.is_running())
            .finish_non_exhaustive()
    }
}

impl StreamsHost {
    /// The connection the host runs on.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// How many consumers this host runs.
    #[must_use]
    pub fn consumers(&self) -> u32 {
        u32::try_from(self.consumers.len()).unwrap_or(u32::MAX)
    }

    /// How many consumers are up (started and not stopped or failed); for
    /// [`health::check`].
    #[must_use]
    pub fn consumers_started(&self) -> u32 {
        let up = self.consumers.iter().filter(|c| c.is_started()).count();
        u32::try_from(up).unwrap_or(u32::MAX)
    }

    /// Whether the host is running: not shut down and not failed.
    #[must_use]
    pub fn is_running(&self) -> bool {
        !self.stop.is_cancelled()
    }

    /// The partitions the consumer `(topic, consumer)` is reading right now, ascending (empty for
    /// an unknown pair).
    #[must_use]
    pub fn owned_partitions(&self, topic: &str, consumer: &str) -> Vec<u32> {
        self.consumers
            .iter()
            .find(|c| c.topic() == topic && c.consumer() == consumer)
            .map(|c| c.owned())
            .unwrap_or_default()
    }

    /// The readiness answer for this process (D2 [`health::check`] with this host's counts).
    pub async fn health(&self) -> StreamHealthReport {
        health::check(Some(&self.conn), self.consumers(), self.consumers_started()).await
    }

    /// Resolves when the host has stopped: `Ok` after [`StreamsHost::shutdown`], `Err` when a
    /// consumer failed it (`ErrorPolicy::Fail`, a configuration error found while running). On a
    /// failure the host has already shut itself down gracefully; the process should exit so the
    /// orchestrator restarts it.
    ///
    /// # Errors
    /// The failure that stopped the host: [`Error::Config`](crate::Error::Config) for a configuration error, otherwise
    /// [`Error::Transport`](crate::Error::Transport) carrying the handler's or the reader's message.
    pub async fn closed(&self) -> Result<()> {
        let mut done = self.done.clone();
        // A closed channel means the host task is gone, which is also "stopped".
        let _ = done.wait_for(|d| *d).await;
        self.outcome()
    }

    /// Graceful shutdown, in the C3 order: workers drain (within `ShutdownTimeoutSeconds`, then
    /// their futures are dropped) → final position flush → ownership release → trimmer and lag
    /// sampler stop.
    ///
    /// # Errors
    /// The failure that stopped the host, if a consumer had already failed it.
    pub async fn shutdown(self) -> Result<()> {
        self.stop.cancel();
        let outcome = self.closed().await;
        for consumer in &self.consumers {
            consumer.release_monitors();
        }
        outcome
    }

    fn outcome(&self) -> Result<()> {
        match &*self.fault.lock().unwrap_or_else(PoisonError::into_inner) {
            None => Ok(()),
            Some(fault) => Err(fault.to_error()),
        }
    }
}

impl Drop for StreamsHost {
    /// Starts the graceful shutdown in the background (if the runtime is still alive); it is not
    /// awaited. Call [`StreamsHost::shutdown`] to wait for it.
    fn drop(&mut self) {
        self.stop.cancel();
        for consumer in &self.consumers {
            consumer.release_monitors();
        }
    }
}
