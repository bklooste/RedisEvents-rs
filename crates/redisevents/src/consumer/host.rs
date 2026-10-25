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

use std::sync::Arc;

use crate::admin::health::StreamHealthReport;
use crate::config::{ConsumerOptions, StreamOptions, TopicOptions};
use crate::connection::Connection;
use crate::consumer::handler::BatchHandler;
use crate::error::Result;
use crate::positions::PositionStore;
use crate::wire::Keys;

/// Entry point of the consumer host (C# `AddStreams` + `AddStream`).
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

/// One consumer: its options and the type-erased handler + store (crate-private).
#[allow(dead_code)] // skeleton
pub(crate) struct Registration {
    pub(crate) options: ConsumerOptions,
    pub(crate) store: Option<StoreFactory>,
    pub(crate) runner: Arc<dyn RunnerFactory>,
}

/// Type-erased position store override (built per read-side generation).
pub(crate) type StoreFactory = Arc<dyn Fn() -> Arc<dyn std::any::Any + Send + Sync> + Send + Sync>;

/// Builds the run future of one worker for a handler type the host no longer names.
pub(crate) trait RunnerFactory: Send + Sync + 'static {}

impl StreamsBuilder {
    /// The key namespace (default [`Keys::from_env`]).
    #[must_use]
    pub fn keys(mut self, keys: Keys) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Replaces the whole configuration (C# `Streams` section): topics, consumer defaults, the
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
    /// Default: `ASPNETCORE_ENVIRONMENT`, `DOTNET_ENVIRONMENT`, `STREAMS_ENVIRONMENT`, `Production`.
    #[must_use]
    pub fn environment(mut self, name: impl Into<String>) -> Self {
        self.environment = Some(name.into());
        self
    }

    /// Sets (replaces) the options of one topic (C# `Streams:Topics:<topic>`).
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
        let _ = (
            self.conn,
            self.keys,
            self.options,
            self.environment,
            self.registrations,
        );
        todo!("C4")
    }
}

/// One consumer registration in progress; [`ConsumerBuilder::handler`] completes it.
pub struct ConsumerBuilder {
    parent: StreamsBuilder,
    options: ConsumerOptions,
    store: Option<StoreFactory>,
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

    /// Uses `store` instead of the Redis position store (C# `IPositionStore` override). Only used
    /// when `Persist` is not `None`.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // skeleton
    pub fn position_store<S: PositionStore>(self, store: S) -> Self {
        let _ = store;
        todo!("C4")
    }

    /// Completes the registration with a batch handler. Every other handler shape is an adapter
    /// onto this ([`PerMessage`](crate::consumer::PerMessage), [`Typed`](crate::consumer::Typed),
    /// C4b's closures).
    #[must_use]
    pub fn handler<H: BatchHandler>(self, handler: H) -> StreamsBuilder {
        let _ = (handler, self.parent, self.options, self.store);
        todo!("C4")
    }
}

/// A running consumer host. [`StreamsHost::shutdown`] stops it gracefully: workers drain (within
/// `ShutdownTimeoutSeconds`), positions are flushed, ownership is released. Dropping it without
/// shutdown cancels everything at once (claims then lapse at their TTL).
pub struct StreamsHost {
    _private: (),
}

impl StreamsHost {
    /// The connection the host runs on.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        todo!("C4")
    }

    /// How many consumers this host runs.
    #[must_use]
    pub fn consumers(&self) -> u32 {
        todo!("C4")
    }

    /// How many consumers are up (started and not failed); for [`StreamHealth::check`](crate::admin::health::StreamHealth::check).
    #[must_use]
    pub fn consumers_started(&self) -> u32 {
        todo!("C4")
    }

    /// Whether the host is running: not shut down and not failed.
    #[must_use]
    pub fn is_running(&self) -> bool {
        todo!("C4")
    }

    /// The partitions the consumer `(topic, consumer)` is reading right now, ascending.
    #[must_use]
    pub fn owned_partitions(&self, topic: &str, consumer: &str) -> Vec<u32> {
        let _ = (topic, consumer);
        todo!("C4")
    }

    /// The readiness answer for this process (D2 `StreamHealth::check` with this host's counts).
    pub async fn health(&self) -> StreamHealthReport {
        todo!("C4")
    }

    /// Resolves when the host has stopped: `Ok` after [`StreamsHost::shutdown`], `Err` when a
    /// consumer failed it (`ErrorPolicy::Fail`, a configuration error found while running). On a
    /// failure the host has already shut itself down gracefully; the process should exit so the
    /// orchestrator restarts it (C# `StopApplication`).
    ///
    /// # Errors
    /// The failure that stopped the host.
    pub async fn closed(&self) -> Result<()> {
        todo!("C4")
    }

    /// Graceful shutdown, in the C3 order: workers drain → final position flush → ownership
    /// release → trimmer stop.
    ///
    /// # Errors
    /// The failure that stopped the host, if a consumer had already failed it.
    pub async fn shutdown(self) -> Result<()> {
        todo!("C4")
    }
}
