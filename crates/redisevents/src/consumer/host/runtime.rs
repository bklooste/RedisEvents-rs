//! The running host: startup checks, one supervisor task per consumer
//! that owns its read side, and the host task that runs the orderly shutdown.
//!
//! A consumer's *read side* is one generation of: stored positions and
//! cold reset markers, the position flusher, a monitor per partition, and the workers (one per read
//! loop). It is rebuilt as a whole when the held partition set changes (lease rebalance), when a
//! contested partition recovers, and after a read loop fails with an error no back-off fixes, as
//! C# does. The ownership registry, the reader sockets and the trimmer outlive generations.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use redis::AsyncCommands as _;
use redis::streams::StreamInfoStreamReply;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use super::Registration;
use super::StreamsHost;
use super::erased::{FlusherDyn, ReadSource, RunnerFactory, StoreDyn, StoreHolder, WorkerRun};
use crate::admin::health;
use crate::admin::health::monitor::{self, PartitionMonitor, StreamLagSampler, Thresholds};
use crate::admin::meta::ensure_topic_meta;
use crate::config::instance::InstanceIdentity;
use crate::config::{
    ConsumerOptions, DeliveryMode, InstanceMode, PersistMode, ReadMode, StreamOptions,
    TopicOptions, WorkQueueOptions,
};
use crate::connection::Connection;
use crate::consumer::fetch::{Fetch, ReaderConnection, StreamStart};
use crate::consumer::shared::{BoxFuture, NoPositions, PartitionStop, PositionSink, ResetSignal};
use crate::consumer::trimmer::{BackgroundTrimmer, StartedTrimmer};
use crate::consumer::work_queue::{self, GroupSettings, WorkQueueFetch};
use crate::consumer::worker::{PartitionSlot, WorkerConfig, WorkerError, block_delay};
use crate::diagnostics::names;
use crate::error::{Error, Result};
use crate::ownership::registry::{OwnershipRegistry, OwnershipRegistryOptions};
use crate::positions::flusher::FlusherOptions;
use crate::positions::reset_marker;
use crate::positions::{RedisPositionStore, StartPosition};
use crate::wire::{Keys, StreamId};

/// How many times one consumer restarts its read side to recover contested stand-downs (C#).
const MAX_RECOVERIES: u32 = 5;
/// Attempts to rebuild the read side after a lease change before the host fails (C#: 3, 2 s apart).
const REBUILD_ATTEMPTS: u32 = 3;
const REBUILD_DELAY: Duration = Duration::from_secs(2);
/// How often the stall rule is checked. Its threshold is whole seconds.
const STALL_CHECK_INTERVAL: Duration = Duration::from_secs(1);

// ------------------------------------------------------------------------------------------------
// Faults and shared state
// ------------------------------------------------------------------------------------------------

/// Why the host failed (kept as text: `Error` is not `Clone`).
#[derive(Debug, Clone)]
pub(crate) struct Fault {
    config: bool,
    message: String,
}

impl Fault {
    fn from_error(context: &str, error: &Error) -> Self {
        Self {
            config: matches!(error, Error::Config(_)),
            message: format!("{context}: {error}"),
        }
    }

    fn from_worker(context: &str, error: &WorkerError) -> Self {
        match error {
            WorkerError::Fetch(e) => Self::from_error(context, e),
            WorkerError::Handler { .. } => Self {
                config: false,
                message: format!("{context}: {error}"),
            },
        }
    }

    pub(crate) fn to_error(&self) -> Error {
        if self.config {
            Error::config(self.message.clone())
        } else {
            Error::transport(self.message.clone())
        }
    }
}

type FaultSlot = Arc<Mutex<Option<Fault>>>;

/// Records the first fault and stops the host.
fn fail(slot: &FaultSlot, stop: &CancellationToken, fault: Fault) {
    tracing::error!(error = %fault.message, "Streams: the consumer host is stopping because of a fatal error.");
    let mut guard = slot.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.is_none() {
        *guard = Some(fault);
    }
    drop(guard);
    stop.cancel();
}

/// What the public handle can see of one consumer.
pub(crate) struct ConsumerState {
    topic: String,
    consumer: String,
    owned: Mutex<Vec<u32>>,
    started: AtomicBool,
    /// Monitors kept registered after a fault (`ErrorPolicy::Fail`) so health still reports the
    /// stopped partitions; forgotten by [`ConsumerState::release_monitors`].
    retained: Mutex<Vec<Arc<PartitionMonitor>>>,
}

impl ConsumerState {
    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) fn consumer(&self) -> &str {
        &self.consumer
    }

    pub(crate) fn owned(&self) -> Vec<u32> {
        self.owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Forgets the monitors a faulted consumer kept (C# forgets them in `StopAsync`).
    pub(crate) fn release_monitors(&self) {
        let kept =
            std::mem::take(&mut *self.retained.lock().unwrap_or_else(PoisonError::into_inner));
        for monitor in &kept {
            monitor::forget(monitor);
        }
    }

    pub(crate) fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    fn set_owned(&self, owned: &[u32]) {
        *self.owned.lock().unwrap_or_else(PoisonError::into_inner) = owned.to_vec();
    }
}

// ------------------------------------------------------------------------------------------------
// Startup
// ------------------------------------------------------------------------------------------------

/// Validates, starts every consumer (a failure tears the started ones down and is returned), the
/// trimmer and the lag sampler, then hands the lot to the host task.
pub(crate) async fn spawn(
    conn: Connection,
    keys: Keys,
    mut options: StreamOptions,
    registrations: Vec<Registration>,
) -> Result<StreamsHost> {
    // The registered consumers are part of the configuration C# validates.
    for registration in &registrations {
        if !options.consumers.iter().any(|c| c == &registration.options) {
            options.consumers.push(registration.options.clone());
        }
    }
    options.validate()?;
    for advisory in options.advisories() {
        advisory.log();
    }

    let stop = CancellationToken::new();
    let fault: FaultSlot = Arc::new(Mutex::new(None));
    let mut seen = BTreeSet::new();
    let mut started: Vec<(Arc<ConsumerState>, JoinHandle<()>)> = Vec::new();

    for registration in registrations {
        let result = start_consumer(
            &conn,
            &keys,
            &options,
            registration,
            &mut seen,
            &stop,
            &fault,
        )
        .await;
        match result {
            Ok(consumer) => started.push(consumer),
            Err(e) => {
                // Tear down what already runs; it must not stay half-up.
                stop.cancel();
                for (_, task) in started {
                    let _ = task.await;
                }
                return Err(e);
            }
        }
    }

    let trimmer = Arc::new(BackgroundTrimmer::new(conn.clone(), keys.clone(), &options)).start();
    let sampler = if started.is_empty() {
        None
    } else {
        StreamLagSampler::start(
            conn.clone(),
            health::DEFAULT_SAMPLE_INTERVAL,
            health::DEFAULT_PUSH_INTERVAL,
        )
        .ok()
    };

    let (done_tx, done_rx) = watch::channel(false);
    let states = started.iter().map(|(s, _)| s.clone()).collect();
    let tasks: Vec<JoinHandle<()>> = started.into_iter().map(|(_, t)| t).collect();
    let host_stop = stop.clone();
    tokio::spawn(host_task(host_stop, tasks, trimmer, sampler, done_tx));

    Ok(StreamsHost {
        conn,
        consumers: states,
        stop,
        fault,
        done: done_rx,
    })
}

/// Waits for the stop signal, then shuts down in order: every consumer (in parallel: workers →
/// flusher → registry), then the trimmer and the lag sampler.
async fn host_task(
    stop: CancellationToken,
    consumers: Vec<JoinHandle<()>>,
    trimmer: StartedTrimmer,
    sampler: Option<StreamLagSampler>,
    done: watch::Sender<bool>,
) {
    stop.cancelled().await;
    for task in consumers {
        if let Err(e) = task.await {
            tracing::error!(error = %e, "Streams: a consumer supervisor ended abnormally during shutdown.");
        }
    }
    trimmer.stop().await;
    if let Some(sampler) = sampler {
        sampler.stop().await;
    }
    let _ = done.send(true);
}

#[allow(clippy::too_many_lines)] // the startup sequence, step by step as C# StartCoreAsync
async fn start_consumer(
    conn: &Connection,
    keys: &Keys,
    options: &StreamOptions,
    registration: Registration,
    seen: &mut BTreeSet<(String, String)>,
    stop: &CancellationToken,
    fault: &FaultSlot,
) -> Result<(Arc<ConsumerState>, JoinHandle<()>)> {
    let Registration {
        options: consumer,
        store,
        instance_id,
        work_queue,
        runner,
    } = registration;
    let name = options.consumer_name(&consumer);
    let topic = consumer.topic.clone();
    if topic.trim().is_empty() {
        return Err(Error::config(format!(
            "Streams: consumer '{name}' has no topic. Set it in consumers or pass it to Streams::builder().topic()."
        )));
    }
    if !seen.insert((topic.clone(), name.clone())) {
        return Err(Error::config(format!(
            "Streams: consumer '{name}' is registered twice on topic '{topic}'. One host runs one handler per (topic, consumer)."
        )));
    }
    let group_mode = consumer.delivery == DeliveryMode::WorkQueue;

    let topic_options = options.topic(&topic);
    // Creates the topic when new, refuses a shrink and an unknown codec version.
    let meta = ensure_topic_meta(conn, keys, &topic, &topic_options).await?;
    let partitions = meta.partitions();
    let codec_version = meta.codec_version();

    let lease = consumer
        .instances
        .as_ref()
        .map_or(InstanceMode::Lease, |i| i.mode)
        == InstanceMode::Lease;
    let identity = InstanceIdentity::from_env(consumer.instances.as_ref())?;
    let pod_name = identity
        .pod_name
        .clone()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "unknown".to_owned());
    let assigned: Vec<u32> = if lease {
        Vec::new()
    } else {
        identity.owned(partitions).iter().collect()
    };

    // Work queue: Redis owns the cursor (the group) and the PEL, so the position store is bypassed
    // entirely and an XACK per handled batch takes its place (C#).
    let use_positions = !group_mode && consumer.persist != PersistMode::None;
    let instance_options = consumer.instances.clone().unwrap_or_default();
    let instance_id = instance_id.unwrap_or_else(crate::ownership::stable_instance_id);
    let store: Arc<dyn StoreDyn> = store.unwrap_or_else(|| {
        Arc::new(StoreHolder::new(RedisPositionStore::new(
            conn.clone(),
            keys.clone(),
            instance_id,
        )))
    });
    if use_positions
        && matches!(
            consumer.persist,
            PersistMode::SyncBatch | PersistMode::SyncMessage
        )
        && store.is_null()
    {
        return Err(Error::config(format!(
            "Streams: consumer '{name}' on topic '{topic}' has Persist={:?} over a position store that keeps nothing. \
             Synchronous persistence needs a real store; use Persist=None to not persist positions.",
            consumer.persist
        )));
    }

    tracing::info!(
        consumer = %name, topic = %topic, partitions,
        ownership = %if lease {
            format!("instances.mode = lease (pod {pod_name}): partitions are claimed from the ownership registry, not assigned")
        } else {
            identity.describe_ownership(partitions)
        },
        read_mode = ?consumer.read_mode, persist = ?consumer.persist, batch_size = consumer.batch_size,
        backpressure = consumer.backpressure.enabled, delivery = ?consumer.delivery,
        "Streams: starting consumer."
    );
    let work_queue = group_mode.then(|| {
        let member = work_queue::member_name(
            work_queue.member_name.as_deref(),
            identity.pod_name.as_deref(),
        );
        tracing::info!(
            consumer = %name, topic = %topic, group = %name, member = %member,
            claim_min_idle_ms = work_queue.claim_min_idle_ms,
            "Streams: delivery = work_queue: reading with XREADGROUP as a member of consumer group '{name}'. Per-key \
             order is not preserved in this mode."
        );
        (work_queue, member)
    });

    let state = Arc::new(ConsumerState {
        topic: topic.clone(),
        consumer: name.clone(),
        owned: Mutex::new(Vec::new()),
        started: AtomicBool::new(false),
        retained: Mutex::new(Vec::new()),
    });

    let registry = if !lease && assigned.is_empty() {
        tracing::warn!(
            consumer = %name, topic = %topic, index = identity.index, count = identity.count,
            "Streams: this consumer owns no partitions, so it starts no workers. That is a misconfiguration unless the \
             instance pool was deliberately over-provisioned."
        );
        None
    } else {
        let mut reg = OwnershipRegistryOptions::new(topic.clone(), name.clone(), partitions);
        reg.mode = if lease {
            InstanceMode::Lease
        } else {
            InstanceMode::Static
        };
        reg.owned_partitions.clone_from(&assigned);
        reg.pod_name.clone_from(&pod_name);
        reg.ttl_seconds = instance_options.lease_ttl_seconds;
        reg.renew_seconds = instance_options.lease_renew_seconds;
        reg.instance_id = instance_id;
        let registry = OwnershipRegistry::new(conn, keys, reg)?;
        // HEXPIRE probe (Error::Config when the server lacks it), one claim, the renew loop.
        registry.start().await?;
        Some(registry)
    };

    // Reader sockets: block mode only, one per ReaderThreads (>= 1), kept across generations.
    let readers = if consumer.read_mode == ReadMode::Block {
        let count = consumer.reader_threads.max(1);
        let service = crate::diagnostics::names::service_name();
        (0..count)
            .map(|index| {
                let client_name = names::reader_client_name(&service, &name, index, &pod_name);
                ReaderConnection::named(conn, consumer.block_ms, Some(client_name))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };
    if consumer.read_mode == ReadMode::Block && !consumer.backpressure.enabled {
        tracing::info!(
            consumer = %name, topic = %topic,
            "Streams: ReadMode=Block with Backpressure.Enabled=false runs the handler inline in the read loop; \
             enable backpressure to overlap reading and handling."
        );
    }

    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let mut supervisor = Supervisor {
        ctx: Arc::new(Ctx {
            conn: conn.clone(),
            keys: keys.clone(),
            options: consumer,
            topic_options,
            name,
            partitions,
            codec_version,
            lease,
            instance_id,
            runner,
            store,
            readers,
            work_queue,
            held: registry.as_ref().map(OwnershipRegistry::subscribe),
            events: events_tx,
        }),
        registry,
        state: state.clone(),
        generation: Generation::empty(0),
        events: events_rx,
        stop: stop.clone(),
        fault: fault.clone(),
        recoveries: 0,
        fetch_failures: 0,
        restart_at: None,
        stalled_out: Vec::new(),
        stall_logged: BTreeSet::new(),
    };

    let held = supervisor.held();
    supervisor.generation = supervisor
        .ctx
        .start_generation(&held, 1)
        .await
        .inspect_err(|_| {
            // The registry's claims must not outlive a failed start.
            if let Some(registry) = supervisor.registry.take() {
                tokio::spawn(async move { registry.stop().await });
            }
        })?;
    state.set_owned(&held);
    state.started.store(true, Ordering::Release);
    if held.is_empty() && lease {
        tracing::info!(
            consumer = %supervisor.ctx.name, topic = %supervisor.ctx.options.topic, partitions,
            "Streams: this consumer holds no lease yet: every partition is claimed by another instance. It starts no \
             workers and picks partitions up as leases lapse."
        );
    }
    let task = tokio::spawn(supervisor.run());
    Ok((state, task))
}

// ------------------------------------------------------------------------------------------------
// Per-consumer context and generations
// ------------------------------------------------------------------------------------------------

/// Events the flusher's callbacks send to the supervisor, tagged with their generation.
enum Event {
    Contested { generation: u64, partition: u32 },
    Recovered { generation: u64, partition: u32 },
}

/// Everything about one consumer that does not change between generations.
struct Ctx {
    conn: Connection,
    keys: Keys,
    options: ConsumerOptions,
    topic_options: TopicOptions,
    name: String,
    partitions: u32,
    codec_version: u32,
    lease: bool,
    instance_id: uuid::Uuid,
    runner: Arc<dyn RunnerFactory>,
    store: Arc<dyn StoreDyn>,
    readers: Vec<ReaderConnection>,
    /// `delivery = work_queue`: the options and this instance's member name.
    work_queue: Option<(WorkQueueOptions, String)>,
    /// The registry's held set (the flusher writes only these).
    held: Option<watch::Receiver<Arc<[u32]>>>,
    events: mpsc::UnboundedSender<Event>,
}

/// One generation of a consumer's read side.
struct Generation {
    number: u64,
    owned: Vec<u32>,
    stops: BTreeMap<u32, PartitionStop>,
    /// Each worker's task, tagged with the partitions it reads.
    workers: JoinSet<(Vec<u32>, std::result::Result<(), WorkerError>)>,
    flusher: Option<Arc<dyn FlusherDyn>>,
    resets: Option<Arc<ResetSignal>>,
    monitors: BTreeMap<u32, Arc<PartitionMonitor>>,
}

impl Generation {
    fn empty(number: u64) -> Self {
        Self {
            number,
            owned: Vec::new(),
            stops: BTreeMap::new(),
            workers: JoinSet::new(),
            flusher: None,
            resets: None,
            monitors: BTreeMap::new(),
        }
    }
}

/// The position sink the workers see: the flusher (or nothing), plus the partition monitors.
struct MonitoredSink {
    inner: Arc<dyn PositionSink>,
    monitors: BTreeMap<u32, Arc<PartitionMonitor>>,
}

impl PositionSink for MonitoredSink {
    fn record(&self, partition: u32, id: StreamId) {
        if let Some(monitor) = self.monitors.get(&partition) {
            monitor.observe(id);
        }
        self.inner.record(partition, id);
    }

    fn rewind(&self, partition: u32) {
        self.inner.rewind(partition);
    }

    fn flush(&self) -> BoxFuture<'_, Result<()>> {
        self.inner.flush()
    }

    fn stopped(&self, partition: u32, reason: &str) {
        if let Some(monitor) = self.monitors.get(&partition) {
            monitor.mark_stopped(reason, false);
        }
    }

    fn blocked(&self, partition: u32, blocked: bool) {
        let Some(monitor) = self.live(partition) else {
            return;
        };
        if blocked {
            monitor.mark_blocked();
        } else if monitor.is_blocked() {
            // Only while still blocked (C#): a partition already marked stopped stays stopped.
            monitor.mark_unblocked();
        }
    }

    fn read_failing(&self, partition: u32) {
        if let Some(monitor) = self.live(partition) {
            monitor.mark_transport_blocked();
        }
    }

    fn fetched(&self, partition: u32) {
        if let Some(monitor) = self.monitors.get(&partition) {
            monitor.mark_fetched();
        }
    }

    fn busy(&self, partition: u32, busy: bool) {
        if let Some(monitor) = self.monitors.get(&partition) {
            if busy {
                monitor.mark_busy();
            } else {
                monitor.mark_idle();
            }
        }
    }
}

impl MonitoredSink {
    /// The partition's monitor, unless the host stood it down for stalling: a late report from
    /// the read side it abandoned must not repaint that verdict.
    fn live(&self, partition: u32) -> Option<&Arc<PartitionMonitor>> {
        self.monitors
            .get(&partition)
            .filter(|m| !m.is_stalled_out())
    }
}

impl Ctx {
    fn topic(&self) -> &str {
        &self.options.topic
    }

    /// Drops every reader socket, so the next read side reads on fresh connections.
    fn recycle_readers(&self) {
        for reader in &self.readers {
            reader.recycle();
        }
    }

    fn budget(&self) -> Duration {
        let seconds = if self.options.shutdown_timeout_seconds > 0 {
            self.options.shutdown_timeout_seconds
        } else {
            10
        };
        Duration::from_secs(u64::from(seconds))
    }

    fn thresholds(&self) -> Thresholds {
        Thresholds {
            unhealthy_lag_ms: self.options.unhealthy_lag_ms,
            unhealthy_block_seconds: self.options.unhealthy_block_seconds,
            unhealthy_stopped_seconds: self.options.unhealthy_stopped_seconds,
            unhealthy_behind_seconds: self.options.unhealthy_behind_seconds,
            degraded_silence_seconds: self.options.degraded_silence_seconds,
            unhealthy_stalled_seconds: self.options.unhealthy_stalled_seconds,
        }
    }

    /// Builds and starts a read side over `owned`. On error nothing of
    /// it is left running.
    #[allow(clippy::too_many_lines)] // one sequence, as C#
    async fn start_generation(self: &Arc<Self>, owned: &[u32], number: u64) -> Result<Generation> {
        let mut generation = Generation::empty(number);
        generation.owned = owned.to_vec();
        if owned.is_empty() {
            return Ok(generation);
        }
        if let Some((wq, member)) = &self.work_queue {
            return self.start_group_generation(generation, wq, member).await;
        }
        let topic = self.topic().to_owned();
        let use_positions = self.options.persist != PersistMode::None;

        let (stored, markers) = if use_positions {
            let stored = self.store.load(&topic, &self.name).await?;
            // Taken before any read loop or flusher of this generation runs: nothing races it.
            let markers =
                reset_marker::take_all(&self.conn, &self.keys, &topic, &self.name).await?;
            (stored, markers)
        } else {
            (BTreeMap::new(), BTreeMap::new())
        };

        let co_locate = self.topic_options.co_locate_partitions;
        let mut starts: Vec<StreamStart> = Vec::with_capacity(owned.len());
        for &partition in owned {
            let key = self.keys.stream(&topic, partition, co_locate);
            let resolved = StartPosition::resolve(&self.options, stored.get(&partition).copied())?;
            let start = reset_marker::apply_to_start(resolved, Some(&markers), partition);
            let from = match start {
                StartPosition::After(id) => id,
                StartPosition::Now => resolve_now(&self.conn, &key).await?,
            };
            starts.push((partition, key, from));
        }

        if use_positions {
            let resets = Arc::new(ResetSignal::new(self.partitions));
            let flusher = self.flusher(number, resets.clone())?;
            flusher.start();
            generation.resets = Some(resets);
            generation.flusher = Some(flusher);
        }

        for (partition, key, from) in &starts {
            let monitor = monitor::track(
                &topic,
                &self.name,
                *partition,
                key,
                self.thresholds(),
                *from,
            )?;
            generation.monitors.insert(*partition, monitor);
        }

        let inner: Arc<dyn PositionSink> = match &generation.flusher {
            Some(f) => f.clone(),
            None => Arc::new(NoPositions),
        };
        let sink: Arc<dyn PositionSink> = Arc::new(MonitoredSink {
            inner,
            monitors: generation.monitors.clone(),
        });

        let token = CancellationToken::new();
        let groups: Vec<Vec<StreamStart>> = if co_locate && starts.len() > 1 {
            vec![starts]
        } else {
            starts.into_iter().map(|s| vec![s]).collect()
        };
        let batch_size = self.options.batch_size.max(1);
        let mut runs: Vec<(Vec<u32>, WorkerRun)> = Vec::with_capacity(groups.len());
        let built: Result<()> = (|| {
            for (i, group) in groups.into_iter().enumerate() {
                let partitions: Vec<u32> = group.iter().map(|s| s.0).collect();
                let fetch = match self.options.read_mode {
                    ReadMode::Block => Fetch::block(
                        self.readers[i % self.readers.len()].clone(),
                        group,
                        batch_size,
                        self.codec_version,
                    )?,
                    ReadMode::Poll => Fetch::poll(
                        &self.conn,
                        group,
                        batch_size,
                        self.options.max_idle_delay_ms,
                        self.codec_version,
                    )?,
                };
                let mut slots = Vec::with_capacity(partitions.len());
                for &p in &partitions {
                    let stop = PartitionStop::new(&token);
                    if let Some(flusher) = &generation.flusher {
                        flusher.attach(p, stop.clone());
                    }
                    generation.stops.insert(p, stop.clone());
                    slots.push(PartitionSlot::new(p, stop));
                }
                let cfg = WorkerConfig::from_options(&self.options, &self.name);
                let run = self.runner.worker(
                    cfg,
                    slots,
                    ReadSource::Stream(fetch),
                    sink.clone(),
                    generation.resets.clone(),
                )?;
                runs.push((partitions, run));
            }
            Ok(())
        })();
        if let Err(e) = built {
            drop(runs);
            self.stop_generation(generation, false).await;
            return Err(e);
        }
        for monitor in generation.monitors.values() {
            monitor.mark_running();
        }
        for (partitions, run) in runs {
            generation
                .workers
                .spawn(async move { (partitions, run.await) });
        }
        tracing::debug!(
            topic = %topic, consumer = %self.name, generation = number, ?owned,
            "Streams: read side started."
        );
        Ok(generation)
    }

    /// The work-queue read side: per partition, its group created (idempotently) at the resolved
    /// start; one worker per read loop over a [`WorkQueueFetch`], acknowledging through its
    /// [`GroupAcker`](work_queue::GroupAcker). No position store, no flusher, no reset markers.
    #[allow(clippy::too_many_lines)] // one sequence, as start_generation
    async fn start_group_generation(
        self: &Arc<Self>,
        mut generation: Generation,
        wq: &WorkQueueOptions,
        member: &str,
    ) -> Result<Generation> {
        let topic = self.topic().to_owned();
        let co_locate = self.topic_options.co_locate_partitions;
        // C# resolves the start with no stored position: the group's cursor is authoritative once
        // it exists, and this is only where it is created.
        let start = StartPosition::resolve(&self.options, None)?;
        let mut streams: Vec<(u32, String)> = Vec::with_capacity(generation.owned.len());
        for &partition in &generation.owned {
            let key = self.keys.stream(&topic, partition, co_locate);
            let from = match start {
                StartPosition::After(id) => id,
                StartPosition::Now => resolve_now(&self.conn, &key).await?,
            };
            // Members share a partition, so "this partition is behind / silent" says nothing about
            // this instance (C# zeroes both rules in group mode).
            let thresholds = Thresholds {
                unhealthy_behind_seconds: 0,
                degraded_silence_seconds: 0,
                ..self.thresholds()
            };
            let monitor = monitor::track(&topic, &self.name, partition, &key, thresholds, from)?;
            generation.monitors.insert(partition, monitor);
            streams.push((partition, key));
        }
        let groups: Vec<Vec<(u32, String)>> = if co_locate && streams.len() > 1 {
            vec![streams]
        } else {
            streams.into_iter().map(|s| vec![s]).collect()
        };
        let token = CancellationToken::new();
        let mut runs: Vec<(Vec<u32>, WorkerRun)> = Vec::with_capacity(groups.len());
        let mut built: Result<()> = Ok(());
        for (i, group) in groups.into_iter().enumerate() {
            let partitions: Vec<u32> = group.iter().map(|s| s.0).collect();
            let settings = GroupSettings::new(
                self.name.clone(),
                member.to_owned(),
                start,
                self.options.batch_size,
                self.codec_version,
                self.options.max_idle_delay_ms,
                wq,
            );
            let fetch = match self.options.read_mode {
                ReadMode::Block => WorkQueueFetch::block(
                    &self.conn,
                    self.readers[i % self.readers.len()].clone(),
                    group,
                    settings,
                ),
                ReadMode::Poll => WorkQueueFetch::poll(&self.conn, group, settings),
            };
            let mut fetch = match fetch {
                Ok(f) => f,
                Err(e) => {
                    built = Err(e);
                    break;
                }
            };
            if let Err(e) = fetch.ensure_groups().await {
                built = Err(e);
                break;
            }
            let sink: Arc<dyn PositionSink> = Arc::new(MonitoredSink {
                inner: fetch.acker(),
                monitors: generation
                    .monitors
                    .iter()
                    .filter(|(p, _)| partitions.contains(p))
                    .map(|(p, m)| (*p, m.clone()))
                    .collect(),
            });
            let mut slots = Vec::with_capacity(partitions.len());
            for &p in &partitions {
                let stop = PartitionStop::new(&token);
                generation.stops.insert(p, stop.clone());
                slots.push(PartitionSlot::new(p, stop));
            }
            let mut cfg = WorkerConfig::from_options(&self.options, &self.name);
            // XACK is this mode's position write, after every handled batch (C#: SyncBatch).
            cfg.persist = PersistMode::SyncBatch;
            match self
                .runner
                .worker(cfg, slots, ReadSource::Group(fetch), sink, None)
            {
                Ok(run) => runs.push((partitions, run)),
                Err(e) => {
                    built = Err(e);
                    break;
                }
            }
        }
        if let Err(e) = built {
            drop(runs);
            self.stop_generation(generation, false).await;
            return Err(e);
        }
        for monitor in generation.monitors.values() {
            monitor.mark_running();
        }
        for (partitions, run) in runs {
            generation
                .workers
                .spawn(async move { (partitions, run.await) });
        }
        tracing::debug!(
            topic = %topic, consumer = %self.name, member, generation = generation.number,
            owned = ?generation.owned, "Streams: work-queue read side started."
        );
        Ok(generation)
    }

    fn flusher(
        self: &Arc<Self>,
        number: u64,
        resets: Arc<ResetSignal>,
    ) -> Result<Arc<dyn FlusherDyn>> {
        let interval = Duration::from_millis(u64::from(self.options.persist_interval_ms.max(1)));
        let mut opts = FlusherOptions::new(
            self.topic(),
            self.name.clone(),
            self.partitions,
            interval,
            self.instance_id,
        );
        if self.store.is_redis() {
            opts.redis = Some((self.conn.clone(), self.keys.clone()));
        }
        // Only held partitions are written: a lease lost mid-interval is not written from here.
        opts.owned.clone_from(&self.held);
        opts.resets = Some(resets);
        opts.exclusive_claims = self.lease;
        opts.contested_grace = Duration::from_secs(u64::from(self.options.contested_grace_seconds));
        opts.recheck_interval = (self.options.contested_recheck_seconds > 0)
            .then(|| Duration::from_secs(u64::from(self.options.contested_recheck_seconds)));
        let events = self.events.clone();
        opts.on_contested = Some(Arc::new(move |partition, _mine, _theirs| {
            let _ = events.send(Event::Contested {
                generation: number,
                partition,
            });
        }));
        let events = self.events.clone();
        opts.on_recovered = Some(Arc::new(move |partition, _theirs| {
            let _ = events.send(Event::Recovered {
                generation: number,
                partition,
            });
        }));
        self.store.flusher(opts)
    }

    /// Stops a read side: graceful stop of every partition not already
    /// revoked, wait for the workers up to the budget (then drop them), final flush, monitors
    /// forgotten (their gauges cleared). `log` = an ordinary shutdown line on a timeout.
    async fn stop_generation(&self, mut generation: Generation, log: bool) {
        for stop in generation.stops.values() {
            stop.shutdown();
        }
        let budget = self.budget();
        let drained = tokio::time::timeout(budget, async {
            while generation.workers.join_next().await.is_some() {}
        })
        .await
        .is_ok();
        if !drained {
            if log {
                tracing::warn!(
                    topic = %self.topic(), consumer = %self.name, timeout_seconds = budget.as_secs(),
                    "Streams: the consumer did not drain within its shutdown budget. Positions recorded so far are still \
                     flushed; whatever was in flight is redelivered on the next start (at-least-once, not data loss)."
                );
            }
            generation.workers.abort_all();
            while generation.workers.join_next().await.is_some() {}
        }
        if let Some(flusher) = generation.flusher.take()
            && tokio::time::timeout(budget, flusher.dispose())
                .await
                .is_err()
        {
            tracing::warn!(
                topic = %self.topic(), consumer = %self.name,
                "Streams: the final position flush did not finish within the shutdown budget; the last interval's \
                 messages will be redelivered on the next start."
            );
        }
        for monitor in generation.monitors.values() {
            monitor::forget(monitor);
        }
    }
}

/// `StartFrom::Now` pinned to a concrete id from the server's tail (`XINFO STREAM`
/// `last-generated-id`, which survives trimming), or `0-0` when the stream does not exist yet.
async fn resolve_now(conn: &Connection, key: &str) -> Result<StreamId> {
    let mut shared = conn.shared();
    match shared.xinfo_stream::<_, StreamInfoStreamReply>(key).await {
        Ok(info) => StreamId::parse(&info.last_generated_id).map_err(|e| {
            Error::transport_with("XINFO STREAM returned a malformed last-generated-id", e)
        }),
        Err(e) if is_no_such_key(&e) => {
            tracing::debug!(
                key,
                "Streams: StartFrom=Now found no stream yet, so it starts from the beginning of an empty stream."
            );
            Ok(StreamId::MIN)
        }
        Err(e) => Err(e.into()),
    }
}

fn is_no_such_key(e: &redis::RedisError) -> bool {
    e.to_string().to_ascii_lowercase().contains("no such key")
}

// ------------------------------------------------------------------------------------------------
// Supervisor
// ------------------------------------------------------------------------------------------------

struct Supervisor {
    ctx: Arc<Ctx>,
    registry: Option<OwnershipRegistry>,
    state: Arc<ConsumerState>,
    generation: Generation,
    events: mpsc::UnboundedReceiver<Event>,
    stop: CancellationToken,
    fault: FaultSlot,
    recoveries: u32,
    fetch_failures: usize,
    /// A scheduled read-side restart (after a read failure).
    restart_at: Option<tokio::time::Instant>,
    /// Monitors of partitions stood down for stalling, kept registered after their read side is
    /// gone so health keeps reporting the stall. Dropped when the partition is read again, or at
    /// shutdown.
    stalled_out: Vec<Arc<PartitionMonitor>>,
    /// Partitions whose stall has been logged (Static mode logs once, not once a second).
    stall_logged: BTreeSet<u32>,
}

impl Supervisor {
    /// What the read side should cover: the held set, never a quarantined partition (even one
    /// the registry still lists because the cycle that gives it up has not landed yet).
    fn held(&self) -> Vec<u32> {
        let Some(registry) = self.registry.as_ref() else {
            return Vec::new();
        };
        let quarantined = registry.quarantined();
        registry
            .held()
            .iter()
            .copied()
            .filter(|p| !quarantined.contains(p))
            .collect()
    }

    async fn run(mut self) {
        let mut held_rx = self.registry.as_ref().map(OwnershipRegistry::subscribe);
        let stall_rule = self.ctx.options.unhealthy_stalled_seconds > 0;
        let mut stall_tick = tokio::time::interval(STALL_CHECK_INTERVAL);
        stall_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let restart_at = self.restart_at;
            tokio::select! {
                () = self.stop.cancelled() => break,
                changed = async {
                    match held_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_err() {
                        held_rx = None;
                        continue;
                    }
                    if let Some(rx) = held_rx.as_mut() {
                        rx.borrow_and_update();
                    }
                    let held = self.held();
                    if held != self.generation.owned {
                        self.rebalance(held).await;
                    }
                }
                Some(event) = self.events.recv() => self.on_event(event).await,
                Some(joined) = self.generation.workers.join_next(), if !self.generation.workers.is_empty() => {
                    self.on_worker_exit(joined).await;
                }
                _ = stall_tick.tick(), if stall_rule => {
                    self.check_stalls().await;
                    // A quarantine that ended without the held set changing (the cycle that gave
                    // the lease up never landed) still has to put the partition back.
                    let held = self.held();
                    if held != self.generation.owned {
                        self.rebalance(held).await;
                    }
                }
                () = async {
                    match restart_at {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.restart_at = None;
                    let owned = self.generation.owned.clone();
                    tracing::info!(topic = %self.ctx.topic(), consumer = %self.ctx.name, "Streams: restarting the read side after a read failure.");
                    self.rebuild(owned).await;
                }
            }
        }
        self.shutdown().await;
    }

    /// The held set changed: revoke what was lost, drain the rest, restart on the new set.
    async fn rebalance(&mut self, held: Vec<u32>) {
        tracing::info!(
            topic = %self.ctx.topic(), consumer = %self.ctx.name,
            before = ?self.generation.owned, after = ?held,
            "Streams: rebalancing: the held partitions changed."
        );
        let lost: Vec<u32> = self
            .generation
            .owned
            .iter()
            .copied()
            .filter(|p| !held.contains(p))
            .collect();
        for p in &lost {
            if let Some(stop) = self.generation.stops.get(p) {
                stop.revoke();
            }
            if let Some(resets) = &self.generation.resets {
                resets.forget(*p);
            }
            if let Some(flusher) = &self.generation.flusher {
                flusher.detach(*p);
            }
        }
        self.recoveries = 0;
        self.fetch_failures = 0;
        self.restart_at = None;
        self.rebuild(held).await;
    }

    /// Stops the current generation and starts one over `owned`, retrying (C#: 3 attempts, 2 s
    /// apart); if that fails the host fails, since it would hold leases it does not read.
    async fn rebuild(&mut self, owned: Vec<u32>) {
        let number = self.generation.number + 1;
        let old = std::mem::replace(&mut self.generation, Generation::empty(number));
        self.ctx.stop_generation(old, true).await;
        self.state.set_owned(&[]);
        for attempt in 1..=REBUILD_ATTEMPTS {
            if self.stop.is_cancelled() {
                return;
            }
            match self.ctx.start_generation(&owned, number).await {
                Ok(generation) => {
                    self.generation = generation;
                    self.state.set_owned(&owned);
                    // Read again: its fresh monitor replaced the stalled-out one under its key.
                    self.stalled_out.retain(|m| {
                        let again = owned.contains(&m.partition());
                        if again {
                            monitor::forget(m);
                        }
                        !again
                    });
                    return;
                }
                Err(e) if matches!(e, Error::Config(_)) || attempt == REBUILD_ATTEMPTS => {
                    fail(
                        &self.fault,
                        &self.stop,
                        Fault::from_error(
                            &format!(
                                "consumer '{}' on topic '{}' could not restart its read side on partitions {owned:?}",
                                self.ctx.name,
                                self.ctx.topic()
                            ),
                            &e,
                        ),
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        topic = %self.ctx.topic(), consumer = %self.ctx.name, attempt, error = %e,
                        "Streams: could not restart the read side; retrying."
                    );
                    tokio::select! {
                        () = tokio::time::sleep(REBUILD_DELAY) => {}
                        () = self.stop.cancelled() => return,
                    }
                }
            }
        }
    }

    async fn on_event(&mut self, event: Event) {
        match event {
            Event::Contested {
                generation,
                partition,
            } if generation == self.generation.number => {
                // The flusher has revoked the partition's stop already.
                tracing::warn!(
                    topic = %self.ctx.topic(), consumer = %self.ctx.name, partition,
                    "Streams: standing the partition down: another instance is writing its position. Check \
                     STREAMS_INSTANCE_COUNT against spec.replicas."
                );
                if let Some(resets) = &self.generation.resets {
                    resets.forget(partition);
                }
                if let Some(monitor) = self.generation.monitors.get(&partition) {
                    monitor.mark_stopped(
                        "contested position: another instance was also writing it, so this instance stood down",
                        true,
                    );
                }
            }
            Event::Recovered {
                generation,
                partition,
            } if generation == self.generation.number => {
                self.recoveries += 1;
                if self.recoveries > MAX_RECOVERIES {
                    tracing::error!(
                        topic = %self.ctx.topic(), consumer = %self.ctx.name, partition, max = MAX_RECOVERIES,
                        "Streams: the partition has been recovered from a contested stand-down too many times, so \
                         this instance stops trying; a restart is the fix."
                    );
                    return;
                }
                tracing::warn!(
                    topic = %self.ctx.topic(), consumer = %self.ctx.name, partition,
                    "Streams: restarting the read side: the instance this partition stood down for is gone."
                );
                let owned = self.generation.owned.clone();
                self.rebuild(owned).await;
            }
            _ => {} // from an older generation
        }
    }

    async fn on_worker_exit(
        &mut self,
        joined: std::result::Result<
            (Vec<u32>, std::result::Result<(), WorkerError>),
            tokio::task::JoinError,
        >,
    ) {
        let context = format!(
            "consumer '{}' on topic '{}'",
            self.ctx.name,
            self.ctx.topic()
        );
        match joined {
            Ok((partitions, Ok(()))) => {
                // StopPartition (or a contested revoke): those partitions stand down until restart.
                for p in &partitions {
                    let revoked = self
                        .generation
                        .stops
                        .get(p)
                        .is_some_and(PartitionStop::is_revoked);
                    if let Some(monitor) = self.generation.monitors.get(p)
                        && !revoked
                        && !monitor.is_stopped()
                    {
                        monitor.mark_stopped("stopped by ErrorPolicy.StopPartition", false);
                    }
                }
                tracing::info!(
                    topic = %self.ctx.topic(), consumer = %self.ctx.name, ?partitions,
                    "Streams: a read loop has stood down while the host keeps running; nothing reads those partitions \
                     until this consumer restarts. Their stored positions are unchanged, so the backlog replays then."
                );
            }
            Ok((partitions, Err(error))) => match &error {
                WorkerError::Handler { .. } | WorkerError::Fetch(Error::Config(_)) => {
                    fail(
                        &self.fault,
                        &self.stop,
                        Fault::from_worker(&context, &error),
                    );
                }
                WorkerError::Fetch(e) if self.hands_on() => {
                    // Lease mode (C# #52): a dead read loop reads nothing however long it is left,
                    // so it is not worth waiting out the stall window; hand its partitions on.
                    let reason = format!("its read loop failed: {e}");
                    self.stand_down(partitions, &reason).await;
                }
                WorkerError::Fetch(e) => {
                    let delay = block_delay(self.fetch_failures);
                    self.fetch_failures += 1;
                    tracing::error!(
                        topic = %self.ctx.topic(), consumer = %self.ctx.name, ?partitions, error = %e,
                        delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "Streams: a read loop failed with an error no retry fixes; the read side restarts from the \
                         stored positions after a back-off."
                    );
                    let at = tokio::time::Instant::now() + delay;
                    self.restart_at = Some(self.restart_at.map_or(at, |t| t.min(at)));
                }
            },
            Err(e) if e.is_cancelled() => {}
            Err(e) if self.hands_on() => {
                tracing::error!(
                    topic = %self.ctx.topic(), consumer = %self.ctx.name, error = %e,
                    "Streams: a read loop panicked; its partitions are handed to another instance."
                );
                let all = self.generation.owned.clone();
                self.stand_down(all, "its read loop panicked").await;
            }
            Err(e) => {
                // A panic outside the handler (the handler's own are caught by the worker).
                let delay = block_delay(self.fetch_failures);
                self.fetch_failures += 1;
                tracing::error!(
                    topic = %self.ctx.topic(), consumer = %self.ctx.name, error = %e,
                    "Streams: a read loop panicked; the read side restarts after a back-off."
                );
                let at = tokio::time::Instant::now() + delay;
                self.restart_at = Some(self.restart_at.map_or(at, |t| t.min(at)));
            }
        }
    }

    /// Whether a stuck partition is handed to another instance (Lease mode) rather than only
    /// reported (Static mode: no lease to give up; the orchestrator restarts the process).
    fn hands_on(&self) -> bool {
        self.ctx.lease && self.registry.is_some()
    }

    /// One pass of the stall rule (C# `CheckStallsAsync`): feeds each monitor how long position
    /// writes have been failing, finds the partitions past `unhealthy_stalled_seconds`, and in
    /// Lease mode stands them down. In Static mode it only logs, once per stall; the monitor's own
    /// clock grades the partition Unhealthy.
    async fn check_stalls(&mut self) {
        let threshold_ms = f64::from(self.ctx.options.unhealthy_stalled_seconds) * 1000.0;
        if threshold_ms <= 0.0 || self.stop.is_cancelled() {
            return;
        }
        let unpersisted = self
            .generation
            .flusher
            .as_ref()
            .map_or(Duration::ZERO, |f| f.failing_for());
        let mut stalled = Vec::new();
        let mut worst_ms = 0.0_f64;
        for (&partition, monitor) in &self.generation.monitors {
            monitor.set_persist_failing(unpersisted);
            let stalled_ms = monitor.stalled_ms();
            if stalled_ms < threshold_ms {
                self.stall_logged.remove(&partition);
                continue;
            }
            stalled.push(partition);
            worst_ms = worst_ms.max(stalled_ms);
        }
        if stalled.is_empty() {
            return;
        }
        let stalled_seconds = Duration::from_secs_f64(worst_ms / 1000.0).as_secs();
        let cause = if unpersisted.is_zero() {
            "no read returned and no batch completed".to_owned()
        } else {
            format!("position writes failing for {}s", unpersisted.as_secs())
        };
        if !self.hands_on() {
            let fresh: Vec<u32> = stalled
                .into_iter()
                .filter(|p| self.stall_logged.insert(*p))
                .collect();
            if !fresh.is_empty() {
                tracing::error!(
                    topic = %self.ctx.topic(), consumer = %self.ctx.name, partitions = ?fresh,
                    stalled_seconds = stalled_seconds, threshold_seconds = self.ctx.options.unhealthy_stalled_seconds,
                    "Streams: partitions have made no progress ({cause}) past unhealthy_stalled_seconds. Health reports \
                     Unhealthy; instances.mode = static has no lease to hand on, so a restart is what moves them."
                );
            }
            return;
        }
        let reason = format!("stalled: no progress for {stalled_seconds}s ({cause})");
        self.stand_down(stalled, &reason).await;
    }

    /// The Lease-mode stand-down (C# `StandDownStalled` + `ForceLeaseCycleAsync`): marks each
    /// partition stalled-out and keeps its monitor registered, revokes it, quarantines it in the
    /// registry (the next cycle releases it, claims none of it, and withdraws this instance's
    /// presence), recycles the reader sockets, runs that cycle now, and rebuilds the read side on
    /// what is left. A cycle that does not reach Redis only means the leases are not renewed and
    /// lapse within the TTL: another instance can claim them either way.
    async fn stand_down(&mut self, partitions: Vec<u32>, reason: &str) {
        let Some(registry) = self.registry.as_ref() else {
            return;
        };
        let instances = self.ctx.options.instances.clone().unwrap_or_default();
        // Long enough that a lone instance stays Unhealthy for a whole stall window (for a probe to
        // act on) before it tries the partitions again itself, on fresh connections.
        let quarantine = Duration::from_secs(u64::from(
            (2 * instances.lease_ttl_seconds).max(self.ctx.options.unhealthy_stalled_seconds),
        ));
        for p in &partitions {
            if let Some(monitor) = self.generation.monitors.remove(p) {
                monitor.mark_stalled_out(reason);
                self.stalled_out.push(monitor);
            }
            if let Some(stop) = self.generation.stops.get(p) {
                stop.revoke();
            }
            if let Some(resets) = &self.generation.resets {
                resets.forget(*p);
            }
            if let Some(flusher) = &self.generation.flusher {
                flusher.detach(*p);
            }
        }
        registry.quarantine(&partitions, quarantine);
        // Revoked: nothing reads them any more, even while the old read side drains.
        let reading: Vec<u32> = self
            .generation
            .owned
            .iter()
            .copied()
            .filter(|p| !partitions.contains(p))
            .collect();
        self.state.set_owned(&reading);
        self.ctx.recycle_readers();
        tracing::error!(
            topic = %self.ctx.topic(), consumer = %self.ctx.name, ?partitions, reason,
            renew_seconds = instances.lease_renew_seconds, quarantine_seconds = quarantine.as_secs(),
            "Streams: giving up the lease on stuck partitions. Another instance claims them within a renew interval; \
                     this instance will not take them back during the quarantine, and its health reports Unhealthy (rule \
                     stalled) until it reads them again or restarts. A restart rebuilds its Redis connections, which is what \
                     usually wedged."
        );
        if let Err(e) = registry.refresh().await {
            tracing::warn!(
                topic = %self.ctx.topic(), consumer = %self.ctx.name, error = %e,
                ttl_seconds = instances.lease_ttl_seconds,
                "Streams: could not run the lease cycle that gives up the stuck partitions; they are no longer renewed \
                 and lapse within the lease TTL."
            );
        }
        let held = self.held();
        self.recoveries = 0;
        self.fetch_failures = 0;
        self.restart_at = None;
        self.rebuild(held).await;
    }

    /// The C3 order: workers → flusher (final flush) → registry release.
    async fn shutdown(mut self) {
        tracing::info!(
            topic = %self.ctx.topic(), consumer = %self.ctx.name, partitions = ?self.generation.owned,
            budget_seconds = self.ctx.budget().as_secs(),
            "Streams: stopping consumer; draining."
        );
        // C# `IsRunning` is "StartAsync ran and StopAsync has not": a consumer that faulted the
        // host (`ErrorPolicy.Fail`) still counts as started until the host is stopped, so health
        // reports "all partition workers have stopped", not "no consumer has started".
        let faulted = self
            .fault
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some();
        if !faulted {
            self.state.started.store(false, Ordering::Release);
        }
        let mut generation = std::mem::replace(&mut self.generation, Generation::empty(0));
        if faulted {
            // Kept registered (stopped, not gone) until the host handle is shut down or dropped.
            let kept: Vec<_> = std::mem::take(&mut generation.monitors)
                .into_values()
                .collect();
            *self
                .state
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = kept;
        }
        self.ctx.stop_generation(generation, true).await;
        for monitor in self.stalled_out.drain(..) {
            monitor::forget(&monitor);
        }
        self.state.set_owned(&[]);
        if let Some(registry) = self.registry.take()
            && tokio::time::timeout(self.ctx.budget(), registry.dispose())
                .await
                .is_err()
        {
            tracing::warn!(
                topic = %self.ctx.topic(), consumer = %self.ctx.name,
                "Streams: releasing ownership did not finish within the shutdown budget; the claims lapse at their TTL."
            );
        }
        tracing::info!(topic = %self.ctx.topic(), consumer = %self.ctx.name, "Streams: stopped consumer.");
    }
}
