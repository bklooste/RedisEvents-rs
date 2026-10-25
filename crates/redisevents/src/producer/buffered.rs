//! [`BufferedStreamPublisher`]. **Owner: B2** (port of `BufferedStreamPublisher.cs`): bounded
//! queue, flush task on `max_wait_ms` / `max_batch`, backpressure or drop-oldest, drain on
//! [`BufferedStreamPublisher::shutdown`]. The struct, `new`, `shutdown` and the trait impls are
//! frozen; the private fields are B2's.
//!
//! # Shape
//!
//! `caller → bounded queue → one flush task → one pipeline of XADDs (queue order) → settle flushes`.
//!
//! - **Trigger.** A flush fires at `max_batch` entries, or `max_wait_ms` after the *oldest* waiting
//!   entry was enqueued, whichever comes first, or at once when someone is waiting in
//!   [`flush`](BufferedPublisher::flush).
//! - **Backpressure.** The queue holds `max_queue` items (entries and pending flush markers). Full
//!   means [`enqueue`](BufferedPublisher::enqueue) waits. With `drop_oldest` the oldest item is shed
//!   instead, counted on `streams.buffer.dropped`; a shed flush marker fails its `flush()` rather
//!   than claiming the queue was written.
//! - **Order.** One flush task writes one batch at a time as one pipeline in queue order, so
//!   messages sharing a key land in enqueue order — including on the very first flush. (The C#
//!   publisher can reorder a cold first flush because each pipelined publish awaits topic-meta setup
//!   separately; here the meta is ensured once, in [`StreamPublisher::new`], before this publisher
//!   exists, and a flush is a single pipeline, so there is nothing to race.)
//! - **Errors.** No retry, as in C# (a silent retry would hide the outage and reorder the key). A
//!   failed entry is logged and lost; a `flush()` fails with the first error among the entries it
//!   covers in the batch it landed in, and succeeds if all of those landed.
//!
//! # Loss
//!
//! **An enqueued message that has not been flushed is lost if the process dies.** Not delayed, not
//! retried on restart: lost. That is the deliberate trade for throughput; anything that must not be
//! lost goes through [`Publisher::publish`] or the outbox. [`shutdown`](BufferedStreamPublisher::shutdown)
//! drains with a bounded wait (5 s) and logs a warning naming the count it abandoned. Dropping the
//! publisher without `shutdown` closes it and lets the flush task drain in the background for as
//! long as the runtime lives.

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::publisher::PipelinedEntry;
use super::{BufferedPublisher, PublishOptions, Publisher, StreamPublisher};
use crate::config::ProducerOptions;
use crate::diagnostics::activity::current_traceparent;
use crate::diagnostics::instruments::{
    clear_buffer_queued, record_buffer_dropped, set_buffer_queued,
};
use crate::error::{Error, Result};
use crate::wire::StreamId;

/// How long [`BufferedStreamPublisher::shutdown`] drains before abandoning the rest (C#
/// `DefaultShutdownFlushTimeout`).
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

const CLOSED: &str = "buffered publisher";

/// A buffering publisher over a [`StreamPublisher`]. `publish`/`publish_batch` bypass the buffer
/// (after flushing what is queued, so order per key is kept).
#[derive(Debug)]
pub struct BufferedStreamPublisher {
    inner: Arc<dyn Sink>,
    options: ProducerOptions,
    shared: Arc<Shared>,
    pump: Mutex<Option<JoinHandle<()>>>,
    shutdown_timeout: Duration,
}

impl BufferedStreamPublisher {
    /// Starts the flush task on the current tokio runtime.
    ///
    /// A `max_batch` or `max_queue` of 0 is treated as 1 (the C# constructor rejects it; this one
    /// cannot fail).
    ///
    /// # Panics
    /// Outside a tokio runtime.
    #[must_use]
    pub fn new(inner: StreamPublisher, options: ProducerOptions) -> Self {
        Self::with_sink(Arc::new(inner), options, SHUTDOWN_FLUSH_TIMEOUT)
    }

    fn with_sink(
        sink: Arc<dyn Sink>,
        options: ProducerOptions,
        shutdown_timeout: Duration,
    ) -> Self {
        if options.max_batch == 0 || options.max_queue == 0 {
            tracing::warn!(
                topic = sink.topic(),
                max_batch = options.max_batch,
                max_queue = options.max_queue,
                "Streams: a buffered producer's MaxBatch and MaxQueue must be at least 1; using 1."
            );
        }
        let shared = Arc::new(Shared {
            state: Mutex::new(State::default()),
            pump_wake: Notify::new(),
            space: Notify::new(),
            capacity: usize::try_from(options.max_queue.max(1)).unwrap_or(usize::MAX),
            max_batch: usize::try_from(options.max_batch.max(1)).unwrap_or(usize::MAX),
            max_wait: Duration::from_millis(u64::from(options.max_wait_ms)),
            drop_oldest: options.drop_oldest,
            topic: sink.topic().to_owned(),
            dropped: AtomicU64::new(0),
            published: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
        });
        let pump = tokio::spawn(run_pump(Arc::clone(&shared), Arc::clone(&sink)));
        Self {
            inner: sink,
            options,
            shared,
            pump: Mutex::new(Some(pump)),
            shutdown_timeout,
        }
    }

    /// Stops accepting, flushes everything queued, and stops the flush task.
    ///
    /// The drain is bounded (5 s): what is still queued then is abandoned and logged at warning
    /// with its count, and this returns `Ok` (as C# `StopAsync` does). Calling it again is a no-op.
    ///
    /// # Errors
    /// The error of the final flush.
    pub async fn shutdown(&self) -> Result<()> {
        let _ = &self.options;
        let marker = {
            let mut st = self.shared.lock();
            if st.closed {
                None
            } else {
                // The final marker goes in past the bound: shutdown must not wait for space.
                let (tx, rx) = oneshot::channel();
                st.queue.push_back(Item::Marker(tx));
                st.closed = true;
                Some(rx)
            }
        };
        self.shared.pump_wake.notify_one();
        self.shared.space.notify_waiters();
        let pump = self
            .pump
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();

        let deadline = Instant::now() + self.shutdown_timeout;
        let mut result = Ok(());
        if let Some(rx) = marker {
            match tokio::time::timeout_at(deadline, rx).await {
                Ok(Ok(outcome)) => result = outcome,
                Ok(Err(_)) => result = Err(Error::Closed(CLOSED)),
                Err(_) => {
                    self.abandon(pump.as_ref(), "the shutdown flush did not finish in time");
                    return Ok(());
                }
            }
        }
        if let Some(pump) = pump {
            let abort = pump.abort_handle();
            if tokio::time::timeout_at(deadline, pump).await.is_err() {
                abort.abort();
                self.abandon(None, "the flush task did not stop in time");
            }
        }
        result
    }

    /// Stops the flush task and empties the queue, logging what was lost.
    fn abandon(&self, pump: Option<&JoinHandle<()>>, why: &str) {
        if let Some(pump) = pump {
            pump.abort();
        }
        let abandoned = self.shared.drain_abandoned();
        self.shared
            .abandoned
            .fetch_add(abandoned, Ordering::Relaxed);
        clear_buffer_queued(&self.shared.topic);
        tracing::warn!(
            topic = %self.shared.topic,
            abandoned,
            "Streams: buffered publisher for topic '{}' abandoned at least {} queued entr(ies) because {}. Buffered entries that are not flushed are lost; use the direct publish path or the outbox for anything that matters.",
            self.shared.topic,
            abandoned,
            why
        );
    }

    fn check_open(&self) -> Result<()> {
        if self.shared.lock().closed {
            return Err(Error::Closed(CLOSED));
        }
        Ok(())
    }
}

impl Drop for BufferedStreamPublisher {
    /// Closes the queue; the flush task drains what is queued in the background and exits.
    fn drop(&mut self) {
        self.shared.lock().closed = true;
        self.shared.pump_wake.notify_one();
        self.shared.space.notify_waiters();
    }
}

impl Publisher for BufferedStreamPublisher {
    async fn publish(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<StreamId> {
        self.check_open()?;
        self.flush().await?;
        self.inner
            .publish(partition_key, body, msg_type, options)
            .await
    }

    async fn publish_batch(
        &self,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<()> {
        self.check_open()?;
        self.flush().await?;
        self.inner
            .publish_batch(partition_key, bodies, msg_type, options)
            .await
    }
}

impl BufferedPublisher for BufferedStreamPublisher {
    /// Validates and routes now (an explicit partition out of range or an empty type fails here),
    /// captures the caller's `traceparent`, and queues. `Bytes` is immutable, so no copy is needed
    /// for the caller to reuse its buffer. Cancel-safe: a dropped call either queued the entry or
    /// did not, never half.
    async fn enqueue(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<()> {
        if msg_type.is_empty() {
            return Err(Error::invalid_argument(
                "A message type is required; consumer-side filtering is built on it.",
            ));
        }
        let partition = self.inner.route(partition_key, options.partition)?;
        let mut options = options.clone();
        options.partition = Some(partition);
        if options.trace_parent.is_none() {
            options.trace_parent = current_traceparent();
        }
        let pending = Pending {
            partition_key: partition_key.to_owned(),
            body,
            msg_type: msg_type.to_owned(),
            options,
            stamp: Instant::now(),
        };
        self.shared.push(Item::Entry(pending)).await
    }

    /// Queues a marker and waits for the flush that reaches it. Fails with the first error among
    /// the entries it covers in its batch (see the module docs); after shutdown,
    /// [`Error::Closed`].
    async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.shared.push(Item::Marker(tx)).await?;
        rx.await.unwrap_or(Err(Error::Closed(CLOSED)))
    }
}

// ---------------------------------------------------------------------------------------------
// The queue
// ---------------------------------------------------------------------------------------------

/// One queued message, already routed (`options.partition` is set) and carrying the caller's
/// `traceparent`.
#[derive(Debug)]
struct Pending {
    partition_key: String,
    body: Bytes,
    msg_type: String,
    options: PublishOptions,
    /// When it was enqueued, for the oldest-anchored time trigger.
    stamp: Instant,
}

#[derive(Debug)]
enum Item {
    Entry(Pending),
    /// A `flush()` waiting for everything ahead of it.
    Marker(oneshot::Sender<Result<()>>),
}

#[derive(Debug, Default)]
struct State {
    queue: VecDeque<Item>,
    closed: bool,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    /// Wakes the flush task (single consumer: `notify_one` keeps a permit).
    pump_wake: Notify,
    /// Wakes enqueuers waiting for room.
    space: Notify,
    capacity: usize,
    max_batch: usize,
    max_wait: Duration,
    drop_oldest: bool,
    topic: String,
    dropped: AtomicU64,
    published: AtomicU64,
    abandoned: AtomicU64,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `item`, waiting for room (or shedding the oldest under `drop_oldest`).
    async fn push(&self, item: Item) -> Result<()> {
        let mut item = Some(item);
        loop {
            let mut notified = pin!(self.space.notified());
            notified.as_mut().enable();
            {
                let mut st = self.lock();
                if st.closed {
                    return Err(Error::Closed(CLOSED));
                }
                if st.queue.len() < self.capacity || self.drop_oldest {
                    while st.queue.len() >= self.capacity {
                        if let Some(shed) = st.queue.pop_front() {
                            self.on_dropped(shed);
                        }
                    }
                    if let Some(item) = item.take() {
                        st.queue.push_back(item);
                    }
                    drop(st);
                    self.pump_wake.notify_one();
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn on_dropped(&self, shed: Item) {
        match shed {
            Item::Marker(tx) => {
                // A shed marker covered nothing; completing it would claim the queue is in Redis.
                let _ = tx.send(Err(Error::transport(format!(
                    "Streams: the flush marker for topic '{}' was shed because the buffer was full and DropOldest is set, so the flush covered nothing and cannot report on the entries queued before it. DropOldest trades durability for latency; use the direct publish path or the outbox for anything that matters.",
                    self.topic
                ))));
            }
            Item::Entry(_) => {
                let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                record_buffer_dropped(&self.topic);
                if total.is_power_of_two() {
                    tracing::warn!(
                        topic = %self.topic,
                        dropped = total,
                        "Streams: buffered publisher for topic '{}' dropped an entry because its queue was full (DropOldest is on); {} dropped so far.",
                        self.topic,
                        total
                    );
                }
            }
        }
    }

    /// Moves up to `max_batch` entries (and the markers among them) into hand. Returns whether the
    /// queue is closed.
    fn take(&self, batch: &mut Vec<Pending>, waiters: &mut Vec<Waiter>) -> bool {
        let mut st = self.lock();
        let mut took = false;
        while batch.len() < self.max_batch {
            match st.queue.pop_front() {
                Some(Item::Entry(entry)) => batch.push(entry),
                Some(Item::Marker(tx)) => waiters.push(Waiter {
                    tx,
                    covers: batch.len(),
                }),
                None => break,
            }
            took = true;
        }
        let closed = st.closed;
        drop(st);
        if took {
            self.space.notify_waiters();
        }
        closed
    }

    /// Empties the queue: markers complete (shutdown loss is reported once, by the caller), entries
    /// are counted.
    fn drain_abandoned(&self) -> u64 {
        let items: Vec<Item> = self.lock().queue.drain(..).collect();
        let mut abandoned = 0;
        for item in items {
            match item {
                Item::Marker(tx) => {
                    let _ = tx.send(Ok(()));
                }
                Item::Entry(_) => abandoned += 1,
            }
        }
        abandoned
    }

    fn depth(&self) -> i64 {
        i64::try_from(self.lock().queue.len()).unwrap_or(i64::MAX)
    }
}

/// A marker in hand: it covers `batch[..covers]`.
struct Waiter {
    tx: oneshot::Sender<Result<()>>,
    covers: usize,
}

// ---------------------------------------------------------------------------------------------
// The flush task
// ---------------------------------------------------------------------------------------------

async fn run_pump(shared: Arc<Shared>, sink: Arc<dyn Sink>) {
    let mut batch: Vec<Pending> = Vec::new();
    let mut waiters: Vec<Waiter> = Vec::new();
    loop {
        let mut exit = false;
        loop {
            let closed = shared.take(&mut batch, &mut waiters);
            if !waiters.is_empty() || batch.len() >= shared.max_batch {
                break;
            }
            let Some(oldest) = batch.first() else {
                if closed {
                    exit = true;
                    break;
                }
                shared.pump_wake.notified().await;
                continue;
            };
            if closed {
                break;
            }
            // Time trigger, from the oldest entry in hand, not from now.
            let deadline = oldest.stamp + shared.max_wait;
            if Instant::now() >= deadline {
                break;
            }
            tokio::select! {
                () = shared.pump_wake.notified() => {}
                () = tokio::time::sleep_until(deadline) => break,
            }
        }
        if exit {
            break;
        }
        flush_batch(&shared, sink.as_ref(), &mut batch, &mut waiters).await;
        set_buffer_queued(&shared.topic, shared.depth());
    }
    clear_buffer_queued(&shared.topic);
}

async fn flush_batch(
    shared: &Shared,
    sink: &dyn Sink,
    batch: &mut Vec<Pending>,
    waiters: &mut Vec<Waiter>,
) {
    let outcomes = if batch.is_empty() {
        Vec::new()
    } else {
        sink.write(batch).await
    };
    let failed = outcomes.iter().filter(|o| o.is_err()).count();
    let written = outcomes.len() - failed;
    shared.published.fetch_add(
        u64::try_from(written).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    if let Some(Err(first)) = outcomes.iter().find(|o| o.is_err()) {
        tracing::error!(
            topic = %shared.topic,
            failed,
            count = batch.len(),
            error = %first,
            "Streams: buffered publisher for topic '{}' failed to write {} of a batch of {} entr(ies); they are lost.",
            shared.topic,
            failed,
            batch.len()
        );
    }
    for waiter in waiters.drain(..) {
        let covered = outcomes[..waiter.covers.min(outcomes.len())]
            .iter()
            .find_map(|o| o.as_ref().err());
        let _ = waiter.tx.send(match covered {
            None => Ok(()),
            Some(e) => Err(duplicate(e)),
        });
    }
    batch.clear();
}

/// `Error` is not `Clone`; a flush failure reaches every waiter that covered it.
fn duplicate(e: &Error) -> Error {
    match e {
        Error::Config(m) => Error::Config(m.clone()),
        Error::InvalidArgument(m) => Error::InvalidArgument(m.clone()),
        Error::Closed(c) => Error::Closed(c),
        Error::Transport { message, .. } => Error::transport(message.clone()),
        other => Error::transport(other.to_string()),
    }
}

// ---------------------------------------------------------------------------------------------
// What the buffer writes through
// ---------------------------------------------------------------------------------------------

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The direct publisher behind the buffer: a [`StreamPublisher`] in production, a recording double
/// in the unit tests.
trait Sink: fmt::Debug + Send + Sync + 'static {
    fn topic(&self) -> &str;
    fn route(&self, partition_key: &str, explicit: Option<u32>) -> Result<u32>;
    /// Writes a batch in order; one outcome per entry.
    fn write<'a>(&'a self, batch: &'a [Pending]) -> BoxFuture<'a, Vec<Result<()>>>;
    fn publish<'a>(
        &'a self,
        partition_key: &'a str,
        body: Bytes,
        msg_type: &'a str,
        options: &'a PublishOptions,
    ) -> BoxFuture<'a, Result<StreamId>>;
    fn publish_batch<'a>(
        &'a self,
        partition_key: &'a str,
        bodies: &'a [Bytes],
        msg_type: &'a str,
        options: &'a PublishOptions,
    ) -> BoxFuture<'a, Result<()>>;
}

impl Sink for StreamPublisher {
    fn topic(&self) -> &str {
        StreamPublisher::topic(self)
    }

    fn route(&self, partition_key: &str, explicit: Option<u32>) -> Result<u32> {
        StreamPublisher::route(self, partition_key, explicit)
    }

    fn write<'a>(&'a self, batch: &'a [Pending]) -> BoxFuture<'a, Vec<Result<()>>> {
        Box::pin(async move {
            let entries: Vec<PipelinedEntry<'_>> = batch
                .iter()
                .map(|p| PipelinedEntry {
                    partition_key: &p.partition_key,
                    body: &p.body,
                    msg_type: &p.msg_type,
                    options: &p.options,
                })
                .collect();
            self.publish_pipelined(&entries).await
        })
    }

    fn publish<'a>(
        &'a self,
        partition_key: &'a str,
        body: Bytes,
        msg_type: &'a str,
        options: &'a PublishOptions,
    ) -> BoxFuture<'a, Result<StreamId>> {
        Box::pin(Publisher::publish(
            self,
            partition_key,
            body,
            msg_type,
            options,
        ))
    }

    fn publish_batch<'a>(
        &'a self,
        partition_key: &'a str,
        bodies: &'a [Bytes],
        msg_type: &'a str,
        options: &'a PublishOptions,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(Publisher::publish_batch(
            self,
            partition_key,
            bodies,
            msg_type,
            options,
        ))
    }
}

/// Unit tests: port of the buffered parts of C# `ProducerTests.cs`, over a recording sink (C#
/// `RecordingPublisher` / `FailingPublisher`). Named after the C# test in `snake_case`.
#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::watch;

    use super::*;
    use crate::wire::partition_for_key;

    const TOPIC: &str = "unit-buffered";

    #[derive(Debug, Clone)]
    struct Sent {
        key: String,
        body: Bytes,
        partition: Option<u32>,
    }

    /// Records what it is asked to write. With a gate, every write waits for the gate to open;
    /// `started` counts writes begun; `fail_body` fails the entries with that body.
    #[derive(Debug)]
    struct Recording {
        partitions: u32,
        sent: Mutex<Vec<Sent>>,
        gate: watch::Receiver<bool>,
        started: AtomicUsize,
        fail_body: Option<&'static str>,
    }

    impl Recording {
        fn new(partitions: u32, open: bool) -> (Arc<Self>, watch::Sender<bool>) {
            Self::failing(partitions, open, None)
        }

        fn failing(
            partitions: u32,
            open: bool,
            fail_body: Option<&'static str>,
        ) -> (Arc<Self>, watch::Sender<bool>) {
            let (tx, rx) = watch::channel(open);
            let sink = Arc::new(Self {
                partitions,
                sent: Mutex::new(Vec::new()),
                gate: rx,
                started: AtomicUsize::new(0),
                fail_body,
            });
            (sink, tx)
        }

        fn sent(&self) -> Vec<Sent> {
            self.sent.lock().unwrap().clone()
        }

        fn started(&self) -> usize {
            self.started.load(Ordering::SeqCst)
        }
    }

    impl Sink for Recording {
        fn topic(&self) -> &str {
            TOPIC
        }

        fn route(&self, key: &str, explicit: Option<u32>) -> Result<u32> {
            match explicit {
                Some(p) if p >= self.partitions => Err(Error::invalid_argument("out of range")),
                Some(p) => Ok(p),
                None => Ok(partition_for_key(key.as_bytes(), self.partitions)),
            }
        }

        fn write<'a>(&'a self, batch: &'a [Pending]) -> BoxFuture<'a, Vec<Result<()>>> {
            Box::pin(async move {
                self.started.fetch_add(1, Ordering::SeqCst);
                let mut gate = self.gate.clone();
                let _ = gate.wait_for(|open| *open).await;
                let mut out = Vec::new();
                for p in batch {
                    if self.fail_body.is_some_and(|f| p.body == f.as_bytes()) {
                        out.push(Err(Error::transport("XADD rejected")));
                        continue;
                    }
                    self.sent.lock().unwrap().push(Sent {
                        key: p.partition_key.clone(),
                        body: p.body.clone(),
                        partition: p.options.partition,
                    });
                    out.push(Ok(()));
                }
                out
            })
        }

        fn publish<'a>(
            &'a self,
            partition_key: &'a str,
            body: Bytes,
            _msg_type: &'a str,
            options: &'a PublishOptions,
        ) -> BoxFuture<'a, Result<StreamId>> {
            Box::pin(async move {
                self.sent.lock().unwrap().push(Sent {
                    key: partition_key.to_owned(),
                    body,
                    partition: options.partition,
                });
                Ok(StreamId::new(1, 0))
            })
        }

        fn publish_batch<'a>(
            &'a self,
            _partition_key: &'a str,
            _bodies: &'a [Bytes],
            _msg_type: &'a str,
            _options: &'a PublishOptions,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn options(
        max_batch: u32,
        max_wait_ms: u32,
        max_queue: u32,
        drop_oldest: bool,
    ) -> ProducerOptions {
        ProducerOptions {
            topic: TOPIC.to_owned(),
            buffered: true,
            max_wait_ms,
            max_batch,
            max_queue,
            drop_oldest,
        }
    }

    fn buffered(sink: &Arc<Recording>, options: ProducerOptions) -> BufferedStreamPublisher {
        BufferedStreamPublisher::with_sink(
            Arc::clone(sink) as Arc<dyn Sink>,
            options,
            SHUTDOWN_FLUSH_TIMEOUT,
        )
    }

    fn body(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    async fn wait_until(mut cond: impl FnMut() -> bool, within: Duration) {
        let deadline = Instant::now() + within;
        while !cond() {
            assert!(Instant::now() < deadline, "condition not met in {within:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn enqueue(b: &BufferedStreamPublisher, key: &str, text: &str) {
        b.enqueue(key, body(text), "T", &PublishOptions::new())
            .await
            .unwrap();
    }

    /// C# `BufferedExplicitPartitionOverridesKeyHashing`.
    #[tokio::test]
    async fn buffered_explicit_partition_overrides_key_hashing() {
        let (sink, _gate) = Recording::new(8, true);
        let b = buffered(&sink, options(1, 0, 100, false));
        b.enqueue(
            "customer-42",
            body("a"),
            "T",
            &PublishOptions::new().partition(6),
        )
        .await
        .unwrap();
        b.flush().await.unwrap();
        let sent = sink.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].partition, Some(6));
    }

    /// The explicit-partition range check happens at enqueue.
    #[tokio::test]
    async fn buffered_explicit_partition_outside_range_fails_at_enqueue() {
        let (sink, _gate) = Recording::new(4, true);
        let b = buffered(&sink, options(1, 0, 100, false));
        let err = b
            .enqueue("k", body("a"), "T", &PublishOptions::new().partition(4))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    /// C# `FlushFiresAtMaxBatchWithoutWaitingForTheTimer`.
    #[tokio::test]
    async fn flush_fires_at_max_batch_without_waiting_for_the_timer() {
        let (sink, _gate) = Recording::new(1, true);
        let b = buffered(&sink, options(4, 60_000, 100, false));
        for i in 0..3 {
            enqueue(&b, "k", &format!("m{i}")).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            sink.sent().is_empty(),
            "MaxBatch not reached, MaxWaitMs 60 s away"
        );
        enqueue(&b, "k", "m3").await;
        wait_until(|| sink.sent().len() == 4, Duration::from_secs(5)).await;
    }

    /// C# `FlushFiresAtMaxWaitMeasuredFromTheOldestEntry`.
    #[tokio::test]
    async fn flush_fires_at_max_wait_measured_from_the_oldest_entry() {
        let (sink, _gate) = Recording::new(1, true);
        let b = Arc::new(buffered(&sink, options(10_000, 150, 100_000, false)));
        let started = Instant::now();
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let trickle = {
            let b = Arc::clone(&b);
            tokio::spawn(async move {
                let mut i = 0;
                loop {
                    enqueue(&b, "k", &format!("m{i}")).await;
                    i += 1;
                    tokio::select! {
                        _ = stop_rx.wait_for(|s| *s) => return,
                        () = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
            })
        };
        wait_until(|| !sink.sent().is_empty(), Duration::from_secs(5)).await;
        let elapsed = started.elapsed();
        stop_tx.send(true).unwrap();
        trickle.await.unwrap();
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
        assert!(sink.sent().len() < 20, "{}", sink.sent().len());
    }

    /// C# `FlushCompletesOnlyAfterEverythingEnqueuedBeforeItIsWritten`.
    #[tokio::test]
    async fn flush_completes_only_after_everything_enqueued_before_it_is_written() {
        let (sink, gate) = Recording::new(1, false);
        let b = Arc::new(buffered(&sink, options(1_000, 60_000, 100_000, false)));
        for i in 0..10 {
            enqueue(&b, "k", &format!("m{i}")).await;
        }
        let flush = {
            let b = Arc::clone(&b);
            tokio::spawn(async move { b.flush().await })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !flush.is_finished(),
            "the writes behind the barrier are not done"
        );
        assert!(sink.sent().is_empty());
        gate.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), flush)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(sink.sent().len(), 10);
        assert_eq!(b.shared.published.load(Ordering::Relaxed), 10);
    }

    /// C# `FullQueueWaitsByDefaultAndDropsNothing`.
    #[tokio::test]
    async fn full_queue_waits_by_default_and_drops_nothing() {
        let (sink, gate) = Recording::new(1, false);
        let opts = options(1, 0, 2, false);
        assert!(
            !ProducerOptions::default().drop_oldest,
            "DropOldest must be opt-in"
        );
        let b = Arc::new(buffered(&sink, opts));
        let producer = {
            let b = Arc::clone(&b);
            tokio::spawn(async move {
                for i in 0..50 {
                    enqueue(&b, "k", &format!("m{i}")).await;
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !producer.is_finished(),
            "a full queue must block the producer"
        );
        assert_eq!(b.shared.dropped.load(Ordering::Relaxed), 0);
        gate.send(true).unwrap();
        producer.await.unwrap();
        b.flush().await.unwrap();
        assert_eq!(sink.sent().len(), 50);
    }

    /// C# `DropOldestShedsAndCountsInsteadOfBlocking`.
    #[tokio::test]
    async fn drop_oldest_sheds_and_counts_instead_of_blocking() {
        let (sink, _gate) = Recording::new(1, false);
        let b = buffered(&sink, options(1, 0, 2, true));
        tokio::time::timeout(Duration::from_secs(5), async {
            for i in 0..50 {
                enqueue(&b, "k", &format!("m{i}")).await;
            }
        })
        .await
        .expect("nothing blocks under DropOldest");
        let dropped = b.shared.dropped.load(Ordering::Relaxed);
        assert!(dropped > 0 && dropped < 50, "{dropped}");
    }

    /// C# `FlushFaultsOnlyTheWaitersWhoseOwnEntriesFailed`.
    #[tokio::test]
    async fn flush_faults_only_the_waiters_whose_own_entries_failed() {
        let (sink, gate) = Recording::failing(1, false, Some("B"));
        let b = Arc::new(buffered(&sink, options(16, 0, 64, false)));
        enqueue(&b, "k", "warm").await;
        wait_until(|| sink.started() == 1, Duration::from_secs(5)).await;

        enqueue(&b, "k", "A").await;
        let flush_a = {
            let b = Arc::clone(&b);
            tokio::spawn(async move { b.flush().await })
        };
        wait_until(|| b.shared.lock().queue.len() == 2, Duration::from_secs(5)).await;
        enqueue(&b, "k", "B").await;
        let flush_b = {
            let b = Arc::clone(&b);
            tokio::spawn(async move { b.flush().await })
        };
        wait_until(|| b.shared.lock().queue.len() == 4, Duration::from_secs(5)).await;
        enqueue(&b, "k", "C").await;

        gate.send(true).unwrap();
        flush_a.await.unwrap().expect("A landed");
        let err = flush_b.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("XADD rejected"), "{err}");
        b.flush().await.unwrap();
        assert_eq!(b.shared.published.load(Ordering::Relaxed), 3);
    }

    /// C# `StopAsyncBoundsTheShutdownFlushAndReportsWhatItAbandoned`. The C# test reads the warning
    /// from a logger; this one reads the abandoned count the warning reports.
    #[tokio::test]
    async fn stop_async_bounds_the_shutdown_flush_and_reports_what_it_abandoned() {
        let (sink, _gate) = Recording::new(1, false);
        let b = BufferedStreamPublisher::with_sink(
            Arc::clone(&sink) as Arc<dyn Sink>,
            options(1, 0, 64, false),
            Duration::from_millis(250),
        );
        for i in 0..20 {
            enqueue(&b, "k", &format!("m{i}")).await;
        }
        wait_until(|| sink.started() == 1, Duration::from_secs(5)).await;
        let started = Instant::now();
        b.shutdown().await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(b.shared.abandoned.load(Ordering::Relaxed) > 0);
    }

    /// C# `ADroppedFlushMarkerFaultsRatherThanClaimingTheQueueWasWritten`.
    #[tokio::test]
    async fn a_dropped_flush_marker_faults_rather_than_claiming_the_queue_was_written() {
        let (sink, _gate) = Recording::new(1, false);
        let b = Arc::new(buffered(&sink, options(1, 0, 1, true)));
        enqueue(&b, "k", "warm").await;
        wait_until(|| sink.started() == 1, Duration::from_secs(5)).await;
        let flush = {
            let b = Arc::clone(&b);
            tokio::spawn(async move { b.flush().await })
        };
        wait_until(|| b.shared.lock().queue.len() == 1, Duration::from_secs(5)).await;
        for i in 0..5 {
            enqueue(&b, "k", &format!("m{i}")).await;
        }
        let err = flush.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("DropOldest"), "{err}");
    }

    /// Per-key order across many batches, keys interleaved (the C# cold-flush reorder must not
    /// happen here: one pipeline per batch, batches in sequence).
    #[tokio::test]
    async fn per_key_order_is_preserved_across_batches() {
        let (sink, _gate) = Recording::new(4, true);
        let b = buffered(&sink, options(7, 1, 1_000, false));
        for i in 0..500 {
            enqueue(&b, &format!("k{}", i % 3), &i.to_string()).await;
        }
        b.flush().await.unwrap();
        let sent = sink.sent();
        assert_eq!(sent.len(), 500);
        for key in ["k0", "k1", "k2"] {
            let seq: Vec<u32> = sent
                .iter()
                .filter(|s| s.key == key)
                .map(|s| std::str::from_utf8(&s.body).unwrap().parse().unwrap())
                .collect();
            assert!(seq.windows(2).all(|w| w[0] < w[1]), "{key} reordered");
        }
    }

    /// `publish` flushes the queue first, so a direct publish lands after what was enqueued.
    #[tokio::test]
    async fn publish_flushes_the_queue_first() {
        let (sink, _gate) = Recording::new(1, true);
        let b = buffered(&sink, options(1_000, 60_000, 100, false));
        enqueue(&b, "k", "queued").await;
        b.publish("k", body("direct"), "T", &PublishOptions::new())
            .await
            .unwrap();
        let bodies: Vec<Bytes> = sink.sent().into_iter().map(|s| s.body).collect();
        assert_eq!(bodies, [body("queued"), body("direct")]);
    }

    /// Shutdown drains, then everything is `Closed`.
    #[tokio::test]
    async fn shutdown_flushes_then_rejects_with_closed() {
        let (sink, _gate) = Recording::new(1, true);
        let b = buffered(&sink, options(1_000, 60_000, 100, false));
        for i in 0..5 {
            enqueue(&b, "k", &format!("m{i}")).await;
        }
        b.shutdown().await.unwrap();
        assert_eq!(sink.sent().len(), 5);
        let closed = |r: Result<()>| matches!(r, Err(Error::Closed(_)));
        assert!(closed(
            b.enqueue("k", body("x"), "T", &PublishOptions::new()).await
        ));
        assert!(closed(b.flush().await));
        assert!(matches!(
            b.publish("k", body("x"), "T", &PublishOptions::new()).await,
            Err(Error::Closed(_))
        ));
        b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn an_empty_type_is_rejected() {
        let (sink, _gate) = Recording::new(1, true);
        let b = buffered(&sink, options(1, 0, 10, false));
        assert!(matches!(
            b.enqueue("k", body("x"), "", &PublishOptions::new()).await,
            Err(Error::InvalidArgument(_))
        ));
    }
}
