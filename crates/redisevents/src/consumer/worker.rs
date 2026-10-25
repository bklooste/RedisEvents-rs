//! The per-partition worker: read loop hand-off, skip/block error contract, back-off
//! ([`BLOCK_BACKOFF`]), panic-as-skip, reset hand-off with the flusher.
//! **Owner: C2** (port of `PartitionWorker.*`, `HandlerAdapter.cs`, `Backoff.cs`). Crate-private.
//!
//! # Shape
//!
//! A [`Worker`] runs one read loop over one [`FetchSource`] (C1's [`Fetch`]: one partition, or a
//! co-located group read with one multi-stream `XREAD`) and hands each partition's batches to one
//! [`BatchHandler`]. Two execution modes, exactly as C#:
//!
//! - **Pooled** (`Backpressure.Enabled = true`, the default): one bounded channel (capacity in
//!   batches) and one processor task per partition. The read loop writes a batch *before* it issues
//!   the next fetch, so a full channel stops the reads. A partition whose processor stops
//!   (`StopPartition`, revoke) is retired: the group reads on and discards what it reads for it.
//! - **Inline** (`Backpressure.Enabled = false`): the read loop calls the handler itself before the
//!   next fetch. In a co-located inline group, `StopPartition` on one partition stops the whole loop
//!   (C#: there is only one loop to stop). C# hops a `ReadMode.Block` inline handler to the thread
//!   pool so it never runs on the dedicated reader thread; in Rust the read loop is an ordinary
//!   tokio task (the `XREAD` is a future, not a thread), so there is nothing to hop off.
//!
//! # The error contract
//!
//! See [`HandleError`](super::HandleError). Per batch: `Ok` → record `last`; `Skip` →
//! `ConsumerOptions::on_error`; `Block` → retry the same batch on [`BLOCK_BACKOFF`] while other
//! partitions run, `streams.blocked` = 1; a panic is caught and is a `Skip`. A retry that fails with
//! `Skip` ends the block and the policy decides (C#). An error returned after the partition's stop
//! token fired is a shutdown: no log, no metric, no advance.
//!
//! [`PerMessage`](super::PerMessage) reports how many leading messages it finished through a
//! task-local [`BatchScope`], so a `Block` (or a `StopPartition`/`Fail` skip) on message *i* counts
//! messages `..i` as done: they are recorded and a block retry resumes at message *i*.
//!
//! # Positions
//!
//! Recorded through [`PositionSink`] when processed, never when read (see `shared.rs`).

// Wired by C4 (host); until then only tests drive the worker.
#![cfg_attr(not(test), allow(dead_code))]

use std::any::Any;
use std::cell::Cell;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::fetch::{Fetch, StreamBatch};
use super::shared::{PartitionStop, PositionSink, ResetSignal};
use super::{BLOCK_BACKOFF, BatchHandler, HandleError, PartitionContext};
use crate::config::{ConsumerOptions, ErrorPolicy, PersistMode};
use crate::diagnostics::activity::{self, StreamSpanContext};
use crate::diagnostics::instruments;
use crate::error::{BoxError, Error, Result};
use crate::wire::{StreamId, StreamMsg};

#[cfg(test)]
mod ported_tests;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;

/// At most one "still blocked" error line per this interval (C# `BlockLogIntervalMs`).
pub(crate) const BLOCK_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// The delay before block retry `attempt` (0-based): 1, 2, 4, 8, 16, then 30 s forever.
pub(crate) fn block_delay(attempt: usize) -> Duration {
    BLOCK_BACKOFF[attempt.min(BLOCK_BACKOFF.len() - 1)]
}

// ------------------------------------------------------------------------------------------------
// The fetch seam
// ------------------------------------------------------------------------------------------------

/// What the read loop reads from (C# `MultiStreamFetch` delegate): C1's [`Fetch`] in production, a
/// scripted fetch in unit tests ([`testing::ScriptedFetch`]).
pub(crate) trait FetchSource: Send + 'static {
    /// One read: a batch per partition that had entries (never an empty batch). Cancel-safe.
    fn fetch(&mut self) -> impl Future<Output = Result<Vec<StreamBatch>>> + Send;

    /// Moves `partition`'s read cursor to after `after`. `false` if not covered.
    fn seek(&mut self, partition: u32, after: StreamId) -> bool;
}

impl FetchSource for Fetch {
    fn fetch(&mut self) -> impl Future<Output = Result<Vec<StreamBatch>>> + Send {
        Fetch::fetch(self)
    }

    fn seek(&mut self, partition: u32, after: StreamId) -> bool {
        Fetch::seek(self, partition, after)
    }
}

// ------------------------------------------------------------------------------------------------
// Configuration
// ------------------------------------------------------------------------------------------------

/// The per-consumer settings the worker needs (from [`ConsumerOptions`]).
#[derive(Debug, Clone)]
pub(crate) struct WorkerConfig {
    /// Topic name (tags, spans, contexts).
    pub(crate) topic: Arc<str>,
    /// Consumer name.
    pub(crate) consumer: Arc<str>,
    /// Message types to deliver; `None` / empty / blanks = all.
    pub(crate) filter: Option<Vec<String>>,
    /// What an ordinary (`Skip`) failure does.
    pub(crate) on_error: ErrorPolicy,
    /// When positions are recorded and flushed.
    pub(crate) persist: PersistMode,
    /// `Some(capacity)` = pooled mode with a channel of that many batches; `None` = inline.
    pub(crate) channel_capacity: Option<usize>,
}

impl WorkerConfig {
    /// Defaults for `topic`/`consumer`: no filter, `BestEffort`, `AsyncBatch`, pooled with 4.
    pub(crate) fn new(topic: impl Into<Arc<str>>, consumer: impl Into<Arc<str>>) -> Self {
        Self {
            topic: topic.into(),
            consumer: consumer.into(),
            filter: None,
            on_error: ErrorPolicy::BestEffort,
            persist: PersistMode::AsyncBatch,
            channel_capacity: Some(4),
        }
    }

    /// From the consumer's options; `consumer` is the resolved consumer name.
    pub(crate) fn from_options(options: &ConsumerOptions, consumer: &str) -> Self {
        Self {
            topic: options.topic.as_str().into(),
            consumer: consumer.into(),
            filter: options.filter.clone(),
            on_error: options.on_error,
            persist: options.persist,
            channel_capacity: options.backpressure.enabled.then(|| {
                usize::try_from(options.backpressure.capacity.max(1)).unwrap_or(usize::MAX)
            }),
        }
    }
}

/// Why a worker ended abnormally. `Ok(())` from [`Worker::run`] means every partition stopped
/// (shutdown, revoke, `StopPartition`).
#[derive(Debug)]
pub(crate) enum WorkerError {
    /// `ErrorPolicy::Fail`: a handler failed and the host must fail (C# rethrow).
    Handler {
        /// The partition whose handler failed.
        partition: u32,
        /// The handler's error.
        error: BoxError,
    },
    /// A read failed with something no back-off fixes (malformed reply, non-transient Redis error).
    Fetch(Error),
}

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handler { partition, error } => write!(
                f,
                "ErrorPolicy.Fail: the handler for partition {partition} failed: {error}"
            ),
            Self::Fetch(e) => write!(f, "the read loop failed: {e}"),
        }
    }
}

impl std::error::Error for WorkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Handler { error, .. } => Some(error.as_ref()),
            Self::Fetch(e) => Some(e),
        }
    }
}

/// One partition the worker reads: its index and its stop input (C4 keeps a clone).
#[derive(Debug, Clone)]
pub(crate) struct PartitionSlot {
    /// The partition index (must be covered by the fetch).
    pub(crate) partition: u32,
    /// Stops this partition (shutdown / revoke). Its token is the handler's context token.
    pub(crate) stop: PartitionStop,
}

impl PartitionSlot {
    /// A slot for `partition` stopped by `stop`.
    pub(crate) fn new(partition: u32, stop: PartitionStop) -> Self {
        Self { partition, stop }
    }
}

// ------------------------------------------------------------------------------------------------
// Type filter
// ------------------------------------------------------------------------------------------------

/// The consumer's message-type filter (C# `TypeFilter`). Built once per worker.
#[derive(Debug, Clone, Default)]
pub(crate) struct TypeFilter {
    types: Option<Box<[String]>>,
}

impl TypeFilter {
    /// `None`, empty, or nothing but blanks = keep everything. Duplicates are dropped.
    pub(crate) fn new(filter: Option<&[String]>) -> Self {
        let mut kept: Vec<String> = Vec::new();
        for t in filter.unwrap_or_default() {
            if !t.trim().is_empty() && !kept.contains(t) {
                kept.push(t.clone());
            }
        }
        Self {
            types: (!kept.is_empty()).then(|| kept.into_boxed_slice()),
        }
    }

    /// Whether this filter keeps everything.
    pub(crate) fn keeps_everything(&self) -> bool {
        self.types.is_none()
    }

    /// Whether a message of `msg_type` survives. An entry with no `t` (empty) never matches a filter.
    pub(crate) fn matches(&self, msg_type: &str) -> bool {
        self.types
            .as_ref()
            .is_none_or(|types| types.iter().any(|t| t == msg_type))
    }
}

// ------------------------------------------------------------------------------------------------
// Panic catching
// ------------------------------------------------------------------------------------------------

/// A future that turns a panic while polling `F` into `Err(payload)` (`catch_unwind` on a future).
pub(crate) struct CatchUnwind<F> {
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = std::result::Result<F::Output, Box<dyn Any + Send>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(move || inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

/// Wraps `fut` so a panic inside it is returned instead of unwinding.
pub(crate) fn catch_unwind<F: Future>(fut: F) -> CatchUnwind<F> {
    CatchUnwind {
        inner: Box::pin(fut),
    }
}

/// A caught panic as an error ("handler panicked: <message>").
#[derive(Debug)]
pub(crate) struct PanicError(pub(crate) String);

impl std::fmt::Display for PanicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "handler panicked: {}", self.0)
    }
}

impl std::error::Error for PanicError {}

/// The `Skip` a caught panic becomes.
pub(crate) fn panic_to_skip(payload: &(dyn Any + Send)) -> HandleError {
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    HandleError::Skip(Box::new(PanicError(message)))
}

// ------------------------------------------------------------------------------------------------
// The batch scope PerMessage reports through
// ------------------------------------------------------------------------------------------------

/// Set by the worker around every handler call; read by [`PerMessage`](super::PerMessage).
pub(crate) struct BatchScope {
    on_error: ErrorPolicy,
    done: Cell<usize>,
}

tokio::task_local! {
    static SCOPE: BatchScope;
}

/// The consumer's `on_error` when called from a worker; `None` when a handler is called directly.
pub(crate) fn scope_policy() -> Option<ErrorPolicy> {
    SCOPE.try_with(|s| s.on_error).ok()
}

/// `PerMessage`: the first `done` messages of the current batch were handled.
pub(crate) fn scope_report_done(done: usize) {
    let _ = SCOPE.try_with(|s| s.done.set(done));
}

/// The `exception.type` tag of `streams.errors` for an error.
pub(crate) fn error_type(error: &(dyn std::error::Error + Send + Sync + 'static)) -> &'static str {
    if error.is::<PanicError>() {
        return "panic";
    }
    if let Some(own) = error.downcast_ref::<Error>() {
        return match own {
            Error::Config(_) => "Config",
            Error::Transport { .. } => "Transport",
            Error::Redis(_) => "Redis",
            Error::InvalidArgument(_) => "InvalidArgument",
            Error::Codec(_) => "Codec",
            Error::Closed(_) => "Closed",
        };
    }
    if error.is::<std::io::Error>() {
        return "io::Error";
    }
    if error.is::<crate::message::CodecError>() {
        return "Codec";
    }
    "Error"
}

/// `PerMessage` under `BestEffort`: logs and counts one skipped message.
pub(crate) fn skip_message(ctx: &PartitionContext, msg: &StreamMsg, error: &HandleError) {
    let e = error.error();
    count_error(ctx.topic(), ctx.consumer(), "best_effort", error_type(e));
    tracing::error!(
        topic = ctx.topic(),
        partition = ctx.partition(),
        consumer = ctx.consumer(),
        id = %msg.id(),
        msg_type = msg.msg_type(),
        error = %e,
        "Streams: handler failed for one message. ErrorPolicy.BestEffort: skipping it and continuing with the next. \
         The library does not retry and has no dead-letter queue; block (HandleError::Block) instead to retry."
    );
}

// ------------------------------------------------------------------------------------------------
// Metric taps (production calls go to diagnostics::instruments; tests also observe them here)
// ------------------------------------------------------------------------------------------------

#[cfg(test)]
pub(crate) static TAP: Mutex<Vec<testing::Measurement>> = Mutex::new(Vec::new());

#[cfg(test)]
fn tap(m: testing::Measurement) {
    TAP.lock().unwrap_or_else(PoisonError::into_inner).push(m);
}

fn count_filtered(topic: &str, consumer: &str, dropped: u64) {
    instruments::record_filtered(topic, consumer, dropped);
    #[cfg(test)]
    tap(testing::Measurement::Filtered {
        topic: topic.to_owned(),
        consumer: consumer.to_owned(),
        count: dropped,
    });
}

fn count_consumed(topic: &str, partition: u32, consumer: &str, count: u64) {
    if count == 0 {
        return;
    }
    instruments::record_consumed(topic, partition, consumer, count);
    #[cfg(test)]
    tap(testing::Measurement::Consumed {
        topic: topic.to_owned(),
        partition,
        count,
    });
}

fn count_error(topic: &str, consumer: &str, policy: &'static str, exception_type: &'static str) {
    instruments::record_error(topic, consumer, policy, exception_type);
    #[cfg(test)]
    tap(testing::Measurement::Error {
        topic: topic.to_owned(),
        policy,
        exception_type,
    });
}

fn record_attempt(topic: &str, consumer: &str, size: usize, started: Instant) {
    if size == 0 {
        // A batch the filter emptied is not a sample of handling (C# R-10).
        return;
    }
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    instruments::record_batch(topic, consumer, size as u64, ms);
    #[cfg(test)]
    tap(testing::Measurement::Batch {
        topic: topic.to_owned(),
        size: size as u64,
    });
}

// ------------------------------------------------------------------------------------------------
// The processing step (shared by inline and pooled modes)
// ------------------------------------------------------------------------------------------------

/// One partition's identity inside a worker.
#[derive(Debug, Clone)]
struct Part {
    partition: u32,
    stop: PartitionStop,
    ctx: PartitionContext,
    /// `topic:partition:consumer`, the per-partition gauge key.
    key: String,
}

/// What the loop does after a batch.
#[derive(Debug)]
enum Outcome {
    /// Take the next batch.
    Continue,
    /// The partition's stop token fired (shutdown or revoke): stop, nothing more to say.
    Stopped,
    /// `ErrorPolicy::StopPartition`.
    StopPartition,
    /// `ErrorPolicy::Fail`.
    Fault(BoxError),
}

/// A failed attempt: the error, and how many leading messages of the attempted slice were done.
struct Failure {
    error: HandleError,
    done: usize,
    /// The position through `done` was already recorded (an advance failed after its record, or
    /// `SyncMessage` recorded each finished message), so it must not be recorded again.
    recorded: bool,
}

/// Everything the processing step needs; shared by the read loop and the processors.
struct Processor<H> {
    cfg: WorkerConfig,
    filter: TypeFilter,
    handler: Arc<H>,
    positions: Arc<dyn PositionSink>,
}

const fn policy_tag(policy: ErrorPolicy) -> &'static str {
    match policy {
        ErrorPolicy::BestEffort => "best_effort",
        ErrorPolicy::StopPartition => "stop_partition",
        ErrorPolicy::Fail => "fail",
    }
}

fn first_id(msgs: &[StreamMsg], last: StreamId) -> StreamId {
    msgs.first().map_or(last, StreamMsg::id)
}

fn first_type(msgs: &[StreamMsg]) -> &str {
    msgs.first().map_or("", StreamMsg::msg_type)
}

impl<H: BatchHandler> Processor<H> {
    fn span(
        &self,
        part: &Part,
        msgs: &[StreamMsg],
        last: StreamId,
    ) -> Option<activity::StreamSpan> {
        (!msgs.is_empty()).then(|| {
            activity::start_process(
                &StreamSpanContext {
                    topic: &self.cfg.topic,
                    partition: part.partition,
                    consumer: &self.cfg.consumer,
                },
                msgs,
                last,
            )
        })
    }

    /// Applies the type filter, counting what it dropped.
    fn filter(&self, msgs: Vec<StreamMsg>) -> Vec<StreamMsg> {
        if self.filter.keeps_everything() {
            return msgs;
        }
        let read = msgs.len();
        let kept: Vec<StreamMsg> = msgs
            .into_iter()
            .filter(|m| self.filter.matches(m.msg_type()))
            .collect();
        let dropped = read - kept.len();
        if dropped > 0 {
            count_filtered(&self.cfg.topic, &self.cfg.consumer, dropped as u64);
        }
        kept
    }

    /// Runs one batch through the handler and applies the position and error rules.
    async fn process(&self, part: &Part, batch: StreamBatch) -> Outcome {
        let StreamBatch { msgs, last, .. } = batch;
        let msgs = self.filter(msgs);
        if part.stop.is_revoked() {
            return Outcome::Stopped;
        }

        let started = Instant::now();
        let mut span = self.span(part, &msgs, last);
        let first = self.invoke(part, &msgs, last).await;
        record_attempt(&self.cfg.topic, &self.cfg.consumer, msgs.len(), started);

        match first {
            Ok(()) => {
                self.consumed(part, msgs.len());
                Outcome::Continue
            }
            Err(_) if part.stop.is_stopped() => {
                // Shutdown, not a failure: no log, no metric, no position. The batch redelivers.
                Outcome::Stopped
            }
            Err(Failure {
                error: HandleError::Block(e),
                done,
                recorded,
            }) => {
                if let Some(s) = span.as_mut() {
                    s.set_error(&e.to_string());
                }
                // The first attempt's span ends here, before the block: each retry gets its own.
                drop(span);
                self.block_and_retry(part, &msgs, last, done, recorded, e)
                    .await
            }
            Err(Failure {
                error: HandleError::Skip(e),
                done,
                recorded,
            }) => {
                if let Some(s) = span.as_mut() {
                    s.set_error(&e.to_string());
                }
                self.on_failure(part, &msgs, last, done, recorded, e).await
            }
        }
    }

    fn consumed(&self, part: &Part, count: usize) {
        count_consumed(
            &self.cfg.topic,
            part.partition,
            &self.cfg.consumer,
            count as u64,
        );
    }

    /// Calls the handler on `msgs` inside a [`BatchScope`], catching panics.
    async fn call(&self, part: &Part, msgs: &[StreamMsg]) -> std::result::Result<(), Failure> {
        let scope = BatchScope {
            on_error: self.cfg.on_error,
            done: Cell::new(0),
        };
        let handler = &self.handler;
        let ctx = &part.ctx;
        let fut = SCOPE.scope(scope, async move {
            let result = handler.handle_batch(msgs, ctx).await;
            let done = SCOPE.try_with(|s| s.done.get()).unwrap_or(0);
            (result, done)
        });
        match catch_unwind(fut).await {
            Ok((Ok(()), _)) => Ok(()),
            Ok((Err(error), done)) => Err(Failure {
                error,
                done: done.min(msgs.len()),
                recorded: false,
            }),
            Err(payload) => Err(Failure {
                error: panic_to_skip(payload.as_ref()),
                done: 0,
                recorded: false,
            }),
        }
    }

    /// Handler, then advance: the two steps that happen together on every attempt (C# `InvokeAsync`).
    async fn invoke(
        &self,
        part: &Part,
        msgs: &[StreamMsg],
        last: StreamId,
    ) -> std::result::Result<(), Failure> {
        if msgs.is_empty() {
            // Everything was filtered out (or already done): nothing to hand a handler, but the
            // position still advances past what was read, inside the error handling.
            return self.advance(part, last).await.map_err(|error| Failure {
                error,
                done: 0,
                recorded: true,
            });
        }
        if self.cfg.persist == PersistMode::SyncMessage {
            for (i, msg) in msgs.iter().enumerate() {
                self.call(part, std::slice::from_ref(msg))
                    .await
                    .map_err(|f| Failure {
                        error: f.error,
                        done: i,
                        recorded: true,
                    })?;
                self.advance(part, msg.id())
                    .await
                    .map_err(|error| Failure {
                        error,
                        done: i + 1,
                        recorded: true,
                    })?;
            }
            if msgs.last().map(StreamMsg::id) != Some(last) {
                self.advance(part, last).await.map_err(|error| Failure {
                    error,
                    done: msgs.len(),
                    recorded: true,
                })?;
            }
            return Ok(());
        }
        self.call(part, msgs).await?;
        self.advance(part, last).await.map_err(|error| Failure {
            error,
            done: msgs.len(),
            recorded: true,
        })
    }

    /// Records (and under `Sync*`, persists) a processed position (C# `AdvanceAsync`).
    async fn advance(&self, part: &Part, id: StreamId) -> std::result::Result<(), HandleError> {
        if self.cfg.persist == PersistMode::None || part.stop.is_revoked() {
            return Ok(());
        }
        self.positions.record(part.partition, id);
        if matches!(
            self.cfg.persist,
            PersistMode::SyncBatch | PersistMode::SyncMessage
        ) {
            self.positions.flush().await.map_err(HandleError::from)?;
        }
        Ok(())
    }

    /// Records the messages of a failed attempt that were finished before it failed.
    fn record_partial(&self, part: &Part, msgs: &[StreamMsg], done: usize, recorded: bool) {
        if done == 0 {
            return;
        }
        self.consumed(part, done);
        if !recorded && self.cfg.persist != PersistMode::None && !part.stop.is_revoked() {
            self.positions.record(part.partition, msgs[done - 1].id());
        }
    }

    /// Applies `on_error` to an ordinary failure (C# `OnFailureAsync`).
    async fn on_failure(
        &self,
        part: &Part,
        msgs: &[StreamMsg],
        last: StreamId,
        done: usize,
        recorded: bool,
        error: BoxError,
    ) -> Outcome {
        let policy = self.cfg.on_error;
        count_error(
            &self.cfg.topic,
            &self.cfg.consumer,
            policy_tag(policy),
            error_type(error.as_ref()),
        );
        let (topic, consumer, partition) = (&*self.cfg.topic, &*self.cfg.consumer, part.partition);
        let (first, ty, count) = (first_id(msgs, last), first_type(msgs), msgs.len());
        match policy {
            ErrorPolicy::StopPartition => {
                tracing::error!(
                    topic, partition, consumer, %first, %last, count, msg_type = ty, error = %error,
                    "Streams: handler failed. ErrorPolicy.StopPartition: the position was NOT advanced and this \
                     partition is stopping; other partitions keep running."
                );
                self.record_partial(part, msgs, done, recorded);
                Outcome::StopPartition
            }
            ErrorPolicy::Fail => {
                tracing::error!(
                    topic, partition, consumer, %first, %last, count, msg_type = ty, error = %error,
                    "Streams: handler failed. ErrorPolicy.Fail: failing the host."
                );
                self.record_partial(part, msgs, done, recorded);
                Outcome::Fault(error)
            }
            ErrorPolicy::BestEffort => {
                tracing::error!(
                    topic, partition, consumer, %first, %last, count, msg_type = ty, error = %error,
                    "Streams: handler failed. ErrorPolicy.BestEffort: advancing past the batch. The library does not \
                     retry and has no dead-letter queue; the handler owns retry, or should block (HandleError::Block)."
                );
                self.record_partial(part, msgs, done, recorded);
                if let Err(advance) = self.advance(part, last).await {
                    // BestEffort must still mean "carry on": the batch redelivers on the next start.
                    tracing::warn!(
                        topic, partition, consumer, error = %advance,
                        "Streams: the position write after a best-effort failure itself failed. The partition keeps \
                         running and the batch redelivers on the next start."
                    );
                }
                Outcome::Continue
            }
        }
    }

    /// Blocks this partition on a failing batch and retries it on [`BLOCK_BACKOFF`] until it
    /// succeeds, fails with a `Skip`, or the partition is stopped (C# `BlockAndRetryAsync`).
    async fn block_and_retry(
        &self,
        part: &Part,
        msgs: &[StreamMsg],
        last: StreamId,
        done: usize,
        recorded: bool,
        first: BoxError,
    ) -> Outcome {
        let (topic, consumer, partition) = (&*self.cfg.topic, &*self.cfg.consumer, part.partition);
        count_error(topic, consumer, "block", error_type(first.as_ref()));
        self.record_partial(part, msgs, done, recorded);
        let mut from = done;

        let blocked_at = Instant::now();
        let mut last_logged = blocked_at;
        instruments::set_blocked(&part.key, 1);
        // Health: Degraded now, Unhealthy past `unhealthy_block_seconds` (the clock runs across
        // every retry).
        self.positions.blocked(part.partition, true);
        tracing::error!(
            topic, partition, consumer,
            first = %first_id(&msgs[from..], last), %last, count = msgs.len() - from,
            msg_type = first_type(&msgs[from..]), error = %first,
            "Streams: handler blocked (HandleError::Block). The position was NOT advanced and this partition is now \
             BLOCKED, retrying the same batch with backoff (1s→2→4→8→16→30s cap, no attempt limit) until it succeeds. \
             Other partitions keep running. Further failures are logged once every 30 s."
        );

        let mut attempt = 0usize;
        let outcome = loop {
            instruments::set_block_duration_ms(&part.key, millis(blocked_at.elapsed()));
            let delay = block_delay(attempt);
            attempt += 1;
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = part.stop.token().cancelled() => {
                    tracing::info!(
                        topic, partition, consumer, attempts = attempt,
                        elapsed_ms = millis(blocked_at.elapsed()),
                        "Streams: stopping while the partition was blocked. The position was never advanced, so \
                         this batch redelivers."
                    );
                    break Outcome::Stopped;
                }
            }
            instruments::set_block_duration_ms(&part.key, millis(blocked_at.elapsed()));

            let rest = &msgs[from..];
            let started = Instant::now();
            let mut span = self.span(part, rest, last);
            let result = self.invoke(part, rest, last).await;
            record_attempt(topic, consumer, rest.len(), started);
            match result {
                Ok(()) => {
                    self.consumed(part, rest.len());
                    tracing::info!(
                        topic, partition, consumer, attempts = attempt + 1,
                        elapsed_ms = millis(blocked_at.elapsed()), %last,
                        "Streams: partition recovered; the blocked batch was processed and the position advanced."
                    );
                    break Outcome::Continue;
                }
                Err(_) if part.stop.is_stopped() => break Outcome::Stopped,
                Err(Failure {
                    error: HandleError::Block(e),
                    done,
                    recorded,
                }) => {
                    count_error(topic, consumer, "block", error_type(e.as_ref()));
                    if let Some(s) = span.as_mut() {
                        s.set_error(&e.to_string());
                    }
                    self.record_partial(part, rest, done, recorded);
                    from += done;
                    if last_logged.elapsed() >= BLOCK_LOG_INTERVAL {
                        last_logged = Instant::now();
                        tracing::error!(
                            topic, partition, consumer, attempts = attempt + 1,
                            elapsed_ms = millis(blocked_at.elapsed()), error = %e,
                            next_retry_ms = millis(block_delay(attempt)),
                            "Streams: partition is still BLOCKED."
                        );
                    }
                }
                Err(Failure {
                    error: HandleError::Skip(e),
                    done,
                    recorded,
                }) => {
                    // No longer declared un-skippable: the ordinary policy decides, and the block ends.
                    if let Some(s) = span.as_mut() {
                        s.set_error(&e.to_string());
                    }
                    break self.on_failure(part, rest, last, done, recorded, e).await;
                }
            }
        };
        self.positions.blocked(part.partition, false);
        instruments::set_blocked(&part.key, 0);
        instruments::set_block_duration_ms(&part.key, 0.0);
        outcome
    }
}

fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

// ------------------------------------------------------------------------------------------------
// Transport outages
// ------------------------------------------------------------------------------------------------

fn transient_redis(e: &redis::RedisError) -> bool {
    if e.is_io_error() || e.is_connection_dropped() || e.is_timeout() || e.is_connection_refusal() {
        return true;
    }
    e.code().is_some_and(|code| {
        ["LOADING", "MASTERDOWN", "CLUSTERDOWN", "TRYAGAIN", "BUSY"].contains(&code)
    })
}

/// Whether a fetch failure is an outage to wait out rather than a fault (C#
/// `IsTransientTransportFailure`): a dropped/refused/timed-out connection or a "not right now"
/// server error, including a failed reconnect (C1 wraps it in `Transport` with the Redis source).
pub(crate) fn is_transient(error: &Error) -> bool {
    match error {
        Error::Redis(e) => transient_redis(e),
        Error::Transport {
            source: Some(source),
            ..
        } => {
            if let Some(Error::Redis(e)) = source.downcast_ref::<Error>() {
                transient_redis(e)
            } else if let Some(e) = source.downcast_ref::<redis::RedisError>() {
                transient_redis(e)
            } else {
                false
            }
        }
        _ => false,
    }
}

// ------------------------------------------------------------------------------------------------
// The worker
// ------------------------------------------------------------------------------------------------

/// Resolves once any of `tokens` is cancelled (immediately if `tokens` is empty: nothing to wait for).
async fn any_cancelled(tokens: Vec<CancellationToken>) {
    if tokens.is_empty() {
        return;
    }
    let mut waits: Vec<_> = tokens
        .into_iter()
        .map(|t| Box::pin(t.cancelled_owned()))
        .collect();
    std::future::poll_fn(|cx| {
        if waits.iter_mut().any(|w| w.as_mut().poll(cx).is_ready()) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

/// One read loop and its partitions. See the module docs.
pub(crate) struct Worker<H, F> {
    processor: Arc<Processor<H>>,
    fetch: F,
    parts: Vec<Part>,
    resets: Option<Arc<ResetSignal>>,
}

impl<H: BatchHandler, F: FetchSource> Worker<H, F> {
    /// A worker reading `partitions` through `fetch` (which must cover exactly them).
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for no partitions or a duplicate; [`Error::Config`] for a
    /// `Sync*` persist mode with no real sink is not detectable here and is C4's to refuse.
    pub(crate) fn new(
        cfg: WorkerConfig,
        partitions: Vec<PartitionSlot>,
        fetch: F,
        handler: Arc<H>,
        positions: Arc<dyn PositionSink>,
        resets: Option<Arc<ResetSignal>>,
    ) -> Result<Self> {
        if partitions.is_empty() {
            return Err(Error::invalid_argument(
                "A worker needs at least one partition.",
            ));
        }
        for (i, p) in partitions.iter().enumerate() {
            if partitions[..i].iter().any(|o| o.partition == p.partition) {
                return Err(Error::invalid_argument(format!(
                    "Partition {} appears twice in one worker.",
                    p.partition
                )));
            }
        }
        let parts = partitions
            .into_iter()
            .map(|s| Part {
                partition: s.partition,
                ctx: PartitionContext::new(
                    cfg.topic.clone(),
                    cfg.consumer.clone(),
                    s.partition,
                    s.stop.token().clone(),
                ),
                key: format!("{}:{}:{}", cfg.topic, s.partition, cfg.consumer),
                stop: s.stop,
            })
            .collect();
        Ok(Self {
            processor: Arc::new(Processor {
                filter: TypeFilter::new(cfg.filter.as_deref()),
                cfg,
                handler,
                positions,
            }),
            fetch,
            parts,
            resets,
        })
    }

    /// Runs until every partition has stopped. `Ok(())` for shutdown, revoke and `StopPartition`;
    /// `Err` for `ErrorPolicy::Fail` or a read failure no back-off fixes. Dropping the future stops
    /// everything at once (cancel-safe: nothing is recorded for what was not handled).
    #[allow(clippy::too_many_lines)] // one loop, mirroring C# ReadGroupLoopAsync section by section
    pub(crate) async fn run(self) -> std::result::Result<(), WorkerError> {
        let Self {
            processor,
            mut fetch,
            parts,
            resets,
        } = self;
        let count = parts.len();
        let mut retired = vec![false; count];
        let topic = processor.cfg.topic.clone();
        let consumer = processor.cfg.consumer.clone();

        // Pooled mode: one channel and processor per partition.
        let fault = CancellationToken::new();
        let fault_slot: Arc<Mutex<Option<WorkerError>>> = Arc::new(Mutex::new(None));
        let mut processors = JoinSet::new();
        let mut senders: Vec<Option<mpsc::Sender<StreamBatch>>> = Vec::with_capacity(count);
        // Fires when a partition's processor has exited, so a parked read wakes and retires it.
        let exited: Vec<CancellationToken> = (0..count).map(|_| CancellationToken::new()).collect();
        if let Some(capacity) = processor.cfg.channel_capacity {
            for (part, exited) in parts.iter().zip(&exited) {
                let (tx, rx) = mpsc::channel(capacity.max(1));
                senders.push(Some(tx));
                let exited = exited.clone().drop_guard();
                let processor = processor.clone();
                let part = part.clone();
                let fault = fault.clone();
                let fault_slot = fault_slot.clone();
                processors.spawn(async move {
                    let _exited = exited;
                    process_loop(processor, part, rx, fault, fault_slot).await;
                });
            }
        }
        let pooled = processor.cfg.channel_capacity.is_some();

        tracing::debug!(
            topic = &*topic,
            consumer = &*consumer,
            partitions = count,
            pooled,
            "Streams: read loop started."
        );

        let mut outage = 0usize;
        let result: std::result::Result<(), WorkerError> = 'read: loop {
            // Retire partitions that were stopped, or whose processor has gone.
            for i in 0..count {
                if retired[i] {
                    continue;
                }
                let gone = pooled && senders[i].as_ref().is_none_or(mpsc::Sender::is_closed);
                if parts[i].stop.is_stopped() || gone {
                    retired[i] = true;
                    if pooled {
                        // Dropping the sender lets the processor drain what is queued and exit.
                        senders[i] = None;
                    }
                }
            }
            if fault.is_cancelled() || retired.iter().all(|r| *r) {
                break 'read Ok(());
            }

            // Live resets: take, rewind the flusher, seek — before the next fetch.
            if let Some(resets) = resets.as_ref().filter(|r| r.has_pending()) {
                for (i, part) in parts.iter().enumerate() {
                    if retired[i] {
                        continue;
                    }
                    if let Some(marker) = resets.try_take(part.partition) {
                        processor.positions.rewind(part.partition);
                        fetch.seek(part.partition, marker.target);
                        tracing::warn!(
                            topic = &*topic, partition = part.partition, consumer = &*consumer,
                            target = %marker.target,
                            "Streams: reset applied — restarting reads after the target. Entries after it are \
                             delivered again, and batches already read are still processed, so handlers must be \
                             idempotent."
                        );
                    }
                }
            }

            let mut wake: Vec<CancellationToken> = Vec::with_capacity(2 * count + 1);
            for (i, part) in parts.iter().enumerate() {
                if !retired[i] {
                    wake.push(part.stop.token().clone());
                    if pooled {
                        wake.push(exited[i].clone());
                    }
                }
            }
            wake.push(fault.clone());

            let read = tokio::select! {
                r = fetch.fetch() => r,
                () = any_cancelled(wake.clone()) => continue 'read,
            };

            let batches = match read {
                Ok(b) => b,
                Err(e) if is_transient(&e) => {
                    if outage == 0 {
                        count_error(&topic, &consumer, "transport", error_type(&e));
                        tracing::error!(
                            topic = &*topic, consumer = &*consumer, error = %e,
                            "Streams: reading failed. The connection is treated as a transient outage: the read is \
                             retried with backoff (1s→2→4→8→16→30s cap, no attempt limit). The position was not \
                             advanced, so nothing is skipped."
                        );
                    } else {
                        tracing::debug!(topic = &*topic, consumer = &*consumer, error = %e, "Streams: retrying the read.");
                    }
                    let delay = block_delay(outage);
                    outage += 1;
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = any_cancelled(wake) => {}
                    }
                    continue 'read;
                }
                Err(e) => break 'read Err(WorkerError::Fetch(e)),
            };
            if outage != 0 {
                outage = 0;
                tracing::info!(
                    topic = &*topic,
                    consumer = &*consumer,
                    "Streams: reading recovered; the read resumed from the unchanged cursor, so nothing was skipped."
                );
            }

            for batch in batches {
                let Some(slot) = parts.iter().position(|p| p.partition == batch.partition) else {
                    tracing::warn!(
                        topic = &*topic,
                        partition = batch.partition,
                        "Streams: the fetch returned a partition this worker does not read; ignoring it."
                    );
                    continue;
                };
                if retired[slot] {
                    // Read on, discard, record nothing: the stored position stays where it stopped.
                    continue;
                }
                let part = &parts[slot];
                if pooled {
                    let Some(tx) = senders[slot].as_ref() else {
                        continue;
                    };
                    let sent = tokio::select! {
                        r = tx.send(batch) => r.is_ok(),
                        () = part.stop.token().cancelled() => false,
                        () = fault.cancelled() => false,
                    };
                    if !sent {
                        retired[slot] = true;
                        senders[slot] = None;
                        if !part.stop.is_stopped() && !fault.is_cancelled() {
                            tracing::warn!(
                                topic = &*topic,
                                partition = part.partition,
                                consumer = &*consumer,
                                "Streams: partition was stood down, so the reader discards what it reads for it. \
                                 Its stored position is unchanged; co-located siblings keep running."
                            );
                        }
                    }
                } else {
                    match processor.process(part, batch).await {
                        Outcome::Continue => {}
                        Outcome::Stopped => retired[slot] = true,
                        Outcome::StopPartition => {
                            if count > 1 {
                                tracing::warn!(
                                    topic = &*topic,
                                    partition = part.partition,
                                    consumer = &*consumer,
                                    partitions = count,
                                    "Streams: inline co-located loop stopping because a partition stopped; the \
                                     partitions share this loop, so all of them stop together."
                                );
                            }
                            break 'read Ok(());
                        }
                        Outcome::Fault(error) => {
                            break 'read Err(WorkerError::Handler {
                                partition: part.partition,
                                error,
                            });
                        }
                    }
                }
            }
        };

        // Let the processors drain (graceful) or stop (revoked / fault), then collect a fault.
        drop(senders);
        if matches!(result, Err(WorkerError::Handler { .. })) {
            // ErrorPolicy::Fail: the host is failing; nothing else is worth finishing.
            processors.abort_all();
        }
        while let Some(joined) = processors.join_next().await {
            if let Err(e) = joined
                && e.is_panic()
            {
                std::panic::resume_unwind(e.into_panic());
            }
        }
        tracing::debug!(
            topic = &*topic,
            consumer = &*consumer,
            "Streams: read loop stopped."
        );
        result?;
        let fault = fault_slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        fault.map_or(Ok(()), Err)
    }
}

/// Pooled mode: one partition's processor (C# `ProcessLoopAsync`).
async fn process_loop<H: BatchHandler>(
    processor: Arc<Processor<H>>,
    part: Part,
    mut rx: mpsc::Receiver<StreamBatch>,
    fault: CancellationToken,
    fault_slot: Arc<Mutex<Option<WorkerError>>>,
) {
    // Drains what is queued after the sender is dropped (graceful stop), unless revoked.
    while let Some(batch) = rx.recv().await {
        if part.stop.is_revoked() {
            break;
        }
        match processor.process(&part, batch).await {
            Outcome::Continue => {}
            Outcome::Stopped | Outcome::StopPartition => break,
            Outcome::Fault(error) => {
                let mut slot = fault_slot.lock().unwrap_or_else(PoisonError::into_inner);
                slot.get_or_insert(WorkerError::Handler {
                    partition: part.partition,
                    error,
                });
                fault.cancel();
                break;
            }
        }
    }
}
