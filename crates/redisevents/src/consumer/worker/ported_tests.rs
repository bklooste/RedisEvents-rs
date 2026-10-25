#![allow(
    clippy::unused_async_trait_impl,
    clippy::assert_is_empty,
    clippy::items_after_statements
)]
//! C2t: the bulk C# ports against the worker, 1:1 by name (`snake_case` of the C# test name).
//! Sources: `ErrorHandlingTests.cs`, `ConsumerPipelineTests.cs`, `ConsumerCoreUnitTests.cs`.
//!
//! Each test says in a comment where the Rust behaviour differs from C#, or where the C# case has
//! no Rust counterpart (and why). Log lines are asserted through a `tracing` capture layer, so the
//! log-level and message assertions are real, not dropped.
//!
//! Deferred to C4t (need the host or host-owned state; not written here): the C# startup refusal
//! of a `Sync*` persist mode without a real flush (`ErrorHandlingTests`, `ConsumerPipelineTests`),
//! and the partition-monitor assertions (`StreamLag` running/caught-up state, `lag.ms`).
//! Not applicable to the Rust worker (no C# counterpart by design): the `Backoff` poll-idle
//! ladder (C1 `fetch.rs`), the pooled `StreamBatch` rent/return accounting, the reader-connection
//! surface reflection, the `ProcessLoopAsync`/channel-reader entry-point guards, and the
//! linear-scan/frozen-set filter split.

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use redis::Value;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;

use super::testing::{
    Measurement, RecordingSink, ScriptedFetch, Step, batch, measurements, unique,
};
use super::*;
use crate::consumer::HandleResult;
use crate::consumer::fetch::{SliceKey, parse_all};
use crate::consumer::shared::PartitionStop;
use crate::error::Result;

// ------------------------------------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------------------------------------

type Script = dyn Fn(usize, &[StreamMsg]) -> HandleResult + Send + Sync;

/// A handler that records every call and answers from a script `(call index, batch)`.
struct Probe {
    calls: Mutex<Vec<Vec<StreamId>>>,
    bodies: Mutex<Vec<String>>,
    at: Mutex<Vec<Instant>>,
    script: Box<Script>,
}

impl Probe {
    fn new(
        script: impl Fn(usize, &[StreamMsg]) -> HandleResult + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::default(),
            bodies: Mutex::default(),
            at: Mutex::default(),
            script: Box::new(script),
        })
    }

    fn ok() -> Arc<Self> {
        Self::new(|_, _| Ok(()))
    }

    fn calls(&self) -> Vec<Vec<StreamId>> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn bodies(&self) -> Vec<String> {
        self.bodies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn times(&self) -> Vec<Instant> {
        self.at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl BatchHandler for Probe {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        let n = {
            let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
            calls.push(batch.iter().map(StreamMsg::id).collect());
            calls.len() - 1
        };
        self.bodies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(
                batch
                    .iter()
                    .map(|m| String::from_utf8_lossy(m.body()).into_owned()),
            );
        self.at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Instant::now());
        (self.script)(n, batch)
    }
}

/// A handler that parks on a semaphore before answering (the C# "handler that will not return" and
/// "gate" shapes). `fail` makes every answer an ordinary (`Skip`) failure.
struct Gated {
    gate: Semaphore,
    entered: AtomicUsize,
    fail: bool,
    seen: Mutex<Vec<StreamId>>,
}

impl Gated {
    fn new(fail: bool) -> Arc<Self> {
        Arc::new(Self {
            gate: Semaphore::new(0),
            entered: AtomicUsize::new(0),
            fail,
            seen: Mutex::default(),
        })
    }

    fn open(&self, permits: usize) {
        self.gate.add_permits(permits);
    }

    fn entered(&self) -> usize {
        self.entered.load(Ordering::SeqCst)
    }
}

impl BatchHandler for Gated {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(batch.iter().map(StreamMsg::id));
        self.gate.acquire().await.expect("gate open").forget();
        if self.fail {
            Err(HandleError::skip(boom()))
        } else {
            Ok(())
        }
    }
}

/// An endlessly hot fetch: every read returns one entry at once, so only the channel (or a
/// blocked handler) can stop the reader.
struct Hot {
    fetches: Arc<AtomicUsize>,
    next: u64,
}

impl FetchSource for Hot {
    async fn fetch(&mut self) -> Result<Vec<StreamBatch>> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.next += 1;
        Ok(vec![batch(0, vec![m(0, self.next, 0, "Keep", "x")])])
    }

    fn seek(&mut self, _partition: u32, _after: StreamId) -> bool {
        true
    }
}

/// A service-defined "must not skip" error, declared outside the library's own error type.
#[derive(Debug)]
struct Down(&'static str);

impl fmt::Display for Down {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Down {}

fn boom() -> std::io::Error {
    std::io::Error::other("boom")
}

fn id(ms: u64) -> StreamId {
    StreamId::new(ms, 0)
}

fn m(partition: u32, ms: u64, seq: u64, msg_type: &str, body: &str) -> StreamMsg {
    StreamMsg::new(
        StreamId::new(ms, seq),
        partition,
        msg_type,
        Bytes::from(body.to_owned()),
    )
}

fn cfg(topic: &str) -> WorkerConfig {
    WorkerConfig::new(topic, "c2t-consumer")
}

fn inline(topic: &str) -> WorkerConfig {
    WorkerConfig {
        channel_capacity: None,
        ..cfg(topic)
    }
}

fn stops(n: usize) -> Vec<PartitionStop> {
    (0..n).map(|_| PartitionStop::detached()).collect()
}

/// Starts a worker over `parts` (partition, stop) and returns its task.
fn spawn<F: FetchSource, H: BatchHandler>(
    config: WorkerConfig,
    parts: &[(u32, PartitionStop)],
    fetch: F,
    handler: Arc<H>,
    sink: Arc<RecordingSink>,
) -> JoinHandle<std::result::Result<(), WorkerError>> {
    let slots = parts
        .iter()
        .map(|(p, s)| PartitionSlot::new(*p, s.clone()))
        .collect();
    let worker = Worker::new(config, slots, fetch, handler, sink, None).unwrap();
    tokio::spawn(worker.run())
}

async fn until(cond: impl Fn() -> bool) {
    for _ in 0..24_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition not reached");
}

fn error_measurements(topic: &str, policy: &str) -> usize {
    measurements(topic)
        .iter()
        .filter(|x| matches!(x, Measurement::Error { policy: p, .. } if *p == policy))
        .count()
}

fn flushes(sink: &RecordingSink) -> usize {
    sink.events()
        .iter()
        .filter(|e| matches!(e, super::testing::SinkEvent::Flush))
        .count()
}

fn batch_measurements(topic: &str) -> usize {
    measurements(topic)
        .iter()
        .filter(|x| matches!(x, Measurement::Batch { .. }))
        .count()
}

type Logs = Arc<Mutex<Vec<(tracing::Level, String)>>>;

/// Records the message of every event, on this thread, for the test's lifetime.
struct Capture(Logs);

impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        struct Message<'a>(&'a mut String);
        impl tracing::field::Visit for Message<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                if field.name() == "message" {
                    let _ = write!(self.0, "{value:?}");
                }
            }
        }
        let mut text = String::new();
        event.record(&mut Message(&mut text));
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*event.metadata().level(), text));
    }
}

/// Captures log lines on the current thread (the tests run on a current-thread runtime).
fn capture() -> (Logs, tracing::subscriber::DefaultGuard) {
    let logs: Logs = Arc::default();
    let subscriber = tracing_subscriber::registry().with(Capture(logs.clone()));
    (logs, tracing::subscriber::set_default(subscriber))
}

fn logs_at(logs: &Logs, level: tracing::Level) -> Vec<String> {
    logs.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|(l, _)| *l == level)
        .map(|(_, m)| m.clone())
        .collect()
}

// ------------------------------------------------------------------------------------------------
// ErrorHandlingTests: BestEffortTests
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn an_ordinary_exception_logs_once_advances_and_the_next_batch_is_processed() {
    let (logs, _guard) = capture();
    let topic = unique("ordinary");
    let stop = PartitionStop::detached();
    let handler = Probe::new(|n, _| if n == 0 { Err(boom().into()) } else { Ok(()) });
    let fetch = ScriptedFetch::batches([
        batch(0, vec![m(0, 1, 0, "A", "poison")]),
        batch(0, vec![m(0, 2, 0, "A", "good")]),
    ]);
    let sink = Arc::new(RecordingSink::default());
    let task = spawn(
        cfg(&topic),
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| sink.records().len() == 2).await;
    stop.shutdown();
    task.await.unwrap().unwrap();

    // Invoked once per batch: the library does not retry a best-effort failure.
    assert_eq!(handler.calls(), [vec![id(1)], vec![id(2)]]);
    // The position advances past the poison batch, so it is never seen again.
    assert_eq!(sink.records(), [(0, id(1)), (0, id(2))]);
    assert_eq!(error_measurements(&topic, "best_effort"), 1);
    assert!(
        !measurements(&topic)
            .iter()
            .any(|x| matches!(x, Measurement::Error { policy, .. } if *policy == "block")),
        "no backoff is entered for an ordinary failure"
    );
    // One Error line per failure, naming the policy and that it does not retry.
    let errors = logs_at(&logs, tracing::Level::ERROR);
    assert_eq!(
        errors.len(),
        1,
        "one Error line per failure, not one per attempt"
    );
    assert!(errors[0].contains("BestEffort") && errors[0].contains("does not retry"));
}

#[tokio::test]
async fn shutdown_cancellation_from_the_handler_is_not_an_error() {
    let (logs, _guard) = capture();
    let topic = unique("shutdown-err");
    let stop = PartitionStop::detached();
    let s = stop.clone();
    // The handler cancels the host token, then fails: that failure is a shutdown, not a failure.
    let handler = Probe::new(move |_, _| {
        s.shutdown();
        Err(boom().into())
    });
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 2, 0, "A", "x")])]);
    let task = spawn(cfg(&topic), &[(0, stop)], fetch, handler, sink.clone());

    task.await.unwrap().unwrap();

    assert!(
        sink.records().is_empty(),
        "nothing was processed, so nothing is checkpointed; the batch redelivers"
    );
    assert!(
        logs_at(&logs, tracing::Level::ERROR).is_empty(),
        "shutdown is not a handler failure"
    );
    assert_eq!(error_measurements(&topic, "best_effort"), 0);
}

// ------------------------------------------------------------------------------------------------
// ErrorHandlingTests: BlockingRetryTests
// ------------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_service_defined_subclass_blocks_never_advances_and_re_handles_the_same_batch() {
    let (logs, _guard) = capture();
    let topic = unique("block-never");
    let stop = PartitionStop::detached();
    let s = stop.clone();
    // C# cancels inside the 8th backoff delay. The Rust stop is observed by the 9th attempt, which
    // runs before its sleep is needed: the first eight attempts are the same as C#'s.
    let handler = Probe::new(move |n, _| {
        if n == 8 {
            s.shutdown();
        }
        Err(HandleError::block(Down("the ledger is unreachable")))
    });
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 3, 0, "A", "x")])]);
    let task = spawn(
        cfg(&topic),
        &[(0, stop)],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    task.await.unwrap().unwrap();

    let calls = handler.calls();
    assert_eq!(
        calls.len(),
        9,
        "the first attempt plus one per retry, until the stop"
    );
    assert!(
        calls.iter().all(|c| c == &vec![id(3)]),
        "the same batch is re-handled on every attempt"
    );
    assert!(
        sink.records().is_empty(),
        "a blocked partition NEVER advances its position"
    );
    // The stopping attempt is not counted as a block; each of the others is.
    assert_eq!(error_measurements(&topic, "block"), 8);

    let errors = logs_at(&logs, tracing::Level::ERROR);
    assert!(errors[0].contains("BLOCKED") && errors[0].contains("NOT advanced"));
}

#[tokio::test(start_paused = true)]
async fn the_backoff_walks_1_2_4_8_16_30_and_then_holds_at_30_seconds() {
    let (_logs, _guard) = capture();
    let topic = unique("ladder");
    let stop = PartitionStop::detached();
    let s = stop.clone();
    // Nine delays, then the stop is observed by the tenth attempt (see the test above).
    let handler = Probe::new(move |n, _| {
        if n == 9 {
            s.shutdown();
        }
        Err(HandleError::block(Error::transport("XREAD failed")))
    });
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 4, 0, "A", "x")])]);
    let task = spawn(cfg(&topic), &[(0, stop)], fetch, handler.clone(), sink);

    task.await.unwrap().unwrap();

    assert_eq!(handler.calls().len(), 10);
    let gaps: Vec<u64> = handler
        .times()
        .windows(2)
        .map(|w| (w[1] - w[0]).as_secs())
        .collect();
    assert_eq!(gaps, [1, 2, 4, 8, 16, 30, 30, 30, 30]);
}

#[test]
fn the_backoff_schedule_is_capped_and_has_no_attempt_limit() {
    // C# `NextBlockDelayMs` (a mutable millisecond counter) has no Rust counterpart: the Rust
    // schedule is the pure `block_delay(attempt)`, asserted here. C#'s negative-input clamp is
    // unrepresentable for a `usize` attempt.
    let used: Vec<u64> = (0..12).map(|a| block_delay(a).as_secs()).collect();
    assert_eq!(used, [1, 2, 4, 8, 16, 30, 30, 30, 30, 30, 30, 30]);
    assert_eq!(block_delay(0), Duration::from_secs(1));
    assert_eq!(block_delay(usize::MAX), Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn the_block_log_is_rate_limited_rather_than_one_line_per_attempt() {
    let (logs, _guard) = capture();
    let topic = unique("rate");
    let stop = PartitionStop::detached();
    let s = stop.clone();
    const ATTEMPTS: usize = 200;
    let handler = Probe::new(move |n, _| {
        if n == ATTEMPTS {
            s.shutdown();
        }
        Err(HandleError::block(Down("still down")))
    });
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 5, 0, "A", "x")])]);
    let started = Instant::now();
    let task = spawn(cfg(&topic), &[(0, stop)], fetch, handler.clone(), sink);

    task.await.unwrap().unwrap();
    let elapsed_ms = started.elapsed().as_millis();

    assert_eq!(
        handler.calls().len(),
        ATTEMPTS + 1,
        "the retry loop really did run that many times"
    );
    // The C# budget: the first failure, one line per interval of wall clock, one for the partial.
    // Rust rate-limits on the wall-clock `Instant`, not the paused tokio clock, so the paused run
    // shows as the single opening line.
    let allowed = 2 + usize::try_from(elapsed_ms / 30_000).unwrap_or(usize::MAX);
    let errors = logs_at(&logs, tracing::Level::ERROR);
    assert!(!errors.is_empty(), "the first failure is always logged");
    assert!(errors.len() <= allowed);
    assert!(
        errors.len() < ATTEMPTS / 10,
        "a Redis outage must not emit a line per retry"
    );
    assert_eq!(errors.iter().filter(|e| e.contains("BLOCKED")).count(), 1);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_the_backoff_returns_promptly() {
    let (logs, _guard) = capture();
    let topic = unique("cancel-backoff");
    let stop = PartitionStop::detached();
    let handler = Probe::new(|_, _| Err(HandleError::block(Down("down"))));
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 6, 0, "A", "x")])]);
    let task = spawn(
        cfg(&topic),
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| handler.calls().len() == 1).await;
    let stopping = Instant::now();
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert!(
        stopping.elapsed() < block_delay(0),
        "shutdown must not wait out the current backoff delay"
    );
    assert!(
        sink.records().is_empty(),
        "nothing advanced, so the batch redelivers on restart"
    );
    // C# wording is "shutting down while"; the Rust line is the same event.
    assert_eq!(
        logs_at(&logs, tracing::Level::INFO)
            .iter()
            .filter(|l| l.contains("stopping while the partition was blocked"))
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn a_recovered_block_processes_the_same_batch_and_advances() {
    let (logs, _guard) = capture();
    let topic = unique("recovered");
    let stop = PartitionStop::detached();
    let handler = Probe::new(|n, _| {
        if n < 3 {
            Err(HandleError::block(Down("still down")))
        } else {
            Ok(())
        }
    });
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 7, 0, "A", "x")])]);
    let task = spawn(
        cfg(&topic),
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| !sink.records().is_empty()).await;
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert_eq!(
        handler.calls().len(),
        4,
        "three failures then the attempt that succeeds"
    );
    let gaps: Vec<u64> = handler
        .times()
        .windows(2)
        .map(|w| (w[1] - w[0]).as_secs())
        .collect();
    assert_eq!(gaps, [1, 2, 4]);
    // The position advances only once the batch finally succeeds.
    assert_eq!(sink.records(), [(0, id(7))]);
    assert_eq!(
        logs_at(&logs, tracing::Level::INFO)
            .iter()
            .filter(|l| l.contains("partition recovered"))
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn a_different_exception_during_a_block_hands_back_to_the_error_policy() {
    let (logs, _guard) = capture();
    let topic = unique("hand-back");
    let stop = PartitionStop::detached();
    let handler = Probe::new(|n, _| {
        if n == 0 {
            Err(HandleError::block(Down("down")))
        } else {
            Err(HandleError::skip(std::io::Error::other(
                "a different failure",
            )))
        }
    });
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 8, 0, "A", "x")])]);
    let task = spawn(
        cfg(&topic),
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| !sink.records().is_empty()).await;
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert_eq!(
        handler.calls().len(),
        2,
        "the block ends as soon as the failure is no longer un-skippable"
    );
    // BestEffort took over and advanced past the batch.
    assert_eq!(sink.records(), [(0, id(8))]);
    let errors = logs_at(&logs, tracing::Level::ERROR);
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().any(|e| e.contains("BLOCKED")));
    assert!(errors.iter().any(|e| e.contains("BestEffort")));
}

#[tokio::test(start_paused = true)]
async fn the_exception_class_beats_the_error_policy() {
    let (logs, _guard) = capture();
    let topic = unique("class-beats");
    let stop = PartitionStop::detached();
    let s = stop.clone();
    // C# cancels inside the 4th delay; as above, the Rust stop is observed by the 5th attempt.
    let handler = Probe::new(move |n, _| {
        if n == 4 {
            s.shutdown();
        }
        Err(HandleError::block(Down("down")))
    });
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 9, 0, "A", "x")])]);
    // ErrorPolicy.Fail would fault the host for an ordinary failure; a blocking one still blocks.
    let config = WorkerConfig {
        on_error: ErrorPolicy::Fail,
        ..cfg(&topic)
    };
    let task = spawn(config, &[(0, stop)], fetch, handler.clone(), Arc::default());

    let result = task.await.unwrap();

    assert!(
        result.is_ok(),
        "the host is not faulted by a blocking failure"
    );
    assert_eq!(
        handler.calls().len(),
        5,
        "it blocked and retried rather than faulting"
    );
    assert!(
        logs_at(&logs, tracing::Level::ERROR)
            .iter()
            .all(|l| !l.contains("failing the host")),
        "the Fail policy was never reached"
    );
}

#[test]
fn the_base_class_is_public_and_abstract_so_services_can_derive_their_own() {
    // C# `DontIgnoreException` is an abstract base a service subclasses. The Rust counterpart is the
    // `HandleError::block` constructor, which takes any service error type, and the set of library
    // errors that are un-skippable (`Config`, `Transport`).
    assert!(HandleError::block(Down("service-defined")).is_block());
    assert!(HandleError::from(Error::config("bad config")).is_block());
    assert!(HandleError::from(Error::transport("down")).is_block());
    assert!(!HandleError::from(boom()).is_block());
}

// ------------------------------------------------------------------------------------------------
// ErrorHandlingTests: ErrorPolicyTests
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn stop_partition_does_not_advance_and_stops_only_that_partition() {
    let (logs, _guard) = capture();
    let topic = unique("stop-partition");
    let (s0, s1) = (PartitionStop::detached(), PartitionStop::detached());
    // Partition 0 fails on its first batch; partition 1 keeps going. Both share the policy.
    let handler = Probe::new(|_, msgs| {
        if msgs[0].id() == id(20) {
            Ok(())
        } else {
            Err(boom().into())
        }
    });
    let fetch = ScriptedFetch::new([
        Step::Batches(vec![
            batch(0, vec![m(0, 10, 0, "A", "failing")]),
            batch(1, vec![m(1, 20, 0, "A", "a")]),
        ]),
        Step::Batches(vec![batch(0, vec![m(0, 11, 0, "A", "queued")])]),
    ]);
    let sink = Arc::new(RecordingSink::default());
    let config = WorkerConfig {
        on_error: ErrorPolicy::StopPartition,
        ..cfg(&topic)
    };
    let task = spawn(
        config,
        &[(0, s0), (1, s1.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| {
        error_measurements(&topic, "stop_partition") == 1 && sink.records().contains(&(1, id(20)))
    })
    .await;
    s1.shutdown();
    task.await.unwrap().unwrap();

    // The partition stands down after the first failure: its queued batch is never handled.
    let partition0_calls = handler
        .calls()
        .into_iter()
        .filter(|c| c[0] != id(20))
        .count();
    assert_eq!(
        partition0_calls, 1,
        "the partition stands down after the first failure; it does not retry"
    );
    assert!(
        sink.records().iter().all(|(p, _)| *p != 0),
        "StopPartition does not advance"
    );
    // Another partition is entirely unaffected by the one that stopped.
    assert_eq!(sink.records(), [(1, id(20))]);
    let errors = logs_at(&logs, tracing::Level::ERROR);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("StopPartition") && errors[0].contains("NOT advanced"));
}

#[tokio::test]
async fn fail_faults_the_host_with_the_original_exception_and_does_not_advance() {
    let (logs, _guard) = capture();
    let topic = unique("fail");
    let stop = PartitionStop::detached();
    let handler = Probe::new(|_, _| Err(boom().into()));
    let sink = Arc::new(RecordingSink::default());
    let config = WorkerConfig {
        on_error: ErrorPolicy::Fail,
        ..cfg(&topic)
    };
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 11, 0, "A", "x")])]);
    let task = spawn(config, &[(0, stop)], fetch, handler, sink.clone());

    let err = task.await.unwrap().unwrap_err();

    assert!(
        matches!(err, WorkerError::Handler { partition: 0, .. }),
        "{err}"
    );
    assert!(
        err.to_string().contains("boom"),
        "the original exception is the fault"
    );
    assert!(
        sink.records().is_empty(),
        "a faulting host must not checkpoint the batch that killed it"
    );
    let errors = logs_at(&logs, tracing::Level::ERROR);
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("ErrorPolicy.Fail"));
}

// ------------------------------------------------------------------------------------------------
// ErrorHandlingTests: StartupFailFastTests
// ------------------------------------------------------------------------------------------------

#[test]
fn a_startup_configuration_error_fails_fast_rather_than_retrying() {
    // Startup validation: a worker with no partitions is refused at construction, before any read
    // or any backoff. The handler is never reached.
    let handler = Probe::ok();
    let err = Worker::new(
        cfg("startup"),
        Vec::new(),
        ScriptedFetch::new([]),
        handler.clone(),
        Arc::new(RecordingSink::default()),
        None,
    )
    .err()
    .expect("no partitions is refused");

    assert!(matches!(err, Error::InvalidArgument(_)), "{err}");
    assert!(handler.calls().is_empty());
}

// ------------------------------------------------------------------------------------------------
// ConsumerPipelineTests: TypeFilterTests (C# `TypeFilterTests`; the linear/frozen split has no
// Rust counterpart, so the boundary case is not ported)
// ------------------------------------------------------------------------------------------------

const UNIVERSE: [&str; 9] = [
    "A.One", "A.Two", "A.Three", "A.Four", "A.Five", "A.Six", "A.Seven", "A.Eight", "A.Nine",
];

#[test]
fn filter_paths_agree_with_a_naive_oracle() {
    for size in [1, 2, 3, 4, 5, 6, 9] {
        let configured: Vec<String> = UNIVERSE
            .iter()
            .take(size)
            .map(|s| (*s).to_owned())
            .collect();
        let oracle: std::collections::HashSet<&str> = UNIVERSE.iter().take(size).copied().collect();
        let filter = TypeFilter::new(Some(configured.as_slice()));

        assert!(!filter.keeps_everything());
        for candidate in UNIVERSE
            .iter()
            .copied()
            .chain(["A.On", "A.Ones", "", "a.one"])
        {
            assert_eq!(
                filter.matches(candidate),
                oracle.contains(candidate),
                "type '{candidate}' with a filter of {size}"
            );
        }
    }
}

#[test]
fn a_null_filter_keeps_everything() {
    let filter = TypeFilter::new(None);
    assert!(filter.keeps_everything());
    assert!(filter.matches("anything"));
    assert!(filter.matches(""));
}

#[test]
fn an_empty_filter_keeps_everything() {
    let none: &[String] = &[];
    let filter = TypeFilter::new(Some(none));
    assert!(filter.keeps_everything());
    assert!(filter.matches("anything"));
}

#[test]
fn a_filter_of_blanks_keeps_everything() {
    let blanks = [String::new(), "  ".to_owned(), "\t".to_owned()];
    let filter = TypeFilter::new(Some(&blanks[..]));
    assert!(filter.keeps_everything());
    assert!(filter.matches("anything"));
}

#[test]
fn filter_collapses_duplicates_onto_the_linear_path() {
    // Six configured entries, three distinct, plus a blank.
    let configured: Vec<String> = ["A.One", "A.One", "A.Two", "A.Two", "A.Three", "  "]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    let filter = TypeFilter::new(Some(configured.as_slice()));

    assert!(filter.matches("A.One"));
    assert!(filter.matches("A.Three"));
    assert!(!filter.matches("A.Four"));
}

#[test]
fn filter_drops_an_entry_with_no_type_field() {
    let configured = ["A.One".to_owned()];
    let filter = TypeFilter::new(Some(configured.as_slice()));

    // An entry with no `t` field arrives with an empty type, which never matches a filter.
    assert!(!filter.matches(""));
}

// ------------------------------------------------------------------------------------------------
// ConsumerPipelineTests: ConsumerPipelineTests
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn pipeline_delivers_every_message_in_order_and_advances_the_position() {
    let topic = unique("pipeline");
    let stop = PartitionStop::detached();
    let handler = Probe::ok();
    let fetch = ScriptedFetch::batches([
        batch(0, vec![m(0, 1, 0, "Keep", "a"), m(0, 1, 1, "Keep", "b")]),
        batch(0, vec![m(0, 2, 0, "Keep", "c")]),
    ]);
    let sink = Arc::new(RecordingSink::default());
    let task = spawn(
        cfg(&topic),
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| handler.bodies().len() == 3).await;
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert_eq!(handler.bodies(), ["a", "b", "c"]);
    assert_eq!(
        sink.records(),
        [(0, StreamId::new(1, 1)), (0, StreamId::new(2, 0))]
    );
}

#[tokio::test]
async fn a_batch_filtered_to_zero_still_advances_the_position() {
    let topic = unique("filtered-zero");
    let stop = PartitionStop::detached();
    let handler = Probe::ok();
    let config = WorkerConfig {
        filter: Some(vec!["Keep".to_owned()]),
        ..cfg(&topic)
    };
    // Nothing in this read survives the filter.
    let fetch = ScriptedFetch::batches([batch(
        0,
        vec![
            m(0, 5, 0, "Drop", "x"),
            m(0, 5, 1, "Drop", "x"),
            m(0, 5, 2, "Drop", "x"),
        ],
    )]);
    let sink = Arc::new(RecordingSink::default());
    let task = spawn(
        config,
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| sink.records().len() == 1).await;
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert!(
        handler.calls().is_empty(),
        "there was nothing to hand a handler"
    );
    // The position advances past every entry READ, not every entry kept.
    assert_eq!(sink.records(), [(0, StreamId::new(5, 2))]);
    assert_eq!(
        measurements(&topic)
            .iter()
            .filter_map(|x| match x {
                Measurement::Filtered { count, .. } => Some(*count),
                _ => None,
            })
            .sum::<u64>(),
        3
    );
}

#[tokio::test]
async fn a_partially_filtered_batch_advances_past_the_dropped_tail() {
    let topic = unique("partial");
    let stop = PartitionStop::detached();
    let handler = Probe::ok();
    let config = WorkerConfig {
        filter: Some(vec!["Keep".to_owned()]),
        ..cfg(&topic)
    };
    let fetch = ScriptedFetch::batches([batch(
        0,
        vec![
            m(0, 7, 0, "Drop", "dropped"),
            m(0, 7, 1, "Keep", "kept"),
            m(0, 7, 2, "Drop", "dropped"),
            m(0, 7, 3, "Drop", "dropped"),
        ],
    )]);
    let sink = Arc::new(RecordingSink::default());
    let task = spawn(
        config,
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| sink.records().len() == 1).await;
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert_eq!(handler.bodies(), ["kept"]);
    assert_eq!(sink.records(), [(0, StreamId::new(7, 3))]);
}

#[tokio::test(start_paused = true)]
async fn a_full_channel_genuinely_blocks_the_reader() {
    for capacity in [1, 4] {
        let topic = unique("backpressure");
        let stop = PartitionStop::detached();
        let fetches = Arc::new(AtomicUsize::new(0));
        let gate = Gated::new(false);
        let config = WorkerConfig {
            channel_capacity: Some(capacity),
            ..cfg(&topic)
        };
        let fetch = Hot {
            fetches: fetches.clone(),
            next: 0,
        };
        let task = spawn(
            config,
            &[(0, stop.clone())],
            fetch,
            gate.clone(),
            Arc::default(),
        );

        // One batch is held by the handler, `capacity` sit in the channel, one is parked in the send.
        let expected = capacity + 2;
        until(|| fetches.load(Ordering::SeqCst) >= expected).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            fetches.load(Ordering::SeqCst),
            expected,
            "the reader must be blocked on the channel write, not spinning"
        );

        // Releasing the handler frees one slot, which lets exactly one more round trip through.
        gate.open(1);
        until(|| fetches.load(Ordering::SeqCst) > expected).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(fetches.load(Ordering::SeqCst), expected + 1);

        stop.shutdown();
        gate.open(1_000);
        task.await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_the_batches_already_in_flight() {
    let topic = unique("drain");
    let stop = PartitionStop::detached();
    let gate = Gated::new(false);
    let fetch = ScriptedFetch::batches([
        batch(0, vec![m(0, 9, 0, "Keep", "x")]),
        batch(0, vec![m(0, 9, 1, "Keep", "x")]),
        batch(0, vec![m(0, 9, 2, "Keep", "x")]),
    ]);
    let log = fetch.log();
    let sink = Arc::new(RecordingSink::default());
    let config = WorkerConfig {
        channel_capacity: Some(8),
        ..cfg(&topic)
    };
    let task = spawn(
        config,
        &[(0, stop.clone())],
        fetch,
        gate.clone(),
        sink.clone(),
    );

    // All three batches are read (the fourth read is the parked one), while the handler holds the
    // first. Then shut down with the channel full of unprocessed work.
    until(|| log.lock().unwrap_or_else(PoisonError::into_inner).reads >= 4).await;
    stop.shutdown();
    gate.open(10);

    task.await.unwrap().unwrap();

    assert_eq!(
        gate.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len(),
        3,
        "in-flight batches finish rather than being dropped"
    );
    assert_eq!(
        sink.records(),
        [
            (0, StreamId::new(9, 0)),
            (0, StreamId::new(9, 1)),
            (0, StreamId::new(9, 2))
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_is_bounded_by_a_timeout_when_a_handler_will_not_return() {
    let topic = unique("wedged");
    let stop = PartitionStop::detached();
    // A handler that ignores the token entirely.
    let gate = Gated::new(false);
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 11, 0, "Keep", "x")])]);
    let sink = Arc::new(RecordingSink::default());
    let mut task = spawn(cfg(&topic), &[(0, stop.clone())], fetch, gate.clone(), sink);

    until(|| gate.entered() == 1).await;
    stop.shutdown();

    // The drain budget: the worker does not finish while the handler is wedged.
    let budget = tokio::time::timeout(Duration::from_millis(250), &mut task).await;
    assert!(
        budget.is_err(),
        "a wedged handler must not extend shutdown past its budget"
    );

    // Once the handler does come back, the loop finishes on its own.
    gate.open(1);
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("finishes once the handler returns")
        .unwrap()
        .unwrap();
}

// ------------------------------------------------------------------------------------------------
// ConsumerCoreUnitTests
// ------------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_transient_transport_failure_is_retried_rather_than_fatal() {
    // The C# monitor assertions (`StreamLag` state and lag) belong to the host (C4t); the worker's
    // half of the contract is that the group survives the outage and delivers what follows it.
    let (logs, _guard) = capture();
    let topic = unique("transient");
    let stops = stops(2);
    let handler = Probe::ok();
    let fetch = ScriptedFetch::new([
        Step::Fail(Error::Redis(redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "the primary went away",
        )))),
        Step::Batches(vec![batch(0, vec![m(0, 1, 0, "Keep", "after-the-outage")])]),
    ]);
    let sink = Arc::new(RecordingSink::default());
    let parts = [(0, stops[0].clone()), (1, stops[1].clone())];
    let task = spawn(inline(&topic), &parts, fetch, handler.clone(), sink.clone());

    until(|| sink.records().len() == 1).await;
    assert!(
        !task.is_finished(),
        "a transport outage must not end the group"
    );
    assert_eq!(handler.bodies(), ["after-the-outage"]);
    // C# `WaitOutTransportFailureAsync` marks every partition of the shared read blocked for the
    // outage and running again once a fetch succeeds.
    assert_eq!(
        sink.blocked_calls(),
        [(0, true), (1, true), (0, false), (1, false)]
    );

    for s in &stops {
        s.shutdown();
    }
    task.await.unwrap().unwrap();

    assert_eq!(error_measurements(&topic, "transport"), 1);
    assert_eq!(
        logs_at(&logs, tracing::Level::ERROR)
            .iter()
            .filter(|l| l.contains("transient outage"))
            .count(),
        1
    );
}

#[tokio::test]
async fn completing_the_writer_under_a_parked_reader_ends_the_loop_cleanly() {
    // The Rust worker has no writer to complete. The same race is the processor exiting while the
    // reader is parked in the channel send: the reader must be retired, not faulted.
    let topic = unique("writer-race");
    let stop = PartitionStop::detached();
    let fetches = Arc::new(AtomicUsize::new(0));
    let gate = Gated::new(true);
    let config = WorkerConfig {
        channel_capacity: Some(1),
        on_error: ErrorPolicy::StopPartition,
        ..cfg(&topic)
    };
    let fetch = Hot {
        fetches: fetches.clone(),
        next: 0,
    };
    let task = spawn(config, &[(0, stop)], fetch, gate.clone(), Arc::default());

    // The handler holds the first batch, one sits in the channel, one is parked in the send.
    until(|| fetches.load(Ordering::SeqCst) >= 3).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    gate.open(1);
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the loop ends once its only partition has stood down")
        .unwrap();

    assert!(
        result.is_ok(),
        "a stood-down processor means nobody is left to hand a batch to: a stop, not a fault"
    );
}

#[tokio::test(start_paused = true)]
async fn a_filtered_empty_batch_blocks_on_a_failing_flush_instead_of_faulting() {
    let (logs, _guard) = capture();
    let topic = unique("empty-flush");
    let stop = PartitionStop::detached();
    let sink = Arc::new(RecordingSink::failing_flushes(2));
    let config = WorkerConfig {
        persist: PersistMode::SyncBatch,
        filter: Some(vec!["Keep".to_owned()]),
        ..cfg(&topic)
    };
    // Everything in the read is filtered out, but there is a real position to checkpoint.
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 5, 0, "Drop", "x")])]);
    let task = spawn(
        config,
        &[(0, stop.clone())],
        fetch,
        Probe::ok(),
        sink.clone(),
    );

    // Two flushes fail (the partition blocks and retries), the third succeeds and advances.
    until(|| flushes(&sink) >= 3).await;
    assert!(
        !task.is_finished(),
        "a DontIgnore from the flush blocks the partition; it never faults the worker"
    );
    // Each attempt records the same position before its flush; it is never a different one.
    assert!(
        sink.records().iter().all(|r| *r == (0, id(5))),
        "the position still advances past what was read"
    );
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert!(
        logs_at(&logs, tracing::Level::ERROR)
            .iter()
            .any(|l| l.contains("BLOCKED")),
        "the block is the documented behaviour and has to be visible"
    );
}

#[tokio::test(start_paused = true)]
async fn a_blocked_batch_does_not_record_its_retry_period_as_batch_duration() {
    // C# records a duration histogram sample per attempt and asserts its bound. The Rust worker
    // records one batch measurement per attempt (its size); the bound on the duration itself is
    // not observable through the tap, so this asserts the per-attempt sampling.
    let topic = unique("attempt-samples");
    let stop = PartitionStop::detached();
    let handler = Probe::new(|n, _| {
        if n < 2 {
            Err(HandleError::block(Down("the mandatory downstream is down")))
        } else {
            Ok(())
        }
    });
    let sink = Arc::new(RecordingSink::default());
    let fetch = ScriptedFetch::batches([batch(0, vec![m(0, 5, 0, "A", "x")])]);
    let task = spawn(
        cfg(&topic),
        &[(0, stop.clone())],
        fetch,
        handler.clone(),
        sink.clone(),
    );

    until(|| !sink.records().is_empty()).await;
    stop.shutdown();
    task.await.unwrap().unwrap();

    assert_eq!(
        handler.calls().len(),
        3,
        "the blocked batch succeeds on its third attempt"
    );
    assert_eq!(
        batch_measurements(&topic),
        3,
        "every attempt records its own sample"
    );
}

#[test]
fn an_unrequested_key_is_only_claimed_when_it_is_the_only_stream_in_the_reply() {
    fn stream(key: &str, id: &str) -> Value {
        Value::Array(vec![
            Value::BulkString(key.as_bytes().to_vec()),
            Value::Array(vec![Value::Array(vec![
                Value::BulkString(id.as_bytes().to_vec()),
                Value::Array(vec![
                    Value::BulkString(b"b".to_vec()),
                    Value::BulkString(b"x".to_vec()),
                ]),
            ])]),
        ])
    }
    let keys: [&[u8]; 1] = [b"s:t:0"];
    let mine = stream("prefixed:s:t:0", "1-0");
    let foreign = stream("s:other:9", "500-0");

    let alone = parse_all(Value::Array(vec![mine.clone()]), &keys).unwrap();
    assert_eq!(alone.len(), 1);
    assert!(
        matches!(alone[0].key, SliceKey::Requested(0)),
        "a single-stream reply to a single-key read is that key, prefix or not"
    );

    let both = parse_all(Value::Array(vec![mine, foreign]), &keys).unwrap();
    assert_eq!(both.len(), 2);
    assert!(
        both.iter().all(|s| matches!(s.key, SliceKey::Foreign(_))),
        "neither of two unmatched streams can be claimed as the requested one"
    );
}
