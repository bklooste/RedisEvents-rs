//! C2's own unit tests (no Redis): the worker's logic over a scripted fetch, plus the port of C#
//! `FilteredMetricTests`. The bulk C# ports (`ErrorHandlingTests`, `ConsumerPipelineTests`,
//! `ConsumerCoreUnitTests`) are C2t's.
#![allow(
    clippy::unused_async_trait_impl,
    clippy::assert_is_empty,
    clippy::items_after_statements,
    clippy::cast_possible_truncation
)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use time::OffsetDateTime;
use tokio::time::Instant;

use super::testing::{
    Measurement, RecordingSink, ScriptedFetch, SinkEvent, Step, batch, measurements, msg, unique,
};
use super::*;
use crate::consumer::shared::{NoPositions, PartitionStop, ResetSignal};
use crate::consumer::{MessageHandler, PerMessage, Typed, TypedBatchHandler, TypedHandler};
use crate::positions::reset_marker::ResetMarker;
use crate::wire::TypedMsg;

// ------------------------------------------------------------------ helpers

type Script = dyn Fn(usize, &[StreamMsg]) -> HandleResult + Send + Sync;

/// A batch handler that records every call and answers from a script `(call index, batch)`.
struct Scripted {
    calls: Mutex<Vec<Vec<StreamId>>>,
    at: Mutex<Vec<Instant>>,
    script: Box<Script>,
}

use super::super::HandleResult;

impl Scripted {
    fn new(
        script: impl Fn(usize, &[StreamMsg]) -> HandleResult + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::default(),
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

    fn times(&self) -> Vec<Instant> {
        self.at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl BatchHandler for Scripted {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        let n = {
            let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
            calls.push(batch.iter().map(StreamMsg::id).collect());
            calls.len() - 1
        };
        self.at
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Instant::now());
        (self.script)(n, batch)
    }
}

fn boom() -> std::io::Error {
    std::io::Error::other("boom")
}

fn id(ms: u64) -> StreamId {
    StreamId::new(ms, 0)
}

fn cfg(topic: &str) -> WorkerConfig {
    WorkerConfig::new(topic, "c2-consumer")
}

fn inline(topic: &str) -> WorkerConfig {
    WorkerConfig {
        channel_capacity: None,
        ..cfg(topic)
    }
}

struct Rig {
    stops: Vec<PartitionStop>,
    sink: Arc<RecordingSink>,
}

fn start<H: BatchHandler>(
    config: WorkerConfig,
    partitions: &[u32],
    fetch: ScriptedFetch,
    handler: Arc<H>,
    sink: RecordingSink,
    resets: Option<Arc<ResetSignal>>,
) -> (
    Rig,
    tokio::task::JoinHandle<std::result::Result<(), WorkerError>>,
) {
    let stops: Vec<PartitionStop> = partitions
        .iter()
        .map(|_| PartitionStop::detached())
        .collect();
    let sink = Arc::new(sink);
    let slots = partitions
        .iter()
        .zip(&stops)
        .map(|(p, s)| PartitionSlot::new(*p, s.clone()))
        .collect();
    let worker = Worker::new(config, slots, fetch, handler, sink.clone(), resets).unwrap();
    let task = tokio::spawn(worker.run());
    (Rig { stops, sink }, task)
}

impl Rig {
    fn shutdown(&self) {
        for s in &self.stops {
            s.shutdown();
        }
    }
}

async fn settle() {
    // Let spawned tasks run; with a paused clock this also auto-advances timers.
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

async fn wait_for(cond: impl Fn() -> bool) {
    for _ in 0..24_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("condition not reached");
}

// ------------------------------------------------------------------ FilteredMetricTests (port)

/// C# `FilteredMetricTests.A_filtered_batch_records_what_it_dropped`.
#[tokio::test]
async fn a_filtered_batch_records_what_it_dropped() {
    let topic = unique("filtered");
    let handler = Scripted::ok();
    let fetch = ScriptedFetch::batches([batch(
        0,
        vec![
            msg(0, 1, "Keep"),
            msg(0, 2, "Drop"),
            msg(0, 3, "Drop"),
            msg(0, 4, "Drop"),
        ],
    )]);
    let config = WorkerConfig {
        filter: Some(vec!["Keep".into()]),
        ..cfg(&topic)
    };
    let (rig, task) = start(
        config,
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| !rig.sink.records().is_empty()).await;
    rig.shutdown();
    task.await.unwrap().unwrap();

    assert_eq!(
        handler.calls(),
        [vec![id(1)]],
        "only the one matching entry survives"
    );
    let filtered: Vec<_> = measurements(&topic)
        .into_iter()
        .filter(|m| matches!(m, Measurement::Filtered { .. }))
        .collect();
    assert_eq!(
        filtered,
        [Measurement::Filtered {
            topic: topic.clone(),
            consumer: "c2-consumer".into(),
            count: 3
        }],
        "three entries were dropped by the filter, not four and not one"
    );
    assert_eq!(
        rig.sink.records(),
        [(0, id(4))],
        "the position covers the dropped tail"
    );
}

/// C# `FilteredMetricTests.An_unfiltered_batch_records_nothing`.
#[tokio::test]
async fn an_unfiltered_batch_records_nothing() {
    let topic = unique("unfiltered");
    let handler = Scripted::ok();
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A"), msg(0, 2, "B")])]);
    let (rig, task) = start(
        cfg(&topic),
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| !rig.sink.records().is_empty()).await;
    rig.shutdown();
    task.await.unwrap().unwrap();

    assert_eq!(handler.calls(), [vec![id(1), id(2)]]);
    assert!(
        !measurements(&topic)
            .iter()
            .any(|m| matches!(m, Measurement::Filtered { .. })),
        "nothing was filtered, so no series should exist for this topic at all"
    );
}

// ------------------------------------------------------------------ filter

#[test]
fn type_filter_ignores_blanks_and_duplicates_and_drops_untyped_entries() {
    assert!(TypeFilter::new(None).keeps_everything());
    assert!(TypeFilter::new(Some(&[" ".into(), String::new()])).keeps_everything());
    let f = TypeFilter::new(Some(&["A".into(), "A".into(), "B".into()]));
    assert!(f.matches("A") && f.matches("B"));
    assert!(!f.matches("a") && !f.matches(""));
}

#[tokio::test]
async fn a_batch_filtered_to_nothing_still_checkpoints_without_calling_the_handler() {
    let topic = unique("all-filtered");
    let handler = Scripted::ok();
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 7, "X")])]);
    let config = WorkerConfig {
        filter: Some(vec!["Keep".into()]),
        ..inline(&topic)
    };
    let (rig, task) = start(
        config,
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| !rig.sink.records().is_empty()).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert!(handler.calls().is_empty());
    assert_eq!(rig.sink.records(), [(0, id(7))]);
    assert!(
        !measurements(&topic)
            .iter()
            .any(|m| matches!(m, Measurement::Batch { .. } | Measurement::Consumed { .. })),
        "an emptied batch is not a handling sample"
    );
}

// ------------------------------------------------------------------ the error contract

#[tokio::test]
async fn success_records_the_batch_last_and_counts_consumed() {
    for config in [cfg("x"), inline("x")] {
        let topic = unique("ok");
        let config = WorkerConfig {
            topic: topic.as_str().into(),
            ..config
        };
        let fetch = ScriptedFetch::batches([
            batch(0, vec![msg(0, 1, "A"), msg(0, 2, "A")]),
            batch(0, vec![msg(0, 3, "A")]),
        ]);
        let (rig, task) = start(
            config,
            &[0],
            fetch,
            Scripted::ok(),
            RecordingSink::default(),
            None,
        );
        wait_for(|| rig.sink.records().len() == 2).await;
        rig.shutdown();
        task.await.unwrap().unwrap();
        assert_eq!(rig.sink.records(), [(0, id(2)), (0, id(3))]);
        let consumed: u64 = measurements(&topic)
            .iter()
            .map(|m| match m {
                Measurement::Consumed { count, .. } => *count,
                _ => 0,
            })
            .sum();
        assert_eq!(consumed, 3);
    }
}

#[tokio::test]
async fn skip_under_best_effort_advances_and_counts_an_error() {
    let topic = unique("skip");
    let handler = Scripted::new(|n, _| if n == 0 { Err(boom().into()) } else { Ok(()) });
    let fetch = ScriptedFetch::batches([
        batch(0, vec![msg(0, 1, "A")]),
        batch(0, vec![msg(0, 2, "A")]),
    ]);
    let (rig, task) = start(
        cfg(&topic),
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| rig.sink.records().len() == 2).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(rig.sink.records(), [(0, id(1)), (0, id(2))]);
    assert!(measurements(&topic).contains(&Measurement::Error {
        topic: topic.clone(),
        policy: "best_effort",
        exception_type: "io::Error"
    }));
}

#[tokio::test]
async fn skip_under_stop_partition_stops_without_advancing() {
    for config in [cfg("x"), inline("x")] {
        let topic = unique("stop");
        let config = WorkerConfig {
            topic: topic.as_str().into(),
            on_error: ErrorPolicy::StopPartition,
            ..config
        };
        let handler = Scripted::new(|_, _| Err(boom().into()));
        let fetch = ScriptedFetch::batches([
            batch(0, vec![msg(0, 1, "A")]),
            batch(0, vec![msg(0, 2, "A")]),
        ]);
        let (rig, task) = start(
            config,
            &[0],
            fetch,
            handler.clone(),
            RecordingSink::default(),
            None,
        );
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the worker ends by itself")
            .unwrap()
            .unwrap();
        assert!(rig.sink.records().is_empty());
        assert_eq!(
            handler.calls().len(),
            1,
            "nothing after the failing batch is handled"
        );
    }
}

#[tokio::test]
async fn skip_under_fail_faults_the_worker() {
    for config in [cfg("x"), inline("x")] {
        let config = WorkerConfig {
            on_error: ErrorPolicy::Fail,
            ..config
        };
        let fetch = ScriptedFetch::batches([batch(3, vec![msg(3, 1, "A")])]);
        let handler = Scripted::new(|_, _| Err(boom().into()));
        let (rig, task) = start(config, &[3], fetch, handler, RecordingSink::default(), None);
        let err = task.await.unwrap().unwrap_err();
        assert!(
            matches!(err, WorkerError::Handler { partition: 3, .. }),
            "{err}"
        );
        assert!(rig.sink.records().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn block_retries_the_same_batch_on_the_backoff_ladder_then_advances() {
    let topic = unique("block");
    let handler = Scripted::new(|n, _| {
        if n < 4 {
            Err(HandleError::block(boom()))
        } else {
            Ok(())
        }
    });
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A"), msg(0, 2, "A")])]);
    let (rig, task) = start(
        cfg(&topic),
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| !rig.sink.records().is_empty()).await;
    rig.shutdown();
    task.await.unwrap().unwrap();

    let calls = handler.calls();
    assert_eq!(calls.len(), 5);
    assert!(
        calls.iter().all(|c| c == &vec![id(1), id(2)]),
        "the same batch every time"
    );
    let times = handler.times();
    let gaps: Vec<u64> = times.windows(2).map(|w| (w[1] - w[0]).as_secs()).collect();
    assert_eq!(gaps, [1, 2, 4, 8]);
    assert_eq!(
        rig.sink.records(),
        [(0, id(2))],
        "recorded once, after the success"
    );
    let blocks = measurements(&topic)
        .iter()
        .filter(|m| {
            matches!(
                m,
                Measurement::Error {
                    policy: "block",
                    ..
                }
            )
        })
        .count();
    assert_eq!(blocks, 4);
    assert_eq!(gauge(crate::diagnostics::metrics::BLOCKED, &topic), Some(0));
}

fn gauge(name: &str, topic: &str) -> Option<i64> {
    use crate::diagnostics::instruments::{GaugeValue, TagValue, gauge_series};
    gauge_series(name)
        .into_iter()
        .find(|s| s.tags.contains(&("topic", TagValue::Str(topic.to_owned()))))
        .map(|s| match s.value {
            GaugeValue::Int(v) => v,
            GaugeValue::Float(f) => f as i64,
        })
}

#[test]
fn block_delay_is_the_ladder_and_the_last_step_repeats() {
    let secs: Vec<u64> = (0..8).map(|i| block_delay(i).as_secs()).collect();
    assert_eq!(secs, [1, 2, 4, 8, 16, 30, 30, 30]);
}

#[tokio::test(start_paused = true)]
async fn a_blocked_partition_does_not_hold_up_its_siblings() {
    let topic = unique("block-sibling");
    let handler = Scripted::new(|_, b| {
        if b[0].partition() == 0 {
            Err(HandleError::block(boom()))
        } else {
            Ok(())
        }
    });
    let fetch = ScriptedFetch::new([
        Step::Batches(vec![
            batch(0, vec![msg(0, 1, "A")]),
            batch(1, vec![msg(1, 1, "A")]),
        ]),
        Step::Batches(vec![batch(1, vec![msg(1, 2, "A")])]),
    ]);
    let (rig, task) = start(
        cfg(&topic),
        &[0, 1],
        fetch,
        handler,
        RecordingSink::default(),
        None,
    );
    wait_for(|| rig.sink.records().len() == 2).await;
    assert_eq!(gauge(crate::diagnostics::metrics::BLOCKED, &topic), Some(1));
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(rig.sink.records(), [(1, id(1)), (1, id(2))]);
    assert_eq!(gauge(crate::diagnostics::metrics::BLOCKED, &topic), Some(0));
}

#[tokio::test(start_paused = true)]
async fn a_skip_during_a_block_ends_it_under_the_policy() {
    let handler = Scripted::new(|n, _| {
        if n == 0 {
            Err(HandleError::block(boom()))
        } else {
            Err(HandleError::skip(boom()))
        }
    });
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A")])]);
    let (rig, task) = start(
        inline("x"),
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| !rig.sink.records().is_empty()).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(handler.calls().len(), 2);
    assert_eq!(
        rig.sink.records(),
        [(0, id(1))],
        "BestEffort advances past it"
    );
}

#[tokio::test(start_paused = true)]
async fn stopping_while_blocked_ends_without_advancing() {
    let handler = Scripted::new(|_, _| Err(HandleError::block(boom())));
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A")])]);
    let (rig, task) = start(
        cfg("x"),
        &[0],
        fetch,
        handler,
        RecordingSink::default(),
        None,
    );
    tokio::time::sleep(Duration::from_secs(10)).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert!(rig.sink.records().is_empty());
}

#[tokio::test]
async fn a_panic_is_a_skip() {
    let topic = unique("panic");
    let handler = Scripted::new(|n, _| if n == 0 { panic!("kaboom") } else { Ok(()) });
    let fetch = ScriptedFetch::batches([
        batch(0, vec![msg(0, 1, "A")]),
        batch(0, vec![msg(0, 2, "A")]),
    ]);
    let (rig, task) = start(
        cfg(&topic),
        &[0],
        fetch,
        handler,
        RecordingSink::default(),
        None,
    );
    wait_for(|| rig.sink.records().len() == 2).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert!(measurements(&topic).contains(&Measurement::Error {
        topic: topic.clone(),
        policy: "best_effort",
        exception_type: "panic"
    }));
}

// ------------------------------------------------------------------ PerMessage

struct PerMsg {
    seen: Mutex<Vec<StreamId>>,
    fail: Box<dyn Fn(usize, StreamId) -> HandleResult + Send + Sync>,
    calls: AtomicUsize,
}

impl MessageHandler for PerMsg {
    async fn handle(&self, msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(msg.id());
        (self.fail)(n, msg.id())
    }
}

fn per_msg(
    fail: impl Fn(usize, StreamId) -> HandleResult + Send + Sync + 'static,
) -> Arc<PerMessage<PerMsg>> {
    Arc::new(PerMessage(PerMsg {
        seen: Mutex::default(),
        fail: Box::new(fail),
        calls: AtomicUsize::new(0),
    }))
}

#[tokio::test]
async fn per_message_skip_skips_one_message_and_continues() {
    let topic = unique("per-msg-skip");
    let h = per_msg(|_, id| {
        if id.ms == 2 {
            Err(boom().into())
        } else {
            Ok(())
        }
    });
    let fetch = ScriptedFetch::batches([batch(
        0,
        vec![msg(0, 1, "A"), msg(0, 2, "A"), msg(0, 3, "A")],
    )]);
    let (rig, task) = start(
        cfg(&topic),
        &[0],
        fetch,
        h.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| !rig.sink.records().is_empty()).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(*h.0.seen.lock().unwrap(), [id(1), id(2), id(3)]);
    assert_eq!(rig.sink.records(), [(0, id(3))]);
    let errors = measurements(&topic)
        .iter()
        .filter(|m| matches!(m, Measurement::Error { .. }))
        .count();
    assert_eq!(errors, 1);
}

#[tokio::test(start_paused = true)]
async fn per_message_block_resumes_at_the_blocking_message() {
    // Message 2 blocks twice, then succeeds.
    let h = per_msg(|n, id| {
        if id.ms == 2 && n < 3 {
            Err(HandleError::block(boom()))
        } else {
            Ok(())
        }
    });
    let fetch = ScriptedFetch::batches([batch(
        0,
        vec![msg(0, 1, "A"), msg(0, 2, "A"), msg(0, 3, "A")],
    )]);
    let (rig, task) = start(
        cfg("x"),
        &[0],
        fetch,
        h.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| rig.sink.records().len() == 2).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(
        *h.0.seen.lock().unwrap(),
        [id(1), id(2), id(2), id(2), id(3)]
    );
    assert_eq!(
        rig.sink.records(),
        [(0, id(1)), (0, id(3))],
        "message 1 counts as done"
    );
}

#[tokio::test]
async fn per_message_stop_partition_records_what_was_done() {
    let config = WorkerConfig {
        on_error: ErrorPolicy::StopPartition,
        ..cfg("x")
    };
    let h = per_msg(|_, id| {
        if id.ms == 2 {
            Err(boom().into())
        } else {
            Ok(())
        }
    });
    let fetch = ScriptedFetch::batches([batch(
        0,
        vec![msg(0, 1, "A"), msg(0, 2, "A"), msg(0, 3, "A")],
    )]);
    let (rig, task) = start(
        config,
        &[0],
        fetch,
        h.clone(),
        RecordingSink::default(),
        None,
    );
    task.await.unwrap().unwrap();
    assert_eq!(*h.0.seen.lock().unwrap(), [id(1), id(2)]);
    assert_eq!(rig.sink.records(), [(0, id(1))]);
}

#[tokio::test]
async fn per_message_called_directly_behaves_as_best_effort() {
    let h = PerMessage(PerMsg {
        seen: Mutex::default(),
        fail: Box::new(|_, id| if id.ms == 1 { panic!("x") } else { Ok(()) }),
        calls: AtomicUsize::new(0),
    });
    let ctx = PartitionContext::new("t", "c", 0, CancellationToken::new());
    let msgs = [msg(0, 1, "A"), msg(0, 2, "A")];
    assert!(h.handle_batch(&msgs, &ctx).await.is_ok());
    assert_eq!(h.0.calls.load(Ordering::SeqCst), 2);
}

// ------------------------------------------------------------------ persist modes

#[tokio::test(start_paused = true)]
async fn sync_batch_flushes_after_each_batch_and_a_failed_flush_blocks() {
    let config = WorkerConfig {
        persist: PersistMode::SyncBatch,
        ..cfg("x")
    };
    let handler = Scripted::ok();
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A")])]);
    let (rig, task) = start(
        config,
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::failing_flushes(1),
        None,
    );
    wait_for(|| rig.sink.events().len() == 4).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(
        rig.sink.events(),
        [
            SinkEvent::Record(0, id(1)),
            SinkEvent::Flush,
            SinkEvent::Record(0, id(1)),
            SinkEvent::Flush
        ]
    );
    assert_eq!(
        handler.calls().len(),
        1,
        "the retry re-runs only the flush; the handler already succeeded"
    );
}

#[tokio::test]
async fn sync_message_hands_one_message_at_a_time_and_flushes_each() {
    let config = WorkerConfig {
        persist: PersistMode::SyncMessage,
        ..cfg("x")
    };
    let handler = Scripted::ok();
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A"), msg(0, 2, "A")])]);
    let (rig, task) = start(
        config,
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| rig.sink.events().len() == 4).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(handler.calls(), [vec![id(1)], vec![id(2)]]);
    assert_eq!(
        rig.sink.events(),
        [
            SinkEvent::Record(0, id(1)),
            SinkEvent::Flush,
            SinkEvent::Record(0, id(2)),
            SinkEvent::Flush
        ]
    );
}

#[tokio::test]
async fn persist_none_records_nothing() {
    let config = WorkerConfig {
        persist: PersistMode::None,
        ..cfg("x")
    };
    let handler = Scripted::ok();
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A")])]);
    let (rig, task) = start(
        config,
        &[0],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| handler.calls().len() == 1).await;
    settle().await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert!(rig.sink.events().is_empty());
}

// ------------------------------------------------------------------ resets, stop, revoke

#[tokio::test]
async fn a_pending_reset_rewinds_then_seeks_before_the_next_fetch() {
    let resets = Arc::new(ResetSignal::new(4));
    resets.request(ResetMarker {
        partition: 2,
        target: id(5),
        issued_utc: OffsetDateTime::UNIX_EPOCH,
    });
    let fetch = ScriptedFetch::new([]);
    let log = fetch.log();
    let (rig, task) = start(
        cfg("x"),
        &[1, 2],
        fetch,
        Scripted::ok(),
        RecordingSink::default(),
        Some(resets.clone()),
    );
    wait_for(|| log.lock().unwrap().reads == 1).await;
    assert_eq!(log.lock().unwrap().seeks, [(2, id(5))]);
    assert_eq!(rig.sink.events(), [SinkEvent::Rewind(2)]);
    assert!(resets.is_taken(2) && !resets.has_pending());
    rig.shutdown();
    task.await.unwrap().unwrap();
}

/// A handler that waits until released, then succeeds.
struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl BatchHandler for Gate {
    async fn handle_batch(&self, _batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

fn gate() -> Arc<Gate> {
    Arc::new(Gate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    })
}

#[tokio::test]
async fn a_revoked_partition_records_nothing_more_not_even_the_batch_in_flight() {
    for config in [cfg("x"), inline("x")] {
        let g = gate();
        let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A")])]);
        let (rig, task) = start(
            config,
            &[0],
            fetch,
            g.clone(),
            RecordingSink::default(),
            None,
        );
        g.entered.notified().await;
        rig.stops[0].revoke();
        g.release.notify_one();
        task.await.unwrap().unwrap();
        assert!(rig.sink.records().is_empty());
    }
}

#[tokio::test]
async fn a_graceful_stop_lets_the_batch_in_flight_finish_and_record() {
    for config in [cfg("x"), inline("x")] {
        let g = gate();
        let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A")])]);
        let (rig, task) = start(
            config,
            &[0],
            fetch,
            g.clone(),
            RecordingSink::default(),
            None,
        );
        g.entered.notified().await;
        rig.shutdown();
        g.release.notify_one();
        task.await.unwrap().unwrap();
        assert_eq!(rig.sink.records(), [(0, id(1))]);
    }
}

#[tokio::test]
async fn an_error_after_the_stop_fired_is_a_shutdown_not_a_failure() {
    let topic = unique("shutdown-err");
    let stop_seen = Arc::new(tokio::sync::Notify::new());
    struct FailOnCancel(Arc<tokio::sync::Notify>);
    impl BatchHandler for FailOnCancel {
        async fn handle_batch(&self, _b: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
            self.0.notify_one();
            ctx.cancellation().cancelled().await;
            Err(boom().into())
        }
    }
    let fetch = ScriptedFetch::batches([batch(0, vec![msg(0, 1, "A")])]);
    let (rig, task) = start(
        inline(&topic),
        &[0],
        fetch,
        Arc::new(FailOnCancel(stop_seen.clone())),
        RecordingSink::default(),
        None,
    );
    stop_seen.notified().await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert!(rig.sink.records().is_empty());
    assert!(
        !measurements(&topic)
            .iter()
            .any(|m| matches!(m, Measurement::Error { .. }))
    );
}

#[tokio::test]
async fn a_stopped_partition_in_a_pooled_group_is_retired_while_siblings_run() {
    let config = WorkerConfig {
        on_error: ErrorPolicy::StopPartition,
        ..cfg("x")
    };
    let handler = Scripted::new(|_, b| {
        if b[0].partition() == 0 {
            Err(boom().into())
        } else {
            Ok(())
        }
    });
    let fetch = ScriptedFetch::new([
        Step::Batches(vec![
            batch(0, vec![msg(0, 1, "A")]),
            batch(1, vec![msg(1, 1, "A")]),
        ]),
        Step::Batches(vec![
            batch(0, vec![msg(0, 2, "A")]),
            batch(1, vec![msg(1, 2, "A")]),
        ]),
    ]);
    let (rig, task) = start(
        config,
        &[0, 1],
        fetch,
        handler.clone(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| rig.sink.records().len() == 2).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(rig.sink.records(), [(1, id(1)), (1, id(2))]);
    assert_eq!(
        handler
            .calls()
            .iter()
            .filter(|c| c[0] == id(1) || c[0] == id(2))
            .count(),
        3,
        "partition 0 handled once, partition 1 twice"
    );
}

#[tokio::test]
async fn stop_partition_in_an_inline_group_stops_the_whole_loop() {
    let config = WorkerConfig {
        on_error: ErrorPolicy::StopPartition,
        ..inline("x")
    };
    let handler = Scripted::new(|_, b| {
        if b[0].partition() == 1 {
            Err(boom().into())
        } else {
            Ok(())
        }
    });
    let fetch = ScriptedFetch::new([
        Step::Batches(vec![
            batch(0, vec![msg(0, 1, "A")]),
            batch(1, vec![msg(1, 1, "A")]),
        ]),
        Step::Batches(vec![batch(0, vec![msg(0, 2, "A")])]),
    ]);
    let (rig, task) = start(
        config,
        &[0, 1],
        fetch,
        handler,
        RecordingSink::default(),
        None,
    );
    task.await.unwrap().unwrap();
    assert_eq!(rig.sink.records(), [(0, id(1))]);
}

// ------------------------------------------------------------------ fetch failures

fn dropped() -> Error {
    Error::Redis(redis::RedisError::from(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "reset",
    )))
}

#[tokio::test(start_paused = true)]
async fn a_transient_read_failure_is_waited_out_and_a_permanent_one_faults() {
    assert!(is_transient(&dropped()));
    assert!(is_transient(&Error::transport_with("connect", dropped())));
    assert!(!is_transient(&Error::transport("malformed reply")));

    let fetch = ScriptedFetch::new([
        Step::Fail(dropped()),
        Step::Fail(dropped()),
        Step::Batches(vec![batch(0, vec![msg(0, 1, "A")])]),
        Step::Fail(Error::transport("malformed reply")),
    ]);
    let begun = Instant::now();
    let (rig, task) = start(
        cfg("x"),
        &[0],
        fetch,
        Scripted::ok(),
        RecordingSink::default(),
        None,
    );
    let err = task.await.unwrap().unwrap_err();
    assert!(
        matches!(err, WorkerError::Fetch(Error::Transport { .. })),
        "{err}"
    );
    assert!(
        begun.elapsed() >= Duration::from_secs(3),
        "1 s then 2 s of outage back-off"
    );
    assert_eq!(rig.sink.records(), [(0, id(1))]);
}

/// C# #52 `The_read_loop_waits_out_an_unreadable_reply_instead_of_ending`: a reply the client
/// could not read is a garbled connection, not a fault in the stream. The loop backs off and reads
/// again instead of ending the partition while its lease stays renewed.
#[tokio::test(start_paused = true)]
async fn the_read_loop_waits_out_an_unreadable_reply_instead_of_ending() {
    let unreadable = || {
        Error::transport_with(
            "garbled XREAD reply",
            crate::consumer::fetch::UnreadableReply,
        )
    };
    assert!(is_transient(&unreadable()));
    assert!(crate::consumer::fetch::is_unreadable_reply(&unreadable()));
    assert!(!crate::consumer::fetch::is_unreadable_reply(&dropped()));
    assert!(!crate::consumer::fetch::is_unreadable_reply(
        &Error::transport("decode failed")
    ));

    let fetch = ScriptedFetch::new([
        Step::Fail(unreadable()),
        Step::Batches(vec![batch(0, vec![msg(0, 1, "A")])]),
    ]);
    let (rig, task) = start(
        cfg("x"),
        &[0],
        fetch,
        Scripted::ok(),
        RecordingSink::default(),
        None,
    );
    wait_for(|| rig.sink.records() == [(0, id(1))]).await;
    rig.shutdown();
    task.await.unwrap().unwrap();
}

// ------------------------------------------------------------------ Typed

#[derive(serde::Deserialize, Debug, PartialEq)]
struct Order {
    n: u32,
}

struct Orders(Mutex<Vec<u32>>);

impl TypedHandler<Order> for Orders {
    async fn handle(&self, msg: &TypedMsg<Order>, _ctx: &PartitionContext) -> HandleResult {
        self.0.lock().unwrap().push(msg.value.n);
        Ok(())
    }
}

impl TypedBatchHandler<Order> for Orders {
    async fn handle_batch(
        &self,
        batch: &[TypedMsg<Order>],
        _ctx: &PartitionContext,
    ) -> HandleResult {
        self.0
            .lock()
            .unwrap()
            .extend(batch.iter().map(|m| m.value.n));
        Ok(())
    }
}

fn json(ms: u64, body: &'static str) -> StreamMsg {
    StreamMsg::new(
        id(ms),
        0,
        "Order",
        bytes::Bytes::from_static(body.as_bytes()),
    )
}

#[tokio::test]
async fn typed_message_handler_skips_an_undecodable_body() {
    let h = Typed::<_, Order>::new(Orders(Mutex::default()));
    let ctx = PartitionContext::new("t", "c", 0, CancellationToken::new());
    assert!(
        MessageHandler::handle(&h, &json(1, r#"{"n":4}"#), &ctx)
            .await
            .is_ok()
    );
    let err = MessageHandler::handle(&h, &json(2, "nope"), &ctx)
        .await
        .unwrap_err();
    assert!(!err.is_block());
    assert_eq!(*h.inner().0.lock().unwrap(), [4]);
}

#[tokio::test]
async fn typed_batch_handler_drops_undecodable_bodies_and_never_calls_with_nothing() {
    let h = Typed::<_, Order>::new(Orders(Mutex::default()));
    let ctx = PartitionContext::new("t", "c", 0, CancellationToken::new());
    let batch = [json(1, r#"{"n":1}"#), json(2, "bad"), json(3, r#"{"n":3}"#)];
    assert!(BatchHandler::handle_batch(&h, &batch, &ctx).await.is_ok());
    assert!(
        BatchHandler::handle_batch(&h, &[json(4, "bad")], &ctx)
            .await
            .is_ok()
    );
    assert_eq!(*h.inner().0.lock().unwrap(), [1, 3]);
}

// ------------------------------------------------------------------ shape

#[test]
fn worker_construction_rejects_no_or_duplicate_partitions() {
    let make = |parts: Vec<u32>| {
        Worker::new(
            cfg("x"),
            parts
                .into_iter()
                .map(|p| PartitionSlot::new(p, PartitionStop::detached()))
                .collect(),
            ScriptedFetch::new([]),
            Scripted::ok(),
            Arc::new(NoPositions),
            None,
        )
        .map(|_| ())
    };
    assert!(make(vec![]).is_err());
    assert!(make(vec![1, 1]).is_err());
    assert!(make(vec![0, 1]).is_ok());
}

/// The worker future must be spawnable (C4 `tokio::spawn`s it) over C1's real fetch.
#[allow(dead_code)]
fn the_worker_future_is_send<H: BatchHandler>(w: Worker<H, Fetch>) {
    fn send<T: Send>(_: T) {}
    send(w.run());
}

#[test]
fn config_from_options_maps_backpressure_to_the_execution_mode() {
    let mut options = ConsumerOptions {
        topic: "orders".into(),
        on_error: ErrorPolicy::Fail,
        ..ConsumerOptions::default()
    };
    let c = WorkerConfig::from_options(&options, "reader");
    assert_eq!((&*c.topic, &*c.consumer), ("orders", "reader"));
    assert_eq!(c.on_error, ErrorPolicy::Fail);
    assert_eq!(c.channel_capacity, Some(4));
    options.backpressure.enabled = false;
    assert_eq!(
        WorkerConfig::from_options(&options, "reader").channel_capacity,
        None
    );
}
