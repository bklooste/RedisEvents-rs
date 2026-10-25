//! Ports of `BatchProjectionTests` and `ProjectionsTracingTests`.
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    clippy::assert_is_empty,
    clippy::cast_possible_truncation
)]

mod common;

use std::sync::{Arc, Mutex};

use common::{Rig, json, msg, store_down};
use redisevents::testing::StreamTestHarness;
use redisevents::{HandleError, HandleResult, PartitionContext, StreamId, StreamMsg};
use redisevents_projections::{
    BatchError, BatchProjection, EventMeta, EventTypeRegistry, Projection, ProjectionBatch,
    Projector, malformed_events_total,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
struct OrderPlaced {
    id: String,
}
#[derive(Serialize, Deserialize)]
struct OrderNoted {
    id: String,
}

fn registry() -> EventTypeRegistry {
    EventTypeRegistry::new()
        .register_json::<OrderPlaced>("a")
        .unwrap()
        .register_json::<OrderNoted>("b")
        .unwrap()
}

/// `(type, id)` pairs on partition 3, ids `100-0`, `101-0`, ...
fn batch(events: &[(&str, &str)]) -> Vec<StreamMsg> {
    events
        .iter()
        .enumerate()
        .map(|(i, (wire, id))| {
            let body = if *wire == "a" {
                json(&OrderPlaced { id: (*id).into() })
            } else {
                json(&OrderNoted { id: (*id).into() })
            };
            StreamMsg::new(StreamId::new(100 + i as u64, 0), 3, *wire, body)
                .with_partition_key(format!("order-{id}"))
        })
        .collect()
}

type Failure = fn() -> HandleError;

#[derive(Default)]
struct Buffering {
    gate: Option<Arc<Semaphore>>,
    timeline: Mutex<Vec<String>>,
    flushed: Mutex<Vec<Vec<String>>>,
    scopes: Mutex<Vec<ProjectionBatch>>,
    flush_fails: Option<Failure>,
    throw_on: Option<(&'static str, Failure)>,
    cancel_on: Option<(&'static str, CancellationToken)>,
}

impl Buffering {
    async fn buffer(&self, id: &str, meta: &EventMeta) -> HandleResult {
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        let scope = meta.batch.clone().unwrap();
        self.scopes.lock().unwrap().push(scope.clone());
        self.timeline.lock().unwrap().push(format!("handle {id}"));
        if let Some((on, fail)) = &self.throw_on
            && *on == id
        {
            return Err(fail());
        }
        if let Some((on, token)) = &self.cancel_on
            && *on == id
        {
            token.cancel();
            return Err(HandleError::skip("cancelled"));
        }
        scope
            .with_state(self, Vec::<String>::new, |rows| rows.push(id.to_owned()))
            .map_err(HandleError::skip)
    }
}

impl Projection<OrderPlaced> for Buffering {
    async fn handle(
        &self,
        e: &OrderPlaced,
        meta: &EventMeta,
        _: &PartitionContext,
    ) -> HandleResult {
        self.buffer(&e.id, meta).await
    }
}

impl Projection<OrderNoted> for Buffering {
    async fn handle(&self, e: &OrderNoted, meta: &EventMeta, _: &PartitionContext) -> HandleResult {
        self.buffer(&e.id, meta).await
    }
}

impl BatchProjection for Buffering {
    async fn flush(&self, batch: &ProjectionBatch, _: &PartitionContext) -> HandleResult {
        if let Some(fail) = self.flush_fails {
            return Err(fail());
        }
        let rows = batch
            .take_state::<_, Vec<String>>(self)
            .map_err(HandleError::skip)?
            .unwrap_or_default();
        self.timeline
            .lock()
            .unwrap()
            .push(format!("flush {}", rows.join(",")));
        self.flushed.lock().unwrap().push(rows);
        Ok(())
    }
}

impl Buffering {
    fn flushed(&self) -> Vec<Vec<String>> {
        self.flushed.lock().unwrap().clone()
    }
    fn timeline(&self) -> Vec<String> {
        self.timeline.lock().unwrap().clone()
    }
}

fn projector_for(topic: &str, projections: &[&Arc<Buffering>]) -> Projector {
    let mut builder = Projector::builder(topic, registry());
    for p in projections {
        builder = builder
            .batch_projection::<OrderPlaced, _>(Arc::clone(p))
            .batch_projection::<OrderNoted, _>(Arc::clone(p));
    }
    builder.build().unwrap()
}

async fn run(projector: &Projector, batch: &[StreamMsg]) -> HandleResult {
    StreamTestHarness::new("orders")
        .deliver_batch(projector, batch)
        .await
}

fn strings(rows: &[&str]) -> Vec<String> {
    rows.iter().map(|s| (*s).to_owned()).collect()
}

#[tokio::test]
async fn the_flush_runs_once_after_every_event_is_dispatched_and_sees_them_all_in_order() {
    let p = Arc::new(Buffering::default());
    let projector = projector_for("orders", &[&p]);

    run(&projector, &batch(&[("a", "1"), ("a", "2"), ("a", "3")]))
        .await
        .unwrap();

    assert_eq!(
        p.timeline(),
        strings(&["handle 1", "handle 2", "handle 3", "flush 1,2,3"])
    );
    assert_eq!(p.flushed(), vec![strings(&["1", "2", "3"])]);
}

#[tokio::test]
async fn every_event_in_one_batch_shares_one_scope_and_the_next_batch_gets_a_new_one() {
    let p = Arc::new(Buffering::default());
    let projector = projector_for("orders", &[&p]);

    run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap();
    run(&projector, &batch(&[("a", "3")])).await.unwrap();

    let scopes = p.scopes.lock().unwrap().clone();
    assert_eq!(scopes.len(), 3);
    assert_eq!(scopes[0], scopes[1]);
    assert_ne!(scopes[2], scopes[0]);
    assert_eq!(scopes[0].topic(), "orders");
    assert_eq!(scopes[0].partition(), 3);
    assert_eq!(p.flushed(), vec![strings(&["1", "2"]), strings(&["3"])]);
}

#[tokio::test]
async fn the_scope_is_closed_when_the_projector_returns_so_a_kept_reference_cannot_buffer() {
    let p = Arc::new(Buffering::default());
    let projector = projector_for("orders", &[&p]);

    run(&projector, &batch(&[("a", "1")])).await.unwrap();

    let kept = p.scopes.lock().unwrap()[0].clone();
    assert!(kept.is_closed());
    let buffer = kept.with_state(&*p, Vec::<String>::new, |_| ());
    assert!(matches!(buffer, Err(BatchError::Closed { .. })));
    assert!(buffer.unwrap_err().to_string().contains("closed"));
    assert!(
        kept.try_with_state::<_, Vec<String>, _>(&*p, |_| ())
            .is_err()
    );
}

#[derive(Default)]
struct Plain {
    received: Mutex<Vec<String>>,
    saw_batch: Mutex<Vec<bool>>,
}

impl Projection<OrderPlaced> for Plain {
    async fn handle(
        &self,
        e: &OrderPlaced,
        meta: &EventMeta,
        _: &PartitionContext,
    ) -> HandleResult {
        self.received.lock().unwrap().push(e.id.clone());
        self.saw_batch.lock().unwrap().push(meta.batch.is_some());
        Ok(())
    }
}

#[tokio::test]
async fn a_projection_that_does_not_opt_in_is_dispatched_as_before_and_never_flushed() {
    let plain = Arc::new(Plain::default());
    let projector = Projector::builder("orders", registry())
        .projection::<OrderPlaced, _>(Arc::clone(&plain))
        .build()
        .unwrap();

    run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap();

    assert_eq!(*plain.received.lock().unwrap(), strings(&["1", "2"]));
    assert!(
        plain.saw_batch.lock().unwrap().iter().all(|b| *b),
        "scope set on every event"
    );
}

#[derive(Default)]
struct NotesOnly {
    flushes: Mutex<u32>,
}

impl Projection<OrderNoted> for NotesOnly {
    async fn handle(&self, _: &OrderNoted, _: &EventMeta, _: &PartitionContext) -> HandleResult {
        Ok(())
    }
}

impl BatchProjection for NotesOnly {
    async fn flush(&self, _: &ProjectionBatch, _: &PartitionContext) -> HandleResult {
        *self.flushes.lock().unwrap() += 1;
        Ok(())
    }
}

#[tokio::test]
async fn a_batch_projection_handed_no_event_in_the_batch_is_not_flushed() {
    let notes = Arc::new(NotesOnly::default());
    let projector = Projector::builder("orders", registry())
        .batch_projection::<OrderNoted, _>(Arc::clone(&notes))
        .build()
        .unwrap();

    run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap();
    assert_eq!(
        *notes.flushes.lock().unwrap(),
        0,
        "nothing in the batch bound to it"
    );

    run(&projector, &batch(&[("a", "3"), ("b", "x")]))
        .await
        .unwrap();
    assert_eq!(*notes.flushes.lock().unwrap(), 1);
}

#[tokio::test]
async fn two_batch_projections_are_each_flushed_once_with_only_their_own_state() {
    let (first, second) = (
        Arc::new(Buffering::default()),
        Arc::new(Buffering::default()),
    );
    let projector = projector_for("orders", &[&first, &second]);

    run(&projector, &batch(&[("a", "1"), ("b", "x"), ("a", "2")]))
        .await
        .unwrap();

    assert_eq!(first.flushed(), vec![strings(&["1", "x", "2"])]);
    assert_eq!(second.flushed(), vec![strings(&["1", "x", "2"])]);
    assert!(first.timeline().last().unwrap().starts_with("flush"));
}

#[tokio::test]
async fn concurrent_batches_through_two_projectors_sharing_one_projection_keep_separate_state() {
    let gate = Arc::new(Semaphore::new(0));
    let p = Arc::new(Buffering {
        gate: Some(Arc::clone(&gate)),
        ..Buffering::default()
    });
    let left = projector_for("left", &[&p]);
    let right = projector_for("right", &[&p]);
    let (lb, rb) = (
        batch(&[("a", "L1"), ("a", "L2")]),
        batch(&[("a", "R1"), ("a", "R2")]),
    );

    let (l, r, ()) = tokio::join!(run(&left, &lb), run(&right, &rb), async {
        tokio::task::yield_now().await;
        gate.add_permits(4);
    });
    l.unwrap();
    r.unwrap();

    let mut flushed = p.flushed();
    flushed.sort();
    assert_eq!(
        flushed,
        vec![strings(&["L1", "L2"]), strings(&["R1", "R2"])]
    );
}

#[tokio::test]
async fn a_block_from_the_flush_propagates_unchanged_so_core_blocks_the_batch() {
    let p = Arc::new(Buffering {
        flush_fails: Some(store_down),
        ..Buffering::default()
    });
    let projector = projector_for("orders", &[&p]);

    let error = run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap_err();

    assert!(error.is_block());
    let scopes = p.scopes.lock().unwrap().clone();
    assert!(
        scopes.iter().all(ProjectionBatch::is_closed),
        "a retry starts over in a new scope"
    );
}

#[tokio::test]
async fn an_ordinary_error_from_one_flush_still_flushes_the_others_then_propagates_unchanged() {
    let failing = Arc::new(Buffering {
        flush_fails: Some(|| HandleError::skip("row rejected")),
        ..Buffering::default()
    });
    let healthy = Arc::new(Buffering::default());
    let projector = projector_for("orders", &[&failing, &healthy]);

    let error = run(&projector, &batch(&[("a", "1")])).await.unwrap_err();

    assert!(!error.is_block());
    assert_eq!(error.error().to_string(), "row rejected");
    assert_eq!(
        healthy.flushed().len(),
        1,
        "core skips the batch, so the other work must land"
    );
}

#[tokio::test]
async fn a_block_from_one_flush_skips_the_remaining_flushes_because_the_batch_redelivers() {
    let failing = Arc::new(Buffering {
        flush_fails: Some(store_down),
        ..Buffering::default()
    });
    let later = Arc::new(Buffering::default());
    let projector = projector_for("orders", &[&failing, &later]);

    let error = run(&projector, &batch(&[("a", "1")])).await.unwrap_err();

    assert!(error.is_block());
    assert!(later.flushed().is_empty());
}

#[tokio::test]
async fn an_ordinary_error_from_an_event_flushes_the_events_before_it_then_returns_the_original() {
    let p = Arc::new(Buffering {
        throw_on: Some(("2", || HandleError::skip("bad body"))),
        ..Buffering::default()
    });
    let projector = projector_for("orders", &[&p]);

    let error = run(&projector, &batch(&[("a", "1"), ("a", "2"), ("a", "3")]))
        .await
        .unwrap_err();

    assert_eq!(error.error().to_string(), "bad body");
    assert_eq!(p.flushed(), vec![strings(&["1"])]);
}

#[tokio::test]
async fn a_block_from_an_event_is_returned_without_a_flush() {
    let p = Arc::new(Buffering {
        throw_on: Some(("2", store_down)),
        ..Buffering::default()
    });
    let projector = projector_for("orders", &[&p]);

    let error = run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap_err();

    assert!(error.is_block());
    assert!(
        p.flushed().is_empty(),
        "the whole batch is redelivered; flushing part is wasted work"
    );
}

#[tokio::test]
async fn shutdown_cancellation_from_an_event_is_returned_without_a_flush() {
    let token = CancellationToken::new();
    let p = Arc::new(Buffering {
        cancel_on: Some(("2", token.clone())),
        ..Buffering::default()
    });
    let projector = projector_for("orders", &[&p]);
    let harness = StreamTestHarness::new("orders").with_cancellation(token);

    let result = harness
        .deliver_batch(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await;

    assert!(result.is_err());
    assert!(p.flushed().is_empty());
}

#[tokio::test]
async fn when_an_event_fails_ordinarily_and_the_flush_finds_the_store_down_the_batch_blocks() {
    let p = Arc::new(Buffering {
        throw_on: Some(("2", || HandleError::skip("bad body"))),
        flush_fails: Some(store_down),
        ..Buffering::default()
    });
    let projector = projector_for("orders", &[&p]);

    let error = run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap_err();

    assert!(
        error.is_block(),
        "skipping would advance past event 1's unwritten rows"
    );
}

#[tokio::test]
async fn when_an_event_fails_ordinarily_and_the_flush_also_fails_ordinarily_the_original_is_returned()
 {
    let p = Arc::new(Buffering {
        throw_on: Some(("2", || HandleError::skip("bad body"))),
        flush_fails: Some(|| HandleError::skip("row rejected")),
        ..Buffering::default()
    });
    let projector = projector_for("orders", &[&p]);

    let error = run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap_err();

    assert_eq!(error.error().to_string(), "bad body");
}

#[tokio::test]
async fn a_batch_of_only_a_malformed_entry_is_skipped_without_dispatch_or_flush() {
    let p = Arc::new(Buffering::default());
    let projector = projector_for("orders", &[&p]);
    let poison =
        StreamMsg::new(StreamId::new(1, 0), 3, "a", "not json".into()).with_partition_key("k");

    run(&projector, &[poison]).await.unwrap();

    assert!(p.timeline().is_empty());
}

#[tokio::test]
async fn a_malformed_entry_is_skipped_and_the_rest_of_the_batch_is_still_projected_and_flushed() {
    let rig = Rig::default();
    let _guard = rig.install();
    let buffering = Arc::new(Buffering::default());
    let plain = Arc::new(Plain::default());
    let projector = Projector::builder("orders", registry())
        .batch_projection::<OrderPlaced, _>(Arc::clone(&buffering))
        .projection::<OrderPlaced, _>(Arc::clone(&plain))
        .build()
        .unwrap();
    let mut entries = batch(&[("a", "1"), ("a", "2"), ("a", "3"), ("a", "4")]);
    entries[1] =
        StreamMsg::new(entries[1].id(), 3, "a", "{not json".into()).with_partition_key("k");
    let before = malformed_events_total();

    run(&projector, &entries).await.unwrap();

    assert_eq!(buffering.flushed(), vec![strings(&["1", "3", "4"])]);
    assert_eq!(*plain.received.lock().unwrap(), strings(&["1", "3", "4"]));
    assert!(malformed_events_total() > before);
    let errors: Vec<_> = rig
        .events()
        .into_iter()
        .filter(|e| e.level == "ERROR")
        .collect();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].message().contains("malformed") && errors[0].message().contains("id=101-0"));
}

#[tokio::test]
async fn each_flush_gets_its_own_span_named_for_the_projection() {
    let rig = Rig::default();
    let _guard = rig.install();
    let p = Arc::new(Buffering::default());
    let projector = projector_for("flush-span", &[&p]);

    run(&projector, &batch(&[("a", "1"), ("a", "2")]))
        .await
        .unwrap();

    let flushes: Vec<_> = rig
        .spans()
        .into_iter()
        .filter(|s| s.name == "projections.flush")
        .collect();
    assert_eq!(flushes.len(), 1);
    assert_eq!(
        flushes[0].field("otel.name"),
        Some("projections.flush Buffering")
    );
    assert_eq!(flushes[0].field("projections.topic"), Some("flush-span"));
    assert_eq!(flushes[0].field("projections.partition"), Some("3"));
}

#[test]
fn with_state_creates_once_per_owner_and_keeps_owners_apart() {
    let batch = ProjectionBatch::new("t", 0);
    let (owner_a, owner_b) = (1u8, 2u8);
    let mut created = 0;

    batch
        .with_state(
            &owner_a,
            || {
                created += 1;
                vec![1]
            },
            |v| v.push(2),
        )
        .unwrap();
    batch
        .with_state(
            &owner_a,
            || {
                created += 1;
                vec![9]
            },
            |v| v.push(3),
        )
        .unwrap();
    batch
        .with_state(&owner_b, Vec::<i32>::new, |v| v.push(7))
        .unwrap();

    assert_eq!(created, 1);
    assert_eq!(
        batch
            .try_with_state(&owner_a, |v: &mut Vec<i32>| v.clone())
            .unwrap(),
        Some(vec![1, 2, 3])
    );
    assert_eq!(
        batch
            .try_with_state(&owner_b, |v: &mut Vec<i32>| v.clone())
            .unwrap(),
        Some(vec![7])
    );
    assert_eq!(
        batch
            .try_with_state(&3u8, |v: &mut Vec<i32>| v.len())
            .unwrap(),
        None
    );
}

#[test]
fn asking_for_an_owners_state_as_another_type_is_an_error_not_a_default() {
    let batch = ProjectionBatch::new("t", 0);
    let owner = 1u8;
    batch.with_state(&owner, Vec::<i32>::new, |_| ()).unwrap();

    let error = batch
        .with_state(&owner, Vec::<String>::new, |_| ())
        .unwrap_err();

    assert!(matches!(error, BatchError::WrongType { .. }));
    assert!(error.to_string().contains("Vec"));
    assert!(batch.take_state::<_, Vec<String>>(&owner).is_err());
    assert_eq!(
        batch.take_state::<_, Vec<i32>>(&owner).unwrap(),
        Some(vec![])
    );
}

// ---- ProjectionsTracingTests ----

#[derive(Serialize, Deserialize)]
struct Touched {
    id: String,
}

#[derive(Default)]
struct TouchSpy {
    received: Mutex<usize>,
    fail: Option<Failure>,
}

impl Projection<Touched> for TouchSpy {
    async fn handle(&self, _: &Touched, _: &EventMeta, _: &PartitionContext) -> HandleResult {
        if let Some(fail) = self.fail {
            return Err(fail());
        }
        *self.received.lock().unwrap() += 1;
        Ok(())
    }
}

fn touched(key: &str, ms: u64) -> StreamMsg {
    msg(
        "touched",
        json(&Touched { id: key.into() }),
        key,
        "",
        StreamId::new(ms, 0),
    )
}

fn touch_registry() -> EventTypeRegistry {
    EventTypeRegistry::new()
        .register_json::<Touched>("touched")
        .unwrap()
}

#[tokio::test]
async fn handle_batch_emits_one_project_span_per_dispatched_event() {
    let rig = Rig::default();
    let _guard = rig.install();
    let spy = Arc::new(TouchSpy::default());
    let projector = Projector::builder("orders", touch_registry())
        .projection::<Touched, _>(Arc::clone(&spy))
        .projection::<Touched, _>(Arc::new(TouchSpy::default()))
        .build()
        .unwrap();

    run(&projector, &[touched("k-1", 1), touched("k-1", 2)])
        .await
        .unwrap();

    let spans: Vec<_> = rig
        .spans()
        .into_iter()
        .filter(|s| s.name == "projections.project")
        .collect();
    assert_eq!(
        spans.len(),
        2,
        "one span per dispatched event, not per bound projection"
    );
    assert!(
        spans
            .iter()
            .all(|s| s.field("otel.name") == Some("projections.project touched"))
    );
    assert!(
        spans
            .iter()
            .all(|s| s.field("otel.kind") == Some("internal"))
    );
    assert!(
        spans
            .iter()
            .all(|s| s.field("projections.wire_type") == Some("touched"))
    );
    assert_eq!(spans[0].field("projections.stream_id"), Some("1-0"));
    assert_eq!(spans[1].field("projections.stream_id"), Some("2-0"));
    assert_eq!(spans[0].field("projections.partition_key"), Some("k-1"));
}

#[tokio::test]
async fn a_failing_projection_marks_its_span_as_an_error_and_still_returns_the_error_unchanged() {
    let rig = Rig::default();
    let _guard = rig.install();
    let spy = Arc::new(TouchSpy::default());
    let projector = Projector::builder("orders", touch_registry())
        .projection::<Touched, _>(Arc::new(TouchSpy {
            fail: Some(|| HandleError::skip("boom")),
            ..TouchSpy::default()
        }))
        .projection::<Touched, _>(Arc::clone(&spy))
        .build()
        .unwrap();

    let error = run(&projector, &[touched("k-2", 1), touched("k-2", 2)])
        .await
        .unwrap_err();

    assert_eq!(error.error().to_string(), "boom");
    assert_eq!(
        *spy.received.lock().unwrap(),
        0,
        "the second binder and second event never run"
    );
    let spans: Vec<_> = rig
        .spans()
        .into_iter()
        .filter(|s| s.name == "projections.project")
        .collect();
    assert_eq!(
        spans.len(),
        1,
        "the failure stopped the batch before the second span"
    );
    assert_eq!(spans[0].field("otel.status_code"), Some("ERROR"));
    assert_eq!(spans[0].field("otel.status_description"), Some("boom"));
}

#[tokio::test]
async fn with_no_subscriber_project_still_works() {
    let spy = Arc::new(TouchSpy::default());
    let projector = Projector::builder("orders", touch_registry())
        .projection::<Touched, _>(Arc::clone(&spy))
        .build()
        .unwrap();

    run(&projector, &[touched("k-3", 1)]).await.unwrap();

    assert_eq!(*spy.received.lock().unwrap(), 1);
}
