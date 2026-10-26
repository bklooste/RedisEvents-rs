//! Ports of `EventProjectorTests`, `EventTypeRegistryTests` and `EventMetaTests`.
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

use common::{EventA, EventB, Rig, json, msg, msg_a};
use redisevents::testing::StreamTestHarness;
use redisevents::{
    Error, HandleError, HandleResult, HeaderBlock, PartitionContext, StreamId, StreamMsg,
};
use redisevents_projections::{EventMeta, EventTypeRegistry, Projection, Projector};

const SAMPLE_A: fn() -> EventA = || EventA { id: "a-1".into() };

#[derive(Default)]
struct Spy {
    a: Mutex<Vec<EventA>>,
    b: Mutex<Vec<EventB>>,
    last_meta: Mutex<Option<EventMeta>>,
}

impl Projection<EventA> for Spy {
    async fn handle(&self, e: &EventA, meta: &EventMeta, _: &PartitionContext) -> HandleResult {
        self.a.lock().unwrap().push(e.clone());
        *self.last_meta.lock().unwrap() = Some(meta.clone());
        Ok(())
    }
}

impl Projection<EventB> for Spy {
    async fn handle(&self, e: &EventB, meta: &EventMeta, _: &PartitionContext) -> HandleResult {
        self.b.lock().unwrap().push(e.clone());
        *self.last_meta.lock().unwrap() = Some(meta.clone());
        Ok(())
    }
}

struct Thrower(fn() -> HandleError);

impl Projection<EventA> for Thrower {
    async fn handle(&self, _: &EventA, _: &EventMeta, _: &PartitionContext) -> HandleResult {
        Err((self.0)())
    }
}

fn registry() -> EventTypeRegistry {
    EventTypeRegistry::new()
        .register_json::<EventA>("event.a")
        .unwrap()
        .register_json::<EventB>("event.b")
        .unwrap()
}

async fn run(projector: &Projector, batch: &[StreamMsg]) -> HandleResult {
    StreamTestHarness::new("orders")
        .deliver_batch(projector, batch)
        .await
}

#[tokio::test]
async fn a_single_bound_projection_receives_the_event_with_correct_meta() {
    let spy = Arc::new(Spy::default());
    let projector = Projector::builder("orders", registry())
        .shared_projection::<EventA, _>(spy.clone())
        .build()
        .unwrap();
    let m = msg(
        "event.a",
        json(&SAMPLE_A()),
        "agg-1",
        "corr-1",
        StreamId::new(1, 0),
    );

    run(&projector, std::slice::from_ref(&m)).await.unwrap();

    assert_eq!(*spy.a.lock().unwrap(), vec![SAMPLE_A()]);
    let meta = spy.last_meta.lock().unwrap().clone().unwrap();
    assert_eq!(meta.partition_key, "agg-1");
    assert_eq!(meta.correlation_id, "corr-1");
    assert_eq!(meta.id, m.id());
}

#[tokio::test]
async fn a_message_with_no_headers_at_all_projects_without_error() {
    let spy = Arc::new(Spy::default());
    let projector = Projector::builder("orders", registry())
        .shared_projection::<EventA, _>(spy.clone())
        .build()
        .unwrap();
    let m = msg_a("event.a", &SAMPLE_A(), "agg-1", StreamId::new(1, 0))
        .with_headers(HeaderBlock::default());

    run(&projector, &[m]).await.unwrap();

    assert_eq!(spy.a.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn a_projection_handling_two_event_types_is_dispatched_for_both() {
    let spy = Arc::new(Spy::default());
    let projector = Projector::builder("orders", registry())
        .shared_projection::<EventA, _>(spy.clone())
        .shared_projection::<EventB, _>(spy.clone())
        .build()
        .unwrap();
    let a = msg_a("event.a", &SAMPLE_A(), "agg-1", StreamId::new(1, 0));
    let b = msg(
        "event.b",
        json(&EventB { id: "b-1".into() }),
        "agg-1",
        "c",
        StreamId::new(1, 1),
    );

    run(&projector, &[a, b]).await.unwrap();

    assert_eq!(*spy.a.lock().unwrap(), vec![SAMPLE_A()]);
    assert_eq!(*spy.b.lock().unwrap(), vec![EventB { id: "b-1".into() }]);
}

#[tokio::test]
async fn two_projection_instances_bound_to_the_same_event_both_receive_it() {
    let (first, second) = (Arc::new(Spy::default()), Arc::new(Spy::default()));
    let projector = Projector::builder("orders", registry())
        .shared_projection::<EventA, _>(first.clone())
        .shared_projection::<EventA, _>(second.clone())
        .build()
        .unwrap();

    run(
        &projector,
        &[msg_a("event.a", &SAMPLE_A(), "agg-1", StreamId::new(1, 0))],
    )
    .await
    .unwrap();

    assert_eq!(first.a.lock().unwrap().len(), 1);
    assert_eq!(second.a.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn an_unregistered_wire_type_is_skipped_with_no_error_and_no_dispatch() {
    let spy = Arc::new(Spy::default());
    let projector = Projector::builder("orders", registry())
        .shared_projection::<EventA, _>(spy.clone())
        .build()
        .unwrap();

    run(
        &projector,
        &[msg_a(
            "event.unknown",
            &SAMPLE_A(),
            "agg-1",
            StreamId::new(1, 0),
        )],
    )
    .await
    .unwrap();

    assert!(spy.a.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_registered_event_with_no_bound_projection_is_skipped_with_no_error() {
    // Only EventB is bound, so an EventA message binds to nothing.
    let spy = Arc::new(Spy::default());
    let projector = Projector::builder("orders", registry())
        .shared_projection::<EventB, _>(spy.clone())
        .build()
        .unwrap();

    run(
        &projector,
        &[msg_a("event.a", &SAMPLE_A(), "agg-1", StreamId::new(1, 0))],
    )
    .await
    .unwrap();

    assert!(spy.b.lock().unwrap().is_empty());
}

#[tokio::test]
async fn an_ordinary_error_propagates_unchanged_and_stops_the_rest_of_the_batch() {
    let spy = Arc::new(Spy::default());
    let projector = Projector::builder("orders", registry())
        .projection::<EventA, _>(Thrower(|| HandleError::skip("boom")))
        .shared_projection::<EventA, _>(spy.clone())
        .build()
        .unwrap();
    let batch = [
        msg_a("event.a", &SAMPLE_A(), "agg-1", StreamId::new(1, 0)),
        msg_a("event.a", &SAMPLE_A(), "agg-1", StreamId::new(1, 1)),
    ];

    let error = run(&projector, &batch).await.unwrap_err();

    assert!(!error.is_block());
    assert_eq!(error.error().to_string(), "boom");
    assert!(spy.a.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_block_error_from_a_projection_propagates_unchanged() {
    let projector = Projector::builder("orders", registry())
        .projection::<EventA, _>(Thrower(|| HandleError::block("blocked")))
        .build()
        .unwrap();

    let error = run(
        &projector,
        &[msg_a("event.a", &SAMPLE_A(), "agg-1", StreamId::new(1, 0))],
    )
    .await
    .unwrap_err();

    assert!(error.is_block());
    assert_eq!(error.error().to_string(), "blocked");
}

#[tokio::test]
async fn an_unregistered_wire_type_is_warned_once_with_topic_and_id_then_debug() {
    let rig = Rig::default();
    let _guard = rig.install();
    let projector = Projector::builder("orders", registry())
        .projection::<EventA, _>(Spy::default())
        .build()
        .unwrap();
    let batch = [
        msg_a("event.unknown", &SAMPLE_A(), "agg-1", StreamId::new(5, 1)),
        msg_a("event.unknown", &SAMPLE_A(), "agg-2", StreamId::new(5, 2)),
        msg_a("event.other", &SAMPLE_A(), "agg-3", StreamId::new(5, 3)),
    ];

    run(&projector, &batch).await.unwrap();

    let events = rig.events();
    let warnings: Vec<_> = events.iter().filter(|e| e.level == "WARN").collect();
    assert_eq!(warnings.len(), 2, "one per distinct unknown wire type");
    for needle in [
        "'event.unknown'",
        "topic=orders",
        "id=5-1",
        "partitionKey=agg-1",
    ] {
        assert!(warnings[0].message().contains(needle), "{needle}");
    }
    assert!(
        warnings[1].message().contains("'event.other'") && warnings[1].message().contains("id=5-3")
    );
    let debugs: Vec<_> = events.iter().filter(|e| e.level == "DEBUG").collect();
    assert_eq!(debugs.len(), 1);
    assert!(
        debugs[0].message().contains("'event.unknown'") && debugs[0].message().contains("id=5-2")
    );
}

#[test]
fn binding_a_type_no_wire_type_decodes_to_is_a_config_error() {
    let registry = EventTypeRegistry::new()
        .register_json::<EventA>("event.a")
        .unwrap();
    let error = Projector::builder("orders", registry)
        .projection::<EventB, _>(Spy::default())
        .build()
        .unwrap_err();
    assert!(matches!(error, Error::Config(_)));
}

// ---- EventTypeRegistryTests ----

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct WidgetCreated {
    id: String,
    name: String,
}
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct WidgetRenamed {
    new_name: String,
}
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct WidgetCreatedV1 {
    id: String,
    name: String,
}
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct WidgetCreatedV2 {
    id: String,
    name: String,
}

fn widget(id: &str, name: &str) -> WidgetCreated {
    WidgetCreated {
        id: id.into(),
        name: name.into(),
    }
}

#[test]
fn register_json_round_trips_an_event_through_encode_and_try_decode() {
    let registry = EventTypeRegistry::new()
        .register_json::<WidgetCreated>("widget.created")
        .unwrap();

    let (wire, body) = registry.encode(&widget("w-1", "Widget")).unwrap();

    assert_eq!(wire, "widget.created");
    let decoded = registry.try_decode(wire, &body).unwrap().unwrap();
    assert_eq!(
        decoded.downcast_ref::<WidgetCreated>(),
        Some(&widget("w-1", "Widget"))
    );
}

#[test]
fn register_with_round_trips_through_a_hand_rolled_serialiser_pair() {
    let registry = EventTypeRegistry::new()
        .register_with::<WidgetCreated>(
            "widget.created",
            |e| Ok(serde_json::to_vec(e)?.into()),
            |b| Ok(serde_json::from_slice(b)?),
        )
        .unwrap();

    let (wire, body) = registry.encode(&widget("w-2", "Gadget")).unwrap();

    let decoded: WidgetCreated = registry.decode(wire, &body).unwrap().unwrap();
    assert_eq!(decoded, widget("w-2", "Gadget"));
}

#[test]
fn registering_the_same_wire_type_twice_is_a_config_error() {
    let registry = EventTypeRegistry::new()
        .register_json::<WidgetCreated>("widget.created")
        .unwrap();
    let error = registry
        .register_json::<WidgetRenamed>("widget.created")
        .unwrap_err();
    assert!(matches!(error, Error::Config(_)));
}

#[test]
fn registering_the_same_type_under_two_wire_types_is_a_config_error() {
    let registry = EventTypeRegistry::new()
        .register_json::<WidgetCreated>("widget.created")
        .unwrap();
    let error = registry
        .register_json::<WidgetCreated>("widget.created.v2")
        .unwrap_err();
    assert!(matches!(error, Error::Config(_)));
}

#[test]
fn a_blank_wire_type_is_a_config_error() {
    let error = EventTypeRegistry::new()
        .register_json::<WidgetCreated>("  ")
        .unwrap_err();
    assert!(matches!(error, Error::Config(_)));
}

#[test]
fn try_decode_with_an_unregistered_wire_type_is_none() {
    let registry = EventTypeRegistry::new()
        .register_json::<WidgetCreated>("widget.created")
        .unwrap();
    assert!(registry.try_decode("unknown.type", b"").unwrap().is_none());
}

#[test]
fn try_decode_of_a_malformed_body_is_an_error_not_none() {
    let registry = EventTypeRegistry::new()
        .register_json::<WidgetCreated>("widget.created")
        .unwrap();
    assert!(registry.try_decode("widget.created", b"{not json").is_err());
}

#[test]
fn encode_of_an_unregistered_type_is_an_invalid_argument_naming_the_type() {
    let registry = EventTypeRegistry::new()
        .register_json::<WidgetCreated>("widget.created")
        .unwrap();
    let error = registry
        .encode(&WidgetRenamed {
            new_name: "x".into(),
        })
        .unwrap_err();
    assert!(matches!(&error, Error::InvalidArgument(m) if m.contains("WidgetRenamed")));
}

#[test]
fn the_wire_type_is_independent_of_the_type_name_across_a_rename() {
    let v1 = EventTypeRegistry::new()
        .register_json::<WidgetCreatedV1>("widget.created")
        .unwrap();
    let v2 = EventTypeRegistry::new()
        .register_json::<WidgetCreatedV2>("widget.created")
        .unwrap();

    let (w1, b1) = v1
        .encode(&WidgetCreatedV1 {
            id: "w-1".into(),
            name: "Widget".into(),
        })
        .unwrap();
    let (w2, b2) = v2
        .encode(&WidgetCreatedV2 {
            id: "w-1".into(),
            name: "Widget".into(),
        })
        .unwrap();

    assert_eq!((w1, w2), ("widget.created", "widget.created"));
    assert_eq!(b1, b2);
    assert!(v1.decode::<WidgetCreatedV1>(w1, &b1).unwrap().is_some());
    assert!(v2.decode::<WidgetCreatedV2>(w2, &b2).unwrap().is_some());
    // The wrong type for the wire string decodes to nothing: the Rust type is what the registry says.
    assert!(v1.decode::<WidgetCreatedV2>(w1, &b1).unwrap().is_none());
}

#[test]
fn a_family_decodes_many_wire_types_to_one_type_and_none_means_unknown() {
    #[derive(Debug, PartialEq)]
    enum Ev {
        Created,
        Renamed,
    }
    let registry = EventTypeRegistry::new()
        .register_family::<Ev, std::convert::Infallible>(&["c", "r"], |wire, _| {
            Ok(match wire {
                "c" => Some(Ev::Created),
                "r" => Some(Ev::Renamed),
                _ => None,
            })
        })
        .unwrap();

    assert_eq!(registry.decode::<Ev>("r", b"").unwrap(), Some(Ev::Renamed));
    assert!(registry.decodes_to::<Ev>());
    assert!(registry.contains("c") && !registry.contains("x"));
}

// ---- EventMetaTests ----

#[test]
fn event_meta_instances_with_the_same_values_are_equal() {
    let id = StreamId::new(1, 0);
    assert_eq!(
        EventMeta::new("agg-1", id, "corr-1"),
        EventMeta::new("agg-1", id, "corr-1")
    );
}

#[test]
fn event_meta_instances_differing_by_id_are_not_equal() {
    assert_ne!(
        EventMeta::new("agg-1", StreamId::new(1, 0), "corr-1"),
        EventMeta::new("agg-1", StreamId::new(1, 1), "corr-1")
    );
}

#[test]
fn event_meta_properties_round_trip_the_constructor_arguments() {
    let meta = EventMeta::new("agg-9", StreamId::new(42, 1), "corr-9");
    assert_eq!(meta.partition_key, "agg-9");
    assert_eq!(meta.id, StreamId::new(42, 1));
    assert_eq!(meta.correlation_id, "corr-9");
}
