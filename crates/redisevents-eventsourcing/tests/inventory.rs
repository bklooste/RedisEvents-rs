//! The Inventory sample's pure domain on the aggregate abstraction, wire compatibility with C#,
//! and the C# `EventSourcingEndToEndTests` (save -> real projector -> view). Needs Redis.
//!
//! Skipped: `Consumer_restart_resumes_without_replay` (consumer positions are the core crate's
//! `host_consumes_flushes_on_shutdown_and_resumes_without_replay`; nothing here is specific to
//! event sourcing).
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    clippy::assert_is_empty
)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use inventory_domain::events::{
    InventoryItemCreated, InventoryItemRenamed, ItemsCheckedInToInventory, wire_type,
};
use inventory_domain::{AGGREGATE_NAME, DomainError, InventoryEvent, InventoryItem};
use redisevents::config::{ConsumerOptions, StreamOptions, TopicOptions};
use redisevents::consumer::Streams;
use redisevents::producer::{PublishOptions, StreamStore};
use redisevents::{HandleResult, PartitionContext};
use redisevents_eventsourcing::{
    Aggregate, AggregateDecision, AggregateRoot, CachedAggregate, Error, EventRepository,
    ExecuteError, ExecuteOptions, RedisEventRepository, execute, stream_name,
};
use redisevents_projections::{EventMeta, EventTypeRegistry, Projection, Projector};
use redisevents_test_support::{RedisFixture, require_redis, wait_until_async};
use uuid::Uuid;

/// The marker the command service defines: state and event from the domain crate.
struct Inventory;

impl Aggregate for Inventory {
    type State = InventoryItem;
    type Event = InventoryEvent;
    const NAME: &'static str = AGGREGATE_NAME;

    fn apply(state: &mut InventoryItem, event: &InventoryEvent) {
        state.apply(event);
    }

    fn encode(
        _: &EventTypeRegistry,
        event: &InventoryEvent,
    ) -> redisevents_eventsourcing::Result<(String, Bytes)> {
        Ok((event.wire_type().to_owned(), Bytes::from(event.encode())))
    }
}

fn registry() -> EventTypeRegistry {
    EventTypeRegistry::new()
        .register_family::<InventoryEvent, _>(
            &[
                wire_type::CREATED,
                wire_type::RENAMED,
                wire_type::CHECKED_IN,
                wire_type::REMOVED,
                wire_type::DEACTIVATED,
            ],
            InventoryEvent::decode,
        )
        .unwrap()
}

fn topic_options() -> TopicOptions {
    TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    }
}

async fn store(redis: &RedisFixture, topic: &str) -> StreamStore {
    StreamStore::new(redis.connect().await, redis.keys(), topic, topic_options()).unwrap()
}

async fn repo(redis: &RedisFixture, topic: &str) -> RedisEventRepository {
    RedisEventRepository::new(store(redis, topic).await, registry())
}

const ID: Uuid = Uuid::from_u128(0x3f2b_8c1e_9a4d_4e6f_8b21_5c7d_9e0a_1b2c);

/// What C# writes (System.Text.Json, golden bodies from `inventory-domain/tests/csharp_wire.rs`).
const CSHARP: [(&str, &str); 4] = [
    (
        "inventory.created",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","Name":"Widget"}"#,
    ),
    (
        "inventory.renamed",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","NewName":"Gadget"}"#,
    ),
    (
        "inventory.checked-in",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","Count":7}"#,
    ),
    (
        "inventory.removed",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","Count":3}"#,
    ),
];

async fn raw_entries(redis: &RedisFixture, key: &str) -> Vec<Vec<(String, Vec<u8>)>> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .query_async(&mut redis.connect().await.shared())
        .await
        .unwrap();
    reply
        .ids
        .into_iter()
        .map(|e| {
            let mut fields: Vec<_> = e
                .map
                .into_iter()
                .map(|(k, v)| (k, redis::from_redis_value::<Vec<u8>>(v).unwrap()))
                .collect();
            fields.sort();
            fields
        })
        .collect()
}

// ---- the pure domain on the abstraction ----

#[tokio::test]
async fn the_pure_inventory_domain_runs_through_execute() {
    let redis = require_redis!();
    let topic = redis.new_topic("inv-domain");
    let repo = repo(redis, &topic).await;
    let id = Uuid::new_v4();
    let key = id.to_string();

    let created = execute(
        &repo,
        &key,
        &ExecuteOptions::default(),
        async move |item: &mut AggregateRoot<Inventory>| {
            let event = InventoryItem::create(id, "widget")?;
            item.emit(event);
            Ok::<_, DomainError>(AggregateDecision::Done(()))
        },
    )
    .await
    .unwrap();
    assert_eq!(created.version, 1);

    let checked_in = execute(
        &repo,
        &key,
        &ExecuteOptions::default(),
        async |item: &mut AggregateRoot<Inventory>| {
            let event = item.check_in(5, 1.5)?;
            item.emit(event);
            Ok::<_, DomainError>(AggregateDecision::Done(()))
        },
    )
    .await
    .unwrap();
    assert_eq!(checked_in.version, 2);

    // A refused command: the domain error comes back, nothing is saved.
    let err = execute(
        &repo,
        &key,
        &ExecuteOptions::default(),
        async |item: &mut AggregateRoot<Inventory>| {
            let event = item.remove(0)?;
            item.emit(event);
            Ok::<_, DomainError>(AggregateDecision::Done(()))
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        ExecuteError::Decide(DomainError::InvalidOperation(_))
    ));

    let loaded = repo.load::<Inventory>(&key).await.unwrap().unwrap();
    assert_eq!(loaded.version(), 2);
    assert_eq!(loaded.id(), key);
    assert!(loaded.is_activated());
    assert_eq!(loaded.state().id(), id);
}

#[tokio::test]
async fn a_cached_inventory_item_saves_through_the_domain() {
    let redis = require_redis!();
    let topic = redis.new_topic("inv-cached");
    let repo = repo(redis, &topic).await;
    let id = Uuid::new_v4();
    let key = id.to_string();
    let cache = CachedAggregate::<Inventory>::new();

    for i in 0..3 {
        cache
            .run(
                &repo,
                &key,
                &ExecuteOptions::default().max_attempts(std::num::NonZeroU32::new(2).unwrap()),
                async |item, _| {
                    let event = if item.version() == 0 {
                        InventoryItem::create(id, "widget")?
                    } else {
                        item.check_in(1, 1.0)?
                    };
                    item.emit(event);
                    Ok::<_, DomainError>(AggregateDecision::Done(()))
                },
            )
            .await
            .unwrap_or_else(|e| panic!("run {i}: {e}"));
    }

    let loaded = repo.load::<Inventory>(&key).await.unwrap().unwrap();
    assert_eq!(loaded.version(), 3);
}

// ---- cross-language ----

#[tokio::test]
async fn an_aggregate_written_by_csharp_loads_in_rust_at_the_right_version() {
    let redis = require_redis!();
    let topic = redis.new_topic("xlang-load");
    let repo = repo(redis, &topic).await;
    let key = ID.to_string();
    let state_key = redis.keys().state(&topic, &stream_name("Inventory", &key));

    // Exactly the entries the C# RedisEventRepository writes: fields b and t, in that order, no k.
    let mut conn = redis.connect().await.shared();
    for (wire, body) in CSHARP {
        let _: String = redis::cmd("XADD")
            .arg(&state_key)
            .arg("*")
            .arg("b")
            .arg(body.as_bytes())
            .arg("t")
            .arg(wire)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    let mut item = repo.load::<Inventory>(&key).await.unwrap().unwrap();
    assert_eq!(item.version(), 4, "version is the state stream length");
    assert_eq!(item.state().id(), ID);
    assert!(item.is_activated());

    // And a Rust save on top of the C# history carries the right expected version.
    let event = item.check_in(2, 1.0).unwrap();
    item.emit(event);
    let version = repo
        .save(&mut item, None, &PublishOptions::default())
        .await
        .unwrap();
    assert_eq!(version, 5);
    assert_eq!(raw_entries(redis, &state_key).await.len(), 5);
}

#[tokio::test]
async fn an_aggregate_saved_from_rust_has_the_raw_entries_csharp_writes() {
    let redis = require_redis!();
    let topic = redis.new_topic("xlang-save");
    let repo = repo(redis, &topic).await;
    let key = ID.to_string();
    let mut item = AggregateRoot::<Inventory>::new(&key).unwrap();
    item.emit(InventoryEvent::Created(InventoryItemCreated {
        id: ID,
        name: "Widget".into(),
    }));
    item.emit(InventoryEvent::Renamed(InventoryItemRenamed {
        id: ID,
        new_name: "Gadget".into(),
    }));
    item.emit(InventoryEvent::CheckedIn(ItemsCheckedInToInventory {
        id: ID,
        count: 7,
    }));
    repo.save(&mut item, None, &PublishOptions::default())
        .await
        .unwrap();

    let state = raw_entries(
        redis,
        &redis.keys().state(&topic, &stream_name("Inventory", &key)),
    )
    .await;
    let topic_entries = raw_entries(redis, &redis.keys().stream(&topic, 0, true)).await;
    assert_eq!(state.len(), 3);
    assert_eq!(topic_entries.len(), 3);
    for (i, (wire, body)) in CSHARP[..3].iter().enumerate() {
        let text = |v: &[u8]| String::from_utf8(v.to_vec()).unwrap();
        // State entry: b and t only, never k; body byte-identical to System.Text.Json's.
        assert_eq!(
            state[i]
                .iter()
                .map(|(k, v)| (k.as_str(), text(v)))
                .collect::<Vec<_>>(),
            [("b", (*body).to_owned()), ("t", (*wire).to_owned())]
        );
        // Topic entry: b, t and k (the aggregate id).
        assert_eq!(
            topic_entries[i]
                .iter()
                .map(|(k, v)| (k.as_str(), text(v)))
                .collect::<Vec<_>>(),
            [
                ("b", (*body).to_owned()),
                ("k", key.clone()),
                ("t", (*wire).to_owned())
            ]
        );
    }
}

// ---- EventSourcingEndToEndTests ----

#[derive(Default, Clone, Debug, PartialEq, Eq)]
struct View {
    name: String,
    count: i32,
}

#[derive(Default)]
struct Views(Mutex<HashMap<Uuid, View>>);

impl Views {
    fn get(&self, id: Uuid) -> Option<View> {
        self.0.lock().unwrap().get(&id).cloned()
    }
}

impl Projection<InventoryEvent> for Views {
    async fn handle(
        &self,
        event: &InventoryEvent,
        _: &EventMeta,
        _: &PartitionContext,
    ) -> HandleResult {
        let mut views = self.0.lock().unwrap();
        let view = views.entry(event.id()).or_default();
        match event {
            InventoryEvent::Created(e) => view.name.clone_from(&e.name),
            InventoryEvent::Renamed(e) => view.name.clone_from(&e.new_name),
            InventoryEvent::CheckedIn(e) => view.count += e.count,
            InventoryEvent::Removed(e) => view.count -= e.count,
            InventoryEvent::Deactivated(_) => {}
        }
        Ok(())
    }
}

async fn start_projector(
    redis: &RedisFixture,
    topic: &str,
    views: Arc<Views>,
) -> redisevents::consumer::StreamsHost {
    let mut options = StreamOptions::default();
    options.topics.insert(topic.to_owned(), topic_options());
    let projector = Projector::builder(topic, registry())
        .shared_projection::<InventoryEvent, _>(views)
        .build()
        .unwrap();
    Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(options)
        .topic(topic)
        .options(ConsumerOptions {
            topic: topic.to_owned(),
            consumer: Some(redis.new_consumer("es")),
            persist_interval_ms: 50,
            block_ms: 200,
            ..ConsumerOptions::default()
        })
        .handler(projector)
        .spawn()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn save_flows_through_the_real_projector_into_the_view() {
    let redis = require_redis!();
    let topic = redis.new_topic("e2e");
    let views = Arc::new(Views::default());
    let host = start_projector(redis, &topic, views.clone()).await;
    let repo = repo(redis, &topic).await;
    let key = ID.to_string();

    let mut item = AggregateRoot::<Inventory>::new(&key).unwrap();
    item.emit(InventoryItem::create(ID, "Widget").unwrap());
    repo.save(&mut item, None, &PublishOptions::default())
        .await
        .unwrap();
    let rename = item.change_name("Widget v2").unwrap();
    item.emit(rename);
    let check_in = item.check_in(5, 1.0).unwrap();
    item.emit(check_in);
    repo.save(&mut item, None, &PublishOptions::default())
        .await
        .unwrap();
    let remove = item.remove(2).unwrap();
    item.emit(remove);
    repo.save(&mut item, None, &PublishOptions::default())
        .await
        .unwrap();

    let want = View {
        name: "Widget v2".into(),
        count: 3,
    };
    wait_until_async(
        || {
            let views = views.clone();
            let want = want.clone();
            async move { views.get(ID) == Some(want) }
        },
        Some(Duration::from_secs(15)),
        "the projector to carry every save into the view",
    )
    .await;
    host.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_racing_saves_leave_one_winner_and_the_view_matches() {
    let redis = require_redis!();
    let topic = redis.new_topic("e2e-race");
    let views = Arc::new(Views::default());
    let host = start_projector(redis, &topic, views.clone()).await;
    let repo = repo(redis, &topic).await;
    let key = ID.to_string();

    let mut seed = AggregateRoot::<Inventory>::new(&key).unwrap();
    seed.emit(InventoryItem::create(ID, "Original").unwrap());
    repo.save(&mut seed, None, &PublishOptions::default())
        .await
        .unwrap();

    let mut first = repo.load::<Inventory>(&key).await.unwrap().unwrap();
    let mut second = repo.load::<Inventory>(&key).await.unwrap().unwrap();
    assert_eq!((first.version(), second.version()), (1, 1));
    let a = first.change_name("From first").unwrap();
    first.emit(a);
    let b = second.change_name("From second").unwrap();
    second.emit(b);

    assert_eq!(
        repo.save(&mut first, None, &PublishOptions::default())
            .await
            .unwrap(),
        2,
        "the first save wins the version check"
    );
    let err = repo
        .save(&mut second, None, &PublishOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::Concurrency(c) if c.aggregate_name == "Inventory"));

    wait_until_async(
        || {
            let views = views.clone();
            async move { views.get(ID).is_some_and(|v| v.name == "From first") }
        },
        Some(Duration::from_secs(15)),
        "the projector to catch up to the winning save",
    )
    .await;
    // Give a stray second event time to show up, then check it never did.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(views.get(ID).unwrap().name, "From first");
    host.shutdown().await.unwrap();
}
