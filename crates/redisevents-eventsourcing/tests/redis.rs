//! Ports of the C# `EventRepositoryTests` against a real Redis (skips without Redis, see
//! `require_redis!`). The aggregate is the C# `TestWidget`: one enum over three wire types.
//!
//! Skipped: `Saving_an_unbound_aggregate_is_refused` (an `AggregateRoot` cannot exist without an
//! id; `a_blank_id_is_refused` covers the equivalent), `EventSourcingPerfBenchmarkTests` (4,
//! benchmarks).
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::assert_is_empty,
    clippy::items_after_statements,
    clippy::unused_async_trait_impl,
    clippy::similar_names
)]

use std::sync::Arc;

use bytes::Bytes;
use redis::aio::ConnectionManager;
use redisevents::config::{TopicOptions, TrimMode};
use redisevents::producer::{PublishOptions, StateEvent, StreamStore};
use redisevents::wire::codec::{CODEC_VERSION, decode};
use redisevents::{StreamId, StreamMsg};
use redisevents_eventsourcing::{
    Aggregate, AggregateDecision, AggregateRoot, Error, EventRepository, ExecuteOptions,
    LOAD_PAGE_SIZE, RedisEventRepository, execute, load_or_create, stream_name,
};
use redisevents_projections::EventTypeRegistry;
use redisevents_test_support::{RedisFixture, require_redis};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Created {
    id: String,
    name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Renamed {
    new_name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Ticked {
    sequence: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WidgetEvent {
    Created(Created),
    Renamed(Renamed),
    Ticked(Ticked),
}

#[derive(Default, Debug)]
struct WidgetState {
    name: String,
    ticks: Vec<i32>,
}

struct Widget;

impl Aggregate for Widget {
    type State = WidgetState;
    type Event = WidgetEvent;
    const NAME: &'static str = "TestWidget";

    fn apply(state: &mut WidgetState, event: &WidgetEvent) {
        match event {
            WidgetEvent::Created(e) => state.name.clone_from(&e.name),
            WidgetEvent::Renamed(e) => state.name.clone_from(&e.new_name),
            WidgetEvent::Ticked(e) => state.ticks.push(e.sequence),
        }
    }

    fn encode(
        _: &EventTypeRegistry,
        event: &WidgetEvent,
    ) -> redisevents_eventsourcing::Result<(String, Bytes)> {
        let (wire, body) = match event {
            WidgetEvent::Created(e) => ("widget.created", serde_json::to_vec(e)),
            WidgetEvent::Renamed(e) => ("widget.renamed", serde_json::to_vec(e)),
            WidgetEvent::Ticked(e) => ("widget.ticked", serde_json::to_vec(e)),
        };
        Ok((wire.to_owned(), Bytes::from(body.expect("serializes"))))
    }
}

fn registry() -> EventTypeRegistry {
    EventTypeRegistry::new()
        .register_family::<WidgetEvent, _>(
            &["widget.created", "widget.renamed", "widget.ticked"],
            |wire, body| -> Result<Option<WidgetEvent>, serde_json::Error> {
                Ok(Some(match wire {
                    "widget.created" => WidgetEvent::Created(serde_json::from_slice(body)?),
                    "widget.renamed" => WidgetEvent::Renamed(serde_json::from_slice(body)?),
                    "widget.ticked" => WidgetEvent::Ticked(serde_json::from_slice(body)?),
                    _ => return Ok(None),
                }))
            },
        )
        .unwrap()
}

type Root = AggregateRoot<Widget>;

fn create(id: &str, name: &str) -> Root {
    let mut w = Root::new(id).unwrap();
    w.emit(WidgetEvent::Created(Created {
        id: id.into(),
        name: name.into(),
    }));
    w
}
fn rename(w: &mut Root, name: &str) {
    w.emit(WidgetEvent::Renamed(Renamed {
        new_name: name.into(),
    }));
}
fn tick(w: &mut Root, sequence: i32) {
    w.emit(WidgetEvent::Ticked(Ticked { sequence }));
}

fn topic_options() -> TopicOptions {
    TopicOptions {
        partitions: 1,
        trim: TrimMode::None,
        ..TopicOptions::default()
    }
}

async fn repository(redis: &RedisFixture, hint: &str) -> (RedisEventRepository, String) {
    let topic = redis.new_topic(hint);
    let store =
        StreamStore::new(redis.connect().await, redis.keys(), &topic, topic_options()).unwrap();
    (RedisEventRepository::new(store, registry()), topic)
}

async fn xlen(redis: &RedisFixture, key: &str) -> u64 {
    redis::cmd("XLEN")
        .arg(key)
        .query_async(&mut redis.connect().await.shared())
        .await
        .unwrap()
}

fn state_key(redis: &RedisFixture, topic: &str, id: &str) -> String {
    redis.keys().state(topic, &stream_name("TestWidget", id))
}

/// Decoded entries of a stream plus their raw field names.
async fn entries(conn: &mut ConnectionManager, key: &str) -> Vec<(StreamMsg, Vec<String>)> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .query_async(conn)
        .await
        .unwrap();
    reply
        .ids
        .into_iter()
        .map(|e| {
            let id = StreamId::parse(&e.id).unwrap();
            let mut names = Vec::new();
            let mut pairs = Vec::new();
            for (k, v) in e.map {
                names.push(k.clone());
                pairs.push((
                    k,
                    Bytes::from(redis::from_redis_value::<Vec<u8>>(v).unwrap()),
                ));
            }
            names.sort();
            (decode(id, 0, pairs, CODEC_VERSION).unwrap(), names)
        })
        .collect()
}

#[tokio::test]
async fn save_then_load_round_trips_state_and_version() {
    let redis = require_redis!();
    let (repo, _) = repository(redis, "roundtrip").await;
    let mut widget = create("w-1", "First");

    let version = repo
        .save(&mut widget, None, &PublishOptions::default())
        .await
        .unwrap();

    assert_eq!(version, 1);
    assert_eq!(widget.version(), 1, "commit advances it");
    assert!(widget.pending().is_empty());

    let loaded = repo.load::<Widget>("w-1").await.unwrap().unwrap();
    assert_eq!(loaded.id(), "w-1");
    assert_eq!(loaded.name, "First");
    assert_eq!(loaded.version(), 1);
    assert!(
        loaded.pending().is_empty(),
        "replayed history is not new activity"
    );
}

#[tokio::test]
async fn second_save_appends_and_accumulates_state() {
    let redis = require_redis!();
    let (repo, _) = repository(redis, "second").await;
    let mut created = create("w-2", "First");
    repo.save(&mut created, None, &PublishOptions::default())
        .await
        .unwrap();

    let mut widget = repo.load::<Widget>("w-2").await.unwrap().unwrap();
    rename(&mut widget, "Second");
    tick(&mut widget, 7);
    let version = repo
        .save(&mut widget, None, &PublishOptions::default())
        .await
        .unwrap();

    assert_eq!(version, 3);
    let loaded = repo.load::<Widget>("w-2").await.unwrap().unwrap();
    assert_eq!(loaded.name, "Second");
    assert_eq!(loaded.ticks, [7]);
    assert_eq!(loaded.version(), 3);
}

#[tokio::test]
async fn save_with_nothing_uncommitted_writes_nothing() {
    let redis = require_redis!();
    let (repo, topic) = repository(redis, "nothing").await;
    let mut widget = create("w-3", "First");
    repo.save(&mut widget, None, &PublishOptions::default())
        .await
        .unwrap();

    let version = repo
        .save(&mut widget, None, &PublishOptions::default())
        .await
        .unwrap();

    assert_eq!(version, 1);
    assert_eq!(xlen(redis, &state_key(redis, &topic, "w-3")).await, 1);
}

#[tokio::test]
async fn stale_expected_version_fails_and_writes_nothing() {
    let redis = require_redis!();
    let (repo, topic) = repository(redis, "stale").await;
    let mut created = create("w-4", "First");
    repo.save(&mut created, None, &PublishOptions::default())
        .await
        .unwrap();
    let mut widget = repo.load::<Widget>("w-4").await.unwrap().unwrap();
    rename(&mut widget, "Doomed");

    let err = repo
        .save(&mut widget, Some(5), &PublishOptions::default())
        .await
        .unwrap_err();

    let conflict = err.as_concurrency().expect("a concurrency error");
    assert_eq!(conflict.aggregate_name, "TestWidget");
    assert_eq!(conflict.id, "w-4");
    assert_eq!(conflict.expected_version, 5);
    assert_eq!(
        widget.pending().len(),
        1,
        "nothing was committed, so nothing was cleared"
    );
    assert_eq!(widget.version(), 1);
    assert_eq!(xlen(redis, &state_key(redis, &topic, "w-4")).await, 1);
    assert_eq!(xlen(redis, &redis.keys().stream(&topic, 0, true)).await, 1);
    let reloaded = repo.load::<Widget>("w-4").await.unwrap().unwrap();
    assert_eq!(reloaded.version(), 1);
    assert_eq!(reloaded.name, "First", "the doomed rename never landed");
}

#[tokio::test]
async fn two_concurrent_saves_leave_exactly_one_winner() {
    let redis = require_redis!();
    let topic = redis.new_topic("race");
    let mk = || async {
        RedisEventRepository::new(
            StreamStore::new(redis.connect().await, redis.keys(), &topic, topic_options()).unwrap(),
            registry(),
        )
    };
    let (seeder, left, right) = (mk().await, mk().await, mk().await);
    let mut seed = create("w-5", "First");
    seeder
        .save(&mut seed, None, &PublishOptions::default())
        .await
        .unwrap();

    let mut one = left.load::<Widget>("w-5").await.unwrap().unwrap();
    let mut two = right.load::<Widget>("w-5").await.unwrap().unwrap();
    assert_eq!((one.version(), two.version()), (1, 1));
    rename(&mut one, "From one");
    rename(&mut two, "From two");

    let o = PublishOptions::default();
    let (a, b) = tokio::join!(
        left.save(&mut one, None, &o),
        right.save(&mut two, None, &o)
    );

    let outcomes = [a, b];
    assert_eq!(
        outcomes.iter().filter(|r| r.is_ok()).count(),
        1,
        "one saver must win"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Err(Error::Concurrency(_))))
            .count(),
        1,
        "and the other must lose the version check"
    );
    assert_eq!(xlen(redis, &state_key(redis, &topic, "w-5")).await, 2);
    assert_eq!(xlen(redis, &redis.keys().stream(&topic, 0, true)).await, 2);
}

#[tokio::test]
async fn save_publishes_every_event_to_the_topic_in_order_with_only_the_callers_headers() {
    let redis = require_redis!();
    let (repo, topic) = repository(redis, "publish").await;
    let mut widget = create("w-6", "First");
    rename(&mut widget, "Second");
    tick(&mut widget, 1);
    let mut options = PublishOptions::default();
    options.correlation_id = Some("corr-e2f9".into());
    options.headers = vec![("tenant".into(), "acme".into())];
    repo.save(&mut widget, None, &options).await.unwrap();
    tick(&mut widget, 2);
    repo.save(&mut widget, None, &PublishOptions::default())
        .await
        .unwrap();

    let mut conn = redis.connect().await.shared();
    let published = entries(&mut conn, &redis.keys().stream(&topic, 0, true)).await;
    assert_eq!(published.len(), 4);
    assert!(published.iter().all(|(m, _)| m.partition_key() == "w-6"));
    assert_eq!(
        published
            .iter()
            .map(|(m, _)| m.msg_type())
            .collect::<Vec<_>>(),
        [
            "widget.created",
            "widget.renamed",
            "widget.ticked",
            "widget.ticked"
        ],
        "events are published in the order they were raised, across saves"
    );
    let ids: Vec<_> = published.iter().map(|(m, _)| m.id()).collect();
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
    for (m, _) in &published[..3] {
        assert_eq!(m.correlation_id(), "corr-e2f9");
        assert_eq!(
            m.headers().to_vec().unwrap(),
            [("tenant".to_owned(), "acme".to_owned())],
            "the repository adds no headers of its own"
        );
    }
    assert!(
        published[3].0.headers().is_empty(),
        "no caller headers means no header field at all"
    );

    // The aggregate's own history holds the same events with b and t only: the version is the
    // position, so it needs no key, trace or correlation id.
    let history = entries(&mut conn, &state_key(redis, &topic, "w-6")).await;
    assert_eq!(history.len(), 4);
    for ((h, names), (p, _)) in history.iter().zip(&published) {
        assert_eq!(h.msg_type(), p.msg_type());
        assert_eq!(names, &["b", "t"]);
    }
}

#[tokio::test]
async fn load_of_an_unknown_id_returns_none() {
    let redis = require_redis!();
    let (repo, _) = repository(redis, "unknown").await;
    assert!(repo.load::<Widget>("nobody-home").await.unwrap().is_none());
}

#[tokio::test]
async fn an_aggregate_longer_than_one_page_loads_completely_and_in_order() {
    const BATCHES: i32 = 15;
    const PER_BATCH: i32 = 100;
    const TOTAL: u64 = (BATCHES * PER_BATCH) as u64 + 1;
    assert!(
        TOTAL > LOAD_PAGE_SIZE as u64,
        "the point is to cross a page boundary"
    );
    let redis = require_redis!();
    let (repo, topic) = repository(redis, "paged").await;
    let mut widget = create("w-7", "First");
    repo.save(&mut widget, None, &PublishOptions::default())
        .await
        .unwrap();
    for batch in 0..BATCHES {
        for i in 0..PER_BATCH {
            tick(&mut widget, batch * PER_BATCH + i);
        }
        repo.save(&mut widget, None, &PublishOptions::default())
            .await
            .unwrap();
    }
    assert_eq!(widget.version(), TOTAL);
    assert_eq!(xlen(redis, &state_key(redis, &topic, "w-7")).await, TOTAL);

    let mut loaded = repo.load::<Widget>("w-7").await.unwrap().unwrap();

    assert_eq!(loaded.version(), TOTAL);
    assert_eq!(
        loaded.ticks,
        (0..BATCHES * PER_BATCH).collect::<Vec<_>>(),
        "replay is in stream order, with no gaps"
    );
    rename(&mut loaded, "After the long replay");
    assert_eq!(
        repo.save(&mut loaded, None, &PublishOptions::default())
            .await
            .unwrap(),
        TOTAL + 1
    );
}

#[tokio::test]
async fn load_of_an_unregistered_wire_type_in_history_fails() {
    let redis = require_redis!();
    let topic = redis.new_topic("future");
    let store =
        StreamStore::new(redis.connect().await, redis.keys(), &topic, topic_options()).unwrap();
    let repo = RedisEventRepository::new(
        StreamStore::new(redis.connect().await, redis.keys(), &topic, topic_options()).unwrap(),
        registry(),
    );
    let mut widget = create("w-8", "First");
    repo.save(&mut widget, None, &PublishOptions::default())
        .await
        .unwrap();
    store
        .append_and_publish(
            &stream_name("TestWidget", "w-8"),
            1,
            "w-8",
            &[StateEvent::new(
                Bytes::from_static(b"{}"),
                "widget.from-the-future",
            )],
        )
        .await
        .unwrap()
        .unwrap();

    let err = repo.load::<Widget>("w-8").await.unwrap_err();

    assert!(matches!(err, Error::History(_)));
    assert!(err.to_string().contains("widget.from-the-future"));
}

#[tokio::test]
async fn execute_runs_commands_against_the_real_store() {
    let redis = require_redis!();
    let (repo, _) = repository(redis, "execute").await;
    for sequence in 1..=2 {
        let result = execute(
            &repo,
            "sb-2",
            &ExecuteOptions::default(),
            async |w: &mut Root| {
                tick(w, sequence);
                Ok::<_, std::convert::Infallible>(AggregateDecision::Done(w.ticks.len()))
            },
        )
        .await
        .unwrap();
        assert_eq!(
            (result.value, result.version),
            (sequence as usize, sequence as u64)
        );
    }
}

#[tokio::test]
async fn execute_retries_a_real_conflict_and_reaches_the_expected_total() {
    let redis = require_redis!();
    let (repo, _) = repository(redis, "retry").await;
    let repo = Arc::new(repo);
    let mut seed = create("w-9", "x");
    repo.save(&mut seed, None, &PublishOptions::default())
        .await
        .unwrap();

    {
        let r = repo.clone();
        tokio::spawn(async move { r.load::<Widget>("x").await.ok().map(|_| ()) });
    }
    let tasks: Vec<_> = (0..8)
        .map(|n| {
            let repo = repo.clone();
            tokio::spawn(async move {
                execute(
                    &*repo,
                    "w-9",
                    &ExecuteOptions::default().max_attempts(std::num::NonZeroU32::new(50).unwrap()),
                    async move |w: &mut Root| {
                        tick(w, n);
                        Ok::<_, std::convert::Infallible>(AggregateDecision::Done(()))
                    },
                )
                .await
                .map(|r| r.version)
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().unwrap();
    }

    let loaded = load_or_create::<_, Widget>(&*repo, "w-9").await.unwrap();
    assert_eq!(loaded.version(), 9);
    let mut ticks = loaded.ticks.clone();
    ticks.sort_unstable();
    assert_eq!(
        ticks,
        (0..8).collect::<Vec<_>>(),
        "every command landed exactly once"
    );
}

#[tokio::test]
async fn a_blank_id_is_refused() {
    let redis = require_redis!();
    let (repo, _) = repository(redis, "blank").await;
    assert!(matches!(
        repo.load::<Widget>(" ").await,
        Err(Error::BlankId)
    ));
}
