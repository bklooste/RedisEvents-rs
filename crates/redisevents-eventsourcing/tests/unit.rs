//! Tests of `AggregateRoot`, `ConcurrencyExceptionTests`, `CachedAggregateTests`,
//! `EventRepositoryExtensionsTests` and `EventSourcingTracingTests` (no Redis needed).
//!
//! Skipped: `Raise_with_no_registered_handler_throws`, `Registering_the_same_event_type_twice_throws`,
//! `LoadFromHistory_with_an_unregistered_event_type_throws` (the `On<T>` handler table does not exist:
//! `apply` is a total function over the event type), the three `BindId` tests and
//! `LoadOrCreate_binds_a_loaded_instance...` (the id is a constructor argument, never rebound),
//! `Is_a_plain_exception_and_not_a_dont_ignore_exception` (no such base class; `Error` is not a
//! `HandleError`), `EventSourcingRegistrationTests` (9, DI sugar), the micro and perf benchmarks.
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unused_async,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::assert_is_empty,
    clippy::items_after_statements,
    clippy::unused_async_trait_impl,
    clippy::similar_names,
    clippy::unnecessary_wraps,
    clippy::option_option
)]

mod common;

use std::any::Any;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use bytes::Bytes;
use common::Rig;
use redisevents::producer::{PublishOptions, StateEvent};
use redisevents::{StreamId, StreamMsg};
use redisevents_eventsourcing::{
    Aggregate, AggregateDecision, AggregateRoot, CachedAggregate, ConcurrencyError, Error,
    EventRepository, EventStore, ExecuteError, ExecuteOptions, RedisEventRepository, execute,
    load_or_create,
};
use redisevents_projections::EventTypeRegistry;
use serde::{Deserialize, Serialize};

// ---- the counter aggregate ----

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Incremented {
    by: i32,
}

#[derive(Default, Debug)]
struct CounterState {
    value: i32,
}

struct Counter;

impl Aggregate for Counter {
    type State = CounterState;
    type Event = Incremented;
    const NAME: &'static str = "Counter";

    fn apply(state: &mut CounterState, event: &Incremented) {
        state.value += event.by;
    }
}

fn increment(root: &mut AggregateRoot<Counter>, by: i32) -> i32 {
    root.emit(Incremented { by });
    root.value
}

type Decision = Result<AggregateDecision<i32>, Infallible>;

fn attempts(n: u32) -> std::num::NonZeroU32 {
    std::num::NonZeroU32::new(n).unwrap()
}

fn done(value: i32) -> Decision {
    Ok(AggregateDecision::Done(value))
}

const ID: &str = "counter-1";

// ---- the in-memory repository (the C# FakeEventRepository) ----

#[derive(Default)]
struct FakeRepository {
    streams: Mutex<HashMap<String, Vec<Incremented>>>,
    loads: AtomicUsize,
    saves: AtomicUsize,
    unchecked_saves: AtomicUsize,
    conflicts_to_force: AtomicI64,
    last_expected: Mutex<Option<Option<u64>>>,
}

impl FakeRepository {
    fn conflicts(n: i64) -> Self {
        let r = Self::default();
        r.conflicts_to_force.store(n, Ordering::SeqCst);
        r
    }
    fn loads(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }
    fn saves(&self) -> usize {
        self.saves.load(Ordering::SeqCst)
    }
    fn unchecked(&self) -> usize {
        self.unchecked_saves.load(Ordering::SeqCst)
    }

    fn append<A: Aggregate>(&self, root: &mut AggregateRoot<A>) -> u64 {
        let mut streams = self.streams.lock().unwrap();
        let history = streams.entry(root.id().to_owned()).or_default();
        for event in root.pending() {
            let event = (event as &dyn Any)
                .downcast_ref::<Incremented>()
                .expect("the fake only stores counter events");
            history.push(event.clone());
        }
        root.commit();
        root.version()
    }
}

impl EventRepository for FakeRepository {
    async fn load<A: Aggregate>(
        &self,
        id: &str,
    ) -> redisevents_eventsourcing::Result<Option<AggregateRoot<A>>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        let history = self.streams.lock().unwrap().get(id).cloned();
        let Some(history) = history else {
            return Ok(None);
        };
        AggregateRoot::<A>::from_history(
            id,
            history.into_iter().map(|e| {
                *(Box::new(e) as Box<dyn Any>)
                    .downcast::<A::Event>()
                    .expect("the fake only stores counter events")
            }),
        )
        .map(Some)
    }

    async fn save<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        expected_version: Option<u64>,
        _options: &PublishOptions,
    ) -> redisevents_eventsourcing::Result<u64> {
        self.saves.fetch_add(1, Ordering::SeqCst);
        *self.last_expected.lock().unwrap() = Some(expected_version);
        if self.conflicts_to_force.load(Ordering::SeqCst) > 0 {
            self.conflicts_to_force.fetch_sub(1, Ordering::SeqCst);
            return Err(ConcurrencyError::new(
                A::NAME,
                root.id(),
                expected_version.unwrap_or(root.version()),
            )
            .into());
        }
        Ok(self.append(root))
    }

    async fn save_unchecked<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        _options: &PublishOptions,
    ) -> redisevents_eventsourcing::Result<u64> {
        self.unchecked_saves.fetch_add(1, Ordering::SeqCst);
        Ok(self.append(root))
    }
}

// ---- AggregateRootTests ----

#[test]
fn raise_applies_the_event_immediately_and_records_it_as_uncommitted() {
    let mut counter = AggregateRoot::<Counter>::new(ID).unwrap();

    increment(&mut counter, 3);

    assert_eq!(counter.value, 3);
    assert_eq!(counter.pending(), [Incremented { by: 3 }]);
}

#[test]
fn from_history_applies_every_event_sets_version_and_leaves_nothing_pending() {
    let counter =
        AggregateRoot::<Counter>::from_history(ID, [Incremented { by: 2 }, Incremented { by: 5 }])
            .unwrap();

    assert_eq!(counter.value, 7);
    assert_eq!(counter.version(), 2);
    assert!(counter.pending().is_empty());
}

#[test]
fn commit_advances_version_by_the_pending_count_and_clears_them() {
    let mut counter = AggregateRoot::<Counter>::new(ID).unwrap();
    increment(&mut counter, 1);
    increment(&mut counter, 1);

    counter.commit();

    assert_eq!(counter.version(), 2);
    assert!(counter.pending().is_empty());
}

#[test]
fn a_blank_id_is_rejected() {
    assert!(matches!(
        AggregateRoot::<Counter>::new("  "),
        Err(Error::BlankId)
    ));
}

// ---- ConcurrencyExceptionTests ----

#[test]
fn concurrency_error_carries_the_aggregate_the_id_and_the_expected_version() {
    let e = ConcurrencyError::new("Inventory", "42", 7);
    assert_eq!(e.aggregate_name, "Inventory");
    assert_eq!(e.id, "42");
    assert_eq!(e.expected_version, 7);
}

#[test]
fn concurrency_error_message_names_the_aggregate_the_id_and_the_expected_version() {
    assert_eq!(
        ConcurrencyError::new("Inventory", "42", 7).to_string(),
        "'Inventory' '42': expected version 7, but it has changed."
    );
}

// ---- EventRepositoryExtensionsTests ----

#[tokio::test]
async fn load_or_create_returns_a_new_instance_bound_to_the_id_when_there_is_no_history() {
    let repository = FakeRepository::default();

    let counter = load_or_create::<_, Counter>(&repository, ID).await.unwrap();

    assert_eq!(counter.id(), ID);
    assert_eq!(counter.version(), 0);
}

#[tokio::test]
async fn load_or_create_returns_the_loaded_instance() {
    let repository = FakeRepository::default();
    execute(
        &repository,
        ID,
        &ExecuteOptions::default(),
        async |c: &mut AggregateRoot<Counter>| done(increment(c, 1)),
    )
    .await
    .unwrap();

    let counter = load_or_create::<_, Counter>(&repository, ID).await.unwrap();

    assert_eq!(counter.id(), ID);
    assert_eq!(counter.version(), 1);
}

#[tokio::test]
async fn execute_creates_saves_and_returns_the_result_and_new_version() {
    let repository = FakeRepository::default();

    let result = execute(
        &repository,
        ID,
        &ExecuteOptions::default(),
        async |c: &mut AggregateRoot<Counter>| done(increment(c, 2)),
    )
    .await
    .unwrap();

    assert_eq!((result.value, result.version), (2, 1));
    assert_eq!(repository.saves(), 1);
}

#[tokio::test]
async fn execute_loads_fresh_on_every_call() {
    let repository = FakeRepository::default();
    for _ in 0..2 {
        execute(
            &repository,
            ID,
            &ExecuteOptions::default(),
            async |c: &mut AggregateRoot<Counter>| done(increment(c, 1)),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        repository.loads(),
        2,
        "execute is the uncached runner; CachedAggregate is the cached one"
    );
}

#[tokio::test]
async fn a_decision_that_raises_nothing_is_not_saved() {
    let repository = FakeRepository::default();

    let result = execute(
        &repository,
        ID,
        &ExecuteOptions::default(),
        async |c: &mut AggregateRoot<Counter>| done(c.value),
    )
    .await
    .unwrap();

    assert_eq!((result.value, result.version), (0, 0));
    assert_eq!(repository.saves(), 0);
}

#[tokio::test]
async fn a_lost_version_check_reloads_and_redecides_against_the_new_state() {
    let repository = FakeRepository::default();
    execute(
        &repository,
        ID,
        &ExecuteOptions::default(),
        async |c: &mut AggregateRoot<Counter>| done(increment(c, 1)),
    )
    .await
    .unwrap();
    repository.conflicts_to_force.store(1, Ordering::SeqCst);
    let mut decisions = 0;

    let result = execute(
        &repository,
        ID,
        &ExecuteOptions::default().max_attempts(attempts(3)),
        async |c: &mut AggregateRoot<Counter>| {
            decisions += 1;
            done(increment(c, 1))
        },
    )
    .await
    .unwrap();

    assert_eq!(decisions, 2);
    assert_eq!((result.value, result.version), (2, 2));
}

#[tokio::test]
async fn exhausting_max_attempts_propagates_the_concurrency_error_and_calls_on_conflict_for_each_retried_loss()
 {
    let repository = FakeRepository::conflicts(i64::MAX);
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let hook_seen = seen.clone();
    let options = ExecuteOptions::default()
        .max_attempts(attempts(3))
        .on_conflict(move |attempt, _| {
            hook_seen.lock().unwrap().push(attempt);
            async {}
        });

    let err = execute(
        &repository,
        ID,
        &options,
        async |c: &mut AggregateRoot<Counter>| done(increment(c, 1)),
    )
    .await
    .unwrap_err();

    assert!(err.is_concurrency());
    assert_eq!(repository.saves(), 3);
    assert_eq!(*seen.lock().unwrap(), [1, 2]);
}

#[tokio::test]
async fn a_refusal_saves_nothing_and_returns_its_result() {
    let repository = FakeRepository::default();

    let result = execute(
        &repository,
        ID,
        &ExecuteOptions::default(),
        async |_: &mut AggregateRoot<Counter>| {
            Ok::<_, Infallible>(AggregateDecision::Refused("no"))
        },
    )
    .await
    .unwrap();

    assert_eq!(result.value, "no");
    assert_eq!(repository.saves(), 0);
}

#[tokio::test]
async fn a_refusal_after_raising_fails_and_saves_nothing() {
    let repository = FakeRepository::default();

    let err = execute(
        &repository,
        ID,
        &ExecuteOptions::default(),
        async |c: &mut AggregateRoot<Counter>| {
            increment(c, 1);
            Ok::<_, Infallible>(AggregateDecision::Refused("changed my mind"))
        },
    )
    .await
    .unwrap_err();

    let ExecuteError::Repository(Error::RefusedAfterEmitting { emitted, .. }) = err else {
        panic!("expected RefusedAfterEmitting");
    };
    assert_eq!(emitted, 1);
    assert_eq!(repository.saves(), 0);
}

#[tokio::test]
async fn redo_reloads_and_decides_again_uncounted() {
    let repository = FakeRepository::default();
    let mut calls = 0;

    let result = execute(
        &repository,
        ID,
        &ExecuteOptions::default().max_attempts(attempts(1)),
        async |c: &mut AggregateRoot<Counter>| {
            calls += 1;
            if calls == 1 {
                Ok::<_, Infallible>(AggregateDecision::Redo)
            } else {
                done(increment(c, 1))
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(result.value, 1);
    assert_eq!(repository.loads(), 2);
}

#[tokio::test]
async fn without_a_concurrency_check_the_unchecked_save_is_used() {
    let repository = FakeRepository::conflicts(i64::MAX);

    let result = execute(
        &repository,
        ID,
        &ExecuteOptions::default().concurrency_check(false),
        async |c: &mut AggregateRoot<Counter>| done(increment(c, 1)),
    )
    .await
    .unwrap();

    assert_eq!((result.value, result.version), (1, 1));
    assert_eq!(repository.unchecked(), 1);
    assert_eq!(repository.saves(), 0);
}

#[tokio::test]
async fn a_decide_error_is_returned_and_saves_nothing() {
    let repository = FakeRepository::default();

    let err = execute(
        &repository,
        ID,
        &ExecuteOptions::default(),
        async |_: &mut AggregateRoot<Counter>| Err::<AggregateDecision<()>, _>("rule broken"),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, ExecuteError::Decide("rule broken")));
    assert_eq!(repository.saves(), 0);
}

#[tokio::test]
async fn the_futures_are_send_so_a_command_handler_can_be_spawned() {
    let repository = std::sync::Arc::new(FakeRepository::default());
    let handle = tokio::spawn(async move {
        execute(
            &*repository,
            ID,
            &ExecuteOptions::default(),
            async |c: &mut AggregateRoot<Counter>| done(increment(c, 1)),
        )
        .await
        .map(|r| r.version)
        .ok()
    });
    assert_eq!(handle.await.unwrap(), Some(1));
}

// ---- CachedAggregateTests ----

fn opts() -> ExecuteOptions {
    ExecuteOptions::default()
}

#[tokio::test]
async fn first_run_loads_and_the_next_reuses_the_cached_instance() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new();

    for _ in 0..2 {
        runner
            .run(&repository, ID, &opts(), async |c, _| done(increment(c, 1)))
            .await
            .unwrap();
    }

    assert_eq!(
        repository.loads(),
        1,
        "the second run reuses the cached instance"
    );
    assert_eq!(repository.saves(), 2);
}

#[tokio::test]
async fn a_cached_decision_that_raises_nothing_is_not_saved_but_is_still_cached() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new();

    let result = runner
        .run(&repository, ID, &opts(), async |c, _| done(c.value))
        .await
        .unwrap();

    assert_eq!(result, 0);
    assert_eq!(repository.saves(), 0);
    runner
        .run(&repository, ID, &opts(), async |c, _| done(c.value))
        .await
        .unwrap();
    assert_eq!(repository.loads(), 1);
}

#[tokio::test]
async fn a_cached_lost_concurrency_check_reloads_and_redecides_against_the_new_state() {
    let repository = FakeRepository::conflicts(1);
    let runner = CachedAggregate::<Counter>::new();

    let result = runner
        .run(
            &repository,
            ID,
            &opts().max_attempts(attempts(3)),
            async |c, _| done(increment(c, 1)),
        )
        .await
        .unwrap();

    assert_eq!(result, 1);
    assert_eq!(repository.loads(), 2, "the lost check must reload");
    assert_eq!(repository.saves(), 2);
}

#[tokio::test]
async fn a_cached_run_exhausting_max_attempts_lets_the_concurrency_error_propagate() {
    let repository = FakeRepository::conflicts(i64::MAX);
    let runner = CachedAggregate::<Counter>::new();

    let err = runner
        .run(
            &repository,
            ID,
            &opts().max_attempts(attempts(3)),
            async |c, _| done(increment(c, 1)),
        )
        .await
        .unwrap_err();

    assert!(err.is_concurrency());
    assert_eq!(repository.saves(), 3, "exactly max_attempts saves");
}

#[tokio::test]
async fn on_conflict_runs_once_per_lost_attempt_with_its_one_based_number_and_not_on_the_final_loss()
 {
    let repository = FakeRepository::conflicts(i64::MAX);
    let runner = CachedAggregate::<Counter>::new();
    let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
    let hook_seen = seen.clone();
    let options = opts()
        .max_attempts(attempts(3))
        .on_conflict(move |attempt, _| {
            hook_seen.lock().unwrap().push(attempt);
            async {}
        });

    let _ = runner
        .run(&repository, ID, &options, async |c, _| {
            done(increment(c, 1))
        })
        .await
        .unwrap_err();

    assert_eq!(*seen.lock().unwrap(), [1, 2]);
}

#[tokio::test]
async fn a_failed_save_evicts_the_cache_so_the_next_run_reloads() {
    let repository = FakeRepository::conflicts(i64::MAX);
    let runner = CachedAggregate::<Counter>::new();

    runner
        .run(
            &repository,
            ID,
            &opts().max_attempts(attempts(1)),
            async |c, _| done(increment(c, 1)),
        )
        .await
        .unwrap_err();
    assert!(!runner.is_cached().await);

    repository.conflicts_to_force.store(0, Ordering::SeqCst);
    runner
        .run(&repository, ID, &opts(), async |c, _| done(increment(c, 1)))
        .await
        .unwrap();

    assert_eq!(
        repository.loads(),
        2,
        "the exhausted instance must not be reused"
    );
}

#[tokio::test]
async fn decide_failing_evicts_the_cache_rather_than_leaving_a_half_decided_instance() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new();

    runner
        .run(&repository, ID, &opts(), async |c, _| {
            increment(c, 1);
            Err::<AggregateDecision<i32>, _>("boom")
        })
        .await
        .unwrap_err();

    runner
        .run(&repository, ID, &opts(), async |c, _| done(c.value))
        .await
        .unwrap();
    assert_eq!(
        repository.loads(),
        2,
        "the instance decide failed against must not be reused"
    );
}

#[tokio::test]
async fn cached_redo_discards_the_instance_and_tries_again_uncounted() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new();
    let mut calls = 0;

    let result = runner
        .run(
            &repository,
            ID,
            &opts().max_attempts(attempts(1)),
            async |c, _| {
                calls += 1;
                if calls == 1 {
                    Ok::<_, Infallible>(AggregateDecision::Redo)
                } else {
                    done(increment(c, 1))
                }
            },
        )
        .await
        .unwrap();

    assert_eq!(result, 1);
    assert_eq!(calls, 2);
    assert_eq!(
        repository.loads(),
        2,
        "Redo discards the instance it was handed"
    );
}

#[tokio::test]
async fn decide_learns_whether_its_instance_came_from_the_cache() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new();
    let mut seen = Vec::new();

    for _ in 0..2 {
        runner
            .run(&repository, ID, &opts(), async |c, from_cache| {
                seen.push(from_cache);
                done(c.value)
            })
            .await
            .unwrap();
    }

    assert_eq!(seen, [false, true]);
}

#[tokio::test]
async fn without_an_expected_version_selector_the_save_defaults_to_the_aggregates_own_version() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new();

    runner
        .run(&repository, ID, &opts(), async |c, _| done(increment(c, 1)))
        .await
        .unwrap();

    assert_eq!(*repository.last_expected.lock().unwrap(), Some(None));
}

#[tokio::test]
async fn an_expected_version_selector_overrides_the_aggregates_own_version() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new().with_expected_version(|_| 41);

    runner
        .run(&repository, ID, &opts(), async |c, _| done(increment(c, 1)))
        .await
        .unwrap();

    assert_eq!(*repository.last_expected.lock().unwrap(), Some(Some(41)));
}

#[tokio::test]
async fn without_a_concurrency_check_a_conflict_cannot_happen_and_the_instance_stays_cached() {
    let repository = FakeRepository::conflicts(i64::MAX);
    let runner = CachedAggregate::<Counter>::new();
    let options = opts().concurrency_check(false);

    runner
        .run(&repository, ID, &options, async |c, _| {
            done(increment(c, 1))
        })
        .await
        .unwrap();
    let result = runner
        .run(&repository, ID, &options, async |c, _| {
            done(increment(c, 1))
        })
        .await
        .unwrap();

    assert_eq!(result, 2);
    assert_eq!(repository.unchecked(), 2);
    assert_eq!(
        repository.loads(),
        1,
        "an unchecked save that committed leaves the instance cached"
    );
}

#[tokio::test]
async fn cached_aggregate_applies_the_same_refusal_guard_and_drops_the_instance() {
    let repository = FakeRepository::default();
    let runner = CachedAggregate::<Counter>::new();

    let err = runner
        .run(&repository, ID, &opts(), async |c, _| {
            increment(c, 1);
            Ok::<_, Infallible>(AggregateDecision::Refused(0))
        })
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ExecuteError::Repository(Error::RefusedAfterEmitting { .. })
    ));

    runner
        .run(&repository, ID, &opts(), async |c, _| done(c.value))
        .await
        .unwrap();
    assert_eq!(
        repository.loads(),
        2,
        "the refusing instance must not be reused"
    );
    assert_eq!(repository.saves(), 0);
}

#[tokio::test]
async fn the_cached_futures_are_send() {
    let repository = std::sync::Arc::new(FakeRepository::default());
    let handle = tokio::spawn(async move {
        let runner = CachedAggregate::<Counter>::new();
        runner
            .run(
                &*repository,
                ID,
                &ExecuteOptions::default(),
                async |c, _| done(increment(c, 1)),
            )
            .await
            .ok()
    });
    assert_eq!(handle.await.unwrap(), Some(1));
}

// ---- EventSourcingTracingTests (a fake store) ----

#[derive(Default)]
struct FakeStore {
    read_result: Mutex<Vec<StreamMsg>>,
    append_result: Mutex<Option<Vec<StreamId>>>,
    throw_on_append: Mutex<Option<String>>,
}

impl EventStore for FakeStore {
    async fn read(
        &self,
        _name: &str,
        _after: StreamId,
        _max: usize,
    ) -> redisevents::Result<Vec<StreamMsg>> {
        Ok(std::mem::take(&mut *self.read_result.lock().unwrap()))
    }

    async fn append_and_publish(
        &self,
        _name: &str,
        _expected_len: u64,
        _partition_key: &str,
        _events: &[StateEvent],
    ) -> redisevents::Result<Option<Vec<StreamId>>> {
        if let Some(message) = &*self.throw_on_append.lock().unwrap() {
            return Err(redisevents::Error::transport(message.clone()));
        }
        Ok(self.append_result.lock().unwrap().clone())
    }

    async fn append_and_publish_unchecked(
        &self,
        _name: &str,
        _partition_key: &str,
        _events: &[StateEvent],
    ) -> redisevents::Result<Vec<StreamId>> {
        Ok(self
            .append_result
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_default())
    }
}

fn registry() -> EventTypeRegistry {
    EventTypeRegistry::new()
        .register_json::<Incremented>("incremented")
        .unwrap()
}

fn history_msg(version: u64) -> StreamMsg {
    StreamMsg::new(
        StreamId::new(version, 0),
        0,
        "incremented",
        Bytes::from_static(br#"{"by":1}"#),
    )
}

fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn touched(id: &str) -> AggregateRoot<Counter> {
    let mut root = AggregateRoot::<Counter>::new(id).unwrap();
    increment(&mut root, 1);
    root
}

fn spans_for(rig: &Rig, id: &str) -> Vec<common::Rec> {
    rig.spans()
        .into_iter()
        .filter(|s| s.field("eventsourcing.aggregate_id") == Some(id))
        .collect()
}

#[tokio::test]
async fn save_emits_a_save_span_tagged_with_the_aggregate_and_the_outcome() {
    let rig = Rig::default();
    let _guard = rig.install();
    let store = FakeStore::default();
    *store.append_result.lock().unwrap() = Some(vec![StreamId::new(1, 0)]);
    let repo = RedisEventRepository::new(store, registry());
    let id = new_id();
    let mut aggregate = touched(&id);

    let version = repo
        .save(&mut aggregate, None, &PublishOptions::default())
        .await
        .unwrap();

    assert_eq!(version, 1);
    let spans = spans_for(&rig, &id);
    assert_eq!(spans.len(), 1);
    let span = &spans[0];
    assert_eq!(span.name, "eventsourcing.save");
    assert_eq!(span.field("otel.name"), Some("eventsourcing.save Counter"));
    assert_eq!(span.field("otel.kind"), Some("client"));
    assert_eq!(
        span.field("otel.status_code"),
        None,
        "a successful save is not an error"
    );
    assert_eq!(span.field("eventsourcing.aggregate"), Some("Counter"));
    assert_eq!(span.field("eventsourcing.expected_version"), Some("0"));
    assert_eq!(span.field("eventsourcing.version"), Some("1"));
    assert_eq!(span.field("eventsourcing.event_count"), Some("1"));
}

#[tokio::test]
async fn save_that_loses_its_version_check_marks_the_span_as_a_conflict() {
    let rig = Rig::default();
    let _guard = rig.install();
    let repo = RedisEventRepository::new(FakeStore::default(), registry());
    let id = new_id();
    let mut aggregate = touched(&id);

    let err = repo
        .save(&mut aggregate, None, &PublishOptions::default())
        .await
        .unwrap_err();

    assert!(err.as_concurrency().is_some());
    assert_eq!(aggregate.pending().len(), 1, "nothing was committed");
    let spans = spans_for(&rig, &id);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].field("otel.status_code"), Some("ERROR"));
    assert!(
        spans[0]
            .field("otel.status_description")
            .unwrap()
            .to_lowercase()
            .contains("concurrency")
    );
}

#[tokio::test]
async fn save_that_fails_marks_the_span_as_a_failure_distinct_from_a_conflict() {
    let rig = Rig::default();
    let _guard = rig.install();
    let store = FakeStore::default();
    *store.throw_on_append.lock().unwrap() = Some("transport is gone".into());
    let repo = RedisEventRepository::new(store, registry());
    let id = new_id();
    let mut aggregate = touched(&id);

    let err = repo
        .save(&mut aggregate, None, &PublishOptions::default())
        .await
        .unwrap_err();

    assert!(err.as_concurrency().is_none());
    assert_eq!(err.to_string(), "transport is gone");
    let spans = spans_for(&rig, &id);
    assert_eq!(spans[0].field("otel.status_code"), Some("ERROR"));
    assert_eq!(
        spans[0].field("otel.status_description"),
        Some("transport is gone"),
        "a transport failure must read differently from a lost version check"
    );
}

#[tokio::test]
async fn load_emits_a_load_span_tagged_with_the_version_found() {
    let rig = Rig::default();
    let _guard = rig.install();
    let store = FakeStore::default();
    *store.read_result.lock().unwrap() = vec![history_msg(1)];
    let repo = RedisEventRepository::new(store, registry());
    let id = new_id();

    let loaded = repo.load::<Counter>(&id).await.unwrap();

    assert!(loaded.is_some());
    let spans = spans_for(&rig, &id);
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans[0].field("otel.name"),
        Some("eventsourcing.load Counter")
    );
    assert_eq!(spans[0].field("otel.kind"), Some("client"));
    assert_eq!(spans[0].field("eventsourcing.found"), Some("true"));
    assert_eq!(spans[0].field("eventsourcing.version"), Some("1"));
}

#[tokio::test]
async fn load_with_no_history_tags_the_span_as_not_found() {
    let rig = Rig::default();
    let _guard = rig.install();
    let repo = RedisEventRepository::new(FakeStore::default(), registry());
    let id = new_id();

    assert!(repo.load::<Counter>(&id).await.unwrap().is_none());

    let spans = spans_for(&rig, &id);
    assert_eq!(spans[0].field("eventsourcing.found"), Some("false"));
    assert_eq!(
        spans[0].field("eventsourcing.version"),
        None,
        "nothing was found, so there is no version to report"
    );
}

#[tokio::test]
async fn with_no_subscriber_save_and_load_still_work() {
    let store = FakeStore::default();
    *store.append_result.lock().unwrap() = Some(vec![StreamId::new(1, 0)]);
    let repo = RedisEventRepository::new(store, registry());
    let id = new_id();
    let mut aggregate = touched(&id);

    assert_eq!(
        repo.save(&mut aggregate, None, &PublishOptions::default())
            .await
            .unwrap(),
        1
    );

    *repo.store().read_result.lock().unwrap() = vec![history_msg(1)];
    assert!(repo.load::<Counter>(&id).await.unwrap().is_some());
}
