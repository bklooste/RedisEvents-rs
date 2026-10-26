//! Registry tests. The Redis ones are a 1:1 port of C# `OwnershipTests` (Static mode) plus the
//! Lease-mode split / takeover cases. They live inside the crate because the registry is
//! crate-private; they take only the URL from `redisevents-test-support`.
#![allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::assert_is_empty,
    clippy::bool_to_int_with_if
)]

use std::sync::Mutex;
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use uuid::Uuid;

use super::*;
use crate::config::instance::{self, InstanceIdentity, InstanceSource, PartitionRange};

const PARTITIONS: u32 = 8;

// ------------------------------------------------------------------ pure

#[test]
fn embedded_scripts_equal_wire_spec_byte_for_byte() {
    assert_eq!(
        LEASE_SCRIPT,
        include_str!("../../../../wire-spec/lua/lease.lua")
    );
    assert_eq!(
        RELEASE_SCRIPT,
        include_str!("../../../../wire-spec/lua/release.lua")
    );
}

#[test]
fn read_owners_skips_fields_that_are_not_a_partition_number() {
    let id = Uuid::new_v4();
    let v = format_owner(id, "svc-0");
    let entries: Vec<(String, String)> = [
        ("0", v.as_str()),
        ("7", "garbage"),
        (presence_field(id).as_str(), v.as_str()),
        ("__hexpire-probe:x", "1"),
        ("+1", v.as_str()),
        (" 2", v.as_str()),
        ("-3", v.as_str()),
        ("", v.as_str()),
        ("99999999999", v.as_str()),
    ]
    .iter()
    .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
    .collect();
    let owners = read_owners(&entries);
    assert_eq!(owners.keys().copied().collect::<Vec<_>>(), vec![0, 7]);
    assert_eq!(owners[&0].instance_id, id);
    assert_eq!(owners[&0].pod_name, "svc-0");
    // C# Parse never fails: an unreadable owner is still an owner, with the nil id.
    assert_eq!(owners[&7].instance_id, Uuid::nil());
    assert_eq!(owners[&7].pod_name, "garbage");
    assert_eq!(count_members(&entries), 1);
    assert_eq!(count_members(&[]), 1);
}

#[test]
fn presence_field_is_not_a_partition_and_not_a_marker() {
    let field = presence_field(crate::ownership::instance_id_for(Some("svc-1")));
    assert!(field.starts_with("i:"));
    assert!(parse_partition(&field).is_none());
}

#[test]
fn probe_verdict_accepts_success_and_due_only() {
    assert!(probe_verdict("k", "t", &[1]).is_ok());
    assert!(probe_verdict("k", "t", &[2]).is_ok());
    for bad in [&[][..], &[-2], &[0]] {
        let err = probe_verdict("k", "t", bad).unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        assert!(err.to_string().contains("Redis 7.4"), "{err}");
    }
}

#[test]
fn describe_map_names_every_partition() {
    let id = Uuid::parse_str("3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c").unwrap();
    let mut map = OwnershipMap::default();
    map.owners.insert(
        0,
        PartitionOwner {
            partition: 0,
            instance_id: id,
            pod_name: "svc-0".into(),
        },
    );
    map.owners
        .insert(2, PartitionOwner::parse_lenient(2, "who"));
    assert_eq!(
        describe_map(&map, 3),
        "full map: 0->svc-0(3f2b8c1e) 1->(none) 2->who"
    );
}

// ------------------------------------------------------------------ log capture

/// Every event in this test binary, as `(level, instance field, message)`. One global subscriber
/// rather than a scoped one per test: scoped dispatchers race on tracing's callsite-interest cache
/// when tests run in parallel. Tests tell their lines apart by their own (random) instance ids.
static LOGS: Mutex<Vec<(Level, String, String)>> = Mutex::new(Vec::new());

/// A view of [`LOGS`] from a starting point (`clear` moves it).
#[derive(Default)]
struct Capture(std::sync::atomic::AtomicUsize);

impl Capture {
    fn install(&self) {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Sink))
                .expect("no other global subscriber in the lib tests");
        });
        self.clear();
    }

    fn messages(&self, level: Level, instance: Uuid) -> Vec<String> {
        let id = instance.to_string();
        let from = self.0.load(std::sync::atomic::Ordering::SeqCst);
        LOGS.lock().unwrap()[from..]
            .iter()
            .filter(|(l, i, _)| *l == level && *i == id)
            .map(|(_, _, m)| m.clone())
            .collect()
    }

    fn clear(&self) {
        let len = LOGS.lock().unwrap().len();
        self.0.store(len, std::sync::atomic::Ordering::SeqCst);
    }
}

struct Sink;

#[derive(Default)]
struct Fields {
    message: String,
    instance: String,
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "instance" => self.instance = format!("{value:?}"),
            _ => {}
        }
    }
}

impl<S: Subscriber> Layer<S> for Sink {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        LOGS.lock()
            .unwrap()
            .push((*event.metadata().level(), fields.instance, fields.message));
    }
}

// ------------------------------------------------------------------ fixture

async fn connection(redis: &redisevents_test_support::RedisFixture) -> Connection {
    Connection::connect(redis.url()).await.expect("connect")
}

fn keys() -> Keys {
    Keys::with_prefix("test:re:")
}

fn owned_range(index: u32, count: u32) -> PartitionRange {
    instance::owned(PARTITIONS, count, index)
}

fn static_registry(
    conn: &Connection,
    topic: &str,
    consumer: &str,
    range: PartitionRange,
    pod: &str,
    instance_id: Option<Uuid>,
    ttl_seconds: u32,
    renew_seconds: u32,
) -> OwnershipRegistry {
    let mut options = OwnershipRegistryOptions::new(topic, consumer, PARTITIONS);
    options.owned_partitions = range.iter().collect();
    options.pod_name = pod.to_owned();
    options.mode = InstanceMode::Static;
    options.instance_id = instance_id.unwrap_or_else(Uuid::new_v4);
    options.ttl_seconds = ttl_seconds;
    options.renew_seconds = renew_seconds;
    OwnershipRegistry::new(conn, &keys(), options).expect("valid options")
}

fn simple(
    conn: &Connection,
    topic: &str,
    consumer: &str,
    range: PartitionRange,
    pod: &str,
) -> OwnershipRegistry {
    static_registry(conn, topic, consumer, range, pod, None, 30, 10)
}

fn lease_registry(
    conn: &Connection,
    topic: &str,
    consumer: &str,
    pod: &str,
    ttl: u32,
    renew: u32,
) -> OwnershipRegistry {
    let mut options = OwnershipRegistryOptions::new(topic, consumer, PARTITIONS);
    options.pod_name = pod.to_owned();
    options.instance_id = Uuid::new_v4();
    options.ttl_seconds = ttl;
    options.renew_seconds = renew;
    OwnershipRegistry::new(conn, &keys(), options).expect("valid options")
}

async fn owners_on_server(
    conn: &Connection,
    topic: &str,
    consumer: &str,
) -> BTreeMap<u32, PartitionOwner> {
    let mut c = conn.shared();
    read_owners(
        &hgetall(&mut c, &keys().ownership(topic, consumer))
            .await
            .unwrap(),
    )
}

async fn field_exists(conn: &Connection, topic: &str, consumer: &str, field: &str) -> bool {
    let mut c = conn.shared();
    redis::cmd("HEXISTS")
        .arg(keys().ownership(topic, consumer))
        .arg(field)
        .query_async::<bool>(&mut c)
        .await
        .unwrap()
}

// ------------------------------------------------------------------ OwnershipTests (Static)

#[tokio::test]
async fn s6_two_instances_own_contiguous_halves_with_no_overlap_and_no_gaps() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("s6-split");
    let consumer = redis.new_consumer("s6-split");

    let range0 = owned_range(0, 2);
    let range1 = owned_range(1, 2);
    assert_eq!(range0, PartitionRange { start: 0, len: 4 });
    assert_eq!(range1, PartitionRange { start: 4, len: 4 });
    assert_eq!(range1.start, range0.end());

    let log = Capture::default();
    log.install();

    let pod0 = simple(&conn, &topic, &consumer, range0, "svc-0");
    let pod1 = simple(&conn, &topic, &consumer, range1, "svc-1");
    pod0.start().await.unwrap();
    pod1.start().await.unwrap();

    let full = |id| -> Vec<String> {
        log.messages(Level::INFO, id)
            .into_iter()
            .filter(|m| m.contains("full map:"))
            .collect()
    };
    let m0 = full(pod0.instance_id());
    // One line per instance: the first report, which also covers instance 0's startup gap.
    assert_eq!(m0.len(), 1, "{m0:?}");
    assert!(m0[0].contains("claims partitions 0,1,2,3"));
    let m1 = full(pod1.instance_id());
    assert_eq!(m1.len(), 1, "{m1:?}");
    assert!(m1[0].contains("claims partitions 4,5,6,7"));
    assert!(!m1[0].contains("(none)"));
    for p in 0..PARTITIONS {
        let pod = if p < 4 { 0 } else { 1 };
        assert!(m1[0].contains(&format!("{p}->svc-{pod}")), "partition {p}");
    }

    log.clear();
    let map0 = pod0.refresh().await.unwrap();
    let map1 = pod1.refresh().await.unwrap();
    for map in [&map0, &map1] {
        assert!(map.unowned.is_empty());
        assert!(map.contested.is_empty());
        assert!(!map.is_degraded());
        assert_eq!(map.owners.len(), 8);
        for p in 0..PARTITIONS {
            let o = &map.owners[&p];
            assert_eq!(o.pod_name, if p < 4 { "svc-0" } else { "svc-1" });
            assert_eq!(
                o.instance_id,
                if p < 4 {
                    pod0.instance_id()
                } else {
                    pod1.instance_id()
                }
            );
        }
        assert!(!describe_map(map, PARTITIONS).contains("(none)"));
    }
    assert!(log.messages(Level::ERROR, pod0.instance_id()).is_empty());
    assert!(log.messages(Level::ERROR, pod1.instance_id()).is_empty());
    assert_eq!(owners_on_server(&conn, &topic, &consumer).await.len(), 8);

    pod0.dispose().await;
    pod1.dispose().await;
}

#[tokio::test]
async fn s6b_a_missing_instance_leaves_its_partitions_unowned_and_health_degraded() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("s6b-gap");
    let consumer = redis.new_consumer("s6b-gap");

    let range0 = owned_range(0, 3);
    let range1 = owned_range(1, 3);
    let missing = owned_range(2, 3);
    assert_eq!(range0.iter().collect::<Vec<_>>(), [0, 1, 2]);
    assert_eq!(range1.iter().collect::<Vec<_>>(), [3, 4, 5]);
    assert_eq!(missing.iter().collect::<Vec<_>>(), [6, 7]);

    let log = Capture::default();
    log.install();
    let pod0 = simple(&conn, &topic, &consumer, range0, "svc-0");
    let pod1 = simple(&conn, &topic, &consumer, range1, "svc-1");
    pod0.start().await.unwrap();
    pod1.start().await.unwrap();
    log.clear();

    let map0 = pod0.refresh().await.unwrap();
    let map = pod1.refresh().await.unwrap();
    assert_eq!(map0.unowned, [6, 7]);
    assert_eq!(map.unowned, [6, 7]);
    assert!(map.contested.is_empty());
    assert!(map.is_degraded());
    assert_eq!(map.owners.len(), 6);
    let described = describe_map(&map, PARTITIONS);
    assert!(described.contains("6->(none)") && described.contains("7->(none)"));

    for id in [pod0.instance_id(), pod1.instance_id()] {
        let gaps: Vec<_> = log
            .messages(Level::ERROR, id)
            .into_iter()
            .filter(|m| m.contains("ownership gap"))
            .collect();
        assert_eq!(gaps.len(), 1, "{gaps:?}");
        assert!(gaps[0].contains("partitions 6,7 of 8"));
        assert!(gaps[0].contains("consumed by nobody"));
        assert!(gaps[0].contains("STREAMS_INSTANCE_COUNT"));
    }

    let snapshot = OwnershipRegistry::snapshot(&topic, &consumer).expect("snapshot");
    assert!(snapshot.is_degraded());
    // The C# health-check half (StreamsHealthCheck) belongs to D2; the registry side is this:
    assert!(OwnershipRegistry::any_degraded());

    pod0.dispose().await;
    pod1.dispose().await;
    assert!(OwnershipRegistry::snapshot(&topic, &consumer).is_none());
}

#[tokio::test]
async fn s6c_two_instances_claiming_index_zero_are_reported_as_contested() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("s6c-overlap");
    let consumer = redis.new_consumer("s6c-overlap");

    let everything = owned_range(0, 1);
    assert_eq!(
        everything.iter().collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>()
    );

    let log = Capture::default();
    log.install();
    let pod_a = simple(&conn, &topic, &consumer, everything, "deploy-abc12-x2k9p");
    let pod_b = simple(&conn, &topic, &consumer, everything, "deploy-abc12-7f4dq");
    assert_ne!(pod_a.instance_id(), pod_b.instance_id());

    pod_a.start().await.unwrap();
    pod_b.start().await.unwrap();

    let map_b = pod_b.latest().expect("latest");
    assert!(map_b.unowned.is_empty());
    assert_eq!(map_b.contested.len(), 8);
    assert_eq!(
        map_b
            .contested
            .iter()
            .map(|c| c.partition)
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>()
    );
    for c in &map_b.contested {
        assert_eq!(c.expected, pod_b.instance_id());
        assert_eq!(c.actual.instance_id, pod_a.instance_id());
        assert_eq!(c.actual.pod_name, "deploy-abc12-x2k9p");
    }
    assert!(map_b.is_degraded());

    let overlap: Vec<_> = log
        .messages(Level::ERROR, pod_b.instance_id())
        .into_iter()
        .filter(|m| m.contains("ownership overlap"))
        .collect();
    assert_eq!(overlap.len(), 8, "one Error per contested partition");
    assert!(overlap[0].contains("processed twice"));
    assert!(overlap[0].contains("deploy-abc12-x2k9p"));
    assert!(overlap[0].contains("StatefulSet, not a Deployment"));

    log.clear();
    let map_a = pod_a.refresh().await.unwrap();
    assert_eq!(map_a.contested.len(), 8);
    assert!(
        map_a
            .contested
            .iter()
            .all(|c| c.actual.instance_id == pod_b.instance_id())
    );
    assert!(
        log.messages(Level::ERROR, pod_a.instance_id())
            .iter()
            .any(|m| m.contains("ownership overlap"))
    );
    assert_eq!(owners_on_server(&conn, &topic, &consumer).await.len(), 8);
    assert!(OwnershipRegistry::any_degraded());

    pod_a.dispose().await;
    pod_b.dispose().await;
}

#[tokio::test]
async fn s6d_a_restarted_instance_reclaims_its_ordinal_range_with_no_stale_duplicate() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("s6d-bounce");
    let consumer = redis.new_consumer("s6d-bounce");

    let resolve_one = || {
        InstanceIdentity::resolve(None, |name| match name {
            instance::POD_NAME_VARIABLE => Some("svc-1".to_owned()),
            instance::COUNT_VARIABLE => Some("2".to_owned()),
            _ => None,
        })
        .unwrap()
    };
    let before = resolve_one();
    assert_eq!((before.index, before.count), (1, 2));
    assert_eq!(before.index_source, InstanceSource::PodOrdinal);
    let range_one = before.owned(PARTITIONS);
    assert_eq!(range_one.iter().collect::<Vec<_>>(), [4, 5, 6, 7]);

    let log = Capture::default();
    log.install();
    let pod0 = simple(&conn, &topic, &consumer, owned_range(0, 2), "svc-0");
    pod0.start().await.unwrap();

    // First life of instance 1: one claim, 2 s TTL, no renew loop, then "SIGKILL".
    let killed_id = Uuid::new_v4();
    let killed = static_registry(
        &conn,
        &topic,
        &consumer,
        range_one,
        "svc-1",
        Some(killed_id),
        2,
        1,
    );
    let killed_map = killed.refresh().await.unwrap();
    killed.dispose().await; // never started: no release, exactly like a hard kill
    assert!(killed_map.unowned.is_empty());
    assert!(killed_map.contested.is_empty());
    assert_eq!(owners_on_server(&conn, &topic, &consumer).await.len(), 8);

    redisevents_test_support::wait_until_async(
        || async { owners_on_server(&conn, &topic, &consumer).await.len() == 4 },
        Some(Duration::from_secs(15)),
        "the hard-killed instance's HEXPIRE'd claims to lapse",
    )
    .await;
    let owners = owners_on_server(&conn, &topic, &consumer).await;
    assert_eq!(owners.keys().copied().collect::<Vec<_>>(), [0, 1, 2, 3]);
    assert!(owners.values().all(|o| o.instance_id == pod0.instance_id()));

    log.clear();
    let gap = pod0.refresh().await.unwrap();
    assert_eq!(gap.unowned, [4, 5, 6, 7]);
    assert!(gap.is_degraded());
    assert!(
        log.messages(Level::ERROR, pod0.instance_id())
            .iter()
            .any(|m| m.contains("ownership gap"))
    );

    let after = resolve_one();
    assert_eq!(after.index, before.index);
    let reclaimed = after.owned(PARTITIONS);
    assert_eq!(reclaimed, range_one);

    let restarted_id = Uuid::new_v4();
    assert_ne!(restarted_id, killed_id);
    let restarted = static_registry(
        &conn,
        &topic,
        &consumer,
        reclaimed,
        "svc-1",
        Some(restarted_id),
        30,
        10,
    );
    restarted.start().await.unwrap();
    let map = restarted.latest().expect("latest");
    assert!(map.unowned.is_empty());
    assert!(map.contested.is_empty());
    assert!(!map.is_degraded());

    let final_owners = owners_on_server(&conn, &topic, &consumer).await;
    assert_eq!(final_owners.len(), 8);
    for p in 0..PARTITIONS {
        let o = &final_owners[&p];
        assert_eq!(o.pod_name, if p < 4 { "svc-0" } else { "svc-1" });
        assert_eq!(
            o.instance_id,
            if p < 4 {
                pod0.instance_id()
            } else {
                restarted_id
            }
        );
        assert_ne!(o.instance_id, killed_id);
    }
    assert!(log.messages(Level::ERROR, restarted_id).is_empty());
    let restart_map: Vec<_> = log
        .messages(Level::INFO, restarted_id)
        .into_iter()
        .filter(|m| m.contains("full map:"))
        .collect();
    assert_eq!(restart_map.len(), 1);
    assert!(restart_map[0].contains("claims partitions 4,5,6,7"));
    assert!(!restart_map[0].contains("(none)"));

    pod0.dispose().await;
    restarted.dispose().await;
}

#[tokio::test]
async fn release_deletes_only_the_claims_this_instance_still_holds() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("release-cas");
    let consumer = redis.new_consumer("release-cas");
    let everything = owned_range(0, 1);

    let leaving = simple(&conn, &topic, &consumer, everything, "deploy-abc12-leaving");
    let staying = simple(&conn, &topic, &consumer, everything, "deploy-abc12-staying");
    leaving.start().await.unwrap();
    staying.start().await.unwrap();

    let claimed = owners_on_server(&conn, &topic, &consumer).await;
    assert_eq!(claimed.len(), 8);
    assert!(
        claimed
            .values()
            .all(|o| o.instance_id == staying.instance_id())
    );

    leaving.stop().await;

    let after = owners_on_server(&conn, &topic, &consumer).await;
    assert_eq!(after.len(), 8, "must not delete what overwrote it");
    assert!(
        after
            .values()
            .all(|o| o.instance_id == staying.instance_id())
    );
    assert!(
        !field_exists(
            &conn,
            &topic,
            &consumer,
            &presence_field(leaving.instance_id())
        )
        .await
    );
    assert!(
        field_exists(
            &conn,
            &topic,
            &consumer,
            &presence_field(staying.instance_id())
        )
        .await
    );

    leaving.dispose().await;
    staying.dispose().await;
}

/// C# R-31 passes an already-cancelled token to `StopAsync`. Rust has no token: the equivalent is a
/// stop that runs while the renew loop is mid-wait and the caller is shutting down; it must still
/// release the claims and the presence field.
#[tokio::test]
async fn a_stop_on_an_already_cancelled_token_still_drops_the_claims_and_the_presence_field() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("release-cancelled");
    let consumer = redis.new_consumer("release-cancelled");

    let leaving = simple(
        &conn,
        &topic,
        &consumer,
        owned_range(0, 1),
        "deploy-abc12-leaving",
    );
    leaving.start().await.unwrap();
    assert!(
        field_exists(
            &conn,
            &topic,
            &consumer,
            &presence_field(leaving.instance_id())
        )
        .await
    );

    leaving.stop().await;

    assert!(
        !field_exists(
            &conn,
            &topic,
            &consumer,
            &presence_field(leaving.instance_id())
        )
        .await
    );
    assert!(owners_on_server(&conn, &topic, &consumer).await.is_empty());
    leaving.dispose().await;
}

// ------------------------------------------------------------------ Lease mode

#[tokio::test]
async fn lease_two_instances_split_eight_partitions_and_one_takes_over_after_release() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("lease-split");
    let consumer = redis.new_consumer("lease-split");

    let a = lease_registry(&conn, &topic, &consumer, "svc-a", 30, 10);
    let b = lease_registry(&conn, &topic, &consumer, "svc-b", 30, 10);
    let mut b_changes = b.subscribe();

    a.start().await.unwrap();
    assert_eq!(
        &a.held()[..],
        (0..8).collect::<Vec<_>>(),
        "alone, a takes everything"
    );
    b.start().await.unwrap();
    assert!(
        b.held().is_empty(),
        "everything is held, so b claims nothing yet"
    );

    let a_changes = a.subscribe();
    a.refresh().await.unwrap(); // two members now: a sheds down to its cap of 4
    assert!(
        a_changes.has_changed().unwrap(),
        "a lost partitions: signalled"
    );
    assert_eq!(&a.held()[..], [0, 1, 2, 3]);
    b.refresh().await.unwrap();
    assert_eq!(&b.held()[..], [4, 5, 6, 7]);
    assert!(b_changes.has_changed().unwrap());
    b_changes.mark_unchanged();

    let map = a.refresh().await.unwrap();
    assert!(map.unowned.is_empty() && map.contested.is_empty());
    assert_eq!(map.owners.len(), 8);
    for p in 0..PARTITIONS {
        let expect = if p < 4 {
            a.instance_id()
        } else {
            b.instance_id()
        };
        assert_eq!(map.owners[&p].instance_id, expect);
    }

    a.stop().await;
    assert!(a.held().is_empty());
    assert!(!field_exists(&conn, &topic, &consumer, &presence_field(a.instance_id())).await);
    assert_eq!(owners_on_server(&conn, &topic, &consumer).await.len(), 4);

    b.refresh().await.unwrap();
    assert_eq!(
        &b.held()[..],
        (0..8).collect::<Vec<_>>(),
        "b takes over a's released half"
    );
    assert!(b_changes.has_changed().unwrap());

    a.dispose().await;
    b.dispose().await;
    assert!(owners_on_server(&conn, &topic, &consumer).await.is_empty());
}

#[tokio::test]
async fn lease_renew_loop_rebalances_and_signals_through_the_watch() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("lease-loop");
    let consumer = redis.new_consumer("lease-loop");

    let a = lease_registry(&conn, &topic, &consumer, "svc-a", 3, 1);
    let b = lease_registry(&conn, &topic, &consumer, "svc-b", 3, 1);
    let mut a_rx = a.subscribe();
    a.start().await.unwrap();
    b.start().await.unwrap();

    // a's loop must shed to 4 on its own; the watch wakes us.
    tokio::time::timeout(Duration::from_secs(10), a_rx.wait_for(|h| h.len() == 4))
        .await
        .expect("a sheds within a few renew intervals")
        .unwrap();
    let mut b_rx = b.subscribe();
    tokio::time::timeout(Duration::from_secs(10), b_rx.wait_for(|h| h.len() == 4))
        .await
        .expect("b claims the freed half")
        .unwrap();
    let mut all: Vec<u32> = a.held().iter().chain(b.held().iter()).copied().collect();
    all.sort_unstable();
    assert_eq!(all, (0..8).collect::<Vec<_>>());

    a.dispose().await;
    tokio::time::timeout(Duration::from_secs(10), b_rx.wait_for(|h| h.len() == 8))
        .await
        .expect("b takes over after a releases")
        .unwrap();
    b.dispose().await;
}

#[tokio::test]
async fn lease_mode_refuses_pre_assigned_partitions_and_bad_timings() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;

    let mut options = OwnershipRegistryOptions::new("t", "c", PARTITIONS);
    options.owned_partitions = vec![0, 1];
    let err = OwnershipRegistry::new(&conn, &keys(), options)
        .err()
        .unwrap();
    assert!(
        matches!(err, Error::Config(ref m) if m.contains("Lease mode")),
        "{err}"
    );

    let mut options = OwnershipRegistryOptions::new("t", "c", PARTITIONS);
    options.renew_seconds = 30;
    let err = OwnershipRegistry::new(&conn, &keys(), options)
        .err()
        .unwrap();
    assert!(
        matches!(err, Error::Config(ref m) if m.contains("LeaseRenewSeconds")),
        "{err}"
    );

    let options = OwnershipRegistryOptions::new("t", "c", 0);
    assert!(matches!(
        OwnershipRegistry::new(&conn, &keys(), options),
        Err(Error::Config(_))
    ));
    let options = OwnershipRegistryOptions::new(" ", "c", 1);
    assert!(matches!(
        OwnershipRegistry::new(&conn, &keys(), options),
        Err(Error::InvalidArgument(_))
    ));
}

#[tokio::test]
async fn start_probes_hexpire_and_leaves_no_probe_field() {
    let redis = redisevents_test_support::require_redis!();
    let conn = connection(redis).await;
    let topic = redis.new_topic("probe");
    let consumer = redis.new_consumer("probe");
    let a = lease_registry(&conn, &topic, &consumer, "svc-a", 30, 10);
    a.start().await.unwrap();
    let probe = format!("{PROBE_FIELD_PREFIX}{}", a.instance_id());
    assert!(!field_exists(&conn, &topic, &consumer, &probe).await);
    a.dispose().await;
}
