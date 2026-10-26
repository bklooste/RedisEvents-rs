//! A stalled Lease owner hands its partitions on (C# #52 `StalledLeaseHandoffTests`, end to end on
//! a real host). A consumer whose handler never returns stalls past `unhealthy_stalled_seconds`,
//! grades Unhealthy (rule `Stalled`), gives its leases up, withdraws its presence from the fair
//! share, does not re-claim the partitions while quarantined, and reads them again once the
//! quarantine ends. Skips without Redis.
//!
//! The rival is either a second host (delivery) or the wire-spec lease script run by hand (health):
//! two hosts in one process share the process-wide monitor registry, so the health assertions
//! need the rival to be something other than a host.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::assert_is_empty
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use redisevents::admin::health::{self, StreamHealthRule, StreamHealthStatus};
use redisevents::config::{
    ConsumerOptions, InstanceMode, InstanceOptions, StreamOptions, TopicOptions,
};
use redisevents::consumer::{Streams, StreamsHost};
use redisevents::ownership::instance_id_for;
use redisevents::producer::{PublishOptions, Publisher, StreamPublisher};
use redisevents::{BatchHandler, HandleResult, PartitionContext, StreamMsg};
use redisevents_test_support::{RecordingHandler, RedisFixture, require_redis, wait_until};
use tokio::sync::Mutex;
use uuid::Uuid;

const TTL: u32 = 3;
const STALL_SECONDS: u32 = 2;
const WAIT: Option<Duration> = Some(Duration::from_secs(30));
const LEASE_LUA: &str = include_str!("../../../wire-spec/lua/lease.lua");

/// The process-wide monitor registry is shared by every test in this binary.
static SERIAL: Mutex<()> = Mutex::const_new(());

fn topic_options() -> TopicOptions {
    TopicOptions {
        partitions: 2,
        co_locate_partitions: false,
        ..TopicOptions::default()
    }
}

fn consumer_options(topic: &str, consumer: &str, mode: InstanceMode) -> ConsumerOptions {
    ConsumerOptions {
        topic: topic.to_owned(),
        consumer: Some(consumer.to_owned()),
        batch_size: 10,
        persist_interval_ms: 50,
        block_ms: 200,
        unhealthy_stalled_seconds: STALL_SECONDS,
        shutdown_timeout_seconds: 1,
        instances: Some(InstanceOptions {
            mode,
            lease_ttl_seconds: TTL,
            lease_renew_seconds: 1,
            count: (mode == InstanceMode::Static).then_some(1),
            index: (mode == InstanceMode::Static).then_some(0),
            ..InstanceOptions::default()
        }),
        ..ConsumerOptions::default()
    }
}

/// A handler that never returns while `wedged` is set (ignoring its cancellation token, like one
/// stuck on a call that will not complete), and records what it handles once released.
struct Wedge {
    wedged: AtomicBool,
    entered: AtomicUsize,
    handled: std::sync::Mutex<Vec<String>>,
}

impl Wedge {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            wedged: AtomicBool::new(true),
            entered: AtomicUsize::new(0),
            handled: std::sync::Mutex::new(Vec::new()),
        })
    }
}

impl BatchHandler for Wedge {
    async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        if self.wedged.load(Ordering::SeqCst) {
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
        let mut handled = self.handled.lock().unwrap();
        handled.extend(
            batch
                .iter()
                .map(|m| String::from_utf8_lossy(m.body()).into_owned()),
        );
        Ok(())
    }
}

async fn spawn<H: BatchHandler>(
    redis: &RedisFixture,
    topic: &str,
    options: ConsumerOptions,
    handler: H,
    pod: &str,
) -> StreamsHost {
    let mut stream_options = StreamOptions::default();
    stream_options
        .topics
        .insert(topic.to_owned(), topic_options());
    Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(stream_options)
        .topic(topic)
        .options(options)
        .instance_id(instance_id_for(Some(pod)))
        .handler(handler)
        .spawn()
        .await
        .expect("spawn the consumer host")
}

async fn publish_one_per_partition(redis: &RedisFixture, topic: &str) {
    let publisher =
        StreamPublisher::new(redis.connect().await, redis.keys(), topic, topic_options())
            .await
            .expect("publisher");
    for partition in 0..2 {
        publisher
            .publish(
                "k",
                Bytes::from(format!("price p{partition}")),
                "t",
                &PublishOptions::new().partition(partition),
            )
            .await
            .expect("publish");
    }
}

async fn ownership(redis: &RedisFixture, topic: &str, consumer: &str) -> HashMap<String, String> {
    let mut conn = redis.connect().await.shared();
    redis::cmd("HGETALL")
        .arg(redis.keys().ownership(topic, consumer))
        .query_async(&mut conn)
        .await
        .expect("HGETALL")
}

fn presence(id: Uuid) -> String {
    format!("i:{}", id.hyphenated())
}

/// Another replica's lease cycle, exactly as WIRE.md §8 defines it (no quarantine).
async fn rival_cycle(redis: &RedisFixture, topic: &str, consumer: &str, pod: &str) -> Vec<u32> {
    let id = instance_id_for(Some(pod));
    let mut conn = redis.connect().await.shared();
    let mut held: Vec<u32> = redis::Script::new(LEASE_LUA)
        .key(redis.keys().ownership(topic, consumer))
        .arg(format!("{}|{pod}", id.hyphenated()))
        .arg(TTL)
        .arg(2)
        .arg(presence(id))
        .invoke_async(&mut conn)
        .await
        .expect("lease script");
    held.sort_unstable();
    held
}

fn mine(topic: &str, consumer: &str) -> Vec<health::StreamPartitionStatus> {
    health::partitions()
        .into_iter()
        .filter(|p| p.topic == topic && p.consumer == consumer)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lease_owner_that_stops_making_progress_hands_its_partitions_over_and_reports_unhealthy()
{
    let redis = require_redis!();
    let _serial = SERIAL.lock().await;
    let topic = redis.new_topic("stall-handoff");
    let consumer = redis.new_consumer("stall-handoff");
    let wedge = Wedge::new();
    let pod = "offer-odds-projection-76d86fc5f8-vr5zj";
    let rival_pod = "offer-odds-projection-76d86fc5f8-q2m7t";
    let me = instance_id_for(Some(pod));

    let host = spawn(
        redis,
        &topic,
        consumer_options(&topic, &consumer, InstanceMode::Lease),
        wedge.clone(),
        pod,
    )
    .await;
    assert_eq!(
        host.owned_partitions(&topic, &consumer),
        vec![0, 1],
        "the only instance up claims both partitions"
    );

    publish_one_per_partition(redis, &topic).await;
    wait_until(
        || wedge.entered.load(Ordering::SeqCst) == 2,
        WAIT,
        "both partitions' handlers to wedge",
    )
    .await;

    // The other replica keeps cycling, as its renew loop would. It must end up with both.
    let deadline = Instant::now() + Duration::from_secs(30);
    while rival_cycle(redis, &topic, &consumer, rival_pod).await != [0, 1] {
        assert!(
            Instant::now() < deadline,
            "the rival never took both partitions over from the stalled owner"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let fields = ownership(redis, &topic, &consumer).await;
    let rival_id = instance_id_for(Some(rival_pod)).hyphenated().to_string();
    for p in ["0", "1"] {
        assert!(
            fields[p].starts_with(&rival_id),
            "the stalled owner gave partition {p} up: {fields:?}"
        );
    }
    assert!(
        !fields.contains_key(&presence(me)),
        "while quarantined the stalled owner does not count towards the fair share"
    );

    // The stalled partitions stay visible to the health check after the leases are gone.
    let stalled = mine(&topic, &consumer);
    assert!(!stalled.is_empty());
    assert!(stalled.iter().all(|p| p.stood_down_stalled), "{stalled:?}");
    let report = host.health().await;
    assert_eq!(
        report.status,
        StreamHealthStatus::Unhealthy,
        "{}",
        report.description
    );
    assert_eq!(
        report.rule,
        StreamHealthRule::Stalled,
        "{}",
        report.description
    );
    assert!(
        report.description.contains("stood down"),
        "{}",
        report.description
    );

    // Not taken back while quarantined, however many cycles run.
    for _ in 0..(TTL * 2) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            rival_cycle(redis, &topic, &consumer, rival_pod).await,
            [0, 1]
        );
    }
    assert!(
        host.owned_partitions(&topic, &consumer).is_empty(),
        "its read side follows the lease it gave up"
    );

    // Recovery: the wedge clears and the rival goes away. Once the quarantine (2 x TTL) ends the
    // instance claims the partitions again on fresh reader connections, and reads what it never
    // recorded.
    wedge.wedged.store(false, Ordering::SeqCst);
    wait_until(
        || host.owned_partitions(&topic, &consumer) == vec![0, 1],
        WAIT,
        "the instance to take its partitions back once the quarantine ends and the rival's leases lapse",
    )
    .await;
    wait_until(
        || wedge.handled.lock().unwrap().len() == 2,
        WAIT,
        "the entries the wedged handler never finished to be delivered again",
    )
    .await;
    let back = mine(&topic, &consumer);
    assert_eq!(back.len(), 2);
    assert!(back.iter().all(|p| !p.stood_down_stalled), "{back:?}");
    assert_eq!(host.health().await.status, StreamHealthStatus::Healthy);
    assert!(
        ownership(redis, &topic, &consumer)
            .await
            .contains_key(&presence(me))
    );

    host.shutdown().await.expect("shutdown");
    assert!(
        mine(&topic, &consumer).is_empty(),
        "a stopped host takes its partitions out of the health check with it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_instance_takes_a_stalled_owners_partitions_over_within_about_one_lease_ttl() {
    let redis = require_redis!();
    let _serial = SERIAL.lock().await;
    let topic = redis.new_topic("stall-takeover");
    let consumer = redis.new_consumer("stall-takeover");
    let options = consumer_options(&topic, &consumer, InstanceMode::Lease);
    let wedge = Wedge::new();

    let stuck = spawn(redis, &topic, options.clone(), wedge.clone(), "pod-a").await;
    assert_eq!(stuck.owned_partitions(&topic, &consumer), vec![0, 1]);
    let rival = Arc::new(RecordingHandler::new());
    let healthy = spawn(redis, &topic, options, rival.clone(), "pod-b").await;

    publish_one_per_partition(redis, &topic).await;
    let wedged_at = Instant::now();
    wait_until(
        || wedge.entered.load(Ordering::SeqCst) == 2,
        WAIT,
        "the first instance's handlers to wedge",
    )
    .await;

    wait_until(
        || healthy.owned_partitions(&topic, &consumer) == vec![0, 1],
        WAIT,
        "the second instance to own both partitions",
    )
    .await;
    let took = wedged_at.elapsed();
    // Stall window + check tick + one renew cycle of the rival, plus slack for a loaded machine.
    assert!(
        took <= Duration::from_secs(u64::from(STALL_SECONDS + TTL) + 5),
        "took {took:?}"
    );
    rival.wait_for(2, WAIT).await;
    let mut bodies = rival.bodies();
    bodies.sort();
    assert_eq!(bodies, ["price p0", "price p1"]);
    assert!(stuck.owned_partitions(&topic, &consumer).is_empty());

    healthy.shutdown().await.expect("shutdown b");
    stuck.shutdown().await.expect("shutdown a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_static_owner_that_stalls_reports_unhealthy_and_keeps_its_partitions() {
    let redis = require_redis!();
    let _serial = SERIAL.lock().await;
    let topic = redis.new_topic("stall-static");
    let consumer = redis.new_consumer("stall-static");
    let wedge = Wedge::new();
    let host = spawn(
        redis,
        &topic,
        consumer_options(&topic, &consumer, InstanceMode::Static),
        wedge.clone(),
        "pod-static",
    )
    .await;
    publish_one_per_partition(redis, &topic).await;
    wait_until(
        || wedge.entered.load(Ordering::SeqCst) == 2,
        WAIT,
        "both handlers to wedge",
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let report = host.health().await;
        if report.rule == StreamHealthRule::Stalled {
            assert_eq!(report.status, StreamHealthStatus::Unhealthy);
            assert!(
                report.description.contains("no progress"),
                "{}",
                report.description
            );
            break;
        }
        assert!(Instant::now() < deadline, "never stalled: {report:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // No lease to hand on: the partitions stay where they are until a restart.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(host.owned_partitions(&topic, &consumer), vec![0, 1]);
    assert!(
        mine(&topic, &consumer)
            .iter()
            .all(|p| !p.stood_down_stalled)
    );

    wedge.wedged.store(false, Ordering::SeqCst);
    host.shutdown().await.expect("shutdown");
}
