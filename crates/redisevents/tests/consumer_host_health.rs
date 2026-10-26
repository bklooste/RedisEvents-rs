//! The consumer host's readiness answer for a partition blocked on `HandleError::Block` (C#
//! `PartitionWorker` → `Monitor.MarkBlocked` → `StreamHealth`). Its own test binary: health reads
//! every partition monitor in the process, so other host tests running in parallel would leak in.
//! Skips without Redis.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use redisevents::admin::health::{StreamHealthReport, StreamHealthRule, StreamHealthStatus};
use redisevents::config::{ConsumerOptions, StreamOptions, TopicOptions};
use redisevents::consumer::{HandleError, Streams, StreamsHost};
use redisevents::producer::{PublishOptions, Publisher, StreamPublisher};
use redisevents::{BatchHandler, HandleResult, PartitionContext, StreamMsg};
use redisevents_test_support::{RedisFixture, require_redis};

/// Blocks while `failing` is set, then succeeds.
struct Blocking {
    failing: Arc<AtomicBool>,
}

impl BatchHandler for Blocking {
    #[allow(clippy::unused_async_trait_impl)]
    async fn handle_batch(&self, _batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
        if self.failing.load(Ordering::SeqCst) {
            return Err(HandleError::block(std::io::Error::other("downstream down")));
        }
        Ok(())
    }
}

async fn spawn(redis: &RedisFixture, topic: &str, handler: Blocking) -> StreamsHost {
    let mut options = StreamOptions::default();
    options.topics.insert(
        topic.to_owned(),
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    );
    Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(options)
        .topic(topic)
        .options(ConsumerOptions {
            topic: topic.to_owned(),
            consumer: Some(redis.new_consumer("c")),
            persist_interval_ms: 50,
            block_ms: 200,
            ..ConsumerOptions::default()
        })
        .handler(handler)
        .spawn()
        .await
        .expect("spawn")
}

async fn wait_health(
    host: &StreamsHost,
    what: &str,
    ok: impl Fn(&StreamHealthReport) -> bool,
) -> StreamHealthReport {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let report = host.health().await;
        if ok(&report) {
            return report;
        }
        assert!(Instant::now() < deadline, "{what}: last report {report:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_partition_degrades_health_until_its_batch_succeeds() {
    let redis = require_redis!();
    let topic = redis.new_topic("host-health-blocked");
    let publisher = StreamPublisher::new(
        redis.connect().await,
        redis.keys(),
        &topic,
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await
    .expect("publisher");
    publisher
        .publish(
            "k",
            Bytes::from_static(b"x"),
            "t",
            &PublishOptions::default(),
        )
        .await
        .expect("publish");

    let failing = Arc::new(AtomicBool::new(true));
    let host = spawn(
        redis,
        &topic,
        Blocking {
            failing: failing.clone(),
        },
    )
    .await;

    let report = wait_health(&host, "the blocked partition shows", |r| {
        r.rule == StreamHealthRule::Blocked
    })
    .await;
    assert_eq!(report.status, StreamHealthStatus::Degraded, "{report:?}");
    assert_eq!(report.details.partitions_blocked, Some(1), "{report:?}");

    // The next retry (1 s back-off) succeeds: Running again.
    failing.store(false, Ordering::SeqCst);
    let report = wait_health(&host, "health recovers", |r| {
        r.status == StreamHealthStatus::Healthy
    })
    .await;
    assert_eq!(report.rule, StreamHealthRule::None);
    assert_eq!(report.details.partitions_blocked, Some(0));
    host.shutdown().await.expect("shutdown");
}
