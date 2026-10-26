//! The stall clock (C# #52 `StalledPartitionGradingTests`, 1:1), on paused tokio time: a partition
//! that makes no progress (no read returning, no batch completing, no position write landing) is
//! graded Unhealthy, rule `Stalled`, on this process's own clock, where behind-the-tail needs a
//! fresh tail sample from the same broken connection.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::monitor::{self, HealthClock, PartitionMonitor, Thresholds};
use super::*;

const WINDOW: u32 = 300;
const START: StreamId = StreamId::new(1_700_000_000_000, 0);

/// Monotonic time from tokio's (pausable) clock; wall time follows it from a fixed base.
struct TokioClock {
    base: tokio::time::Instant,
}

impl HealthClock for TokioClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }

    fn unix_ms(&self) -> i64 {
        1_790_000_000_000 + i64::try_from(self.base.elapsed().as_millis()).unwrap_or(i64::MAX)
    }
}

fn clock() -> Arc<TokioClock> {
    Arc::new(TokioClock {
        base: tokio::time::Instant::now(),
    })
}

fn monitor_on(
    clock: &Arc<TokioClock>,
    unhealthy_stalled_seconds: u32,
    partition: u32,
) -> PartitionMonitor {
    let m = PartitionMonitor::new(
        "mult_feed_odds",
        "offer_odds_proj",
        partition,
        &format!("s:{{mult_feed_odds}}:{partition}"),
        Thresholds {
            unhealthy_lag_ms: 120_000,
            unhealthy_block_seconds: 300,
            unhealthy_stopped_seconds: 300,
            unhealthy_behind_seconds: 300,
            degraded_silence_seconds: 0,
            unhealthy_stalled_seconds,
        },
        START,
        clock.clone(),
    );
    m.mark_running();
    m
}

fn evaluate_all(monitors: &[&PartitionMonitor]) -> StreamHealthReport {
    let snapshots: Vec<_> = monitors.iter().map(|m| m.status()).collect();
    let context = StreamHealthContext {
        consumers: 1,
        consumers_started: 1,
        ..StreamHealthContext::default()
    };
    evaluate(&snapshots, &context)
}

fn grade_of(m: &PartitionMonitor) -> StreamPartitionHealth {
    grade(&m.status())
}

async fn advance(seconds: u64) {
    tokio::time::advance(Duration::from_secs(seconds)).await;
}

#[tokio::test(start_paused = true)]
async fn a_partition_with_no_progress_past_the_window_is_unhealthy_even_with_no_tail_sample() {
    let clock = clock();
    let m = monitor_on(&clock, WINDOW, 0);
    let now = u64::try_from(clock.unix_ms()).unwrap();
    m.observe(StreamId::new(now, 0));
    // The last sample before the connection went bad: behind, with entries waiting. It goes stale,
    // so behind-the-tail stands down.
    m.set_tail(StreamId::new(now + 10_000, 0));
    m.set_lag_entries(4_000);

    advance(u64::from(WINDOW) + 1).await;

    let status = m.status();
    assert!(
        !status.is_behind_tail,
        "a tail sample this old is not knowledge of the tail"
    );
    assert!(status.stalled_ms >= f64::from(WINDOW) * 1000.0);
    let report = evaluate_all(&[&m]);
    assert_eq!(report.status, StreamHealthStatus::Unhealthy);
    assert_eq!(report.rule, StreamHealthRule::Stalled);
    assert!(
        report.description.contains("no progress"),
        "{}",
        report.description
    );
    assert!(
        report
            .description
            .contains(&format!("UnhealthyStalledSeconds of {WINDOW}")),
        "{}",
        report.description
    );
    assert_eq!(report.partition.unwrap().partition, 0);
    assert_eq!(
        report.details.stalled_partition.as_deref(),
        Some("mult_feed_odds[0]/offer_odds_proj")
    );
}

#[tokio::test(start_paused = true)]
async fn with_the_stall_rule_off_the_same_partition_is_only_degraded() {
    let clock = clock();
    let m = monitor_on(&clock, 0, 0);
    let now = u64::try_from(clock.unix_ms()).unwrap();
    m.observe(StreamId::new(now, 0));
    m.set_tail(StreamId::new(now + 10_000, 0));
    m.set_lag_entries(4_000);

    advance(u64::from(WINDOW) + 1).await;

    let report = evaluate_all(&[&m]);
    assert_eq!(report.status, StreamHealthStatus::Degraded);
    assert_eq!(report.rule, StreamHealthRule::Lagging);
}

#[tokio::test(start_paused = true)]
async fn an_idle_consumer_completing_empty_reads_never_stalls() {
    let clock = clock();
    let m = monitor_on(&clock, WINDOW, 0);
    for _ in 0..WINDOW * 3 {
        advance(1).await;
        m.mark_fetched();
    }
    assert!(m.status().stalled_ms < 2_000.0);
    assert_eq!(evaluate_all(&[&m]).status, StreamHealthStatus::Healthy);
}

#[tokio::test(start_paused = true)]
async fn a_slow_handler_that_completes_batches_never_stalls() {
    let clock = clock();
    let m = monitor_on(&clock, WINDOW, 0);
    for i in 1..=10 {
        advance(u64::from(WINDOW) - 10).await;
        m.observe(StreamId::new(1_700_000_000_000 + i, 0));
    }
    assert_ne!(grade_of(&m).rule, StreamHealthRule::Stalled);
}

#[tokio::test(start_paused = true)]
async fn a_handler_that_never_returns_stalls_even_while_empty_reads_complete() {
    let clock = clock();
    let m = monitor_on(&clock, WINDOW, 0);
    m.mark_busy();
    for _ in 0..=WINDOW {
        advance(1).await;
        m.mark_fetched();
    }
    assert_eq!(grade_of(&m).rule, StreamHealthRule::Stalled);

    m.mark_idle();
    assert_ne!(
        grade_of(&m).rule,
        StreamHealthRule::Stalled,
        "the batch finishing is progress"
    );
}

#[tokio::test(start_paused = true)]
async fn a_partition_whose_reads_keep_failing_stalls() {
    let clock = clock();
    let m = monitor_on(&clock, WINDOW, 0);
    m.mark_transport_blocked();
    advance(u64::from(WINDOW) + 1).await;

    assert!(m.is_blocked());
    assert_eq!(
        grade_of(&m),
        StreamPartitionHealth {
            status: StreamHealthStatus::Unhealthy,
            rule: StreamHealthRule::Stalled
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_handler_block_is_graded_by_the_block_rule_not_the_stall_rule() {
    let clock = clock();
    let m = monitor_on(&clock, 10, 0);
    m.mark_blocked();
    advance(60).await;

    assert_eq!(m.status().stalled_ms, 0.0);
    assert_eq!(
        grade_of(&m),
        StreamPartitionHealth {
            status: StreamHealthStatus::Degraded,
            rule: StreamHealthRule::Blocked
        }
    );

    m.mark_unblocked();
    assert!(
        m.status().stalled_ms < 1_000.0,
        "the end of a block is progress"
    );
}

#[tokio::test(start_paused = true)]
async fn position_writes_that_keep_failing_stall_a_partition_that_is_still_reading() {
    let clock = clock();
    let m = monitor_on(&clock, WINDOW, 0);
    m.mark_fetched();
    m.set_persist_failing(Duration::from_secs(u64::from(WINDOW) + 5));
    assert_eq!(grade_of(&m).rule, StreamHealthRule::Stalled);

    m.set_persist_failing(Duration::ZERO);
    assert_ne!(
        grade_of(&m).rule,
        StreamHealthRule::Stalled,
        "a write that lands clears it"
    );
}

#[tokio::test(start_paused = true)]
async fn a_stalled_out_partition_is_unhealthy_until_it_is_read_again() {
    let clock = clock();
    let sibling = monitor_on(&clock, WINDOW, 1);
    sibling.mark_fetched();
    let m = monitor_on(&clock, WINDOW, 0);
    m.mark_stalled_out("stalled: no progress for 300s (no read returned and no batch completed)");

    let status = m.status();
    assert!(status.stood_down_stalled);
    assert_eq!(status.state, StreamPartitionRunState::Stopped);

    let report = evaluate_all(&[&sibling, &m]);
    assert_eq!(report.status, StreamHealthStatus::Unhealthy);
    assert_eq!(report.rule, StreamHealthRule::Stalled);
    assert!(
        report.description.contains("stood down"),
        "{}",
        report.description
    );
    assert!(
        report.description.contains("restart"),
        "{}",
        report.description
    );

    m.mark_running();
    assert_ne!(grade_of(&m).rule, StreamHealthRule::Stalled);
}

#[tokio::test(start_paused = true)]
async fn a_policy_stop_is_not_a_stall() {
    let clock = clock();
    let m = monitor_on(&clock, 10, 0);
    m.mark_stopped("ErrorPolicy.StopPartition after an error: boom", false);
    advance(60).await;

    assert_eq!(m.status().stalled_ms, 0.0);
    assert_eq!(grade_of(&m).rule, StreamHealthRule::Stopped);
}

#[tokio::test(start_paused = true)]
async fn a_partition_that_never_manages_its_first_read_is_stalled_from_acquisition() {
    let clock = clock();
    let m = monitor_on(&clock, 5, 0);
    advance(4).await;
    assert_ne!(grade_of(&m).rule, StreamHealthRule::Stalled);
    advance(1).await;
    assert_eq!(grade_of(&m).rule, StreamHealthRule::Stalled);
}

#[tokio::test]
async fn forgetting_a_replaced_monitor_leaves_the_live_one_registered() {
    let _guard = super::tests::registry_guard().await;
    let thresholds = Thresholds {
        unhealthy_lag_ms: 120_000,
        unhealthy_block_seconds: 300,
        unhealthy_stopped_seconds: 300,
        unhealthy_behind_seconds: 0,
        degraded_silence_seconds: 0,
        unhealthy_stalled_seconds: WINDOW,
    };
    let old = monitor::track(
        "stall-forget",
        "c",
        0,
        "s:{stall-forget}:0",
        thresholds,
        START,
    )
    .unwrap();
    old.mark_stalled_out("stalled");
    let live = monitor::track(
        "stall-forget",
        "c",
        0,
        "s:{stall-forget}:0",
        thresholds,
        START,
    )
    .unwrap();

    monitor::forget(&old);
    let registered: Vec<_> = monitor::all()
        .into_iter()
        .filter(|m| m.topic() == "stall-forget")
        .collect();
    assert_eq!(registered.len(), 1);
    assert!(
        Arc::ptr_eq(&registered[0], &live),
        "the stalled-out monitor's forget must not take the live one"
    );

    monitor::forget(&live);
    assert!(monitor::all().iter().all(|m| m.topic() != "stall-forget"));
}
