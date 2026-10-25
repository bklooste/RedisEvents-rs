//! Port of C# `BehindTailHealthTests` (26 cases) and the `StreamsHealthCheck` halves of
//! `OwnershipTests` S6b / S6c. The grading matrix runs on a manual clock; the process-registry
//! tests share one lock because the registry is process-wide.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use super::monitor::{self, HealthClock, PartitionMonitor, Thresholds};
use super::*;
use crate::config::{ConsumerOptions, InstanceMode};
use crate::connection::Connection;
use crate::ownership::registry::{OwnershipRegistry, OwnershipRegistryOptions};
use crate::wire::Keys;

const WINDOW: u32 = 300;
const TAIL: StreamId = StreamId::new(1_700_000_000_500, 0);
const EARLIER: StreamId = StreamId::new(1_700_000_000_100, 0);
const STEP: Duration = Duration::from_secs(15);

/// A clock that moves only when told to (C# `ManualClock`).
struct ManualClock {
    base: Instant,
    unix_base_ms: i64,
    offset: Mutex<Duration>,
}

impl ManualClock {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            base: Instant::now(),
            unix_base_ms: 1_790_000_000_000,
            offset: Mutex::new(Duration::ZERO),
        })
    }

    fn advance(&self, by: Duration) {
        *self.offset.lock().unwrap_or_else(PoisonError::into_inner) += by;
    }

    fn offset(&self) -> Duration {
        *self.offset.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl HealthClock for ManualClock {
    fn now(&self) -> Instant {
        self.base + self.offset()
    }

    fn unix_ms(&self) -> i64 {
        self.unix_base_ms + i64::try_from(self.offset().as_millis()).unwrap_or(i64::MAX)
    }
}

fn thresholds(
    unhealthy_behind_seconds: u32,
    unhealthy_block_seconds: u32,
    unhealthy_stopped_seconds: u32,
) -> Thresholds {
    Thresholds {
        unhealthy_lag_ms: 120_000,
        unhealthy_block_seconds,
        unhealthy_stopped_seconds,
        unhealthy_behind_seconds,
        degraded_silence_seconds: 0,
    }
}

fn monitor_with(
    clock: &Arc<ManualClock>,
    unhealthy_behind_seconds: u32,
    start: StreamId,
    block: u32,
    stopped: u32,
    partition: u32,
) -> PartitionMonitor {
    let m = PartitionMonitor::new(
        "orders",
        "billing",
        partition,
        &format!("s:{{orders}}:{partition}"),
        thresholds(unhealthy_behind_seconds, block, stopped),
        start,
        clock.clone(),
    );
    m.mark_running();
    m
}

fn monitor(clock: &Arc<ManualClock>, start: StreamId) -> PartitionMonitor {
    monitor_with(clock, WINDOW, start, 300, 300, 0)
}

fn grade(monitors: &[&PartitionMonitor]) -> StreamHealthReport {
    let snapshots: Vec<_> = monitors.iter().map(|m| m.status()).collect();
    StreamHealth::evaluate(&snapshots, &StreamHealthContext::default())
}

/// Advances in sampler-sized steps, re-sampling the same tail each time.
fn hold(clock: &ManualClock, m: &PartitionMonitor, tail: StreamId, duration: Duration) {
    let mut elapsed = Duration::ZERO;
    while elapsed < duration {
        clock.advance(STEP);
        m.set_tail(tail);
        elapsed += STEP;
    }
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn behind_the_tail_with_a_frozen_position_is_unhealthy_once_the_window_has_passed() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    m.set_tail(TAIL);

    hold(&clock, &m, TAIL, secs(u64::from(WINDOW) - 15));
    assert_eq!(
        grade(&[&m]).status,
        StreamHealthStatus::Healthy,
        "the window has not passed yet"
    );

    hold(&clock, &m, TAIL, secs(15));
    let report = grade(&[&m]);

    assert_eq!(report.status, StreamHealthStatus::Unhealthy);
    assert_eq!(report.rule, StreamHealthRule::BehindTail);
    assert_eq!(report.partition.as_ref().unwrap().partition, 0);
    for needle in [
        "orders[0]/billing",
        "behind the tail",
        &EARLIER.to_string(),
        &TAIL.to_string(),
        "UnhealthyBehindSeconds of 300",
    ] {
        assert!(
            report.description.contains(needle),
            "{needle} not in {}",
            report.description
        );
    }
    assert_eq!(report.data["partitionsBehind"], HealthValue::Int(1));
    assert_eq!(
        report.data["behindPartition"],
        HealthValue::Text("orders[0]/billing".to_owned())
    );
}

#[test]
fn a_moving_tail_does_not_restart_the_clock_only_the_position_does() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);

    for i in 0..=u64::from(WINDOW) / 15 {
        m.set_tail(StreamId::new(TAIL.ms + i, 0));
        clock.advance(STEP);
    }

    assert_eq!(
        grade(&[&m]).rule,
        StreamHealthRule::BehindTail,
        "entries arriving behind a consumer that is not reading them is the fault, not a reason to wait longer"
    );
}

#[test]
fn behind_but_advancing_is_never_unhealthy_however_large_the_backlog() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    let far_ahead = StreamId::new(TAIL.ms + 86_400_000, 0);
    m.set_tail(far_ahead);

    // An hour of a consumer that manages one entry every 100 seconds.
    for i in 1..=36 {
        hold(&clock, &m, far_ahead, secs(90));
        m.observe(StreamId::new(EARLIER.ms + i, 0));
        clock.advance(secs(10));

        let status = m.status();
        assert!(status.is_behind_tail);
        assert!(status.behind_ms < f64::from(WINDOW) * 1000.0);
        assert_ne!(
            grade(&[&m]).status,
            StreamHealthStatus::Unhealthy,
            "any change of position restarts the clock"
        );
    }
}

#[test]
fn a_reset_that_moves_the_position_backwards_restarts_the_clock_too() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    m.set_tail(TAIL);
    hold(&clock, &m, TAIL, secs(u64::from(WINDOW) - 30));

    m.observe(StreamId::new(EARLIER.ms - 50, 0));
    hold(&clock, &m, TAIL, secs(60));

    assert_ne!(
        grade(&[&m]).status,
        StreamHealthStatus::Unhealthy,
        "a change is a change; the rule is about a position that does not move at all"
    );
}

#[test]
fn caught_up_and_idle_is_never_unhealthy() {
    let clock = ManualClock::new();
    let m = monitor(&clock, TAIL);
    m.set_tail(TAIL);

    hold(&clock, &m, TAIL, secs(24 * 3600));

    let status = m.status();
    assert!(
        !status.is_behind_tail,
        "the tail equals the position: there is nothing unread"
    );
    assert_eq!(status.behind_ms, 0.0);
    assert!(status.position_unchanged_ms >= (24.0 * 3600.0 * 1000.0));
    assert_eq!(grade(&[&m]).status, StreamHealthStatus::Healthy);
}

#[test]
fn an_entry_landing_after_a_long_idle_starts_the_clock_then_rather_than_inheriting_the_idle_time() {
    let clock = ManualClock::new();
    let m = monitor(&clock, TAIL);
    m.set_tail(TAIL);
    hold(&clock, &m, TAIL, secs(24 * 3600));

    let next = StreamId::new(TAIL.ms + 1, 0);
    m.set_tail(next);

    let status = m.status();
    assert!(status.is_behind_tail);
    assert_eq!(
        status.behind_ms, 0.0,
        "the partition has only just been seen behind"
    );
    assert_eq!(
        grade(&[&m]).status,
        StreamHealthStatus::Healthy,
        "a day of silence followed by one entry must give the consumer the whole window to read it"
    );

    hold(&clock, &m, next, secs(u64::from(WINDOW)));
    assert_eq!(
        grade(&[&m]).rule,
        StreamHealthRule::BehindTail,
        "and if it does not, that is the fault"
    );
}

#[test]
fn the_rule_is_off_by_default() {
    let clock = ManualClock::new();
    let m = monitor_with(&clock, 0, EARLIER, 300, 300, 0);
    m.set_tail(TAIL);
    hold(&clock, &m, TAIL, secs(24 * 3600));

    let status = m.status();
    assert!(
        status.is_behind_tail,
        "the signal is reported whatever the threshold"
    );
    assert!(status.behind_ms > 0.0);
    assert_eq!(
        grade(&[&m]).status,
        StreamHealthStatus::Healthy,
        "0 means no existing consumer changes behaviour"
    );
    assert_eq!(ConsumerOptions::default().unhealthy_behind_seconds, 0);
}

#[test]
fn an_unsampled_tail_is_unknown_and_unknown_is_not_unhealthy() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    clock.advance(secs(3600));

    let status = m.status();
    assert_eq!(status.tail_id, None);
    assert_eq!(status.tail_sample_age_ms, -1.0);
    assert_eq!(status.tail_unchanged_ms, -1.0);
    assert!(!status.is_behind_tail);
    assert_eq!(grade(&[&m]).status, StreamHealthStatus::Healthy);
}

#[test]
fn a_tail_sample_that_has_gone_stale_stops_counting_until_sampling_resumes() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    m.set_tail(TAIL);
    hold(&clock, &m, TAIL, secs(u64::from(WINDOW)));
    assert_eq!(grade(&[&m]).rule, StreamHealthRule::BehindTail);

    // The sampler stops answering. One missed sample is tolerated...
    clock.advance(StreamLag::TAIL_STALE_AFTER);
    assert!(
        m.status().is_behind_tail,
        "a sample inside the staleness bound still counts"
    );

    // ...but past four of them the tail is no longer something we know.
    clock.advance(secs(1));
    let stale = m.status();
    assert_eq!(
        stale.tail_id,
        Some(TAIL),
        "the last value is still reported, with its age"
    );
    assert!(stale.tail_sample_age_ms > StreamLag::TAIL_STALE_AFTER.as_secs_f64() * 1000.0);
    assert!(!stale.is_behind_tail);
    assert_eq!(stale.behind_ms, 0.0);
    assert_eq!(
        grade(&[&m]).status,
        StreamHealthStatus::Healthy,
        "a failed sample must never be what restarts a pod"
    );

    m.set_tail(TAIL);
    assert_eq!(
        grade(&[&m]).rule,
        StreamHealthRule::BehindTail,
        "the position never moved, so once the tail is confirmed again the partition was behind all along"
    );
}

#[test]
fn a_position_past_the_tail_after_a_trim_or_a_recreated_stream_is_not_behind() {
    let clock = ManualClock::new();
    let m = monitor(&clock, TAIL);
    m.set_tail(EARLIER);
    hold(&clock, &m, EARLIER, secs(3600));

    assert!(!m.status().is_behind_tail);
    assert_eq!(grade(&[&m]).status, StreamHealthStatus::Healthy);
}

#[test]
fn an_empty_or_missing_stream_is_not_behind_even_at_position_zero() {
    let clock = ManualClock::new();
    let m = monitor_with(&clock, WINDOW, StreamId::MIN, 300, 300, 0);
    m.set_tail(StreamId::MIN);
    hold(&clock, &m, StreamId::MIN, secs(3600));

    let status = m.status();
    assert_eq!(status.position, StreamId::MIN);
    assert_eq!(
        status.tail_id,
        Some(StreamId::MIN),
        "an empty stream is a known tail, distinct from an unsampled one"
    );
    assert!(
        !status.is_behind_tail,
        "0-0 is not strictly greater than 0-0"
    );
    assert_eq!(grade(&[&m]).status, StreamHealthStatus::Healthy);
}

#[test]
fn a_stream_trimmed_to_empty_behind_a_frozen_position_stops_being_behind() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    m.set_tail(TAIL);
    hold(&clock, &m, TAIL, secs(u64::from(WINDOW)));
    assert_eq!(grade(&[&m]).rule, StreamHealthRule::BehindTail);

    m.set_tail(StreamId::MIN);

    assert_eq!(
        grade(&[&m]).status,
        StreamHealthStatus::Healthy,
        "there is no longer an entry to read"
    );
}

#[test]
fn a_restarted_consumer_that_has_processed_nothing_is_measured_from_its_stored_position() {
    // The stored position is the tail: caught up, restarted, and the topic stays quiet.
    let clock = ManualClock::new();
    let m = monitor(&clock, TAIL);
    m.set_tail(TAIL);
    hold(&clock, &m, TAIL, secs(3600));

    let status = m.status();
    assert_eq!(
        status.last_processed,
        StreamId::MIN,
        "nothing has been processed since the restart"
    );
    assert_eq!(
        status.position, TAIL,
        "but the partition is not at 0-0, it is where it resumed from"
    );
    assert!(!status.is_behind_tail);
    assert_eq!(grade(&[&m]).status, StreamHealthStatus::Healthy);
}

#[test]
fn the_clock_starts_when_the_partition_is_acquired_not_when_the_process_started() {
    // The process has been up for a day before this instance takes the partition over.
    let clock = ManualClock::new();
    clock.advance(secs(24 * 3600));

    let m = monitor(&clock, EARLIER);
    m.set_tail(TAIL);

    let status = m.status();
    assert_eq!(status.position_unchanged_ms, 0.0);
    assert_eq!(status.behind_ms, 0.0);
    assert_eq!(
        grade(&[&m]).status,
        StreamHealthStatus::Healthy,
        "a new owner inherits a backlog, not the old owner's stall time"
    );

    hold(&clock, &m, TAIL, secs(u64::from(WINDOW)));
    assert_eq!(
        grade(&[&m]).rule,
        StreamHealthRule::BehindTail,
        "and gets exactly one window to start moving"
    );
}

#[test]
fn a_blocked_partition_answers_to_its_block_threshold_not_to_the_behind_window() {
    let clock = ManualClock::new();
    let m = monitor_with(&clock, 60, EARLIER, 3600, 300, 0);
    m.set_tail(TAIL);
    m.mark_blocked();

    hold(&clock, &m, TAIL, secs(600));

    let inside = grade(&[&m]);
    assert_eq!(
        inside.status,
        StreamHealthStatus::Degraded,
        "an hour of retrying was configured; the shorter behind window must not cut it to a minute"
    );
    assert_eq!(inside.rule, StreamHealthRule::Blocked);
    assert!(
        m.status().is_behind_tail,
        "the signal itself is still reported"
    );

    hold(&clock, &m, TAIL, secs(3600));

    let past = grade(&[&m]);
    assert_eq!(past.rule, StreamHealthRule::BlockedTooLong);
    assert!(
        past.description.contains("blocked")
            && past.description.contains("UnhealthyBlockSeconds of 3600")
    );
}

#[test]
fn a_policy_stop_stays_degraded_however_long_it_sits_behind_the_tail() {
    let clock = ManualClock::new();
    let running = monitor_with(&clock, WINDOW, TAIL, 300, 300, 1);
    running.set_tail(TAIL);

    let m = monitor_with(&clock, 60, EARLIER, 300, 0, 0);
    m.set_tail(TAIL);
    m.set_lag_entries(1_000_000);
    m.mark_stopped(
        "ErrorPolicy.StopPartition after InvalidOperationException: poison",
        false,
    );

    hold(&clock, &m, TAIL, secs(3600));
    running.set_tail(TAIL);

    assert!(!m.status().escalates);

    let report = grade(&[&m, &running]);
    assert_eq!(
        report.status,
        StreamHealthStatus::Degraded,
        "restarting would only replay the poison entry the operator chose to stop on"
    );
    assert_eq!(report.rule, StreamHealthRule::Stopped);
}

#[test]
fn an_escalating_stop_is_reported_as_the_stop_it_is() {
    let clock = ManualClock::new();
    let running = monitor_with(&clock, WINDOW, TAIL, 300, 300, 1);
    running.set_tail(TAIL);

    let m = monitor_with(&clock, 60, EARLIER, 300, 120, 0);
    m.set_tail(TAIL);
    m.set_lag_entries(42);
    m.mark_stopped(
        "contested position: another instance was also writing it",
        true,
    );

    hold(&clock, &m, TAIL, secs(90));
    running.set_tail(TAIL);

    assert!(m.status().escalates);
    assert_eq!(
        grade(&[&m, &running]).rule,
        StreamHealthRule::Stopped,
        "a stopped partition waits for UnhealthyStoppedSeconds, not the shorter behind window"
    );

    hold(&clock, &m, TAIL, secs(60));
    running.set_tail(TAIL);

    let report = grade(&[&m, &running]);
    assert_eq!(report.rule, StreamHealthRule::StoppedTooLong);
    assert!(
        report.description.contains("stood down")
            && report.description.contains("contested position")
    );
}

#[test]
fn escalates_reads_false_on_a_partition_that_is_no_longer_stopped() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    m.mark_stopped("contested position", true);
    assert!(m.status().escalates);

    m.mark_running();
    assert!(!m.status().escalates);
}

#[test]
fn a_partition_that_never_gets_past_starting_is_caught_by_the_same_rule() {
    let clock = ManualClock::new();
    let m = PartitionMonitor::new(
        "orders",
        "billing",
        0,
        "s:{orders}:0",
        thresholds(WINDOW, 300, 300),
        EARLIER,
        clock.clone(),
    );
    m.set_tail(TAIL);

    hold(&clock, &m, TAIL, secs(u64::from(WINDOW)));

    let report = grade(&[&m]);
    assert_eq!(report.rule, StreamHealthRule::BehindTail);
    assert!(report.description.contains("has not started reading"));
}

#[test]
fn a_blocked_partition_past_its_threshold_is_named_ahead_of_a_behind_one() {
    let clock = ManualClock::new();
    let blocked = monitor_with(&clock, WINDOW, EARLIER, 60, 300, 0);
    let frozen = monitor_with(&clock, WINDOW, EARLIER, 300, 300, 1);
    blocked.mark_blocked();
    blocked.set_tail(TAIL);
    frozen.set_tail(TAIL);

    clock.advance(secs(u64::from(WINDOW)));
    blocked.set_tail(TAIL);
    frozen.set_tail(TAIL);

    let report = grade(&[&frozen, &blocked]);
    assert_eq!(
        report.rule,
        StreamHealthRule::BlockedTooLong,
        "the declared state is the more specific diagnosis"
    );
    assert_eq!(report.data["partitionsBehind"], HealthValue::Int(2));
}

#[test]
fn the_time_the_tail_last_changed_ignores_a_trim_and_follows_new_entries() {
    let clock = ManualClock::new();
    let m = monitor(&clock, TAIL);
    m.set_tail(TAIL);
    hold(&clock, &m, TAIL, secs(600));
    assert_eq!(m.status().tail_unchanged_ms, 600_000.0);

    m.set_tail(StreamId::MIN);
    clock.advance(STEP);
    assert_eq!(
        m.status().tail_unchanged_ms,
        615_000.0,
        "emptying the stream is not something being written"
    );

    m.set_tail(StreamId::new(TAIL.ms + 1, 0));
    let status = m.status();
    assert_eq!(status.tail_unchanged_ms, 0.0);
    assert_eq!(status.tail_sample_age_ms, 0.0);
}

#[test]
fn the_tail_unchanged_clock_counts_from_the_first_sample_on_a_stream_that_has_never_had_an_entry() {
    let clock = ManualClock::new();
    let m = monitor(&clock, StreamId::MIN);
    clock.advance(secs(3600));
    m.set_tail(StreamId::MIN);
    hold(&clock, &m, StreamId::MIN, secs(120));

    let status = m.status();
    assert_eq!(status.tail_unchanged_ms, 120_000.0);
    assert_eq!(
        status.position_unchanged_ms, 3_720_000.0,
        "the position clock has run since the partition was acquired"
    );
}

#[test]
fn the_sampler_tick_notices_a_position_change_without_a_sample() {
    let clock = ManualClock::new();
    let m = monitor(&clock, EARLIER);
    m.set_tail(TAIL);
    clock.advance(secs(200));

    m.observe(StreamId::new(EARLIER.ms + 1, 0));
    m.refresh_progress();
    clock.advance(secs(200));
    m.set_tail(TAIL);

    let status = m.status();
    assert_eq!(
        status.position_unchanged_ms, 200_000.0,
        "the change was stamped when the tick saw it, not when the snapshot was taken"
    );
    assert_eq!(status.behind_ms, 200_000.0);
    assert_ne!(grade(&[&m]).status, StreamHealthStatus::Unhealthy);
}

#[test]
fn a_hand_built_snapshot_grades_without_the_new_members_and_is_never_behind() {
    let status = StreamPartitionStatus {
        topic: "orders".to_owned(),
        consumer: "billing".to_owned(),
        partition: 0,
        state: StreamPartitionRunState::Running,
        stop_reason: None,
        last_processed: EARLIER,
        is_caught_up: false,
        lag_ms: 0.0,
        lag_entries: 5,
        blocked_ms: 0.0,
        stopped_ms: 0.0,
        unhealthy_lag_ms: 120_000,
        unhealthy_block_seconds: 300,
        unhealthy_stopped_seconds: 300,
        unhealthy_behind_seconds: 0,
        degraded_silence_seconds: 0,
        escalates: false,
        position: StreamId::MIN,
        position_unchanged_ms: 0.0,
        tail_id: None,
        tail_sample_age_ms: -1.0,
        tail_unchanged_ms: -1.0,
        is_behind_tail: false,
        behind_ms: 0.0,
    };

    assert_eq!(
        StreamHealth::grade(&status),
        StreamPartitionHealth {
            status: StreamHealthStatus::Healthy,
            rule: StreamHealthRule::None
        }
    );

    // A duration alone, without the tail comparison behind it, is not a verdict.
    let duration_only = StreamPartitionStatus {
        unhealthy_behind_seconds: 1,
        behind_ms: 10_000_000.0,
        ..status.clone()
    };
    assert_eq!(
        StreamHealth::grade(&duration_only).status,
        StreamHealthStatus::Healthy
    );

    let behind = StreamPartitionStatus {
        is_behind_tail: true,
        ..duration_only
    };
    assert_eq!(
        StreamHealth::grade(&behind),
        StreamPartitionHealth {
            status: StreamHealthStatus::Unhealthy,
            rule: StreamHealthRule::BehindTail
        }
    );
}

#[test]
fn process_level_facts_come_through_the_context() {
    let clock = ManualClock::new();
    let m = monitor(&clock, TAIL);
    m.set_tail(TAIL);
    let snapshot = [m.status()];

    let down = StreamHealth::evaluate(
        &snapshot,
        &StreamHealthContext {
            connection_fault: Some("redis:6379".to_owned()),
            ..Default::default()
        },
    );
    assert_eq!(down.status, StreamHealthStatus::Unhealthy);
    assert_eq!(down.rule, StreamHealthRule::ConnectionDown);
    assert!(down.description.contains("redis:6379"));

    let never_started = StreamHealth::evaluate(
        &snapshot,
        &StreamHealthContext {
            consumers: 2,
            consumers_started: 0,
            ..Default::default()
        },
    );
    assert_eq!(never_started.rule, StreamHealthRule::NoConsumerStarted);

    let ownership = StreamHealth::evaluate(
        &snapshot,
        &StreamHealthContext {
            ownership_degraded: true,
            ..Default::default()
        },
    );
    assert_eq!(ownership.status, StreamHealthStatus::Degraded);
    assert_eq!(ownership.rule, StreamHealthRule::OwnershipDegraded);

    assert_eq!(
        StreamHealth::evaluate(&[], &StreamHealthContext::default()).status,
        StreamHealthStatus::Healthy,
        "a producer-only process has nothing to grade"
    );
}

// ------------------------------------------------------------------ process registry

/// Serialises the tests that touch the process-wide monitor registry and ownership registry.
async fn registry_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

#[tokio::test]
async fn the_health_check_reports_what_the_core_grading_decided() {
    let _guard = registry_guard().await;
    monitor::clear();
    let clock = ManualClock::new();
    let position = EARLIER;
    let tail = TAIL;

    let t = thresholds(30, 300, 300);
    let m = monitor::track_with_clock(
        "orders",
        "billing",
        0,
        "s:{orders}:0",
        t,
        position,
        clock.clone(),
    )
    .expect("track");
    m.mark_running();
    m.set_tail(tail);

    let before = StreamHealth::check(None, 0, 0).await;
    assert_ne!(before.status, StreamHealthStatus::Unhealthy);

    clock.advance(secs(30));
    m.set_tail(tail);

    let core = StreamHealth::evaluate(&StreamStatus::partitions(), &StreamHealthContext::default());
    let result = StreamHealth::check(None, 0, 0).await;

    assert_eq!(core.rule, StreamHealthRule::BehindTail);
    assert_eq!(result.status, StreamHealthStatus::Unhealthy);
    assert_eq!(result.description, core.description);
    assert_eq!(
        result.data["behindPartition"],
        HealthValue::Text("orders[0]/billing".to_owned())
    );
    monitor::forget(&m);
}

fn owned(index: u32, count: u32) -> Vec<u32> {
    crate::config::instance::InstanceResolver::owned(8, count, index)
        .iter()
        .collect()
}

fn static_pod(
    conn: &Connection,
    topic: &str,
    consumer: &str,
    owned: Vec<u32>,
    pod: &str,
) -> OwnershipRegistry {
    let mut options = OwnershipRegistryOptions::new(topic, consumer, 8);
    options.owned_partitions = owned;
    options.pod_name = pod.to_owned();
    options.mode = InstanceMode::Static;
    options.instance_id = uuid::Uuid::new_v4();
    OwnershipRegistry::new(conn, &Keys::with_prefix("test:re:"), options).expect("valid options")
}

/// Health half of S6b: a missing instance leaves partitions 6,7 unowned, and the health check is
/// Degraded with the ownership rule.
#[tokio::test]
async fn s6b_health_is_degraded_when_the_ownership_registry_shows_a_gap() {
    let redis = redisevents_test_support::require_redis!();
    let _guard = registry_guard().await;
    monitor::clear();
    let conn = Connection::connect(redis.url()).await.expect("connect");
    let topic = redis.new_topic("s6b-health");
    let consumer = redis.new_consumer("s6b-health");

    let pod0 = static_pod(&conn, &topic, &consumer, owned(0, 3), "svc-0");
    let pod1 = static_pod(&conn, &topic, &consumer, owned(1, 3), "svc-1");
    pod0.start().await.unwrap();
    pod1.start().await.unwrap();
    pod1.refresh().await.unwrap();

    let m = monitor::track(
        "orders",
        &consumer,
        0,
        "s:{orders}:0",
        thresholds(0, 300, 300),
        StreamId::MIN,
    )
    .expect("track");
    m.mark_running();

    let report = StreamHealth::check(Some(&conn), 0, 0).await;
    assert_eq!(report.status, StreamHealthStatus::Degraded);
    assert_eq!(report.rule, StreamHealthRule::OwnershipDegraded);
    assert!(
        report
            .description
            .contains("ownership registry shows a gap or an overlap")
    );
    assert_eq!(report.data["ownershipDegraded"], HealthValue::Bool(true));

    monitor::forget(&m);
    pod0.dispose().await;
    pod1.dispose().await;
}

/// Health half of S6c: two pods both claiming everything make the map contested, so the health
/// check is Degraded with the ownership rule.
#[tokio::test]
async fn s6c_health_is_degraded_when_two_instances_claim_the_same_partitions() {
    let redis = redisevents_test_support::require_redis!();
    let _guard = registry_guard().await;
    monitor::clear();
    let conn = Connection::connect(redis.url()).await.expect("connect");
    let topic = redis.new_topic("s6c-health");
    let consumer = redis.new_consumer("s6c-health");

    let everything: Vec<u32> = (0..8).collect();
    let pod_a = static_pod(
        &conn,
        &topic,
        &consumer,
        everything.clone(),
        "deploy-abc12-x2k9p",
    );
    let pod_b = static_pod(&conn, &topic, &consumer, everything, "deploy-abc12-7f4dq");
    pod_a.start().await.unwrap();
    pod_b.start().await.unwrap();

    let m = monitor::track(
        "orders",
        &consumer,
        0,
        "s:{orders}:0",
        thresholds(0, 300, 300),
        StreamId::MIN,
    )
    .expect("track");
    m.mark_running();

    let report = StreamHealth::check(Some(&conn), 0, 0).await;
    assert_eq!(report.status, StreamHealthStatus::Degraded);
    assert_eq!(report.rule, StreamHealthRule::OwnershipDegraded);
    assert!(
        report
            .description
            .contains("ownership registry shows a gap or an overlap")
    );
    assert_eq!(report.data["ownershipDegraded"], HealthValue::Bool(true));

    monitor::forget(&m);
    pod_a.dispose().await;
    pod_b.dispose().await;
}

/// The sampler reads the tail and the entries-behind estimate from the real stream: five entries
/// at 100..500 ms, a position at 200 ms, so `5 * (500 - 200) / (500 - 100) = 3` entries behind. A
/// stream that does not exist is an empty one.
#[tokio::test]
async fn the_sampler_reads_tail_and_entries_behind_from_the_stream() {
    let redis = redisevents_test_support::require_redis!();
    let _guard = registry_guard().await;
    monitor::clear();
    let conn = Connection::connect(redis.url()).await.expect("connect");
    let topic = redis.new_topic("sampler");
    let key = redis.keys().stream(&topic, 0, false);
    let missing_key = redis
        .keys()
        .stream(&redis.new_topic("sampler-missing"), 0, false);

    let mut writer = conn.shared();
    for ms in [100u64, 200, 300, 400, 500] {
        redis::cmd("XADD")
            .arg(&key)
            .arg(format!("{ms}-0"))
            .arg("b")
            .arg("x")
            .query_async::<String>(&mut writer)
            .await
            .expect("xadd");
    }

    let clock = ManualClock::new();
    let t = thresholds(0, 300, 300);
    let present = monitor::track_with_clock(
        &topic,
        "c",
        0,
        &key,
        t,
        StreamId::new(200, 0),
        clock.clone(),
    )
    .expect("track");
    present.mark_running();
    let absent = monitor::track_with_clock(
        &topic,
        "c",
        1,
        &missing_key,
        t,
        StreamId::MIN,
        clock.clone(),
    )
    .expect("track");
    absent.mark_running();

    monitor::sample_once(&conn).await;

    let status = present.status();
    assert_eq!(status.tail_id, Some(StreamId::new(500, 0)));
    assert_eq!(status.lag_entries, 3);

    let empty = absent.status();
    assert_eq!(
        empty.tail_id,
        Some(StreamId::MIN),
        "a missing stream is a known, empty tail"
    );
    assert_eq!(empty.lag_entries, 0);

    monitor::forget(&present);
    monitor::forget(&absent);
}
