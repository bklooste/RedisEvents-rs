//! Redis-backed tests of the fetch layer: ports of `ReaderThreadTests.cs` and
//! `ReadModeComparisonTests.cs` at the fetch level (the host, C4, and the worker, C2, are not here
//! yet), plus cancellation, seek and reconnect tests.
//!
//! These are unit tests (the fetch types are crate-private), so they cannot use
//! `RedisFixture::connect` (it returns the non-test build's `Connection`); they connect from
//! `fixture.url()` instead.
//!
//! Skipped C# tests, with reasons:
//! - `S21c_reader_thread_count_follows_the_configured_worker_count`: there is no `SocketManager`
//!   and no reader thread in Rust; a redis-rs connection is a task on the tokio runtime.
//!   `ReaderThreads` maps to a number of reader connections, which C4 opens.
//! - `S21d_handler_runs_on_the_thread_pool_never_on_a_reader_thread`: handler placement is C2/C4's
//!   (there is no reader thread for a handler to land on); C2t/C4t cover delivery.
//!
//! `S21c_each_block_consumer_costs_exactly_one_named_reader_thread` is ported as "costs exactly one
//! connection, closed again on drop", the Rust cost of a reader.

#![allow(clippy::assert_is_empty)]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::{Fetch, ReaderConnection, StreamBatch};
use crate::Connection;
use crate::wire::{CODEC_VERSION, EntryFields, HeaderBlock, Keys, StreamId};

const MAX_IDLE_DELAY_MS: u32 = 50;

fn keys() -> Keys {
    Keys::with_prefix("test:re:")
}

async fn connect(fixture: &redisevents_test_support::RedisFixture) -> Connection {
    Connection::connect(fixture.url()).await.expect("connect")
}

async fn xadd(conn: &Connection, key: &str, partition_key: &str, body: &[u8]) -> StreamId {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(key).arg("*");
    EntryFields {
        body,
        msg_type: "readmode",
        partition_key: Some(partition_key),
        correlation_id: None,
        trace_parent: None,
        headers: &HeaderBlock::empty(),
    }
    .write_to(&mut cmd);
    let id: String = cmd.query_async(&mut conn.shared()).await.expect("XADD");
    StreamId::parse(&id).unwrap()
}

async fn client_ids(conn: &Connection) -> HashSet<i64> {
    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut conn.shared())
        .await
        .expect("CLIENT LIST");
    list.lines()
        .filter_map(|l| l.split(' ').find_map(|f| f.strip_prefix("id=")))
        .filter_map(|id| id.parse().ok())
        .collect()
}

async fn xread_calls(conn: &Connection) -> u64 {
    let info: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut conn.shared())
        .await
        .expect("INFO");
    info.lines()
        .find_map(|l| l.strip_prefix("cmdstat_xread:"))
        .and_then(|l| l.split(',').find_map(|f| f.strip_prefix("calls=")))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

fn block(conn: &Connection, block_ms: u32, key: &str, batch: u32) -> Fetch {
    let reader = ReaderConnection::new(conn, block_ms).unwrap();
    Fetch::block(
        reader,
        vec![(0, key.to_owned(), StreamId::MIN)],
        batch,
        CODEC_VERSION,
    )
    .unwrap()
}

fn poll(conn: &Connection, key: &str, batch: u32) -> Fetch {
    Fetch::poll(
        conn,
        vec![(0, key.to_owned(), StreamId::MIN)],
        batch,
        MAX_IDLE_DELAY_MS,
        CODEC_VERSION,
    )
    .unwrap()
}

fn total(batches: &[StreamBatch]) -> usize {
    batches.iter().map(|b| b.msgs.len()).sum()
}

// ------------------------------------------------------------------------------------------------
// ReaderThreadTests
// ------------------------------------------------------------------------------------------------

/// S21c, ported: N blocking readers cost exactly N server connections, and every one is closed
/// again once its reader is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s21c_each_block_consumer_costs_exactly_one_named_reader_thread() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("s21c"), 0, false);
    for consumers in [1usize, 4, 8] {
        let mut fetches: Vec<Fetch> = (0..consumers).map(|_| block(&conn, 50, &key, 10)).collect();
        let mut readers = Vec::new();
        for f in &mut fetches {
            assert!(f.fetch().await.unwrap().is_empty());
            let super::Source::Block(r) = &f.source else {
                unreachable!()
            };
            // Two reads, one socket: the reader keeps its connection between reads.
            let r = r.clone();
            assert!(f.fetch().await.unwrap().is_empty());
            readers.push(r);
        }
        let mut ids = HashSet::new();
        for r in &readers {
            ids.insert(r.client_id().await.unwrap());
        }
        assert_eq!(
            ids.len(),
            consumers,
            "each blocking reader costs exactly one connection"
        );
        assert!(ids.is_subset(&client_ids(&conn).await));
        drop(readers);
        drop(fetches);
        let probe = conn.clone();
        redisevents_test_support::wait_until_async(
            || {
                let probe = probe.clone();
                let ids = ids.clone();
                async move { client_ids(&probe).await.is_disjoint(&ids) }
            },
            Some(Duration::from_secs(15)),
            "every reader connection to close once its reader is dropped",
        )
        .await;
    }
}

/// S21d, ported to the fetch level: two partitions read by separate loops sharing one reader
/// connection (`CoLocatePartitions = false`). Partition 0's "handler" blocks its thread for 4 s;
/// partition 1's message, published afterwards, still arrives while it is blocked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s21d_inline_block_handler_does_not_occupy_the_reader_connection() {
    const HANDLER_BLOCK: Duration = Duration::from_secs(4);
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let topic = fixture.new_topic("s21d");
    let (key_a, key_b) = (
        keys().stream(&topic, 0, false),
        keys().stream(&topic, 1, false),
    );
    let reader = ReaderConnection::new(&conn, 250).unwrap();
    let mut fetch_a = Fetch::block(
        reader.clone(),
        vec![(0, key_a.clone(), StreamId::MIN)],
        8,
        CODEC_VERSION,
    )
    .unwrap();
    let mut fetch_b = Fetch::block(
        reader,
        vec![(1, key_b.clone(), StreamId::MIN)],
        8,
        CODEC_VERSION,
    )
    .unwrap();

    let blocking = Arc::new(tokio::sync::Notify::new());
    let released = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let loop_a = tokio::spawn({
        let (blocking, released, stop) = (blocking.clone(), released.clone(), stop.clone());
        async move {
            while !stop.load(Ordering::Relaxed) {
                if !fetch_a.fetch().await.unwrap().is_empty() {
                    blocking.notify_one();
                    // Deliberately synchronous: occupy the thread the "handler" was given.
                    std::thread::sleep(HANDLER_BLOCK);
                    released.store(true, Ordering::SeqCst);
                }
            }
        }
    });
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let loop_b = tokio::spawn({
        let stop = stop.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                if !fetch_b.fetch().await.unwrap().is_empty() {
                    let _ = tx.send(());
                }
            }
        }
    });

    xadd(&conn, &key_a, "a", b"a").await;
    tokio::time::timeout(Duration::from_secs(20), blocking.notified())
        .await
        .expect("partition 0 delivered");
    let watch = Instant::now();
    xadd(&conn, &key_b, "b", b"b").await;
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect(
            "partition 1 was not delivered within 2 s while partition 0's handler was blocked: \
             the handler is occupying something the reader needs",
        );
    assert!(
        !released.load(Ordering::SeqCst),
        "partition 1 must be served while partition 0's handler is still blocked"
    );
    eprintln!(
        "partition 1 delivered {:?} after publish while partition 0's handler was blocked",
        watch.elapsed()
    );
    stop.store(true, Ordering::Relaxed);
    loop_a.await.unwrap();
    loop_b.await.unwrap();
}

// ------------------------------------------------------------------------------------------------
// ReadModeComparisonTests
// ------------------------------------------------------------------------------------------------

/// Runs `fetch` against a silent topic for `window` and returns how many reads it issued.
async fn idle_reads(mut fetch: Fetch, window: Duration) -> u64 {
    // Do not count until the first read has been issued (handshake excluded).
    assert!(fetch.fetch().await.unwrap().is_empty());
    let start = fetch.reads_issued();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        assert!(fetch.fetch().await.unwrap().is_empty());
    }
    fetch.reads_issued() - start
}

/// S19: with no traffic a polling reader issues about `window / MaxIdleDelayMs` reads and a
/// blocking one about `window / BlockMs`. Counted by the fetch (the server counter is shared with
/// parallel tests) and cross-checked against `INFO commandstats`, which can only be higher.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_command_cost_is_an_order_of_magnitude_lower_under_block() {
    const BLOCK_MS: u32 = 1000;
    const IDLE_SECONDS: u64 = 10;
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key_block = keys().stream(&fixture.new_topic("idle-block"), 0, false);
    let key_poll = keys().stream(&fixture.new_topic("idle-poll"), 0, false);
    let window = Duration::from_secs(IDLE_SECONDS);
    let server_before = xread_calls(&conn).await;
    let (block_calls, poll_calls) = tokio::join!(
        idle_reads(block(&conn, BLOCK_MS, &key_block, 100), window),
        idle_reads(poll(&conn, &key_poll, 100), window),
    );
    let server_delta = xread_calls(&conn).await - server_before;
    let predicted_poll = IDLE_SECONDS * 1000 / u64::from(MAX_IDLE_DELAY_MS);
    let predicted_block = IDLE_SECONDS * 1000 / u64::from(BLOCK_MS);
    eprintln!(
        "S19 idle XREADs over {IDLE_SECONDS}s: block {block_calls} (predicted {predicted_block}), poll {poll_calls} (predicted {predicted_poll}), server saw {server_delta}"
    );
    assert!(
        server_delta >= block_calls + poll_calls,
        "every counted read reached the server"
    );
    assert!(
        (predicted_poll / 3..=predicted_poll * 2).contains(&poll_calls),
        "a polling reader idle for {IDLE_SECONDS}s must issue about {predicted_poll} XREADs, issued {poll_calls}"
    );
    assert!(
        (predicted_block / 2..=predicted_block * 2 + 5).contains(&block_calls),
        "a blocking reader idle for {IDLE_SECONDS}s wakes about {predicted_block} times, woke {block_calls}"
    );
    assert!(
        poll_calls > block_calls * 5,
        "blocking must cost an order of magnitude fewer idle commands than polling"
    );
}

struct Latency {
    p50: f64,
    p90: f64,
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let rank = ((q * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

/// Publishes `samples` messages ~200 ms apart into an idle reader and returns the publish-to-fetch
/// latency in ms. The body carries the publish time as nanoseconds since `epoch`.
async fn idle_latency(conn: &Connection, mut fetch: Fetch, key: String, samples: usize) -> Latency {
    let epoch = Instant::now();
    let seen = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let reader = tokio::spawn({
        let (seen, stop) = (seen.clone(), stop.clone());
        async move {
            let mut latencies = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                for b in fetch.fetch().await.unwrap() {
                    let arrived = epoch.elapsed();
                    for m in &b.msgs {
                        let sent = u64::from_le_bytes(m.body()[..8].try_into().unwrap());
                        #[allow(clippy::cast_precision_loss)]
                        latencies.push((arrived.as_nanos() as f64 - sent as f64) / 1_000_000.0);
                        seen.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
            latencies
        }
    });
    let stamp = |epoch: Instant| {
        u64::try_from(epoch.elapsed().as_nanos())
            .unwrap()
            .to_le_bytes()
    };
    // Warm-up, thrown away.
    xadd(conn, &key, "k", &stamp(epoch)).await;
    redisevents_test_support::wait_until(|| seen.load(Ordering::SeqCst) >= 1, None, "warm-up")
        .await;
    // Seeded jitter so the poller's sleeps cannot phase-lock with the publisher.
    let mut rng: u32 = 20_260_906;
    for _ in 0..samples {
        rng = rng.wrapping_mul(1_103_515_245).wrapping_add(12345);
        tokio::time::sleep(Duration::from_millis(180 + u64::from((rng >> 16) % 41))).await;
        xadd(conn, &key, "k", &stamp(epoch)).await;
    }
    redisevents_test_support::wait_until(
        || seen.load(Ordering::SeqCst) > samples,
        None,
        "every measured message",
    )
    .await;
    stop.store(true, Ordering::Relaxed);
    let mut latencies = reader.await.unwrap();
    latencies.remove(0);
    assert_eq!(latencies.len(), samples);
    latencies.sort_by(f64::total_cmp);
    Latency {
        p50: percentile(&latencies, 0.5),
        p90: percentile(&latencies, 0.9),
    }
}

/// S22: publish-to-fetch latency on a quiet topic is materially lower under `BLOCK`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_to_handler_latency_is_materially_lower_under_block() {
    const SAMPLES: usize = 50;
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key_block = keys().stream(&fixture.new_topic("latency-block"), 0, false);
    let key_poll = keys().stream(&fixture.new_topic("latency-poll"), 0, false);
    let (blocked, polled) = tokio::join!(
        idle_latency(
            &conn,
            block(&conn, 1000, &key_block, 100),
            key_block.clone(),
            SAMPLES
        ),
        idle_latency(
            &conn,
            poll(&conn, &key_poll, 100),
            key_poll.clone(),
            SAMPLES
        ),
    );
    let max = f64::from(MAX_IDLE_DELAY_MS);
    eprintln!(
        "S22: block p50 {:.3} ms p90 {:.3} ms; poll p50 {:.3} ms p90 {:.3} ms",
        blocked.p50, blocked.p90, polled.p50, polled.p90
    );
    assert!(
        (max * 0.15..=max * 0.95).contains(&polled.p50),
        "an idle poller's median should sit near half of MaxIdleDelayMs, was {}",
        polled.p50
    );
    assert!(blocked.p50 < max / 5.0, "block p50 {}", blocked.p50);
    assert!(
        blocked.p50 < polled.p50 / 3.0,
        "the gate: block is materially faster"
    );
    assert!(blocked.p90 < max / 2.0, "block p90 {}", blocked.p90);
}

/// S22b: under a backlog `XREAD BLOCK` returns at once, so the loop is a drain loop with no
/// per-batch sleep.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn block_drains_a_backlog_far_faster_than_any_per_batch_sleep_allows() {
    const TOTAL: usize = 20_000;
    const BATCH: u32 = 100;
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("drain"), 0, false);
    let body = [0u8; 64];
    for _ in 0..TOTAL / 1000 {
        let mut pipe = redis::pipe();
        for _ in 0..1000 {
            let cmd = pipe.cmd("XADD").arg(&key).arg("*");
            cmd.arg("b")
                .arg(&body[..])
                .arg("t")
                .arg("readmode")
                .arg("k")
                .arg("k");
        }
        pipe.query_async::<()>(&mut conn.shared()).await.unwrap();
    }
    let mut fetch = block(&conn, 1000, &key, BATCH);
    let started = Instant::now();
    let (mut seen, mut batches) = (0usize, 0usize);
    while seen < TOTAL {
        let got = fetch.fetch().await.unwrap();
        seen += total(&got);
        batches += got.len();
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    #[allow(clippy::cast_precision_loss)]
    let floor = batches as f64 * f64::from(MAX_IDLE_DELAY_MS);
    eprintln!(
        "S22b: {batches} batches in {elapsed_ms:.0} ms (a sleeping loop needs {floor:.0} ms)"
    );
    assert_eq!(seen, TOTAL);
    assert!(batches >= TOTAL / BATCH as usize / 2);
    assert!(elapsed_ms < floor / 4.0);
}

// ------------------------------------------------------------------------------------------------
// Rust-only: cancellation, seek, co-location, reconnect
// ------------------------------------------------------------------------------------------------

/// Dropping a parked blocking read discards its socket: the next read runs on a fresh connection
/// (not queued behind the parked `BLOCK`), returns the right entry, and the old socket closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_parked_read_discards_the_connection_and_leaves_no_stale_reply() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("cancel"), 0, false);
    let reader = ReaderConnection::new(&conn, 10_000).unwrap();
    let mut fetch = Fetch::block(
        reader.clone(),
        vec![(0, key.clone(), StreamId::MIN)],
        10,
        CODEC_VERSION,
    )
    .unwrap();
    let old_id = reader.client_id().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fetch.fetch())
            .await
            .is_err(),
        "the read parks on an empty stream"
    );
    assert_eq!(
        reader.live_generation(),
        None,
        "the parked socket was discarded"
    );
    assert_eq!(
        fetch.position(0),
        Some(StreamId::MIN),
        "a dropped read moves no cursor"
    );

    let id = xadd(&conn, &key, "k", b"fresh").await;
    let started = Instant::now();
    let got = tokio::time::timeout(Duration::from_secs(5), fetch.fetch())
        .await
        .expect("the next read is not stuck behind the parked BLOCK")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].msgs[0].id(), id);
    assert_eq!(&got[0].msgs[0].body()[..], b"fresh");
    assert_eq!(fetch.position(0), Some(id));

    let probe = conn.clone();
    redisevents_test_support::wait_until_async(
        || {
            let probe = probe.clone();
            async move { !client_ids(&probe).await.contains(&old_id) }
        },
        Some(Duration::from_secs(15)),
        "the discarded socket to close",
    )
    .await;
}

/// A seek rewinds one partition's cursor; the next read returns entries after the new id.
#[tokio::test]
async fn seek_rewinds_a_partition_for_the_reset_protocol() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("seek"), 0, false);
    let first = xadd(&conn, &key, "k", b"1").await;
    xadd(&conn, &key, "k", b"2").await;
    let last = xadd(&conn, &key, "k", b"3").await;
    for mut fetch in [block(&conn, 100, &key, 10), poll(&conn, &key, 10)] {
        let got = fetch.fetch().await.unwrap();
        assert_eq!(total(&got), 3);
        assert_eq!(got[0].last, last);
        assert!(fetch.fetch().await.unwrap().is_empty());
        assert!(fetch.seek(0, first));
        assert!(!fetch.seek(7, first), "not a partition of this fetch");
        assert_eq!(fetch.idle_delay_ms(), 0, "a seek resets the idle back-off");
        let again = fetch.fetch().await.unwrap();
        assert_eq!(total(&again), 2);
        assert_eq!(&again[0].msgs[0].body()[..], b"2");
    }
}

/// A co-located group is one multi-stream read; each partition gets its own batch and cursor, and
/// a reset of one leaves the others alone.
#[tokio::test]
async fn co_located_fetch_splits_by_partition_and_seeks_one() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let topic = fixture.new_topic("group");
    let k: Vec<String> = (0..3).map(|p| keys().stream(&topic, p, true)).collect();
    let a0 = xadd(&conn, &k[0], "a", b"a0").await;
    xadd(&conn, &k[2], "c", b"c0").await;
    let c1 = xadd(&conn, &k[2], "c", b"c1").await;
    let starts = (0..3u32)
        .map(|p| (p, k[p as usize].clone(), StreamId::MIN))
        .collect::<Vec<_>>();
    let reader = ReaderConnection::new(&conn, 100).unwrap();
    for mut fetch in [
        Fetch::block(reader.clone(), starts.clone(), 10, CODEC_VERSION).unwrap(),
        Fetch::poll(&conn, starts.clone(), 10, MAX_IDLE_DELAY_MS, CODEC_VERSION).unwrap(),
    ] {
        let mut got = fetch.fetch().await.unwrap();
        got.sort_by_key(|b| b.partition);
        assert_eq!(got.len(), 2);
        assert_eq!((got[0].partition, got[0].msgs.len()), (0, 1));
        assert_eq!(
            (got[1].partition, got[1].msgs.len(), got[1].last),
            (2, 2, c1)
        );
        assert!(
            got.iter()
                .all(|b| b.msgs.iter().all(|m| m.partition() == b.partition))
        );
        assert_eq!(fetch.position(0), Some(a0));
        assert_eq!(fetch.position(1), Some(StreamId::MIN));
        fetch.seek(2, StreamId::MIN);
        let again = fetch.fetch().await.unwrap();
        assert_eq!(again.len(), 1);
        assert_eq!((again[0].partition, again[0].msgs.len()), (2, 2));
    }
}

/// The reader owns reconnecting: a killed socket fails at most one read, then reads resume.
#[tokio::test]
async fn a_killed_reader_connection_reconnects_on_the_next_read() {
    let fixture = redisevents_test_support::require_redis!();
    let conn = connect(fixture).await;
    let key = keys().stream(&fixture.new_topic("reconnect"), 0, false);
    let reader = ReaderConnection::new(&conn, 100).unwrap();
    let mut fetch = Fetch::block(
        reader.clone(),
        vec![(0, key.clone(), StreamId::MIN)],
        10,
        CODEC_VERSION,
    )
    .unwrap();
    assert!(fetch.fetch().await.unwrap().is_empty());
    let id = reader.client_id().await.unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(id)
        .query_async(&mut conn.shared())
        .await
        .unwrap();
    xadd(&conn, &key, "k", b"after").await;
    let mut delivered = None;
    for _ in 0..3 {
        if let Ok(got) = fetch.fetch().await
            && !got.is_empty()
        {
            delivered = Some(got);
            break;
        }
    }
    let got = delivered.expect("reads resume after a reconnect");
    assert_eq!(&got[0].msgs[0].body()[..], b"after");
    assert_ne!(reader.client_id().await.unwrap(), id);
}

#[test]
fn block_zero_and_bad_arguments_are_refused() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let Some(fixture) = redisevents_test_support::redis().await else {
            return;
        };
        let conn = connect(fixture).await;
        assert!(matches!(
            ReaderConnection::new(&conn, 0),
            Err(crate::Error::Config(_))
        ));
        let reader = ReaderConnection::new(&conn, 10).unwrap();
        assert!(Fetch::block(reader.clone(), vec![], 1, CODEC_VERSION).is_err());
        assert!(
            Fetch::block(
                reader.clone(),
                vec![(0, "k".into(), StreamId::MIN)],
                0,
                CODEC_VERSION
            )
            .is_err()
        );
        assert!(
            Fetch::block(
                reader.clone(),
                vec![
                    (0, "a".into(), StreamId::MIN),
                    (0, "b".into(), StreamId::MIN)
                ],
                1,
                CODEC_VERSION
            )
            .is_err()
        );
        assert!(Fetch::block(reader, vec![(0, "k".into(), StreamId::MIN)], 1, 99).is_err());
    });
}
