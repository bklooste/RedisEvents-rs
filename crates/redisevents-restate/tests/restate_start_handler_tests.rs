//! Ports of the C# `RestateStartHandlerTests` (every test and theory case) against a fake
//! ingress, driven through `StreamTestHarness`. Not ported: the Activity-span test
//! (`with_tracing_on_each_start_is_a_client_span...`): there is no `Activity`; the `restate.start`
//! `tracing` span is not asserted. Its sibling, the traceparent continuation, is ported.
#![allow(clippy::unused_async)]

mod fake_ingress;

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use bytes::Bytes;
use fake_ingress::{Act, FakeIngress};
use redisevents::consumer::{CancellationToken, HandleError, HandleResult, Typed};
use redisevents::testing::StreamTestHarness;
use redisevents::{BoxError, StreamId, StreamMsg, TypedMsg};
use redisevents_restate::{
    RestateStart, RestateStartBlocked, RestateStartHandler, RestateStartOptions,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Bet {
    id: String,
    claim: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct StartInput {
    bet_id: String,
}

type Keep = Box<dyn Fn(&TypedMsg<Bet>) -> bool + Send + Sync>;
type Map = Box<dyn Fn(&TypedMsg<Bet>) -> Result<RestateStart, BoxError> + Send + Sync>;
type Starter = Typed<RestateStartHandler<Bet, Keep, Map>, Bet>;

fn default_keep() -> Keep {
    Box::new(|m| m.value.claim.is_some())
}

fn default_map() -> Map {
    Box::new(|m| {
        Ok(
            RestateStart::new("BonusPayout", m.value.claim.clone().unwrap()).with_input(
                &StartInput {
                    bet_id: m.value.id.clone(),
                },
            )?,
        )
    })
}

fn fast(ingress: &FakeIngress, attempts: u32) -> RestateStartOptions {
    RestateStartOptions {
        max_attempts: std::num::NonZeroU32::new(attempts).unwrap(),
        initial_retry_delay: Duration::from_millis(1),
        max_retry_delay: Duration::from_millis(4),
        attempt_timeout: Duration::from_secs(10),
        ..RestateStartOptions::new(ingress.url.clone())
    }
}

fn starter_with(client: Client, options: RestateStartOptions, keep: Keep, map: Map) -> Starter {
    Typed::new(RestateStartHandler::new(client, options, keep, map).unwrap())
}

fn starter(ingress: &FakeIngress) -> Starter {
    starter_with(
        Client::new(),
        fast(ingress, 5),
        default_keep(),
        default_map(),
    )
}

/// `size` bets, the ones `claimed` picks carrying claim `c{i}`; ids `1000+i`-0.
fn batch_of(size: usize, claimed: impl Fn(usize) -> bool, trace: Option<&str>) -> Vec<StreamMsg> {
    (0..size)
        .map(|i| {
            let bet = Bet {
                id: format!("bet{i}"),
                claim: claimed(i).then(|| format!("c{i}")),
            };
            StreamMsg::new(
                StreamId::new(1000 + i as u64, 0),
                0,
                "BetSettled",
                Bytes::from(serde_json::to_vec(&bet).unwrap()),
            )
            .with_partition_key(bet.id)
            .with_trace_parent(trace.map(str::to_owned))
        })
        .collect()
}

fn every_nth(size: usize, every: usize) -> Vec<StreamMsg> {
    batch_of(size, |i| i % every == 0, None)
}

async fn deliver(h: &Starter, batch: &[StreamMsg]) -> HandleResult {
    StreamTestHarness::new("bets").deliver_batch(h, batch).await
}

fn blocked(result: HandleResult) -> RestateStartBlocked {
    match result {
        Err(HandleError::Block(e)) => *e
            .downcast::<RestateStartBlocked>()
            .expect("Block carries a RestateStartBlocked"),
        other => panic!("expected Block, got {other:?}"),
    }
}

fn json(code: u16, body: &str) -> Act {
    Act::Reply(code, body.to_owned())
}

const TRANSIENT: [&str; 9] = [
    "connection dropped",
    "attempt timeout",
    "client timeout",
    "500",
    "502",
    "503",
    "408",
    "429",
    "garbage",
];

fn fail(kind: &str) -> Act {
    match kind {
        "connection dropped" => Act::Drop,
        "attempt timeout" | "client timeout" => Act::Hang,
        "garbage" => Act::Garbage,
        code => json(code.parse().unwrap(), "{\"message\":\"later\"}"),
    }
}

fn client_for(kind: &str) -> Client {
    if kind == "client timeout" {
        Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap()
    } else {
        Client::new()
    }
}

fn transient_options(ingress: &FakeIngress, kind: &str) -> RestateStartOptions {
    let mut o = fast(ingress, 5);
    if kind == "attempt timeout" {
        o.attempt_timeout = Duration::from_millis(100);
    }
    o
}

#[tokio::test]
async fn a_batch_of_1000_with_10_kept_makes_exactly_10_calls_to_their_own_workflow_keys() {
    let ingress = FakeIngress::start().await;
    let handler = starter(&ingress);

    deliver(&handler, &every_nth(1_000, 100)).await.unwrap();

    let seen = ingress.requests();
    assert_eq!(seen.len(), 10);
    let paths: BTreeSet<_> = seen.iter().map(|s| s.path.clone()).collect();
    let expected: BTreeSet<_> = (0..10)
        .map(|i| format!("/BonusPayout/c{}/run/send", i * 100))
        .collect();
    assert_eq!(paths, expected);
    assert!(seen.iter().all(|s| !s.has_idempotency_key));
    assert!(
        seen.iter()
            .all(|s| s.content_type.as_deref() == Some("application/json"))
    );
    assert_eq!(
        seen.iter()
            .find(|s| s.path == "/BonusPayout/c300/run/send")
            .unwrap()
            .body,
        "{\"BetId\":\"bet300\"}"
    );
}

#[tokio::test]
async fn a_batch_with_nothing_kept_makes_no_call_and_returns() {
    let ingress = FakeIngress::start().await;
    let handler = starter(&ingress);

    deliver(&handler, &batch_of(500, |_| false, None))
        .await
        .unwrap();
    // The harness (like the worker) calls the handler for an empty batch too.
    deliver(&handler, &[]).await.unwrap();

    assert!(ingress.requests().is_empty());
}

#[tokio::test]
async fn three_items_failing_past_their_attempts_block_the_batch_and_the_retry_sends_all_ten_again()
{
    let ingress = FakeIngress::start().await;
    let failing: BTreeSet<String> = ["BonusPayout/c700", "BonusPayout/c800", "BonusPayout/c900"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    let f = failing.clone();
    ingress.respond(move |seen| {
        if f.contains(&seen.workflow_key) {
            json(503, "{\"message\":\"unavailable\"}")
        } else {
            Act::Default
        }
    });
    let handler = starter(&ingress);
    let batch = every_nth(1_000, 100);

    let b = blocked(deliver(&handler, &batch).await);
    assert_eq!(
        b.failed_keys().iter().cloned().collect::<BTreeSet<_>>(),
        failing
    );
    assert!(!b.shutting_down());
    assert!(b.to_string().contains("HTTP 503"));
    assert_eq!(ingress.requests().len(), 7 + 3 * 5);
    let answers = ingress.answers();
    assert_eq!(answers.len(), 7);
    assert!(answers.iter().all(|a| a.1 == "Accepted"));

    ingress.clear_respond();
    ingress.clear();
    deliver(&handler, &batch).await.unwrap();

    assert_eq!(ingress.requests().len(), 10);
    let answers = ingress.answers();
    assert_eq!(
        answers
            .iter()
            .filter(|a| a.1 == "PreviouslyAccepted")
            .count(),
        7
    );
    let fresh: BTreeSet<String> = answers
        .iter()
        .filter(|a| a.1 == "Accepted")
        .map(|a| a.0.clone())
        .collect();
    assert_eq!(fresh, failing);
}

#[tokio::test]
async fn a_transient_failure_is_retried_in_place_and_the_batch_is_acked_once_it_goes_through() {
    for kind in TRANSIENT {
        let ingress = FakeIngress::start().await;
        let calls = std::sync::atomic::AtomicUsize::new(0);
        ingress.respond(move |_| {
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
                fail(kind)
            } else {
                Act::Default
            }
        });
        let handler = starter_with(
            client_for(kind),
            transient_options(&ingress, kind),
            default_keep(),
            default_map(),
        );

        deliver(&handler, &batch_of(1, |_| true, None))
            .await
            .unwrap_or_else(|e| panic!("{kind}: {e}"));

        assert_eq!(ingress.requests().len(), 3, "{kind}");
        let answers = ingress.answers();
        assert_eq!(answers.len(), 1, "{kind}");
        assert_eq!(answers[0].1, "Accepted", "{kind}");
    }
}

#[tokio::test]
async fn a_transient_failure_that_outlasts_the_attempts_blocks_and_never_escapes_as_anything_else()
{
    for kind in TRANSIENT {
        let ingress = FakeIngress::start().await;
        ingress.respond(move |_| fail(kind));
        let handler = starter_with(
            client_for(kind),
            transient_options(&ingress, kind),
            default_keep(),
            default_map(),
        );

        let b = blocked(deliver(&handler, &batch_of(1, |_| true, None)).await);

        assert_eq!(b.failed_keys(), ["BonusPayout/c0"], "{kind}");
        assert!(!b.shutting_down(), "{kind}");
        assert_eq!(ingress.requests().len(), 5, "{kind}");
    }
}

#[tokio::test]
async fn a_4xx_other_than_already_started_blocks_without_retrying_and_never_skips() {
    for status in [400_u16, 401, 403, 404, 409, 422] {
        let ingress = FakeIngress::start().await;
        ingress
            .respond(move |_| json(status, &format!("{{\"code\":{status},\"message\":\"no\"}}")));
        let handler = starter(&ingress);

        let b = blocked(deliver(&handler, &batch_of(3, |i| i == 1, None)).await);

        assert_eq!(b.failed_keys(), ["BonusPayout/c1"]);
        assert!(b.to_string().contains(&format!("HTTP {status}")));
        assert_eq!(ingress.requests().len(), 1, "{status}");
    }
}

#[tokio::test]
async fn two_handler_instances_given_the_same_batch_concurrently_produce_one_accepted_start_per_key()
 {
    let ingress = FakeIngress::start().await;
    ingress.set_latency(Duration::from_millis(2));
    let batch = batch_of(200, |i| i % 4 == 0, None);
    let a = starter(&ingress);
    let b = starter(&ingress);

    for _ in 0..5 {
        let (ra, rb) = tokio::join!(deliver(&a, &batch), deliver(&b, &batch));
        ra.unwrap();
        rb.unwrap();
    }

    assert!(
        !ingress.idempotency_key_seen(),
        "a workflow submit must never carry Idempotency-Key"
    );
    assert_eq!(ingress.requests().len(), 50 * 2 * 5);
    let answers = ingress.answers();
    let accepted: Vec<_> = answers
        .iter()
        .filter(|x| x.1 == "Accepted")
        .map(|x| x.0.clone())
        .collect();
    assert_eq!(accepted.len(), 50);
    let unique: BTreeSet<_> = accepted.iter().cloned().collect();
    assert_eq!(unique.len(), 50);
    let expected: BTreeSet<_> = (0..50).map(|i| format!("BonusPayout/c{}", i * 4)).collect();
    assert_eq!(unique, expected);
    assert_eq!(
        answers
            .iter()
            .filter(|x| x.1 == "PreviouslyAccepted")
            .count(),
        50 * 2 * 5 - 50
    );
}

#[tokio::test]
async fn the_fake_ingress_rejects_an_idempotency_key_like_restate_so_a_handler_that_sent_one_would_block()
 {
    let ingress = FakeIngress::start().await;

    let response = Client::new()
        .post(format!("{}BonusPayout/k/run/send", ingress.url))
        .header("Idempotency-Key", "k")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status().as_u16(), 400);
    assert!(ingress.idempotency_key_seen());
}

#[tokio::test]
async fn submits_are_bounded_parallel_at_the_configured_degree() {
    let ingress = FakeIngress::start().await;
    ingress.set_latency(Duration::from_millis(20));
    deliver(&starter(&ingress), &batch_of(40, |_| true, None))
        .await
        .unwrap();
    assert!(
        (2..=8).contains(&ingress.max_in_flight()),
        "{}",
        ingress.max_in_flight()
    );

    let narrow = FakeIngress::start().await;
    narrow.set_latency(Duration::from_millis(20));
    let mut options = fast(&narrow, 5);
    options.max_parallelism = std::num::NonZeroUsize::new(3).unwrap();
    let handler = starter_with(Client::new(), options, default_keep(), default_map());
    deliver(&handler, &batch_of(20, |_| true, None))
        .await
        .unwrap();
    assert!(
        (2..=3).contains(&narrow.max_in_flight()),
        "{}",
        narrow.max_in_flight()
    );
}

#[tokio::test]
async fn any_2xx_is_accepted_and_previously_accepted_counts_as_already_started() {
    let ingress = FakeIngress::start().await;
    ingress.respond(|seen| match seen.workflow_key.as_str() {
        "BonusPayout/c0" => json(
            202,
            "{\"invocationId\":\"inv_1\",\"status\":\"PreviouslyAccepted\"}",
        ),
        "BonusPayout/c1" => Act::Reply(200, "not json".to_owned()),
        "BonusPayout/c2" => Act::Reply(204, String::new()),
        _ => Act::Default,
    });
    let handler = starter(&ingress);

    deliver(&handler, &batch_of(4, |_| true, None))
        .await
        .unwrap();
    assert_eq!(
        ingress.requests().len(),
        4,
        "any 2xx is accepted: no retries"
    );
}

#[tokio::test]
async fn a_throwing_map_blocks_naming_the_message_and_the_other_items_are_still_sent() {
    let ingress = FakeIngress::start().await;
    let map: Map = Box::new(|m| {
        if m.value.id == "bet2" {
            Err("bad payload".into())
        } else {
            Ok(RestateStart::new(
                "BonusPayout",
                m.value.claim.clone().unwrap(),
            ))
        }
    });
    let handler = starter_with(Client::new(), fast(&ingress, 5), default_keep(), map);

    let b = blocked(deliver(&handler, &batch_of(5, |_| true, None)).await);

    assert_eq!(b.failed_keys(), ["stream id 1002-0"]);
    assert!(b.to_string().contains("Map failed: bad payload"));
    assert_eq!(ingress.requests().len(), 4);
}

#[tokio::test]
async fn a_panicking_map_blocks_rather_than_skipping() {
    let ingress = FakeIngress::start().await;
    let map: Map = Box::new(|m| {
        assert!(m.value.id != "bet2", "bad payload");
        Ok(RestateStart::new(
            "BonusPayout",
            m.value.claim.clone().unwrap(),
        ))
    });
    let handler = starter_with(Client::new(), fast(&ingress, 5), default_keep(), map);

    let b = blocked(deliver(&handler, &batch_of(5, |_| true, None)).await);

    assert_eq!(b.failed_keys(), ["stream id 1002-0"]);
    assert!(b.to_string().contains("Map panicked: bad payload"));
    assert_eq!(ingress.requests().len(), 4);
}

#[tokio::test]
async fn a_throwing_keep_blocks_rather_than_skipping() {
    let ingress = FakeIngress::start().await;
    let keep: Keep = Box::new(|m| {
        assert!(m.value.id != "bet1", "bad");
        m.value.claim.is_some()
    });
    let handler = starter_with(Client::new(), fast(&ingress, 5), keep, default_map());

    let b = blocked(deliver(&handler, &batch_of(3, |i| i != 1, None)).await);

    assert_eq!(b.failed_keys(), ["stream id 1001-0"]);
    assert_eq!(ingress.requests().len(), 2);
}

#[tokio::test]
async fn an_empty_workflow_or_key_blocks() {
    for (workflow, key) in [("", "key"), ("BonusPayout", "")] {
        let ingress = FakeIngress::start().await;
        let map: Map = Box::new(move |_| Ok(RestateStart::new(workflow, key)));
        let handler = starter_with(Client::new(), fast(&ingress, 5), default_keep(), map);

        let _ = blocked(deliver(&handler, &batch_of(1, |_| true, None)).await);

        assert!(ingress.requests().is_empty());
    }
}

#[tokio::test]
async fn an_input_that_cannot_be_serialized_fails_the_map_and_blocks() {
    let ingress = FakeIngress::start().await;
    let map: Map = Box::new(|m| {
        // JSON object keys must be strings: a tuple key cannot be serialized.
        let unserializable: HashMap<(i32, i32), i32> = HashMap::from([((1, 2), 3)]);
        Ok(
            RestateStart::new("BonusPayout", m.value.claim.clone().unwrap())
                .with_input(&unserializable)?,
        )
    });
    let handler = starter_with(Client::new(), fast(&ingress, 5), default_keep(), map);

    let b = blocked(deliver(&handler, &batch_of(1, |_| true, None)).await);

    assert_eq!(b.failed_keys(), ["stream id 1000-0"]);
    assert!(b.to_string().contains("Map failed"), "{b}");
    assert!(ingress.requests().is_empty());
}

#[tokio::test]
async fn a_no_input_start_sends_no_body() {
    let ingress = FakeIngress::start().await;
    let map: Map = Box::new(|m| Ok(RestateStart::new("Verify", m.value.claim.clone().unwrap())));
    let handler = starter_with(Client::new(), fast(&ingress, 5), default_keep(), map);

    deliver(&handler, &batch_of(1, |_| true, None))
        .await
        .unwrap();

    let seen = ingress.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].body, "");
    assert!(seen[0].content_type.is_none());
}

#[tokio::test]
async fn keys_and_workflow_names_are_escaped_into_one_path_segment_each() {
    let ingress = FakeIngress::start().await;
    let mut options = fast(&ingress, 5);
    options.ingress = ingress.url.join("prefix/").unwrap();
    options.handler = "Run".to_owned();
    let map: Map = Box::new(|_| Ok(RestateStart::new("Verify", "cust:42/1")));
    let handler = starter_with(Client::new(), options, default_keep(), map);

    deliver(&handler, &batch_of(1, |_| true, None))
        .await
        .unwrap();

    let seen = ingress.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].path, "/prefix/Verify/cust%3A42%2F1/Run/send");
}

#[tokio::test]
async fn cancellation_mid_batch_is_reported_as_a_blocked_batch_never_as_a_skip() {
    let ingress = FakeIngress::start().await;
    let token = CancellationToken::new();
    let t = token.clone();
    ingress.respond(move |_| {
        t.cancel();
        Act::Hang
    });
    let handler = starter(&ingress);
    let harness = StreamTestHarness::new("bets").with_cancellation(token);

    let b = blocked(
        harness
            .deliver_batch(&handler, &batch_of(30, |_| true, None))
            .await,
    );

    assert!(b.shutting_down());
    assert_eq!(b.failed_keys().len(), 30);
}

#[tokio::test]
async fn an_already_cancelled_token_sends_nothing_and_still_blocks_a_batch_with_kept_items() {
    let ingress = FakeIngress::start().await;
    let handler = starter(&ingress);
    let token = CancellationToken::new();
    token.cancel();
    let harness = StreamTestHarness::new("bets").with_cancellation(token);

    let b = blocked(
        harness
            .deliver_batch(&handler, &batch_of(3, |_| true, None))
            .await,
    );
    assert!(b.shutting_down());
    assert!(ingress.requests().is_empty());

    // Nothing kept: nothing to lose, so it returns and the batch is acked.
    harness
        .deliver_batch(&handler, &batch_of(3, |_| false, None))
        .await
        .unwrap();
}

#[tokio::test]
async fn the_traceparent_continues_the_trace_of_the_message_that_caused_the_start() {
    const PRODUCER: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    let ingress = FakeIngress::start().await;
    let map: Map = Box::new(|m| {
        Ok(RestateStart::new(
            "TraceFlow",
            m.value.claim.clone().unwrap(),
        ))
    });
    let handler = starter_with(Client::new(), fast(&ingress, 5), default_keep(), map);

    deliver(&handler, &batch_of(1, |_| true, Some(PRODUCER)))
        .await
        .unwrap();

    let seen = ingress.requests();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].trace_parent.as_deref(), Some(PRODUCER));
}

#[test]
fn the_handler_refuses_to_start_with_a_bad_ingress_or_bad_options() {
    let new = |o: RestateStartOptions| {
        RestateStartHandler::<Bet, _, _>::new(Client::new(), o, default_keep(), default_map())
    };
    let ingress = reqwest::Url::parse("http://r:8080").unwrap();

    assert!(new(RestateStartOptions::new(ingress.clone())).is_ok());
    let ftp = reqwest::Url::parse("ftp://r/").unwrap();
    assert!(new(RestateStartOptions::new(ftp)).is_err());
    let mut blank_handler = RestateStartOptions::new(ingress.clone());
    blank_handler.handler = " ".to_owned();
    assert!(new(blank_handler).is_err());
    let mut inverted = RestateStartOptions::new(ingress);
    inverted.max_retry_delay = Duration::from_millis(1);
    assert!(new(inverted).is_err());
}

#[test]
fn the_defaults_are_the_ones_the_ack_rule_was_designed_around() {
    let o = RestateStartOptions::new(reqwest::Url::parse("http://r:8080").unwrap());

    assert_eq!(o.handler, "run");
    assert_eq!(o.max_parallelism.get(), 8);
    assert_eq!(o.max_attempts.get(), 5);
    assert_eq!(o.initial_retry_delay, Duration::from_millis(200));
    assert_eq!(o.max_retry_delay, Duration::from_secs(5));
}
