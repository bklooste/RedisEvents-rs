//! The readiness endpoint over a real Redis, driven through the router with `oneshot`.
//!
//! Ported from C# `RedisEvents.UnitTests/StreamStatusTests.cs`-style process-state cases and the
//! `StreamsHealthCheck` mapping in `RedisEvents.UnitTests/BehindTailHealthTests.cs`. Partition
//! monitors are crate-private in the core and are only fed by a running consumer host, so the
//! cases that need one are in the skip list in the crate's PR description rather than here.

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use redisevents_axum::health_router;
use redisevents_test_support::require_redis;
use tower::ServiceExt as _;

async fn readiness(router: axum::Router) -> (StatusCode, serde_json::Value) {
    let response = router
        .oneshot(Request::get("/").body(Body::empty()).expect("request"))
        .await
        .expect("infallible");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, serde_json::from_slice(&bytes).expect("json"))
}

/// C# `StreamHealth.Evaluate` answers Healthy for a process with nothing registered, a producer-only
/// service. The route passes that through, with a live connection ping.
#[tokio::test]
async fn a_producer_only_process_is_ready_with_a_live_connection() {
    let redis = require_redis!();

    let (status, body) = readiness(health_router(Some(redis.connect().await), || (0, 0))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "Healthy");
    assert_eq!(body["entries"]["streams"]["status"], "Healthy");
    assert_eq!(
        body["entries"]["streams"]["tags"],
        serde_json::json!(["streams"])
    );
    assert!(
        body["entries"]["streams"]["description"]
            .as_str()
            .is_some_and(|d| d.contains("no stream consumers are registered")),
        "{body}"
    );
}

/// The same answer with no connection to ping: the readiness rules do not need Redis to grade a
/// process that has nothing registered.
#[tokio::test]
async fn the_check_runs_without_a_connection() {
    let (status, body) = readiness(health_router(None, || (0, 0))).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "Healthy");
}

/// Registered consumers with none started is Unhealthy, and the route answers 503, which is the
/// only status a restart can fix. Counts come from the consumer host through the closure.
#[tokio::test]
async fn consumers_registered_but_none_started_answers_503() {
    let redis = require_redis!();

    let (status, body) = readiness(health_router(Some(redis.connect().await), || (2, 0))).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["status"], "Unhealthy");
    let entry = &body["entries"]["streams"];
    assert_eq!(entry["status"], "Unhealthy");
    assert!(
        entry["description"]
            .as_str()
            .is_some_and(|d| d.contains("none of the 2 registered stream consumers has started")),
        "{body}"
    );
    assert_eq!(entry["data"]["consumers"], 2);
    assert_eq!(entry["data"]["consumersStarted"], 0);
}

/// The counts closure is read on every request, not once at mounting.
#[tokio::test]
async fn the_consumer_counts_are_read_per_request() {
    let redis = require_redis!();
    let counts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
    let reader = counts.clone();
    let router = health_router(Some(redis.connect().await), move || {
        (reader.load(std::sync::atomic::Ordering::SeqCst), 0)
    });

    let (first, _) = readiness(router.clone()).await;
    counts.store(1, std::sync::atomic::Ordering::SeqCst);
    let (second, body) = readiness(router).await;

    assert_eq!(first, StatusCode::OK);
    assert_eq!(second, StatusCode::SERVICE_UNAVAILABLE, "{body}");
}
