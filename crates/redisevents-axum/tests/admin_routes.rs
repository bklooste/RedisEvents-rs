//! The admin routes against a real Redis, driven through the router with `oneshot`.
//!
//! Ported from C# `RedisEvents.Tests/AdminEndpointServiceTests.cs`. The C# tests call the handlers
//! directly; these go through the mounted router, so routing, the authorization layer and the
//! status codes are covered too.

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::Request as AxumRequest;
use axum::http::{Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use redisevents::admin::meta::ensure_topic_meta;
use redisevents::admin::stream_admin::{ResetOptions, ResetTarget, StreamAdmin};
use redisevents::config::TopicOptions;
use redisevents::wire::StreamId;
use redisevents_axum::{StreamAdminOptions, stream_admin_router};
use redisevents_test_support::{RedisFixture, require_redis};
use tower::ServiceExt as _;

/// Opens the admin router under `/admin/streams`, with the allow-everything layer a test host uses.
async fn app(redis: &RedisFixture, options: StreamAdminOptions) -> Router {
    let conn = redis.connect().await;
    let admin = stream_admin_router(
        conn,
        redis.keys(),
        options,
        middleware::from_fn(pass_through),
    );
    Router::new().nest("/admin/streams", admin)
}

async fn pass_through(request: AxumRequest, next: Next) -> Response {
    next.run(request).await
}

async fn send(app: &Router, method: Method, uri: &str) -> (StatusCode, String) {
    let request = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("infallible");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, String::from_utf8(bytes.to_vec()).expect("utf-8"))
}

async fn get(app: &Router, uri: &str) -> (StatusCode, String) {
    send(app, Method::GET, uri).await
}

async fn post(app: &Router, uri: &str) -> (StatusCode, String) {
    send(app, Method::POST, uri).await
}

fn enc(value: &str) -> String {
    value.replace('}', "%7D").replace('{', "%7B")
}

async fn ensure(redis: &RedisFixture, topic: &str, options: &TopicOptions) {
    let conn = redis.connect().await;
    ensure_topic_meta(&conn, &redis.keys(), topic, options)
        .await
        .expect("ensure topic");
}

/// Seeds `count` entries into partition 0 of `topic`, one second apart, and returns their ids.
async fn seed(
    redis: &RedisFixture,
    topic: &str,
    options: &TopicOptions,
    count: usize,
) -> Vec<StreamId> {
    let keys = redis.keys();
    let key = keys.stream(topic, 0, options.co_locate_partitions);
    let mut db = redis.connect().await.shared();
    let now_ms =
        u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp()).expect("after 1970") * 1000;
    let first = now_ms - u64::try_from(count).expect("small") * 60_000;
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let id = StreamId::new(first + u64::try_from(i).expect("small") * 1000, 0);
        let _: String = redis::cmd("XADD")
            .arg(&key)
            .arg(id.to_string())
            .arg("b")
            .arg(i)
            .query_async(&mut db)
            .await
            .expect("xadd");
        ids.push(id);
    }
    ids
}

async fn stored_positions(
    redis: &RedisFixture,
    topic: &str,
    consumer: &str,
) -> Vec<(String, String)> {
    let mut db = redis.connect().await.shared();
    let map: std::collections::BTreeMap<String, String> = redis::cmd("HGETALL")
        .arg(redis.keys().positions(topic, consumer))
        .query_async(&mut db)
        .await
        .expect("hgetall");
    map.into_iter().collect()
}

async fn set_owner(redis: &RedisFixture, topic: &str, consumer: &str, partition: u32, owner: &str) {
    let mut db = redis.connect().await.shared();
    let _: i64 = redis::cmd("HSET")
        .arg(redis.keys().ownership(topic, consumer))
        .arg(partition)
        .arg(owner)
        .query_async(&mut db)
        .await
        .expect("hset");
}

#[tokio::test]
async fn ownership_route_finds_the_consumers_of_a_topic() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-owner");
    let consumer = redis.new_consumer("worker");
    ensure(
        redis,
        &topic,
        &TopicOptions {
            partitions: 2,
            ..TopicOptions::default()
        },
    )
    .await;
    set_owner(redis, &topic, &consumer, 0, "pod-0").await;
    let app = app(redis, StreamAdminOptions::default()).await;

    let (status, body) = get(&app, &format!("/admin/streams/{topic}/ownership")).await;

    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("No consumers found"), "{body}");
    assert!(body.contains(&format!("Consumer: {consumer}")), "{body}");
    assert!(body.contains("Owned: 1"), "{body}");
    assert!(body.contains("Unowned partitions: 1"), "{body}");
}

#[tokio::test]
async fn ownership_route_refuses_a_topic_name_that_would_re_tag_the_key() {
    let redis = require_redis!();
    let app = app(redis, StreamAdminOptions::default()).await;

    let (status, body) = get(
        &app,
        &format!("/admin/streams/{}/ownership", enc("orders}:{other")),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("topic"), "{body}");
}

#[tokio::test]
async fn preview_reads_a_non_co_located_topic_with_the_layout_it_actually_has() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-spread-preview");
    let consumer = redis.new_consumer("worker");
    let options = TopicOptions {
        partitions: 1,
        co_locate_partitions: false,
        ..TopicOptions::default()
    };
    ensure(redis, &topic, &options).await;
    let ids = seed(redis, &topic, &options, 3).await;
    let app = app(redis, StreamAdminOptions::default()).await;

    // The route has no configuration to hand; the layout must be read back from Redis.
    let (status, body) = get(
        &app,
        &format!("/admin/streams/{topic}/{consumer}/reset/preview?to=end"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.contains("of 3"),
        "the partition really holds three entries: {body}"
    );
    assert!(
        body.contains(&ids[2].to_string()),
        "the target is the partition's own tail: {body}"
    );
}

#[tokio::test]
async fn reset_to_end_on_a_non_co_located_topic_writes_the_tail_not_zero() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-spread-apply");
    let consumer = redis.new_consumer("worker");
    let options = TopicOptions {
        partitions: 1,
        co_locate_partitions: false,
        ..TopicOptions::default()
    };
    ensure(redis, &topic, &options).await;
    let ids = seed(redis, &topic, &options, 3).await;
    let app = app(redis, StreamAdminOptions::default()).await;

    let (status, body) = post(
        &app,
        &format!("/admin/streams/{topic}/{consumer}/reset?to=end"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("Applied reset to 1 partition(s)."), "{body}");
    let stored = stored_positions(redis, &topic, &consumer).await;
    assert!(
        stored
            .iter()
            .any(|(field, value)| field == "0" && value.starts_with(&ids[2].to_string())),
        "0-0 here would be a rewind to the beginning, the opposite of skipping the backlog: {stored:?}"
    );
}

#[tokio::test]
async fn a_reset_to_the_end_of_a_stream_that_is_not_there_is_refused_rather_than_writing_zero() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-missing-stream");
    let consumer = redis.new_consumer("worker");
    // Metadata but no streams: the shape a wrong topic name, or the wrong layout, produces.
    let mut db = redis.connect().await.shared();
    let _: i64 = redis::cmd("HSET")
        .arg(redis.keys().topic_meta(&topic))
        .arg("partitions")
        .arg(1)
        .query_async(&mut db)
        .await
        .expect("hset");
    let app = app(redis, StreamAdminOptions::default()).await;

    let (status, body) = post(
        &app,
        &format!("/admin/streams/{topic}/{consumer}/reset?to=end"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("does not exist"), "{body}");
    assert_eq!(stored_positions(redis, &topic, &consumer).await, Vec::new());
}

#[tokio::test]
async fn an_apply_is_refused_while_the_consumer_still_holds_claims() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-running");
    let consumer = redis.new_consumer("worker");
    let options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    ensure(redis, &topic, &options).await;
    seed(redis, &topic, &options, 2).await;
    set_owner(redis, &topic, &consumer, 0, "pod-0").await;
    let app = app(redis, StreamAdminOptions::default()).await;
    let uri = format!("/admin/streams/{topic}/{consumer}/reset");

    let (status, body) = post(&app, &format!("{uri}?to=start")).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body.contains("scale to zero"), "{body}");
    assert_eq!(
        stored_positions(redis, &topic, &consumer).await,
        Vec::new(),
        "a refused reset writes nothing"
    );

    // A preview is never refused; it says so and carries on.
    let (preview_status, preview_body) = get(
        &app,
        &format!("/admin/streams/{topic}/{consumer}/reset/preview?to=start"),
    )
    .await;
    assert_eq!(preview_status, StatusCode::OK);
    assert!(preview_body.contains("Warning:"), "{preview_body}");

    let (forced_status, _) = post(&app, &format!("{uri}?to=start&force=true")).await;
    assert_eq!(forced_status, StatusCode::OK);
    let stored = stored_positions(redis, &topic, &consumer).await;
    assert!(
        stored
            .iter()
            .any(|(field, value)| field == "0" && value.starts_with("0-0")),
        "{stored:?}"
    );
}

#[tokio::test]
async fn allow_reset_while_running_applies_without_force() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-allowed");
    let consumer = redis.new_consumer("worker");
    ensure(
        redis,
        &topic,
        &TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await;
    set_owner(redis, &topic, &consumer, 0, "pod-0").await;
    let mut options = StreamAdminOptions::default();
    options.allow_reset_while_running = true;
    let app = app(redis, options).await;

    let (status, body) = post(
        &app,
        &format!("/admin/streams/{topic}/{consumer}/reset?to=start"),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("Warning:"), "{body}");
}

#[tokio::test]
async fn a_partition_past_the_end_of_the_topic_is_refused() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-range");
    let consumer = redis.new_consumer("worker");
    ensure(
        redis,
        &topic,
        &TopicOptions {
            partitions: 2,
            ..TopicOptions::default()
        },
    )
    .await;
    let app = app(redis, StreamAdminOptions::default()).await;

    let (status, body) = post(
        &app,
        &format!("/admin/streams/{topic}/{consumer}/reset?to=start&partition=7"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("does not exist"), "{body}");
    assert_eq!(
        stored_positions(redis, &topic, &consumer).await,
        Vec::new(),
        "nothing is written"
    );
}

#[tokio::test]
async fn the_core_reset_refuses_a_partition_past_the_end_too() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-core-range");
    let consumer = redis.new_consumer("worker");
    ensure(
        redis,
        &topic,
        &TopicOptions {
            partitions: 2,
            ..TopicOptions::default()
        },
    )
    .await;
    let admin = StreamAdmin::new(redis.connect().await, redis.keys());
    let options = ResetOptions {
        partition: Some(9),
        ..ResetOptions::default()
    };

    let error = admin
        .reset_position(&topic, &consumer, ResetTarget::Start, &options)
        .await
        .expect_err("partition 9 does not exist");

    assert!(
        error.to_string().contains("partition 9 does not exist"),
        "{error}"
    );
}

#[tokio::test]
async fn an_unknown_topic_answers_404() {
    let redis = require_redis!();
    let app = app(redis, StreamAdminOptions::default()).await;

    let (status, body) = get(
        &app,
        &format!(
            "/admin/streams/{}/{}/reset/preview?to=start",
            redis.new_topic("admin-unknown"),
            redis.new_consumer("w")
        ),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(body.contains("never been created"), "{body}");
}

#[tokio::test]
async fn a_missing_or_doubled_target_is_refused() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-targets");
    let consumer = redis.new_consumer("worker");
    let app = app(redis, StreamAdminOptions::default()).await;

    let (none, none_body) = get(
        &app,
        &format!("/admin/streams/{topic}/{consumer}/reset/preview"),
    )
    .await;
    assert_eq!(none, StatusCode::BAD_REQUEST);
    assert!(none_body.contains("exactly one"), "{none_body}");

    let (both, _) = get(
        &app,
        &format!(
            "/admin/streams/{topic}/{consumer}/reset/preview?from=2026-09-01T00:00:00Z&to=end"
        ),
    )
    .await;
    assert_eq!(both, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_preview_with_no_stored_position_says_what_it_assumed() {
    let redis = require_redis!();
    let topic = redis.new_topic("admin-assumed");
    let consumer = redis.new_consumer("worker");
    let options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    ensure(redis, &topic, &options).await;
    let ids = seed(redis, &topic, &options, 4).await;
    let app = app(redis, StreamAdminOptions::default()).await;
    let from = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ids[1].ms) * 1_000_000)
        .expect("in range")
        .format(&time::format_description::well_known::Rfc3339)
        .expect("format");

    // when_missing defaults to beginning: the consumer would have started at 0-0, so moving to the
    // second entry skips one.
    let (status, beginning) = get(
        &app,
        &format!(
            "/admin/streams/{topic}/{consumer}/reset/preview?from={}",
            enc(&from)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{beginning}");
    assert!(beginning.contains("assuming 0-0"), "{beginning}");
    assert!(beginning.contains("skip"), "{beginning}");

    // when_missing=now: it would have started at the tail, so the same request is a rewind.
    let (status, now) = get(
        &app,
        &format!(
            "/admin/streams/{topic}/{consumer}/reset/preview?from={}&when_missing=now",
            enc(&from)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{now}");
    assert!(now.contains(&format!("assuming {}", ids[3])), "{now}");
    assert!(now.contains("rewind"), "{now}");
    assert!(now.contains("of 4"), "{now}");
}

#[tokio::test]
async fn the_layout_probe_reads_back_what_redis_holds() {
    let redis = require_redis!();
    let spread = redis.new_topic("admin-probe-spread");
    let tagged = redis.new_topic("admin-probe-tagged");
    ensure(
        redis,
        &spread,
        &TopicOptions {
            partitions: 2,
            co_locate_partitions: false,
            ..TopicOptions::default()
        },
    )
    .await;
    ensure(
        redis,
        &tagged,
        &TopicOptions {
            partitions: 3,
            ..TopicOptions::default()
        },
    )
    .await;
    let admin = StreamAdmin::new(redis.connect().await, redis.keys());

    let spread_layout = admin.read_topic_layout(&spread).await.expect("layout");
    assert!(!spread_layout.co_locate_partitions);
    assert_eq!(spread_layout.partitions, 2);

    let tagged_layout = admin.read_topic_layout(&tagged).await.expect("layout");
    assert!(tagged_layout.co_locate_partitions);
    assert_eq!(tagged_layout.partitions, 3);

    let unknown = admin
        .read_topic_layout(&redis.new_topic("admin-probe-none"))
        .await
        .expect("layout");
    assert_eq!(
        unknown.partitions, 0,
        "0 is how a caller tells 'no such topic' from 'one partition'"
    );
}

#[tokio::test]
async fn every_route_passes_through_the_authorization_layer() {
    let redis = require_redis!();
    let conn = redis.connect().await;
    let deny = middleware::from_fn(|_request: AxumRequest, _next: Next| async {
        (StatusCode::UNAUTHORIZED, "no")
    });
    let app = Router::new().nest(
        "/admin/streams",
        stream_admin_router(conn, redis.keys(), StreamAdminOptions::default(), deny),
    );
    let topic = redis.new_topic("admin-auth");
    let consumer = redis.new_consumer("worker");

    for (method, uri) in [
        (
            Method::GET,
            format!("/admin/streams/{topic}/{consumer}/reset/preview?to=start"),
        ),
        (
            Method::POST,
            format!("/admin/streams/{topic}/{consumer}/reset?to=start"),
        ),
        (Method::GET, format!("/admin/streams/{topic}/ownership")),
    ] {
        let (status, _) = send(&app, method.clone(), &uri).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {uri} must carry the layer"
        );
    }
}

#[tokio::test]
async fn an_unknown_path_is_not_found_and_never_reaches_the_layer() {
    let redis = require_redis!();
    let app = app(redis, StreamAdminOptions::default()).await;

    let (status, _) = get(&app, "/admin/streams/only-one-segment").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}
