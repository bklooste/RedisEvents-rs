//! The command routes over real Redis: status codes per route, and what a saved item publishes.
//! Needs Redis (`REDIS_URL` or Docker; CI sets `REDISEVENTS_REQUIRE_REDIS`).
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use inventory_command::{TOPIC, router};
use inventory_domain::AGGREGATE_NAME;
use redisevents::config::TopicOptions;
use redisevents::producer::StreamStore;
use redisevents_eventsourcing::stream_name;
use redisevents_test_support::{RedisFixture, require_redis};
use tower::ServiceExt;
use uuid::Uuid;

async fn app(redis: &RedisFixture, topic: &str) -> Router {
    let options = TopicOptions {
        partitions: 1,
        ..TopicOptions::default()
    };
    let store = StreamStore::new(redis.connect().await, redis.keys(), topic, options).unwrap();
    router(inventory_command::repository(store))
}

async fn call(app: &Router, method: Method, uri: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn post(app: &Router, uri: &str) -> StatusCode {
    call(app, Method::POST, uri).await.0
}

/// The `(wire type, body)` of every entry on the topic's partition 0, oldest first.
async fn published(redis: &RedisFixture, topic: &str) -> Vec<(String, String)> {
    let key = redis.keys().stream(topic, 0, true);
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(&key)
        .arg("-")
        .arg("+")
        .query_async(&mut redis.connect().await.shared())
        .await
        .unwrap();
    reply
        .ids
        .into_iter()
        .map(|entry| {
            let field = |name: &str| {
                let value = entry.map.get(name).unwrap().clone();
                String::from_utf8(redis::from_redis_value::<Vec<u8>>(value).unwrap()).unwrap()
            };
            (field("t"), field("b"))
        })
        .collect()
}

/// Adds an item with a fresh id and returns the id.
async fn add_item(app: &Router, name: &str) -> Uuid {
    let id = Uuid::new_v4();
    assert_eq!(
        post(app, &format!("/InventoryCommand/Add?name={name}&id={id}")).await,
        StatusCode::NO_CONTENT
    );
    id
}

#[test]
fn the_topic_and_aggregate_names_are_the_csharp_ones() {
    assert_eq!(TOPIC, "inventory");
    assert_eq!(AGGREGATE_NAME, "Inventory");
}

#[tokio::test]
async fn is_available_is_200_true() {
    let redis = require_redis!();
    let app = app(redis, &redis.new_topic("cmd-available")).await;

    let (status, body) = call(&app, Method::GET, "/InventoryCommand/IsAvailable").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "true");
}

#[tokio::test]
async fn add_is_204_and_publishes_created_with_csharp_wire_format() {
    let redis = require_redis!();
    let topic = redis.new_topic("cmd-add");
    let app = app(redis, &topic).await;
    let id = Uuid::new_v4();

    let status = post(&app, &format!("/InventoryCommand/Add?name=Widget&id={id}")).await;

    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        published(redis, &topic).await,
        vec![(
            "inventory.created".to_owned(),
            format!(r#"{{"Id":"{id}","Name":"Widget"}}"#)
        )]
    );
    // The state stream is the aggregate's source of truth: one entry, version 1.
    let state = redis
        .keys()
        .state(&topic, &stream_name(AGGREGATE_NAME, &id.to_string()));
    let len: u64 = redis::cmd("XLEN")
        .arg(&state)
        .query_async(&mut redis.connect().await.shared())
        .await
        .unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn add_without_an_id_generates_one() {
    let redis = require_redis!();
    let topic = redis.new_topic("cmd-add-generated");
    let app = app(redis, &topic).await;

    assert_eq!(
        post(&app, "/InventoryCommand/Add?name=Widget").await,
        StatusCode::NO_CONTENT
    );
    let events = published(redis, &topic).await;
    assert_eq!(events.len(), 1);
    assert!(events[0].1.starts_with(r#"{"Id":""#), "{}", events[0].1);
}

#[tokio::test]
async fn add_status_codes() {
    let redis = require_redis!();
    let app = app(redis, &redis.new_topic("cmd-add-codes")).await;
    let id = Uuid::new_v4();

    // Missing and empty names are domain errors: 400, as in C#.
    assert_eq!(
        post(&app, &format!("/InventoryCommand/Add?id={id}")).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(&app, &format!("/InventoryCommand/Add?name=&id={id}")).await,
        StatusCode::BAD_REQUEST
    );
    // A malformed id is a query-binding error: 400.
    assert_eq!(
        post(&app, "/InventoryCommand/Add?name=x&id=not-a-guid").await,
        StatusCode::BAD_REQUEST
    );
    // A second Add of the same id is a lost version check (expected 0): 409.
    assert_eq!(
        post(&app, &format!("/InventoryCommand/Add?name=x&id={id}")).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        post(&app, &format!("/InventoryCommand/Add?name=x&id={id}")).await,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn change_name_status_codes_and_event() {
    let redis = require_redis!();
    let topic = redis.new_topic("cmd-rename");
    let app = app(redis, &topic).await;
    let id = add_item(&app, "widget").await;
    let unknown = Uuid::new_v4();

    // Missing item: 404, checked before the name is validated (as in C#).
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/ChangeName?id={unknown}&name=g&version=0")
        )
        .await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/ChangeName?id={id}&name=&version=1")
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/ChangeName?id={id}&name=g&version=0")
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/ChangeName?id={id}&name=g")
        )
        .await,
        StatusCode::BAD_REQUEST,
        "missing version"
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/ChangeName?id={id}&name=gadget&version=1")
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let events = published(redis, &topic).await;
    assert_eq!(events[1].0, "inventory.renamed");
    assert_eq!(
        events[1].1,
        format!(r#"{{"Id":"{id}","NewName":"gadget"}}"#)
    );
}

#[tokio::test]
async fn deactivate_status_codes_and_event() {
    let redis = require_redis!();
    let topic = redis.new_topic("cmd-deactivate");
    let app = app(redis, &topic).await;
    let id = add_item(&app, "widget").await;

    assert_eq!(
        post(
            &app,
            &format!(
                "/InventoryCommand/Deactivate?id={}&version=0",
                Uuid::new_v4()
            )
        )
        .await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/Deactivate?id={id}&version=0")
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/Deactivate?id={id}&version=1")
        )
        .await,
        StatusCode::NO_CONTENT
    );
    // Already deactivated is a domain error: 400, whatever the version.
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/Deactivate?id={id}&version=2")
        )
        .await,
        StatusCode::BAD_REQUEST
    );

    let events = published(redis, &topic).await;
    assert_eq!(events[1].0, "inventory.deactivated");
    assert_eq!(events[1].1, format!(r#"{{"Id":"{id}"}}"#));
}

#[tokio::test]
async fn check_in_status_codes_and_event() {
    let redis = require_redis!();
    let topic = redis.new_topic("cmd-checkin");
    let app = app(redis, &topic).await;
    let id = add_item(&app, "widget").await;

    assert_eq!(
        post(
            &app,
            &format!(
                "/InventoryCommand/CheckIn?id={}&number=1&version=0",
                Uuid::new_v4()
            )
        )
        .await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/CheckIn?id={id}&number=0&version=1")
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/CheckIn?id={id}&number=7&version=0")
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        post(&app, &format!("/InventoryCommand/CheckIn?id={id}&number=7")).await,
        StatusCode::BAD_REQUEST,
        "missing version"
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/CheckIn?id={id}&number=7&version=1")
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let events = published(redis, &topic).await;
    assert_eq!(events[1].0, "inventory.checked-in");
    assert_eq!(events[1].1, format!(r#"{{"Id":"{id}","Count":7}}"#));
}

#[tokio::test]
async fn remove_status_codes_and_event() {
    let redis = require_redis!();
    let topic = redis.new_topic("cmd-remove");
    let app = app(redis, &topic).await;
    let id = add_item(&app, "widget").await;

    assert_eq!(
        post(
            &app,
            &format!(
                "/InventoryCommand/Remove?id={}&number=1&version=0",
                Uuid::new_v4()
            )
        )
        .await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/Remove?id={id}&number=-1&version=1")
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/Remove?id={id}&number=3&version=0")
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/Remove?id={id}&number=3&version=1")
        )
        .await,
        StatusCode::NO_CONTENT
    );

    let events = published(redis, &topic).await;
    assert_eq!(events[1].0, "inventory.removed");
    assert_eq!(events[1].1, format!(r#"{{"Id":"{id}","Count":3}}"#));
}

#[tokio::test]
async fn a_negative_version_conflicts_rather_than_saving_unchecked() {
    let redis = require_redis!();
    let topic = redis.new_topic("cmd-negative");
    let app = app(redis, &topic).await;
    let id = add_item(&app, "widget").await;

    assert_eq!(
        post(
            &app,
            &format!("/InventoryCommand/CheckIn?id={id}&number=1&version=-1")
        )
        .await,
        StatusCode::CONFLICT
    );
    assert_eq!(published(redis, &topic).await.len(), 1);
}
