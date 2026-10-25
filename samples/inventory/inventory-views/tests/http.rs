//! The read side end to end over a real Redis: events are published with the C# wire types, the
//! consumer host projects them, and both routes answer. No HTTP socket: the router is driven
//! directly.
#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use inventory_domain::InventoryEvent;
use inventory_domain::events::{
    InventoryItemCreated, InventoryItemDeactivated, InventoryItemRenamed,
    ItemsCheckedInToInventory, ItemsRemovedFromInventory,
};
use inventory_views::{CONSUMER, Views, projector, router};
use redisevents::config::TopicOptions;
use redisevents::consumer::host::Streams;
use redisevents::producer::{PublishOptions, Publisher, StreamPublisher};
use redisevents_test_support::{require_redis, wait_until_async};
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

async fn get(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn publish(publisher: &StreamPublisher, event: &InventoryEvent) {
    publisher
        .publish(
            &event.id().to_string(),
            event.encode().into(),
            event.wire_type(),
            &PublishOptions::new(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn both_routes_serve_the_projected_views() {
    let redis = require_redis!();
    let (conn, keys) = (redis.connect().await, redis.keys());
    let topic = redis.new_topic("inventory");
    let publisher = StreamPublisher::new(
        conn.clone(),
        keys.clone(),
        topic.clone(),
        TopicOptions::default(),
    )
    .await
    .unwrap();

    let (id, gone) = (Uuid::new_v4(), Uuid::new_v4());
    let script = [
        InventoryEvent::Created(InventoryItemCreated {
            id,
            name: "widget".into(),
        }),
        InventoryEvent::CheckedIn(ItemsCheckedInToInventory { id, count: 10 }),
        InventoryEvent::Renamed(InventoryItemRenamed {
            id,
            new_name: "gadget".into(),
        }),
        InventoryEvent::CheckedIn(ItemsCheckedInToInventory { id, count: 7 }),
        InventoryEvent::Removed(ItemsRemovedFromInventory { id, count: 2 }),
        InventoryEvent::Created(InventoryItemCreated {
            id: gone,
            name: "short-lived".into(),
        }),
        InventoryEvent::Deactivated(InventoryItemDeactivated { id: gone }),
    ];
    for event in &script {
        publish(&publisher, event).await;
    }

    let views = Views::new(&conn, &keys, &topic).unwrap();
    let app = router(&views);
    let host = Streams::builder(conn.clone())
        .keys(keys.clone())
        .topic(&topic)
        .consumer(format!("{CONSUMER}-{}", Uuid::new_v4()))
        .handler(projector(&views, &topic).unwrap())
        .spawn()
        .await
        .unwrap();

    let detail = format!("/items/{id}");
    wait_until_async(
        || {
            let app = app.clone();
            let detail = detail.clone();
            async move {
                let (status, body) = get(&app, &detail).await;
                status == StatusCode::OK && body["currentCount"] == json!(15)
            }
        },
        Some(Duration::from_secs(30)),
        "the detail view reaches count 15",
    )
    .await;

    let (status, list) = get(&app, "/items/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list, json!([{ "id": id, "name": "gadget" }]));

    let (status, body) = get(&app, &detail).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"], json!(id));
    assert_eq!(body["name"], json!("gadget"));
    assert_eq!(body["currentCount"], json!(15));
    assert!(body["streamPosition"]["ms"].is_u64());
    assert!(body["streamPosition"]["seq"].as_u64().unwrap() > 0);

    let (status, _) = get(&app, &format!("/items/{gone}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "deactivated items are gone");
    let (status, _) = get(&app, &format!("/items/{}", Uuid::new_v4())).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get(&app, "/items/not-a-guid").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    host.shutdown().await.unwrap();
}
