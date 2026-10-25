//! Port of `InMemoryViewStoreTests` and `RedisViewStoreTests` (Redis ones skip without Redis).
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    clippy::assert_is_empty,
    clippy::cast_possible_truncation
)]

use redisevents_projections::{InMemoryViewStore, RedisViewStore, ViewStore, default_owner};
use redisevents_test_support::require_redis;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestView {
    id: String,
    name: String,
    version: i32,
}

fn view(id: &str, name: &str, version: i32) -> TestView {
    TestView {
        id: id.into(),
        name: name.into(),
        version,
    }
}

fn sorted(mut views: Vec<TestView>) -> Vec<TestView> {
    views.sort_by(|a, b| a.id.cmp(&b.id));
    views
}

#[tokio::test]
async fn set_then_get_round_trips_the_view() {
    let store = InMemoryViewStore::new();
    store.set("a1", &view("a1", "Widget", 1)).await.unwrap();
    assert_eq!(
        store.get("a1").await.unwrap(),
        Some(view("a1", "Widget", 1))
    );
}

#[tokio::test]
async fn get_on_a_missing_id_returns_none() {
    let store = InMemoryViewStore::<TestView>::new();
    assert_eq!(store.get("missing").await.unwrap(), None);
}

#[tokio::test]
async fn set_again_replaces_the_prior_value() {
    let store = InMemoryViewStore::new();
    store.set("a1", &view("a1", "Widget", 1)).await.unwrap();
    store
        .set("a1", &view("a1", "Widget renamed", 2))
        .await
        .unwrap();
    assert_eq!(
        store.get("a1").await.unwrap(),
        Some(view("a1", "Widget renamed", 2))
    );
}

#[tokio::test]
async fn delete_removes_the_view_and_a_subsequent_get_returns_none() {
    let store = InMemoryViewStore::new();
    store.set("a1", &view("a1", "Widget", 1)).await.unwrap();
    store.delete("a1").await.unwrap();
    assert_eq!(store.get("a1").await.unwrap(), None);
}

#[tokio::test]
async fn delete_on_a_missing_id_does_not_fail() {
    InMemoryViewStore::<TestView>::new()
        .delete("missing")
        .await
        .unwrap();
}

#[tokio::test]
async fn list_returns_every_set_view() {
    let store = InMemoryViewStore::new();
    let views = [
        view("a1", "One", 1),
        view("a2", "Two", 1),
        view("a3", "Three", 1),
    ];
    for v in &views {
        store.set(&v.id, v).await.unwrap();
    }
    assert_eq!(sorted(store.list().await.unwrap()), views);
}

#[tokio::test]
async fn list_on_an_empty_store_yields_nothing() {
    assert!(
        InMemoryViewStore::<TestView>::new()
            .list()
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_blank_id_is_rejected() {
    let store = InMemoryViewStore::<TestView>::new();
    assert!(store.get(" ").await.is_err());
}

// ---- Redis (RedisViewStoreTests) ----

async fn exists(redis: &redisevents_test_support::RedisFixture, key: &str, field: &str) -> bool {
    let mut conn = redis.connect().await.shared();
    redis::cmd("HEXISTS")
        .arg(key)
        .arg(field)
        .query_async(&mut conn)
        .await
        .unwrap()
}

#[tokio::test]
async fn redis_set_then_get_round_trips_a_view() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let store = RedisViewStore::new(&conn, &keys, &topic, "detail").unwrap();
    store.set("a1", &view("a1", "Widget", 1)).await.unwrap();
    assert_eq!(
        store.get("a1").await.unwrap(),
        Some(view("a1", "Widget", 1))
    );
}

#[tokio::test]
async fn redis_get_on_a_missing_id_returns_none() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let store = RedisViewStore::<TestView>::new(&conn, &keys, &topic, "detail").unwrap();
    assert_eq!(store.get("missing").await.unwrap(), None);
}

#[tokio::test]
async fn redis_delete_removes_it_and_a_subsequent_get_returns_none() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let store = RedisViewStore::new(&conn, &keys, &topic, "detail").unwrap();
    store.set("a1", &view("a1", "Widget", 1)).await.unwrap();
    store.delete("a1").await.unwrap();
    assert_eq!(store.get("a1").await.unwrap(), None);
}

#[tokio::test]
async fn redis_list_returns_every_set_view_across_scan_pages() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let store = RedisViewStore::new(&conn, &keys, &topic, "detail").unwrap();
    // More than one HSCAN page (the hash leaves its compact encoding, where HSCAN pages by 10).
    let views: Vec<_> = (0..600)
        .map(|i| view(&format!("a{i:03}"), "n", i))
        .collect();
    for v in &views {
        store.set(&v.id, v).await.unwrap();
    }
    assert_eq!(sorted(store.list().await.unwrap()), views);
}

#[tokio::test]
async fn redis_two_view_names_on_the_same_topic_do_not_collide() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let details = RedisViewStore::new(&conn, &keys, &topic, "detail").unwrap();
    let summaries = RedisViewStore::new(&conn, &keys, &topic, "summary").unwrap();

    details
        .set("a1", &view("a1", "Detail view", 1))
        .await
        .unwrap();
    summaries
        .set("a1", &view("a1", "Summary view", 7))
        .await
        .unwrap();

    assert_eq!(
        details.get("a1").await.unwrap(),
        Some(view("a1", "Detail view", 1))
    );
    assert_eq!(
        summaries.get("a1").await.unwrap(),
        Some(view("a1", "Summary view", 7))
    );
    assert_eq!(
        details.list().await.unwrap(),
        vec![view("a1", "Detail view", 1)]
    );
}

#[tokio::test]
async fn redis_default_key_uses_the_executable_name_as_owner() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let store = RedisViewStore::new(&conn, &keys, &topic, "detail").unwrap();
    store.set("a1", &view("a1", "Widget", 1)).await.unwrap();

    let expected = format!("test:re:{{{topic}}}:view:{}:detail", default_owner());
    assert_eq!(store.key(), expected);
    assert!(exists(redis, &expected, "a1").await);
}

#[tokio::test]
async fn redis_explicit_owner_is_used_in_the_key() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let store = RedisViewStore::with_owner(&conn, &keys, &topic, "detail", "wallet-views").unwrap();
    store.set("a1", &view("a1", "Widget", 1)).await.unwrap();

    assert!(
        exists(
            redis,
            &format!("test:re:{{{topic}}}:view:wallet-views:detail"),
            "a1"
        )
        .await
    );
    assert!(
        !exists(
            redis,
            &format!("test:re:{{{topic}}}:view:{}:detail", default_owner()),
            "a1"
        )
        .await
    );
}

#[tokio::test]
async fn redis_a_store_written_with_an_owner_is_read_by_the_same_owner_only() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let open = |owner| {
        RedisViewStore::<TestView>::with_owner(&conn, &keys, &topic, "detail", owner).unwrap()
    };
    let (writer, reader, stranger) = (
        open("wallet-views"),
        open("wallet-views"),
        open("other-owner"),
    );

    writer.set("a1", &view("a1", "Widget", 1)).await.unwrap();

    assert_eq!(
        reader.get("a1").await.unwrap(),
        Some(view("a1", "Widget", 1))
    );
    assert_eq!(stranger.get("a1").await.unwrap(), None);
    assert!(stranger.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn redis_a_blank_owner_topic_or_view_name_is_rejected() {
    let redis = require_redis!();
    let (conn, keys) = (redis.connect().await, redis.keys());
    for owner in ["", "  "] {
        assert!(
            RedisViewStore::<TestView>::with_owner(&conn, &keys, "t", "detail", owner).is_err()
        );
    }
    assert!(RedisViewStore::<TestView>::new(&conn, &keys, "", "detail").is_err());
    assert!(RedisViewStore::<TestView>::new(&conn, &keys, "t", " ").is_err());
}

#[tokio::test]
async fn redis_an_unreadable_value_is_a_transport_error_not_a_panic() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("views"),
    );
    let store = RedisViewStore::<TestView>::new(&conn, &keys, &topic, "detail").unwrap();
    let mut raw = redis.connect().await.shared();
    redis::cmd("HSET")
        .arg(store.key())
        .arg("a1")
        .arg("{not json")
        .query_async::<()>(&mut raw)
        .await
        .unwrap();

    assert!(matches!(
        store.get("a1").await,
        Err(redisevents::Error::Transport { .. })
    ));
}
