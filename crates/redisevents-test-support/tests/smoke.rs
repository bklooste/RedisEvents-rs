//! Smoke test: the fixture yields a Redis the library can talk to. Skips without Redis.

#[tokio::test]
async fn ping() {
    let redis = redisevents_test_support::require_redis!();
    let mut conn = redis.connect().await.shared();
    let pong: String = redis::cmd("PING").query_async(&mut conn).await.unwrap();
    assert_eq!(pong, "PONG");

    let mut dedicated = redis.connect().await.dedicated().await.unwrap();
    let pong: String = redis::cmd("PING")
        .query_async(&mut dedicated)
        .await
        .unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
async fn server_supports_hexpire() {
    let redis = redisevents_test_support::require_redis!();
    let mut conn = redis.connect().await.shared();
    let key = format!(
        "{}smoke:{}",
        redis.keys().prefix(),
        redis.new_topic("hexpire")
    );
    let _: () = redis::cmd("HSET")
        .arg(&key)
        .arg("f")
        .arg("v")
        .query_async(&mut conn)
        .await
        .unwrap();
    let set: Vec<i64> = redis::cmd("HEXPIRE")
        .arg(&key)
        .arg(30)
        .arg("FIELDS")
        .arg(1)
        .arg("f")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(set, [1]);
    let _: () = redis::cmd("DEL")
        .arg(&key)
        .query_async(&mut conn)
        .await
        .unwrap();
}
