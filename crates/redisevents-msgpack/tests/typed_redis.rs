//! Typed MessagePack round-trips over a real Redis: publish through the B1 `StreamPublisher`, read
//! the raw stream entry back, and decode it through the `Typed` adapter (and by hand).
//!
//! Skipped (no Redis): run `REDIS_URL=redis://... cargo test` or start Docker. C# DI registration
//! tests (`MessagePackRegistrationTests`) are not ported: the builder/host registration is C4b.
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unused_async
)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use bytes::Bytes;
use redisevents::config::TopicOptions;
use redisevents::consumer::{BatchHandler, CancellationToken, MessageHandler, PartitionContext};
use redisevents::consumer::{HandleResult, TypedBatchHandler, TypedHandler};
use redisevents::producer::{PublishOptions, PublisherExt, StreamPublisher};
use redisevents::wire::{CODEC_VERSION, Keys, StreamId, StreamMsg, TypedMsg, codec};
use redisevents::{Connection, Message, PayloadCodec};
use redisevents_msgpack::{
    Compression, Layout, MessagePack, MessagePackMapTyped, MessagePackMsgExt,
    MessagePackPublisherExt, MessagePackTyped, Options, decompress, encode_plain,
};
use redisevents_test_support::require_redis;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    id: i32,
    name: String,
}

impl Message for Order {
    const TYPE: &'static str = "Tests.Order";
}

/// Above the 1024-byte default threshold once encoded, and compressible.
fn large(id: i32) -> Order {
    Order {
        id,
        name: "abcdefgh".repeat(400),
    }
}

fn small(id: i32) -> Order {
    Order {
        id,
        name: format!("order-{id}"),
    }
}

fn options(layout: Layout, compression: Compression) -> Options {
    Options {
        layout,
        compression,
    }
}

/// Reads every entry of `key` with XRANGE and decodes it through the same codec path the consumer
/// uses (`wire::codec::decode`), so the result is a real `StreamMsg`.
async fn read_back(conn: &Connection, key: &str) -> Vec<StreamMsg> {
    let entries: Vec<(String, HashMap<String, Vec<u8>>)> = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .query_async(&mut conn.shared())
        .await
        .expect("XRANGE");
    entries
        .into_iter()
        .map(|(id, fields)| {
            let fields = fields.into_iter().map(|(k, v)| (k, Bytes::from(v)));
            codec::decode(StreamId::parse(&id).unwrap(), 0, fields, CODEC_VERSION)
                .expect("decode entry")
        })
        .collect()
}

/// A stream, its publisher, and a connection to read back with.
async fn setup(
    fixture: &redisevents_test_support::RedisFixture,
) -> (StreamPublisher, Connection, String, Keys) {
    let conn = fixture.connect().await;
    let keys = fixture.keys();
    let topic = fixture.new_topic("msgpack");
    let publisher = StreamPublisher::new(
        conn.clone(),
        keys.clone(),
        topic.clone(),
        TopicOptions::default(),
    )
    .await
    .expect("publisher");
    let key = keys.stream(&topic, 0, TopicOptions::default().co_locate_partitions);
    (publisher, conn, key, keys)
}

#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<Order>>,
}

impl TypedHandler<Order> for Recorder {
    fn handle(
        &self,
        msg: &TypedMsg<Order>,
        _ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send {
        self.seen.lock().unwrap().push(msg.value.clone());
        std::future::ready(Ok(()))
    }
}

impl TypedBatchHandler<Order> for Recorder {
    fn handle_batch(
        &self,
        batch: &[TypedMsg<Order>],
        _ctx: &PartitionContext,
    ) -> impl Future<Output = HandleResult> + Send {
        self.seen
            .lock()
            .unwrap()
            .extend(batch.iter().map(|m| m.value.clone()));
        std::future::ready(Ok(()))
    }
}

fn ctx() -> PartitionContext {
    PartitionContext::new("topic", "consumer", 0, CancellationToken::new())
}

/// Publishes `order` with `opts`, reads it back, and checks the typed adapter decodes it equal.
async fn round_trip(order: &Order, opts: Options) {
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;

    publisher
        .publish_msgpack("k", order, &opts, &PublishOptions::new())
        .await
        .unwrap();

    let entries = read_back(&conn, &key).await;
    let entry = entries.first().expect("one entry");
    assert_eq!(entry.msg_type(), Order::TYPE);
    assert_eq!(entries.len(), 1);

    // Hand decode.
    assert_eq!(&entry.decode_msgpack::<Order>().unwrap(), order);

    // Through the Typed adapter (single-message path).
    let typed = MessagePackTyped::new(Recorder::default());
    typed.handle(entry, &ctx()).await.unwrap();
    assert_eq!(
        typed.inner().seen.lock().unwrap().as_slice(),
        std::slice::from_ref(order)
    );
}

#[tokio::test]
async fn small_array_round_trips_uncompressed() {
    let order = small(1);
    round_trip(&order, Options::default()).await;

    // Below the threshold the stored body is exactly the plain array encoding.
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    publisher
        .publish_msgpack("k", &order, &Options::default(), &PublishOptions::new())
        .await
        .unwrap();
    let entry = read_back(&conn, &key).await.remove(0);
    assert_eq!(
        entry.body().as_ref(),
        encode_plain(&order, Layout::Array).unwrap().as_slice()
    );
}

#[tokio::test]
async fn small_map_round_trips_uncompressed() {
    round_trip(&small(2), options(Layout::Map, Compression::DEFAULT)).await;
}

#[tokio::test]
async fn large_array_round_trips_compressed() {
    let order = large(3);
    round_trip(&order, Options::default()).await;

    // Above the threshold the stored body is Lz4BlockArray framed, and smaller than plain.
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    publisher
        .publish_msgpack("k", &order, &Options::default(), &PublishOptions::new())
        .await
        .unwrap();
    let entry = read_back(&conn, &key).await.remove(0);
    let plain = encode_plain(&order, Layout::Array).unwrap();
    assert!(
        entry.body().len() < plain.len(),
        "a large repetitive body should compress"
    );
    assert_eq!(decompress(entry.body()).unwrap().as_ref(), plain.as_slice());
}

#[tokio::test]
async fn large_map_round_trips_compressed() {
    round_trip(&large(4), options(Layout::Map, Compression::DEFAULT)).await;
}

#[tokio::test]
async fn disabled_compression_stores_large_body_plain() {
    let order = large(5);
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    let opts = options(Layout::Array, Compression::DISABLED);
    publisher
        .publish_msgpack("k", &order, &opts, &PublishOptions::new())
        .await
        .unwrap();
    let entry = read_back(&conn, &key).await.remove(0);
    assert_eq!(
        entry.body().as_ref(),
        encode_plain(&order, Layout::Array).unwrap().as_slice()
    );
    assert_eq!(&entry.decode_msgpack::<Order>().unwrap(), &order);
}

#[tokio::test]
async fn lowered_threshold_compresses_a_small_body() {
    let order = small(6);
    let plain = encode_plain(&order, Layout::Array).unwrap();
    assert!(plain.len() > 5 && plain.len() < 1024);
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    let opts = options(
        Layout::Array,
        Compression {
            threshold: 5,
            ..Compression::DEFAULT
        },
    );
    publisher
        .publish_msgpack("k", &order, &opts, &PublishOptions::new())
        .await
        .unwrap();
    let entry = read_back(&conn, &key).await.remove(0);
    assert_ne!(entry.body().as_ref(), plain.as_slice());
    assert_eq!(&entry.decode_msgpack::<Order>().unwrap(), &order);
}

#[tokio::test]
async fn batch_publish_and_batch_handler_keep_order() {
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    let orders = vec![small(10), large(11), small(12), large(13)];

    publisher
        .publish_msgpack_batch("k", &orders, &Options::default(), &PublishOptions::new())
        .await
        .unwrap();

    let entries = read_back(&conn, &key).await;
    assert_eq!(entries.len(), orders.len());
    let typed = MessagePackTyped::new(Recorder::default());
    typed.handle_batch(&entries, &ctx()).await.unwrap();
    assert_eq!(*typed.inner().seen.lock().unwrap(), orders);
}

#[tokio::test]
async fn map_layout_body_decodes_through_the_map_typed_alias() {
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    let order = large(20);
    let opts = options(Layout::Map, Compression::DEFAULT);
    publisher
        .publish_msgpack("k", &order, &opts, &PublishOptions::new())
        .await
        .unwrap();

    let entry = read_back(&conn, &key).await.remove(0);
    let typed: MessagePackMapTyped<Recorder, Order> = MessagePackMapTyped::new(Recorder::default());
    typed.handle(&entry, &ctx()).await.unwrap();
    assert_eq!(
        typed.inner().seen.lock().unwrap().as_slice(),
        std::slice::from_ref(&order)
    );
}

#[tokio::test]
async fn core_publish_message_with_msgpack_codec_is_readable_by_the_typed_adapter() {
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    let order = large(30);
    publisher
        .publish_message::<MessagePack, _>("k", &order, &PublishOptions::new())
        .await
        .unwrap();
    let entry = read_back(&conn, &key).await.remove(0);
    let typed = MessagePackTyped::new(Recorder::default());
    typed.handle(&entry, &ctx()).await.unwrap();
    assert_eq!(
        typed.inner().seen.lock().unwrap().as_slice(),
        std::slice::from_ref(&order)
    );
}

#[tokio::test]
async fn publish_metadata_survives_the_msgpack_path() {
    let fixture = require_redis!();
    let (publisher, conn, key, _) = setup(fixture).await;
    let publish = PublishOptions::new()
        .correlation_id("corr-1")
        .header("tenant", "acme");
    publisher
        .publish_msgpack("order-key", &small(40), &Options::default(), &publish)
        .await
        .unwrap();
    let entry = read_back(&conn, &key).await.remove(0);
    assert_eq!(entry.partition_key(), "order-key");
    assert_eq!(entry.correlation_id(), "corr-1");
    assert_eq!(
        entry.headers().get("tenant").unwrap().as_deref(),
        Some("acme")
    );
}

#[tokio::test]
async fn corrupt_lz4_body_through_the_adapter_is_a_skip() {
    let order = large(50);
    let mut body = MessagePack::encode(&order).unwrap().to_vec();
    // Corrupt the framing: keep the first bytes (array + ext header) and break the LZ4 payload.
    let n = body.len();
    body.truncate(n / 2);
    let msg = StreamMsg::new(StreamId::new(1, 0), 0, Order::TYPE, Bytes::from(body));
    let typed = MessagePackTyped::new(Recorder::default());
    let err = typed.handle(&msg, &ctx()).await.unwrap_err();
    assert!(!err.is_block(), "a malformed body is skipped, not blocked");
    assert!(typed.inner().seen.lock().unwrap().is_empty());
}

#[test]
fn encode_options_round_trip_without_redis() {
    let o = small(60);
    let body = redisevents_msgpack::encode_with(&o, &Options::default()).unwrap();
    assert_eq!(redisevents_msgpack::decode::<Order>(&body).unwrap(), o);
}
