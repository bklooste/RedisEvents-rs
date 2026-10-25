//! End-to-end: a host registered with `typed_handler_with::<MessagePack, _, _>` decodes MessagePack
//! bodies published with `publish_msgpack` over a real Redis. Skips without Redis. Lives in this
//! crate (not `redisevents`) because it needs the MessagePack codec.
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    clippy::missing_panics_doc
)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use redisevents::Message;
use redisevents::config::{ConsumerOptions, StreamOptions, TopicOptions};
use redisevents::consumer::{HandleResult, PartitionContext, Streams, TypedHandler};
use redisevents::producer::{PublishOptions, StreamPublisher};
use redisevents::wire::TypedMsg;
use redisevents_msgpack::{MessagePack, MessagePackPublisherExt, Options};
use redisevents_test_support::{require_redis, wait_until};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct MsgPackOrder {
    id: i32,
    name: String,
}

impl Message for MsgPackOrder {
    const TYPE: &'static str = "Tests.BuilderMsgPackOrder";
}

const COUNT: i32 = 6;

#[derive(Default)]
struct Orders(Mutex<Vec<MsgPackOrder>>);

impl TypedHandler<MsgPackOrder> for Arc<Orders> {
    async fn handle(&self, msg: &TypedMsg<MsgPackOrder>, _ctx: &PartitionContext) -> HandleResult {
        self.0.lock().unwrap().push(msg.value.clone());
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_handler_with_messagepack_decodes_published_orders() {
    let redis = require_redis!();
    let topic = redis.new_topic("builder-msgpack");
    let consumer = redis.new_consumer("builder-msgpack");

    let publisher = StreamPublisher::new(
        redis.connect().await,
        redis.keys(),
        &topic,
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await
    .expect("publisher");
    for id in 0..COUNT {
        let order = MsgPackOrder {
            id,
            name: format!("order-{id}"),
        };
        publisher
            .publish_msgpack("key", &order, &Options::default(), &PublishOptions::new())
            .await
            .expect("publish");
    }

    let mut stream_options = StreamOptions::default();
    stream_options.topics.insert(
        topic.clone(),
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    );
    let orders = Arc::new(Orders::default());
    let host = Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(stream_options)
        .topic(&topic)
        .options(ConsumerOptions {
            topic: topic.clone(),
            consumer: Some(consumer),
            persist_interval_ms: 50,
            block_ms: 200,
            ..ConsumerOptions::default()
        })
        .typed_handler_with::<MessagePack, MsgPackOrder, _>(orders.clone())
        .spawn()
        .await
        .expect("spawn host");

    wait_until(
        || orders.0.lock().unwrap().len() == COUNT as usize,
        Some(Duration::from_secs(20)),
        "every MessagePack order is decoded and delivered",
    )
    .await;
    host.shutdown().await.expect("shutdown");

    let got = orders.0.lock().unwrap().clone();
    let ids: Vec<i32> = got.iter().map(|o| o.id).collect();
    assert_eq!(ids, (0..COUNT).collect::<Vec<_>>());
    assert_eq!(got[3].name, "order-3");
}
