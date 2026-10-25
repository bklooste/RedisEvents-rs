//! End-to-end checks of every C4b registration form over a real Redis: build a host through the
//! builder, publish with `StreamPublisher`, and assert delivery. Skips without Redis.
//! The MessagePack form lives in `redisevents-msgpack/tests/builder_redis.rs` (it needs that crate).

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

use redisevents::config::{ConsumerOptions, StreamOptions, TopicOptions};
use redisevents::consumer::{
    HandleResult, PartitionContext, Streams, StreamsHost, TypedBatchHandler, TypedHandler,
};
use redisevents::producer::{PublishOptions, PublisherExt, StreamPublisher};
use redisevents::wire::{StreamMsg, TypedMsg};
use redisevents::{Json, Message};
use redisevents_test_support::{RedisFixture, require_redis, wait_until};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Order {
    pub id: i32,
}

impl Message for Order {
    const TYPE: &'static str = "Tests.BuilderOrder";
}

const COUNT: i32 = 12;
const WAIT: Option<Duration> = Some(Duration::from_secs(20));

/// The bodies seen by a handler, in arrival order, plus the number of calls.
#[derive(Default)]
pub struct Seen {
    pub ids: Mutex<Vec<i32>>,
    pub calls: Mutex<usize>,
}

impl Seen {
    pub fn count(&self) -> usize {
        self.ids.lock().unwrap().len()
    }
}

fn stream_options(topic: &str) -> StreamOptions {
    let mut o = StreamOptions::default();
    o.topics.insert(
        topic.to_owned(),
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    );
    o
}

/// Consumer options that read and flush quickly, so the test does not wait on defaults.
fn fast(topic: &str, consumer: &str) -> ConsumerOptions {
    ConsumerOptions {
        topic: topic.to_owned(),
        consumer: Some(consumer.to_owned()),
        persist_interval_ms: 50,
        block_ms: 200,
        ..ConsumerOptions::default()
    }
}

async fn publish_orders(redis: &RedisFixture, topic: &str) {
    let publisher = StreamPublisher::new(
        redis.connect().await,
        redis.keys(),
        topic,
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await
    .expect("publisher");
    for id in 0..COUNT {
        publisher
            .publish_message::<Json, Order>("key", &Order { id }, &PublishOptions::default())
            .await
            .expect("publish");
    }
}

async fn await_ids(seen: &Seen) {
    wait_until(
        || seen.count() == COUNT as usize,
        WAIT,
        "every published order is delivered to the registered handler",
    )
    .await;
}

/// Asserts the delivered ids are exactly `0..COUNT` in publish order.
fn assert_in_order(seen: &Seen) {
    let ids = seen.ids.lock().unwrap().clone();
    assert_eq!(ids, (0..COUNT).collect::<Vec<_>>());
}

/// Starts a host for `topic` through `build`, a closure that registers one handler form.
async fn run<F>(redis: &RedisFixture, topic: &str, build: F) -> StreamsHost
where
    F: FnOnce(redisevents::consumer::ConsumerBuilder) -> redisevents::consumer::StreamsBuilder,
{
    let consumer = redis.new_consumer("builder");
    let builder = Streams::builder(redis.connect().await)
        .keys(redis.keys())
        .options(stream_options(topic))
        .topic(topic)
        .options(fast(topic, &consumer));
    build(builder).spawn().await.expect("spawn host")
}

/// Decodes a JSON `Order` body; used by the message-level forms that receive raw `StreamMsg`s.
fn order_id(msg: &StreamMsg) -> i32 {
    serde_json::from_slice::<Order>(msg.body())
        .expect("json order")
        .id
}

/// Records each decoded order id; one struct serves the per-message and batch typed forms.
#[derive(Default)]
struct OrderSink(Seen);

impl TypedHandler<Order> for Arc<OrderSink> {
    async fn handle(&self, msg: &TypedMsg<Order>, _ctx: &PartitionContext) -> HandleResult {
        self.0.ids.lock().unwrap().push(msg.value.id);
        *self.0.calls.lock().unwrap() += 1;
        Ok(())
    }
}

impl TypedBatchHandler<Order> for Arc<OrderSink> {
    async fn handle_batch(
        &self,
        batch: &[TypedMsg<Order>],
        _ctx: &PartitionContext,
    ) -> HandleResult {
        self.0
            .ids
            .lock()
            .unwrap()
            .extend(batch.iter().map(|m| m.value.id));
        *self.0.calls.lock().unwrap() += 1;
        Ok(())
    }
}

/// A plain message handler: records the id from the raw body.
#[derive(Default)]
struct MessageSink(Seen);

impl redisevents::consumer::MessageHandler for MessageSink {
    async fn handle(&self, msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
        self.0.ids.lock().unwrap().push(order_id(msg));
        *self.0.calls.lock().unwrap() += 1;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_handler_receives_every_message_in_order() {
    let redis = require_redis!();
    let topic = redis.new_topic("builder-message");
    publish_orders(redis, &topic).await;

    let sink = Arc::new(MessageSink::default());
    let host = run(redis, &topic, |b| b.message_handler(sink.clone())).await;
    await_ids(&sink.0).await;
    host.shutdown().await.expect("shutdown");

    assert_in_order(&sink.0);
    // One call per message, as PerMessage promises.
    assert_eq!(*sink.0.calls.lock().unwrap(), COUNT as usize);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_fn_receives_every_message_in_order() {
    let redis = require_redis!();
    let topic = redis.new_topic("builder-message-fn");
    publish_orders(redis, &topic).await;

    let seen = Arc::new(Seen::default());
    let sink = seen.clone();
    let host = run(redis, &topic, |b| {
        b.message_fn(move |msg: StreamMsg, _ctx: PartitionContext| {
            let sink = sink.clone();
            async move {
                sink.ids.lock().unwrap().push(order_id(&msg));
                Ok(())
            }
        })
    })
    .await;
    await_ids(&seen).await;
    host.shutdown().await.expect("shutdown");

    assert_in_order(&seen);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_fn_receives_every_message_across_batches() {
    let redis = require_redis!();
    let topic = redis.new_topic("builder-batch-fn");
    publish_orders(redis, &topic).await;

    let seen = Arc::new(Seen::default());
    let sink = seen.clone();
    let host = run(redis, &topic, |b| {
        b.batch_fn(move |batch: Vec<StreamMsg>, _ctx: PartitionContext| {
            let sink = sink.clone();
            async move {
                sink.ids.lock().unwrap().extend(batch.iter().map(order_id));
                *sink.calls.lock().unwrap() += 1;
                Ok(())
            }
        })
    })
    .await;
    await_ids(&seen).await;
    host.shutdown().await.expect("shutdown");

    assert_in_order(&seen);
    assert!(*seen.calls.lock().unwrap() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_handler_decodes_json_orders() {
    let redis = require_redis!();
    let topic = redis.new_topic("builder-typed");
    publish_orders(redis, &topic).await;

    let sink = Arc::new(OrderSink::default());
    let host = run(redis, &topic, |b| b.typed_handler::<Order, _>(sink.clone())).await;
    await_ids(&sink.0).await;
    host.shutdown().await.expect("shutdown");

    assert_in_order(&sink.0);
    assert_eq!(*sink.0.calls.lock().unwrap(), COUNT as usize);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_batch_handler_decodes_json_orders() {
    let redis = require_redis!();
    let topic = redis.new_topic("builder-typed-batch");
    publish_orders(redis, &topic).await;

    let sink = Arc::new(OrderSink::default());
    let host = run(redis, &topic, |b| {
        b.typed_batch_handler::<Order, _>(sink.clone())
    })
    .await;
    await_ids(&sink.0).await;
    host.shutdown().await.expect("shutdown");

    assert_in_order(&sink.0);
}

/// The README "Consuming typed messages" snippet, mirrored so it stays compiling. Keep it in sync
/// with README.md. Compile-only: it is never run (it needs a live host).
#[allow(dead_code, clippy::future_not_send)]
mod readme_quick_start {
    use redisevents::consumer::{HandleResult, PartitionContext, Streams, TypedHandler};
    use redisevents::{Connection, Message, TypedMsg};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct OrderPlaced {
        order_id: String,
    }

    impl Message for OrderPlaced {
        const TYPE: &'static str = "Shop.OrderPlaced";
    }

    struct Shipping;

    impl TypedHandler<OrderPlaced> for Shipping {
        async fn handle(
            &self,
            msg: &TypedMsg<OrderPlaced>,
            _ctx: &PartitionContext,
        ) -> HandleResult {
            println!("ship {}", msg.value.order_id);
            Ok(())
        }
    }

    async fn run() -> redisevents::Result<()> {
        let conn = Connection::connect("redis://localhost:6379").await?;
        let host = Streams::builder(conn)
            .topic("orders")
            .typed_handler::<OrderPlaced, _>(Shipping)
            .spawn()
            .await?;
        // ... until shutdown
        host.shutdown().await
    }
}
