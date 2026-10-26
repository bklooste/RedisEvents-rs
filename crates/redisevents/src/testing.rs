//! In-memory test doubles (feature `testing`). **Owner: D4** (port of `RedisEvents/Testing/*`):
//! in-memory [`Publisher`] / [`BufferedPublisher`] doubles, an in-memory [`PositionStore`] and a
//! [`StreamTestHarness`] that drives handlers with the same [`StreamMsg`] and [`PartitionContext`]
//! shape the consumer hands them, and the same error-contract outcomes, without Redis.
//!
//! Routing is real: the doubles use the production partition router ([`router`]), so a test that
//! asserts "these keys share a partition" holds against Redis too. A real Redis for integration
//! tests is the `redisevents-test-support` crate, not this module.
//!
//! ```no_run
//! # use redisevents::testing::{InMemoryPublisher, StreamTestHarness};
//! # use redisevents::{PublishOptions, Publisher};
//! # async fn demo(handler: impl redisevents::MessageHandler) -> redisevents::Result<()> {
//! let publisher = InMemoryPublisher::new(8)?;
//! publisher.publish("order-1", bytes::Bytes::from_static(b"{}"), "OrderPlaced", &PublishOptions::default()).await?;
//! let sent = publisher.published();
//! let harness = StreamTestHarness::new("orders");
//! let msg = StreamTestHarness::build_raw("OrderPlaced", sent[0].body.clone(), &PublishOptions::default())?;
//! let outcome = harness.deliver(&handler, &msg).await; // Ok, Err(Skip) or Err(Block), as the handler returned
//! # let _ = outcome; Ok(()) }
//! ```

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::consumer::worker;
use crate::consumer::{
    BatchHandler, CancellationToken, HandleError, HandleResult, MessageHandler, PartitionContext,
    TypedBatchHandler, TypedHandler,
};
use crate::error::{Error, Result};
use crate::message::{Message, PayloadCodec};
use crate::positions::PositionStore;
use crate::producer::{BufferedPublisher, PublishOptions, Publisher};
use crate::wire::router::{self, RoundRobin};
use crate::wire::{HeaderBlock, StreamId, StreamMsg, TypedMsg};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A message recorded by [`InMemoryPublisher`] or [`InMemoryBufferedPublisher`].
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedMessage {
    /// The synthetic id this double assigned: unique and increasing per publisher, from `1-0`.
    pub id: StreamId,
    /// The routing key; empty when the caller round-robined.
    pub partition_key: String,
    /// A copy of the published bytes.
    pub body: Bytes,
    /// The message type string.
    pub msg_type: String,
    /// The correlation id from the options, if any.
    pub correlation_id: Option<String>,
    /// The headers from the options, in the order given.
    pub headers: Vec<(String, String)>,
    /// The partition the message **routed to**.
    pub partition: u32,
    /// The explicit partition the caller asked for, or `None` when routing was left to the key.
    pub requested_partition: Option<u32>,
}

#[derive(Debug, Default)]
struct PublisherState {
    messages: Vec<PublishedMessage>,
    last_ms: u64,
}

/// An in-memory [`Publisher`] that records every message and routes it the way the live publisher
/// does.
///
/// Routing: an explicit partition wins and is range-checked; a non-empty key hashes with the wire
/// router; an empty key round-robins. Ids are per instance. Batches share one partition and are
/// recorded under one lock, so they stay contiguous and in order.
#[derive(Debug)]
pub struct InMemoryPublisher {
    partitions: u32,
    round_robin: RoundRobin,
    state: Mutex<PublisherState>,
}

impl InMemoryPublisher {
    /// A publisher for a topic with `partitions` partitions.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when `partitions` is 0.
    pub fn new(partitions: u32) -> Result<Self> {
        if partitions == 0 {
            return Err(Error::invalid_argument(
                "an in-memory publisher needs at least one partition",
            ));
        }
        Ok(Self {
            partitions,
            round_robin: RoundRobin::new(),
            state: Mutex::new(PublisherState::default()),
        })
    }

    /// The partition count routing is computed against.
    #[must_use]
    pub const fn partitions(&self) -> u32 {
        self.partitions
    }

    /// A snapshot of the recorded messages, in publish order.
    #[must_use]
    pub fn published(&self) -> Vec<PublishedMessage> {
        lock(&self.state).messages.clone()
    }

    /// The number of recorded messages.
    #[must_use]
    pub fn count(&self) -> usize {
        lock(&self.state).messages.len()
    }

    /// A snapshot of the messages that routed to `partition`, in publish order.
    #[must_use]
    pub fn for_partition(&self, partition: u32) -> Vec<PublishedMessage> {
        lock(&self.state)
            .messages
            .iter()
            .filter(|m| m.partition == partition)
            .cloned()
            .collect()
    }

    /// The partition `partition_key` routes to, without publishing. An empty key round-robins, so
    /// this advances the cursor the same way an empty-key publish would.
    #[must_use]
    pub fn partition_for(&self, partition_key: &str) -> u32 {
        self.route_key(partition_key)
    }

    /// Drops the recorded messages. Ids keep increasing, so a cleared publisher never reissues one.
    pub fn clear(&self) {
        lock(&self.state).messages.clear();
    }

    fn route(&self, partition_key: &str, explicit: Option<u32>) -> Result<u32> {
        if let Some(chosen) = explicit {
            if chosen >= self.partitions {
                return Err(Error::invalid_argument(format!(
                    "this publisher has {} partition(s), so an explicit partition must be in [0, {}]",
                    self.partitions,
                    self.partitions - 1
                )));
            }
            return Ok(chosen);
        }
        Ok(self.route_key(partition_key))
    }

    fn route_key(&self, partition_key: &str) -> u32 {
        if partition_key.is_empty() {
            self.round_robin.next(self.partitions)
        } else {
            router::partition_for_key(partition_key.as_bytes(), self.partitions)
        }
    }

    /// Validates and routes once, then records every body on that partition under one lock.
    fn record(
        &self,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<Vec<StreamId>> {
        HeaderBlock::pack(options.headers.as_slice())?;
        let partition = self.route(partition_key, options.partition)?;
        let mut state = lock(&self.state);
        let mut ids = Vec::with_capacity(bodies.len());
        for body in bodies {
            state.last_ms += 1;
            let id = StreamId::new(state.last_ms, 0);
            state.messages.push(PublishedMessage {
                id,
                partition_key: partition_key.to_owned(),
                body: body.clone(),
                msg_type: msg_type.to_owned(),
                correlation_id: options.correlation_id.clone(),
                headers: options.headers.clone(),
                partition,
                requested_partition: options.partition,
            });
            ids.push(id);
        }
        Ok(ids)
    }
}

impl Publisher for InMemoryPublisher {
    fn publish(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<StreamId>> + Send {
        let recorded = self
            .record(
                partition_key,
                std::slice::from_ref(&body),
                msg_type,
                options,
            )
            .map(|ids| ids.first().copied().unwrap_or_default());
        std::future::ready(recorded)
    }

    fn publish_batch(
        &self,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        let recorded = self
            .record(partition_key, bodies, msg_type, options)
            .map(|_| ());
        std::future::ready(recorded)
    }
}

/// A message waiting in [`InMemoryBufferedPublisher`]'s buffer: enqueued, not yet published.
#[derive(Debug, Clone, PartialEq)]
pub struct PendingMessage {
    /// The routing key.
    pub partition_key: String,
    /// The enqueued bytes.
    pub body: Bytes,
    /// The message type string.
    pub msg_type: String,
    /// The options the message was enqueued with.
    pub options: PublishOptions,
}

#[derive(Debug, Default)]
struct BufferState {
    pending: Vec<PendingMessage>,
    fail_next_flush: Option<Error>,
}

/// An in-memory [`BufferedPublisher`]. Direct publishes go
/// straight to the recording, bypassing the buffer. Enqueued messages become visible only on
/// [`flush`](BufferedPublisher::flush), so a test that forgets to flush fails here as it would lose
/// messages in production.
///
/// There is no background pump and no bounded queue: nothing flushes on its own and enqueue never
/// waits. Use [`arm_flush_failure`](Self::arm_flush_failure) to rehearse a failed flush.
#[derive(Debug)]
pub struct InMemoryBufferedPublisher {
    inner: InMemoryPublisher,
    state: Mutex<BufferState>,
}

impl InMemoryBufferedPublisher {
    /// A buffered publisher for a topic with `partitions` partitions.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when `partitions` is 0.
    pub fn new(partitions: u32) -> Result<Self> {
        Ok(Self {
            inner: InMemoryPublisher::new(partitions)?,
            state: Mutex::new(BufferState::default()),
        })
    }

    /// The partition count routing is computed against.
    #[must_use]
    pub const fn partitions(&self) -> u32 {
        self.inner.partitions()
    }

    /// A snapshot of everything that has reached the stream: direct publishes and flushed messages.
    #[must_use]
    pub fn published(&self) -> Vec<PublishedMessage> {
        self.inner.published()
    }

    /// The number of published messages.
    #[must_use]
    pub fn count(&self) -> usize {
        self.inner.count()
    }

    /// A snapshot of the messages in the buffer. In production these are what a crash would lose.
    #[must_use]
    pub fn pending(&self) -> Vec<PendingMessage> {
        lock(&self.state).pending.clone()
    }

    /// The number of messages waiting in the buffer.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        lock(&self.state).pending.len()
    }

    /// The published messages that routed to `partition`, in publish order.
    #[must_use]
    pub fn for_partition(&self, partition: u32) -> Vec<PublishedMessage> {
        self.inner.for_partition(partition)
    }

    /// The partition `partition_key` routes to; see [`InMemoryPublisher::partition_for`].
    #[must_use]
    pub fn partition_for(&self, partition_key: &str) -> u32 {
        self.inner.partition_for(partition_key)
    }

    /// Makes the next [`flush`](BufferedPublisher::flush) fail with `error`, leaving the buffer
    /// intact. One-shot: the flush after it succeeds, so a retrying caller recovers the messages.
    pub fn arm_flush_failure(&self, error: Error) {
        lock(&self.state).fail_next_flush = Some(error);
    }

    /// Cancels an armed flush failure.
    pub fn disarm_flush_failure(&self) {
        lock(&self.state).fail_next_flush = None;
    }

    /// Drops the recorded messages and the buffer.
    pub fn clear(&self) {
        lock(&self.state).pending.clear();
        self.inner.clear();
    }

    /// Takes the whole buffer under the lock, or fails with the armed error and leaves it in place.
    fn take_batch(&self) -> Result<Vec<PendingMessage>> {
        let mut state = lock(&self.state);
        if let Some(error) = state.fail_next_flush.take() {
            return Err(error);
        }
        Ok(std::mem::take(&mut state.pending))
    }
}

impl Publisher for InMemoryBufferedPublisher {
    async fn publish(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<StreamId> {
        Publisher::publish(&self.inner, partition_key, body, msg_type, options).await
    }

    async fn publish_batch(
        &self,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<()> {
        Publisher::publish_batch(&self.inner, partition_key, bodies, msg_type, options).await
    }
}

impl BufferedPublisher for InMemoryBufferedPublisher {
    fn enqueue(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<()>> + Send {
        lock(&self.state).pending.push(PendingMessage {
            partition_key: partition_key.to_owned(),
            body,
            msg_type: msg_type.to_owned(),
            options: options.clone(),
        });
        std::future::ready(Ok(()))
    }

    async fn flush(&self) -> Result<()> {
        for item in self.take_batch()? {
            Publisher::publish(
                &self.inner,
                &item.partition_key,
                item.body,
                &item.msg_type,
                &item.options,
            )
            .await?;
        }
        Ok(())
    }
}

/// An in-memory [`PositionStore`] with the real load / save / reset semantics, held in a map
/// instead of a Redis hash. One instance serves every topic and consumer
/// that shares it. Reset of a single partition creates it; reset of all partitions touches only
/// the positions already stored.
/// Positions by topic, then consumer, then partition.
type Positions = BTreeMap<String, BTreeMap<String, BTreeMap<u32, StreamId>>>;

/// An in-memory position store: real load, save and reset semantics, held in a map. Positions are per topic, consumer and partition.
#[derive(Debug, Default)]
pub struct MemoryPositionStore {
    topics: Mutex<Positions>,
}

impl MemoryPositionStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PositionStore for MemoryPositionStore {
    fn load(
        &self,
        topic: &str,
        consumer: &str,
    ) -> impl Future<Output = Result<BTreeMap<u32, StreamId>>> + Send {
        let loaded = lock(&self.topics)
            .get(topic)
            .and_then(|consumers| consumers.get(consumer))
            .cloned()
            .unwrap_or_default();
        std::future::ready(Ok(loaded))
    }

    fn save(
        &self,
        topic: &str,
        consumer: &str,
        positions: &[(u32, StreamId)],
    ) -> impl Future<Output = Result<()>> + Send {
        let mut topics = lock(&self.topics);
        let stored = topics
            .entry(topic.to_owned())
            .or_default()
            .entry(consumer.to_owned())
            .or_default();
        for (partition, id) in positions {
            stored.insert(*partition, *id);
        }
        std::future::ready(Ok(()))
    }

    fn reset(
        &self,
        topic: &str,
        consumer: &str,
        to: StreamId,
        partition: Option<u32>,
    ) -> impl Future<Output = Result<()>> + Send {
        let mut topics = lock(&self.topics);
        if let Some(partition) = partition {
            topics
                .entry(topic.to_owned())
                .or_default()
                .entry(consumer.to_owned())
                .or_default()
                .insert(partition, to);
        } else if let Some(stored) = topics
            .get_mut(topic)
            .and_then(|consumers| consumers.get_mut(consumer))
        {
            for id in stored.values_mut() {
                *id = to;
            }
        }
        std::future::ready(Ok(()))
    }
}

/// Process-wide synthetic id sequence for harness messages.
static NEXT_MS: AtomicU64 = AtomicU64::new(0);

fn next_id() -> StreamId {
    StreamId::new(NEXT_MS.fetch_add(1, Ordering::Relaxed) + 1, 0)
}

/// Runs a handler future the way the worker does: a panic becomes `Skip` (C2 `catch_unwind`).
async fn guarded<F: Future<Output = HandleResult>>(future: F) -> HandleResult {
    worker::catch_unwind(future)
        .await
        .unwrap_or_else(|panic| Err(worker::panic_to_skip(panic.as_ref())))
}

/// Drives handlers directly, without a consumer host or Redis.
///
/// Each delivery builds the [`StreamMsg`] and [`PartitionContext`] the worker would pass, runs the
/// handler once, and returns its [`HandleResult`] unchanged, so a test asserts `Ok`, `Skip` or
/// `Block` directly (`result.is_err_and(HandleError::is_block)`). A panic in the handler returns
/// `Err(Skip)`, as in production. The harness does not apply `on_error` or retry on `Block`; those
/// belong to the worker and are tested there.
///
/// Messages built here have an empty partition key, empty correlation id unless one is set, no
/// trace parent, and the partition from [`PublishOptions::partition`] (or 0).
#[derive(Debug, Clone)]
pub struct StreamTestHarness {
    topic: Arc<str>,
    consumer: Arc<str>,
    cancellation: CancellationToken,
}

impl StreamTestHarness {
    /// A harness for `topic`, consumer `test-consumer`, with a fresh cancellation token.
    #[must_use]
    pub fn new(topic: impl Into<Arc<str>>) -> Self {
        Self {
            topic: topic.into(),
            consumer: Arc::from("test-consumer"),
            cancellation: CancellationToken::new(),
        }
    }

    /// Sets the consumer name handed to handlers.
    #[must_use]
    pub fn with_consumer(mut self, consumer: impl Into<Arc<str>>) -> Self {
        self.consumer = consumer.into();
        self
    }

    /// Sets the cancellation token handed to handlers, so a test can cancel mid-handler.
    #[must_use]
    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation = token;
        self
    }

    /// The context a handler receives for `partition`.
    #[must_use]
    pub fn context(&self, partition: u32) -> PartitionContext {
        PartitionContext::new(
            Arc::clone(&self.topic),
            Arc::clone(&self.consumer),
            partition,
            self.cancellation.clone(),
        )
    }

    /// Wraps a raw body as the message a handler would receive, without delivering it.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidArgument`] when the headers exceed the wire limits, as the publisher refuses
    /// them.
    pub fn build_raw(msg_type: &str, body: Bytes, options: &PublishOptions) -> Result<StreamMsg> {
        let headers = HeaderBlock::pack(options.headers.as_slice())?;
        Ok(
            StreamMsg::new(next_id(), options.partition.unwrap_or(0), msg_type, body)
                .with_correlation_id(options.correlation_id.clone().unwrap_or_default())
                .with_headers(headers),
        )
    }

    /// Encodes `message` with codec `C` under `M::TYPE` and wraps it as a handler would receive it.
    ///
    /// # Errors
    ///
    /// [`Error::Codec`] when encoding fails, or as [`build_raw`](Self::build_raw).
    pub fn build<C: PayloadCodec, M: Message + Serialize>(
        message: &M,
        options: &PublishOptions,
    ) -> Result<StreamMsg> {
        let body = C::encode(message)?;
        Self::build_raw(M::TYPE, body, options)
    }

    /// Delivers one message to a [`MessageHandler`] and returns its outcome.
    ///
    /// # Errors
    ///
    /// Returns the handler's `Err` unchanged; see the type docs.
    pub async fn deliver<H: MessageHandler>(&self, handler: &H, msg: &StreamMsg) -> HandleResult {
        let ctx = self.context(msg.partition());
        guarded(handler.handle(msg, &ctx)).await
    }

    /// Delivers one call with every message, in order, to a [`BatchHandler`]. The partition comes
    /// from the first message. An empty batch still calls the handler, as the pipeline does.
    ///
    /// # Errors
    ///
    /// Returns the handler's `Err` unchanged; see the type docs.
    pub async fn deliver_batch<H: BatchHandler>(
        &self,
        handler: &H,
        batch: &[StreamMsg],
    ) -> HandleResult {
        let ctx = self.context(batch.first().map_or(0, StreamMsg::partition));
        guarded(handler.handle_batch(batch, &ctx)).await
    }

    /// Encodes `message` with `C`, decodes it back into a [`TypedMsg`] as the typed adapter does,
    /// and delivers it to a [`TypedHandler`]. A round-trip failure is `Err(Skip)`.
    ///
    /// # Errors
    ///
    /// Returns the handler's `Err` unchanged; see the type docs.
    pub async fn deliver_typed<C, M, H>(
        &self,
        handler: &H,
        message: &M,
        options: &PublishOptions,
    ) -> HandleResult
    where
        C: PayloadCodec,
        M: Message + Serialize + DeserializeOwned + Send + Sync,
        H: TypedHandler<M>,
    {
        let envelope = Self::build::<C, M>(message, options).map_err(HandleError::skip)?;
        let value = C::decode::<M>(envelope.body()).map_err(HandleError::skip)?;
        let ctx = self.context(envelope.partition());
        let typed = TypedMsg { value, envelope };
        guarded(handler.handle(&typed, &ctx)).await
    }

    /// Encodes `messages` with `C` and delivers them as one call to a [`TypedBatchHandler`]. A
    /// message that fails to round-trip is dropped and the rest are delivered. An empty result is
    /// never delivered, as with the typed adapter.
    ///
    /// # Errors
    ///
    /// Returns the handler's `Err` unchanged; see the type docs.
    pub async fn deliver_typed_batch<C, M, H>(
        &self,
        handler: &H,
        messages: &[M],
        options: &PublishOptions,
    ) -> HandleResult
    where
        C: PayloadCodec,
        M: Message + Serialize + DeserializeOwned + Send + Sync,
        H: TypedBatchHandler<M>,
    {
        let mut typed = Vec::with_capacity(messages.len());
        for message in messages {
            let envelope = Self::build::<C, M>(message, options).map_err(HandleError::skip)?;
            match C::decode::<M>(envelope.body()) {
                Ok(value) => typed.push(TypedMsg { value, envelope }),
                Err(error) => {
                    tracing::warn!(%error, "dropping a test message that failed to decode");
                }
            }
        }
        let Some(first) = typed.first() else {
            return Ok(());
        };
        let ctx = self.context(first.envelope.partition());
        guarded(handler.handle_batch(&typed, &ctx)).await
    }
}

/// Tests for the test doubles. Cases that do not apply, with reasons:
/// `body_is_copied` and `enqueue_copies_the_body` (`Bytes` is immutable and reference counted, so a
/// caller cannot rewrite a buffer it has handed over and there is nothing to copy against) and
/// `harness_sends_to_a_delegate` (closure handlers are C4b's shape; `MessageHandler` covers the
/// single-message path, as `harness_delivers_to_a_message_handler` tests).
#[cfg(test)]
mod tests {
    // Test handlers are `async fn` without awaits, matching the trait shape.
    #![allow(clippy::unused_async_trait_impl)]

    use std::collections::HashSet;
    use std::sync::atomic::AtomicBool;

    use serde::Deserialize;

    use super::*;
    use crate::consumer::TypedBatchHandler;
    use crate::message::Json;
    use crate::wire::router::partition_for_key;

    fn body(text: &str) -> Bytes {
        Bytes::copy_from_slice(text.as_bytes())
    }

    fn text(bytes: &Bytes) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn opts() -> PublishOptions {
        PublishOptions::default()
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Ping {
        text: String,
    }

    impl Message for Ping {
        const TYPE: &'static str = "Ping";
    }

    fn ping(text: &str) -> Ping {
        Ping {
            text: text.to_owned(),
        }
    }

    // InMemoryStreamPublisher: routing

    #[tokio::test]
    async fn routes_by_key_rather_than_recording_the_requested_partition() {
        let publisher = InMemoryPublisher::new(8).unwrap();

        publisher
            .publish("customer-42", body("body"), "T", &opts())
            .await
            .unwrap();
        let recorded = publisher.published()[0].clone();
        assert_eq!(recorded.requested_partition, None);
        assert!(recorded.partition < 8);

        publisher
            .publish("customer-42", body("body"), "T", &opts())
            .await
            .unwrap();
        assert_eq!(publisher.published()[1].partition, recorded.partition);
    }

    #[tokio::test]
    async fn routing_agrees_with_the_wire_router_for_the_same_key() {
        let partitions = 8;
        let publisher = InMemoryPublisher::new(partitions).unwrap();

        for i in 0..64 {
            publisher
                .publish(&format!("key-{i}"), body("T"), "T", &opts())
                .await
                .unwrap();
        }

        let mut used = HashSet::new();
        for m in publisher.published() {
            assert_eq!(
                m.partition,
                partition_for_key(m.partition_key.as_bytes(), partitions)
            );
            used.insert(m.partition);
        }
        assert!(
            used.len() > 1,
            "a real hash spreads 64 keys across 8 partitions"
        );
    }

    #[tokio::test]
    async fn explicit_partition_wins_and_is_range_checked() {
        let publisher = InMemoryPublisher::new(4).unwrap();

        for p in 0..4 {
            publisher
                .publish("customer-42", body("body"), "T", &opts().partition(p))
                .await
                .unwrap();
        }

        let messages = publisher.published();
        assert_eq!(
            messages.iter().map(|m| m.partition).collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
        assert_eq!(
            messages
                .iter()
                .map(|m| m.requested_partition)
                .collect::<Vec<_>>(),
            [Some(0), Some(1), Some(2), Some(3)]
        );

        let out_of_range = publisher
            .publish("k", body("body"), "T", &opts().partition(4))
            .await;
        assert!(matches!(out_of_range, Err(Error::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn empty_key_round_robins() {
        let publisher = InMemoryPublisher::new(4).unwrap();

        for _ in 0..8 {
            publisher
                .publish("", body("body"), "T", &opts())
                .await
                .unwrap();
        }

        let distinct: HashSet<u32> = publisher.published().iter().map(|m| m.partition).collect();
        assert_eq!(distinct.len(), 4);
    }

    #[tokio::test]
    async fn single_partition_topic_routes_everything_to_zero() {
        let publisher = InMemoryPublisher::new(1).unwrap();

        publisher
            .publish("a", body("body"), "T", &opts())
            .await
            .unwrap();
        publisher
            .publish("", body("body"), "T", &opts())
            .await
            .unwrap();

        assert!(publisher.published().iter().all(|m| m.partition == 0));
    }

    #[tokio::test]
    async fn batch_lands_on_one_partition_and_keeps_order() {
        let publisher = InMemoryPublisher::new(8).unwrap();
        let bodies = [body("1"), body("2"), body("3")];

        publisher
            .publish_batch("customer-42", &bodies, "T", &opts())
            .await
            .unwrap();

        let messages = publisher.published();
        assert_eq!(messages.len(), 3);
        assert_eq!(
            messages
                .iter()
                .map(|m| m.partition)
                .collect::<HashSet<_>>()
                .len(),
            1
        );
        assert_eq!(
            messages.iter().map(|m| text(&m.body)).collect::<Vec<_>>(),
            ["1", "2", "3"]
        );
    }

    // InMemoryStreamPublisher: ids, thread safety

    #[tokio::test]
    async fn ids_are_per_instance_and_increasing() {
        let a = InMemoryPublisher::new(1).unwrap();
        let b = InMemoryPublisher::new(1).unwrap();

        a.publish("k", body("body"), "T", &opts()).await.unwrap();
        a.publish("k", body("body"), "T", &opts()).await.unwrap();
        b.publish("k", body("body"), "T", &opts()).await.unwrap();

        assert_eq!(
            a.published().iter().map(|m| m.id.ms).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            b.published().iter().map(|m| m.id.ms).collect::<Vec<_>>(),
            [1]
        );
        assert!(a.published()[1].id > a.published()[0].id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_publishes_are_all_recorded_and_snapshots_never_tear() {
        let publisher = Arc::new(InMemoryPublisher::new(4).unwrap());
        let stop = Arc::new(AtomicBool::new(false));

        let reader = {
            let publisher = Arc::clone(&publisher);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let total: usize = publisher.published().iter().map(|m| m.body.len()).sum();
                    std::hint::black_box(total);
                }
            })
        };

        let writers: Vec<_> = (0..8)
            .map(|w| {
                let publisher = Arc::clone(&publisher);
                tokio::spawn(async move {
                    for _ in 0..250 {
                        publisher
                            .publish(
                                &format!("w{w}"),
                                body("body"),
                                "T",
                                &PublishOptions::default(),
                            )
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.await.unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        reader.join().unwrap();

        assert_eq!(publisher.count(), 2000);
        let ids: HashSet<StreamId> = publisher.published().iter().map(|m| m.id).collect();
        assert_eq!(ids.len(), 2000);
    }

    #[tokio::test]
    async fn clear_keeps_ids_moving_forward() {
        let publisher = InMemoryPublisher::new(1).unwrap();

        publisher
            .publish("k", body("body"), "T", &opts())
            .await
            .unwrap();
        let before = publisher.published()[0].id;

        publisher.clear();
        assert_eq!(publisher.count(), 0);

        publisher
            .publish("k", body("body"), "T", &opts())
            .await
            .unwrap();
        assert!(publisher.published()[0].id > before);
    }

    #[tokio::test]
    async fn for_partition_filters() {
        let publisher = InMemoryPublisher::new(4).unwrap();

        publisher
            .publish("k", body("body"), "T", &opts().partition(2))
            .await
            .unwrap();
        publisher
            .publish("k", body("body"), "T", &opts().partition(3))
            .await
            .unwrap();

        assert_eq!(publisher.for_partition(2).len(), 1);
        assert_eq!(publisher.for_partition(1).len(), 0);
    }

    #[test]
    fn zero_partitions_is_refused() {
        assert!(matches!(
            InMemoryPublisher::new(0),
            Err(Error::InvalidArgument(_))
        ));
    }

    // InMemoryBufferedStreamPublisher

    #[tokio::test]
    async fn enqueued_messages_are_not_published_until_flush() {
        let publisher = InMemoryBufferedPublisher::new(4).unwrap();

        publisher
            .enqueue("k", body("body"), "T", &opts())
            .await
            .unwrap();

        assert_eq!(publisher.published().len(), 0);
        assert_eq!(publisher.pending_count(), 1);
        assert_eq!(publisher.pending()[0].msg_type, "T");

        publisher.flush().await.unwrap();

        assert_eq!(publisher.pending_count(), 0);
        assert_eq!(publisher.published().len(), 1);
    }

    #[tokio::test]
    async fn flush_preserves_order_and_routing() {
        let publisher = InMemoryBufferedPublisher::new(8).unwrap();

        publisher
            .enqueue("customer-42", body("1"), "T", &opts())
            .await
            .unwrap();
        publisher
            .enqueue("customer-42", body("2"), "T", &opts())
            .await
            .unwrap();
        publisher.flush().await.unwrap();

        let messages = publisher.published();
        assert_eq!(
            messages.iter().map(|m| text(&m.body)).collect::<Vec<_>>(),
            ["1", "2"]
        );
        assert!(
            messages
                .iter()
                .all(|m| m.partition == publisher.partition_for("customer-42"))
        );
    }

    #[tokio::test]
    async fn direct_publish_bypasses_the_buffer() {
        let publisher = InMemoryBufferedPublisher::new(1).unwrap();

        publisher
            .enqueue("k", body("buffered"), "T", &opts())
            .await
            .unwrap();
        publisher
            .publish("k", body("direct"), "T", &opts())
            .await
            .unwrap();

        let messages = publisher.published();
        assert_eq!(messages.len(), 1);
        assert_eq!(text(&messages[0].body), "direct");
        assert_eq!(publisher.pending_count(), 1);
    }

    #[tokio::test]
    async fn armed_flush_failure_faults_and_keeps_the_batch() {
        let publisher = InMemoryBufferedPublisher::new(1).unwrap();
        publisher.arm_flush_failure(Error::transport("redis is away"));

        publisher
            .enqueue("k", body("body"), "T", &opts())
            .await
            .unwrap();

        let failed = publisher.flush().await;
        assert!(matches!(failed, Err(Error::Transport { .. })));
        assert_eq!(publisher.pending_count(), 1);
        assert_eq!(publisher.published().len(), 0);

        // One-shot: the retry gets through.
        publisher.flush().await.unwrap();
        assert_eq!(publisher.published().len(), 1);
    }

    #[tokio::test]
    async fn flushing_an_empty_buffer_is_a_no_op() {
        let publisher = InMemoryBufferedPublisher::new(1).unwrap();
        publisher.flush().await.unwrap();
        assert_eq!(publisher.published().len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_enqueue_and_flush_publish_each_message_once() {
        let publisher = Arc::new(InMemoryBufferedPublisher::new(4).unwrap());

        let writers: Vec<_> = (0..6)
            .map(|w| {
                let publisher = Arc::clone(&publisher);
                tokio::spawn(async move {
                    for i in 0..200 {
                        publisher
                            .enqueue(
                                &format!("w{w}"),
                                body(&format!("{w}:{i}")),
                                "T",
                                &PublishOptions::default(),
                            )
                            .await
                            .unwrap();
                        if i % 25 == 0 {
                            publisher.flush().await.unwrap();
                        }
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.await.unwrap();
        }
        publisher.flush().await.unwrap();

        assert_eq!(publisher.pending_count(), 0);
        assert_eq!(publisher.count(), 1200);
        let bodies: HashSet<String> = publisher
            .published()
            .iter()
            .map(|m| text(&m.body))
            .collect();
        assert_eq!(bodies.len(), 1200);
    }

    #[test]
    fn is_a_buffered_publisher() {
        fn assert_buffered<B: BufferedPublisher + Publisher>(_: &B) {}
        assert_buffered(&InMemoryBufferedPublisher::new(1).unwrap());
    }

    // StreamTestHarness

    #[derive(Default)]
    struct RecordingMessageHandler {
        seen: Mutex<Vec<(String, Bytes)>>,
    }

    impl MessageHandler for RecordingMessageHandler {
        async fn handle(&self, msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
            lock(&self.seen).push((msg.msg_type().to_owned(), msg.body().clone()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingBatchHandler {
        calls: Mutex<usize>,
        texts: Mutex<Vec<String>>,
        ids: Mutex<Vec<StreamId>>,
    }

    impl BatchHandler for RecordingBatchHandler {
        async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
            *lock(&self.calls) += 1;
            for msg in batch {
                lock(&self.ids).push(msg.id());
                lock(&self.texts).push(Json::decode::<Ping>(msg.body()).unwrap().text);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn harness_delivers_to_a_message_handler() {
        let handler = RecordingMessageHandler::default();
        let harness = StreamTestHarness::new("orders");
        let msg = StreamTestHarness::build::<Json, Ping>(&ping("a"), &opts()).unwrap();

        assert!(harness.deliver(&handler, &msg).await.is_ok());

        let seen = lock(&handler.seen);
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, Ping::TYPE);
        assert_eq!(Json::decode::<Ping>(&seen[0].1).unwrap(), ping("a"));
    }

    #[tokio::test]
    async fn harness_delivers_a_batch_to_a_batch_handler() {
        let handler = RecordingBatchHandler::default();
        let harness = StreamTestHarness::new("orders");
        let batch: Vec<StreamMsg> = ["a", "b", "c"]
            .iter()
            .map(|t| StreamTestHarness::build::<Json, Ping>(&ping(t), &opts()).unwrap())
            .collect();

        assert!(harness.deliver_batch(&handler, &batch).await.is_ok());

        assert_eq!(*lock(&handler.calls), 1);
        assert_eq!(*lock(&handler.texts), ["a", "b", "c"]);
        let ids = lock(&handler.ids).clone();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test]
    async fn harness_sends_an_empty_batch() {
        let handler = RecordingBatchHandler::default();

        assert!(
            StreamTestHarness::new("orders")
                .deliver_batch(&handler, &[])
                .await
                .is_ok()
        );

        assert_eq!(*lock(&handler.calls), 1);
        assert_eq!(lock(&handler.texts).len(), 0);
    }

    #[test]
    fn harness_stamps_options_onto_the_message() {
        let options = PublishOptions::default()
            .correlation_id("corr-1")
            .header("tenant", "au")
            .partition(3);

        let msg = StreamTestHarness::build::<Json, Ping>(&ping("a"), &options).unwrap();

        assert_eq!(msg.partition(), 3);
        assert_eq!(msg.correlation_id(), "corr-1");
        assert_eq!(msg.headers().get("tenant").unwrap().as_deref(), Some("au"));
    }

    #[test]
    fn harness_ids_are_unique_under_concurrency() {
        let threads: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..200)
                        .map(|_| {
                            StreamTestHarness::build_raw(
                                "T",
                                Bytes::new(),
                                &PublishOptions::default(),
                            )
                            .unwrap()
                            .id()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let ids: HashSet<StreamId> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        assert_eq!(ids.len(), 8 * 200);
    }

    #[tokio::test]
    async fn harness_returns_the_handler_outcome_unchanged() {
        struct Blocks;
        impl MessageHandler for Blocks {
            async fn handle(&self, _msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
                Err(HandleError::block("database is down"))
            }
        }
        let msg = StreamTestHarness::build::<Json, Ping>(&ping("a"), &opts()).unwrap();

        let outcome = StreamTestHarness::new("orders")
            .deliver(&Blocks, &msg)
            .await;

        assert!(outcome.is_err_and(|e| e.is_block()));
    }

    #[tokio::test]
    async fn harness_turns_a_panic_into_a_skip() {
        struct Panics;
        impl MessageHandler for Panics {
            async fn handle(&self, _msg: &StreamMsg, _ctx: &PartitionContext) -> HandleResult {
                panic!("boom")
            }
        }
        let msg = StreamTestHarness::build::<Json, Ping>(&ping("a"), &opts()).unwrap();

        let outcome = StreamTestHarness::new("orders")
            .deliver(&Panics, &msg)
            .await;

        assert!(matches!(outcome, Err(HandleError::Skip(_))));
    }

    #[tokio::test]
    async fn harness_delivers_typed_and_typed_batches() {
        #[derive(Default)]
        struct Typed {
            seen: Mutex<Vec<String>>,
        }
        impl TypedHandler<Ping> for Typed {
            async fn handle(&self, msg: &TypedMsg<Ping>, ctx: &PartitionContext) -> HandleResult {
                assert_eq!(ctx.topic(), "orders");
                lock(&self.seen).push(msg.value.text.clone());
                Ok(())
            }
        }
        impl TypedBatchHandler<Ping> for Typed {
            async fn handle_batch(
                &self,
                batch: &[TypedMsg<Ping>],
                _ctx: &PartitionContext,
            ) -> HandleResult {
                lock(&self.seen).extend(batch.iter().map(|m| m.value.text.clone()));
                Ok(())
            }
        }

        let harness = StreamTestHarness::new("orders");
        let handler = Typed::default();

        assert!(
            harness
                .deliver_typed::<Json, Ping, _>(&handler, &ping("one"), &opts())
                .await
                .is_ok()
        );
        assert!(
            harness
                .deliver_typed_batch::<Json, Ping, _>(
                    &handler,
                    &[ping("two"), ping("three")],
                    &opts()
                )
                .await
                .is_ok()
        );
        assert!(
            harness
                .deliver_typed_batch::<Json, Ping, _>(&handler, &[], &opts())
                .await
                .is_ok()
        );

        assert_eq!(*lock(&handler.seen), ["one", "two", "three"]);
    }

    #[tokio::test]
    async fn memory_position_store_loads_saves_and_resets() {
        let store = MemoryPositionStore::new();
        assert_eq!(store.load("orders", "billing").await.unwrap().len(), 0);

        store
            .save(
                "orders",
                "billing",
                &[(0, StreamId::new(5, 0)), (1, StreamId::new(7, 0))],
            )
            .await
            .unwrap();
        let loaded = store.load("orders", "billing").await.unwrap();
        assert_eq!(loaded.get(&1), Some(&StreamId::new(7, 0)));
        assert_eq!(store.load("orders", "other").await.unwrap().len(), 0);

        store
            .reset("orders", "billing", StreamId::new(1, 0), Some(0))
            .await
            .unwrap();
        store
            .reset("orders", "billing", StreamId::new(2, 0), None)
            .await
            .unwrap();
        let loaded = store.load("orders", "billing").await.unwrap();
        assert_eq!(
            loaded.values().copied().collect::<Vec<_>>(),
            [StreamId::new(2, 0); 2]
        );
    }
}
