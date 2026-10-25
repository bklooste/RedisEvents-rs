//! The registration surface over the host: every handler shape, closures and index/sole-entry
//! selection from configuration. **Owner: C4b** (port of `StreamsBuilderExtensions.cs` and
//! `TypedConsumeExtensions.cs`), over the C4 skeleton in `host.rs`.
//!
//! Every method here is an adapter onto [`ConsumerBuilder::handler`]; this file never touches the
//! host's internals.
//!
//! | C# overload | Rust |
//! | --- | --- |
//! | `AddStream<THandler>(topic)` with a `IBatchHandler` / `IMessageHandler` | [`ConsumerBuilder::handler`] / [`ConsumerBuilder::message_handler`] (DI-resolved handler: not ported, pass the instance) |
//! | `AddStream<THandler, TMessage>(topic, JsonTypeInfo<TMessage>)` | [`ConsumerBuilder::typed_handler`] / [`ConsumerBuilder::typed_batch_handler`] (JSON, the default codec) |
//! | `AddStream<THandler, TMessage>(topic, Func<ReadOnlyMemory<byte>, TMessage?>)` | the `_with::<C, _, _>` forms: the codec is a type parameter, so a custom decoder is a [`PayloadCodec`] |
//! | `AddStream<THandler, TMessage>(topic, MessagePackSerializerOptions)` | `typed_handler_with::<MessagePack, _, _>` (`redisevents-msgpack`) |
//! | `AddStream(topic, Func<ReadOnlyMemory<StreamMsg>, CancellationToken, ValueTask>)` | [`ConsumerBuilder::batch_fn`] |
//! | `AddStream(index)` / `AddStream(configure)` | [`StreamsBuilder::consumer_at`] / [`StreamsBuilder::sole_consumer`]; `ConsumerBuilder::configure` |
//! | `AddStreamPublisher` | not ported: a DI registration. Publishers are built directly with `StreamPublisher::new` |
//!
//! ```no_run
//! # use redisevents::{Connection, StreamMsg, PartitionContext, HandleResult, BatchHandler};
//! # use redisevents::consumer::host::Streams;
//! struct Print;
//! impl BatchHandler for Print {
//!     async fn handle_batch(&self, batch: &[StreamMsg], _ctx: &PartitionContext) -> HandleResult {
//!         for m in batch { println!("{} {}", m.id(), m.msg_type()); }
//!         Ok(())
//!     }
//! }
//! # async fn run() -> redisevents::Result<()> {
//! let conn = Connection::connect("redis://localhost:6379").await?;
//! let host = Streams::builder(conn).topic("orders").handler(Print).spawn().await?;
//! // ... later
//! host.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! # Closure handlers
//!
//! [`ConsumerBuilder::batch_fn`] and [`ConsumerBuilder::message_fn`] take an async closure that
//! receives the messages **by value** (`Vec<StreamMsg>` / `StreamMsg`) and a [`PartitionContext`]
//! by value, and returns a [`HandleResult`]. The C# delegate borrows `ReadOnlyMemory<StreamMsg>`,
//! but a closure cannot return a `Send` future that borrows its arguments on stable Rust: the
//! only spelling (`F(..): Send`, return-type notation) is unstable (rust#109417), and boxing each
//! call would force every closure into `Box::pin(async move { .. })`. Owned arguments cost one
//! `Vec` of cloned messages per batch (the body is a cheap `Bytes` clone; the strings are small)
//! and keep the closure a plain `async |batch, ctx| { .. }`. A closure that only needs to look at
//! messages can still do so without cloning by implementing [`BatchHandler`] directly.

use std::future::Future;

use serde::de::DeserializeOwned;

use crate::config::ConsumerOptions;
use crate::consumer::handler::{
    BatchHandler, MessageHandler, PerMessage, Typed, TypedBatchHandler, TypedHandler,
};
use crate::consumer::host::{ConsumerBuilder, StreamsBuilder};
use crate::consumer::{HandleResult, PartitionContext};
use crate::error::{Error, Result};
use crate::message::{Json, PayloadCodec};
use crate::wire::StreamMsg;

/// A batch closure as a [`BatchHandler`] (see the module docs for why the arguments are owned).
struct BatchFn<F>(F);

impl<F, Fut> BatchHandler for BatchFn<F>
where
    F: Fn(Vec<StreamMsg>, PartitionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandleResult> + Send + 'static,
{
    async fn handle_batch(&self, batch: &[StreamMsg], ctx: &PartitionContext) -> HandleResult {
        (self.0)(batch.to_vec(), ctx.clone()).await
    }
}

/// A single-message closure as a [`MessageHandler`]; run per message through [`PerMessage`].
struct MessageFn<F>(F);

impl<F, Fut> MessageHandler for MessageFn<F>
where
    F: Fn(StreamMsg, PartitionContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandleResult> + Send + 'static,
{
    async fn handle(&self, msg: &StreamMsg, ctx: &PartitionContext) -> HandleResult {
        (self.0)(msg.clone(), ctx.clone()).await
    }
}

/// The configured consumer at `index` (C# `AddStream(index)`).
fn configured_at(consumers: &[ConsumerOptions], index: usize) -> Result<&ConsumerOptions> {
    consumers.get(index).ok_or_else(|| {
        Error::config(format!(
            "Streams: consumer index {index} is out of range: Streams:Consumers has {} entr{}.",
            consumers.len(),
            if consumers.len() == 1 { "y" } else { "ies" },
        ))
    })
}

/// The only configured consumer (C# `AddStream<THandler>()` without a topic).
fn configured_sole(consumers: &[ConsumerOptions]) -> Result<&ConsumerOptions> {
    match consumers {
        [only] => Ok(only),
        _ => Err(Error::config(format!(
            "Streams: taking the topic from the single Streams:Consumers entry needs exactly one entry, but {} are configured. Use the topic or index form.",
            consumers.len(),
        ))),
    }
}

impl StreamsBuilder {
    /// Starts the registration of the configured consumer at `index` in `Streams:Consumers`
    /// (C# `AddStream<THandler>(index)`). Its options are that entry, unchanged.
    ///
    /// # Errors
    /// [`Error::Config`] when `index` is out of range.
    pub fn consumer_at(self, index: usize) -> Result<ConsumerBuilder> {
        let options = configured_at(&self.stream_options().consumers, index)?.clone();
        Ok(self.topic(options.topic.clone()).options(options))
    }

    /// Starts the registration of the only configured consumer (C# `AddStream<THandler>()`).
    ///
    /// # Errors
    /// [`Error::Config`] unless `Streams:Consumers` has exactly one entry.
    pub fn sole_consumer(self) -> Result<ConsumerBuilder> {
        let options = configured_sole(&self.stream_options().consumers)?.clone();
        Ok(self.topic(options.topic.clone()).options(options))
    }
}

impl ConsumerBuilder {
    /// Completes the registration with a per-message handler; the host calls it once per message,
    /// in order (C# `IMessageHandler`). See [`PerMessage`] for how errors are handled.
    #[must_use]
    pub fn message_handler<H: MessageHandler>(self, handler: H) -> StreamsBuilder {
        self.handler(PerMessage(handler))
    }

    /// Completes the registration with a closure over batches (C# delegate `AddStream`). The
    /// closure receives the batch and the partition context by value; see the module docs.
    #[must_use]
    pub fn batch_fn<F, Fut>(self, f: F) -> StreamsBuilder
    where
        F: Fn(Vec<StreamMsg>, PartitionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandleResult> + Send + 'static,
    {
        self.handler(BatchFn(f))
    }

    /// Completes the registration with a closure over single messages, run per message through
    /// [`PerMessage`].
    #[must_use]
    pub fn message_fn<F, Fut>(self, f: F) -> StreamsBuilder
    where
        F: Fn(StreamMsg, PartitionContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandleResult> + Send + 'static,
    {
        self.message_handler(MessageFn(f))
    }

    /// Completes the registration with a typed per-message handler, JSON bodies (C#
    /// `AddStream<THandler, TMessage>(topic, JsonTypeInfo)`). A body that fails to decode is
    /// skipped (see [`Typed`]).
    #[must_use]
    pub fn typed_handler<T, H>(self, handler: H) -> StreamsBuilder
    where
        H: TypedHandler<T>,
        T: DeserializeOwned + Send + Sync + 'static,
    {
        self.typed_handler_with::<Json, T, H>(handler)
    }

    /// As [`ConsumerBuilder::typed_handler`] with codec `C`, e.g.
    /// `typed_handler_with::<MessagePack, Order, _>(h)` (C# `AddStream<THandler, TMessage>` with
    /// `MessagePackSerializerOptions`).
    #[must_use]
    pub fn typed_handler_with<C, T, H>(self, handler: H) -> StreamsBuilder
    where
        C: PayloadCodec,
        H: TypedHandler<T>,
        T: DeserializeOwned + Send + Sync + 'static,
    {
        self.message_handler(Typed::<H, T, C>::new(handler))
    }

    /// Completes the registration with a typed batch handler, JSON bodies (C# `IBatchHandler<T>`).
    #[must_use]
    pub fn typed_batch_handler<T, H>(self, handler: H) -> StreamsBuilder
    where
        H: TypedBatchHandler<T>,
        T: DeserializeOwned + Send + Sync + 'static,
    {
        self.typed_batch_handler_with::<Json, T, H>(handler)
    }

    /// As [`ConsumerBuilder::typed_batch_handler`] with codec `C`.
    #[must_use]
    pub fn typed_batch_handler_with<C, T, H>(self, handler: H) -> StreamsBuilder
    where
        C: PayloadCodec,
        H: TypedBatchHandler<T>,
        T: DeserializeOwned + Send + Sync + 'static,
    {
        self.handler(Typed::<H, T, C>::new(handler))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::consumer::{CancellationToken, HandleError};
    use crate::wire::StreamId;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: u32,
    }

    /// A codec other than JSON, to show the codec parameter is honoured: bodies are `X:` + JSON.
    struct Prefixed;

    impl PayloadCodec for Prefixed {
        const NAME: &'static str = "prefixed";

        fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, crate::message::CodecError> {
            let mut out = b"X:".to_vec();
            out.extend(
                serde_json::to_vec(value)
                    .map_err(|e| crate::message::CodecError::new(Self::NAME, e))?,
            );
            Ok(Bytes::from(out))
        }

        fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, crate::message::CodecError> {
            let json = body
                .strip_prefix(b"X:")
                .ok_or_else(|| crate::message::CodecError::new(Self::NAME, "missing prefix"))?;
            serde_json::from_slice(json).map_err(|e| crate::message::CodecError::new(Self::NAME, e))
        }
    }

    fn msg(seq: u64, body: &[u8]) -> StreamMsg {
        StreamMsg::new(
            StreamId::new(1, seq),
            0,
            "order",
            Bytes::copy_from_slice(body),
        )
    }

    fn ctx() -> PartitionContext {
        PartitionContext::new("orders", "c1", 0, CancellationToken::new())
    }

    fn assert_send<T: Send>(_: &T) {}

    #[derive(Default)]
    struct CountingTyped {
        seen: std::sync::Mutex<Vec<u32>>,
    }

    #[allow(clippy::unused_async_trait_impl)]
    impl TypedHandler<Order> for Arc<CountingTyped> {
        async fn handle(
            &self,
            msg: &crate::wire::TypedMsg<Order>,
            _ctx: &PartitionContext,
        ) -> HandleResult {
            self.seen.lock().unwrap().push(msg.value.id);
            Ok(())
        }
    }

    #[allow(clippy::unused_async_trait_impl)]
    impl TypedBatchHandler<Order> for Arc<CountingTyped> {
        async fn handle_batch(
            &self,
            batch: &[crate::wire::TypedMsg<Order>],
            _ctx: &PartitionContext,
        ) -> HandleResult {
            self.seen
                .lock()
                .unwrap()
                .extend(batch.iter().map(|m| m.value.id));
            Ok(())
        }
    }

    #[tokio::test]
    async fn message_fn_runs_once_per_message_in_order() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let adapter = PerMessage(MessageFn(move |_m: StreamMsg, _c: PartitionContext| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }));
        let batch = [msg(1, b"a"), msg(2, b"b"), msg(3, b"c")];
        adapter.handle_batch(&batch, &ctx()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn batch_fn_gets_the_whole_batch_and_its_error_propagates() {
        let adapter = BatchFn(|batch: Vec<StreamMsg>, _c: PartitionContext| async move {
            if batch.len() == 2 {
                Err(HandleError::block(std::io::Error::other("boom")))
            } else {
                Ok(())
            }
        });
        let batch = [msg(1, b"a"), msg(2, b"b")];
        let err = adapter.handle_batch(&batch, &ctx()).await.unwrap_err();
        assert!(err.is_block());
    }

    #[tokio::test]
    async fn typed_handler_decodes_with_json_and_skips_bad_bodies() {
        let state = Arc::new(CountingTyped::default());
        let adapter = PerMessage(Typed::<_, Order, Json>::new(state.clone()));
        let batch = [msg(1, br#"{"id":7}"#), msg(2, b"not json")];
        // The bad body is a Skip for the per-message adapter: the batch still completes.
        adapter.handle_batch(&batch, &ctx()).await.unwrap();
        assert_eq!(*state.seen.lock().unwrap(), vec![7]);
    }

    #[tokio::test]
    async fn typed_batch_handler_drops_undecodable_entries_and_keeps_the_rest() {
        let state = Arc::new(CountingTyped::default());
        let adapter = Typed::<_, Order, Prefixed>::new(state.clone());
        let batch = [
            msg(1, b"X:{\"id\":1}"),
            msg(2, br#"{"id":2}"#),
            msg(3, b"X:{\"id\":3}"),
        ];
        adapter.handle_batch(&batch, &ctx()).await.unwrap();
        assert_eq!(*state.seen.lock().unwrap(), vec![1, 3]);
    }

    #[tokio::test]
    async fn typed_with_codec_uses_that_codec_for_decoding() {
        let state = Arc::new(CountingTyped::default());
        let adapter = PerMessage(Typed::<_, Order, Prefixed>::new(state.clone()));
        let batch = [msg(1, b"X:{\"id\":9}"), msg(2, br#"{"id":8}"#)];
        adapter.handle_batch(&batch, &ctx()).await.unwrap();
        assert_eq!(*state.seen.lock().unwrap(), vec![9]);
    }

    #[test]
    fn closure_adapters_produce_send_futures() {
        // The host spawns the batch future onto the runtime, so both closure forms must be Send.
        let (context, first) = (ctx(), msg(1, b"x"));
        let batch = BatchFn(|_b: Vec<StreamMsg>, _c: PartitionContext| async { Ok(()) });
        assert_send(&batch.handle_batch(&[], &context));
        let single = MessageFn(|_m: StreamMsg, _c: PartitionContext| async { Ok(()) });
        assert_send(&single.handle(&first, &context));
    }

    #[test]
    fn index_selection_matches_the_c_sharp_overload() {
        let consumers = vec![
            ConsumerOptions {
                topic: "a".into(),
                ..ConsumerOptions::default()
            },
            ConsumerOptions {
                topic: "b".into(),
                ..ConsumerOptions::default()
            },
        ];
        assert_eq!(configured_at(&consumers, 1).unwrap().topic, "b");
        let err = configured_at(&consumers, 2).unwrap_err();
        assert!(err.to_string().contains("index 2 is out of range"), "{err}");
        assert!(err.to_string().contains("2 entries"), "{err}");
    }

    #[test]
    fn sole_consumer_needs_exactly_one_entry() {
        let one = vec![ConsumerOptions {
            topic: "a".into(),
            ..ConsumerOptions::default()
        }];
        assert_eq!(configured_sole(&one).unwrap().topic, "a");
        assert!(configured_sole(&[]).is_err());
        assert!(configured_sole(&[one[0].clone(), one[0].clone()]).is_err());
    }

    /// Compile-level check that every registration form accepts the bounds its docs promise,
    /// including a non-JSON codec and an `Arc`-shared handler. Never run (the host is C4's).
    #[allow(
        dead_code,
        clippy::many_single_char_names,
        clippy::too_many_arguments,
        clippy::needless_pass_by_value
    )]
    fn every_registration_form_builds(
        a: ConsumerBuilder,
        b: ConsumerBuilder,
        c: ConsumerBuilder,
        d: ConsumerBuilder,
        e: ConsumerBuilder,
        f: ConsumerBuilder,
        g: ConsumerBuilder,
        shared: Arc<CountingTyped>,
    ) -> Vec<StreamsBuilder> {
        vec![
            a.message_fn(|_m: StreamMsg, _c: PartitionContext| async { Ok(()) }),
            b.batch_fn(|_m: Vec<StreamMsg>, _c: PartitionContext| async { Ok(()) }),
            c.typed_handler::<Order, _>(shared.clone()),
            d.typed_batch_handler::<Order, _>(shared.clone()),
            e.typed_handler_with::<Prefixed, Order, _>(shared.clone()),
            f.typed_batch_handler_with::<Prefixed, Order, _>(shared.clone()),
            g.message_handler(Arc::new(MessageFn(
                |_m: StreamMsg, _c: PartitionContext| async { Ok(()) },
            ))),
        ]
    }
}
