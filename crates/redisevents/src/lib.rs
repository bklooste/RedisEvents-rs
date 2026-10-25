//! Wire-compatible Rust port of [RedisEvents](https://github.com/bklooste/RedisEvents): partitioned
//! Redis Streams topics, per-key ordering and at-least-once delivery.
//!
//! A topic is `n` Redis streams (`s:{topic}:<p>`). A publisher routes each message to a partition by
//! hashing its partition key, so every message for one key lands on one stream, in order. A
//! consumer owns a set of partitions, reads each one in order, hands batches to a handler and
//! records how far it got in a positions hash. Everything that reaches Redis is byte-for-byte what
//! the C# library writes; see `wire-spec/WIRE.md`.
//!
//! # Status
//!
//! This is the **contract skeleton** (work package W0.2 in `docs/PLAN.md`). The public types and
//! traits are final; bodies marked `todo!("<WP>")` are filled in by the named work package. See
//! `docs/CONTRACTS.md` for who owns what.
//!
//! # Layout
//!
//! | Module | Contents |
//! | --- | --- |
//! | [`wire`] | stream ids, the message type, header block, key layout, entry codec, partition routing |
//! | [`message`] | the typed-message trait and the payload codec seam (JSON here, MessagePack in `redisevents-msgpack`) |
//! | [`config`] | option structs mirroring the C# `Streams` configuration section |
//! | [`connection`] | the redis-rs connection wrapper |
//! | [`producer`] | publisher traits, direct and buffered publishers, outbox |
//! | [`consumer`] | handler traits, the error contract, partition context, workers and host |
//! | [`positions`] | position store trait and records, reset markers, flusher |
//! | [`ownership`] | partition ownership (static and lease mode) |
//! | [`admin`] | topic meta, reset/preview, ownership and tail reads, health and lag |
//! | [`diagnostics`] | metric and span names, tracing |
//! | [`error`] | the crate error type |
//! | `testing` | in-memory doubles (feature `testing`) |

pub mod admin;
pub mod config;
pub mod connection;
pub mod consumer;
pub mod diagnostics;
pub mod error;
pub mod message;
pub mod ownership;
pub mod positions;
pub mod producer;
#[cfg(feature = "testing")]
pub mod testing;
pub mod wire;

pub use connection::Connection;
pub use consumer::{
    BatchHandler, HandleError, HandleResult, MessageHandler, PartitionContext, TypedBatchHandler,
    TypedHandler,
};
pub use error::{BoxError, Error, Result};
pub use message::{Json, Message, PayloadCodec};
pub use positions::PositionStore;
pub use producer::{BufferedPublisher, PublishOptions, Publisher, PublisherExt};
pub use wire::{HeaderBlock, StreamId, StreamMsg, TypedMsg};
