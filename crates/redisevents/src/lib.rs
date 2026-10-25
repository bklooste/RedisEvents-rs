#![doc = include_str!("../README.md")]

#[doc = include_str!("../../../README.md")]
#[cfg(doctest)]
mod readme_doctests {}

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
