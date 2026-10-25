//! The domain half of the Inventory CQRS-ES sample, a Rust port of the Redis variant of
//! [cqrs-simple-micro-service](https://github.com/bklooste/cqrs-simple-micro-service/tree/main/redis).
//!
//! Nothing here touches Redis. The write side ([`InventoryItem`]) decides which events a command
//! raises, and the read side ([`views`]) turns events into view changes. The command and view
//! services wire these into `redisevents-eventsourcing` and `redisevents-projections`.
//!
//! The events are **wire-compatible with the C# sample**: same topic, same aggregate name, same
//! wire type strings, same JSON. A Rust command service can therefore feed the C# view service, and
//! the reverse. See [`events`] for the parts of that contract that are easy to get wrong.

pub mod events;
mod item;
pub mod views;

pub use events::InventoryEvent;
pub use item::{DomainError, InventoryItem};

/// The topic both services use.
pub const TOPIC: &str = "inventory";

/// The aggregate name. The state stream of item `<id>` is `{inventory}:state:es:Inventory:<id>`.
pub const AGGREGATE_NAME: &str = "Inventory";
