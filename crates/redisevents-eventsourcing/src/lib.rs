//! Event-sourced aggregates and an optimistic-concurrency repository on redisevents (port of
//! `RedisEvents.EventSourcing`).
//!
//! - [`Aggregate`]: a marker-type description of an aggregate (name, state, event, `apply`).
//! - [`AggregateRoot`]: an instance with id, version and uncommitted changes; derefs to the state.
//! - [`EventRepository`] / [`RedisEventRepository`]: load by replaying the state stream
//!   `es:<Aggregate>:<id>`, save with the stream length as the expected version.
//! - [`execute`] and [`CachedAggregate`]: load, decide, save with retry on conflict.
//!
//! The wire format is the C# one: an aggregate saved from either language loads in the other.

mod aggregate;
mod cached;
mod decision;
pub mod diagnostics;
mod error;
mod execute;
mod repository;
mod store;

pub use aggregate::{Aggregate, AggregateRoot};
pub use cached::CachedAggregate;
pub use decision::AggregateDecision;
pub use error::{ConcurrencyError, Error, ExecuteError, Result};
pub use execute::{CommandResult, ConflictHook, ExecuteOptions, execute, load_or_create};
pub use repository::{EventRepository, LOAD_PAGE_SIZE, RedisEventRepository, stream_name};
pub use store::EventStore;
