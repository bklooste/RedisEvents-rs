//! Typed event projections and view stores over a `redisevents` topic.
//!
//! # Shape
//!
//! 1. An [`EventTypeRegistry`] maps **explicit wire type strings** to Rust types and a codec. Wire
//!    strings are never derived from Rust type names.
//! 2. A [`Projection<T>`] handles one decoded event type `T`. One struct may implement it for
//!    several `T`, or `T` may be an enum over a whole topic (see [`EventTypeRegistry::register_family`]).
//! 3. A [`Projector`] is built from the registry and the bound projections. It implements core's
//!    [`BatchHandler`](redisevents::BatchHandler), so it rides the ordinary consumer unchanged:
//!    positions, replay and the `Skip` / `Block` error contract are core's.
//! 4. A [`ViewStore<T>`] holds the read model: [`RedisViewStore`] (a hash per view, JSON) or
//!    [`InMemoryViewStore`].
//!
//! # Dispatch without reflection
//!
//! Rust cannot list the trait impls of a value, so binding is explicit and checked at build time:
//! `Projector::builder(..).projection::<T, _>(p)`. The registry decodes a message to a type-erased
//! value; the projector looks the projections up by the decoded value's `TypeId`; each binding is
//! a small generic shim that downcasts and calls `Projection<T>::handle`. There is no per-event
//! reflection, and a binding for a type the registry cannot produce is a [`Config`] error at
//! `build()` instead of a silently dead projection.
//!
//! # Error contract
//!
//! The projector's contract:
//! * an unknown wire type is skipped (warned once per type, then debug);
//! * a malformed entry (known type, body fails to decode) is skipped **per entry**: logged,
//!   counted, and the rest of the batch is still projected;
//! * a projection error ends the batch and is returned unchanged (`Block` redelivers, anything
//!   else follows `on_error`); see [`Projector`] for the batch-projection flush rules.
//!
//! # JSON casing
//!
//! [`Json`] is `serde_json`, which keeps field names as written. A .NET service using
//! System.Text.Json's default writes **PascalCase**, and a mismatch is silent on that side
//! (missing properties read as defaults). Types shared with such a service need
//! `#[serde(rename_all = "PascalCase")]`; per-type naming is just serde attributes on the type.
//!
//! # Example
//!
//! A projection that counts orders, run by a consumer. The `EventTypeRegistry` is the only place
//! wire strings appear; the Rust type name is never used on the wire.
//!
//! ```no_run
//! use redisevents::consumer::{HandleResult, PartitionContext, Streams};
//! use redisevents::Connection;
//! use redisevents_projections::{EventMeta, EventTypeRegistry, Projection, Projector};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! #[serde(rename_all = "PascalCase")]
//! struct OrderPlaced {
//!     order_id: String,
//! }
//!
//! struct PrintOrders;
//!
//! impl Projection<OrderPlaced> for PrintOrders {
//!     async fn handle(
//!         &self,
//!         event: &OrderPlaced,
//!         _meta: &EventMeta,
//!         _ctx: &PartitionContext,
//!     ) -> HandleResult {
//!         println!("placed {}", event.order_id);
//!         Ok(())
//!     }
//! }
//!
//! async fn run() -> redisevents::Result<()> {
//!     let registry = EventTypeRegistry::new().register_json::<OrderPlaced>("Shop.OrderPlaced")?;
//!     let projector = Projector::builder("orders", registry)
//!         .projection::<OrderPlaced, _>(PrintOrders)
//!         .build()?;
//!     let conn = Connection::connect("redis://127.0.0.1:6379").await?;
//!     let host = Streams::builder(conn)
//!         .topic("orders")
//!         .handler(projector)
//!         .spawn()
//!         .await?;
//!     host.shutdown().await
//! }
//! ```
//!
//! [`Config`]: redisevents::Error::Config
//! [`Json`]: redisevents::Json

mod diagnostics;
mod meta;
mod projection;
mod projector;
mod registry;
mod view_store;

pub use diagnostics::{MALFORMED_EVENTS, SOURCE_NAME, malformed_events_total};
pub use meta::EventMeta;
pub use projection::{BatchFlush, BatchProjection, Projection};
pub use projector::{Projector, ProjectorBuilder};
pub use registry::{Decoded, EventTypeRegistry};
pub use view_store::{InMemoryViewStore, RedisViewStore, ViewStore};
