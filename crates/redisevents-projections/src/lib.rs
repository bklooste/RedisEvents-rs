//! Typed event projections and view stores over a `redisevents` topic (port of
//! `RedisEvents.Projections`).
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
//! C# finds the `IProjection<T>` implementations by reflection. Rust cannot list the trait
//! impls of a value, so binding is explicit and checked at build time:
//! `Projector::builder(..).projection::<T, _>(p)`. The registry decodes a message to a type-erased
//! value; the projector looks the projections up by the decoded value's `TypeId`; each binding is
//! a small generic shim that downcasts and calls `Projection<T>::handle`. There is no per-event
//! reflection, and a binding for a type the registry cannot produce is a [`Config`] error at
//! `build()` instead of a silently dead projection.
//!
//! # Error contract
//!
//! Identical to the C# projector:
//! * an unknown wire type is skipped (warned once per type, then debug);
//! * a malformed entry (known type, body fails to decode) is skipped **per entry**: logged,
//!   counted, and the rest of the batch is still projected;
//! * a projection error ends the batch and is returned unchanged (`Block` redelivers, anything
//!   else follows `on_error`); see [`Projector`] for the batch-projection flush rules.
//!
//! # JSON casing
//!
//! [`Json`] is `serde_json`, which keeps field names as written. C# events use
//! System.Text.Json's default, **PascalCase**, and a mismatch is silent on the C# side (missing
//! properties read as defaults). Types shared with C# normally need
//! `#[serde(rename_all = "PascalCase")]`; per-type naming is just serde attributes on the type.
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
pub use meta::{BatchError, EventMeta, ProjectionBatch};
pub use projection::{BatchProjection, Projection};
pub use projector::{Projector, ProjectorBuilder};
pub use registry::{Decoded, EventTypeRegistry};
pub use view_store::{InMemoryViewStore, RedisViewStore, ViewStore, default_owner};
