//! The wire layer: everything whose bytes must match the C# library (`wire-spec/WIRE.md`).
//!
//! **Types owned by W0.2** ([`StreamId`], [`StreamMsg`], [`TypedMsg`], [`HeaderBlock`] API, [`Keys`]
//! API, codec signatures). **Bodies filled in by A1** (`todo!("A1")`), checked against the golden
//! vectors from W0.1.

pub mod codec;
pub mod header_block;
pub mod keys;
pub mod msg;
pub mod router;
pub mod stream_id;

pub use codec::{CODEC_VERSION, EntryFields};
pub use header_block::{Header, HeaderBlock, HeaderIter};
pub use keys::Keys;
pub use msg::{StreamMsg, TypedMsg};
pub use router::{RoundRobin, partition_for_key};
pub use stream_id::StreamId;
