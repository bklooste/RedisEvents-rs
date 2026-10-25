//! The entry codec, v1 (`WIRE.md` §4). **API: W0.2. Bodies: A1** (port of `Wire/EntryCodec.cs`).
//!
//! Writers call [`EntryFields::write_to`] to append the field/value pairs straight onto an `XADD`
//! command; readers call [`decode`] with the field/value pairs of one entry.

use bytes::Bytes;

use super::{HeaderBlock, StreamId, StreamMsg};
use crate::error::Result;

/// The codec version this build reads and writes, recorded once in `m:{topic}` field `v`.
pub const CODEC_VERSION: u32 = 1;

/// Field names. One byte each.
pub mod fields {
    /// Body, raw bytes. Always present.
    pub const BODY: &str = "b";
    /// Message type string. Always present.
    pub const TYPE: &str = "t";
    /// Partition key. Always present on a topic entry (may be empty); never on a state entry.
    pub const PARTITION_KEY: &str = "k";
    /// Correlation id. Optional.
    pub const CORRELATION_ID: &str = "c";
    /// W3C `traceparent`. Optional.
    pub const TRACE_PARENT: &str = "p";
    /// Packed headers. Optional.
    pub const HEADERS: &str = "h";
}

/// The fields of one entry to write.
#[derive(Debug, Clone, Copy)]
pub struct EntryFields<'a> {
    /// The body (`b`).
    pub body: &'a [u8],
    /// The type string (`t`).
    pub msg_type: &'a str,
    /// `Some(key)` for a topic entry (`k`, written even when empty); `None` for a state entry,
    /// which has no `k`.
    pub partition_key: Option<&'a str>,
    /// Correlation id (`c`); omitted when `None` or empty.
    pub correlation_id: Option<&'a str>,
    /// `traceparent` (`p`); omitted when `None` or empty.
    pub trace_parent: Option<&'a str>,
    /// Packed headers (`h`); omitted when empty. Packing (and its caps) happens before this point,
    /// once per publish call, not once per entry.
    pub headers: &'a HeaderBlock,
}

impl EntryFields<'_> {
    /// Appends `field value …` in the canonical order `b t k c p h` to `cmd`, which the caller has
    /// already started as `XADD <key> [trim args] *`.
    pub fn write_to(&self, cmd: &mut redis::Cmd) {
        let _ = cmd;
        todo!("A1")
    }
}

/// Decodes one entry's field/value pairs into a [`StreamMsg`]. Unknown fields are ignored; a missing
/// `t`, `k` or `c` decodes as empty.
///
/// `codec_version` is the topic's `v`; anything but [`CODEC_VERSION`] is refused.
///
/// # Errors
/// [`Error::Transport`](crate::Error::Transport) for an unsupported codec version.
pub fn decode<N, I>(
    id: StreamId,
    partition: u32,
    fields: I,
    codec_version: u32,
) -> Result<StreamMsg>
where
    N: AsRef<[u8]>,
    I: IntoIterator<Item = (N, Bytes)>,
{
    let _ = (id, partition, fields.into_iter(), codec_version);
    todo!("A1")
}

/// `Ok` when `codec_version` is one this build understands.
///
/// # Errors
/// [`Error::Transport`](crate::Error::Transport) naming the version and the `m:{topic}` field.
pub fn ensure_supported_version(codec_version: u32) -> Result<()> {
    let _ = codec_version;
    todo!("A1")
}
