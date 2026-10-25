//! The entry codec, v1 (`WIRE.md` §4). **API: W0.2. Bodies: A1** (port of `Wire/EntryCodec.cs`).
//!
//! Writers call [`EntryFields::write_to`] to append the field/value pairs straight onto an `XADD`
//! command; readers call [`decode`] with the field/value pairs of one entry.

use bytes::Bytes;

use super::{HeaderBlock, StreamId, StreamMsg};
use crate::config::TrimMode;
use crate::error::{Error, Result};

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
        cmd.arg(fields::BODY).arg(self.body);
        cmd.arg(fields::TYPE).arg(self.msg_type);
        if let Some(key) = self.partition_key {
            cmd.arg(fields::PARTITION_KEY).arg(key);
        }
        if let Some(id) = non_empty(self.correlation_id) {
            cmd.arg(fields::CORRELATION_ID).arg(id);
        }
        if let Some(parent) = non_empty(self.trace_parent) {
            cmd.arg(fields::TRACE_PARENT).arg(parent);
        }
        if !self.headers.is_empty() {
            let packed: &[u8] = self.headers.packed();
            cmd.arg(fields::HEADERS).arg(packed);
        }
    }
}

impl EntryFields<'_> {
    /// How many arguments [`EntryFields::write_to`] appends (field names and values).
    pub(crate) fn arg_count(&self) -> usize {
        let mut n = 4;
        if self.partition_key.is_some() {
            n += 2;
        }
        if non_empty(self.correlation_id).is_some() {
            n += 2;
        }
        if non_empty(self.trace_parent).is_some() {
            n += 2;
        }
        if !self.headers.is_empty() {
            n += 2;
        }
        n
    }
}

/// Decodes one entry's field/value pairs into a [`StreamMsg`]. Unknown fields are ignored; a missing
/// `t`, `k` or `c` decodes as empty.
///
/// `codec_version` is the topic's `v`; anything but [`CODEC_VERSION`] is refused.
///
/// # Errors
/// `Error::Transport` for an unsupported codec version.
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
    ensure_supported_version(codec_version)?;
    let mut body = Bytes::new();
    let mut msg_type = String::new();
    let mut partition_key = String::new();
    let mut correlation_id = String::new();
    let mut trace_parent = None;
    let mut headers = HeaderBlock::empty();
    for (name, value) in fields {
        // Unknown names fall through: fields are additive within a codec version.
        match name.as_ref() {
            b"b" => body = value,
            b"t" => msg_type = lossy(&value),
            b"k" => partition_key = lossy(&value),
            b"c" => correlation_id = lossy(&value),
            b"p" => trace_parent = Some(lossy(&value)),
            b"h" => headers = HeaderBlock::from_packed(value),
            _ => {}
        }
    }
    Ok(StreamMsg::new(id, partition, msg_type, body)
        .with_partition_key(partition_key)
        .with_correlation_id(correlation_id)
        .with_trace_parent(trace_parent)
        .with_headers(headers))
}

/// `Ok` when `codec_version` is one this build understands.
///
/// # Errors
/// `Error::Transport` naming the version and the `m:{topic}` field.
pub fn ensure_supported_version(codec_version: u32) -> Result<()> {
    if codec_version == CODEC_VERSION {
        Ok(())
    } else {
        Err(Error::transport(format!(
            "Stream entries are stamped with codec version {codec_version}, but this build only understands version {CODEC_VERSION}. Refusing to decode rather than mis-parse the entry; upgrade the service or check the 'v' field of the topic's m:{{topic}} hash."
        )))
    }
}

/// `None` for an empty string: an empty optional field is omitted, never written.
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|v| !v.is_empty())
}

fn lossy(value: &Bytes) -> String {
    String::from_utf8_lossy(value).into_owned()
}

/// The `MAXLEN` arguments for `XADD` under `trim` (C# `TrimArgs.GetMaxLenArgs`): empty for `None`,
/// `MAXLEN ~ n` for `Approx`, `MAXLEN n` for `Exact`.
#[allow(
    dead_code,
    reason = "consumed by the direct publisher (B1) and the trimmer (C5)"
)]
pub(crate) fn max_len_args(trim: TrimMode, max_len: u64) -> Vec<String> {
    match trim {
        TrimMode::None => Vec::new(),
        TrimMode::Exact => vec!["MAXLEN".to_owned(), max_len.to_string()],
        TrimMode::Approx => vec!["MAXLEN".to_owned(), "~".to_owned(), max_len.to_string()],
    }
}

/// The `MINID` arguments for `XTRIM` under `trim` (C# `TrimArgs.GetMinIdArgs`).
pub(crate) fn min_id_args(trim: TrimMode, min_id: StreamId) -> Vec<String> {
    match trim {
        TrimMode::None => Vec::new(),
        TrimMode::Exact => vec!["MINID".to_owned(), min_id.to_string()],
        TrimMode::Approx => vec!["MINID".to_owned(), "~".to_owned(), min_id.to_string()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_args_follow_the_mode() {
        let id = StreamId::new(5, 1);
        assert_eq!(max_len_args(TrimMode::None, 10), Vec::<String>::new());
        assert_eq!(max_len_args(TrimMode::Exact, 10), ["MAXLEN", "10"]);
        assert_eq!(max_len_args(TrimMode::Approx, 10), ["MAXLEN", "~", "10"]);
        assert_eq!(min_id_args(TrimMode::None, id), Vec::<String>::new());
        assert_eq!(min_id_args(TrimMode::Exact, id), ["MINID", "5-1"]);
        assert_eq!(min_id_args(TrimMode::Approx, id), ["MINID", "~", "5-1"]);
    }
}
