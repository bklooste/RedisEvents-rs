//! Positions: how far each consumer got on each partition (`WIRE.md` §7). **Trait and
//! [`PositionRecord`]/[`StartPosition`] types: W0.2** (final). Implementations:
//!
//! | File | WP | Contents |
//! | --- | --- | --- |
//! | `redis_store.rs` | B4 | [`RedisPositionStore`], [`NullPositionStore`], record format/parse |
//! | `reset_marker.rs` | B4 | `__reset:<p>` markers (crate-private; consumed by C3 and D1) |
//! | `start.rs` | B4 | [`StartPosition::resolve`] and friends |
//! | `flusher.rs` | C3 | the position flusher (foreign-writer check, reset hand-off, trim interlock) |

pub(crate) mod flusher;
pub mod redis_store;
pub(crate) mod reset_marker;
pub(crate) mod start;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use uuid::Uuid;

pub use redis_store::{NullPositionStore, RedisPositionStore};

use crate::error::Result;
use crate::wire::StreamId;

/// The instance id an operator rewind is written under (`6f72616e-6765-4164-6d69-6e5265736574`,
/// ASCII "orangeAdminReset"). A consumer finding it on a partition it owns treats it as a rewind,
/// not as a rival writer.
pub const ADMIN_INSTANCE_ID: Uuid = Uuid::from_u128(0x6f72_616e_6765_4164_6d69_6e52_6573_6574);

/// One stored position: `<streamId>|<instanceId>` (C# `PositionRecord`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct PositionRecord {
    /// The last processed id (exclusive resume point).
    pub id: StreamId,
    /// Who wrote it. [`Uuid::nil`] when the stored value was a bare id.
    pub instance_id: Uuid,
}

impl PositionRecord {
    /// The separator between id and instance.
    pub const SEPARATOR: char = '|';

    /// `<id>|<instance_id>` with the GUID lowercase hyphenated (.NET `"D"`).
    #[must_use]
    pub fn format(&self) -> String {
        format!(
            "{}{}{}",
            self.id,
            Self::SEPARATOR,
            self.instance_id.hyphenated()
        )
    }

    /// Parses a stored value; a value with no `|` is a bare id, and an instance part that is not a
    /// `"D"` GUID reads as nil (as C#). `None` when the id part does not parse.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        let (id_text, instance_text) = match value.split_once(Self::SEPARATOR) {
            Some((id, instance)) => (id, Some(instance)),
            None => (value, None),
        };
        let id = StreamId::parse(id_text).ok()?;
        // Only the canonical 36-character `"D"` form counts; `Uuid::try_parse` also takes the
        // braced, simple and URN forms, which .NET's `Guid.TryParseExact("D")` rejects.
        let instance_id = instance_text
            .filter(|text| text.len() == 36)
            .and_then(|text| Uuid::try_parse(text).ok())
            .unwrap_or(Uuid::nil());
        Some(Self { id, instance_id })
    }
}

/// Where a partition's read starts (C# `StartPosition`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StartPosition {
    /// Read strictly after this id (`MIN` = the beginning).
    After(StreamId),
    /// Read only entries added from now (`$`).
    Now,
}

/// Loads and saves positions (C# `IPositionStore`).
///
/// Partitions are `u32`; fields of the positions hash that are not decimal partition numbers
/// (reset markers, anything else) are skipped by [`PositionStore::load`].
pub trait PositionStore: Send + Sync + 'static {
    /// Every stored position of `consumer` on `topic`.
    fn load(
        &self,
        topic: &str,
        consumer: &str,
    ) -> impl Future<Output = Result<BTreeMap<u32, StreamId>>> + Send;

    /// Writes the given positions (and their meta fields), stamped with this instance's id.
    fn save(
        &self,
        topic: &str,
        consumer: &str,
        positions: &[(u32, StreamId)],
    ) -> impl Future<Output = Result<()>> + Send;

    /// Sets the position of one partition, or of all when `partition` is `None`, to `to`.
    fn reset(
        &self,
        topic: &str,
        consumer: &str,
        to: StreamId,
        partition: Option<u32>,
    ) -> impl Future<Output = Result<()>> + Send;
}

impl<S: PositionStore> PositionStore for Arc<S> {
    fn load(
        &self,
        topic: &str,
        consumer: &str,
    ) -> impl Future<Output = Result<BTreeMap<u32, StreamId>>> + Send {
        (**self).load(topic, consumer)
    }

    fn save(
        &self,
        topic: &str,
        consumer: &str,
        positions: &[(u32, StreamId)],
    ) -> impl Future<Output = Result<()>> + Send {
        (**self).save(topic, consumer, positions)
    }

    fn reset(
        &self,
        topic: &str,
        consumer: &str,
        to: StreamId,
        partition: Option<u32>,
    ) -> impl Future<Output = Result<()>> + Send {
        (**self).reset(topic, consumer, to, partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_instance_id_matches_csharp() {
        assert_eq!(
            ADMIN_INSTANCE_ID.hyphenated().to_string(),
            "6f72616e-6765-4164-6d69-6e5265736574"
        );
    }
}
