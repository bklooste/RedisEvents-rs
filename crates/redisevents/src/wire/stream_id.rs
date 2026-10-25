//! [`StreamId`], a Redis stream entry id. **Type: W0.2. Parse/format: A1** (`WIRE.md` §4 "Stream ids").

use std::fmt;
use std::str::FromStr;

use time::OffsetDateTime;

use crate::error::ParseStreamIdError;

/// A Redis stream entry id, `<ms>-<seq>`.
///
/// Ordered by `ms`, then `seq`, which is Redis's order. A stored position is *exclusive*: a consumer
/// resumes strictly after it. The enqueue time of an entry is [`StreamId::timestamp`]; entries carry
/// no timestamp field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    /// Milliseconds since the Unix epoch, by the Redis server's clock.
    pub ms: u64,
    /// Sequence number within the millisecond.
    pub seq: u64,
}

impl StreamId {
    /// `0-0`, before every entry. Reading after it reads the whole stream.
    pub const MIN: Self = Self { ms: 0, seq: 0 };

    /// The largest id. Note the C# `StreamId.Max` is `i64::MAX-i64::MAX`; the two never reach the
    /// wire as positions, only as range ends, where both mean "the end".
    pub const MAX: Self = Self {
        ms: u64::MAX,
        seq: u64::MAX,
    };

    /// The id `ms-seq`.
    #[must_use]
    pub const fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    /// The first possible id at `when` (`<ms>-0`). Instants before the epoch clamp to [`StreamId::MIN`].
    #[must_use]
    pub fn from_time(when: OffsetDateTime) -> Self {
        let ms = when.unix_timestamp_nanos() / 1_000_000;
        Self::new(u64::try_from(ms).unwrap_or(0), 0)
    }

    /// The instant of the `ms` part (the enqueue time, for an entry id).
    ///
    /// Saturates at the largest instant `time` can represent.
    #[must_use]
    pub fn timestamp(self) -> OffsetDateTime {
        let nanos = i128::from(self.ms) * 1_000_000;
        OffsetDateTime::from_unix_timestamp_nanos(nanos)
            .unwrap_or_else(|_| time::PrimitiveDateTime::MAX.assume_utc())
    }

    /// Parses `<ms>-<seq>`: two runs of ASCII decimal digits separated by one `-`, nothing else
    /// (no sign, no whitespace, no `*`, no bare `<ms>`). Same rules as C# `StreamId.TryParse`.
    ///
    /// # Errors
    /// [`ParseStreamIdError`] when `s` is not of that form or a part overflows `u64`.
    pub fn parse(s: &str) -> Result<Self, ParseStreamIdError> {
        Self::parse_bytes(s.as_bytes())
    }

    /// [`StreamId::parse`] over raw bytes, for ids taken straight from a RESP reply without a UTF-8
    /// check.
    ///
    /// # Errors
    /// As [`StreamId::parse`].
    pub fn parse_bytes(s: &[u8]) -> Result<Self, ParseStreamIdError> {
        let _ = s;
        todo!("A1")
    }

    /// Formats as `<ms>-<seq>` into `buf` without allocating and returns the written slice.
    /// 41 bytes always suffice.
    pub fn format_into(self, buf: &mut [u8; 41]) -> &str {
        let _ = buf;
        todo!("A1")
    }
}

impl fmt::Display for StreamId {
    /// `<ms>-<seq>`, invariant decimal.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _ = f;
        todo!("A1")
    }
}

impl FromStr for StreamId {
    type Err = ParseStreamIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_ms_then_seq() {
        assert!(StreamId::new(1, 9) < StreamId::new(2, 0));
        assert!(StreamId::new(2, 0) < StreamId::new(2, 1));
        assert!(StreamId::MIN < StreamId::new(0, 1));
        assert!(StreamId::new(u64::MAX, 0) < StreamId::MAX);
    }

    #[test]
    fn time_round_trips_at_millisecond_precision() {
        let id = StreamId::new(1_700_000_000_123, 0);
        assert_eq!(StreamId::from_time(id.timestamp()), id);
        assert_eq!(
            StreamId::from_time(OffsetDateTime::UNIX_EPOCH - time::Duration::DAY),
            StreamId::MIN
        );
    }
}
