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

    /// The largest valid part. C# stores both parts as `long`, so a part above `i64::MAX` is not a
    /// valid id on this wire (`WIRE.md` §4) and [`StreamId::parse`] rejects it.
    pub const MAX_PART: u64 = i64::MAX as u64;

    /// The largest id, `i64::MAX-i64::MAX` (C# `StreamId.Max`).
    pub const MAX: Self = Self {
        ms: Self::MAX_PART,
        seq: Self::MAX_PART,
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
    /// (no sign, no whitespace, no `*`, no bare `<ms>`), each part in `0..=i64::MAX`
    /// ([`StreamId::MAX_PART`]). Same rules as C# `StreamId.TryParse`; pinned by
    /// `wire-spec/vectors/formats.json` `stream_id_parse`.
    ///
    /// # Errors
    /// [`ParseStreamIdError`] when `s` is not of that form or a part exceeds `i64::MAX`.
    pub fn parse(s: &str) -> Result<Self, ParseStreamIdError> {
        Self::parse_bytes(s.as_bytes())
    }

    /// [`StreamId::parse`] over raw bytes, for ids taken straight from a RESP reply without a UTF-8
    /// check.
    ///
    /// # Errors
    /// As [`StreamId::parse`].
    pub fn parse_bytes(s: &[u8]) -> Result<Self, ParseStreamIdError> {
        let invalid = || ParseStreamIdError::new(String::from_utf8_lossy(s));
        let dash = s.iter().position(|&b| b == b'-').ok_or_else(invalid)?;
        let ms = parse_part(&s[..dash]).ok_or_else(invalid)?;
        let seq = parse_part(&s[dash + 1..]).ok_or_else(invalid)?;
        Ok(Self::new(ms, seq))
    }

    /// Formats as `<ms>-<seq>` into `buf` without allocating and returns the written slice.
    /// 41 bytes always suffice (39 for valid ids).
    pub fn format_into(self, buf: &mut [u8; 41]) -> &str {
        let mut at = put_decimal(self.ms, buf);
        buf[at] = b'-';
        at += 1;
        at += put_decimal(self.seq, &mut buf[at..]);
        std::str::from_utf8(&buf[..at]).unwrap_or("")
    }
}

/// One part of an id: non-empty ASCII digits only, value `0..=MAX_PART`.
fn parse_part(part: &[u8]) -> Option<u64> {
    if part.is_empty() || !part.iter().all(u8::is_ascii_digit) {
        return None;
    }
    part.iter()
        .try_fold(0u64, |acc, &d| {
            acc.checked_mul(10)?.checked_add(u64::from(d - b'0'))
        })
        .filter(|&v| v <= StreamId::MAX_PART)
}

/// Writes `value` in decimal at the start of `out` and returns the digit count. `out` must have room.
fn put_decimal(mut value: u64, out: &mut [u8]) -> usize {
    let mut digits = [0u8; 20];
    let mut n = 0;
    loop {
        digits[n] = u8::try_from(value % 10).map_or(b'0', |d| b'0' + d);
        value /= 10;
        n += 1;
        if value == 0 {
            break;
        }
    }
    for (slot, &digit) in out.iter_mut().zip(digits[..n].iter().rev()) {
        *slot = digit;
    }
    n
}

impl fmt::Display for StreamId {
    /// `<ms>-<seq>`, invariant decimal.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
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
        assert!(StreamId::new(StreamId::MAX_PART, 0) < StreamId::MAX);
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
