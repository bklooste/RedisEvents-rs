//! Start-position resolution. **Owner: B4** (port of `StartPosition.cs`).
//!
//! A position is "the last id already dealt with", and a read resumes strictly after it. `$` (`Now`)
//! is resolved by the server on the first read only; [`StartPosition::read_from`] encodes that.
// C2 consumes `read_from`; until then only tests do.
#![allow(dead_code)]

use time::OffsetDateTime;

use super::StartPosition;
use crate::config::{ConsumerOptions, StartFrom};
use crate::error::{Error, Result};
use crate::wire::StreamId;

/// The id to pass to the next `XREAD`: a concrete id, or `$` (new entries only).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadFrom {
    /// Read strictly after this id.
    Id(StreamId),
    /// The server's `$`: only entries added after the read reaches it.
    NewMessages,
}

impl StartPosition {
    /// The beginning of the stream.
    pub const BEGINNING: Self = Self::After(StreamId::MIN);

    /// Just before the first id at `when`, so entries at `when` are included.
    #[must_use]
    pub fn from_time(when: OffsetDateTime) -> Self {
        Self::After(Self::before(StreamId::from_time(when)))
    }

    /// The id immediately before `first` (`MIN` stays `MIN`).
    ///
    /// Sequence numbers run out within a millisecond before the millisecond advances, so
    /// `(ms-1, i64::MAX)` is the largest id below `ms-0`. The arithmetic is exact; no probe read.
    #[must_use]
    pub fn before(first: StreamId) -> StreamId {
        if first.seq > 0 {
            return StreamId::new(first.ms, first.seq - 1);
        }
        if first.ms > 0 {
            return StreamId::new(first.ms - 1, StreamId::MAX_PART);
        }
        StreamId::MIN
    }

    /// Resolves the configured start against the stored position (C# `StartPosition.Resolve`).
    ///
    /// # Errors
    /// [`Error::Config`] for `Stored` with a `Stored` fallback, or `Date` without a date.
    pub fn resolve(options: &ConsumerOptions, stored: Option<StreamId>) -> Result<Self> {
        Self::resolve_with(
            options.start_from,
            options.start_from_when_missing,
            options.start_from_date,
            stored,
            Some(&options.topic),
        )
    }

    /// [`StartPosition::resolve`] from its parts.
    ///
    /// # Errors
    /// As [`StartPosition::resolve`].
    pub fn resolve_with(
        start_from: StartFrom,
        when_missing: StartFrom,
        date: Option<OffsetDateTime>,
        stored: Option<StreamId>,
        topic: Option<&str>,
    ) -> Result<Self> {
        match start_from {
            StartFrom::Stored => {
                if let Some(id) = stored {
                    return Ok(Self::After(id));
                }
                if when_missing == StartFrom::Stored {
                    return Err(Error::config(format!(
                        "Consumer{} has StartFrom=Stored and StartFromWhenMissing=Stored, which is circular. \
                         Set StartFromWhenMissing to Beginning (the default), Now or Date.",
                        describe(topic)
                    )));
                }
                // One level only: the fallback is never itself Stored, so this cannot recurse.
                Self::resolve_with(when_missing, StartFrom::Beginning, date, None, topic)
            }
            StartFrom::Beginning => Ok(Self::BEGINNING),
            StartFrom::Now => Ok(Self::Now),
            StartFrom::Date => match date {
                Some(when) => Ok(Self::from_time(when)),
                None => Err(Error::config(format!(
                    "Consumer{} has StartFrom=Date but no StartFromDate. \
                     Set StartFromDate to an ISO-8601 instant, for example 2026-09-01T00:00:00Z.",
                    describe(topic)
                ))),
            },
        }
    }

    /// The id for the next `XREAD`, given the last id this worker actually read.
    ///
    /// `$` only for the very first read of a `Now` start. After that the worker passes the last id it
    /// saw, so a reconnect resumes instead of skipping whatever was published in between.
    #[must_use]
    pub(crate) fn read_from(self, last_read: Option<StreamId>) -> ReadFrom {
        match (last_read, self) {
            (Some(id), _) | (None, Self::After(id)) => ReadFrom::Id(id),
            (None, Self::Now) => ReadFrom::NewMessages,
        }
    }
}

fn describe(topic: Option<&str>) -> String {
    match topic {
        Some(name) if !name.trim().is_empty() => format!(" for topic '{name}'"),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId::new(ms, seq)
    }

    fn stored_resolve(
        start_from: StartFrom,
        when_missing: StartFrom,
        date: Option<OffsetDateTime>,
        stored: Option<StreamId>,
    ) -> Result<StartPosition> {
        StartPosition::resolve_with(start_from, when_missing, date, stored, None)
    }

    #[test]
    fn resolve_stored_with_stored_position_resumes_after_it() {
        let resolved = stored_resolve(
            StartFrom::Stored,
            StartFrom::Beginning,
            None,
            Some(id(500, 2)),
        )
        .expect("ok");

        assert_eq!(resolved, StartPosition::After(id(500, 2)));
        assert_eq!(resolved.read_from(None), ReadFrom::Id(id(500, 2)));
    }

    #[test]
    fn resolve_stored_when_missing_falls_back_to_beginning_by_default() {
        let resolved =
            stored_resolve(StartFrom::Stored, StartFrom::Beginning, None, None).expect("ok");

        assert_eq!(resolved, StartPosition::BEGINNING);
        assert_eq!(resolved.read_from(None), ReadFrom::Id(StreamId::MIN));
    }

    #[test]
    fn resolve_stored_when_missing_honours_now_fallback() {
        let resolved = stored_resolve(StartFrom::Stored, StartFrom::Now, None, None).expect("ok");

        assert_eq!(resolved, StartPosition::Now);
        assert_eq!(resolved.read_from(None), ReadFrom::NewMessages);
    }

    #[test]
    fn resolve_stored_when_missing_honours_date_fallback() {
        let when = datetime!(2026-09-01 0:00 UTC);

        let resolved =
            stored_resolve(StartFrom::Stored, StartFrom::Date, Some(when), None).expect("ok");

        assert_eq!(resolved, StartPosition::from_time(when));
    }

    #[test]
    fn resolve_stored_when_missing_is_also_stored_is_circular_and_throws() {
        let err = StartPosition::resolve_with(
            StartFrom::Stored,
            StartFrom::Stored,
            None,
            None,
            Some("orders"),
        )
        .expect_err("circular");

        let text = err.to_string();
        assert!(
            text.contains("circular") && text.contains("orders"),
            "{text}"
        );
    }

    #[test]
    fn resolve_beginning_ignores_a_stored_position() {
        let resolved = stored_resolve(StartFrom::Beginning, StartFrom::Now, None, Some(id(999, 0)))
            .expect("ok");

        assert_eq!(resolved, StartPosition::BEGINNING);
    }

    #[test]
    fn resolve_now_ignores_a_stored_position() {
        let resolved = stored_resolve(StartFrom::Now, StartFrom::Beginning, None, Some(id(999, 0)))
            .expect("ok");

        assert_eq!(resolved, StartPosition::Now);
    }

    #[test]
    fn resolve_date_places_the_position_immediately_before_that_millisecond() {
        let when = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("in range");

        let resolved = stored_resolve(
            StartFrom::Date,
            StartFrom::Beginning,
            Some(when),
            Some(id(1, 0)),
        )
        .expect("ok");

        assert_eq!(
            resolved,
            StartPosition::After(id(1_699_999_999_999, StreamId::MAX_PART))
        );
        assert_eq!(
            StartPosition::before(StreamId::from_time(when)),
            id(1_699_999_999_999, StreamId::MAX_PART),
            "an entry written exactly on that millisecond must be delivered, not skipped"
        );
    }

    #[test]
    fn resolve_date_without_a_date_throws() {
        let err = StartPosition::resolve_with(
            StartFrom::Date,
            StartFrom::Beginning,
            None,
            None,
            Some("orders"),
        )
        .expect_err("needs a date");

        assert!(err.to_string().contains("StartFromDate"));
    }

    #[test]
    fn resolve_from_consumer_options_uses_its_three_settings() {
        let options = ConsumerOptions {
            topic: "orders".to_owned(),
            start_from: StartFrom::Stored,
            start_from_when_missing: StartFrom::Now,
            ..ConsumerOptions::default()
        };

        assert_eq!(
            StartPosition::resolve(&options, None).expect("ok"),
            StartPosition::Now
        );
        assert_eq!(
            StartPosition::resolve(&options, Some(id(4, 1))).expect("ok"),
            StartPosition::After(id(4, 1))
        );
    }

    #[test]
    fn read_from_now_uses_dollar_only_for_the_very_first_read() {
        let now = StartPosition::Now;

        assert_eq!(now.read_from(None), ReadFrom::NewMessages);
        assert_eq!(now.read_from(Some(id(10, 4))), ReadFrom::Id(id(10, 4)));
    }

    #[test]
    fn read_from_non_now_modes_never_use_dollar() {
        assert_eq!(
            StartPosition::BEGINNING.read_from(None),
            ReadFrom::Id(StreamId::MIN)
        );
        assert_eq!(
            StartPosition::After(id(7, 2)).read_from(None),
            ReadFrom::Id(id(7, 2))
        );
        assert_eq!(
            StartPosition::After(id(7, 2)).read_from(Some(id(9, 0))),
            ReadFrom::Id(id(9, 0))
        );
    }

    #[test]
    fn before_is_the_largest_id_strictly_below() {
        assert_eq!(StartPosition::before(id(5, 3)), id(5, 2));
        assert_eq!(StartPosition::before(id(5, 0)), id(4, StreamId::MAX_PART));
        assert_eq!(StartPosition::before(id(0, 0)), id(0, 0));
    }
}
