//! Start-position resolution. **Owner: B4** (port of `StartPosition.cs`).

use time::OffsetDateTime;

use super::StartPosition;
use crate::config::{ConsumerOptions, StartFrom};
use crate::error::Result;
use crate::wire::StreamId;

impl StartPosition {
    /// The beginning of the stream.
    pub const BEGINNING: Self = Self::After(StreamId::MIN);

    /// Just before the first id at `when`, so entries at `when` are included.
    #[must_use]
    pub fn from_time(when: OffsetDateTime) -> Self {
        Self::After(Self::before(StreamId::from_time(when)))
    }

    /// The id immediately before `first` (`MIN` stays `MIN`).
    #[must_use]
    pub fn before(first: StreamId) -> StreamId {
        let _ = first;
        todo!("B4")
    }

    /// Resolves the configured start against the stored position (C# `StartPosition.Resolve`).
    ///
    /// # Errors
    /// [`Error::Config`](crate::Error::Config) for `Stored` with a `Stored` fallback, or `Date`
    /// without a date.
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
        let _ = (start_from, when_missing, date, stored, topic);
        todo!("B4")
    }
}
