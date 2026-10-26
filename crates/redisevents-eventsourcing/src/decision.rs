//! [`AggregateDecision`].

use crate::aggregate::{Aggregate, AggregateRoot};
use crate::error::{Error, Result};

/// What a decide closure concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateDecision<T> {
    /// The command succeeded; save whatever was emitted and return the value.
    Done(T),
    /// The command was refused; return the value and save nothing. Refusing after raising an
    /// event is a bug and fails with [`Error::RefusedAfterEmitting`].
    Refused(T),
    /// The instance was stale (for example a cached one): discard it, reload and decide again.
    /// Not counted against `max_attempts`.
    Redo,
}

impl<T> AggregateDecision<T> {
    /// `Ok(None)` for [`Redo`](Self::Redo); otherwise the value, after checking that a refusal
    /// emitted nothing.
    pub(crate) fn settle<A: Aggregate>(self, root: &AggregateRoot<A>) -> Result<Option<T>> {
        let emitted = root.pending().len();
        match self {
            Self::Redo => Ok(None),
            Self::Refused(_) if emitted > 0 => Err(Error::RefusedAfterEmitting {
                aggregate: A::NAME,
                id: root.id().to_owned(),
                emitted,
            }),
            Self::Done(v) | Self::Refused(v) => Ok(Some(v)),
        }
    }
}
