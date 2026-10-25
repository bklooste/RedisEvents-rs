//! [`AggregateDecision`] (C# `AggregateDecision<T>`).

use crate::aggregate::{Aggregate, AggregateRoot};
use crate::error::{Error, Result};

/// What a decide closure concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateDecision<T> {
    /// The command succeeded; save whatever was raised and return the value.
    Done(T),
    /// The command was refused; return the value and save nothing. Refusing after raising an
    /// event is a bug and fails with [`Error::InvalidOperation`].
    Refused(T),
    /// The instance was stale (for example a cached one): discard it, reload and decide again.
    /// Not counted against `max_attempts`.
    Redo,
}

impl<T> AggregateDecision<T> {
    /// [`AggregateDecision::Done`].
    #[must_use]
    pub fn done(value: T) -> Self {
        Self::Done(value)
    }

    /// [`AggregateDecision::Refused`].
    #[must_use]
    pub fn refused(value: T) -> Self {
        Self::Refused(value)
    }

    /// [`AggregateDecision::Redo`].
    #[must_use]
    pub fn redo() -> Self {
        Self::Redo
    }

    /// `Ok(None)` for [`Redo`](Self::Redo); otherwise the value, after checking that a refusal
    /// raised nothing.
    pub(crate) fn settle<A: Aggregate>(self, root: &AggregateRoot<A>) -> Result<Option<T>> {
        let raised = root.uncommitted_changes().len();
        match self {
            Self::Redo => Ok(None),
            Self::Refused(_) if raised > 0 => Err(Error::InvalidOperation(format!(
                "{} '{}' refused a command after already raising {raised} event(s). \
                 A command must decide before it raises anything; nothing was saved.",
                A::NAME,
                root.id()
            ))),
            Self::Done(v) | Self::Refused(v) => Ok(Some(v)),
        }
    }
}
