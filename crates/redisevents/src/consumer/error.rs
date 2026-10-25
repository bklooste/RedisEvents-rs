//! The handler error contract. **Owner: W0.2** (final). C2 implements the behaviour.
//!
//! | Outcome | C# | Rust | Worker does |
//! | --- | --- | --- | --- |
//! | success | return | `Ok(())` | advance the position |
//! | ordinary failure | any exception | [`HandleError::Skip`] | apply `ConsumerOptions::on_error` (default `BestEffort`: log, count `streams.errors`, advance, carry on) |
//! | must not be lost | `DontIgnoreException` | [`HandleError::Block`] | do **not** advance; retry the same batch after [`BLOCK_BACKOFF`] (1s, 2, 4, 8, 16, then 30s forever) while other partitions run; `streams.blocked` = 1 |
//! | panic | (unhandled exception) | — | caught by the worker and treated as `Skip` |
//!
//! `?` on any `std::error::Error` gives `Skip`, except the crate's own
//! [`Error::Config`] / [`Error::Transport`], which
//! give `Block` (they are `DontIgnoreException`s in C#). Use [`ResultExt::or_block`] or
//! [`HandleError::block`] to block on anything else.

use std::fmt;
use std::time::Duration;

use crate::error::{BoxError, Error};

/// The back-off ladder a blocked partition retries on; the last step repeats.
pub const BLOCK_BACKOFF: [Duration; 6] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
    Duration::from_secs(30),
];

/// What a handler returns.
pub type HandleResult = Result<(), HandleError>;

/// Why a handler failed, and so what the worker does next. See the [module docs](self).
///
/// Deliberately not a `std::error::Error`, so that `?` can convert any error into it.
#[non_exhaustive]
pub enum HandleError {
    /// An ordinary failure: handled by the consumer's `on_error` policy (by default log and skip).
    Skip(BoxError),
    /// Must not be skipped: block the partition and retry with back-off.
    Block(BoxError),
}

impl HandleError {
    /// An ordinary failure.
    pub fn skip(error: impl Into<BoxError>) -> Self {
        Self::Skip(error.into())
    }

    /// A blocking failure.
    pub fn block(error: impl Into<BoxError>) -> Self {
        Self::Block(error.into())
    }

    /// `true` for [`HandleError::Block`].
    #[must_use]
    pub fn is_block(&self) -> bool {
        matches!(self, Self::Block(_))
    }

    /// The underlying error.
    #[must_use]
    pub fn error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        match self {
            Self::Skip(e) | Self::Block(e) => e.as_ref(),
        }
    }

    /// Consumes `self` and returns the underlying error.
    #[must_use]
    pub fn into_error(self) -> BoxError {
        match self {
            Self::Skip(e) | Self::Block(e) => e,
        }
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for HandleError {
    fn from(error: E) -> Self {
        let boxed: BoxError = Box::new(error);
        match boxed.downcast::<Error>() {
            Ok(own) if own.is_dont_ignore() => Self::Block(own),
            Ok(own) => Self::Skip(own),
            Err(other) => Self::Skip(other),
        }
    }
}

impl fmt::Debug for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Skip(e) => f.debug_tuple("Skip").field(e).finish(),
            Self::Block(e) => f.debug_tuple("Block").field(e).finish(),
        }
    }
}

impl fmt::Display for HandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Skip(e) => write!(f, "skip: {e}"),
            Self::Block(e) => write!(f, "block: {e}"),
        }
    }
}

/// Picks the outcome explicitly at a `?` site.
pub trait ResultExt<T> {
    /// Fail with [`HandleError::Block`].
    ///
    /// # Errors
    /// The error of `self`, as `Block`.
    fn or_block(self) -> Result<T, HandleError>;

    /// Fail with [`HandleError::Skip`], even for an error that would otherwise block.
    ///
    /// # Errors
    /// The error of `self`, as `Skip`.
    fn or_skip(self) -> Result<T, HandleError>;
}

impl<T, E: Into<BoxError>> ResultExt<T> for Result<T, E> {
    fn or_block(self) -> Result<T, HandleError> {
        self.map_err(HandleError::block)
    }

    fn or_skip(self) -> Result<T, HandleError> {
        self.map_err(HandleError::skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io() -> std::io::Error {
        std::io::Error::other("boom")
    }

    #[test]
    fn question_mark_maps_ordinary_errors_to_skip_and_dont_ignore_to_block() {
        assert!(!HandleError::from(io()).is_block());
        assert!(HandleError::from(Error::config("bad")).is_block());
        assert!(HandleError::from(Error::transport("bad")).is_block());
        assert!(!HandleError::from(Error::invalid_argument("bad")).is_block());
    }

    #[test]
    fn explicit_choice_overrides() {
        let r: Result<(), std::io::Error> = Err(io());
        assert!(r.or_block().unwrap_err().is_block());
        let r: Result<(), Error> = Err(Error::config("x"));
        assert!(!r.or_skip().unwrap_err().is_block());
    }
}
