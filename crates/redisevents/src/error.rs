//! The crate error type.
//!
//! **Owner: W0.2.** Variants are added only through a lead-reviewed PR (`docs/CONTRACTS.md`).
//!
//! The C# library has two exception types that mean "do not ignore this": `StreamConfigurationException`
//! and `StreamTransportException`, both `DontIgnoreException`s. Here they are [`Error::Config`] and
//! [`Error::Transport`], and [`Error::is_dont_ignore`] reports exactly those two. When a handler
//! returns one through `?`, the [`HandleError`](crate::HandleError) conversion turns it into
//! `Block`, as the C# worker does.

use std::fmt;

/// A boxed, thread-safe error, used wherever a source error of any type is carried.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// `Result` with the crate [`Error`] as the default error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything the library can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The configuration is invalid or contradicts what is recorded in Redis (C#
    /// `StreamConfigurationException`). Never retried by the library; blocks a handler.
    #[error("{0}")]
    Config(String),

    /// The data in Redis cannot be read safely: a malformed entry, header block or id, an unknown
    /// codec version, or a failed connect (C# `StreamTransportException`). Blocks a handler.
    #[error("{message}")]
    Transport {
        /// What went wrong, written for an operator.
        message: String,
        /// The underlying error, if any.
        #[source]
        source: Option<BoxError>,
    },

    /// A Redis command failed. Callers decide whether to retry; see
    /// [`redis::RedisError::is_connection_dropped`] and friends.
    #[error(transparent)]
    Redis(#[from] redis::RedisError),

    /// A caller passed a value the API rejects, for example more than 64 headers (C#
    /// `ArgumentException`).
    #[error("{0}")]
    InvalidArgument(String),

    /// A payload could not be encoded or decoded.
    #[error(transparent)]
    Codec(#[from] crate::message::CodecError),

    /// The component (publisher, host, …) named here has shut down and accepts no more work.
    #[error("{0} is closed")]
    Closed(&'static str),
}

impl Error {
    /// A configuration error with the given message.
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    /// A transport error with the given message and no source.
    pub fn transport(message: impl Into<String>) -> Self {
        Self::Transport {
            message: message.into(),
            source: None,
        }
    }

    /// A transport error wrapping `source`.
    pub fn transport_with(message: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Transport {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// An invalid-argument error with the given message.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    /// `true` for the errors the C# library models as `DontIgnoreException`
    /// ([`Error::Config`] and [`Error::Transport`]). A handler returning one blocks its partition.
    #[must_use]
    pub fn is_dont_ignore(&self) -> bool {
        matches!(self, Self::Config(_) | Self::Transport { .. })
    }
}

/// The error returned when a string is not a Redis stream id (`<ms>-<seq>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseStreamIdError {
    input: String,
}

impl ParseStreamIdError {
    /// An error for the given rejected input.
    pub fn new(input: impl Into<String>) -> Self {
        Self {
            input: input.into(),
        }
    }

    /// The rejected input.
    #[must_use]
    pub fn input(&self) -> &str {
        &self.input
    }
}

impl fmt::Display for ParseStreamIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "'{}' is not a valid Redis stream id; expected '<ms>-<seq>'.",
            self.input
        )
    }
}

impl std::error::Error for ParseStreamIdError {}

impl From<ParseStreamIdError> for Error {
    fn from(e: ParseStreamIdError) -> Self {
        Self::transport(e.to_string())
    }
}
