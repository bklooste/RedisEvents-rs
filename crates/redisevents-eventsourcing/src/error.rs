//! Errors (C# `ConcurrencyException` plus the `InvalidOperationException`s of the repository).

/// A save lost its version check: the aggregate changed since it was loaded, and nothing was
/// written (C# `ConcurrencyException`). Reload, decide again and retry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("'{aggregate_name}' '{id}': expected version {expected_version}, but it has changed.")]
pub struct ConcurrencyError {
    /// The aggregate name.
    pub aggregate_name: String,
    /// The aggregate id.
    pub id: String,
    /// The version the save expected the state stream to be at.
    pub expected_version: u64,
}

impl ConcurrencyError {
    /// A concurrency error for `aggregate_name` / `id` at `expected_version`.
    #[must_use]
    pub fn new(
        aggregate_name: impl Into<String>,
        id: impl Into<String>,
        expected_version: u64,
    ) -> Self {
        Self {
            aggregate_name: aggregate_name.into(),
            id: id.into(),
            expected_version,
        }
    }
}

/// Everything the repository and the runners can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The version check failed; see [`ConcurrencyError`]. Caller-recoverable, never blocks a
    /// consumer.
    #[error(transparent)]
    Concurrency(#[from] ConcurrencyError),

    /// The store failed (Redis, transport, an invalid argument to the store, a registry that
    /// cannot encode the event).
    #[error(transparent)]
    Store(#[from] redisevents::Error),

    /// An aggregate's own history could not be understood: a wire type the registry does not
    /// decode to this aggregate's event type, or a body that does not decode. History is never
    /// skipped.
    #[error("{0}")]
    History(String),

    /// The call is not valid in the current state (C# `InvalidOperationException`), for example a
    /// refusal after events were raised.
    #[error("{0}")]
    InvalidOperation(String),

    /// An argument is invalid (C# `ArgumentException` / `ArgumentOutOfRangeException`).
    #[error("{0}")]
    InvalidArgument(String),
}

impl Error {
    /// The [`ConcurrencyError`], if that is what this is.
    #[must_use]
    pub fn as_concurrency(&self) -> Option<&ConcurrencyError> {
        match self {
            Self::Concurrency(e) => Some(e),
            _ => None,
        }
    }
}

/// A [`Result`](std::result::Result) with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What `execute` and `CachedAggregate::run` fail with: the caller's own decide error, or a
/// repository failure.
#[derive(Debug, thiserror::Error)]
pub enum ExecuteError<E> {
    /// The decide closure returned an error (a domain rule refused the command). Nothing was
    /// saved.
    #[error("{0}")]
    Decide(E),
    /// Loading or saving failed; [`Error::Concurrency`] when `max_attempts` was exhausted.
    #[error(transparent)]
    Repository(#[from] Error),
}

impl<E> ExecuteError<E> {
    /// True when attempts were exhausted on a lost version check (HTTP 409 in a service).
    #[must_use]
    pub fn is_concurrency(&self) -> bool {
        matches!(self, Self::Repository(Error::Concurrency(_)))
    }
}
