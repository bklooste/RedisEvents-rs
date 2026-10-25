//! The load, decide, save runner with retry (C# `EventRepositoryExtensions.ExecuteAsync`).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use redisevents::producer::PublishOptions;

use crate::aggregate::{Aggregate, AggregateRoot};
use crate::decision::AggregateDecision;
use crate::error::{ConcurrencyError, Error, ExecuteError, Result};
use crate::repository::EventRepository;

/// Called before a retry after a lost version check, with the 1-based attempt that lost and the
/// error. Not called for the final loss (that error is returned).
pub type ConflictHook =
    Arc<dyn Fn(u32, ConcurrencyError) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// How [`execute`] and [`CachedAggregate::run`](crate::CachedAggregate::run) save and retry.
#[derive(Clone)]
pub struct ExecuteOptions {
    pub(crate) max_attempts: u32,
    pub(crate) concurrency_check: bool,
    pub(crate) publish: PublishOptions,
    pub(crate) on_conflict: Option<ConflictHook>,
}

impl Default for ExecuteOptions {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            concurrency_check: true,
            publish: PublishOptions::default(),
            on_conflict: None,
        }
    }
}

impl std::fmt::Debug for ExecuteOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecuteOptions")
            .field("max_attempts", &self.max_attempts)
            .field("concurrency_check", &self.concurrency_check)
            .field("publish", &self.publish)
            .field("on_conflict", &self.on_conflict.is_some())
            .finish()
    }
}

impl ExecuteOptions {
    /// Total tries (load, decide, save) when the version check is lost. Default 1 (no retry);
    /// 0 is rejected when the runner starts.
    #[must_use]
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// `false` saves without the version check (single-writer aggregates): a conflict cannot
    /// happen, and concurrent writers can interleave silently. Default `true`.
    #[must_use]
    pub fn concurrency_check(mut self, check: bool) -> Self {
        self.concurrency_check = check;
        self
    }

    /// Correlation id, headers and so on, applied to every published event of a save.
    #[must_use]
    pub fn publish_options(mut self, options: PublishOptions) -> Self {
        self.publish = options;
        self
    }

    /// Runs `hook(attempt, error)` before each retry, for example to back off.
    #[must_use]
    pub fn on_conflict<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(u32, ConcurrencyError) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_conflict = Some(Arc::new(move |attempt, error| {
            Box::pin(hook(attempt, error))
        }));
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_attempts < 1 {
            return Err(Error::InvalidArgument(
                "At least one attempt is required (max_attempts).".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A command's value and the aggregate's version after it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommandResult<T> {
    /// What the decide closure returned.
    pub value: T,
    /// The aggregate's version after the command (unchanged when nothing was raised).
    pub version: u64,
}

/// Loads `id`, or a new empty aggregate at version 0 when it has no history (C#
/// `LoadOrCreateAsync`).
///
/// # Errors
/// As [`EventRepository::load`]; a blank id is [`Error::InvalidArgument`].
pub async fn load_or_create<R: EventRepository, A: Aggregate>(
    repository: &R,
    id: &str,
) -> Result<AggregateRoot<A>> {
    match repository.load::<A>(id).await? {
        Some(root) => Ok(root),
        None => AggregateRoot::new(id),
    }
}

/// Load, decide, save (C# `ExecuteAsync`), loading fresh on every call.
///
/// `decide` gets the aggregate, raises events on it and returns an [`AggregateDecision`]. When it
/// raised nothing, nothing is saved. A lost version check reloads and decides again until
/// `max_attempts`; any other save error propagates (the outcome of a connection lost around the
/// write is unknown, and a caller's retry reloads and decides against whichever happened).
///
/// # Errors
/// [`ExecuteError::Decide`] with `decide`'s error; [`ExecuteError::Repository`] otherwise, with
/// [`Error::Concurrency`] once attempts are exhausted and [`Error::InvalidOperation`] for a
/// refusal after raising.
pub async fn execute<R, A, T, E, F>(
    repository: &R,
    id: &str,
    options: &ExecuteOptions,
    mut decide: F,
) -> Result<CommandResult<T>, ExecuteError<E>>
where
    R: EventRepository,
    A: Aggregate,
    F: AsyncFnMut(&mut AggregateRoot<A>) -> Result<AggregateDecision<T>, E>,
{
    options.validate()?;
    let mut attempt = 1_u32;
    loop {
        let mut root = load_or_create::<R, A>(repository, id).await?;
        let decision = decide(&mut root).await.map_err(ExecuteError::Decide)?;
        let Some(value) = decision.settle(&root)? else {
            continue;
        };
        if root.uncommitted_changes().is_empty() {
            return Ok(CommandResult {
                value,
                version: root.version(),
            });
        }
        if !options.concurrency_check {
            let version = repository
                .save_unchecked(&mut root, &options.publish)
                .await?;
            return Ok(CommandResult { value, version });
        }
        match repository.save(&mut root, None, &options.publish).await {
            Ok(version) => return Ok(CommandResult { value, version }),
            Err(Error::Concurrency(conflict)) if attempt < options.max_attempts => {
                if let Some(hook) = &options.on_conflict {
                    hook(attempt, conflict).await;
                }
                attempt += 1;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
