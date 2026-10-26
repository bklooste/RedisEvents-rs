//! [`CachedAggregate`].

use tokio::sync::Mutex;

use crate::aggregate::{Aggregate, AggregateRoot};
use crate::decision::AggregateDecision;
use crate::error::{Error, ExecuteError, Result};
use crate::execute::{ExecuteOptions, load_or_create};
use crate::repository::EventRepository;

/// Keeps the last known-good instance of one aggregate between commands, so a hot aggregate is not
/// replayed on every command.
///
/// One `CachedAggregate` serves one aggregate id and is shared by reference (put it in an `Arc` or
/// in your service state): [`run`](Self::run) takes `&self` and holds an internal lock for the
/// whole load, decide, save, so concurrent commands on this id are serialised instead of racing on
/// the version. The cached instance is dropped whenever it might be stale or half-decided: a lost
/// version check, a refusal after emitting, an error from `decide`, a failed save, or
/// [`Redo`](AggregateDecision::Redo).
pub struct CachedAggregate<A: Aggregate> {
    cached: Mutex<Option<AggregateRoot<A>>>,
    expected_version: Option<fn(&AggregateRoot<A>) -> u64>,
}

impl<A: Aggregate> Default for CachedAggregate<A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: Aggregate> CachedAggregate<A> {
    /// An empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cached: Mutex::new(None),
            expected_version: None,
        }
    }

    /// Overrides the version a save expects (default: the aggregate's own
    /// [`version`](AggregateRoot::version)), for an aggregate whose stream position is not the
    /// number of events replayed onto the instance.
    #[must_use]
    pub fn with_expected_version(mut self, selector: fn(&AggregateRoot<A>) -> u64) -> Self {
        self.expected_version = Some(selector);
        self
    }

    /// Drops the cached instance; the next run reloads.
    pub async fn evict(&self) {
        *self.cached.lock().await = None;
    }

    /// True while an instance is cached.
    pub async fn is_cached(&self) -> bool {
        self.cached.lock().await.is_some()
    }

    /// Runs `decide` against the cached instance (or a fresh load of `id`) and saves what it
    /// emitted, retrying as [`execute`](crate::execute) does. `decide` also learns whether its
    /// instance came from the cache, so it can do its own freshness check and return
    /// [`AggregateDecision::Redo`].
    ///
    /// Returns `decide`'s value.
    ///
    /// # Errors
    /// As [`execute`](crate::execute).
    pub async fn run<R, T, E, F>(
        &self,
        repository: &R,
        id: &str,
        options: &ExecuteOptions,
        mut decide: F,
    ) -> Result<T, ExecuteError<E>>
    where
        R: EventRepository,
        F: AsyncFnMut(&mut AggregateRoot<A>, bool) -> Result<AggregateDecision<T>, E>,
    {
        let mut slot = self.cached.lock().await;
        let mut attempt = 1_u32;
        loop {
            let cached = slot.take();
            let from_cache = cached.is_some();
            let mut root = match cached {
                Some(root) => root,
                None => load_or_create::<R, A>(repository, id).await?,
            };
            // the slot stays empty until a save commits or a no-op decision stands: an error
            // anywhere below must not leave a stale or half-decided instance for the next caller.
            let decision = decide(&mut root, from_cache)
                .await
                .map_err(ExecuteError::Decide)?;
            let Some(value) = decision.settle(&root)? else {
                continue;
            };
            if root.pending().is_empty() {
                *slot = Some(root);
                return Ok(value);
            }
            if !options.concurrency_check {
                repository
                    .save_unchecked(&mut root, &options.publish)
                    .await?;
                *slot = Some(root);
                return Ok(value);
            }
            let expected = self.expected_version.map(|select| select(&root));
            match repository.save(&mut root, expected, &options.publish).await {
                Ok(_) => {
                    *slot = Some(root);
                    return Ok(value);
                }
                Err(Error::Concurrency(conflict)) if attempt < options.max_attempts.get() => {
                    if let Some(hook) = &options.on_conflict {
                        hook(attempt, conflict).await;
                    }
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}
