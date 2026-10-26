//! View stores.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Mutex, PoisonError};

use redis::aio::ConnectionManager;
use redisevents::wire::Keys;
use redisevents::{Connection, Error, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A read model keyed by id.
pub trait ViewStore<T>: Send + Sync + 'static {
    /// The view for `id`, if any.
    fn get(&self, id: &str) -> impl Future<Output = Result<Option<T>>> + Send;
    /// Replaces the view for `id`.
    fn set(&self, id: &str, view: &T) -> impl Future<Output = Result<()>> + Send;
    /// Removes the view for `id`; a missing id is not an error.
    fn delete(&self, id: &str) -> impl Future<Output = Result<()>> + Send;
    /// Every view, in no guaranteed order. Collected into memory: for very large views page through
    /// the store directly.
    fn list(&self) -> impl Future<Output = Result<Vec<T>>> + Send;
}

/// A [`ViewStore`] in a `Mutex<BTreeMap>`, for tests and single-process read models.
#[derive(Debug)]
pub struct InMemoryViewStore<T> {
    views: Mutex<BTreeMap<String, T>>,
}

impl<T> Default for InMemoryViewStore<T> {
    fn default() -> Self {
        Self {
            views: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<T> InMemoryViewStore<T> {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn map(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, T>> {
        self.views.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// The in-memory store has nothing to await; the trait's futures are still the contract.
#[allow(clippy::unused_async_trait_impl)]
impl<T: Clone + Send + Sync + 'static> ViewStore<T> for InMemoryViewStore<T> {
    async fn get(&self, id: &str) -> Result<Option<T>> {
        require_id(id)?;
        Ok(self.map().get(id).cloned())
    }

    async fn set(&self, id: &str, view: &T) -> Result<()> {
        require_id(id)?;
        self.map().insert(id.to_owned(), view.clone());
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        require_id(id)?;
        self.map().remove(id);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<T>> {
        Ok(self.map().values().cloned().collect())
    }
}

fn require_id(id: &str) -> Result<()> {
    if id.trim().is_empty() {
        return Err(Error::invalid_argument("a view id must not be blank"));
    }
    Ok(())
}

/// A [`ViewStore`] on one Redis hash, one JSON value per id.
///
/// The hash is `<prefix>{<topic>}:view:<owner>:<view_name>` ([`Keys::view`]). The `owner` is the
/// service that owns the read model, so two services can keep views of the same name over one
/// topic without colliding. It is part of the key, so a reader sees a writer's views only with the
/// same owner string; a Rust service reading a .NET service's views passes that service's name
/// (its entry assembly name, unless it configured another).
///
/// JSON casing is serde's: add `rename_all` when other services or HTTP clients read the same
/// documents.
#[derive(Clone)]
pub struct RedisViewStore<T> {
    conn: ConnectionManager,
    key: String,
    _view: std::marker::PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for RedisViewStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisViewStore")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl<T> RedisViewStore<T> {
    /// A store for `view_name` over `topic`, owned by `owner`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] when `topic`, `owner` or `view_name` is blank.
    pub fn new(
        conn: &Connection,
        keys: &Keys,
        topic: &str,
        owner: &str,
        view_name: &str,
    ) -> Result<Self> {
        for (name, value) in [("topic", topic), ("view_name", view_name), ("owner", owner)] {
            if value.trim().is_empty() {
                return Err(Error::invalid_argument(format!("{name} must not be blank")));
            }
        }
        Ok(Self {
            conn: conn.shared(),
            key: keys.view(topic, owner, view_name),
            _view: std::marker::PhantomData,
        })
    }

    /// The Redis key of the hash.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    fn decode(&self, bytes: &[u8]) -> Result<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(bytes).map_err(|e| {
            Error::transport_with(
                format!("view hash '{}' held an unreadable value", self.key),
                e,
            )
        })
    }
}

impl<T> ViewStore<T> for RedisViewStore<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn get(&self, id: &str) -> Result<Option<T>> {
        require_id(id)?;
        let mut conn = self.conn.clone();
        let value: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(&self.key)
            .arg(id)
            .query_async(&mut conn)
            .await?;
        value.map(|bytes| self.decode(&bytes)).transpose()
    }

    async fn set(&self, id: &str, view: &T) -> Result<()> {
        require_id(id)?;
        let bytes = serde_json::to_vec(view)
            .map_err(|e| Error::transport_with("view could not be serialized", e))?;
        let mut conn = self.conn.clone();
        redis::cmd("HSET")
            .arg(&self.key)
            .arg(id)
            .arg(bytes)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        require_id(id)?;
        let mut conn = self.conn.clone();
        redis::cmd("HDEL")
            .arg(&self.key)
            .arg(id)
            .query_async::<()>(&mut conn)
            .await?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<T>> {
        let mut conn = self.conn.clone();
        let mut views = Vec::new();
        let mut cursor: u64 = 0;
        loop {
            let (next, fields): (u64, Vec<Vec<u8>>) = redis::cmd("HSCAN")
                .arg(&self.key)
                .arg(cursor)
                .query_async(&mut conn)
                .await?;
            // [field, value, field, value, ...]
            for pair in fields.as_chunks::<2>().0 {
                views.push(self.decode(&pair[1])?);
            }
            if next == 0 {
                return Ok(views);
            }
            cursor = next;
        }
    }
}
