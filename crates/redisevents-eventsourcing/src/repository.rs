//! [`EventRepository`] and its Redis implementation.

use std::future::Future;
use std::sync::Arc;

use redisevents::StreamId;
use redisevents::producer::{PublishOptions, StateEvent, StreamStore};
use redisevents_projections::EventTypeRegistry;
use tracing::Instrument;

use crate::aggregate::{Aggregate, AggregateRoot};
use crate::diagnostics;
use crate::error::{ConcurrencyError, Error, Result};
use crate::store::EventStore;

/// Loads and saves aggregates.
pub trait EventRepository: Send + Sync {
    /// Loads aggregate `id` by replaying its state stream; `None` when it has no history.
    fn load<A: Aggregate>(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Option<AggregateRoot<A>>>> + Send;

    /// Saves the pending events if the stored version is still `expected_version` (default:
    /// the aggregate's own [`version`](AggregateRoot::version)), then marks them committed and
    /// returns the new version. Nothing pending: no round trip, returns the current version.
    ///
    /// # Errors
    /// [`Error::Concurrency`] when the version check failed; nothing was written and the
    /// aggregate keeps its pending events.
    fn save<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        expected_version: Option<u64>,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<u64>> + Send;

    /// [`save`](Self::save) with no version check, for single-writer aggregates.
    fn save_unchecked<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        options: &PublishOptions,
    ) -> impl Future<Output = Result<u64>> + Send;
}

/// Events per page when loading. A short page ends the stream.
pub const LOAD_PAGE_SIZE: usize = 1000;

/// The state stream name of an aggregate, `es:<aggregate>:<id>`. In Redis the key is
/// `<prefix>{<topic>}:state:es:<aggregate>:<id>`.
#[must_use]
pub fn stream_name(aggregate_name: &str, id: &str) -> String {
    format!("es:{aggregate_name}:{id}")
}

/// An [`EventRepository`] over a state stream per aggregate. The version is the stream's length;
/// every event is also published to the store's topic with the aggregate id as partition key.
pub struct RedisEventRepository<S = StreamStore> {
    store: S,
    registry: Arc<EventTypeRegistry>,
}

impl<S: EventStore> RedisEventRepository<S> {
    /// A repository over `store` using `registry` to encode and decode events.
    #[must_use]
    pub fn new(store: S, registry: impl Into<Arc<EventTypeRegistry>>) -> Self {
        Self {
            store,
            registry: registry.into(),
        }
    }

    /// The underlying store.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    async fn load_inner<A: Aggregate>(&self, id: &str) -> Result<Option<AggregateRoot<A>>> {
        let name = stream_name(A::NAME, id);
        let mut history: Vec<A::Event> = Vec::new();
        let mut after = StreamId::MIN;
        loop {
            let page = self.store.read(&name, after, LOAD_PAGE_SIZE).await?;
            let Some(last) = page.last() else { break };
            after = last.id();
            let full = page.len() >= LOAD_PAGE_SIZE;
            for message in &page {
                let decoded = self
                    .registry
                    .decode::<A::Event>(message.msg_type(), message.body())
                    .map_err(|e| {
                        Error::History(format!(
                            "'{}' '{id}' has an event of wire type '{}' at {} in its history \
                             whose body does not decode: {e}",
                            A::NAME,
                            message.msg_type(),
                            message.id()
                        ))
                    })?;
                let Some(event) = decoded else {
                    return Err(Error::History(format!(
                        "'{}' '{id}' has an event of wire type '{}' at {} in its history, which \
                         is not registered with the EventTypeRegistry for this aggregate's event \
                         type. An aggregate's own history must be fully understood; register the \
                         type (or a shim that reads it) rather than skipping the event.",
                        A::NAME,
                        message.msg_type(),
                        message.id()
                    )));
                };
                history.push(event);
            }
            if !full {
                break;
            }
        }
        if history.is_empty() {
            return Ok(None);
        }
        AggregateRoot::<A>::from_history(id, history).map(Some)
    }

    fn encode_all<A: Aggregate>(
        &self,
        root: &AggregateRoot<A>,
        options: &PublishOptions,
    ) -> Result<Vec<StateEvent>> {
        root.pending()
            .iter()
            .map(|event| {
                let (wire, body) = A::encode(&self.registry, event)?;
                Ok(StateEvent::new(body, wire).with_options(options.clone()))
            })
            .collect()
    }

    async fn save_inner<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        expected: u64,
        options: &PublishOptions,
        span: &tracing::Span,
    ) -> Result<u64> {
        let events = self.encode_all(root, options)?;
        let name = stream_name(A::NAME, root.id());
        let ids = self
            .store
            .append_and_publish(&name, expected, root.id(), &events)
            .await?;
        if ids.is_none() {
            // Nothing was written, neither the history nor the topic, so the aggregate keeps its
            // pending events and the caller can reload, decide again and retry.
            tracing::debug!(
                aggregate = A::NAME,
                id = root.id(),
                expected_version = expected,
                "Event sourcing: save lost its version check; nothing was written."
            );
            diagnostics::concurrency_conflict(span);
            return Err(ConcurrencyError::new(A::NAME, root.id(), expected).into());
        }
        root.commit();
        let new_version = expected + events.len() as u64;
        diagnostics::saved(span, new_version, events.len());
        Ok(new_version)
    }

    async fn save_unchecked_inner<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        expected: u64,
        options: &PublishOptions,
        span: &tracing::Span,
    ) -> Result<u64> {
        let events = self.encode_all(root, options)?;
        let name = stream_name(A::NAME, root.id());
        self.store
            .append_and_publish_unchecked(&name, root.id(), &events)
            .await?;
        root.commit();
        let new_version = expected + events.len() as u64;
        diagnostics::saved(span, new_version, events.len());
        Ok(new_version)
    }
}

impl<S: EventStore> EventRepository for RedisEventRepository<S> {
    async fn load<A: Aggregate>(&self, id: &str) -> Result<Option<AggregateRoot<A>>> {
        if id.trim().is_empty() {
            return Err(Error::BlankId);
        }
        let span = diagnostics::load_span(A::NAME, id);
        let result = self.load_inner::<A>(id).instrument(span.clone()).await;
        match &result {
            Ok(root) => diagnostics::loaded(&span, root.as_ref().map(AggregateRoot::version)),
            Err(e) => diagnostics::failed(&span, e),
        }
        result
    }

    async fn save<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        expected_version: Option<u64>,
        options: &PublishOptions,
    ) -> Result<u64> {
        if root.pending().is_empty() {
            // A transaction that appends no events is not an append.
            return Ok(root.version());
        }
        let expected = expected_version.unwrap_or(root.version());
        let span = diagnostics::save_span(A::NAME, root.id(), expected);
        let result = self
            .save_inner(root, expected, options, &span)
            .instrument(span.clone())
            .await;
        if let Err(e) = &result
            && e.as_concurrency().is_none()
        {
            diagnostics::failed(&span, e);
        }
        result
    }

    async fn save_unchecked<A: Aggregate>(
        &self,
        root: &mut AggregateRoot<A>,
        options: &PublishOptions,
    ) -> Result<u64> {
        if root.pending().is_empty() {
            return Ok(root.version());
        }
        let expected = root.version();
        let span = diagnostics::save_span(A::NAME, root.id(), expected);
        let result = self
            .save_unchecked_inner(root, expected, options, &span)
            .instrument(span.clone())
            .await;
        if let Err(e) = &result {
            diagnostics::failed(&span, e);
        }
        result
    }
}
