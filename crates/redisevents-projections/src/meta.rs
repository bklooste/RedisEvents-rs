//! [`EventMeta`] and the per-batch scope [`ProjectionBatch`].

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use redisevents::{StreamId, StreamMsg};

/// What the projector knows about an event besides its decoded value (C# `EventMeta`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EventMeta {
    /// The entry's partition key (the aggregate id for event-sourced topics).
    pub partition_key: String,
    /// The entry's stream position. Strictly increasing within a partition, so it orders
    /// redeliveries.
    pub id: StreamId,
    /// The entry's correlation id (empty when none).
    pub correlation_id: String,
    /// The partition the entry was read from.
    pub partition: u32,
    /// The scope of the batch this event arrived in. `None` only for a hand-built meta.
    pub batch: Option<ProjectionBatch>,
}

impl EventMeta {
    /// A meta with no batch scope, partition 0.
    #[must_use]
    pub fn new(
        partition_key: impl Into<String>,
        id: StreamId,
        correlation_id: impl Into<String>,
    ) -> Self {
        Self {
            partition_key: partition_key.into(),
            id,
            correlation_id: correlation_id.into(),
            partition: 0,
            batch: None,
        }
    }

    /// The meta for `msg`, inside `batch`.
    #[must_use]
    pub fn from_msg(msg: &StreamMsg, batch: &ProjectionBatch) -> Self {
        Self {
            partition_key: msg.partition_key().to_owned(),
            id: msg.id(),
            correlation_id: msg.correlation_id().to_owned(),
            partition: msg.partition(),
            batch: Some(batch.clone()),
        }
    }
}

/// Misuse of a [`ProjectionBatch`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BatchError {
    /// The projector already returned, so the position may be past these events and anything
    /// buffered now would never be flushed. Do not keep a [`ProjectionBatch`] past the handler.
    #[error(
        "the batch for topic={topic} partition={partition} is closed: its projector has returned"
    )]
    Closed {
        /// The batch's topic.
        topic: String,
        /// The batch's partition.
        partition: u32,
    },
    /// The owner already holds state of another type in this batch.
    #[error("this owner holds state of type {found} in this batch, not {expected}")]
    WrongType {
        /// The type asked for.
        expected: &'static str,
        /// The type stored.
        found: &'static str,
    },
}

/// Per-batch scratch space for batch projections (C# `ProjectionBatch`).
///
/// One is created for each call to the projector and shared (it is a cheap handle) by every
/// event of that batch. A [`BatchProjection`](crate::BatchProjection) buffers rows into it while
/// handling events, and writes them in its `flush`. State is keyed by the owner's **address**
/// (pass `self`), so two projections, or the same projection on two topics at once, never see each
/// other's rows. The scope is closed when the projector returns.
#[derive(Clone)]
pub struct ProjectionBatch {
    inner: Arc<Inner>,
}

struct Inner {
    topic: String,
    partition: u32,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    closed: bool,
    slots: HashMap<usize, Slot>,
}

struct Slot {
    type_id: TypeId,
    type_name: &'static str,
    value: Box<dyn Any + Send>,
}

impl ProjectionBatch {
    /// A new, open scope.
    #[must_use]
    pub fn new(topic: impl Into<String>, partition: u32) -> Self {
        Self {
            inner: Arc::new(Inner {
                topic: topic.into(),
                partition,
                state: Mutex::new(State::default()),
            }),
        }
    }

    /// The batch's topic.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.inner.topic
    }

    /// The batch's partition.
    #[must_use]
    pub fn partition(&self) -> u32 {
        self.inner.partition
    }

    /// True once the projector has returned.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Runs `f` on `owner`'s state, creating it with `create` first when absent (C# `GetOrAdd`).
    ///
    /// # Errors
    /// [`BatchError::Closed`] after the projector returned; [`BatchError::WrongType`] when the
    /// owner's state is not an `S`.
    pub fn with_state<O: ?Sized, S: Send + 'static, R>(
        &self,
        owner: &O,
        create: impl FnOnce() -> S,
        f: impl FnOnce(&mut S) -> R,
    ) -> Result<R, BatchError> {
        let mut state = self.open()?;
        let slot = state.slots.entry(key_of(owner)).or_insert_with(|| Slot {
            type_id: TypeId::of::<S>(),
            type_name: std::any::type_name::<S>(),
            value: Box::new(create()),
        });
        slot_mut::<S>(slot).map(f)
    }

    /// Runs `f` on `owner`'s state when it has any (C# `TryGet`).
    ///
    /// # Errors
    /// As [`with_state`](Self::with_state).
    pub fn try_with_state<O: ?Sized, S: Send + 'static, R>(
        &self,
        owner: &O,
        f: impl FnOnce(&mut S) -> R,
    ) -> Result<Option<R>, BatchError> {
        let mut state = self.open()?;
        state
            .slots
            .get_mut(&key_of(owner))
            .map(|slot| slot_mut::<S>(slot).map(f))
            .transpose()
    }

    /// Removes and returns `owner`'s state, typically in `flush`.
    ///
    /// # Errors
    /// As [`with_state`](Self::with_state).
    pub fn take_state<O: ?Sized, S: Send + 'static>(
        &self,
        owner: &O,
    ) -> Result<Option<S>, BatchError> {
        let mut state = self.open()?;
        let key = key_of(owner);
        if let Some(slot) = state.slots.get(&key)
            && slot.type_id != TypeId::of::<S>()
        {
            return Err(BatchError::WrongType {
                expected: std::any::type_name::<S>(),
                found: slot.type_name,
            });
        }
        Ok(state
            .slots
            .remove(&key)
            .and_then(|slot| slot.value.downcast::<S>().ok())
            .map(|b| *b))
    }

    pub(crate) fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        state.slots.clear();
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn open(&self) -> Result<MutexGuard<'_, State>, BatchError> {
        let state = self.lock();
        if state.closed {
            return Err(BatchError::Closed {
                topic: self.inner.topic.clone(),
                partition: self.inner.partition,
            });
        }
        Ok(state)
    }
}

fn key_of<O: ?Sized>(owner: &O) -> usize {
    std::ptr::from_ref(owner).cast::<()>() as usize
}

fn slot_mut<S: 'static>(slot: &mut Slot) -> Result<&mut S, BatchError> {
    let found = slot.type_name;
    slot.value.downcast_mut::<S>().ok_or(BatchError::WrongType {
        expected: std::any::type_name::<S>(),
        found,
    })
}

impl PartialEq for ProjectionBatch {
    /// Two handles are equal when they are the same scope.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for ProjectionBatch {}

impl fmt::Debug for ProjectionBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionBatch")
            .field("topic", &self.inner.topic)
            .field("partition", &self.inner.partition)
            .finish_non_exhaustive()
    }
}
