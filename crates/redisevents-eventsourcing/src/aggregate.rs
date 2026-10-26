//! The aggregate abstraction: [`Aggregate`] (the type-level description) and [`AggregateRoot`] (an
//! instance with id, version and pending events).

use std::marker::PhantomData;
use std::ops::Deref;

use bytes::Bytes;
use redisevents_projections::EventTypeRegistry;

use crate::error::{Error, Result};

/// Describes an event-sourced aggregate: its name, its state, its events and how an event changes
/// the state.
///
/// Implement it on a **marker type**, not on the domain state itself. That keeps pure
/// `decide`/`apply` domains (commands take `&State` and return an event; `apply(&mut State, &Event)`)
/// usable from another crate without wrapper structs, and it is what lets the orphan rule allow
/// `impl Aggregate for Inventory` in a service crate where both the trait and the state live
/// elsewhere. The id and version are not part of the state: [`AggregateRoot`] tracks them.
pub trait Aggregate: Send + Sync + 'static {
    /// The state commands decide against, rebuilt by applying events to `Default`.
    type State: Default + Send + Sync + 'static;
    /// The event type. Usually one enum over the aggregate's events.
    type Event: Send + Sync + 'static;

    /// The aggregate name used in the state stream name `es:<NAME>:<id>`. It is part of the wire
    /// contract and must match the other language.
    const NAME: &'static str;

    /// Applies one event to the state. Must be infallible and deterministic: it runs when an event
    /// is emitted and again on every load.
    fn apply(state: &mut Self::State, event: &Self::Event);

    /// Encodes an event as `(wire type, body)`.
    ///
    /// The default asks the registry (`register_json::<Self::Event>(..)` and friends), which works
    /// when each event is its own registered type. An aggregate whose `Event` is an enum over
    /// several wire types overrides this to pick the wire type and body per variant (the registry
    /// decodes such a family with `register_family`).
    ///
    /// # Errors
    /// [`Error::Store`] when the event type is not registered or its codec fails.
    fn encode(registry: &EventTypeRegistry, event: &Self::Event) -> Result<(String, Bytes)> {
        let (wire, body) = registry.encode(event)?;
        Ok((wire.to_owned(), body))
    }
}

/// One aggregate instance: id, version, state and the events emitted since it was loaded or last
/// saved.
///
/// It derefs to the state, so a pure domain's commands are called directly:
/// `let event = root.check_in(5, 1.0)?; root.emit(event);`.
pub struct AggregateRoot<A: Aggregate> {
    id: String,
    version: u64,
    state: A::State,
    pending: Vec<A::Event>,
    _marker: PhantomData<fn() -> A>,
}

impl<A: Aggregate> std::fmt::Debug for AggregateRoot<A>
where
    A::State: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateRoot")
            .field("aggregate", &A::NAME)
            .field("id", &self.id)
            .field("version", &self.version)
            .field("state", &self.state)
            .field("pending", &self.pending.len())
            .finish()
    }
}

impl<A: Aggregate> AggregateRoot<A> {
    /// A new, empty aggregate at version 0 with `id`.
    ///
    /// # Errors
    /// [`Error::BlankId`] when `id` is blank.
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(Error::BlankId);
        }
        Ok(Self {
            id,
            version: 0,
            state: A::State::default(),
            pending: Vec::new(),
            _marker: PhantomData,
        })
    }

    /// The aggregate name (`A::NAME`).
    #[must_use]
    pub fn name(&self) -> &'static str {
        A::NAME
    }

    /// The id the aggregate is stored under.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The number of events in the state stream as of the last load or save (the optimistic
    /// concurrency token). Pending events are not counted.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The current state, including pending events.
    #[must_use]
    pub fn state(&self) -> &A::State {
        &self.state
    }

    /// Applies a new `event` to the state and queues it to be saved.
    pub fn emit(&mut self, event: A::Event) {
        A::apply(&mut self.state, &event);
        self.pending.push(event);
    }

    /// The events emitted since the last load or save, oldest first.
    #[must_use]
    pub fn pending(&self) -> &[A::Event] {
        &self.pending
    }

    /// Advances the version by the number of pending events and clears them. A repository calls
    /// this after a successful save; application code does not need to.
    pub fn commit(&mut self) {
        self.version += self.pending.len() as u64;
        self.pending.clear();
    }

    /// Rebuilds the aggregate `id` from a stored history, applied in order. The version is the
    /// history's length and nothing is pending.
    ///
    /// # Errors
    /// [`Error::BlankId`] when `id` is blank.
    pub fn from_history(
        id: impl Into<String>,
        history: impl IntoIterator<Item = A::Event>,
    ) -> Result<Self> {
        let mut root = Self::new(id)?;
        for event in history {
            A::apply(&mut root.state, &event);
            root.version += 1;
        }
        Ok(root)
    }
}

impl<A: Aggregate> Deref for AggregateRoot<A> {
    type Target = A::State;

    fn deref(&self) -> &A::State {
        &self.state
    }
}
