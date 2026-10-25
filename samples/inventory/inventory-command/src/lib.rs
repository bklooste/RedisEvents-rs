//! The Inventory write side: a Rust port of `SimpleCQRS.Redis.API`, the C# command service.
//!
//! Routes, query parameters and status codes match the C# sample; see `samples/inventory/README.md`.
//! Each command loads the item's state stream, decides the event with the pure
//! [`InventoryItem`] rules, and saves it with the caller's `version` as the expected version. The
//! save publishes the event to the `inventory` topic in the same transaction.
//!
//! The C# sample's `RepositoryHelper` is not ported: `Add` saves with expected version 0 on a fresh
//! root, so a duplicate id is a 409. The other commands save with the caller's `version`, not the
//! version they loaded, as C# does.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use inventory_domain::events::wire_type;
use inventory_domain::{AGGREGATE_NAME, DomainError, InventoryEvent, InventoryItem};
use redisevents::producer::{PublishOptions, StreamStore};
use redisevents_eventsourcing::{
    Aggregate, AggregateRoot, Error as StoreError, EventRepository, RedisEventRepository,
};
use redisevents_projections::EventTypeRegistry;
use serde::Deserialize;
use uuid::Uuid;

pub use inventory_domain::TOPIC;

/// The price `ExternalLogic.GetPrice` returns in the C# sample. Every check-in uses it.
pub const CHECK_IN_PRICE: f32 = 0.2;

/// The repository the service writes through.
pub type Repository = RedisEventRepository;

/// The `Inventory` aggregate: state and event from the domain crate. The marker lives here, so the
/// domain crate has no Redis or aggregate dependency.
pub struct Inventory;

impl Aggregate for Inventory {
    type State = InventoryItem;
    type Event = InventoryEvent;
    const NAME: &'static str = AGGREGATE_NAME;

    fn apply(state: &mut InventoryItem, event: &InventoryEvent) {
        state.apply(event);
    }

    // `Event` is an enum over five wire types, so the wire type comes from the variant.
    fn encode(
        _: &EventTypeRegistry,
        event: &InventoryEvent,
    ) -> redisevents_eventsourcing::Result<(String, Bytes)> {
        Ok((event.wire_type().to_owned(), Bytes::from(event.encode())))
    }
}

/// The wire registry: every inventory event type, decoded to [`InventoryEvent`].
///
/// # Panics
/// Never: the five wire types are distinct constants.
#[must_use]
pub fn registry() -> EventTypeRegistry {
    EventTypeRegistry::new()
        .register_family::<InventoryEvent, _>(
            &[
                wire_type::CREATED,
                wire_type::RENAMED,
                wire_type::CHECKED_IN,
                wire_type::REMOVED,
                wire_type::DEACTIVATED,
            ],
            InventoryEvent::decode,
        )
        .expect("the inventory wire types are distinct")
}

/// A repository over `store`, with the inventory registry.
#[must_use]
pub fn repository(store: StreamStore) -> Repository {
    RedisEventRepository::new(store, registry())
}

/// Builds the command service's router over `repository`.
pub fn router(repository: Repository) -> Router {
    Router::new()
        .route("/InventoryCommand/Add", post(add))
        .route("/InventoryCommand/ChangeName", post(change_name))
        .route("/InventoryCommand/Deactivate", post(deactivate))
        .route("/InventoryCommand/CheckIn", post(check_in))
        .route("/InventoryCommand/Remove", post(remove))
        .route("/InventoryCommand/IsAvailable", get(is_available))
        .with_state(Arc::new(repository))
}

#[derive(Deserialize)]
struct AddQuery {
    // Missing or empty is a domain error (400), as in C#, where a missing string binds to null.
    #[serde(default)]
    name: String,
    id: Option<Uuid>,
}

#[derive(Deserialize)]
struct ChangeNameQuery {
    id: Uuid,
    #[serde(default)]
    name: String,
    version: i64,
}

#[derive(Deserialize)]
struct ItemQuery {
    id: Uuid,
    version: i64,
}

#[derive(Deserialize)]
struct CountQuery {
    id: Uuid,
    number: i32,
    version: i64,
}

async fn add(State(repo): State<Arc<Repository>>, Query(q): Query<AddQuery>) -> Response {
    let id = q.id.unwrap_or_else(Uuid::new_v4);
    let event = match InventoryItem::create(id, &q.name) {
        Ok(event) => event,
        Err(e) => return refused(&e),
    };
    let mut item = match AggregateRoot::<Inventory>::new(id.to_string()) {
        Ok(item) => item,
        Err(e) => return failure(&e),
    };
    item.raise(event);
    // A fresh root at version 0: an id that already has history is a lost check.
    save(&repo, &mut item, 0).await
}

async fn change_name(
    State(repo): State<Arc<Repository>>,
    Query(q): Query<ChangeNameQuery>,
) -> Response {
    let mut item = match load(&repo, q.id).await {
        Ok(item) => item,
        Err(response) => return *response,
    };
    let event = match item.change_name(&q.name) {
        Ok(event) => event,
        Err(e) => return refused(&e),
    };
    item.raise(event);
    save(&repo, &mut item, expected(q.version)).await
}

async fn deactivate(State(repo): State<Arc<Repository>>, Query(q): Query<ItemQuery>) -> Response {
    let mut item = match load(&repo, q.id).await {
        Ok(item) => item,
        Err(response) => return *response,
    };
    let event = match item.deactivate() {
        Ok(event) => event,
        Err(e) => return refused(&e),
    };
    item.raise(event);
    save(&repo, &mut item, expected(q.version)).await
}

async fn check_in(State(repo): State<Arc<Repository>>, Query(q): Query<CountQuery>) -> Response {
    let mut item = match load(&repo, q.id).await {
        Ok(item) => item,
        Err(response) => return *response,
    };
    let event = match item.check_in(q.number, CHECK_IN_PRICE) {
        Ok(event) => event,
        Err(e) => return refused(&e),
    };
    item.raise(event);
    save(&repo, &mut item, expected(q.version)).await
}

async fn remove(State(repo): State<Arc<Repository>>, Query(q): Query<CountQuery>) -> Response {
    let mut item = match load(&repo, q.id).await {
        Ok(item) => item,
        Err(response) => return *response,
    };
    let event = match item.remove(q.number) {
        Ok(event) => event,
        Err(e) => return refused(&e),
    };
    item.raise(event);
    save(&repo, &mut item, expected(q.version)).await
}

async fn is_available() -> Json<bool> {
    Json(true)
}

/// Loads the item, or the 404 response when it has no history.
async fn load(repo: &Repository, id: Uuid) -> Result<AggregateRoot<Inventory>, Box<Response>> {
    match repo.load::<Inventory>(&id.to_string()).await {
        Ok(Some(item)) => Ok(item),
        Ok(None) => Err(Box::new(StatusCode::NOT_FOUND.into_response())),
        Err(e) => Err(Box::new(failure(&e))),
    }
}

/// Saves the raised events if the stream is still at `expected`: 204, or 409 on a lost check.
async fn save(repo: &Repository, item: &mut AggregateRoot<Inventory>, expected: u64) -> Response {
    match repo
        .save(item, Some(expected), &PublishOptions::default())
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => failure(&e),
    }
}

/// The caller's version as an expected stream length. A negative version can never match a stream
/// length, so it conflicts (409), as C#'s int version does.
fn expected(version: i64) -> u64 {
    u64::try_from(version).unwrap_or(u64::MAX)
}

/// A refused command (C# `ArgumentException` / `InvalidOperationException`): 400 with the message.
fn refused(error: &DomainError) -> Response {
    (StatusCode::BAD_REQUEST, error.to_string()).into_response()
}

/// A store failure: 409 for a lost version check, 500 otherwise.
fn failure(error: &StoreError) -> Response {
    match error {
        StoreError::Concurrency(_) => StatusCode::CONFLICT.into_response(),
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()).into_response(),
    }
}
