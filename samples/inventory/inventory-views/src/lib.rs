//! The read side of the Inventory sample: a projection over the `inventory` topic that keeps two
//! views in Redis (`list` and `detail`), and the axum routes that serve them. The binary in
//! `main.rs` wires these to a consumer host and an HTTP listener.
//!
//! Both views live in Redis, unlike the C# service's in-memory list, so a restart does not lose
//! the list while the consumer position says the events were already applied.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use inventory_domain::InventoryEvent;
use inventory_domain::events::wire_type;
use inventory_domain::views::{
    InventoryItemDetailsDto, InventoryItemListDto, StreamPosition, ViewChange, project_detail,
    project_list,
};
use redisevents::consumer::{HandleError, HandleResult, PartitionContext};
use redisevents::wire::Keys;
use redisevents::{Connection, Result};
use redisevents_projections::{
    EventMeta, EventTypeRegistry, Projection, Projector, RedisViewStore, ViewStore,
};
use uuid::Uuid;

/// The consumer's owner name: it namespaces the view hashes and the consumer position. It does not
/// need to match the C# service, which is a different service with its own views.
pub const OWNER: &str = "inventory-views";

/// The consumer name the host registers under (its position hash).
pub const CONSUMER: &str = "inventory-views";

/// Both read models, shared by the projection (writes) and the routes (reads).
#[derive(Clone)]
pub struct Views {
    list: Arc<RedisViewStore<InventoryItemListDto>>,
    detail: Arc<RedisViewStore<InventoryItemDetailsDto>>,
}

impl Views {
    /// The `list` and `detail` views over `topic`, in Redis.
    ///
    /// # Errors
    /// [`redisevents::Error::InvalidArgument`] for a blank topic.
    pub fn new(conn: &Connection, keys: &Keys, topic: &str) -> Result<Self> {
        Ok(Self {
            list: Arc::new(RedisViewStore::new(conn, keys, topic, OWNER, "list")?),
            detail: Arc::new(RedisViewStore::new(conn, keys, topic, OWNER, "detail")?),
        })
    }
}

/// The inventory event family: every wire type of the topic decodes into [`InventoryEvent`].
///
/// # Errors
/// [`redisevents::Error::Config`] if a wire type is registered twice (not possible here).
pub fn registry() -> Result<EventTypeRegistry> {
    EventTypeRegistry::new().register_family::<InventoryEvent, _>(
        &[
            wire_type::CREATED,
            wire_type::RENAMED,
            wire_type::CHECKED_IN,
            wire_type::REMOVED,
            wire_type::DEACTIVATED,
        ],
        InventoryEvent::decode,
    )
}

/// The projector for `topic`: one projection over the whole event enum, writing both views.
///
/// # Errors
/// [`redisevents::Error::Config`] when the registry cannot decode the bound event type.
pub fn projector(views: &Views, topic: &str) -> Result<Projector> {
    Projector::builder(topic, registry()?)
        .projection::<InventoryEvent, _>(InventoryViews {
            views: views.clone(),
        })
        .build()
}

struct InventoryViews {
    views: Views,
}

/// Applies one view change. A store failure is retried (`block`), never skipped, so the view
/// cannot silently diverge from the stream.
async fn apply<T, S: ViewStore<T>>(store: &S, id: &str, change: ViewChange<T>) -> HandleResult {
    match change {
        ViewChange::Set(row) => store.set(id, &row).await,
        ViewChange::Delete => store.delete(id).await,
        ViewChange::Unchanged => Ok(()),
    }
    .map_err(HandleError::block)
}

impl Projection<InventoryEvent> for InventoryViews {
    async fn handle(
        &self,
        event: &InventoryEvent,
        meta: &EventMeta,
        _: &PartitionContext,
    ) -> HandleResult {
        let id = event.id().to_string();
        let position = StreamPosition {
            ms: meta.id.ms,
            seq: meta.id.seq,
        };

        let list = self.views.list.get(&id).await.map_err(HandleError::block)?;
        apply(&*self.views.list, &id, project_list(list.as_ref(), event)).await?;

        let detail = self
            .views
            .detail
            .get(&id)
            .await
            .map_err(HandleError::block)?;
        apply(
            &*self.views.detail,
            &id,
            project_detail(detail.as_ref(), event, position),
        )
        .await
    }
}

/// The HTTP routes of the read side, the same paths as the C# service.
pub fn router(views: &Views) -> Router {
    Router::new()
        .route("/items/", get(list_items))
        .route("/items/{id}", get(get_item))
        .with_state(views.clone())
}

async fn list_items(State(views): State<Views>) -> Result<Json<Vec<InventoryItemListDto>>, Failed> {
    Ok(Json(views.list.list().await?))
}

async fn get_item(State(views): State<Views>, Path(id): Path<Uuid>) -> Result<Response, Failed> {
    Ok(match views.detail.get(&id.to_string()).await? {
        Some(item) => Json(item).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    })
}

/// A read that failed in Redis: logged, answered with 500.
struct Failed(redisevents::Error);

impl From<redisevents::Error> for Failed {
    fn from(error: redisevents::Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for Failed {
    fn into_response(self) -> Response {
        tracing::error!(error = %self.0, "view read failed");
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}
