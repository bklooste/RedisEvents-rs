//! The inventory sample's domain plugged into the projections API: the shape the view service (E5c)
//! uses. In-memory stores always; one Redis-backed run when Redis is available.
#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unused_async,
    clippy::unused_async_trait_impl,
    clippy::assert_is_empty,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;

use inventory_domain::events::{
    InventoryItemCreated, InventoryItemDeactivated, InventoryItemRenamed,
    ItemsCheckedInToInventory, wire_type,
};
use inventory_domain::views::{
    InventoryItemDetailsDto, InventoryItemListDto, StreamPosition, ViewChange, project_detail,
    project_list,
};
use inventory_domain::{InventoryEvent, TOPIC};
use redisevents::testing::StreamTestHarness;
use redisevents::{HandleError, HandleResult, PartitionContext, StreamId, StreamMsg};
use redisevents_projections::{
    EventMeta, EventTypeRegistry, InMemoryViewStore, Projection, Projector, RedisViewStore,
    ViewStore,
};
use redisevents_test_support::require_redis;
use uuid::Uuid;

/// The view service's projection: both read models, one handler over the whole event enum.
struct InventoryViews<L, D> {
    list: Arc<L>,
    detail: Arc<D>,
}

async fn apply<T, S: ViewStore<T>>(store: &S, id: &str, change: ViewChange<T>) -> HandleResult {
    match change {
        ViewChange::Set(row) => store.set(id, &row).await,
        ViewChange::Delete => store.delete(id).await,
        ViewChange::Unchanged => Ok(()),
    }
    // A store failure must be retried, not skipped past: the view would silently diverge.
    .map_err(HandleError::block)
}

impl<L, D> Projection<InventoryEvent> for InventoryViews<L, D>
where
    L: ViewStore<InventoryItemListDto>,
    D: ViewStore<InventoryItemDetailsDto>,
{
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

        let list = self.list.get(&id).await.map_err(HandleError::block)?;
        apply(&*self.list, &id, project_list(list.as_ref(), event)).await?;
        let detail = self.detail.get(&id).await.map_err(HandleError::block)?;
        apply(
            &*self.detail,
            &id,
            project_detail(detail.as_ref(), event, position),
        )
        .await
    }
}

fn registry() -> EventTypeRegistry {
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
        .unwrap()
}

fn entry(event: &InventoryEvent, seq: u64) -> StreamMsg {
    StreamMsg::new(
        StreamId::new(1_000, seq),
        0,
        event.wire_type(),
        event.encode().into(),
    )
    .with_partition_key(event.id().to_string())
}

fn script(id: Uuid) -> Vec<StreamMsg> {
    let events = [
        InventoryEvent::Created(InventoryItemCreated {
            id,
            name: "widget".into(),
        }),
        InventoryEvent::CheckedIn(ItemsCheckedInToInventory { id, count: 10 }),
        InventoryEvent::Renamed(InventoryItemRenamed {
            id,
            new_name: "gadget".into(),
        }),
    ];
    events
        .iter()
        .zip(1..)
        .map(|(e, seq)| entry(e, seq))
        .collect()
}

#[tokio::test]
async fn the_inventory_domain_projects_both_views_and_ignores_redelivery() {
    let (list, detail) = (
        Arc::new(InMemoryViewStore::new()),
        Arc::new(InMemoryViewStore::new()),
    );
    let projector = Projector::builder(TOPIC, registry())
        .projection::<InventoryEvent, _>(InventoryViews {
            list: list.clone(),
            detail: detail.clone(),
        })
        .build()
        .unwrap();
    let harness = StreamTestHarness::new(TOPIC);
    let id = Uuid::from_u128(7);
    let batch = script(id);

    harness.deliver_batch(&projector, &batch).await.unwrap();
    harness.deliver_batch(&projector, &batch).await.unwrap(); // at-least-once: the same batch again

    let key = id.to_string();
    assert_eq!(list.get(&key).await.unwrap().unwrap().name, "gadget");
    let row = detail.get(&key).await.unwrap().unwrap();
    assert_eq!(
        (row.name.as_str(), row.current_count),
        ("gadget", 10),
        "the count is not applied twice"
    );

    let gone = InventoryEvent::Deactivated(InventoryItemDeactivated { id });
    harness
        .deliver_batch(&projector, &[entry(&gone, 4)])
        .await
        .unwrap();
    assert!(list.get(&key).await.unwrap().is_none() && detail.get(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn camel_case_event_bodies_from_a_wrong_producer_are_a_skipped_malformed_entry() {
    // The C# side reads {"id":..} as an empty guid with no error; here the entry is rejected by the
    // PascalCase contract and skipped without losing the rest of the batch.
    let (list, detail) = (
        Arc::new(InMemoryViewStore::new()),
        Arc::new(InMemoryViewStore::new()),
    );
    let projector = Projector::builder(TOPIC, registry())
        .projection::<InventoryEvent, _>(InventoryViews {
            list: list.clone(),
            detail,
        })
        .build()
        .unwrap();
    let id = Uuid::from_u128(8);
    let bad = StreamMsg::new(
        StreamId::new(1_000, 1),
        0,
        wire_type::CREATED,
        format!(r#"{{"id":"{id}","name":"x"}}"#).into(),
    );
    let good = entry(
        &InventoryEvent::Created(InventoryItemCreated {
            id,
            name: "ok".into(),
        }),
        2,
    );

    StreamTestHarness::new(TOPIC)
        .deliver_batch(&projector, &[bad, good])
        .await
        .unwrap();

    assert_eq!(list.get(&id.to_string()).await.unwrap().unwrap().name, "ok");
}

#[tokio::test]
async fn the_inventory_views_run_against_redis_view_stores() {
    let redis = require_redis!();
    let (conn, keys, topic) = (
        redis.connect().await,
        redis.keys(),
        redis.new_topic("inventory"),
    );
    let list = Arc::new(RedisViewStore::new(&conn, &keys, &topic, "views", "list").unwrap());
    let detail = Arc::new(RedisViewStore::new(&conn, &keys, &topic, "views", "detail").unwrap());
    let projector = Projector::builder(topic.as_str(), registry())
        .projection::<InventoryEvent, _>(InventoryViews {
            list: list.clone(),
            detail: detail.clone(),
        })
        .build()
        .unwrap();
    let id = Uuid::new_v4();

    StreamTestHarness::new(topic.as_str())
        .deliver_batch(&projector, &script(id))
        .await
        .unwrap();

    let all: Vec<InventoryItemListDto> = list.list().await.unwrap();
    assert_eq!(
        all,
        vec![InventoryItemListDto {
            id,
            name: "gadget".into()
        }]
    );
    let row = detail.get(&id.to_string()).await.unwrap().unwrap();
    assert_eq!(row.current_count, 10);
    // camelCase on the wire: this hash is what an HTTP client sees.
    let mut raw = redis.connect().await.shared();
    let json: String = redis::cmd("HGET")
        .arg(detail.key())
        .arg(id.to_string())
        .query_async(&mut raw)
        .await
        .unwrap();
    assert!(json.contains("\"currentCount\":10"), "{json}");
}
