//! The read side: two views, and how each event changes them.
//!
//! RedisEvents delivers at least once, so a projection can see the same event twice. The list
//! view is overwritten or deleted per id, so a repeat is harmless. The detail view keeps a running
//! count, so every handler that adds to it first checks the event's stream position against the
//! one stored in the view, and skips the event when it is not newer. Ordering uses the stream
//! position, not the aggregate version, because the stream is what orders delivery.
//!
//! The functions return a [`ViewChange`] instead of writing to a store. The view service does
//! the get-project-write against `redisevents-projections`, and the rules stay testable without
//! Redis.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::InventoryEvent;

/// A stream entry id (`<ms>-<seq>`), ordered the way Redis orders entries.
///
/// Serialized as `{"ms":…,"seq":…}`. The C# view service's `StreamId` also emits a computed
/// `timestamp`; nothing reads it, and the C# HTTP tests match only `id` and `name`.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct StreamPosition {
    /// Millisecond part.
    pub ms: u64,
    /// Sequence part.
    pub seq: u64,
}

/// One row of the item list. The JSON is camelCase because it is the HTTP response, which in
/// the C# service uses ASP.NET's web defaults. That differs from the PascalCase event bodies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InventoryItemListDto {
    /// The item.
    pub id: Uuid,
    /// Its current name.
    pub name: String,
}

/// The detail view of one item. camelCase for the same reason as [`InventoryItemListDto`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InventoryItemDetailsDto {
    /// The item.
    pub id: Uuid,
    /// Its current name.
    pub name: String,
    /// Stock on hand.
    pub current_count: i32,
    /// The last event applied to this view, used to ignore redeliveries.
    pub stream_position: StreamPosition,
}

/// What a projection does to one view row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewChange<T> {
    /// Write this row.
    Set(T),
    /// Delete the row.
    Delete,
    /// Leave the row as it is.
    Unchanged,
}

/// Projects an event onto the item list.
#[must_use]
pub fn project_list(
    current: Option<&InventoryItemListDto>,
    event: &InventoryEvent,
) -> ViewChange<InventoryItemListDto> {
    match event {
        InventoryEvent::Created(e) => ViewChange::Set(InventoryItemListDto {
            id: e.id,
            name: e.name.clone(),
        }),
        InventoryEvent::Renamed(e) => match current {
            Some(row) => ViewChange::Set(InventoryItemListDto {
                name: e.new_name.clone(),
                ..row.clone()
            }),
            None => ViewChange::Unchanged,
        },
        InventoryEvent::Deactivated(_) => ViewChange::Delete,
        InventoryEvent::CheckedIn(_) | InventoryEvent::Removed(_) => ViewChange::Unchanged,
    }
}

/// Projects an event, read from `position`, onto the item's detail view.
#[must_use]
pub fn project_detail(
    current: Option<&InventoryItemDetailsDto>,
    event: &InventoryEvent,
    position: StreamPosition,
) -> ViewChange<InventoryItemDetailsDto> {
    let update = |apply: &dyn Fn(&mut InventoryItemDetailsDto)| match current {
        Some(row) if row.stream_position < position => {
            let mut row = row.clone();
            apply(&mut row);
            row.stream_position = position;
            ViewChange::Set(row)
        }
        _ => ViewChange::Unchanged,
    };

    match event {
        InventoryEvent::Created(e) => ViewChange::Set(InventoryItemDetailsDto {
            id: e.id,
            name: e.name.clone(),
            current_count: 0,
            stream_position: position,
        }),
        InventoryEvent::Renamed(e) => update(&|row| row.name.clone_from(&e.new_name)),
        InventoryEvent::CheckedIn(e) => update(&|row| row.current_count += e.count),
        InventoryEvent::Removed(e) => update(&|row| row.current_count -= e.count),
        InventoryEvent::Deactivated(_) => ViewChange::Delete,
    }
}

#[cfg(test)]
mod tests {
    // Ports of SimpleCQRS.Views.Test/EventProjectorTests.cs.

    use super::*;
    use crate::events::{
        InventoryItemCreated, InventoryItemDeactivated, ItemsCheckedInToInventory,
    };

    const ID: Uuid = Uuid::from_u128(0x3f2b_8c1e_9a4d_4e6f_8b21_5c7d_9e0a_1b2c);

    fn at(seq: u64) -> StreamPosition {
        StreamPosition { ms: 0, seq }
    }

    fn detail(count: i32, position: StreamPosition) -> InventoryItemDetailsDto {
        InventoryItemDetailsDto {
            id: ID,
            name: "widget".into(),
            current_count: count,
            stream_position: position,
        }
    }

    fn checked_in(count: i32) -> InventoryEvent {
        InventoryEvent::CheckedIn(ItemsCheckedInToInventory { id: ID, count })
    }

    #[test]
    fn when_created_then_both_views_are_projected_to() {
        let event = InventoryEvent::Created(InventoryItemCreated {
            id: ID,
            name: "widget".into(),
        });

        assert_eq!(
            project_list(None, &event),
            ViewChange::Set(InventoryItemListDto {
                id: ID,
                name: "widget".into()
            })
        );
        assert_eq!(
            project_detail(None, &event, at(1)),
            ViewChange::Set(detail(0, at(1)))
        );
    }

    #[test]
    fn given_existing_item_when_receive_items_checked_in_then_expected_count_correct() {
        let change = project_detail(Some(&detail(10, at(1))), &checked_in(5), at(2));

        assert_eq!(change, ViewChange::Set(detail(15, at(2))));
    }

    #[test]
    fn given_stale_redelivery_when_receive_items_checked_in_then_count_is_not_applied_twice() {
        // Position 0-2 is already recorded, so a redelivery of the event at 0-2 must be ignored.
        let change = project_detail(Some(&detail(10, at(2))), &checked_in(5), at(2));

        assert_eq!(change, ViewChange::Unchanged);
    }

    #[test]
    fn when_deactivated_then_both_views_remove_the_item() {
        let event = InventoryEvent::Deactivated(InventoryItemDeactivated { id: ID });
        let row = InventoryItemListDto {
            id: ID,
            name: "widget".into(),
        };

        assert_eq!(project_list(Some(&row), &event), ViewChange::Delete);
        assert_eq!(
            project_detail(Some(&detail(0, at(1))), &event, at(2)),
            ViewChange::Delete
        );
    }

    #[test]
    fn positions_order_by_ms_then_seq() {
        assert!(StreamPosition { ms: 1, seq: 0 } > StreamPosition { ms: 0, seq: 99 });
    }
}
