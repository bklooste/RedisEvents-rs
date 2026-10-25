//! Inventory events and their wire encoding.
//!
//! The C# sample serializes events with System.Text.Json's default naming, so JSON property names
//! are **PascalCase** (`{"Id":…,"Count":7}`). Getting this wrong fails silently on the C# side: a
//! C# consumer reading `{"id":…,"count":7}` gets an empty GUID and a count of 0, with no error.
//! Every event struct here is therefore `rename_all = "PascalCase"`, and `tests/csharp_wire.rs`
//! pins the exact bytes the C# sample writes.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Wire type strings, registered by both services. These match the C# sample's
/// `InventoryEventTypes.Register` and must never change.
pub mod wire_type {
    /// [`super::InventoryItemCreated`].
    pub const CREATED: &str = "inventory.created";
    /// [`super::InventoryItemRenamed`].
    pub const RENAMED: &str = "inventory.renamed";
    /// [`super::ItemsCheckedInToInventory`].
    pub const CHECKED_IN: &str = "inventory.checked-in";
    /// [`super::ItemsRemovedFromInventory`].
    pub const REMOVED: &str = "inventory.removed";
    /// [`super::InventoryItemDeactivated`].
    pub const DEACTIVATED: &str = "inventory.deactivated";
}

/// An item was added to the inventory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InventoryItemCreated {
    /// The item.
    pub id: Uuid,
    /// Its initial name.
    pub name: String,
}

/// An item was renamed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InventoryItemRenamed {
    /// The item.
    pub id: Uuid,
    /// The new name.
    pub new_name: String,
}

/// Stock was checked in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ItemsCheckedInToInventory {
    /// The item.
    pub id: Uuid,
    /// How many were checked in.
    pub count: i32,
}

/// Stock was removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ItemsRemovedFromInventory {
    /// The item.
    pub id: Uuid,
    /// How many were removed.
    pub count: i32,
}

/// An item was deactivated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct InventoryItemDeactivated {
    /// The item.
    pub id: Uuid,
}

/// Every event on the `inventory` topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryEvent {
    /// See [`InventoryItemCreated`].
    Created(InventoryItemCreated),
    /// See [`InventoryItemRenamed`].
    Renamed(InventoryItemRenamed),
    /// See [`ItemsCheckedInToInventory`].
    CheckedIn(ItemsCheckedInToInventory),
    /// See [`ItemsRemovedFromInventory`].
    Removed(ItemsRemovedFromInventory),
    /// See [`InventoryItemDeactivated`].
    Deactivated(InventoryItemDeactivated),
}

impl InventoryEvent {
    /// The item this event belongs to, which is also its partition key.
    #[must_use]
    pub fn id(&self) -> Uuid {
        match self {
            Self::Created(e) => e.id,
            Self::Renamed(e) => e.id,
            Self::CheckedIn(e) => e.id,
            Self::Removed(e) => e.id,
            Self::Deactivated(e) => e.id,
        }
    }

    /// The wire type string written to the entry's `t` field.
    #[must_use]
    pub fn wire_type(&self) -> &'static str {
        match self {
            Self::Created(_) => wire_type::CREATED,
            Self::Renamed(_) => wire_type::RENAMED,
            Self::CheckedIn(_) => wire_type::CHECKED_IN,
            Self::Removed(_) => wire_type::REMOVED,
            Self::Deactivated(_) => wire_type::DEACTIVATED,
        }
    }

    /// Encodes the body written to the entry's `b` field.
    ///
    /// # Panics
    /// Never in practice: every event is plain data that always serializes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let body = match self {
            Self::Created(e) => serde_json::to_vec(e),
            Self::Renamed(e) => serde_json::to_vec(e),
            Self::CheckedIn(e) => serde_json::to_vec(e),
            Self::Removed(e) => serde_json::to_vec(e),
            Self::Deactivated(e) => serde_json::to_vec(e),
        };
        body.expect("inventory events always serialize")
    }

    /// Decodes an entry. Returns `Ok(None)` for a wire type this topic does not know, so the caller
    /// can skip it: a newer producer may add event types this build has never seen.
    ///
    /// # Errors
    /// The body is not valid JSON for its wire type, including JSON with the wrong property
    /// casing.
    pub fn decode(wire_type: &str, body: &[u8]) -> Result<Option<Self>, serde_json::Error> {
        Ok(Some(match wire_type {
            wire_type::CREATED => Self::Created(serde_json::from_slice(body)?),
            wire_type::RENAMED => Self::Renamed(serde_json::from_slice(body)?),
            wire_type::CHECKED_IN => Self::CheckedIn(serde_json::from_slice(body)?),
            wire_type::REMOVED => Self::Removed(serde_json::from_slice(body)?),
            wire_type::DEACTIVATED => Self::Deactivated(serde_json::from_slice(body)?),
            _ => return Ok(None),
        }))
    }
}
