//! The inventory item aggregate: the write side's business rules.

use std::fmt;

use uuid::Uuid;

use crate::events::{
    InventoryEvent, InventoryItemCreated, InventoryItemDeactivated, InventoryItemRenamed,
    ItemsCheckedInToInventory, ItemsRemovedFromInventory,
};

/// A command was refused. Both kinds map to HTTP 400 in the command service, the same as the C#
/// sample's `ArgumentException` and `InvalidOperationException`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    /// A command argument is invalid (C# `ArgumentException`).
    InvalidArgument(String),
    /// The command is not allowed in the item's current state (C# `InvalidOperationException`).
    InvalidOperation(String),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) | Self::InvalidOperation(message) => {
                f.write_str(message)
            }
        }
    }
}

impl std::error::Error for DomainError {}

/// The state an inventory item needs to make decisions, rebuilt by applying its events in
/// order.
///
/// Commands take `&self` and return the event they raise without changing anything. The caller
/// applies the event and saves it. That keeps the rules pure and leaves the event-store wiring to
/// the command service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InventoryItem {
    id: Uuid,
    activated: bool,
}

impl InventoryItem {
    /// The item's id. It is the nil UUID until a `Created` event has been applied.
    #[must_use]
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Whether the item is still active.
    #[must_use]
    pub fn is_activated(&self) -> bool {
        self.activated
    }

    /// Creates a new item.
    ///
    /// # Errors
    /// [`DomainError::InvalidArgument`] when `name` is empty.
    pub fn create(id: Uuid, name: &str) -> Result<InventoryEvent, DomainError> {
        if name.is_empty() {
            return Err(DomainError::InvalidArgument("name is not valid".into()));
        }
        Ok(InventoryEvent::Created(InventoryItemCreated {
            id,
            name: name.into(),
        }))
    }

    /// Renames the item.
    ///
    /// # Errors
    /// [`DomainError::InvalidArgument`] when `new_name` is empty.
    pub fn change_name(&self, new_name: &str) -> Result<InventoryEvent, DomainError> {
        if new_name.is_empty() {
            return Err(DomainError::InvalidArgument("newName is not valid".into()));
        }
        Ok(InventoryEvent::Renamed(InventoryItemRenamed {
            id: self.id,
            new_name: new_name.into(),
        }))
    }

    /// Removes stock.
    ///
    /// # Errors
    /// [`DomainError::InvalidOperation`] when `count` is not positive.
    pub fn remove(&self, count: i32) -> Result<InventoryEvent, DomainError> {
        if count <= 0 {
            return Err(DomainError::InvalidOperation(
                "cant remove negative count from inventory".into(),
            ));
        }
        Ok(InventoryEvent::Removed(ItemsRemovedFromInventory {
            id: self.id,
            count,
        }))
    }

    /// Checks stock in at `price`. The price comes from an external service and is only used in
    /// the error message, as in the C# sample.
    ///
    /// # Errors
    /// [`DomainError::InvalidOperation`] when `count` is not positive.
    pub fn check_in(&self, count: i32, price: f32) -> Result<InventoryEvent, DomainError> {
        if count <= 0 {
            return Err(DomainError::InvalidOperation(format!(
                "must have a count greater than 0 to add to inventory pirce {price}"
            )));
        }
        Ok(InventoryEvent::CheckedIn(ItemsCheckedInToInventory {
            id: self.id,
            count,
        }))
    }

    /// Deactivates the item.
    ///
    /// # Errors
    /// [`DomainError::InvalidOperation`] when the item is already deactivated.
    pub fn deactivate(&self) -> Result<InventoryEvent, DomainError> {
        if !self.activated {
            return Err(DomainError::InvalidOperation("already deactivated".into()));
        }
        Ok(InventoryEvent::Deactivated(InventoryItemDeactivated {
            id: self.id,
        }))
    }

    /// Applies an event to the state. Events that do not change any state the rules need are
    /// accepted and ignored, as in the C# sample.
    pub fn apply(&mut self, event: &InventoryEvent) {
        match event {
            InventoryEvent::Created(e) => {
                self.id = e.id;
                self.activated = true;
            }
            InventoryEvent::Deactivated(_) => self.activated = false,
            InventoryEvent::Renamed(_)
            | InventoryEvent::CheckedIn(_)
            | InventoryEvent::Removed(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    // Ports of SimpleCQRS.API.Test/InventoryItemTests.cs, plus the rules it leaves untested.

    use super::*;

    const ID: Uuid = Uuid::from_u128(0x3f2b_8c1e_9a4d_4e6f_8b21_5c7d_9e0a_1b2c);

    fn created(name: &str) -> InventoryItem {
        let mut item = InventoryItem::default();
        item.apply(&InventoryItem::create(ID, name).unwrap());
        item
    }

    #[test]
    fn when_created_then_create_message_correct() {
        let event = InventoryItem::create(ID, "widget").unwrap();

        assert_eq!(
            event,
            InventoryEvent::Created(InventoryItemCreated {
                id: ID,
                name: "widget".into()
            })
        );
    }

    #[test]
    fn given_created_when_add_then_create_message_correct() {
        let item = created("widget");

        let event = item.check_in(5, 0.1).unwrap();

        assert_eq!(
            event,
            InventoryEvent::CheckedIn(ItemsCheckedInToInventory { id: ID, count: 5 })
        );
    }

    #[test]
    fn given_deactivated_when_dectivate_then_fails() {
        let mut item = created("widget");
        item.apply(&item.deactivate().unwrap());

        assert!(matches!(
            item.deactivate(),
            Err(DomainError::InvalidOperation(_))
        ));
    }

    #[test]
    fn when_created_with_empty_name_then_fails() {
        assert!(matches!(
            InventoryItem::create(ID, ""),
            Err(DomainError::InvalidArgument(_))
        ));
    }

    #[test]
    fn when_renamed_to_empty_then_fails() {
        assert!(matches!(
            created("widget").change_name(""),
            Err(DomainError::InvalidArgument(_))
        ));
    }

    #[test]
    fn when_check_in_or_remove_non_positive_count_then_fails() {
        let item = created("widget");

        for count in [0, -1] {
            assert!(matches!(
                item.check_in(count, 0.2),
                Err(DomainError::InvalidOperation(_))
            ));
            assert!(matches!(
                item.remove(count),
                Err(DomainError::InvalidOperation(_))
            ));
        }
    }

    #[test]
    fn events_raised_after_create_carry_the_item_id() {
        let item = created("widget");

        for event in [
            item.change_name("gadget").unwrap(),
            item.check_in(1, 0.2).unwrap(),
            item.remove(1).unwrap(),
            item.deactivate().unwrap(),
        ] {
            assert_eq!(event.id(), ID);
        }
    }
}
