//! Wire compatibility with the C# sample's events.
//!
//! The `CSHARP` bodies are the exact output of System.Text.Json for the C# sample's event types,
//! using the sample's settings (`IncludeFields = true`, default naming). They were generated from
//! `cqrs-simple-micro-service/redis/inventory-views/src/SimpleCQRS.Views/Events.cs`. Note that
//! C# escapes non-ASCII characters (`é`) where serde writes raw UTF-8. Both sides read either
//! form.

use inventory_domain::InventoryEvent;
use inventory_domain::events::{
    InventoryItemCreated, InventoryItemDeactivated, InventoryItemRenamed,
    ItemsCheckedInToInventory, ItemsRemovedFromInventory, wire_type,
};
use uuid::Uuid;

const ID: Uuid = Uuid::from_u128(0x3f2b_8c1e_9a4d_4e6f_8b21_5c7d_9e0a_1b2c);

const CSHARP: [(&str, &str); 5] = [
    (
        "inventory.created",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","Name":"Widget é ✓"}"#,
    ),
    (
        "inventory.renamed",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","NewName":"Gadget"}"#,
    ),
    (
        "inventory.checked-in",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","Count":7}"#,
    ),
    (
        "inventory.removed",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","Count":3}"#,
    ),
    (
        "inventory.deactivated",
        r#"{"Id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c"}"#,
    ),
];

fn expected() -> [InventoryEvent; 5] {
    [
        InventoryEvent::Created(InventoryItemCreated {
            id: ID,
            name: "Widget é ✓".into(),
        }),
        InventoryEvent::Renamed(InventoryItemRenamed {
            id: ID,
            new_name: "Gadget".into(),
        }),
        InventoryEvent::CheckedIn(ItemsCheckedInToInventory { id: ID, count: 7 }),
        InventoryEvent::Removed(ItemsRemovedFromInventory { id: ID, count: 3 }),
        InventoryEvent::Deactivated(InventoryItemDeactivated { id: ID }),
    ]
}

#[test]
fn decodes_every_event_the_csharp_sample_writes() {
    for ((wire, body), want) in CSHARP.into_iter().zip(expected()) {
        let got = InventoryEvent::decode(wire, body.as_bytes()).unwrap();
        assert_eq!(got, Some(want), "{wire}");
    }
}

#[test]
fn encodes_the_json_the_csharp_sample_reads() {
    for ((wire, body), event) in CSHARP.into_iter().zip(expected()) {
        assert_eq!(event.wire_type(), wire);

        let ours: serde_json::Value = serde_json::from_slice(&event.encode()).unwrap();
        let theirs: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(ours, theirs, "{wire}");
    }
}

#[test]
fn rejects_camel_case_bodies_instead_of_reading_defaults() {
    // System.Text.Json reads this as Id = Guid.Empty, Count = 0, without an error. The Rust side
    // must fail loudly so that a casing bug cannot hide behind a 0.
    let camel = br#"{"id":"3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c","count":7}"#;

    assert!(InventoryEvent::decode(wire_type::CHECKED_IN, camel).is_err());
}

#[test]
fn skips_wire_types_it_does_not_know() {
    assert_eq!(
        InventoryEvent::decode("inventory.repriced", b"{}").unwrap(),
        None
    );
}
