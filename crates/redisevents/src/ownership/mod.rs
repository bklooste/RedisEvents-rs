//! Partition ownership (`WIRE.md` §8). **Public types: W0.2** (final). **Registry: B5**
//! (`registry.rs`, crate-private; port of `OwnershipRegistry.cs` with the shared Lua from
//! `wire-spec/lua/`).
//!
//! Hash `o:{topic}:<consumer>`: field `"<p>"` = `<instanceId>|<POD_NAME>` (the owner), field
//! `i:<instanceId>` = the same value (presence). Both carry a per-field TTL (`HEXPIRE`, Redis 7.4+).

pub mod registry;

use std::collections::BTreeMap;

use uuid::Uuid;

/// The owner of one partition, as stored in the ownership hash (C# `PartitionOwner`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionOwner {
    /// The partition.
    pub partition: u32,
    /// The owning instance.
    pub instance_id: Uuid,
    /// The owner's `POD_NAME`, for operators.
    pub pod_name: String,
}

impl PartitionOwner {
    /// `<instanceId>|<podName>`, GUID in `"D"` form.
    #[must_use]
    pub fn format(&self) -> String {
        todo!("B5")
    }

    /// Parses a field value for `partition`.
    ///
    /// # Errors
    /// [`Error::Transport`](crate::Error::Transport) when the value is not `<guid>|<pod>`.
    pub fn parse(partition: u32, value: &str) -> crate::Result<Self> {
        let _ = (partition, value);
        todo!("B5")
    }
}

/// A partition this instance expected to own that someone else holds (C# `ContestedPartition`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContestedPartition {
    /// The partition.
    pub partition: u32,
    /// The instance id this side expected.
    pub expected: Uuid,
    /// Who actually holds it.
    pub actual: PartitionOwner,
}

/// A snapshot of a topic's ownership (C# `OwnershipMap`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnershipMap {
    /// Owners by partition.
    pub owners: BTreeMap<u32, PartitionOwner>,
    /// Partitions nobody holds.
    pub unowned: Vec<u32>,
    /// Partitions held by someone other than expected.
    pub contested: Vec<ContestedPartition>,
}

impl OwnershipMap {
    /// `true` when any partition is unowned or contested.
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        !self.unowned.is_empty() || !self.contested.is_empty()
    }
}

/// The instance id a pod named `pod_name` runs under: deterministic, so the same name yields the
/// same id in any process and in C#. A blank name yields the per-process random id
/// (C# `OwnershipRegistry.InstanceIdFor`).
#[must_use]
pub fn instance_id_for(pod_name: Option<&str>) -> Uuid {
    let _ = pod_name;
    todo!("B5")
}

/// This process's instance id, resolved once from the environment (C# `StableInstanceId`).
#[must_use]
pub fn stable_instance_id() -> Uuid {
    todo!("B5")
}
