//! Partition ownership (`WIRE.md` §8). **Public types: W0.2** (final). **Registry: B5**
//! (`registry.rs`, crate-private; port of `OwnershipRegistry.cs` with the shared Lua from
//! `wire-spec/lua/`).
//!
//! Hash `o:{topic}:<consumer>`: field `"<p>"` = `<instanceId>|<POD_NAME>` (the owner), field
//! `i:<instanceId>` = the same value (presence). Both carry a per-field TTL (`HEXPIRE`, Redis 7.4+).

pub mod registry;

use std::collections::BTreeMap;
use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::instance::InstanceResolver;

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
        format_owner(self.instance_id, &self.pod_name)
    }

    /// Parses a field value for `partition`.
    ///
    /// # Errors
    /// [`Error::Transport`](crate::Error::Transport) when the value is not `<guid>|<pod>`.
    pub fn parse(partition: u32, value: &str) -> crate::Result<Self> {
        let parsed = value
            .split_once(SEPARATOR)
            .and_then(|(id, pod)| parse_guid_d(id).map(|id| (id, pod)));
        match parsed {
            Some((instance_id, pod)) => Ok(Self {
                partition,
                instance_id,
                pod_name: pod.to_owned(),
            }),
            None => Err(crate::Error::transport(format!(
                "ownership field {partition} holds {value:?}, which is not <instanceId>|<podName>"
            ))),
        }
    }

    /// C# `PartitionOwner.Parse`, which never fails: a value that is not `<guid>|<pod>` becomes an
    /// owner with the nil id and the whole value as its pod name, so the partition still reads as
    /// owned (by somebody this instance cannot identify).
    pub(crate) fn parse_lenient(partition: u32, value: &str) -> Self {
        Self::parse(partition, value).unwrap_or_else(|_| Self {
            partition,
            instance_id: Uuid::nil(),
            pod_name: value.to_owned(),
        })
    }

    /// C# `PartitionOwner.ToString`: `pod(xxxxxxxx)`, or just the pod for an unidentified owner.
    pub(crate) fn label(&self) -> String {
        if self.instance_id.is_nil() {
            self.pod_name.clone()
        } else {
            let id = self.instance_id.hyphenated().to_string();
            format!("{}({})", self.pod_name, &id[..8])
        }
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
    match pod_name {
        Some(name) if !name.trim().is_empty() => derive_instance_id(name),
        _ => process_instance_id(),
    }
}

/// This process's instance id, resolved once from the environment (C# `StableInstanceId`).
#[must_use]
pub fn stable_instance_id() -> Uuid {
    static STABLE: OnceLock<Uuid> = OnceLock::new();
    *STABLE.get_or_init(|| resolve_instance_id(|name| std::env::var(name).ok()))
}

/// The per-process random id (C# `ProcessInstanceId`).
pub(crate) fn process_instance_id() -> Uuid {
    static PROCESS: OnceLock<Uuid> = OnceLock::new();
    *PROCESS.get_or_init(Uuid::new_v4)
}

/// C# `ResolveInstanceId`: `POD_NAME`, else `<MachineName>#<STREAMS_INSTANCE_INDEX>`, else the
/// process id. Like C#, the variables are tested for being non-empty, not trimmed.
pub(crate) fn resolve_instance_id(environment: impl Fn(&str) -> Option<String>) -> Uuid {
    if let Some(pod) = environment(InstanceResolver::POD_NAME_VARIABLE).filter(|v| !v.is_empty()) {
        return derive_instance_id(&pod);
    }
    if let Some(index) = environment(InstanceResolver::INDEX_VARIABLE).filter(|v| !v.is_empty()) {
        return derive_instance_id(&format!("{}#{index}", machine_name()));
    }
    process_instance_id()
}

/// C# `DeriveInstanceId`: SHA-256 of the UTF-8 name, variant bits forced, first 16 bytes read the
/// way `new Guid(ReadOnlySpan<byte>)` reads them (the first three fields little-endian).
pub(crate) fn derive_instance_id(name: &str) -> Uuid {
    let hash = Sha256::digest(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0F) | 0x80;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    Uuid::from_bytes_le(bytes)
}

/// .NET `Environment.MachineName` on Unix: the host name up to its first dot.
fn machine_name() -> String {
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_default();
    match host.split_once('.') {
        Some((short, _)) => short.to_owned(),
        None => host,
    }
}

const SEPARATOR: char = '|';

/// `<instanceId>|<podName>`.
pub(crate) fn format_owner(instance_id: Uuid, pod_name: &str) -> String {
    format!("{}{SEPARATOR}{pod_name}", instance_id.hyphenated())
}

/// .NET `Guid.TryParseExact(.., "D")`: exactly 36 chars, hyphens at 8/13/18/23, hex (any case).
fn parse_guid_d(text: &str) -> Option<Uuid> {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return None;
    }
    for (i, b) in bytes.iter().enumerate() {
        let ok = if matches!(i, 8 | 13 | 18 | 23) {
            *b == b'-'
        } else {
            b.is_ascii_hexdigit()
        };
        if !ok {
            return None;
        }
    }
    Uuid::try_parse(text).ok()
}

#[cfg(test)]
#[allow(clippy::assert_is_empty)]
mod tests {
    use super::*;

    #[test]
    fn owner_value_vectors_from_formats_json() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../wire-spec/vectors/formats.json"
        );
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let cases = json["owner_value"].as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let id = Uuid::parse_str(case["instanceId"].as_str().unwrap()).unwrap();
            let pod = case["pod"].as_str().unwrap();
            let value = case["value"].as_str().unwrap();
            let owner = PartitionOwner {
                partition: 3,
                instance_id: id,
                pod_name: pod.to_owned(),
            };
            assert_eq!(owner.format(), value);
            assert_eq!(PartitionOwner::parse(3, value).unwrap(), owner);
        }
    }

    #[test]
    fn parse_keeps_everything_after_the_first_separator_and_rejects_non_d_guids() {
        let id = "3F2B8C1E-9A4D-4E6F-8B21-5C7D9E0A1B2C";
        let owner = PartitionOwner::parse(1, &format!("{id}|a|b")).unwrap();
        assert_eq!(owner.pod_name, "a|b");
        assert_eq!(owner.instance_id.to_string(), id.to_ascii_lowercase());
        for bad in [
            "",
            "pod-only",
            "|pod",
            "3f2b8c1e9a4d4e6f8b215c7d9e0a1b2c|pod",
            "{3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2c}|pod",
            "3f2b8c1e-9a4d-4e6f-8b21-5c7d9e0a1b2g|pod",
        ] {
            assert!(
                matches!(
                    PartitionOwner::parse(0, bad),
                    Err(crate::Error::Transport { .. })
                ),
                "{bad:?}"
            );
            let lenient = PartitionOwner::parse_lenient(0, bad);
            assert!(lenient.instance_id.is_nil());
            assert_eq!(lenient.pod_name, bad);
        }
    }

    #[test]
    fn instance_id_for_matches_csharp() {
        // Computed by C# `OwnershipRegistry.InstanceIdFor("svc-1")` (dotnet 10).
        assert_eq!(
            instance_id_for(Some("svc-1")).to_string(),
            "11c7db43-78ec-ec85-be43-a32aea5fd96e"
        );
    }

    #[test]
    fn instance_id_for_is_stable_for_one_pod_name_and_distinct_between_pods() {
        let first = instance_id_for(Some("svc-1"));
        assert_eq!(instance_id_for(Some("svc-1")), first);
        assert_ne!(instance_id_for(Some("svc-0")), first);
        assert_ne!(
            instance_id_for(Some("deploy-abc12-7f4dq")),
            instance_id_for(Some("deploy-abc12-x2k9p"))
        );
        assert!(!first.is_nil());
        assert_ne!(first, process_instance_id());
    }

    #[test]
    fn instance_id_for_with_no_name_falls_back_to_the_process_id() {
        assert_eq!(instance_id_for(None), process_instance_id());
        assert_eq!(instance_id_for(Some("   ")), process_instance_id());
    }

    #[test]
    fn resolve_instance_id_prefers_pod_name_then_index_then_the_process_id() {
        let from_pod = resolve_instance_id(|name| match name {
            "POD_NAME" => Some("svc-2".to_owned()),
            "STREAMS_INSTANCE_INDEX" => Some("9".to_owned()),
            _ => None,
        });
        assert_eq!(from_pod, instance_id_for(Some("svc-2")));
        let index = |name: &str| (name == "STREAMS_INSTANCE_INDEX").then(|| "9".to_owned());
        let from_index = resolve_instance_id(index);
        assert_ne!(from_index, from_pod);
        assert_ne!(from_index, process_instance_id());
        assert_eq!(from_index, resolve_instance_id(index));
        assert_eq!(resolve_instance_id(|_| None), process_instance_id());
    }

    #[test]
    fn admin_instance_id_is_nobodys_writer_identity() {
        let admin = crate::positions::ADMIN_INSTANCE_ID;
        assert_ne!(admin, stable_instance_id());
        assert_ne!(admin, process_instance_id());
        assert!(!admin.is_nil());
    }
}
