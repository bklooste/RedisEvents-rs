//! Instance identity and partition ownership (port of `Config/InstanceResolver.cs`). **Owner: A2.**
//!
//! Precedence is strict: explicit [`InstanceOptions`] config, then the `STREAMS_INSTANCE_COUNT` /
//! `STREAMS_INSTANCE_INDEX` environment variables, then the `StatefulSet` ordinal of `POD_NAME` (the
//! count still comes from the environment), and finally `Count = 1, Index = 0`.
//!
//! Ambient inputs are never fatal: a malformed variable falls through to the next level and logs.
//! Only explicit configuration throws.
//!
//! Ownership is a contiguous balanced range rather than a modulo stripe, so "pod 0 has the first
//! half" holds. The arithmetic lives in exactly one place.

use std::fmt::{self, Write as _};

use super::InstanceOptions;
use crate::error::{Error, Result};

/// Where a resolved instance `count` or `index` came from, in precedence order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InstanceSource {
    /// Explicit [`InstanceOptions`] configuration.
    Config,
    /// The `STREAMS_INSTANCE_COUNT` / `STREAMS_INSTANCE_INDEX` environment variables.
    Environment,
    /// The trailing `-<n>` ordinal of a `StatefulSet` `POD_NAME`.
    PodOrdinal,
    /// Nothing was configured: `Count = 1, Index = 0`, this instance owns everything.
    Fallback,
}

/// The resolved identity of this process within its instance pool.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct InstanceIdentity {
    /// Total instances in the pool (always `>= 1` from [`InstanceResolver::resolve`]).
    pub count: u32,
    /// This instance's zero-based index.
    pub index: u32,
    /// Where `count` came from.
    pub count_source: InstanceSource,
    /// Where `index` came from.
    pub index_source: InstanceSource,
    /// The observed `POD_NAME`, or `None` when unset.
    pub pod_name: Option<String>,
}

/// A contiguous range of partitions `[start, start + len)` owned by one instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct PartitionRange {
    /// First owned partition. Meaningless when `len` is zero.
    pub start: u32,
    /// Number of owned partitions; zero when this instance owns nothing.
    pub len: u32,
}

impl PartitionRange {
    /// Exclusive end of the range.
    #[must_use]
    pub const fn end(self) -> u32 {
        self.start + self.len
    }

    /// True when this instance owns no partitions.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len == 0
    }

    /// True when `partition` falls inside this range.
    #[must_use]
    pub const fn contains(self, partition: u32) -> bool {
        partition >= self.start && partition < self.end()
    }

    /// The owned partition ids, in ascending order.
    pub fn iter(self) -> impl Iterator<Item = u32> {
        self.start..self.end()
    }
}

impl fmt::Display for PartitionRange {
    /// Renders the range as `[0,1]`, or `[]` when nothing is owned.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (i, partition) in self.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{partition}")?;
        }
        f.write_str("]")
    }
}

/// Resolves instance identity and the partition range this instance owns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstanceResolver;

impl InstanceResolver {
    /// Environment variable naming the total number of instances.
    pub const COUNT_VARIABLE: &'static str = "STREAMS_INSTANCE_COUNT";
    /// Environment variable naming this instance's index.
    pub const INDEX_VARIABLE: &'static str = "STREAMS_INSTANCE_INDEX";
    /// Environment variable carrying the Kubernetes pod name (via the downward API).
    pub const POD_NAME_VARIABLE: &'static str = "POD_NAME";

    /// Resolves identity from `options` and the process environment.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when explicit configuration is self-contradictory.
    pub fn resolve_from_env(options: Option<&InstanceOptions>) -> Result<InstanceIdentity> {
        Self::resolve(options, |name| std::env::var(name).ok())
    }

    /// Resolves identity against a caller-supplied environment lookup, so tests need not mutate the
    /// process environment. The lookup returns `None` for unset names.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when explicit configuration is self-contradictory (`Count` of zero, or an
    /// `Index` not less than the configured `Count`).
    pub fn resolve(
        options: Option<&InstanceOptions>,
        environment: impl Fn(&str) -> Option<String>,
    ) -> Result<InstanceIdentity> {
        let pod_name = trimmed(environment(Self::POD_NAME_VARIABLE));

        let (count, count_source) = resolve_count(options, &environment)?;
        let (index, index_source) = resolve_index(options, &environment, pod_name.as_deref());

        // Explicit config is the one place a contradiction is a human error we can name.
        if count_source == InstanceSource::Config
            && index_source == InstanceSource::Config
            && index >= count
        {
            return Err(Error::config(format!(
                "Streams:Instances is invalid: Index {index} must be less than Count {count}. \
                 Indexes are zero-based, so a Count of 2 permits indexes 0 and 1."
            )));
        }

        Ok(InstanceIdentity {
            count,
            index,
            count_source,
            index_source,
            pod_name,
        })
    }

    /// The contiguous balanced range of partitions owned by `index` out of `count`. Remainder
    /// partitions go to the earlier instances, so the ranges tile `[0, partitions)` with no gap and
    /// no overlap. An index at or beyond `count` owns nothing (a drifted pool).
    ///
    /// # Panics
    ///
    /// Panics when `count` is zero.
    #[must_use]
    pub fn owned(partitions: u32, count: u32, index: u32) -> PartitionRange {
        assert!(count >= 1, "instance count must be at least 1");

        // A pod ordinal past the declared count owns nothing. Its partitions belong to nobody, which
        // is the scale-up drift the ownership registry exists to surface.
        if index >= count {
            return PartitionRange {
                start: partitions,
                len: 0,
            };
        }

        let per = partitions / count;
        let rem = partitions % count;
        PartitionRange {
            start: (index * per) + index.min(rem),
            len: per + u32::from(index < rem),
        }
    }

    /// [`InstanceResolver::owned`] for a resolved identity. Logs when this instance is beyond the
    /// declared count (its partitions are consumed by nobody) or when partitions are fewer than
    /// instances (surplus instances idle).
    #[must_use]
    pub fn owned_for(partitions: u32, identity: &InstanceIdentity) -> PartitionRange {
        let range = Self::owned(partitions, identity.count.max(1), identity.index);

        if identity.index >= identity.count {
            tracing::error!(
                index = identity.index,
                count = identity.count,
                pod = identity.pod_name.as_deref().unwrap_or("(unset)"),
                "Streams: instance index {} is beyond instance count {} (pod {}); this instance owns no partitions and \
                 partitions assigned to it are consumed by nobody. Check that {} matches spec.replicas.",
                identity.index,
                identity.count,
                identity.pod_name.as_deref().unwrap_or("(unset)"),
                Self::COUNT_VARIABLE
            );
        } else if partitions < identity.count {
            tracing::warn!(
                partitions,
                count = identity.count,
                "Streams: {partitions} partitions across {} instances leaves instances {partitions}..{} idle; surplus \
                 instances own nothing. Reduce replicas or raise Partitions.",
                identity.count,
                identity.count - 1
            );
        }

        range
    }

    /// Parses the `StatefulSet` ordinal from a pod name: the digits after the final `-`. Never fails.
    /// A missing, empty or non-ordinal name (a Deployment's `svc-7d4f8b-x2k9p`, say) returns `None`,
    /// so the caller falls through to the next precedence level.
    #[must_use]
    pub fn try_parse_pod_ordinal(pod_name: Option<&str>) -> Option<u32> {
        let name = pod_name?.trim();
        let dash = name.rfind('-')?;

        // Require something before the dash and something after it.
        if dash == 0 || dash == name.len() - 1 {
            return None;
        }

        // Only digits: rejects signs, separators and whitespace (`svc-+3`, `svc- 3`). Overflow is
        // a parse failure, not a wrap.
        let suffix = &name[dash + 1..];
        if !suffix.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        suffix.parse().ok()
    }

    /// The instance index that owns `partition` under the contiguous balanced rule.
    ///
    /// # Panics
    ///
    /// Panics when `partition >= partitions` or `count` is zero.
    #[must_use]
    pub fn owner_of(partition: u32, partitions: u32, count: u32) -> u32 {
        assert!(
            partition < partitions,
            "partition {partition} is outside 0..{partitions}"
        );
        assert!(count >= 1, "instance count must be at least 1");

        let per = partitions / count;
        let rem = partitions % count;

        // The first `rem` instances hold `per + 1` partitions each; the rest hold `per`.
        let fat = (per + 1) * rem;
        if partition < fat {
            partition / (per + 1)
        } else {
            rem + ((partition - fat) / per)
        }
    }

    /// The startup ownership line, e.g.
    /// `instance 0/2 (pod svc-0) owns partitions [0,1] of 4 | full map: 0->svc-0 1->svc-0 2->svc-1 3->svc-1`.
    #[must_use]
    pub fn describe_ownership(partitions: u32, identity: &InstanceIdentity) -> String {
        let count = identity.count.max(1);
        let range = Self::owned(partitions, count, identity.index);

        let mut line = format!(
            "instance {}/{} (pod {}) owns partitions {range} of {partitions} | full map:",
            identity.index,
            identity.count,
            identity.pod_name.as_deref().unwrap_or("unknown"),
        );
        for partition in 0..partitions {
            let owner = Self::owner_of(partition, partitions, count);
            let _ = write!(line, " {partition}->{}", pod_label(identity, owner));
        }
        line
    }
}

fn resolve_count(
    options: Option<&InstanceOptions>,
    environment: &impl Fn(&str) -> Option<String>,
) -> Result<(u32, InstanceSource)> {
    if let Some(configured) = options.and_then(|o| o.count) {
        if configured < 1 {
            return Err(Error::config(format!(
                "Streams:Instances:Count is {configured}; it must be at least 1."
            )));
        }
        return Ok((configured, InstanceSource::Config));
    }

    if let Some(raw) = trimmed(environment(InstanceResolver::COUNT_VARIABLE)) {
        match raw.parse::<u32>() {
            Ok(parsed) if parsed >= 1 => return Ok((parsed, InstanceSource::Environment)),
            _ => tracing::warn!(
                "Streams: {} is {raw}, which is not a positive integer; falling back to a single instance owning every partition.",
                InstanceResolver::COUNT_VARIABLE
            ),
        }
    }

    Ok((1, InstanceSource::Fallback))
}

fn resolve_index(
    options: Option<&InstanceOptions>,
    environment: &impl Fn(&str) -> Option<String>,
    pod_name: Option<&str>,
) -> (u32, InstanceSource) {
    if let Some(configured) = options.and_then(|o| o.index) {
        return (configured, InstanceSource::Config);
    }

    if let Some(raw) = trimmed(environment(InstanceResolver::INDEX_VARIABLE)) {
        if let Ok(parsed) = raw.parse::<u32>() {
            return (parsed, InstanceSource::Environment);
        }
        tracing::warn!(
            "Streams: {} is {raw}, which is not a non-negative integer; falling back to the pod ordinal.",
            InstanceResolver::INDEX_VARIABLE
        );
    }

    if let Some(ordinal) = InstanceResolver::try_parse_pod_ordinal(pod_name) {
        return (ordinal, InstanceSource::PodOrdinal);
    }

    if let Some(pod) = pod_name {
        tracing::info!(
            "Streams: pod name {pod} has no trailing StatefulSet ordinal, so instance index falls back to 0. \
             Partitioned consumers must run as a StatefulSet to keep ownership stable across restarts."
        );
    }

    (0, InstanceSource::Fallback)
}

/// The pod name to print for an instance index: derived from this pod's own name when it carries an
/// ordinal (`svc-0` gives `svc-1`), our own name for our own index, else `i<n>`.
fn pod_label(identity: &InstanceIdentity, index: u32) -> String {
    if let Some(pod) = identity.pod_name.as_deref().filter(|p| !p.is_empty()) {
        if index == identity.index {
            return pod.to_owned();
        }
        if InstanceResolver::try_parse_pod_ordinal(Some(pod)).is_some()
            && let Some(dash) = pod.rfind('-')
        {
            return format!("{}{index}", &pod[..=dash]);
        }
    }
    format!("i{index}")
}

fn trimmed(value: Option<String>) -> Option<String> {
    value.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::InstanceOptions;

    /// An environment lookup backed by pairs, so no process state is mutated.
    fn env(entries: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        let map: Vec<(String, String)> = entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |key: &str| map.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn identity(count: u32, index: u32, pod: &str) -> InstanceIdentity {
        InstanceIdentity {
            count,
            index,
            count_source: InstanceSource::Environment,
            index_source: InstanceSource::PodOrdinal,
            pod_name: Some(pod.to_owned()),
        }
    }

    const TOPOLOGIES: [(u32, u32); 7] = [(1, 1), (8, 1), (8, 4), (8, 3), (4, 2), (5, 2), (2, 4)];

    // ---------------------------------------------------------------- ownership tiling

    #[test]
    fn owned_tiles_partition_space_no_gaps_no_overlaps() {
        for (partitions, count) in TOPOLOGIES {
            let mut owner: Vec<Option<u32>> = vec![None; partitions as usize];

            for index in 0..count {
                let range = InstanceResolver::owned(partitions, count, index);
                assert_eq!(range.iter().count(), range.len as usize);

                for partition in range.iter() {
                    assert!(partition < partitions);
                    assert!(
                        owner[partition as usize].is_none(),
                        "partition {partition} must be owned by exactly one instance, but {:?} and {index} both claim it",
                        owner[partition as usize]
                    );
                    owner[partition as usize] = Some(index);
                    assert!(range.contains(partition));
                }
            }

            assert!(
                owner.iter().all(Option::is_some),
                "every partition must be owned by someone"
            );
        }
    }

    #[test]
    fn owned_is_balanced_to_within_one_partition() {
        for (partitions, count) in TOPOLOGIES {
            let lengths: Vec<u32> = (0..count)
                .map(|i| InstanceResolver::owned(partitions, count, i).len)
                .collect();

            assert_eq!(lengths.iter().sum::<u32>(), partitions);
            assert!(lengths.iter().max().unwrap() - lengths.iter().min().unwrap() <= 1);
        }
    }

    #[test]
    fn owner_of_agrees_with_owned() {
        for (partitions, count) in TOPOLOGIES {
            for partition in 0..partitions {
                let owner = InstanceResolver::owner_of(partition, partitions, count);

                assert!(owner < count);
                assert!(InstanceResolver::owned(partitions, count, owner).contains(partition));
            }
        }
    }

    #[test]
    fn owned_four_partitions_two_instances_splits_in_half() {
        assert_eq!(
            InstanceResolver::owned(4, 2, 0),
            PartitionRange { start: 0, len: 2 }
        );
        assert_eq!(
            InstanceResolver::owned(4, 2, 1),
            PartitionRange { start: 2, len: 2 }
        );

        assert_eq!(
            InstanceResolver::owned(4, 2, 0).iter().collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            InstanceResolver::owned(4, 2, 1).iter().collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn owned_five_partitions_two_instances_remainder_goes_to_the_earlier_instance() {
        assert_eq!(
            InstanceResolver::owned(5, 2, 0),
            PartitionRange { start: 0, len: 3 }
        );
        assert_eq!(
            InstanceResolver::owned(5, 2, 1),
            PartitionRange { start: 3, len: 2 }
        );

        assert_eq!(
            InstanceResolver::owned(5, 2, 0).iter().collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            InstanceResolver::owned(5, 2, 1).iter().collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn owned_surplus_instances_own_nothing() {
        assert!(InstanceResolver::owned(2, 4, 2).is_empty());
        assert!(InstanceResolver::owned(2, 4, 3).is_empty());
        assert_eq!(InstanceResolver::owned(2, 4, 3).iter().count(), 0);
        assert_eq!(InstanceResolver::owned(2, 4, 3).to_string(), "[]");
    }

    #[test]
    fn owned_index_beyond_count_owns_nothing() {
        let identity = identity(2, 5, "svc-5");

        assert!(InstanceResolver::owned_for(8, &identity).is_empty());
    }

    /// C# `Owned_RejectsInvalidArguments`. Negative partitions and indexes are unrepresentable
    /// (`u32`), so only the zero-count case remains.
    #[test]
    #[should_panic(expected = "at least 1")]
    fn owned_rejects_zero_count() {
        let _ = InstanceResolver::owned(4, 0, 0);
    }

    #[test]
    fn describe_ownership_names_every_partitions_pod() {
        let identity = identity(2, 0, "svc-0");

        let line = InstanceResolver::describe_ownership(4, &identity);

        assert_eq!(
            line,
            "instance 0/2 (pod svc-0) owns partitions [0,1] of 4 | full map: 0->svc-0 1->svc-0 2->svc-1 3->svc-1"
        );
    }

    // ---------------------------------------------------------------- pod ordinal parsing

    #[test]
    fn try_parse_pod_ordinal_parses_statefulset_names() {
        for (pod, expected) in [
            ("svc-0", 0),
            ("svc-12", 12),
            ("bet-processing-3", 3),
            ("svc--3", 3),
        ] {
            assert_eq!(
                InstanceResolver::try_parse_pod_ordinal(Some(pod)),
                Some(expected),
                "{pod}"
            );
        }
    }

    #[test]
    fn try_parse_pod_ordinal_falls_through_on_non_ordinal_names() {
        for pod in [
            Some("svc-abc"),
            None,
            Some(""),
            Some("   "),
            Some("svc"),
            Some("-0"),
            Some("svc-"),
            Some("svc-7d4f8b-x2k9p"),
            Some("svc- 3"),
            Some("svc-+3"),
            Some("svc-99999999999999999999"),
        ] {
            assert_eq!(
                InstanceResolver::try_parse_pod_ordinal(pod),
                None,
                "{pod:?}"
            );
        }
    }

    // ---------------------------------------------------------------- precedence

    #[test]
    fn resolve_config_beats_environment_and_pod_ordinal() {
        let options = InstanceOptions {
            count: Some(3),
            index: Some(2),
            ..InstanceOptions::default()
        };
        let identity = InstanceResolver::resolve(
            Some(&options),
            env(&[
                ("STREAMS_INSTANCE_COUNT", "8"),
                ("STREAMS_INSTANCE_INDEX", "7"),
                ("POD_NAME", "svc-5"),
            ]),
        )
        .unwrap();

        assert_eq!(identity.count, 3);
        assert_eq!(identity.index, 2);
        assert_eq!(identity.count_source, InstanceSource::Config);
        assert_eq!(identity.index_source, InstanceSource::Config);
        assert_eq!(identity.pod_name.as_deref(), Some("svc-5"));
    }

    #[test]
    fn resolve_environment_beats_pod_ordinal() {
        let identity = InstanceResolver::resolve(
            None,
            env(&[
                ("STREAMS_INSTANCE_COUNT", "8"),
                ("STREAMS_INSTANCE_INDEX", "7"),
                ("POD_NAME", "svc-5"),
            ]),
        )
        .unwrap();

        assert_eq!(identity.count, 8);
        assert_eq!(identity.index, 7);
        assert_eq!(identity.count_source, InstanceSource::Environment);
        assert_eq!(identity.index_source, InstanceSource::Environment);
    }

    #[test]
    fn resolve_pod_ordinal_beats_fallback() {
        let identity = InstanceResolver::resolve(
            None,
            env(&[("STREAMS_INSTANCE_COUNT", "4"), ("POD_NAME", "svc-2")]),
        )
        .unwrap();

        assert_eq!(identity.count, 4);
        assert_eq!(identity.index, 2);
        assert_eq!(identity.count_source, InstanceSource::Environment);
        assert_eq!(identity.index_source, InstanceSource::PodOrdinal);
        assert_eq!(identity.pod_name.as_deref(), Some("svc-2"));
    }

    #[test]
    fn resolve_nothing_configured_falls_back_to_single_instance_owning_everything() {
        let identity = InstanceResolver::resolve(None, no_env).unwrap();

        assert_eq!(identity.count, 1);
        assert_eq!(identity.index, 0);
        assert_eq!(identity.count_source, InstanceSource::Fallback);
        assert_eq!(identity.index_source, InstanceSource::Fallback);
        assert_eq!(identity.pod_name, None);

        assert_eq!(
            InstanceResolver::owned_for(8, &identity)
                .iter()
                .collect::<Vec<_>>(),
            (0..8).collect::<Vec<_>>()
        );
    }

    #[test]
    fn resolve_null_pod_name_falls_through_to_index_zero() {
        let identity =
            InstanceResolver::resolve(None, env(&[("STREAMS_INSTANCE_COUNT", "3")])).unwrap();

        assert_eq!(identity.index, 0);
        assert_eq!(identity.index_source, InstanceSource::Fallback);
        assert_eq!(identity.pod_name, None);
    }

    #[test]
    fn resolve_non_ordinal_pod_name_falls_through_to_index_zero() {
        let identity = InstanceResolver::resolve(None, env(&[("POD_NAME", "svc-abc")])).unwrap();

        assert_eq!(identity.index, 0);
        assert_eq!(identity.index_source, InstanceSource::Fallback);
        assert_eq!(identity.pod_name.as_deref(), Some("svc-abc"));
    }

    #[test]
    fn resolve_malformed_count_variable_is_not_fatal() {
        for raw in ["nonsense", "0", "-2"] {
            let identity =
                InstanceResolver::resolve(None, env(&[("STREAMS_INSTANCE_COUNT", raw)])).unwrap();

            assert_eq!(identity.count, 1, "{raw}");
            assert_eq!(identity.count_source, InstanceSource::Fallback, "{raw}");
        }
    }

    #[test]
    fn resolve_malformed_index_variable_falls_through_to_pod_ordinal() {
        let identity = InstanceResolver::resolve(
            None,
            env(&[
                ("STREAMS_INSTANCE_COUNT", "4"),
                ("STREAMS_INSTANCE_INDEX", "not-a-number"),
                ("POD_NAME", "svc-3"),
            ]),
        )
        .unwrap();

        assert_eq!(identity.index, 3);
        assert_eq!(identity.index_source, InstanceSource::PodOrdinal);
    }

    #[test]
    fn resolve_config_count_with_environment_index_mixes_sources() {
        let options = InstanceOptions {
            count: Some(2),
            ..InstanceOptions::default()
        };
        let identity = InstanceResolver::resolve(
            Some(&options),
            env(&[("STREAMS_INSTANCE_INDEX", "1"), ("POD_NAME", "svc-0")]),
        )
        .unwrap();

        assert_eq!(identity.count, 2);
        assert_eq!(identity.count_source, InstanceSource::Config);
        assert_eq!(identity.index, 1);
        assert_eq!(identity.index_source, InstanceSource::Environment);
    }

    #[test]
    fn resolve_contradictory_explicit_config_throws() {
        let options = InstanceOptions {
            count: Some(2),
            index: Some(2),
            ..InstanceOptions::default()
        };
        let err = InstanceResolver::resolve(Some(&options), no_env)
            .expect_err("index must be below count");

        assert!(err.to_string().contains("Streams:Instances"), "{err}");
    }

    #[test]
    fn resolve_zero_config_count_throws_naming_key() {
        let options = InstanceOptions {
            count: Some(0),
            ..InstanceOptions::default()
        };
        let err = InstanceResolver::resolve(Some(&options), no_env).expect_err("count of zero");

        assert!(err.to_string().contains("Streams:Instances:Count"), "{err}");
    }

    /// C# `Resolve_NegativeConfigValues_Throw`, Index half. A negative index is unrepresentable
    /// (`u32`); the equivalent is a deserialize error in the binder.
    #[test]
    fn resolve_negative_config_index_is_a_deserialize_error() {
        let err = crate::config::binder::bind_json(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "Instances": { "Index": -1 } } ] } }"#,
        );
        assert!(matches!(err, Err(Error::Config(_))));
    }

    #[test]
    fn resolve_drifted_pool_from_ambient_sources_does_not_throw() {
        // Scale-down drift: pod ordinal 5 survives while the declared count is 2. This must be
        // reported by the ownership registry, not crash the process at startup.
        let identity = InstanceResolver::resolve(
            None,
            env(&[("STREAMS_INSTANCE_COUNT", "2"), ("POD_NAME", "svc-5")]),
        )
        .unwrap();

        assert_eq!(identity.count, 2);
        assert_eq!(identity.index, 5);
        assert!(InstanceResolver::owned_for(8, &identity).is_empty());
    }

    #[test]
    fn resolve_trims_whitespace_around_ambient_values() {
        let identity = InstanceResolver::resolve(
            None,
            env(&[("STREAMS_INSTANCE_COUNT", " 4 "), ("POD_NAME", " svc-1 ")]),
        )
        .unwrap();

        assert_eq!(identity.count, 4);
        assert_eq!(identity.index, 1);
        assert_eq!(identity.pod_name.as_deref(), Some("svc-1"));
    }
}
