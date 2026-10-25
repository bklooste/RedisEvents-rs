//! Option structs mirroring the C# `Streams` configuration section. **Types and defaults: W0.2.
//! Binding, validation, instance resolution, the advisor and connection-string translation: A2**
//! (`binder.rs`, `instance.rs`, `advisor.rs`).
//!
//! Field names are the C# property names in `snake_case`; serde reads the C# `PascalCase` keys, so
//! one config file per environment serves both languages. Time spans keep the C# unit suffixes
//! (`_ms`, `_seconds`) rather than becoming `Duration`s, for the same reason. Counts are unsigned:
//! a negative value in a config file is a deserialize error rather than a validation message.
//!
//! Where C# tracks "was this set explicitly" (`Delivery`, `Instances.Mode`, `MaxLen`), the Rust
//! field is an `Option` and an accessor applies the default.

pub mod advisor;
pub mod binder;
pub mod instance;

use std::collections::HashMap;

use serde::Deserialize;
use time::OffsetDateTime;

/// The root `Streams` section (C# `StreamOptions`).
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct StreamOptions {
    /// Connection string, C# StackExchange.Redis form (`host:port,abortConnect=false`). A2
    /// translates it to a redis-rs URL. `None` means the C# default `redis-db.infra:6379`.
    pub connection_string: Option<String>,
    /// Default consumer name for every consumer that does not set its own.
    pub consumer: Option<String>,
    /// Per-topic settings, keyed by topic name.
    pub topics: HashMap<String, TopicOptions>,
    /// Consumers to run.
    pub consumers: Vec<ConsumerOptions>,
    /// Producers to create.
    pub producers: Vec<ProducerOptions>,
}

/// Per-topic settings (C# `TopicOptions`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct TopicOptions {
    /// Partition count. May grow, never shrink (`WIRE.md` §3).
    pub partitions: u32,
    /// Approximate per-partition `MAXLEN`. `None` = not configured ([`TopicOptions::DEFAULT_MAX_LEN`]).
    pub max_len: Option<u64>,
    /// How `XADD` trims.
    pub trim: TrimMode,
    /// Time-based retention for the background trimmer, if any.
    pub retention_seconds: Option<u32>,
    /// Background trim interval; `0` = off.
    pub background_trim_interval_seconds: u32,
    /// Put the topic in a `{…}` hash tag so all its keys share a cluster slot.
    pub co_locate_partitions: bool,
    /// Write `c`/`p`/`h` on state entries too.
    pub state_metadata: bool,
    /// Fraction of `MAXLEN` at which a clamped trim is released.
    pub clamp_release_threshold: f64,
}

impl TopicOptions {
    /// The C# `TopicOptions.DefaultMaxLen`.
    pub const DEFAULT_MAX_LEN: u64 = 10_000;

    /// The effective `MAXLEN`.
    #[must_use]
    pub fn max_len(&self) -> u64 {
        self.max_len.unwrap_or(Self::DEFAULT_MAX_LEN)
    }

    /// Whether `MaxLen` was set explicitly.
    #[must_use]
    pub fn max_len_configured(&self) -> bool {
        self.max_len.is_some()
    }
}

impl Default for TopicOptions {
    fn default() -> Self {
        Self {
            partitions: 1,
            max_len: None,
            trim: TrimMode::Approx,
            retention_seconds: None,
            background_trim_interval_seconds: 0,
            co_locate_partitions: true,
            state_metadata: false,
            clamp_release_threshold: 0.8,
        }
    }
}

/// One consumer (C# `ConsumerOptions`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct ConsumerOptions {
    /// The topic to consume. Required.
    pub topic: String,
    /// Consumer name (positions and ownership are per consumer). Falls back to
    /// [`StreamOptions::consumer`].
    pub consumer: Option<String>,
    /// Most entries per read and per handler batch.
    pub batch_size: u32,
    /// Deliver only these message types; `None` = all.
    pub filter: Option<Vec<String>>,
    /// Where to start.
    pub start_from: StartFrom,
    /// The instant for [`StartFrom::Date`], ISO-8601.
    #[serde(with = "time::serde::rfc3339::option")]
    pub start_from_date: Option<OffsetDateTime>,
    /// When positions are written.
    pub persist: PersistMode,
    /// Flush interval for [`PersistMode::AsyncBatch`].
    pub persist_interval_ms: u32,
    /// Read-ahead between the read loop and the handler.
    pub backpressure: BackpressureOptions,
    /// Blocking or polling reads.
    pub read_mode: ReadMode,
    /// `XREAD BLOCK` timeout.
    pub block_ms: u32,
    /// Longest idle back-off in [`ReadMode::Poll`].
    pub max_idle_delay_ms: u32,
    /// What a partition does when a handler fails with an ordinary (`Skip`) error.
    pub on_error: ErrorPolicy,
    /// Health: a partition blocked this long is unhealthy.
    pub unhealthy_block_seconds: u32,
    /// Health: a partition stopped this long is unhealthy.
    pub unhealthy_stopped_seconds: u32,
    /// Health: behind the tail this long is unhealthy; `0` = off.
    pub unhealthy_behind_seconds: u32,
    /// Health: silent this long is degraded; `0` = off.
    pub degraded_silence_seconds: u32,
    /// Ownership: how long a contested partition is tolerated.
    pub contested_grace_seconds: u32,
    /// Ownership: how often a contested partition is rechecked.
    pub contested_recheck_seconds: u32,
    /// Delivery mode. `None` = not set ([`DeliveryMode::Ordered`]); see [`ConsumerOptions::delivery`].
    pub delivery: Option<DeliveryMode>,
    /// Legacy alias: `UseConsumerGroup = true` means `Delivery = WorkQueue`. A2 maps and warns.
    pub use_consumer_group: Option<bool>,
    /// Instance assignment; `None` = defaults.
    pub instances: Option<InstanceOptions>,
    /// Number of reader connections shared by this consumer's partitions.
    pub reader_threads: u32,
    /// Graceful shutdown budget.
    pub shutdown_timeout_seconds: u32,
    /// Health: lag beyond this is unhealthy.
    pub unhealthy_lag_ms: u32,
    /// Fallback when [`StartFrom::Stored`] finds no position. Must not itself be `Stored`.
    pub start_from_when_missing: StartFrom,
    /// Rewind to the start position on every start.
    pub reset_on_start: bool,
}

impl ConsumerOptions {
    /// The effective delivery mode (explicit, else [`DeliveryMode::Ordered`]; A2 folds the legacy
    /// `use_consumer_group` into `delivery` while binding).
    #[must_use]
    pub fn delivery(&self) -> DeliveryMode {
        self.delivery.unwrap_or_default()
    }
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            topic: String::new(),
            consumer: None,
            batch_size: 100,
            filter: None,
            start_from: StartFrom::Stored,
            start_from_date: None,
            persist: PersistMode::AsyncBatch,
            persist_interval_ms: 1000,
            backpressure: BackpressureOptions::default(),
            read_mode: ReadMode::Block,
            block_ms: 1000,
            max_idle_delay_ms: 50,
            on_error: ErrorPolicy::BestEffort,
            unhealthy_block_seconds: 300,
            unhealthy_stopped_seconds: 300,
            unhealthy_behind_seconds: 0,
            degraded_silence_seconds: 0,
            contested_grace_seconds: 60,
            contested_recheck_seconds: 30,
            delivery: None,
            use_consumer_group: None,
            instances: None,
            reader_threads: 1,
            shutdown_timeout_seconds: 10,
            unhealthy_lag_ms: 120_000,
            start_from_when_missing: StartFrom::Beginning,
            reset_on_start: false,
        }
    }
}

/// Read-ahead between the read loop and the handler (C# `BackpressureOptions`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct BackpressureOptions {
    /// Read ahead into a bounded channel; `false` = read inline.
    pub enabled: bool,
    /// Channel capacity, in batches.
    pub capacity: u32,
}

impl Default for BackpressureOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            capacity: 4,
        }
    }
}

/// How partitions are split between instances (C# `InstanceOptions`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct InstanceOptions {
    /// `None` = not set ([`InstanceMode::Lease`]); see [`InstanceOptions::mode`].
    pub mode: Option<InstanceMode>,
    /// Static mode: instance count (else `STREAMS_INSTANCE_COUNT`).
    pub count: Option<u32>,
    /// Static mode: this instance's index (else `STREAMS_INSTANCE_INDEX` or the `StatefulSet` ordinal).
    pub index: Option<u32>,
    /// Lease mode: lease TTL.
    pub lease_ttl_seconds: u32,
    /// Lease mode: renew interval.
    pub lease_renew_seconds: u32,
}

impl InstanceOptions {
    /// The C# `DefaultLeaseTtlSeconds`.
    pub const DEFAULT_LEASE_TTL_SECONDS: u32 = 30;
    /// The C# `DefaultLeaseRenewSeconds`.
    pub const DEFAULT_LEASE_RENEW_SECONDS: u32 = 10;

    /// The effective mode.
    #[must_use]
    pub fn mode(&self) -> InstanceMode {
        self.mode.unwrap_or_default()
    }
}

impl Default for InstanceOptions {
    fn default() -> Self {
        Self {
            mode: None,
            count: None,
            index: None,
            lease_ttl_seconds: Self::DEFAULT_LEASE_TTL_SECONDS,
            lease_renew_seconds: Self::DEFAULT_LEASE_RENEW_SECONDS,
        }
    }
}

/// One producer (C# `ProducerOptions`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct ProducerOptions {
    /// The topic to publish to. Required.
    pub topic: String,
    /// Use the buffered publisher.
    pub buffered: bool,
    /// Buffered: longest a message waits before a flush.
    pub max_wait_ms: u32,
    /// Buffered: most messages per flush.
    pub max_batch: u32,
    /// Buffered: queue capacity.
    pub max_queue: u32,
    /// Buffered: when full, drop the oldest instead of waiting.
    pub drop_oldest: bool,
}

impl Default for ProducerOptions {
    fn default() -> Self {
        Self {
            topic: String::new(),
            buffered: false,
            max_wait_ms: 20,
            max_batch: 500,
            max_queue: 100_000,
            drop_oldest: false,
        }
    }
}

/// Where a consumer starts (C# `StartFrom`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
pub enum StartFrom {
    /// The stored position, else [`ConsumerOptions::start_from_when_missing`].
    #[default]
    Stored,
    /// The start of the stream.
    Beginning,
    /// Only entries added after start.
    Now,
    /// From [`ConsumerOptions::start_from_date`].
    Date,
}

/// When positions are written (C# `PersistMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
pub enum PersistMode {
    /// A background flusher writes them every `persist_interval_ms`.
    #[default]
    AsyncBatch,
    /// After every batch, before the next read.
    SyncBatch,
    /// After every message.
    SyncMessage,
    /// Never.
    None,
}

/// What a partition does when a handler fails with an *ordinary* error (C# `ErrorPolicy`). A
/// `Block` error always blocks and retries regardless of this policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
pub enum ErrorPolicy {
    /// Log, advance the position, skip the message. The handler owns retrying transients.
    #[default]
    BestEffort,
    /// Stop the partition; the others continue and the position does not advance.
    StopPartition,
    /// Fail the whole consumer process.
    Fail,
}

/// Blocking or polling reads (C# `ReadMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
pub enum ReadMode {
    /// `XREAD BLOCK` on dedicated reader connections.
    #[default]
    Block,
    /// Non-blocking `XREAD` with idle back-off.
    Poll,
}

/// How partitions are assigned to instances (C# `InstanceMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
pub enum InstanceMode {
    /// Fixed index/count.
    Static,
    /// Leases in the ownership hash, claimed by the shared Lua.
    #[default]
    Lease,
}

/// Delivery semantics (C# `DeliveryMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
pub enum DeliveryMode {
    /// Kafka-style: one owner per partition, in order.
    #[default]
    Ordered,
    /// `XREADGROUP` competing consumers (deferred, C6).
    WorkQueue,
}

/// How `XADD`/`XTRIM` trims (C# `TrimMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
pub enum TrimMode {
    /// `MAXLEN ~`.
    #[default]
    Approx,
    /// `MAXLEN` exact.
    Exact,
    /// No trimming.
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_csharp() {
        let c = ConsumerOptions::default();
        assert_eq!(
            (c.batch_size, c.block_ms, c.persist_interval_ms),
            (100, 1000, 1000)
        );
        assert_eq!(c.delivery(), DeliveryMode::Ordered);
        assert_eq!(InstanceOptions::default().mode(), InstanceMode::Lease);
        assert_eq!(TopicOptions::default().max_len(), 10_000);
    }

    #[test]
    fn reads_pascal_case_keys() {
        let c: ConsumerOptions = serde_json::from_str(
            r#"{"Topic":"orders","BatchSize":5,"Delivery":"WorkQueue","StartFromDate":"2026-09-01T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(c.topic, "orders");
        assert_eq!(c.batch_size, 5);
        assert_eq!(c.delivery(), DeliveryMode::WorkQueue);
        assert!(c.start_from_date.is_some());
        assert_eq!(c.block_ms, 1000);
    }
}
