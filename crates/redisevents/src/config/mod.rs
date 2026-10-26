//! Option structs for streams, topics, consumers and producers, plus their validation
//! ([`StreamOptions::validate`]), startup advice ([`StreamOptions::advisories`]) and instance
//! identity ([`instance`]).
//!
//! Every struct is a plain `#[derive(Deserialize)]` with `snake_case` fields and
//! `#[serde(default)]`, so it loads from any serde source (TOML, YAML, JSON, the `config` or
//! `figment` crates, environment variables through one of those) or is built in code from
//! [`Default`] plus struct-update syntax. Enum values are `snake_case` (`async_batch`,
//! `work_queue`). Nothing here reads the process environment except [`instance`], whose
//! environment fallbacks are documented there. Configuration never reaches Redis: only the
//! values that shape the wire (topic names, partitions, consumer names, trimming) do, and those
//! are written exactly as `wire-spec/WIRE.md` says.
//!
//! Durations are integer fields with the unit in the name (`_ms`, `_seconds`), which reads
//! unambiguously in every config format. Counts are unsigned, so a negative value is a
//! deserialize error.

pub mod instance;
mod validate;

use std::collections::HashMap;

use serde::Deserialize;
use time::OffsetDateTime;

pub use validate::Advisory;

/// The root of the configuration: the Redis endpoint, topics, consumers and producers.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default)]
pub struct StreamOptions {
    /// Redis URL (`redis://host:6379/0`, `rediss://` for TLS), read by
    /// [`Connection::from_options`](crate::Connection::from_options). `None` means
    /// [`StreamOptions::DEFAULT_REDIS_URL`].
    pub redis_url: Option<String>,
    /// Permit [`TrimMode::None`] on topics. Untrimmed streams grow until Redis runs out of
    /// memory, so validation refuses them unless this is set (local development, benchmarks).
    pub allow_untrimmed: bool,
    /// Default consumer name for every consumer that does not set its own.
    pub consumer: Option<String>,
    /// Per-topic settings, keyed by topic name.
    pub topics: HashMap<String, TopicOptions>,
    /// Consumers to run.
    pub consumers: Vec<ConsumerOptions>,
    /// Producers to create.
    pub producers: Vec<ProducerOptions>,
}

/// Per-topic settings.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
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
    /// The `MAXLEN` used when [`TopicOptions::max_len`] is not set.
    pub const DEFAULT_MAX_LEN: u64 = 10_000;

    /// The effective `MAXLEN`.
    #[must_use]
    pub fn max_len(&self) -> u64 {
        self.max_len.unwrap_or(Self::DEFAULT_MAX_LEN)
    }

    /// Whether `max_len` was set explicitly.
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

/// One consumer.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
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
    /// Health and ownership: a partition this instance reads that makes no progress for this
    /// long is stalled (default 300; `0` = off). Progress is a read that returned (empty reads
    /// count, so a quiet topic never stalls) or a batch the handler finished; a batch held longer
    /// than this, or position writes failing for this long, count as no progress. A stalled
    /// partition grades Unhealthy (rule `Stalled`), and in Lease mode the host also gives its lease
    /// up so another instance reads it. Set it longer than the slowest batch the handler can
    /// legitimately take.
    pub unhealthy_stalled_seconds: u32,
    /// Ownership: how long a contested partition is tolerated.
    pub contested_grace_seconds: u32,
    /// Ownership: how often a contested partition is rechecked.
    pub contested_recheck_seconds: u32,
    /// Delivery mode.
    pub delivery: DeliveryMode,
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
            unhealthy_stalled_seconds: 300,
            contested_grace_seconds: 60,
            contested_recheck_seconds: 30,
            delivery: DeliveryMode::Ordered,
            instances: None,
            reader_threads: 1,
            shutdown_timeout_seconds: 10,
            unhealthy_lag_ms: 120_000,
            start_from_when_missing: StartFrom::Beginning,
            reset_on_start: false,
        }
    }
}

/// Read-ahead between the read loop and the handler.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
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

/// How partitions are split between instances.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct InstanceOptions {
    /// How partitions are assigned.
    pub mode: InstanceMode,
    /// Static mode: instance count (else `STREAMS_INSTANCE_COUNT`, see [`instance`]).
    pub count: Option<u32>,
    /// Static mode: this instance's index (else `STREAMS_INSTANCE_INDEX` or the `StatefulSet`
    /// ordinal of the pod name, see [`instance`]).
    pub index: Option<u32>,
    /// This instance's pod name, used for its `StatefulSet` ordinal and in ownership logs. `None`
    /// falls back to the `POD_NAME` environment variable.
    pub pod_name: Option<String>,
    /// Lease mode: lease TTL.
    pub lease_ttl_seconds: u32,
    /// Lease mode: renew interval.
    pub lease_renew_seconds: u32,
}

impl InstanceOptions {
    /// Default lease TTL.
    pub const DEFAULT_LEASE_TTL_SECONDS: u32 = 30;
    /// Default lease renew interval.
    pub const DEFAULT_LEASE_RENEW_SECONDS: u32 = 10;
}

impl Default for InstanceOptions {
    fn default() -> Self {
        Self {
            mode: InstanceMode::Lease,
            count: None,
            index: None,
            pod_name: None,
            lease_ttl_seconds: Self::DEFAULT_LEASE_TTL_SECONDS,
            lease_renew_seconds: Self::DEFAULT_LEASE_RENEW_SECONDS,
        }
    }
}

/// One producer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
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

/// Where a consumer starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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

/// When positions are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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

/// What a partition does when a handler fails with an *ordinary* error. A
/// `Block` error always blocks and retries regardless of this policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorPolicy {
    /// Log, advance the position, skip the message. The handler owns retrying transients.
    #[default]
    BestEffort,
    /// Stop the partition; the others continue and the position does not advance.
    StopPartition,
    /// Fail the whole consumer process.
    Fail,
}

/// Blocking or polling reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReadMode {
    /// `XREAD BLOCK` on dedicated reader connections.
    #[default]
    Block,
    /// Non-blocking `XREAD` with idle back-off.
    Poll,
}

/// How partitions are assigned to instances.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum InstanceMode {
    /// Fixed index/count.
    Static,
    /// Leases in the ownership hash, claimed by the shared Lua.
    #[default]
    Lease,
}

/// Delivery semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DeliveryMode {
    /// Kafka-style: one owner per partition, in order.
    #[default]
    Ordered,
    /// `XREADGROUP` competing consumers (deferred, C6).
    WorkQueue,
}

/// How `XADD`/`XTRIM` trims.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TrimMode {
    /// `MAXLEN ~`.
    #[default]
    Approx,
    /// `MAXLEN` exact.
    Exact,
    /// No trimming.
    None,
}

impl StreamOptions {
    /// The URL used when [`StreamOptions::redis_url`] is not set.
    pub const DEFAULT_REDIS_URL: &'static str = "redis://127.0.0.1:6379/0";

    /// The effective Redis URL: [`StreamOptions::redis_url`] when set and not blank, else
    /// [`StreamOptions::DEFAULT_REDIS_URL`].
    #[must_use]
    pub fn redis_url(&self) -> &str {
        validate::non_blank(self.redis_url.as_deref()).unwrap_or(Self::DEFAULT_REDIS_URL)
    }

    /// The options for `topic`, or the defaults when it is not configured (legal;
    /// [`StreamOptions::advisories`] reports it).
    #[must_use]
    pub fn topic(&self, topic: &str) -> TopicOptions {
        self.topics.get(topic).cloned().unwrap_or_default()
    }

    /// The effective consumer name of `consumer`: its own `consumer`, else the root `consumer`,
    /// else the running executable's file stem.
    #[must_use]
    pub fn consumer_name(&self, consumer: &ConsumerOptions) -> String {
        validate::consumer_name(self, consumer)
    }

    /// Checks every rule and returns the first violation.
    ///
    /// # Errors
    ///
    /// [`Error::Config`](crate::Error::Config) naming the offending field by its path, such as
    /// `consumers[0].batch_size`.
    pub fn validate(&self) -> crate::Result<()> {
        validate::validate(self)
    }

    /// The advisory lines for a configuration that is valid but probably not what was meant
    /// (unconfigured topics, Block mode without co-located partitions). Call
    /// [`StreamOptions::validate`] first; this does not check for errors.
    #[must_use]
    pub fn advisories(&self) -> Vec<Advisory> {
        validate::advise(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_defaults() {
        let c = ConsumerOptions::default();
        assert_eq!(
            (c.batch_size, c.block_ms, c.persist_interval_ms),
            (100, 1000, 1000)
        );
        assert_eq!(c.delivery, DeliveryMode::Ordered);
        assert_eq!(InstanceOptions::default().mode, InstanceMode::Lease);
        assert_eq!(TopicOptions::default().max_len(), 10_000);
        assert_eq!(
            StreamOptions::default().redis_url(),
            "redis://127.0.0.1:6379/0"
        );
    }

    #[test]
    fn reads_snake_case_fields_and_values() {
        let c: ConsumerOptions = serde_json::from_str(
            r#"{"topic":"orders","batch_size":5,"delivery":"work_queue","persist":"sync_batch","start_from_date":"2026-09-01T00:00:00Z"}"#,
        )
        .unwrap();
        assert_eq!(c.topic, "orders");
        assert_eq!(c.batch_size, 5);
        assert_eq!(c.delivery, DeliveryMode::WorkQueue);
        assert_eq!(c.persist, PersistMode::SyncBatch);
        assert!(c.start_from_date.is_some());
        assert_eq!(c.block_ms, 1000);
    }

    #[test]
    fn a_whole_configuration_reads_from_nested_data() {
        let o: StreamOptions = serde_json::from_str(
            r#"{
              "redis_url": "redis://cache:6380/2",
              "consumer": "svc",
              "topics": { "Orders": { "partitions": 4, "trim": "exact", "max_len": 50 } },
              "consumers": [ { "topic": "Orders", "instances": { "mode": "static", "count": 2, "index": 1 } } ],
              "producers": [ { "topic": "Orders", "buffered": true } ]
            }"#,
        )
        .unwrap();
        assert_eq!(o.redis_url(), "redis://cache:6380/2");
        assert_eq!(o.topics["Orders"].trim, TrimMode::Exact);
        assert_eq!(o.topics["Orders"].max_len(), 50);
        let inst = o.consumers[0].instances.as_ref().unwrap();
        assert_eq!(
            (inst.mode, inst.count, inst.index),
            (InstanceMode::Static, Some(2), Some(1))
        );
        assert!(o.producers[0].buffered);
        o.validate().unwrap();
    }

    #[test]
    fn an_unknown_enum_value_is_an_error() {
        let err =
            serde_json::from_str::<ConsumerOptions>(r#"{"delivery":"WorkQueue"}"#).unwrap_err();
        assert!(err.to_string().contains("work_queue"), "{err}");
    }

    #[test]
    fn an_empty_document_yields_all_defaults() {
        let o: StreamOptions = serde_json::from_str("{}").unwrap();
        assert_eq!(o, StreamOptions::default());
    }
}
