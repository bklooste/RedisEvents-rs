//! The ownership registry: Static and Lease modes, the renew loop, the `HEXPIRE` probe, contested
//! detection and release on shutdown. **Owner: B5.** Crate-private; C3, C4, D1 and D2 consume it.
//! Embeds `wire-spec/lua/lease.lua` and `release.lua` byte for byte (`include_str!`).
//!
//! Port of C# `OwnershipRegistry`. The interface C3/C4 consume (see `CONTRACTS.md` §5, B5):
//!
//! - [`OwnershipRegistry::new`] validates the options (errors are [`Error::Config`]).
//! - [`OwnershipRegistry::start`] probes `HEXPIRE` (refusing to run without it), claims once and
//!   spawns the renew loop. A failed first claim is logged, not returned, as in C#.
//! - [`OwnershipRegistry::subscribe`] / [`OwnershipRegistry::held`]: the partitions this instance
//!   holds, as a `tokio::sync::watch` of a sorted `Arc<[u32]>`. In Lease mode the value changes the
//!   moment a lease cycle returns a different set (before anything else in that cycle), so a worker
//!   that lost its partition mid-batch sees `changed()` fire within one renew interval. In Static
//!   mode it is the pre-assigned set and never changes.
//! - [`OwnershipRegistry::stop`] stops the loop (letting an in-flight cycle finish, so the release
//!   below covers everything claimed) and runs the compare-and-delete release. It must be awaited
//!   **before** the [`Connection`] goes away; dropping the registry only cancels the loop and leaves
//!   the claims to lapse at the TTL (what a `SIGKILL` leaves).
//! - [`OwnershipRegistry::dispose`] = `stop` + clearing the metrics and the process-wide snapshot.

// C3/C4 are the callers; until they land only the tests use this module.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use redis::aio::ConnectionManager;
use redis::{RedisError, Script};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{ContestedPartition, OwnershipMap, PartitionOwner, format_owner, stable_instance_id};
use crate::config::InstanceMode;
use crate::connection::Connection;
use crate::diagnostics::instruments;
use crate::error::{Error, Result};
use crate::wire::Keys;

/// `wire-spec/lua/lease.lua`, byte for byte.
pub(crate) const LEASE_SCRIPT: &str = include_str!("lua/lease.lua");
/// `wire-spec/lua/release.lua`, byte for byte.
pub(crate) const RELEASE_SCRIPT: &str = include_str!("lua/release.lua");

const PRESENCE_PREFIX: &str = "i:";
const PROBE_FIELD_PREFIX: &str = "__hexpire-probe:";

/// `EVALSHA`, falling back to `EVAL` on `NOSCRIPT` (redis-rs `Script`).
static LEASE: LazyLock<Script> = LazyLock::new(|| Script::new(LEASE_SCRIPT));
static RELEASE: LazyLock<Script> = LazyLock::new(|| Script::new(RELEASE_SCRIPT));

/// Latest map per `topic:consumer`, process-wide (C# `Observed`), for health checks.
static OBSERVED: LazyLock<Mutex<HashMap<String, OwnershipMap>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn observed() -> MutexGuard<'static, HashMap<String, OwnershipMap>> {
    OBSERVED.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The partitions an instance holds, sorted ascending.
pub(crate) type Held = Arc<[u32]>;

/// C# `OwnershipRegistryOptions`.
#[derive(Debug, Clone)]
pub(crate) struct OwnershipRegistryOptions {
    pub(crate) topic: String,
    pub(crate) consumer: String,
    pub(crate) partitions: u32,
    /// Static mode only: the pre-assigned partitions. Must be empty in Lease mode.
    pub(crate) owned_partitions: Vec<u32>,
    pub(crate) pod_name: String,
    pub(crate) ttl_seconds: u32,
    pub(crate) renew_seconds: u32,
    pub(crate) instance_id: Uuid,
    pub(crate) mode: InstanceMode,
}

impl OwnershipRegistryOptions {
    /// The C# defaults: Lease mode, TTL 30 s, renew 10 s, pod `unknown`, the stable instance id.
    pub(crate) fn new(
        topic: impl Into<String>,
        consumer: impl Into<String>,
        partitions: u32,
    ) -> Self {
        Self {
            topic: topic.into(),
            consumer: consumer.into(),
            partitions,
            owned_partitions: Vec::new(),
            pod_name: "unknown".to_owned(),
            ttl_seconds: 30,
            renew_seconds: 10,
            instance_id: stable_instance_id(),
            mode: InstanceMode::Lease,
        }
    }
}

/// The ownership registry for one topic and consumer.
pub(crate) struct OwnershipRegistry {
    shared: Arc<Shared>,
    running: tokio::sync::Mutex<Option<Running>>,
}

struct Running {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

struct Shared {
    conn: ConnectionManager,
    options: OwnershipRegistryOptions,
    key: String,
    owner_value: String,
    presence: String,
    /// Static mode: owned partitions as field names, sorted.
    fields: Vec<String>,
    lease: bool,
    metric_key: String,
    held: watch::Sender<Held>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    latest: Option<OwnershipMap>,
    described: bool,
    last_renewed: Option<Instant>,
}

impl OwnershipRegistry {
    /// Validates `options` (C# constructor).
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic or consumer, [`Error::Config`] for impossible
    /// timings, zero partitions or pre-assigned partitions in Lease mode.
    pub(crate) fn new(
        conn: &Connection,
        keys: &Keys,
        options: OwnershipRegistryOptions,
    ) -> Result<Self> {
        if options.topic.trim().is_empty() {
            return Err(Error::invalid_argument(
                "ownership registry topic must not be blank",
            ));
        }
        if options.consumer.trim().is_empty() {
            return Err(Error::invalid_argument(
                "ownership registry consumer must not be blank",
            ));
        }
        if options.partitions < 1 {
            return Err(Error::config(format!(
                "Ownership registry for topic '{}' was given {} partitions; it must be at least 1.",
                options.topic, options.partitions
            )));
        }
        if options.ttl_seconds < 1 {
            return Err(Error::config(format!(
                "Streams:Instances:LeaseTtlSeconds is {}; it must be at least 1 second.",
                options.ttl_seconds
            )));
        }
        if options.renew_seconds < 1 || options.renew_seconds >= options.ttl_seconds {
            return Err(Error::config(format!(
                "Streams:Instances:LeaseRenewSeconds is {} against a TTL of {}s; the renew interval must be at least 1 second and shorter than the TTL, or claims expire between renewals.",
                options.renew_seconds, options.ttl_seconds
            )));
        }
        let lease = options.mode == InstanceMode::Lease;
        if lease && !options.owned_partitions.is_empty() {
            return Err(Error::config(format!(
                "Ownership registry for topic '{}' is in Lease mode but was handed {} pre-assigned partitions. In Lease mode the registry decides what this instance owns by claiming free fields; an assignment computed from Instances:Count / Instances:Index is exactly what the mode exists to replace.",
                options.topic,
                options.owned_partitions.len()
            )));
        }

        let mut mine = options.owned_partitions.clone();
        mine.sort_unstable();
        mine.dedup();
        let initial: Held = if lease {
            Arc::from([])
        } else {
            Arc::from(mine.clone())
        };

        let shared = Shared {
            conn: conn.shared(),
            key: keys.ownership(&options.topic, &options.consumer),
            owner_value: format_owner(options.instance_id, &options.pod_name),
            presence: presence_field(options.instance_id),
            fields: mine.iter().map(u32::to_string).collect(),
            lease,
            metric_key: format!("{}:{}", options.topic, options.consumer),
            held: watch::Sender::new(initial),
            state: Mutex::new(State::default()),
            options,
        };
        Ok(Self {
            shared: Arc::new(shared),
            running: tokio::sync::Mutex::new(None),
        })
    }

    /// This instance's id.
    pub(crate) fn instance_id(&self) -> Uuid {
        self.shared.options.instance_id
    }

    /// The partitions held now (C# `Held`).
    pub(crate) fn held(&self) -> Held {
        self.shared.held.borrow().clone()
    }

    /// A receiver of the held set; see the module docs.
    pub(crate) fn subscribe(&self) -> watch::Receiver<Held> {
        self.shared.held.subscribe()
    }

    /// The last map this registry computed (C# `Latest`).
    pub(crate) fn latest(&self) -> Option<OwnershipMap> {
        self.shared.state().latest.clone()
    }

    /// `true` when any live registry in this process last saw a degraded map (C# `AnyDegraded`).
    pub(crate) fn any_degraded() -> bool {
        observed().values().any(OwnershipMap::is_degraded)
    }

    /// The last map seen for `topic` / `consumer` in this process (C# `TryGetSnapshot`).
    pub(crate) fn snapshot(topic: &str, consumer: &str) -> Option<OwnershipMap> {
        observed().get(&format!("{topic}:{consumer}")).cloned()
    }

    /// Probes `HEXPIRE`, claims once and starts the renew loop (C# `StartAsync`). Idempotent.
    ///
    /// # Errors
    /// [`Error::Config`] when the server cannot expire hash fields. Nothing is left running then.
    pub(crate) async fn start(&self) -> Result<()> {
        let mut running = self.running.lock().await;
        if running.is_some() {
            return Ok(());
        }
        self.shared.probe().await?;

        if let Err(e) = self.shared.refresh().await {
            tracing::error!(
                instance = %self.instance_id(),
                error = %e,
                "Streams: could not claim partition ownership for topic {} consumer {} at startup; ownership drift will go undetected until the next renew.",
                self.shared.options.topic,
                self.shared.options.consumer
            );
        }

        let cancel = CancellationToken::new();
        let task = tokio::spawn(renew_loop(Arc::clone(&self.shared), cancel.clone()));
        *running = Some(Running { cancel, task });
        Ok(())
    }

    /// Stops the renew loop and releases what this instance still holds (C# `StopAsync`). Does
    /// nothing if the registry was never started.
    pub(crate) async fn stop(&self) {
        let Some(running) = self.running.lock().await.take() else {
            return;
        };
        running.cancel.cancel();
        let _ = running.task.await;
        self.shared.release().await;
    }

    /// [`stop`](Self::stop), then clears the gauges and the process-wide snapshot (C# `DisposeAsync`).
    pub(crate) async fn dispose(&self) {
        self.stop().await;
        instruments::set_unowned_partitions(&self.shared.metric_key, 0);
        instruments::set_contested_partitions(&self.shared.metric_key, 0);
        observed().remove(&self.shared.metric_key);
    }

    /// Runs one cycle now (C# `RefreshAsync`).
    ///
    /// # Errors
    /// The Redis failure; the renew loop logs and swallows these.
    pub(crate) async fn refresh(&self) -> Result<OwnershipMap> {
        self.shared.refresh().await
    }
}

impl Drop for OwnershipRegistry {
    fn drop(&mut self) {
        if let Some(running) = self.running.get_mut().take() {
            running.cancel.cancel();
        }
    }
}

async fn renew_loop(shared: Arc<Shared>, cancel: CancellationToken) {
    let period = Duration::from_secs(u64::from(shared.options.renew_seconds));
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            _ = timer.tick() => {}
        }
        // Not raced against `cancel`: a cycle that has sent its script finishes, so `held` (and so
        // the release on stop) covers every field it claimed.
        if let Err(e) = shared.refresh().await {
            tracing::warn!(
                instance = %shared.options.instance_id,
                error = %e,
                "Streams: ownership renew failed for topic {} consumer {}; retrying in {}s. Claims expire after {}s.",
                shared.options.topic,
                shared.options.consumer,
                shared.options.renew_seconds,
                shared.options.ttl_seconds
            );
            shared.check_lapsed();
        }
    }
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn refresh(&self) -> Result<OwnershipMap> {
        if self.lease {
            self.refresh_lease().await
        } else {
            self.refresh_static().await
        }
    }

    async fn refresh_lease(&self) -> Result<OwnershipMap> {
        let mut conn = self.conn.clone();
        let mut claimed: Vec<u32> = LEASE
            .key(&self.key)
            .arg(&self.owner_value)
            .arg(self.options.ttl_seconds)
            .arg(self.options.partitions)
            .arg(&self.presence)
            .invoke_async(&mut conn)
            .await?;
        claimed.sort_unstable();
        self.state().last_renewed = Some(Instant::now());

        // Published before anything else in the cycle, so a lost partition is signalled at once.
        let previous = self.held.borrow().clone();
        let changed = previous[..] != claimed[..];
        if changed {
            self.held.send_replace(Arc::from(claimed.clone()));
        }

        let after = hgetall(&mut conn, &self.key).await?;
        let owners = read_owners(&after);
        let members = count_members(&after);
        let unowned: Vec<u32> = (0..self.options.partitions)
            .filter(|p| !owners.contains_key(p))
            .collect();
        let map = OwnershipMap {
            owners,
            unowned,
            contested: Vec::new(),
        };
        self.publish(&map);
        self.report_lease(&map, &previous, &claimed, members, changed);
        Ok(map)
    }

    async fn refresh_static(&self) -> Result<OwnershipMap> {
        let mut conn = self.conn.clone();
        // Read first: HSET overwrites, so a competing claim is only visible before we stamp ours.
        let before = hgetall(&mut conn, &self.key).await?;
        if !self.fields.is_empty() {
            let mut hset = redis::cmd("HSET");
            hset.arg(&self.key);
            for f in self.leased_fields() {
                hset.arg(f).arg(&self.owner_value);
            }
            let mut hexpire = redis::cmd("HEXPIRE");
            hexpire
                .arg(&self.key)
                .arg(self.options.ttl_seconds)
                .arg("FIELDS")
                .arg(self.fields.len() + 1);
            for f in self.leased_fields() {
                hexpire.arg(f);
            }
            redis::pipe()
                .add_command(hset)
                .ignore()
                .add_command(hexpire)
                .ignore()
                .query_async::<()>(&mut conn)
                .await?;
        }
        let after = hgetall(&mut conn, &self.key).await?;
        let map = self.evaluate(&before, &after);
        self.publish(&map);
        self.report_static(&map);
        Ok(map)
    }

    fn leased_fields(&self) -> impl Iterator<Item = &str> {
        self.fields
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(self.presence.as_str()))
    }

    fn publish(&self, map: &OwnershipMap) {
        self.state().latest = Some(map.clone());
        observed().insert(self.metric_key.clone(), map.clone());
        instruments::set_unowned_partitions(&self.metric_key, len_i64(map.unowned.len()));
        instruments::set_contested_partitions(&self.metric_key, len_i64(map.contested.len()));
    }

    /// A Lease-mode renew failed: once the last good renew is a whole TTL old, every lease may
    /// have passed to someone else, so stop claiming them. (C# keeps `Held` until the next good
    /// cycle; this is the one deliberate deviation, so a worker cut off from Redis stands down.)
    fn check_lapsed(&self) {
        if !self.lease {
            return;
        }
        let ttl = Duration::from_secs(u64::from(self.options.ttl_seconds));
        let lapsed = self
            .state()
            .last_renewed
            .is_some_and(|at| at.elapsed() >= ttl);
        if lapsed && !self.held.borrow().is_empty() {
            tracing::warn!(
                instance = %self.options.instance_id,
                "Streams: no lease renewal for topic {} consumer {} within the {}s TTL; standing down partitions {} until the next successful cycle.",
                self.options.topic,
                self.options.consumer,
                self.options.ttl_seconds,
                describe_set(&self.held.borrow())
            );
            self.held.send_replace(Arc::from([]));
        }
    }

    async fn probe(&self) -> Result<()> {
        // Lease mode always probes; Static only when it writes anything.
        if !self.lease && self.fields.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.clone();
        let probe = format!(
            "{PROBE_FIELD_PREFIX}{}",
            self.options.instance_id.hyphenated()
        );
        let outcome: std::result::Result<Vec<i64>, RedisError> = async {
            redis::cmd("HSET")
                .arg(&self.key)
                .arg(&probe)
                .arg("1")
                .query_async::<()>(&mut conn)
                .await?;
            redis::cmd("HEXPIRE")
                .arg(&self.key)
                .arg(1)
                .arg("FIELDS")
                .arg(1)
                .arg(&probe)
                .query_async(&mut conn)
                .await
        }
        .await;
        // The probe field expires on its own within a second; failing to tidy it is not logged.
        let _ = redis::cmd("HDEL")
            .arg(&self.key)
            .arg(&probe)
            .query_async::<i64>(&mut conn)
            .await;

        match outcome {
            Ok(result) => probe_verdict(&self.key, &self.options.topic, &result),
            Err(e) if is_unreachable(&e) => {
                tracing::warn!(
                    instance = %self.options.instance_id,
                    error = %e,
                    "Streams: could not probe HEXPIRE support for topic {} consumer {}; continuing, and the first claim will report the failure.",
                    self.options.topic,
                    self.options.consumer
                );
                Ok(())
            }
            Err(e) => Err(Error::config(format!(
                "Streams: this Redis server rejected HEXPIRE ({e}), which the ownership registry needs to let a departed instance's claims lapse. Without it the claims for topic {} consumer {} would live forever and every restart would be reported as contested. Redis 7.4 or newer is required.",
                self.options.topic, self.options.consumer
            ))),
        }
    }

    async fn release(&self) {
        let fields: Vec<String> = if self.lease {
            self.held
                .borrow()
                .iter()
                .map(u32::to_string)
                .chain(std::iter::once(self.presence.clone()))
                .collect()
        } else if self.fields.is_empty() {
            return;
        } else {
            self.leased_fields().map(str::to_owned).collect()
        };

        let mut conn = self.conn.clone();
        // Compare-and-delete, not HDEL: a field may already hold the other pod's claim (R-15).
        let result = RELEASE
            .key(&self.key)
            .arg(&self.owner_value)
            .arg(&fields)
            .invoke_async::<i64>(&mut conn)
            .await;
        match result {
            Ok(_) => {
                if self.lease {
                    self.held.send_replace(Arc::from([]));
                }
            }
            Err(e) => tracing::debug!(
                instance = %self.options.instance_id,
                error = %e,
                "Streams: could not release ownership claims for topic {} consumer {}; they expire in at most {}s.",
                self.options.topic,
                self.options.consumer,
                self.options.ttl_seconds
            ),
        }
    }

    fn evaluate(&self, before: &[(String, String)], after: &[(String, String)]) -> OwnershipMap {
        let previous = read_owners(before);
        let owners = read_owners(after);
        let unowned: Vec<u32> = (0..self.options.partitions)
            .filter(|p| !owners.contains_key(p))
            .collect();
        let me = self.options.instance_id;
        let mut owned: Vec<u32> = self.options.owned_partitions.clone();
        owned.sort_unstable();
        owned.dedup();
        let mut contested = Vec::new();
        for partition in owned {
            let rival = previous
                .get(&partition)
                .filter(|was| was.instance_id != me)
                .or_else(|| owners.get(&partition).filter(|now| now.instance_id != me));
            if let Some(actual) = rival {
                contested.push(ContestedPartition {
                    partition,
                    expected: me,
                    actual: actual.clone(),
                });
            }
        }
        OwnershipMap {
            owners,
            unowned,
            contested,
        }
    }

    fn report_lease(
        &self,
        map: &OwnershipMap,
        previous: &[u32],
        current: &[u32],
        members: usize,
        changed: bool,
    ) {
        let o = &self.options;
        let first = {
            let mut state = self.state();
            !std::mem::replace(&mut state.described, true)
        };
        if changed || first {
            tracing::info!(
                instance = %o.instance_id,
                "Streams: consumer={} topic={} instance {} (pod {}) leases partitions {} of {} across {} live instances (was {}) | {}",
                o.consumer,
                o.topic,
                o.instance_id,
                o.pod_name,
                describe_set(current),
                o.partitions,
                members,
                describe_set(previous),
                describe_map(map, o.partitions)
            );
        }
        if !map.unowned.is_empty() {
            tracing::warn!(
                instance = %o.instance_id,
                "Streams: partitions {} of topic {} consumer {} hold no lease; some instance claims them within {}s. {}",
                join(&map.unowned),
                o.topic,
                o.consumer,
                o.ttl_seconds,
                describe_map(map, o.partitions)
            );
        }
    }

    fn report_static(&self, map: &OwnershipMap) {
        let o = &self.options;
        if !map.unowned.is_empty() {
            tracing::error!(
                instance = %o.instance_id,
                "Streams: ownership gap on topic {} consumer {} — partitions {} of {} have no owner, so their messages are consumed by nobody. Check that STREAMS_INSTANCE_COUNT matches spec.replicas. {}",
                o.topic,
                o.consumer,
                join(&map.unowned),
                o.partitions,
                describe_map(map, o.partitions)
            );
        }
        for clash in &map.contested {
            tracing::error!(
                instance = %o.instance_id,
                "Streams: ownership overlap on topic {} consumer {} — partition {} is claimed by {} as well as this instance {} (pod {}), so its messages are processed twice. A partitioned consumer must run as a StatefulSet, not a Deployment.",
                o.topic,
                o.consumer,
                clash.partition,
                clash.actual.label(),
                clash.expected,
                o.pod_name
            );
        }
        let first = {
            let mut state = self.state();
            !std::mem::replace(&mut state.described, true)
        };
        if first || map.is_degraded() {
            let claims = if self.fields.is_empty() {
                "[]".to_owned()
            } else {
                join(&o.owned_partitions)
            };
            tracing::info!(
                instance = %o.instance_id,
                "Streams: consumer={} topic={} instance {} (pod {}) claims partitions {} of {} | {}",
                o.consumer,
                o.topic,
                o.instance_id,
                o.pod_name,
                claims,
                o.partitions,
                describe_map(map, o.partitions)
            );
        }
    }
}

/// HEXPIRE answered: `1` (set) or `2` (deleted, already due) is support (C# `Success` / `Due`).
fn probe_verdict(key: &str, topic: &str, result: &[i64]) -> Result<()> {
    match result.first() {
        Some(1 | 2) => Ok(()),
        other => Err(Error::config(format!(
            "Streams: HEXPIRE on {key} returned {}, so partition ownership claims for topic {topic} would never expire and every later restart would read them as a live rival. Point Streams:ConnectionString at a Redis 7.4 or newer server.",
            other.map_or_else(|| "nothing".to_owned(), i64::to_string)
        ))),
    }
}

/// Redis unreachable rather than incapable (C# catches everything but `RedisServerException`).
fn is_unreachable(e: &RedisError) -> bool {
    e.is_io_error() || e.is_timeout() || e.is_connection_dropped() || e.is_connection_refusal()
}

/// `i:<instanceId>`, the presence field (C# `PresenceField`).
pub(crate) fn presence_field(instance_id: Uuid) -> String {
    format!("{PRESENCE_PREFIX}{}", instance_id.hyphenated())
}

/// `HGETALL`, names and values decoded lossily as UTF-8.
pub(crate) async fn hgetall(
    conn: &mut ConnectionManager,
    key: &str,
) -> Result<Vec<(String, String)>> {
    let raw: Vec<(Vec<u8>, Vec<u8>)> = redis::cmd("HGETALL").arg(key).query_async(conn).await?;
    Ok(raw
        .into_iter()
        .map(|(k, v)| {
            (
                String::from_utf8_lossy(&k).into_owned(),
                String::from_utf8_lossy(&v).into_owned(),
            )
        })
        .collect())
}

/// Owners from an ownership hash, skipping fields that are not a partition number (C# `ReadOwners`,
/// `int.TryParse` with `NumberStyles.None`: digits only).
pub(crate) fn read_owners(entries: &[(String, String)]) -> BTreeMap<u32, PartitionOwner> {
    entries
        .iter()
        .filter_map(|(name, value)| {
            parse_partition(name).map(|p| (p, PartitionOwner::parse_lenient(p, value)))
        })
        .collect()
}

fn parse_partition(name: &str) -> Option<u32> {
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    name.parse::<i32>().ok().and_then(|p| u32::try_from(p).ok())
}

/// Live instances: `i:` fields, at least 1 (C# `CountMembers`).
pub(crate) fn count_members(entries: &[(String, String)]) -> usize {
    entries
        .iter()
        .filter(|(name, _)| name.starts_with(PRESENCE_PREFIX))
        .count()
        .max(1)
}

/// C# `OwnershipMap.Describe`: `full map: 0->pod(xxxxxxxx) 1->(none) …`.
pub(crate) fn describe_map(map: &OwnershipMap, partitions: u32) -> String {
    let mut out = String::from("full map:");
    for p in 0..partitions {
        let owner = map
            .owners
            .get(&p)
            .map_or_else(|| "(none)".to_owned(), PartitionOwner::label);
        let _ = write!(out, " {p}->{owner}");
    }
    out
}

fn describe_set(partitions: &[u32]) -> String {
    if partitions.is_empty() {
        "[]".to_owned()
    } else {
        join(partitions)
    }
}

fn join(partitions: &[u32]) -> String {
    partitions
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn len_i64(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[path = "registry_tests.rs"]
mod tests;
