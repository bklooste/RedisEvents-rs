//! Configuration binding and validation (port of `Config/StreamConfigBinder.cs`). **Owner: A2.**
//!
//! [`bind_json`] reads the `Streams` section from a JSON document with the C# key names, applies
//! the `UseConsumerGroup` migration and refuses contradictory spellings. [`validate`] enforces the
//! throwing rules and [`advise`] produces the advisory lines (unconfigured topics, Block mode without
//! co-location). [`to_redis_url`] translates a C# connection string to a redis-rs URL.
//!
//! The binder never reads the process environment: callers pass the environment name explicitly.

use std::collections::HashMap;
use std::fmt::Write as _;

use serde::Deserialize;
use tracing::Level;

use super::advisor::Advisory;
use super::{
    BackpressureOptions, ConsumerOptions, DeliveryMode, InstanceMode, InstanceOptions, PersistMode,
    ReadMode, StartFrom, StreamOptions, TopicOptions, TrimMode,
};
use crate::error::{Error, Result};

/// The configuration section the options are bound from.
pub const SECTION_NAME: &str = "Streams";

/// Connection string used when [`StreamOptions::connection_string`] is not set. Streams live on
/// `redis-db` (AOF persistence), never on a cache instance.
pub const DEFAULT_CONNECTION_STRING: &str = "redis-db.infra:6379,abortConnect=false";

/// The StackExchange.Redis default `syncTimeout`, used when the connection string does not set one.
pub const DEFAULT_SYNC_TIMEOUT_MS: u32 = 5000;

/// The default Redis port when an endpoint omits one.
const DEFAULT_PORT: u16 = 6379;

/// Connection-string keys that StackExchange.Redis accepts and that have no redis-rs equivalent we
/// need. They are accepted and ignored. Anything else is refused rather than silently dropped.
const IGNORED_KEYS: &[&str] = &[
    "abortconnect",
    "connecttimeout",
    "connectretry",
    "keepalive",
    "allowadmin",
    "clientname",
];

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Root {
    #[serde(default)]
    streams: Option<StreamOptions>,
}

/// Binds [`StreamOptions`] from a JSON document whose `Streams` key holds the section.
///
/// A missing or `null` section is legal and yields all defaults. Unknown keys are ignored, as in
/// C#. A negative count is a deserialize error, because the option types are unsigned.
///
/// The obsolete `UseConsumerGroup` key is mapped onto `Delivery` here, and contradictory spellings
/// are refused.
///
/// # Errors
///
/// [`Error::Config`] when the JSON does not parse, a value has the wrong type or sign, or the two
/// delivery spellings disagree.
pub fn bind_json(json: &str) -> Result<StreamOptions> {
    let mut document: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| Error::config(format!("{SECTION_NAME}: invalid configuration: {e}")))?;
    canonicalise_keys(&mut document);
    let root: Root = serde_json::from_value(document)
        .map_err(|e| Error::config(format!("{SECTION_NAME}: invalid configuration: {e}")))?;
    let mut options = root.streams.unwrap_or_default();
    migrate_legacy_delivery(&mut options)?;
    Ok(options)
}

/// Every configuration key, in the `snake_case` of its field. The PascalCase serde name is derived
/// from it, so this table is the one place a new option must be listed for case-insensitive binding.
const CONFIG_KEYS: &[&str] = &[
    "streams",
    "connection_string",
    "consumer",
    "topics",
    "consumers",
    "producers",
    "partitions",
    "max_len",
    "trim",
    "retention_seconds",
    "background_trim_interval_seconds",
    "co_locate_partitions",
    "state_metadata",
    "clamp_release_threshold",
    "topic",
    "batch_size",
    "filter",
    "start_from",
    "start_from_date",
    "persist",
    "persist_interval_ms",
    "backpressure",
    "read_mode",
    "block_ms",
    "max_idle_delay_ms",
    "on_error",
    "unhealthy_block_seconds",
    "unhealthy_stopped_seconds",
    "unhealthy_behind_seconds",
    "degraded_silence_seconds",
    "contested_grace_seconds",
    "contested_recheck_seconds",
    "delivery",
    "use_consumer_group",
    "instances",
    "reader_threads",
    "shutdown_timeout_seconds",
    "unhealthy_lag_ms",
    "start_from_when_missing",
    "reset_on_start",
    "enabled",
    "capacity",
    "mode",
    "count",
    "index",
    "lease_ttl_seconds",
    "lease_renew_seconds",
    "buffered",
    "max_wait_ms",
    "max_batch",
    "max_queue",
    "drop_oldest",
];

/// The serde (PascalCase) name for a configuration key written in any case, or `None` when the key
/// is not one of ours. Case and underscores are ignored, as in C#.
fn canonical_key(key: &str) -> Option<String> {
    let folded: String = key
        .chars()
        .filter(|c| *c != '_')
        .map(|c| c.to_ascii_lowercase())
        .collect();
    CONFIG_KEYS
        .iter()
        .find(|known| known.replace('_', "") == folded)
        .map(|known| {
            known
                .split('_')
                .map(|word| {
                    let mut chars = word.chars();
                    chars
                        .next()
                        .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                        .unwrap_or_default()
                })
                .collect()
        })
}

/// Rewrites every known key to its canonical spelling, so `streams`, `STREAMS` and `Streams` bind
/// alike. Unknown keys are left alone (serde ignores them, as C# does). Topic names are data (they
/// become Redis key names), so the keys of `Topics` are kept exactly as written.
fn canonicalise_keys(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            let entries = std::mem::take(map);
            for (key, mut child) in entries {
                let canonical = canonical_key(&key);
                if canonical.as_deref() == Some("Topics") {
                    if let serde_json::Value::Object(topics) = &mut child {
                        for topic in topics.values_mut() {
                            canonicalise_keys(topic);
                        }
                    }
                } else {
                    canonicalise_keys(&mut child);
                }
                map.insert(canonical.unwrap_or(key), child);
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(canonicalise_keys),
        _ => {}
    }
}

/// Maps `UseConsumerGroup` onto `Delivery` for every consumer that sets it.
fn migrate_legacy_delivery(options: &mut StreamOptions) -> Result<()> {
    for (i, consumer) in options.consumers.iter_mut().enumerate() {
        let Some(legacy) = consumer.use_consumer_group else {
            continue;
        };
        let key = format!("{SECTION_NAME}:Consumers[{i}]");
        let wanted = legacy_delivery(legacy);

        // A silent winner is the worst outcome: one spelling would do nothing and nothing says
        // which. Refuse instead.
        if let Some(explicit) = consumer.delivery
            && explicit != wanted
        {
            return Err(Error::config(format!(
                "{key}:UseConsumerGroup is {legacy} but {key}:Delivery is {explicit:?}, and they disagree. \
                 UseConsumerGroup is the obsolete spelling of Delivery: true is WorkQueue, false is Ordered. \
                 Remove {key}:UseConsumerGroup and keep {key}:Delivery."
            )));
        }

        consumer.delivery = Some(wanted);
    }
    Ok(())
}

fn legacy_delivery(use_consumer_group: bool) -> DeliveryMode {
    if use_consumer_group {
        DeliveryMode::WorkQueue
    } else {
        DeliveryMode::Ordered
    }
}

/// Validates the options against every throwing rule. Throws on the first violation.
///
/// `environment` is the host environment name (`Production`, `Development`, …); only `Trim = None`
/// depends on it. `sync_timeout_ms` is the resolved connection's `syncTimeout`, see
/// [`resolve_sync_timeout_ms`].
///
/// # Errors
///
/// [`Error::Config`] naming the offending `Streams:…` key.
pub fn validate(options: &StreamOptions, environment: &str, sync_timeout_ms: u32) -> Result<()> {
    let is_development = environment.eq_ignore_ascii_case("Development");
    validate_topics(options, is_development, environment)?;
    validate_consumers(options, sync_timeout_ms)?;
    validate_producers(options)
}

fn validate_topics(options: &StreamOptions, is_development: bool, environment: &str) -> Result<()> {
    let mut names: Vec<&String> = options.topics.keys().collect();
    names.sort();

    for name in names {
        let topic = &options.topics[name];
        let key = format!("{SECTION_NAME}:Topics:{name}");

        if topic.partitions < 1 {
            return Err(Error::config(format!(
                "{key}:Partitions is {}; it must be >= 1.",
                topic.partitions
            )));
        }

        if let Some(retention) = topic.retention_seconds {
            if retention < 1 {
                return Err(Error::config(format!(
                    "{key}:RetentionSeconds is {retention}; it must be >= 1 or omitted."
                )));
            }

            // Time-based trimming does not bound memory, so a ceiling is mandatory. An omitted
            // MaxLen is distinguishable from a configured one, which is what makes this rule
            // reachable at all.
            match topic.max_len {
                Some(0) => {
                    return Err(Error::config(format!(
                        "{key}:RetentionSeconds is set ({retention}) but {key}:MaxLen is 0. \
                         Time-based trimming does not bound memory, so a MaxLen ceiling (>= 1) is mandatory."
                    )));
                }
                None => {
                    return Err(Error::config(format!(
                        "{key}:RetentionSeconds is set ({retention}s) but {key}:MaxLen is not configured. \
                         Time-based trimming does not bound memory, so the ceiling is what actually protects Redis, \
                         and the default of {} entries per partition is almost certainly not the ceiling a \
                         {retention}s window needs. Set {key}:MaxLen explicitly (entries per partition), or remove \
                         {key}:RetentionSeconds and let the inline MAXLEN window do the trimming.",
                        TopicOptions::DEFAULT_MAX_LEN
                    )));
                }
                Some(_) => {}
            }
        }

        if topic.trim == TrimMode::None && !is_development {
            return Err(Error::config(format!(
                "{key}:Trim is None in environment '{environment}'. Trim=None is an OOM footgun and is only allowed in Development."
            )));
        }
    }
    Ok(())
}

fn validate_consumers(options: &StreamOptions, sync_timeout_ms: u32) -> Result<()> {
    let mut seen: HashMap<(&str, String), usize> = HashMap::new();

    for (i, consumer) in options.consumers.iter().enumerate() {
        let key = format!("{SECTION_NAME}:Consumers[{i}]");

        if consumer.topic.trim().is_empty() {
            return Err(Error::config(format!(
                "{key}:Topic is missing; every consumer must name a topic."
            )));
        }
        if consumer.batch_size < 1 {
            return Err(Error::config(format!(
                "{key}:BatchSize is {}; it must be >= 1.",
                consumer.batch_size
            )));
        }
        validate_backpressure(&consumer.backpressure, &key)?;

        if consumer.start_from == StartFrom::Date && consumer.start_from_date.is_none() {
            return Err(Error::config(format!(
                "{key}:StartFrom is Date but {key}:StartFromDate is not set."
            )));
        }

        if consumer.persist == PersistMode::None && consumer.start_from == StartFrom::Stored {
            return Err(Error::config(format!(
                "{key}:Persist is None but {key}:StartFrom is Stored: a contradiction, because nothing is ever stored. \
                 Use StartFrom Now, Beginning or Date."
            )));
        }

        // Nothing in the consumer host applies a start date as a reset first. Accepting the key
        // would mean a redeploy that was meant to replay quietly not replaying.
        if consumer.reset_on_start {
            return Err(Error::config(format!(
                "{key}:ResetOnStart is true, but nothing in the consumer host reads it: the positions would NOT be reset and \
                 the consumer would resume where it left off, silently. Remove the key and perform the replay explicitly: \
                 StreamAdmin::reset_position with the consumer stopped, then start it."
            )));
        }

        validate_instances(consumer.instances.as_ref(), &key)?;

        let name = resolve_consumer_name(options, consumer);
        if let Some(&first) = seen.get(&(consumer.topic.as_str(), name.clone())) {
            return Err(Error::config(format!(
                "{key} duplicates {SECTION_NAME}:Consumers[{first}]: both consume topic '{}' as consumer '{name}'. \
                 Two consumers on the same (Topic, Consumer) pair in one process would fight over the same position hash. \
                 Set a distinct {key}:Consumer.",
                consumer.topic
            )));
        }
        seen.insert((consumer.topic.as_str(), name), i);

        if consumer.read_mode == ReadMode::Block && consumer.block_ms >= sync_timeout_ms {
            return Err(Error::config(format!(
                "{key}:BlockMs is {}ms but the resolved connection's syncTimeout is {sync_timeout_ms}ms. \
                 In Block mode BlockMs must stay well under syncTimeout, otherwise StackExchange.Redis times the XREAD out client-side. \
                 Lower {key}:BlockMs or raise syncTimeout in {SECTION_NAME}:ConnectionString.",
                consumer.block_ms
            )));
        }
    }
    Ok(())
}

fn validate_backpressure(backpressure: &BackpressureOptions, key: &str) -> Result<()> {
    if backpressure.capacity < 1 {
        return Err(Error::config(format!(
            "{key}:Backpressure:Capacity is {}; it must be >= 1.",
            backpressure.capacity
        )));
    }
    Ok(())
}

fn validate_instances(instances: Option<&InstanceOptions>, consumer_key: &str) -> Result<()> {
    let Some(instances) = instances else {
        return Ok(());
    };
    let key = format!("{consumer_key}:Instances");
    let ttl = instances.lease_ttl_seconds;
    let renew = instances.lease_renew_seconds;

    if ttl < 1 {
        return Err(Error::config(format!(
            "{key}:LeaseTtlSeconds is {ttl}; it must be at least 1 second."
        )));
    }
    if renew < 1 {
        return Err(Error::config(format!(
            "{key}:LeaseRenewSeconds is {renew}; it must be at least 1 second."
        )));
    }
    if renew >= ttl {
        return Err(Error::config(format!(
            "{key}:LeaseRenewSeconds is {renew}s against {key}:LeaseTtlSeconds of {ttl}s. \
             The renew interval must be shorter than the TTL, comfortably shorter, since one missed renewal must not cost the \
             claim, or every ownership claim expires between renewals. The defaults are {}s and {}s.",
            InstanceOptions::DEFAULT_LEASE_TTL_SECONDS,
            InstanceOptions::DEFAULT_LEASE_RENEW_SECONDS
        )));
    }

    // Lease reads neither Count nor Index, so declaring them has no effect. Refused rather than
    // ignored. Lease is the default, so a Count or Index with no Mode is a config written when
    // Static was the default: refused with a message naming the new default.
    if instances.mode() == InstanceMode::Lease
        && (instances.count.is_some() || instances.index.is_some())
    {
        return Err(Error::config(if instances.mode.is_some() {
            format!(
                "{key}:Mode is Lease, but {key}:Count / {key}:Index are set and Lease mode reads neither: it claims free partitions \
                 and counts the live instances out of the ownership hash. \
                 Remove them, or set Mode to Static to keep the fixed assignment."
            )
        } else {
            format!(
                "{key}:Count / {key}:Index are set but {key}:Mode is not set, and Mode now defaults to Lease, which reads neither. \
                 Set Mode to Static explicitly to keep the fixed assignment these two keys describe, or remove them to take the \
                 Lease default, where partitions are claimed from the ownership registry and no instance count is maintained."
            )
        }));
    }

    if instances.count == Some(0) {
        return Err(Error::config(format!("{key}:Count is 0; it must be >= 1.")));
    }
    if let (Some(count), Some(index)) = (instances.count, instances.index)
        && index >= count
    {
        return Err(Error::config(format!(
            "{key}:Index is {index} but {key}:Count is {count}; Index must be less than Count (indexes are zero-based)."
        )));
    }
    Ok(())
}

fn validate_producers(options: &StreamOptions) -> Result<()> {
    for (i, producer) in options.producers.iter().enumerate() {
        let key = format!("{SECTION_NAME}:Producers[{i}]");

        if producer.topic.trim().is_empty() {
            return Err(Error::config(format!(
                "{key}:Topic is missing; every producer must name a topic."
            )));
        }
        if producer.max_batch < 1 {
            return Err(Error::config(format!(
                "{key}:MaxBatch is {}; it must be >= 1.",
                producer.max_batch
            )));
        }
        if producer.max_queue < 1 {
            return Err(Error::config(format!(
                "{key}:MaxQueue is {}; it must be >= 1.",
                producer.max_queue
            )));
        }
    }
    Ok(())
}

/// Returns the advisory lines for the options: unconfigured topics (they run on defaults, which is
/// legal) and Block-mode consumers without co-located partitions (one dedicated connection per
/// partition). Call [`validate`] first; this does not check for errors.
#[must_use]
pub fn advise(options: &StreamOptions) -> Vec<Advisory> {
    let mut out = Vec::new();

    for (i, consumer) in options.consumers.iter().enumerate() {
        let key = format!("{SECTION_NAME}:Consumers[{i}]");

        if let Some(legacy) = consumer.use_consumer_group {
            out.push(Advisory {
                level: Level::WARN,
                message: format!(
                    "Streams: {key}:UseConsumerGroup is set and is obsolete; it has been read as {key}:Delivery={:?}. \
                     Rename the key: UseConsumerGroup is removed in the next version. Delivery=WorkQueue reads with \
                     XREADGROUP, so entries of one partition go to whichever member asks first and per-key order is lost.",
                    legacy_delivery(legacy)
                ),
            });
        }

        let topic = resolve_topic_with_advice(options, &consumer.topic, &mut out);
        if consumer.read_mode == ReadMode::Block
            && !topic.co_locate_partitions
            && topic.partitions > 2
        {
            out.push(Advisory {
                level: Level::WARN,
                message: format!(
                    "Streams: {key}:ReadMode is Block and {SECTION_NAME}:Topics:{}:CoLocatePartitions is false with {} partitions. \
                     Each partition needs its own blocking reader connection, so this consumer will open {} dedicated Redis connections. \
                     Set CoLocatePartitions to true to use a single multi-stream XREAD.",
                    consumer.topic, topic.partitions, topic.partitions
                ),
            });
        }
    }

    for producer in &options.producers {
        let _ = resolve_topic_with_advice(options, &producer.topic, &mut out);
    }

    out
}

fn resolve_topic_with_advice(
    options: &StreamOptions,
    topic: &str,
    out: &mut Vec<Advisory>,
) -> TopicOptions {
    if let Some(configured) = options.topics.get(topic) {
        return configured.clone();
    }
    let defaulted = TopicOptions::default();
    out.push(Advisory {
        level: Level::INFO,
        message: format!(
            "Streams: topic {topic} is not present in Streams:Topics; using defaults (Partitions={}, MaxLen={}, Trim={:?}).",
            defaulted.partitions,
            defaulted.max_len(),
            defaulted.trim
        ),
    });
    defaulted
}

/// The topic options for `topic`, or the defaults when the topic is not configured. Unconfigured
/// topics are legal; [`advise`] reports them.
#[must_use]
pub fn resolve_topic(options: &StreamOptions, topic: &str) -> TopicOptions {
    options.topics.get(topic).cloned().unwrap_or_default()
}

/// The effective consumer name: the entry's own `Consumer`, else the root `Consumer`, else the
/// running executable's name.
#[must_use]
pub fn resolve_consumer_name(options: &StreamOptions, consumer: &ConsumerOptions) -> String {
    non_blank(consumer.consumer.as_deref())
        .or_else(|| non_blank(options.consumer.as_deref()))
        .map_or_else(default_consumer_name, str::to_owned)
}

fn default_consumer_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|path| path.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// The effective connection string: the configured one, else [`DEFAULT_CONNECTION_STRING`].
#[must_use]
pub fn resolve_connection_string(options: &StreamOptions) -> String {
    non_blank(options.connection_string.as_deref())
        .unwrap_or(DEFAULT_CONNECTION_STRING)
        .to_owned()
}

fn non_blank(value: Option<&str>) -> Option<&str> {
    value.filter(|v| !v.trim().is_empty())
}

/// The `syncTimeout` in milliseconds from a connection string, or the StackExchange.Redis default
/// when it is not given. `None` and blank strings use [`DEFAULT_CONNECTION_STRING`].
///
/// # Errors
///
/// [`Error::Config`] naming `Streams:ConnectionString` when the string does not parse.
pub fn resolve_sync_timeout_ms(connection_string: Option<&str>) -> Result<u32> {
    let parsed =
        parse_connection_string(non_blank(connection_string).unwrap_or(DEFAULT_CONNECTION_STRING))?;
    Ok(parsed.sync_timeout_ms.unwrap_or(DEFAULT_SYNC_TIMEOUT_MS))
}

/// Translates a StackExchange.Redis connection string (`host:port,password=…,ssl=true,…`) into a
/// redis-rs URL for [`Connection::connect`](crate::Connection::connect).
///
/// Supported keys (case-insensitive): `password`, `user`, `ssl`, `defaultDatabase`, `syncTimeout`
/// (read by [`resolve_sync_timeout_ms`]), and the accepted-but-ignored keys in [`IGNORED_KEYS`].
/// Other keys are refused. A single endpoint is supported: cluster and sentinel are not wired.
///
/// # Errors
///
/// [`Error::Config`] naming `Streams:ConnectionString`. The message never echoes the password.
pub fn to_redis_url(connection_string: &str) -> Result<String> {
    let parsed = parse_connection_string(connection_string)?;
    let scheme = if parsed.tls { "rediss" } else { "redis" };
    let mut url = format!("{scheme}://");
    match (&parsed.user, &parsed.password) {
        (Some(user), Some(password)) => {
            let _ = write!(
                url,
                "{}:{}@",
                percent_encode(user),
                percent_encode(password)
            );
        }
        (None, Some(password)) => {
            let _ = write!(url, ":{}@", percent_encode(password));
        }
        (Some(user), None) => {
            let _ = write!(url, "{}@", percent_encode(user));
        }
        (None, None) => {}
    }
    let _ = write!(url, "{}/{}", parsed.host_port, parsed.database.unwrap_or(0));
    Ok(url)
}

/// What the connection string said, before it is rendered as a URL.
#[derive(Debug, Default)]
struct ParsedConnection {
    host_port: String,
    user: Option<String>,
    password: Option<String>,
    tls: bool,
    database: Option<u32>,
    sync_timeout_ms: Option<u32>,
}

fn parse_connection_string(connection_string: &str) -> Result<ParsedConnection> {
    parse_inner(connection_string).map_err(|detail| {
        Error::config(format!(
            "{SECTION_NAME}:ConnectionString is not a valid Redis connection string: {detail}"
        ))
    })
}

fn parse_inner(connection_string: &str) -> std::result::Result<ParsedConnection, String> {
    let mut parsed = ParsedConnection::default();
    let mut endpoints = Vec::new();

    for part in connection_string
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        let Some((raw_key, value)) = part.split_once('=') else {
            endpoints.push(part);
            continue;
        };
        let key = raw_key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "password" => parsed.password = Some(value.to_owned()),
            "user" => parsed.user = Some(value.to_owned()),
            "ssl" => {
                parsed.tls = match value.to_ascii_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(format!("ssl must be true or false, not '{value}'")),
                };
            }
            "defaultdatabase" => {
                parsed.database = Some(value.parse::<u32>().map_err(|_| {
                    format!("defaultDatabase must be a non-negative integer, not '{value}'")
                })?);
            }
            "synctimeout" => {
                parsed.sync_timeout_ms = Some(value.parse::<u32>().map_err(|_| {
                    format!("syncTimeout must be a non-negative integer, not '{value}'")
                })?);
            }
            k if IGNORED_KEYS.contains(&k) => {}
            _ => return Err(format!("unsupported option '{}'", raw_key.trim())),
        }
    }

    match endpoints.as_slice() {
        [] => return Err("no endpoint (expected host:port)".to_owned()),
        [one] => parsed.host_port = normalise_endpoint(one)?,
        _ => {
            return Err(
                "more than one endpoint; only a single node is supported (cluster and sentinel are not wired)".to_owned(),
            );
        }
    }
    Ok(parsed)
}

/// `host`, `host:port`, `[v6]` or `[v6]:port`, rendered with brackets where needed and the default
/// port filled in.
fn normalise_endpoint(endpoint: &str) -> std::result::Result<String, String> {
    let (host, port) = if let Some(rest) = endpoint.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .ok_or_else(|| format!("unclosed '[' in endpoint '{endpoint}'"))?;
        let port =
            match after {
                "" => None,
                other => Some(other.strip_prefix(':').ok_or_else(|| {
                    format!("unexpected text after ']' in endpoint '{endpoint}'")
                })?),
            };
        (format!("[{host}]"), port)
    } else {
        match endpoint.matches(':').count() {
            0 => (endpoint.to_owned(), None),
            1 => {
                let (host, port) = endpoint.split_once(':').unwrap_or((endpoint, ""));
                (host.to_owned(), Some(port))
            }
            _ => {
                return Err(format!(
                    "IPv6 endpoints must be bracketed, e.g. [::1]:6379: '{endpoint}'"
                ));
            }
        }
    };

    if host.is_empty() || host == "[]" {
        return Err(format!("endpoint '{endpoint}' has no host"));
    }

    let port = match port {
        None => DEFAULT_PORT,
        Some(text) => text
            .parse::<u16>()
            .map_err(|_| format!("port '{text}' is not a valid port number"))?,
    };
    Ok(format!("{host}:{port}"))
}

/// Percent-encodes everything outside the RFC 3986 unreserved set, so a password with `@`, `:` or
/// `/` survives the URL.
fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ErrorPolicy, ProducerOptions};
    use time::macros::datetime;

    /// Every key in the section, each set to something that is not its default.
    const FULL_SECTION: &str = r#"{
      "Streams": {
        "ConnectionString": "localhost:6380,syncTimeout=9000",
        "Consumer": "svc-wide-name",
        "Topics": {
          "orders": {
            "Partitions": 4,
            "MaxLen": 250000,
            "Trim": "Exact",
            "RetentionSeconds": 3600,
            "BackgroundTrimIntervalSeconds": 60,
            "CoLocatePartitions": false,
            "ClampReleaseThreshold": 0.5,
            "StateMetadata": true
          }
        },
        "Consumers": [
          {
            "Topic": "orders",
            "Consumer": "orders-reader",
            "BatchSize": 250,
            "Filter": [ "OrderPlaced", "OrderCancelled" ],
            "StartFrom": "Date",
            "StartFromDate": "2026-01-02T03:04:05+00:00",
            "Persist": "SyncBatch",
            "PersistIntervalMs": 250,
            "Backpressure": { "Enabled": false, "Capacity": 9 },
            "ReadMode": "Poll",
            "BlockMs": 750,
            "MaxIdleDelayMs": 25,
            "OnError": "StopPartition",
            "UnhealthyBlockSeconds": 90,
            "Instances": { "Mode": "Static", "Count": 3, "Index": 1 },
            "ReaderThreads": 2,
            "ShutdownTimeoutSeconds": 30,
            "UnhealthyLagMs": 45000,
            "UnhealthyBehindSeconds": 240,
            "DegradedSilenceSeconds": 900,
            "StartFromWhenMissing": "Now"
          }
        ],
        "Producers": [
          {
            "Topic": "orders",
            "Buffered": true,
            "MaxWaitMs": 5,
            "MaxBatch": 1000,
            "MaxQueue": 250,
            "DropOldest": true
          }
        ]
      }
    }"#;

    const PRODUCTION: &str = "Production";
    const DEVELOPMENT: &str = "Development";

    fn bind(json: &str) -> StreamOptions {
        bind_json(json).expect("bind")
    }

    fn throws(json: &str) -> String {
        let options = bind_json(json).expect("binding should succeed; the validation should fail");
        validate(&options, PRODUCTION, 5000)
            .expect_err("validation should fail")
            .to_string()
    }

    fn with_topic(name: &str, topic: TopicOptions) -> StreamOptions {
        StreamOptions {
            topics: HashMap::from([(name.to_owned(), topic)]),
            ..StreamOptions::default()
        }
    }

    fn with_consumer(consumer: ConsumerOptions) -> StreamOptions {
        StreamOptions {
            consumer: Some("svc".to_owned()),
            consumers: vec![consumer],
            ..StreamOptions::default()
        }
    }

    fn with_producer(producer: ProducerOptions) -> StreamOptions {
        StreamOptions {
            producers: vec![producer],
            ..StreamOptions::default()
        }
    }

    fn validate_err(options: &StreamOptions, environment: &str) -> String {
        validate(options, environment, 5000)
            .expect_err("validation should fail")
            .to_string()
    }

    // ------------------------------------------------------------ binding actually binds

    #[test]
    fn bind_root_keys_are_assigned() {
        let options = bind(FULL_SECTION);

        assert_eq!(
            options.connection_string.as_deref(),
            Some("localhost:6380,syncTimeout=9000")
        );
        assert_eq!(options.consumer.as_deref(), Some("svc-wide-name"));
        assert!(options.topics.contains_key("orders"));
        assert_eq!(options.consumers.len(), 1);
        assert_eq!(options.producers.len(), 1);
    }

    #[test]
    fn bind_topic_keys_are_assigned() {
        let topic = &bind(FULL_SECTION).topics["orders"];

        assert_eq!(topic.partitions, 4);
        assert_eq!(topic.max_len, Some(250_000));
        assert_eq!(topic.trim, TrimMode::Exact);
        assert_eq!(topic.retention_seconds, Some(3600));
        assert_eq!(topic.background_trim_interval_seconds, 60);
        assert!(!topic.co_locate_partitions);
        assert!((topic.clamp_release_threshold - 0.5).abs() < f64::EPSILON);
        assert!(topic.state_metadata);
    }

    #[test]
    fn bind_consumer_keys_are_assigned() {
        let consumer = &bind(FULL_SECTION).consumers[0];

        assert_eq!(consumer.topic, "orders");
        assert_eq!(consumer.consumer.as_deref(), Some("orders-reader"));
        assert_eq!(consumer.batch_size, 250);
        assert_eq!(
            consumer.filter.as_deref(),
            Some(&["OrderPlaced".to_owned(), "OrderCancelled".to_owned()][..])
        );
        assert_eq!(consumer.start_from, StartFrom::Date);
        assert_eq!(
            consumer.start_from_date,
            Some(datetime!(2026-01-02 03:04:05 UTC))
        );
        assert_eq!(consumer.persist, PersistMode::SyncBatch);
        assert_eq!(consumer.persist_interval_ms, 250);
        assert!(!consumer.backpressure.enabled);
        assert_eq!(consumer.backpressure.capacity, 9);
        assert_eq!(consumer.read_mode, ReadMode::Poll);
        assert_eq!(consumer.block_ms, 750);
        assert_eq!(consumer.max_idle_delay_ms, 25);
        assert_eq!(consumer.on_error, ErrorPolicy::StopPartition);
        assert_eq!(consumer.unhealthy_block_seconds, 90);
        assert_eq!(consumer.reader_threads, 2);
        assert_eq!(consumer.shutdown_timeout_seconds, 30);
        assert_eq!(consumer.unhealthy_lag_ms, 45_000);
        assert_eq!(consumer.unhealthy_behind_seconds, 240);
        assert_eq!(consumer.degraded_silence_seconds, 900);
        assert_eq!(consumer.start_from_when_missing, StartFrom::Now);
        let instances = consumer.instances.as_ref().expect("instances");
        assert_eq!(instances.count, Some(3));
        assert_eq!(instances.index, Some(1));
    }

    #[test]
    fn bind_producer_keys_are_assigned() {
        let producer = &bind(FULL_SECTION).producers[0];

        assert_eq!(producer.topic, "orders");
        assert!(producer.buffered);
        assert_eq!(producer.max_wait_ms, 5);
        assert_eq!(producer.max_batch, 1000);
        assert_eq!(producer.max_queue, 250);
        assert!(producer.drop_oldest);
    }

    #[test]
    fn bind_full_section_validates_cleanly() {
        let options = bind(FULL_SECTION);

        validate(&options, PRODUCTION, 5000)
            .expect("a fully populated section that binds must also be a legal one");
    }

    #[test]
    fn bind_keys_not_present_keep_their_defaults() {
        let options = bind(r#"{ "Streams": { "Topics": { "orders": { "Partitions": 8 } } } }"#);

        let topic = &options.topics["orders"];
        assert_eq!(topic.partitions, 8);
        assert_eq!(
            topic.max_len(),
            TopicOptions::DEFAULT_MAX_LEN,
            "an absent key must not overwrite the default"
        );
        assert_eq!(topic.trim, TrimMode::Approx);
        assert!(topic.co_locate_partitions);
    }

    // ------------------------------------------------------------ MaxLen: configured vs defaulted

    #[test]
    fn bind_max_len_tracks_whether_it_was_configured() {
        let configured = &bind(r#"{ "Streams": { "Topics": { "orders": { "MaxLen": 10000 } } } }"#)
            .topics["orders"];
        let defaulted = &bind(r#"{ "Streams": { "Topics": { "orders": { "Partitions": 2 } } } }"#)
            .topics["orders"];

        assert_eq!(configured.max_len(), TopicOptions::DEFAULT_MAX_LEN);
        assert_eq!(defaulted.max_len(), TopicOptions::DEFAULT_MAX_LEN);

        assert!(
            configured.max_len_configured(),
            "the two are indistinguishable by value: an operator who writes the default explicitly has still chosen a ceiling"
        );
        assert!(!defaulted.max_len_configured());
    }

    #[test]
    fn validate_retention_without_max_len_throws_naming_both_keys() {
        let message =
            throws(r#"{ "Streams": { "Topics": { "orders": { "RetentionSeconds": 604800 } } } }"#);

        assert!(message.contains("Streams:Topics:orders:RetentionSeconds"));
        assert!(message.contains("Streams:Topics:orders:MaxLen"));
        assert!(message.contains("not configured"));
    }

    #[test]
    fn validate_retention_with_max_len_is_accepted() {
        let options = bind(
            r#"{ "Streams": { "Topics": { "orders": { "RetentionSeconds": 604800, "MaxLen": 5000000 } } } }"#,
        );

        validate(&options, PRODUCTION, 5000).expect("retention with a ceiling is legal");
    }

    // ------------------------------------------------------------ keys that bound and did nothing

    #[test]
    fn validate_lease_mode_is_accepted_now_that_the_host_implements_it() {
        let options = bind(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "Instances": { "Mode": "Lease" } } ] } }"#,
        );

        validate(&options, PRODUCTION, 5000)
            .expect("Lease mode claims its partitions from the ownership registry");
    }

    #[test]
    fn validate_lease_mode_with_a_count_or_index_is_refused() {
        for keys in [
            r#""Count": 2"#,
            r#""Index": 0"#,
            r#""Count": 2, "Index": 1"#,
        ] {
            let message = throws(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Instances": {{ "Mode": "Lease", {keys} }} }} ] }} }}"#
            ));

            assert!(
                message.contains("Streams:Consumers[0]:Instances:Mode is Lease"),
                "{message}"
            );
            assert!(
                message.contains("Streams:Consumers[0]:Instances:Count"),
                "{message}"
            );
            assert!(
                message.contains("Streams:Consumers[0]:Instances:Index"),
                "{message}"
            );
        }
    }

    #[test]
    fn validate_count_or_index_with_no_mode_is_refused_and_names_the_new_lease_default() {
        for keys in [r#""Count": 2"#, r#""Index": 0"#] {
            let message = throws(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Instances": {{ {keys} }} }} ] }} }}"#
            ));

            assert!(
                message.contains("Streams:Consumers[0]:Instances:Mode is not"),
                "{message}"
            );
            assert!(message.contains("defaults to Lease"), "{message}");
            assert!(message.contains("Static"), "{message}");
        }
    }

    #[test]
    fn bind_instances_with_no_mode_defaults_to_lease() {
        let options = bind(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "Instances": { "LeaseTtlSeconds": 30 } } ] } }"#,
        );

        assert_eq!(
            options.consumers[0]
                .instances
                .as_ref()
                .map(InstanceOptions::mode),
            Some(InstanceMode::Lease)
        );
        validate(&options, PRODUCTION, 5000).expect("Lease default validates clean");
    }

    #[test]
    fn validate_lease_timings_are_accepted_because_the_host_passes_them_through() {
        for keys in [
            r#""LeaseTtlSeconds": 120, "LeaseRenewSeconds": 45"#,
            r#""LeaseTtlSeconds": 120"#,
            r#""LeaseRenewSeconds": 5"#,
        ] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Instances": {{ {keys} }} }} ] }} }}"#
            ));

            validate(&options, PRODUCTION, 5000).unwrap_or_else(|e| panic!("{keys}: {e}"));
        }
    }

    #[test]
    fn validate_lease_renew_at_or_past_the_ttl_is_refused() {
        for keys in [
            r#""LeaseRenewSeconds": 45"#,
            r#""LeaseTtlSeconds": 10, "LeaseRenewSeconds": 10"#,
        ] {
            let message = throws(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Instances": {{ {keys} }} }} ] }} }}"#
            ));

            assert!(
                message.contains("Streams:Consumers[0]:Instances:LeaseRenewSeconds"),
                "{message}"
            );
            assert!(
                message.contains("Streams:Consumers[0]:Instances:LeaseTtlSeconds"),
                "{message}"
            );
        }
    }

    #[test]
    fn validate_lease_timings_below_one_second_are_refused() {
        for (key, expected) in [
            ("LeaseTtlSeconds", "LeaseTtlSeconds"),
            ("LeaseRenewSeconds", "LeaseRenewSeconds"),
        ] {
            let message = throws(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Instances": {{ "{key}": 0 }} }} ] }} }}"#
            ));

            assert!(
                message.contains(&format!("Streams:Consumers[0]:Instances:{expected}")),
                "{message}"
            );
            assert!(message.contains("at least 1 second"), "{message}");
        }
    }

    #[test]
    fn validate_lease_timings_at_their_defaults_are_accepted() {
        let options = bind(
            r#"{ "Streams": { "Consumers": [
              { "Topic": "orders", "Instances": { "Mode": "Static", "Count": 2, "Index": 0, "LeaseTtlSeconds": 30, "LeaseRenewSeconds": 10 } }
            ] } }"#,
        );

        validate(&options, PRODUCTION, 5000)
            .expect("30s/10s is what the ownership registry claims and renews on");
    }

    #[test]
    fn validate_reset_on_start_is_refused_because_nothing_reads_it() {
        let message = throws(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "ResetOnStart": true, "StartFrom": "Date", "StartFromDate": "2026-01-01T00:00:00Z" } ] } }"#,
        );

        assert!(message.contains("Streams:Consumers[0]:ResetOnStart"));
        assert!(message.contains("StreamAdmin"), "{message}");
    }

    #[test]
    fn validate_reset_on_start_false_is_accepted() {
        let options = bind(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "ResetOnStart": false } ] } }"#,
        );

        validate(&options, PRODUCTION, 5000).expect("ResetOnStart false is legal");
    }

    // ------------------------------------------------------------ advisory rules

    #[test]
    fn validate_block_without_co_location_warns() {
        let options = bind(
            r#"{ "Streams": {
              "Topics": { "orders": { "Partitions": 8, "CoLocatePartitions": false } },
              "Consumers": [ { "Topic": "orders", "ReadMode": "Block" } ]
            } }"#,
        );

        let warnings: Vec<_> = advise(&options)
            .into_iter()
            .filter(|a| a.level == Level::WARN)
            .collect();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].message.contains("dedicated Redis connections"));
    }

    #[test]
    fn validate_unconfigured_topic_logs_the_defaults_it_will_run_on() {
        let options = bind(r#"{ "Streams": { "Consumers": [ { "Topic": "orders" } ] } }"#);

        let lines = advise(&options);
        assert!(
            lines.iter().any(|a| a.level == Level::INFO
                && a.message.contains("is not present in Streams:Topics")),
            "{lines:?}"
        );
    }

    // ------------------------------------------------------------ Delivery / UseConsumerGroup

    #[test]
    fn delivery_defaults_to_ordered() {
        assert_eq!(
            ConsumerOptions::default().delivery(),
            DeliveryMode::Ordered,
            "the ordered named-cursor model is the default; WorkQueue loses per-key order and must be asked for"
        );

        let options = bind(r#"{ "Streams": { "Consumers": [ { "Topic": "orders" } ] } }"#);
        assert_eq!(options.consumers[0].delivery(), DeliveryMode::Ordered);
    }

    #[test]
    fn bind_delivery_binds_work_queue() {
        let options = bind(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "Delivery": "WorkQueue" } ] } }"#,
        );

        assert_eq!(options.consumers[0].delivery(), DeliveryMode::WorkQueue);
    }

    #[test]
    fn validate_legacy_use_consumer_group_maps_onto_delivery_and_warns() {
        for (legacy, expected) in [
            (true, DeliveryMode::WorkQueue),
            (false, DeliveryMode::Ordered),
        ] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "UseConsumerGroup": {legacy} }} ] }} }}"#
            ));

            assert_eq!(
                options.consumers[0].delivery(),
                expected,
                "the migration runs in bind, for callers that never validate"
            );
            validate(&options, PRODUCTION, 5000).expect("the migrated spelling is legal");

            let warning = advise(&options)
                .into_iter()
                .find(|a| a.level == Level::WARN && a.message.contains("UseConsumerGroup"))
                .expect("a UseConsumerGroup warning");
            assert!(warning.message.contains("obsolete"));
            assert!(
                warning.message.contains("Streams:Consumers[0]:Delivery"),
                "the warning has to name the new key"
            );
            assert!(warning.message.contains(&format!("{expected:?}")));
        }
    }

    #[test]
    fn validate_legacy_use_consumer_group_warns_once_across_bind_and_validate() {
        let options = bind(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "UseConsumerGroup": true } ] } }"#,
        );

        validate(&options, PRODUCTION, 5000).expect("valid");
        validate(&options, PRODUCTION, 5000).expect("valid");

        // Validation never warns; the advisor reports the obsolete key once per startup.
        let count = advise(&options)
            .iter()
            .filter(|a| a.message.contains("UseConsumerGroup"))
            .count();
        assert_eq!(
            count, 1,
            "repeating it on every validate would train operators to ignore it"
        );
    }

    #[test]
    fn bind_both_delivery_keys_disagreeing_is_refused() {
        for (legacy, delivery) in [("true", "Ordered"), ("false", "WorkQueue")] {
            let message = bind_json(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "UseConsumerGroup": {legacy}, "Delivery": "{delivery}" }} ] }} }}"#
            ))
            .expect_err("disagreeing spellings must be refused")
            .to_string();

            assert!(
                message.contains("Streams:Consumers[0]:UseConsumerGroup"),
                "{message}"
            );
            assert!(
                message.contains("Streams:Consumers[0]:Delivery"),
                "{message}"
            );
            assert!(message.contains("disagree"), "{message}");
        }
    }

    #[test]
    fn bind_both_delivery_keys_agreeing_is_accepted() {
        for (legacy, delivery) in [("true", "WorkQueue"), ("false", "Ordered")] {
            bind_json(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "UseConsumerGroup": {legacy}, "Delivery": "{delivery}" }} ] }} }}"#
            ))
            .expect("saying the same thing twice is redundant, not contradictory");
        }
    }

    // ------------------------------------------------------------ ConfigTests: binding and defaults

    #[test]
    fn bind_empty_streams_section_yields_all_defaults() {
        let options = bind(r#"{ "Streams": { } }"#);

        assert_eq!(options.connection_string, None);
        assert_eq!(options.consumer, None);
        assert_eq!(options.topics.len(), 0);
        assert_eq!(options.consumers.len(), 0);
        assert_eq!(options.producers.len(), 0);
    }

    #[test]
    fn bind_missing_streams_section_yields_all_defaults() {
        let options = bind(r#"{ "Logging": { "LogLevel": { "Default": "Information" } } }"#);

        assert_eq!(options.connection_string, None);
        assert_eq!(options.topics.len(), 0);
        assert_eq!(options.consumers.len(), 0);
        assert_eq!(options.producers.len(), 0);
    }

    #[test]
    fn bind_empty_and_missing_section_validate_cleanly() {
        for json in [r#"{ "Streams": { } }"#, "{ }"] {
            let options = bind(json);
            validate(&options, PRODUCTION, 5000).expect("zero configuration is legal");
        }
    }

    #[test]
    fn defaults_match_documented_values() {
        let topic = TopicOptions::default();
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.max_len(), 10_000);
        assert_eq!(topic.trim, TrimMode::Approx);
        assert_eq!(topic.retention_seconds, None);
        assert!(topic.co_locate_partitions);
        assert!((topic.clamp_release_threshold - 0.8).abs() < f64::EPSILON);

        let consumer = ConsumerOptions::default();
        assert_eq!(consumer.batch_size, 100);
        assert_eq!(consumer.start_from, StartFrom::Stored);
        assert_eq!(consumer.persist, PersistMode::AsyncBatch);
        assert_eq!(consumer.persist_interval_ms, 1000);
        assert_eq!(consumer.read_mode, ReadMode::Block);
        assert_eq!(consumer.block_ms, 1000);
        assert_eq!(consumer.on_error, ErrorPolicy::BestEffort);
        assert_eq!(consumer.instances, None);

        // Lease, not Static: the enum's zero value (Static) deliberately no longer expresses the default.
        let instances = InstanceOptions::default();
        assert_eq!(instances.mode(), InstanceMode::Lease);
        assert_eq!(instances.count, None);
        assert_eq!(instances.index, None);
        assert_eq!(consumer.start_from_when_missing, StartFrom::Beginning);
        assert!(consumer.backpressure.enabled);
        assert_eq!(consumer.backpressure.capacity, 4);

        let producer = ProducerOptions::default();
        assert!(!producer.buffered);
        assert_eq!(producer.max_wait_ms, 20);
        assert_eq!(producer.max_batch, 500);
        assert_eq!(producer.max_queue, 100_000);
        assert!(!producer.drop_oldest);
    }

    #[test]
    fn resolve_topic_unconfigured_topic_returns_defaults_not_an_error() {
        assert_eq!(
            resolve_topic(&StreamOptions::default(), "feed_source_racing"),
            TopicOptions::default()
        );
    }

    #[test]
    fn resolve_connection_string_unset_uses_redis_db_default() {
        assert_eq!(
            resolve_connection_string(&StreamOptions::default()),
            DEFAULT_CONNECTION_STRING
        );

        let blank = StreamOptions {
            connection_string: Some("   ".to_owned()),
            ..StreamOptions::default()
        };
        assert_eq!(resolve_connection_string(&blank), DEFAULT_CONNECTION_STRING);

        let set = StreamOptions {
            connection_string: Some("localhost:6379".to_owned()),
            ..StreamOptions::default()
        };
        assert_eq!(resolve_connection_string(&set), "localhost:6379");
    }

    #[test]
    fn resolve_sync_timeout_ms_defaults_and_parses() {
        assert_eq!(
            resolve_sync_timeout_ms(None).unwrap(),
            DEFAULT_SYNC_TIMEOUT_MS
        );
        assert_eq!(
            resolve_sync_timeout_ms(Some("localhost:6379,syncTimeout=9000")).unwrap(),
            9000
        );
    }

    #[test]
    fn resolve_consumer_name_prefers_entry_then_root() {
        let options = StreamOptions {
            consumer: Some("root-consumer".to_owned()),
            ..StreamOptions::default()
        };

        let own = ConsumerOptions {
            topic: "t".to_owned(),
            consumer: Some("own".to_owned()),
            ..ConsumerOptions::default()
        };
        assert_eq!(resolve_consumer_name(&options, &own), "own");

        let inherits = ConsumerOptions {
            topic: "t".to_owned(),
            ..ConsumerOptions::default()
        };
        assert_eq!(resolve_consumer_name(&options, &inherits), "root-consumer");
        assert_ne!(
            resolve_consumer_name(&StreamOptions::default(), &inherits).trim(),
            ""
        );
    }

    // ------------------------------------------------------------ ConfigTests: topic rules

    #[test]
    fn validate_topic_partitions_below_one_throws_naming_key() {
        let message = validate_err(
            &with_topic(
                "orders",
                TopicOptions {
                    partitions: 0,
                    ..TopicOptions::default()
                },
            ),
            PRODUCTION,
        );
        assert!(
            message.contains("Streams:Topics:orders:Partitions"),
            "{message}"
        );
    }

    #[test]
    fn validate_topic_partitions_negative_is_a_deserialize_error() {
        let err = bind_json(r#"{ "Streams": { "Topics": { "orders": { "Partitions": -1 } } } }"#);
        assert!(matches!(err, Err(Error::Config(_))), "{err:?}");
    }

    #[test]
    fn validate_topic_retention_below_one_throws_naming_key() {
        let message = validate_err(
            &with_topic(
                "orders",
                TopicOptions {
                    retention_seconds: Some(0),
                    ..TopicOptions::default()
                },
            ),
            PRODUCTION,
        );
        assert!(
            message.contains("Streams:Topics:orders:RetentionSeconds"),
            "{message}"
        );
    }

    /// An explicit `MaxLen: 0` is a ceiling of zero, which is not a ceiling. The omitted-key case is
    /// covered by [`validate_retention_without_max_len_throws_naming_both_keys`].
    #[test]
    fn validate_topic_retention_with_max_len_zero_throws_naming_both_keys() {
        let message = validate_err(
            &with_topic(
                "orders",
                TopicOptions {
                    retention_seconds: Some(60),
                    max_len: Some(0),
                    ..TopicOptions::default()
                },
            ),
            PRODUCTION,
        );

        assert!(message.contains("Streams:Topics:orders:RetentionSeconds"));
        assert!(message.contains("Streams:Topics:orders:MaxLen"));
    }

    #[test]
    fn validate_topic_retention_with_no_max_len_at_all_throws_naming_both_keys() {
        let message = validate_err(
            &with_topic(
                "orders",
                TopicOptions {
                    retention_seconds: Some(60),
                    ..TopicOptions::default()
                },
            ),
            PRODUCTION,
        );

        assert!(message.contains("Streams:Topics:orders:RetentionSeconds"));
        assert!(message.contains("Streams:Topics:orders:MaxLen"));
        assert!(message.contains("not configured"));
    }

    #[test]
    fn validate_trim_none_outside_development_throws_naming_key_and_environment() {
        let message = validate_err(
            &with_topic(
                "orders",
                TopicOptions {
                    trim: TrimMode::None,
                    ..TopicOptions::default()
                },
            ),
            PRODUCTION,
        );

        assert!(message.contains("Streams:Topics:orders:Trim"));
        assert!(message.contains(PRODUCTION));
    }

    #[test]
    fn validate_trim_none_in_development_is_allowed() {
        let options = with_topic(
            "orders",
            TopicOptions {
                trim: TrimMode::None,
                ..TopicOptions::default()
            },
        );

        validate(&options, DEVELOPMENT, 5000).expect("Trim=None is allowed in Development");
    }

    // ------------------------------------------------------------ ConfigTests: consumer rules

    #[test]
    fn validate_consumer_without_topic_throws_naming_key() {
        for topic in ["", "   "] {
            let message = validate_err(
                &with_consumer(ConsumerOptions {
                    topic: topic.to_owned(),
                    ..ConsumerOptions::default()
                }),
                PRODUCTION,
            );
            assert!(message.contains("Streams:Consumers[0]:Topic"), "{message}");
        }
    }

    #[test]
    fn validate_consumer_batch_size_below_one_throws_naming_key() {
        let message = validate_err(
            &with_consumer(ConsumerOptions {
                topic: "orders".to_owned(),
                batch_size: 0,
                ..ConsumerOptions::default()
            }),
            PRODUCTION,
        );
        assert!(
            message.contains("Streams:Consumers[0]:BatchSize"),
            "{message}"
        );
    }

    #[test]
    fn validate_consumer_batch_size_negative_is_a_deserialize_error() {
        let err = bind_json(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "BatchSize": -5 } ] } }"#,
        );
        assert!(matches!(err, Err(Error::Config(_))), "{err:?}");
    }

    #[test]
    fn validate_backpressure_capacity_below_one_throws_naming_key() {
        let message = validate_err(
            &with_consumer(ConsumerOptions {
                topic: "orders".to_owned(),
                backpressure: BackpressureOptions {
                    enabled: true,
                    capacity: 0,
                },
                ..ConsumerOptions::default()
            }),
            PRODUCTION,
        );
        assert!(
            message.contains("Streams:Consumers[0]:Backpressure:Capacity"),
            "{message}"
        );
    }

    #[test]
    fn validate_start_from_date_without_date_throws_naming_key() {
        let message = validate_err(
            &with_consumer(ConsumerOptions {
                topic: "orders".to_owned(),
                start_from: StartFrom::Date,
                ..ConsumerOptions::default()
            }),
            PRODUCTION,
        );

        assert!(message.contains("Streams:Consumers[0]:StartFrom"));
        assert!(message.contains("Streams:Consumers[0]:StartFromDate"));
    }

    #[test]
    fn validate_start_from_date_with_date_is_allowed() {
        let options = with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            start_from: StartFrom::Date,
            start_from_date: Some(datetime!(1970-01-01 00:00:00 UTC)),
            ..ConsumerOptions::default()
        });

        validate(&options, PRODUCTION, 5000).expect("a date is set");
    }

    #[test]
    fn validate_persist_none_with_start_from_stored_throws_naming_both_keys() {
        let message = validate_err(
            &with_consumer(ConsumerOptions {
                topic: "orders".to_owned(),
                persist: PersistMode::None,
                start_from: StartFrom::Stored,
                ..ConsumerOptions::default()
            }),
            PRODUCTION,
        );

        assert!(message.contains("Streams:Consumers[0]:Persist"));
        assert!(message.contains("Streams:Consumers[0]:StartFrom"));
    }

    #[test]
    fn validate_duplicate_topic_consumer_pair_throws_naming_both_entries() {
        let options = StreamOptions {
            consumer: Some("svc".to_owned()),
            consumers: vec![
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    ..ConsumerOptions::default()
                },
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    ..ConsumerOptions::default()
                },
            ],
            ..StreamOptions::default()
        };

        let message = validate_err(&options, PRODUCTION);
        assert!(message.contains("Streams:Consumers[1]"), "{message}");
        assert!(message.contains("Streams:Consumers[0]"), "{message}");
        assert!(message.contains("orders"), "{message}");
    }

    #[test]
    fn validate_same_topic_distinct_consumer_names_is_allowed() {
        let options = StreamOptions {
            consumer: Some("svc".to_owned()),
            consumers: vec![
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    consumer: Some("a".to_owned()),
                    ..ConsumerOptions::default()
                },
                ConsumerOptions {
                    topic: "orders".to_owned(),
                    consumer: Some("b".to_owned()),
                    ..ConsumerOptions::default()
                },
            ],
            ..StreamOptions::default()
        };

        validate(&options, PRODUCTION, 5000).expect("distinct names do not collide");
    }

    #[test]
    fn validate_block_ms_at_or_above_sync_timeout_throws_naming_key() {
        let options = with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            read_mode: ReadMode::Block,
            block_ms: 5000,
            ..ConsumerOptions::default()
        });

        let message = validate(&options, PRODUCTION, 5000)
            .expect_err("BlockMs >= syncTimeout")
            .to_string();
        assert!(
            message.contains("Streams:Consumers[0]:BlockMs"),
            "{message}"
        );
        assert!(message.contains("syncTimeout"), "{message}");
    }

    #[test]
    fn validate_block_ms_ignored_in_poll_mode() {
        let options = with_consumer(ConsumerOptions {
            topic: "orders".to_owned(),
            read_mode: ReadMode::Poll,
            block_ms: 60_000,
            ..ConsumerOptions::default()
        });

        validate(&options, PRODUCTION, 5000).expect("BlockMs is not read in Poll mode");
    }

    // ------------------------------------------------------------ ConfigTests: instance rules

    #[test]
    fn validate_instance_count_below_one_throws_naming_key() {
        let message = validate_err(
            &with_consumer(ConsumerOptions {
                topic: "orders".to_owned(),
                instances: Some(InstanceOptions {
                    mode: Some(InstanceMode::Static),
                    count: Some(0),
                    ..InstanceOptions::default()
                }),
                ..ConsumerOptions::default()
            }),
            PRODUCTION,
        );
        assert!(
            message.contains("Streams:Consumers[0]:Instances:Count"),
            "{message}"
        );
    }

    #[test]
    fn validate_instance_index_negative_is_a_deserialize_error() {
        let err = bind_json(
            r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "Instances": { "Mode": "Static", "Count": 2, "Index": -1 } } ] } }"#,
        );
        assert!(matches!(err, Err(Error::Config(_))), "{err:?}");
    }

    #[test]
    fn validate_instance_index_not_less_than_count_throws_naming_both_keys() {
        let message = validate_err(
            &with_consumer(ConsumerOptions {
                topic: "orders".to_owned(),
                instances: Some(InstanceOptions {
                    mode: Some(InstanceMode::Static),
                    count: Some(2),
                    index: Some(2),
                    ..InstanceOptions::default()
                }),
                ..ConsumerOptions::default()
            }),
            PRODUCTION,
        );

        assert!(message.contains("Streams:Consumers[0]:Instances:Index"));
        assert!(message.contains("Streams:Consumers[0]:Instances:Count"));
    }

    // ------------------------------------------------------------ ConfigTests: producer rules

    #[test]
    fn validate_producer_without_topic_throws_naming_key() {
        for topic in ["", "  "] {
            let message = validate_err(
                &with_producer(ProducerOptions {
                    topic: topic.to_owned(),
                    ..ProducerOptions::default()
                }),
                PRODUCTION,
            );
            assert!(message.contains("Streams:Producers[0]:Topic"), "{message}");
        }
    }

    #[test]
    fn validate_producer_max_batch_below_one_throws_naming_key() {
        let message = validate_err(
            &with_producer(ProducerOptions {
                topic: "orders".to_owned(),
                max_batch: 0,
                ..ProducerOptions::default()
            }),
            PRODUCTION,
        );
        assert!(
            message.contains("Streams:Producers[0]:MaxBatch"),
            "{message}"
        );
    }

    #[test]
    fn validate_producer_max_queue_below_one_throws_naming_key() {
        let message = validate_err(
            &with_producer(ProducerOptions {
                topic: "orders".to_owned(),
                max_queue: 0,
                ..ProducerOptions::default()
            }),
            PRODUCTION,
        );
        assert!(
            message.contains("Streams:Producers[0]:MaxQueue"),
            "{message}"
        );
    }

    #[test]
    fn resolve_sync_timeout_ms_unparseable_connection_string_throws_naming_key() {
        let err = resolve_sync_timeout_ms(Some("localhost:6379,syncTimeout=not-a-number"))
            .expect_err("unparseable");

        assert!(
            err.to_string().contains("Streams:ConnectionString"),
            "{err}"
        );
    }

    // ------------------------------------------------------------ connection string translation

    fn url_of(connection_string: &str) -> String {
        to_redis_url(connection_string).expect("translates")
    }

    #[test]
    fn default_connection_string_translates_to_a_plain_redis_url() {
        assert_eq!(
            url_of(DEFAULT_CONNECTION_STRING),
            "redis://redis-db.infra:6379/0"
        );
    }

    #[test]
    fn endpoint_without_port_gets_the_default_port() {
        assert_eq!(url_of("cache"), "redis://cache:6379/0");
        assert_eq!(url_of("cache:6380"), "redis://cache:6380/0");
    }

    #[test]
    fn bracketed_ipv6_endpoint_is_kept_bracketed() {
        assert_eq!(url_of("[::1]:6380"), "redis://[::1]:6380/0");
        assert_eq!(url_of("[::1]"), "redis://[::1]:6379/0");
    }

    #[test]
    fn unbracketed_ipv6_endpoint_is_refused() {
        let err = to_redis_url("::1:6379").expect_err("needs brackets");
        assert!(
            err.to_string().contains("Streams:ConnectionString"),
            "{err}"
        );
    }

    #[test]
    fn password_is_carried_as_the_redis_password() {
        assert_eq!(
            url_of("localhost:6380,password=s3cret"),
            "redis://:s3cret@localhost:6380/0"
        );
    }

    #[test]
    fn user_and_password_are_both_carried() {
        assert_eq!(
            url_of("localhost:6380,user=bob,password=p"),
            "redis://bob:p@localhost:6380/0"
        );
    }

    #[test]
    fn ssl_true_selects_rediss() {
        assert_eq!(
            url_of("localhost:6380,ssl=true"),
            "rediss://localhost:6380/0"
        );
        assert_eq!(
            url_of("localhost:6380,ssl=false"),
            "redis://localhost:6380/0"
        );
    }

    #[test]
    fn default_database_becomes_the_url_path() {
        assert_eq!(
            url_of("localhost:6379,defaultDatabase=3"),
            "redis://localhost:6379/3"
        );
    }

    #[test]
    fn keys_are_case_insensitive_and_whitespace_tolerant() {
        assert_eq!(
            url_of(" localhost:6379 , SSL = TRUE "),
            "rediss://localhost:6379/0"
        );
    }

    #[test]
    fn ignored_stackexchange_keys_do_not_change_the_url() {
        assert_eq!(
            url_of(
                "localhost:6379,abortConnect=false,syncTimeout=9000,connectTimeout=3000,clientName=svc"
            ),
            "redis://localhost:6379/0"
        );
    }

    #[test]
    fn special_characters_in_the_password_are_percent_encoded_and_round_trip() {
        let password = "p@ss:w/rd #%~ü";
        let url = url_of(&format!("localhost:6379,password={password}"));

        let info = redis::IntoConnectionInfo::into_connection_info(url.as_str())
            .expect("the URL parses in redis-rs");
        assert_eq!(info.redis_settings().password(), Some(password));
    }

    #[test]
    fn sync_timeout_is_read_from_the_connection_string() {
        assert_eq!(
            resolve_sync_timeout_ms(Some("localhost:6379,syncTimeout=9000")).unwrap(),
            9000
        );
        assert_eq!(
            resolve_sync_timeout_ms(Some("localhost:6379")).unwrap(),
            DEFAULT_SYNC_TIMEOUT_MS
        );
    }

    #[test]
    fn unsupported_key_is_refused_naming_the_key() {
        let err = to_redis_url("localhost:6379,bogusOption=1").expect_err("unknown key");
        let message = err.to_string();
        assert!(message.contains("Streams:ConnectionString"), "{message}");
        assert!(message.contains("bogusOption"), "{message}");
    }

    #[test]
    fn multiple_endpoints_are_refused() {
        let err = to_redis_url("a:6379,b:6379").expect_err("single node only");
        assert!(err.to_string().contains("more than one endpoint"));
    }

    #[test]
    fn missing_endpoint_is_refused() {
        assert!(to_redis_url("password=x").is_err());
    }

    #[test]
    fn bad_port_is_refused() {
        assert!(to_redis_url("localhost:notaport").is_err());
    }

    #[test]
    fn error_message_never_echoes_the_password() {
        let err = to_redis_url("localhost:notaport,password=hunter2").expect_err("bad port");
        assert!(!err.to_string().contains("hunter2"));
    }

    // ------------------------------------------------------------ case-insensitive binding

    /// Re-keys every object key of a document with `case`, leaving values and topic names alone.
    fn rekey(value: &serde_json::Value, case: fn(&str) -> String) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => serde_json::Value::Object(
                map.iter()
                    .map(|(k, v)| {
                        let v = if k == "Topics" {
                            match v {
                                serde_json::Value::Object(topics) => serde_json::Value::Object(
                                    topics
                                        .iter()
                                        .map(|(n, t)| (n.clone(), rekey(t, case)))
                                        .collect(),
                                ),
                                other => other.clone(),
                            }
                        } else {
                            rekey(v, case)
                        };
                        (case(k), v)
                    })
                    .collect(),
            ),
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.iter().map(|i| rekey(i, case)).collect())
            }
            other => other.clone(),
        }
    }

    #[test]
    fn enum_values_bind_case_insensitively() {
        for spelling in ["date", "DATE", "Date", "dAtE"] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "StartFrom": "{spelling}", "StartFromDate": "2026-01-01T00:00:00Z" }} ] }} }}"#
            ));
            assert_eq!(
                options.consumers[0].start_from,
                StartFrom::Date,
                "{spelling}"
            );
        }

        for (spelling, expected) in [
            ("syncbatch", PersistMode::SyncBatch),
            ("NONE", PersistMode::None),
        ] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Persist": "{spelling}", "StartFrom": "Now" }} ] }} }}"#
            ));
            assert_eq!(options.consumers[0].persist, expected, "{spelling}");
        }

        for (spelling, expected) in [
            ("stopPartition", ErrorPolicy::StopPartition),
            ("FAIL", ErrorPolicy::Fail),
        ] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "OnError": "{spelling}" }} ] }} }}"#
            ));
            assert_eq!(options.consumers[0].on_error, expected, "{spelling}");
        }

        for (spelling, expected) in [("poll", ReadMode::Poll), ("BLOCK", ReadMode::Block)] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "ReadMode": "{spelling}" }} ] }} }}"#
            ));
            assert_eq!(options.consumers[0].read_mode, expected, "{spelling}");
        }

        for (spelling, expected) in [
            ("lease", InstanceMode::Lease),
            ("LEASE", InstanceMode::Lease),
            ("sTaTiC", InstanceMode::Static),
        ] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Instances": {{ "Mode": "{spelling}" }} }} ] }} }}"#
            ));
            assert_eq!(
                options.consumers[0]
                    .instances
                    .as_ref()
                    .map(InstanceOptions::mode),
                Some(expected),
                "{spelling}"
            );
        }

        for (spelling, expected) in [
            ("workqueue", DeliveryMode::WorkQueue),
            ("ORDERED", DeliveryMode::Ordered),
        ] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "Delivery": "{spelling}" }} ] }} }}"#
            ));
            assert_eq!(options.consumers[0].delivery(), expected, "{spelling}");
        }

        for (spelling, expected) in [
            ("exact", TrimMode::Exact),
            ("APPROX", TrimMode::Approx),
            ("none", TrimMode::None),
        ] {
            let options = bind(&format!(
                r#"{{ "Streams": {{ "Topics": {{ "orders": {{ "Trim": "{spelling}" }} }} }} }}"#
            ));
            assert_eq!(options.topics["orders"].trim, expected, "{spelling}");
        }
    }

    #[test]
    fn unknown_enum_value_is_still_an_error_in_any_case() {
        for spelling in ["Sometimes", "sometimes", "SOMETIMES"] {
            let err = bind_json(&format!(
                r#"{{ "Streams": {{ "Consumers": [ {{ "Topic": "orders", "StartFrom": "{spelling}" }} ] }} }}"#
            ));
            assert!(matches!(err, Err(Error::Config(_))), "{spelling}: {err:?}");
        }
    }

    #[test]
    fn partial_enum_match_is_not_accepted() {
        // Case-insensitive means whole-value equality, not a prefix or substring.
        assert!(
            bind_json(
                r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "ReadMode": "blo" } ] } }"#
            )
            .is_err()
        );
    }

    #[test]
    fn config_keys_bind_case_insensitively() {
        let canonical = bind(FULL_SECTION);
        let document: serde_json::Value = serde_json::from_str(FULL_SECTION).unwrap();

        for (label, case) in [
            ("lower", str::to_ascii_lowercase as fn(&str) -> String),
            ("upper", str::to_ascii_uppercase as fn(&str) -> String),
        ] {
            let text = rekey(&document, case).to_string();
            assert_eq!(bind_json(&text).unwrap(), canonical, "{label}");
        }
    }

    #[test]
    fn section_name_is_case_insensitive() {
        for section in ["Streams", "streams", "STREAMS", "sTrEaMs"] {
            let options = bind(&format!(r#"{{ "{section}": {{ "Consumer": "svc" }} }}"#));
            assert_eq!(options.consumer.as_deref(), Some("svc"), "{section}");
        }
    }

    #[test]
    fn underscored_and_plain_key_spellings_both_bind() {
        // Keys are matched with case and underscores ignored; the field names are the ones in the table.
        let options =
            bind(r#"{ "Streams": { "Consumers": [ { "Topic": "orders", "BATCH_SIZE": 7 } ] } }"#);
        assert_eq!(options.consumers[0].batch_size, 7);
    }

    #[test]
    fn topic_names_keep_their_case() {
        let options = bind(r#"{ "Streams": { "Topics": { "Orders": { "partitions": 3 } } } }"#);

        assert_eq!(options.topics["Orders"].partitions, 3);
        assert!(!options.topics.contains_key("orders"));
    }
}
