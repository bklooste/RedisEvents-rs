//! Topic meta (`WIRE.md` §3). **Owner: B1** (port of the meta part of `Admin/StreamAdmin.cs`).
//! B1 exposes `pub async fn ensure_topic_meta(conn, keys, topic, &TopicOptions) -> Result<TopicMeta>`
//! here; C4 calls it at startup too.
//! Operator and startup operations on a topic. Files:
//!
//! | File | WP | Contents |
//! | --- | --- | --- |
//! | `meta.rs` | B1 | topic meta `m:{topic}`: `HSETNX` of `partitions`, `v`, `createdUtc`, `maxLen`; grow = warn + overwrite, shrink = refuse |
//! | `stream_admin.rs` | D1 | `StreamAdmin`: reset/preview, ownership view, `read_topic_tail` |
//! | `health.rs` | D2 | `StreamHealth`, `StreamLag`, `StreamStatus`, `StreamPartitionStatus` |
//!
//! `StreamAdmin` and the health types are public API added by D1/D2 (lead review per
//! `docs/CONTRACTS.md`); `redisevents-axum` (D3) consumes them.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock, PoisonError};

use time::OffsetDateTime;
use time::macros::format_description;

use crate::config::TopicOptions;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::wire::Keys;
use crate::wire::codec::CODEC_VERSION;

/// Field of `m:{topic}` holding the codec version the topic was created under.
const FIELD_VERSION: &str = "v";
/// Field of `m:{topic}` holding the partition count.
const FIELD_PARTITIONS: &str = "partitions";
/// Field of `m:{topic}` holding the creation time, .NET `"O"` format.
const FIELD_CREATED_UTC: &str = "createdUtc";
/// Field of `m:{topic}` holding the `MAXLEN` in force when the topic was created (a breadcrumb,
/// not the live value: trimming is driven by each process's own configuration).
const FIELD_MAX_LEN: &str = "maxLen";

/// .NET `DateTimeOffset.ToString("O")` for UTC: `yyyy-MM-ddTHH:mm:ss.fffffff+00:00`.
const CREATED_UTC_FORMAT: &[time::format_description::BorrowedFormatItem<'static>] = format_description!(
    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:7][offset_hour sign:mandatory]:[offset_minute]"
);

/// The topic meta as it stands after [`ensure_topic_meta`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicMeta {
    partitions: u32,
}

impl TopicMeta {
    /// The partition count in force: the configured one, which is at least the recorded one.
    #[must_use]
    pub const fn partitions(self) -> u32 {
        self.partitions
    }

    /// The codec version this build writes (`v`).
    #[must_use]
    pub const fn codec_version(self) -> u32 {
        CODEC_VERSION
    }
}

/// `(meta key, partitions)` pairs this process has already reconciled. The reconcile is idempotent
/// but costs several round trips, and every publisher and consumer asks for it at startup. The
/// partition count is part of the key so a different configuration is still checked.
fn ensured() -> &'static Mutex<HashSet<String>> {
    static ENSURED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    ENSURED.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Records the topic's meta and reconciles its partition count, creating every partition stream
/// that does not exist yet. Safe to call from every process on every start.
///
/// The partition count may only grow. Growing is lossless but breaks per-key ordering while a
/// backlog remains on the old partitions, so it is logged at WARN. Shrinking would orphan
/// unprocessed entries, so it is refused. A recorded codec version this build cannot read is also
/// refused, as is a recorded count that is not a positive integer being treated as absent.
///
/// # Errors
/// [`Error::Config`] when the configured count is below one, is below the recorded count, or the
/// topic was created under another codec version; [`Error::Redis`] when Redis fails.
pub async fn ensure_topic_meta(
    conn: &Connection,
    keys: &Keys,
    topic: &str,
    options: &TopicOptions,
) -> Result<TopicMeta> {
    if topic.trim().is_empty() {
        return Err(Error::invalid_argument("topic name must not be empty"));
    }
    let configured = options.partitions;
    if configured < 1 {
        return Err(Error::config(format!(
            "Topic '{topic}' is configured with {configured} partitions; at least one is required."
        )));
    }

    let meta_key = keys.topic_meta(topic);
    let guard = format!("{meta_key}|{configured}");
    if ensured()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&guard)
    {
        return Ok(TopicMeta {
            partitions: configured,
        });
    }

    let mut shared = conn.shared();

    // Seed with HSETNX so two processes starting together cannot overwrite each other, and a
    // re-run never rewrites the creation breadcrumbs.
    let created = OffsetDateTime::now_utc()
        .format(CREATED_UTC_FORMAT)
        .map_err(|e| Error::transport_with("could not format the topic creation time", e))?;
    let _: Vec<redis::Value> = redis::pipe()
        .cmd("HSETNX")
        .arg(&meta_key)
        .arg(FIELD_PARTITIONS)
        .arg(configured)
        .cmd("HSETNX")
        .arg(&meta_key)
        .arg(FIELD_VERSION)
        .arg(CODEC_VERSION)
        .cmd("HSETNX")
        .arg(&meta_key)
        .arg(FIELD_CREATED_UTC)
        .arg(created)
        .cmd("HSETNX")
        .arg(&meta_key)
        .arg(FIELD_MAX_LEN)
        .arg(options.max_len())
        .query_async(&mut shared)
        .await?;

    let (recorded_partitions, recorded_version): (Option<String>, Option<String>) =
        redis::cmd("HMGET")
            .arg(&meta_key)
            .arg(FIELD_PARTITIONS)
            .arg(FIELD_VERSION)
            .query_async(&mut shared)
            .await?;

    let recorded = recorded_partitions
        .as_deref()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(configured);
    if let Some(version) = recorded_version
        .as_deref()
        .and_then(|v| v.parse::<u32>().ok())
        && version != CODEC_VERSION
    {
        return Err(Error::config(format!(
            "Topic '{topic}' was created under stream codec version {version}, but this build writes and reads version {CODEC_VERSION}. Refusing to start rather than mis-parse entries; upgrade the service or check the 'v' field of the topic's m:{{topic}} hash."
        )));
    }

    if configured < recorded {
        return Err(Error::config(format!(
            "Topic '{topic}' is recorded with {recorded} partitions but this process is configured for {configured}. Decreasing the partition count would orphan every unprocessed message on the partitions that fall out of range, so it is refused. Restore the configured count, or migrate deliberately to a new topic name."
        )));
    }

    ensure_streams_exist(&mut shared, keys, topic, options).await?;

    if configured > recorded {
        let _: () = redis::cmd("HSET")
            .arg(&meta_key)
            .arg(FIELD_PARTITIONS)
            .arg(configured)
            .query_async(&mut shared)
            .await?;
        tracing::warn!(
            topic,
            old_partitions = recorded,
            new_partitions = configured,
            "stream topic partition count increased; no messages are lost, but per-key ordering \
             breaks for as long as a backlog remains on the original partitions. Drain to near-zero \
             lag before increasing."
        );
    }

    ensured()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(guard);
    Ok(TopicMeta {
        partitions: configured,
    })
}

/// Creates each missing partition stream empty, via `XGROUP CREATE … MKSTREAM` and a scratch
/// group that is destroyed again (`XADD … NOMKSTREAM` cannot create a stream). Without this a
/// never-published topic makes `XLEN` and lag reads error instead of returning zero.
async fn ensure_streams_exist(
    shared: &mut redis::aio::ConnectionManager,
    keys: &Keys,
    topic: &str,
    options: &TopicOptions,
) -> Result<()> {
    let names: Vec<String> = (0..options.partitions)
        .map(|p| keys.stream(topic, p, options.co_locate_partitions))
        .collect();

    let mut probe = redis::pipe();
    for name in &names {
        probe.cmd("EXISTS").arg(name);
    }
    let exists: Vec<bool> = probe.query_async(shared).await?;

    // A per-call scratch group: two processes racing here both create and both destroy, and it
    // can never collide with a consumer group someone else owns.
    let scratch = format!("__redisevents-ensure-{}", uuid::Uuid::new_v4().simple());
    for (name, present) in names.iter().zip(exists) {
        if present {
            continue;
        }
        let created: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(name)
            .arg(&scratch)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(shared)
            .await;
        match created {
            Ok(()) => {}
            // Another process created the same scratch group first; the stream exists regardless.
            Err(e) if e.to_string().contains("BUSYGROUP") => {}
            Err(e) => return Err(e.into()),
        }
        let _: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(name)
            .arg(&scratch)
            .query_async(shared)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_utc_uses_the_dotnet_o_shape() {
        let when = time::macros::datetime!(2026-10-25 09:30:01.123_456_7 UTC);
        assert_eq!(
            when.format(CREATED_UTC_FORMAT).unwrap(),
            "2026-10-25T09:30:01.1234567+00:00"
        );
    }
}
