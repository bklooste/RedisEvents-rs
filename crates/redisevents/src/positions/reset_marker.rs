//! Reset markers, `__reset:<p>` = `<targetId>|<issuedUtc>` (`WIRE.md` §7). **Owner: B4** (port of
//! `ResetMarker.cs`). Crate-private; C3 applies them, D1 writes them.
//!
//! The admin write path is [`RedisPositionStore::reset`](super::RedisPositionStore), which stores the
//! marker in the same `HSET` as the position. The live hand-off (`ResetSignal`) and the flusher's
//! clearing are C3's; this module owns the encoding and the cold-start path.
// C2 (`read_from`) and C3 (`take_all`, `read_markers`) consume these; until then only tests do.

use std::collections::BTreeMap;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::datetime;

use super::StartPosition;
use super::redis_store::{format_o, hgetall, partition_number};
use crate::connection::Connection;
use crate::error::Result;
use crate::wire::{Keys, StreamId};

/// The hash field prefix; the full field is `__reset:<partition>`.
pub(crate) const PREFIX: &str = "__reset";

const SEPARATOR: char = '|';

/// One pending reset: where a partition must restart, and when the operator asked for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResetMarker {
    /// The partition the reset applies to.
    pub(crate) partition: u32,
    /// The id to restart after (exclusive, as a stored position).
    pub(crate) target: StreamId,
    /// When the reset was issued, or `0001-01-01T00:00:00Z` when the marker carried no time.
    pub(crate) issued_utc: OffsetDateTime,
}

/// The issue time of a marker that carries none (C# `DateTimeOffset.MinValue`).
fn no_time() -> OffsetDateTime {
    datetime!(0001-01-01 0:00 UTC)
}

/// The marker field for one partition: `__reset:<partition>`.
pub(crate) fn field(partition: u32) -> String {
    format!("{PREFIX}:{partition}")
}

/// The marker value: `<targetId>|<issuedUtc>` with the time in .NET `"O"` form.
pub(crate) fn format(target: StreamId, issued_utc: OffsetDateTime) -> String {
    format!("{target}{SEPARATOR}{}", format_o(issued_utc))
}

/// Parses a marker value. The time is optional on read: a bare `<targetId>` gets the minimum time.
/// `None` when the target id does not parse.
pub(crate) fn parse(value: &str) -> Option<(StreamId, OffsetDateTime)> {
    let (id_text, time_text) = match value.split_once(SEPARATOR) {
        Some((id, time)) => (id, Some(time)),
        None => (value, None),
    };
    let target = StreamId::parse(id_text).ok()?;
    let issued = time_text
        .and_then(|text| OffsetDateTime::parse(text, &Rfc3339).ok())
        .unwrap_or_else(no_time);
    Some((target, issued))
}

/// The partition a marker field names, or `None` for any other field.
fn partition_of_field(name: &str) -> Option<u32> {
    partition_number(name.strip_prefix(PREFIX)?.strip_prefix(':')?)
}

/// The reset markers in a positions hash, by partition. Positions and unparseable values are
/// skipped.
pub(crate) fn read_markers(entries: &[(String, String)]) -> BTreeMap<u32, ResetMarker> {
    entries
        .iter()
        .filter_map(|(name, value)| {
            let partition = partition_of_field(name)?;
            let (target, issued_utc) = parse(value)?;
            Some((
                partition,
                ResetMarker {
                    partition,
                    target,
                    issued_utc,
                },
            ))
        })
        .collect()
}

/// Overrides a resolved start with a marker's target when the partition has one. A marker wins over
/// every `StartFrom` mode, `Now` included: the operator asked for the replay.
pub(crate) fn apply_to_start(
    resolved: StartPosition,
    markers: Option<&BTreeMap<u32, ResetMarker>>,
    partition: u32,
) -> StartPosition {
    markers
        .and_then(|found| found.get(&partition))
        .map_or(resolved, |marker| StartPosition::After(marker.target))
}

/// The cold path: reads every marker on a consumer's positions hash, deletes them, and returns them
/// so each partition starts at its target. Runs before any read loop exists, so nothing races it.
/// The positions themselves are left as the admin write left them.
///
/// # Errors
/// [`Error::Redis`](crate::Error::Redis) when the read or the delete fails.
pub(crate) async fn take_all(
    conn: &Connection,
    keys: &Keys,
    topic: &str,
    consumer: &str,
) -> Result<BTreeMap<u32, ResetMarker>> {
    let key = keys.positions(topic, consumer);
    let entries = hgetall(conn, &key).await?;
    let markers = read_markers(&entries);
    if markers.is_empty() {
        return Ok(markers);
    }

    for marker in markers.values() {
        tracing::warn!(
            topic,
            consumer,
            partition = marker.partition,
            target = %marker.target,
            issued_utc = %format_o(marker.issued_utc),
            "applying a reset marker at startup: reads start after the target; entries after it are processed again, so handlers must be idempotent"
        );
    }

    let fields: Vec<String> = markers.keys().map(|&partition| field(partition)).collect();
    let mut manager = conn.shared();
    redis::cmd("HDEL")
        .arg(&key)
        .arg(&fields)
        .query_async::<()>(&mut manager)
        .await?;
    Ok(markers)
}

#[cfg(test)]
mod tests {

    /// A connection and keys built from this crate's own types. The fixture's `connect()` and
    /// `keys()` return the library's types as compiled for the test crate, which are a different
    /// copy of `redisevents` from the one under test, so they are not used.
    async fn conn(redis: &RedisFixture) -> Connection {
        Connection::connect(redis.url()).await.expect("connect")
    }

    fn keys() -> Keys {
        Keys::with_prefix("test:re:")
    }
    use super::super::redis_store::read_positions;
    use super::*;
    use crate::connection::Connection;
    use crate::wire::Keys;
    use redisevents_test_support::{RedisFixture, require_redis};
    use uuid::Uuid;

    const FORMATS: &str = include_str!("../../../../wire-spec/vectors/formats.json");

    fn str_of<'a>(v: &'a serde_json::Value, key: &str) -> &'a str {
        v[key].as_str().expect("string field")
    }

    fn at(text: &str) -> OffsetDateTime {
        OffsetDateTime::parse(text, &Rfc3339).expect("valid instant")
    }

    #[test]
    fn field_name_and_value_match_vectors() {
        let formats: serde_json::Value = serde_json::from_str(FORMATS).expect("parses");
        let case = &formats["reset_marker"];
        let target = StreamId::parse(str_of(case, "target")).expect("valid");
        let issued = at(str_of(case, "issuedUtc"));

        assert_eq!(field(3), str_of(case, "field"));
        assert_eq!(format(target, issued), str_of(case, "value"));
    }

    #[test]
    fn reset_marker_parse_matches_vectors() {
        let formats: serde_json::Value = serde_json::from_str(FORMATS).expect("parses");
        for case in formats["reset_marker_parse"].as_array().expect("array") {
            let parsed = parse(str_of(case, "value"));
            assert_eq!(parsed.is_some(), case["valid"].as_bool().expect("bool"));
            if let Some((target, issued)) = parsed {
                assert_eq!(
                    target,
                    StreamId::parse(str_of(case, "target")).expect("valid")
                );
                assert_eq!(format_o(issued), str_of(case, "issuedUtc"));
            }
        }
    }

    #[test]
    fn marker_time_is_optional_on_read() {
        let (target, issued) = parse("5-0").expect("bare id is a marker");
        assert_eq!(target, StreamId::new(5, 0));
        assert_eq!(issued, no_time());
        assert!(parse("5-x|2026-10-25T12:00:00Z").is_none());
    }

    #[test]
    fn read_markers_skips_positions_and_unparseable_values() {
        let entries = [
            ("0", "12-3|00000000-0000-0000-0000-000000000001"),
            ("0:updatedUtc", "2026-09-06T00:00:00.0000000Z"),
            ("__reset:1", "8-1|1970-01-01T00:00:00.0000000+00:00"),
            ("__reset:2", "not-an-id"),
            ("__reset:notanumber", "1-0"),
        ]
        .map(|(a, b)| (a.to_owned(), b.to_owned()));

        let markers = read_markers(&entries);

        assert_eq!(markers.len(), 1);
        assert_eq!(
            markers[&1],
            ResetMarker {
                partition: 1,
                target: StreamId::new(8, 1),
                issued_utc: at("1970-01-01T00:00:00Z"),
            }
        );

        // The same hash read as positions must see the position and none of the markers.
        let positions = read_positions(&entries);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[&0].id, StreamId::new(12, 3));
    }

    #[test]
    fn apply_to_start_prefers_the_marker_over_the_configured_start() {
        let markers = BTreeMap::from([(
            0,
            ResetMarker {
                partition: 0,
                target: StreamId::new(10, 0),
                issued_utc: no_time(),
            },
        )]);

        assert_eq!(
            apply_to_start(StartPosition::Now, Some(&markers), 0),
            StartPosition::After(StreamId::new(10, 0))
        );
        assert_eq!(
            apply_to_start(StartPosition::Now, Some(&markers), 1),
            StartPosition::Now
        );
        assert_eq!(
            apply_to_start(StartPosition::BEGINNING, None, 0),
            StartPosition::BEGINNING
        );
    }

    async fn seed(conn: &Connection, key: &str, pairs: &[(&str, &str)]) {
        let mut manager = conn.shared();
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key);
        for (field, value) in pairs {
            cmd.arg(field).arg(value);
        }
        cmd.query_async::<()>(&mut manager).await.expect("seed");
    }

    #[tokio::test]
    async fn reset_cold_is_applied_at_startup_and_cleared() {
        let redis = require_redis!();
        let topic = redis.new_topic("cold");
        let consumer = redis.new_consumer("svc");
        let key = keys().positions(&topic, &consumer);
        let conn = conn(redis).await;
        let issued = OffsetDateTime::now_utc();
        let marker0 = format(StreamId::new(10, 0), issued);
        let marker3 = format(StreamId::new(20, 0), issued);
        seed(
            &conn,
            &key,
            &[
                ("0", "50-0|6f72616e-6765-4164-6d69-6e5265736574"),
                ("__reset:0", &marker0),
                ("__reset:3", &marker3),
            ],
        )
        .await;

        let taken = take_all(&conn, &keys(), &topic, &consumer)
            .await
            .expect("take");

        assert_eq!(taken.len(), 2);
        assert_eq!(taken[&0].target, StreamId::new(10, 0));
        assert_eq!(taken[&3].target, StreamId::new(20, 0));

        let mut manager = conn.shared();
        let left: Vec<String> = redis::cmd("HKEYS")
            .arg(&key)
            .query_async(&mut manager)
            .await
            .expect("keys");
        assert_eq!(left, vec!["0".to_owned()], "markers cleared, position kept");

        assert_eq!(
            apply_to_start(StartPosition::Now, Some(&taken), 0),
            StartPosition::After(StreamId::new(10, 0))
        );
    }

    #[tokio::test]
    async fn reset_cold_with_no_markers_reads_nothing_back() {
        let redis = require_redis!();
        let topic = redis.new_topic("cold-none");
        let consumer = redis.new_consumer("svc");
        let key = keys().positions(&topic, &consumer);
        let conn = conn(redis).await;
        seed(&conn, &key, &[("0", &format!("50-0|{}", Uuid::nil()))]).await;

        let taken = take_all(&conn, &keys(), &topic, &consumer)
            .await
            .expect("take");

        assert!(taken.is_empty());
        let mut manager = conn.shared();
        let position: Option<String> = redis::cmd("HGET")
            .arg(&key)
            .arg("0")
            .query_async(&mut manager)
            .await
            .expect("position");
        assert!(position.is_some(), "nothing was deleted");
    }
}
