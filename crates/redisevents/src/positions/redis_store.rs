//! [`RedisPositionStore`] and [`NullPositionStore`]. **Owner: B4** (port of `RedisPositionStore.cs`,
//! `NullPositionStore.cs`). Struct names, `new` and the trait impls are frozen; fields are B4's.
//!
//! Layout (`WIRE.md` §7): `p:{topic}:<consumer>` holds one field per partition, `<p>` =
//! `<streamId>|<instanceId>`, plus `__reset:<p>` reset markers. `p:{topic}:<consumer>:meta` holds
//! `<p>:updatedUtc` (.NET `"O"`) and `<p>:instanceId` for diagnostics only.

use std::collections::{BTreeMap, HashMap};
use std::future::{Future, ready};

use time::OffsetDateTime;
use time::format_description::FormatItem;
use time::macros::format_description;
use uuid::Uuid;

use super::reset_marker;
use super::{ADMIN_INSTANCE_ID, PositionRecord, PositionStore};
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::wire::{Keys, StreamId};

/// .NET `"O"`: seven fractional digits and a numeric offset (`WIRE.md` §9).
const O_FORMAT: &[FormatItem<'static>] = format_description!(
    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:7][offset_hour sign:mandatory]:[offset_minute]"
);

/// Renders an instant the way .NET's `ToString("O")` does, keeping its offset.
pub(crate) fn format_o(when: OffsetDateTime) -> String {
    when.format(O_FORMAT)
        .expect("the O format describes every OffsetDateTime")
}

/// Positions in `p:{topic}:<consumer>` plus `p:{topic}:<consumer>:meta`.
#[derive(Debug, Clone)]
pub struct RedisPositionStore {
    conn: Connection,
    keys: Keys,
    instance_id: Uuid,
}

impl RedisPositionStore {
    /// A store writing under `instance_id`.
    #[must_use]
    pub fn new(conn: Connection, keys: Keys, instance_id: Uuid) -> Self {
        Self {
            conn,
            keys,
            instance_id,
        }
    }

    /// The instance id stamped on every write.
    #[must_use]
    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    /// Every recorded position of `consumer` on `topic`, with the instance that wrote each one, in
    /// one `HGETALL`. The form the C3 flusher's second-writer check needs; [`PositionStore::load`]
    /// is the same read with the attribution dropped.
    ///
    /// Fields that are not a partition number, and values that are not a position, are skipped
    /// (reset markers among them).
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for an empty topic or consumer, [`Error::Redis`] when the read fails.
    pub async fn load_records(
        &self,
        topic: &str,
        consumer: &str,
    ) -> Result<BTreeMap<u32, PositionRecord>> {
        check_names(topic, consumer)?;
        let entries = hgetall(&self.conn, &self.keys.positions(topic, consumer)).await?;
        Ok(read_positions(&entries))
    }

    /// Writes positions (and reset markers, when `reset_issued` is set), their meta fields, in two
    /// commands on one round trip: the positions hash first, then the meta hash.
    async fn write(
        &self,
        topic: &str,
        consumer: &str,
        positions: &[(u32, StreamId)],
        writer: Uuid,
        reset_issued: Option<OffsetDateTime>,
    ) -> Result<()> {
        let stamp = format_o(OffsetDateTime::now_utc());
        let writer_text = writer.hyphenated().to_string();

        let mut values = Vec::with_capacity(positions.len() * 2);
        let mut meta = Vec::with_capacity(positions.len() * 2);
        for &(partition, id) in positions {
            let record = PositionRecord {
                id,
                instance_id: writer,
            };
            values.push((partition.to_string(), record.format()));
            if let Some(issued) = reset_issued {
                values.push((
                    reset_marker::field(partition),
                    reset_marker::format(id, issued),
                ));
            }
            meta.push((format!("{partition}:updatedUtc"), stamp.clone()));
            meta.push((format!("{partition}:instanceId"), writer_text.clone()));
        }

        let mut manager = self.conn.shared();
        redis::pipe()
            .add_command(hset_cmd(&self.keys.positions(topic, consumer), &values))
            .ignore()
            .add_command(hset_cmd(&self.keys.positions_meta(topic, consumer), &meta))
            .ignore()
            .query_async::<()>(&mut manager)
            .await?;
        Ok(())
    }
}

impl PositionStore for RedisPositionStore {
    async fn load(&self, topic: &str, consumer: &str) -> Result<BTreeMap<u32, StreamId>> {
        Ok(self
            .load_records(topic, consumer)
            .await?
            .into_iter()
            .map(|(partition, record)| (partition, record.id))
            .collect())
    }

    async fn save(&self, topic: &str, consumer: &str, positions: &[(u32, StreamId)]) -> Result<()> {
        check_names(topic, consumer)?;
        // An idle consumer writes nothing at all.
        if positions.is_empty() {
            return Ok(());
        }
        self.write(topic, consumer, positions, self.instance_id, None)
            .await
    }

    /// Writes the position and its `__reset:<p>` marker together, stamped with the admin identity
    /// whatever this store carries, so a running consumer reads it as a rewind rather than as a
    /// rival writer. With `partition` `None`, every partition that already has a recorded position
    /// is moved; partitions with none resolve from their `StartFrom` fallback and are left alone.
    async fn reset(
        &self,
        topic: &str,
        consumer: &str,
        to: StreamId,
        partition: Option<u32>,
    ) -> Result<()> {
        check_names(topic, consumer)?;
        let targets: Vec<(u32, StreamId)> = match partition {
            Some(one) => vec![(one, to)],
            None => self
                .load_records(topic, consumer)
                .await?
                .into_keys()
                .map(|existing| (existing, to))
                .collect(),
        };
        if targets.is_empty() {
            return Ok(());
        }
        self.write(
            topic,
            consumer,
            &targets,
            ADMIN_INSTANCE_ID,
            Some(OffsetDateTime::now_utc()),
        )
        .await
    }
}

/// Stores nothing (`PersistMode::None`): `load` is always empty.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullPositionStore;

impl PositionStore for NullPositionStore {
    fn load(
        &self,
        _topic: &str,
        _consumer: &str,
    ) -> impl Future<Output = Result<BTreeMap<u32, StreamId>>> + Send {
        ready(Ok(BTreeMap::new()))
    }

    fn save(
        &self,
        _topic: &str,
        _consumer: &str,
        _positions: &[(u32, StreamId)],
    ) -> impl Future<Output = Result<()>> + Send {
        ready(Ok(()))
    }

    fn reset(
        &self,
        _topic: &str,
        _consumer: &str,
        _to: StreamId,
        _partition: Option<u32>,
    ) -> impl Future<Output = Result<()>> + Send {
        ready(Ok(()))
    }
}

/// A partition number as a hash field name: ASCII digits only, in `u32` range. Anything else
/// (`__reset:3`, `0:updatedUtc`, `+1`, `-1`) is not a partition.
pub(crate) fn partition_number(text: &str) -> Option<u32> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// The positions in a positions hash, skipping every field that is not a partition with a valid
/// `<streamId>[|<instanceId>]` value.
pub(crate) fn read_positions(entries: &[(String, String)]) -> BTreeMap<u32, PositionRecord> {
    entries
        .iter()
        .filter_map(|(name, value)| Some((partition_number(name)?, PositionRecord::parse(value)?)))
        .collect()
}

/// `HGETALL` as text pairs. Bytes that are not UTF-8 become replacement characters, which then fail
/// to parse and are skipped like any other non-position field.
pub(crate) async fn hgetall(conn: &Connection, key: &str) -> Result<Vec<(String, String)>> {
    let mut manager = conn.shared();
    let raw: HashMap<Vec<u8>, Vec<u8>> = redis::cmd("HGETALL")
        .arg(key)
        .query_async(&mut manager)
        .await?;
    Ok(raw
        .into_iter()
        .map(|(name, value)| {
            (
                String::from_utf8_lossy(&name).into_owned(),
                String::from_utf8_lossy(&value).into_owned(),
            )
        })
        .collect())
}

fn hset_cmd(key: &str, pairs: &[(String, String)]) -> redis::Cmd {
    let mut cmd = redis::cmd("HSET");
    cmd.arg(key);
    for (field, value) in pairs {
        cmd.arg(field).arg(value);
    }
    cmd
}

fn check_names(topic: &str, consumer: &str) -> Result<()> {
    if topic.trim().is_empty() {
        return Err(Error::InvalidArgument("topic must not be empty".into()));
    }
    if consumer.trim().is_empty() {
        return Err(Error::InvalidArgument("consumer must not be empty".into()));
    }
    Ok(())
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
    use super::*;
    use crate::connection::Connection;
    use crate::wire::Keys;
    use crate::wire::StreamId;
    use redisevents_test_support::{RedisFixture, require_redis};

    const FORMATS: &str = include_str!("../../../../wire-spec/vectors/formats.json");

    fn formats() -> serde_json::Value {
        serde_json::from_str(FORMATS).expect("formats.json parses")
    }

    fn str_of<'a>(v: &'a serde_json::Value, key: &str) -> &'a str {
        v[key].as_str().expect("string field")
    }

    #[test]
    fn position_value_matches_vectors() {
        let formats = formats();
        for case in formats["position_value"].as_array().expect("array") {
            let record = PositionRecord {
                id: StreamId::parse(str_of(case, "id")).expect("valid id"),
                instance_id: Uuid::parse_str(str_of(case, "instanceId")).expect("valid guid"),
            };
            assert_eq!(record.format(), str_of(case, "value"));
            assert_eq!(PositionRecord::parse(str_of(case, "value")), Some(record));
        }
        assert_eq!(
            ADMIN_INSTANCE_ID.hyphenated().to_string(),
            str_of(&formats, "admin_instance_id")
        );
    }

    #[test]
    fn datetime_offset_o_matches_vectors() {
        const EPOCH_TICKS: i128 = 621_355_968_000_000_000;
        let formats = formats();
        for case in formats["datetime_offset_o"].as_array().expect("array") {
            // `ticks` is the wall-clock time in .NET ticks, so the UTC instant is that minus the offset.
            let ticks: i128 = str_of(case, "ticks").parse().expect("ticks");
            let offset_minutes = case["offsetMinutes"].as_i64().expect("offset");
            let utc_nanos =
                (ticks - EPOCH_TICKS) * 100 - i128::from(offset_minutes) * 60_000_000_000;
            let offset = time::UtcOffset::from_whole_seconds(
                i32::try_from(offset_minutes * 60).expect("offset in range"),
            )
            .expect("valid offset");
            let when = OffsetDateTime::from_unix_timestamp_nanos(utc_nanos)
                .expect("in range")
                .to_offset(offset);
            assert_eq!(format_o(when), str_of(case, "o"));
        }
    }

    #[test]
    fn read_positions_skips_non_numeric_fields() {
        let entries = [
            ("0", "12-3|00000000-0000-0000-0000-000000000001"),
            ("0:updatedUtc", "2026-09-06T00:00:00.0000000Z"),
            ("__reset:1", "8-1|2026-10-25T12:00:00.0000000+00:00"),
            ("+2", "1-0"),
            ("-3", "1-0"),
            ("4", "not-an-id"),
            ("5", "77-2"),
        ]
        .map(|(a, b)| (a.to_owned(), b.to_owned()));

        let positions = read_positions(&entries);

        assert_eq!(positions.len(), 2);
        assert_eq!(positions[&0].id, StreamId::new(12, 3));
        assert_eq!(
            positions[&5],
            PositionRecord {
                id: StreamId::new(77, 2),
                instance_id: Uuid::nil()
            }
        );
    }

    #[test]
    fn position_record_round_trips() {
        let instance = Uuid::new_v4();
        let record = PositionRecord {
            id: StreamId::new(123, 45),
            instance_id: instance,
        };
        let formatted = record.format();

        assert_eq!(formatted, format!("123-45|{}", instance.hyphenated()));
        assert_eq!(PositionRecord::parse(&formatted), Some(record));
    }

    #[test]
    fn position_record_bare_id_parses_as_unattributed() {
        assert_eq!(
            PositionRecord::parse("77-2"),
            Some(PositionRecord {
                id: StreamId::new(77, 2),
                instance_id: Uuid::nil(),
            })
        );
        let loose = PositionRecord::parse("77-2|not-a-guid").expect("a bad guid keeps the id");
        assert_eq!(loose.instance_id, Uuid::nil());
        assert_eq!(PositionRecord::parse("nonsense"), None);
    }

    #[tokio::test]
    async fn null_position_store_loads_nothing_and_writes_nothing() {
        let store = NullPositionStore;
        assert!(store.load("orders", "svc").await.expect("load").is_empty());
        store
            .save("orders", "svc", &[(0, StreamId::new(1, 0))])
            .await
            .expect("save");
        store
            .reset("orders", "svc", StreamId::new(1, 0), Some(0))
            .await
            .expect("reset");
    }

    #[tokio::test]
    async fn save_then_load_round_trips_and_writes_meta() {
        let redis = require_redis!();
        let topic = redis.new_topic("pos");
        let consumer = redis.new_consumer("svc");
        let writer = Uuid::from_u128(0x1111_1111_1111_4111_8111_1111_1111_1111);
        let store = RedisPositionStore::new(conn(redis).await, keys(), writer);

        store
            .save(
                &topic,
                &consumer,
                &[(0, StreamId::new(5, 1)), (3, StreamId::new(9, 0))],
            )
            .await
            .expect("save");

        let loaded = store.load(&topic, &consumer).await.expect("load");
        assert_eq!(
            loaded,
            BTreeMap::from([(0, StreamId::new(5, 1)), (3, StreamId::new(9, 0))])
        );
        let records = store
            .load_records(&topic, &consumer)
            .await
            .expect("records");
        assert_eq!(records[&3].instance_id, writer);

        let mut raw = conn(redis).await.shared();
        let meta_key = keys().positions_meta(&topic, &consumer);
        let stamp: String = redis::cmd("HGET")
            .arg(&meta_key)
            .arg("0:updatedUtc")
            .query_async(&mut raw)
            .await
            .expect("meta field");
        assert_eq!(stamp.len(), 33, "O format: 7 fractional digits and +00:00");
        assert!(stamp.ends_with("+00:00"));
        let owner: String = redis::cmd("HGET")
            .arg(&meta_key)
            .arg("0:instanceId")
            .query_async(&mut raw)
            .await
            .expect("meta owner");
        assert_eq!(owner, writer.hyphenated().to_string());
    }

    #[tokio::test]
    async fn load_skips_non_numeric_fields_in_redis() {
        let redis = require_redis!();
        let topic = redis.new_topic("skip");
        let consumer = redis.new_consumer("svc");
        let key = keys().positions(&topic, &consumer);
        let mut raw = conn(redis).await.shared();
        redis::cmd("HSET")
            .arg(&key)
            .arg("0")
            .arg("4-1|6f72616e-6765-4164-6d69-6e5265736574")
            .arg("__reset:0")
            .arg("9-9|2026-10-25T12:00:00.0000000+00:00")
            .arg("0:updatedUtc")
            .arg("2026-10-25T12:00:00.0000000+00:00")
            .query_async::<()>(&mut raw)
            .await
            .expect("seed");

        let store = RedisPositionStore::new(conn(redis).await, keys(), Uuid::new_v4());
        let loaded = store.load(&topic, &consumer).await.expect("load");

        assert_eq!(loaded, BTreeMap::from([(0, StreamId::new(4, 1))]));
    }

    #[tokio::test]
    async fn reset_writes_marker_under_admin_identity() {
        let redis = require_redis!();
        let topic = redis.new_topic("reset");
        let consumer = redis.new_consumer("svc");
        let store = RedisPositionStore::new(conn(redis).await, keys(), Uuid::new_v4());

        // Nothing recorded yet: a reset with no partition writes nothing.
        store
            .reset(&topic, &consumer, StreamId::MIN, None)
            .await
            .expect("noop reset");
        assert!(
            store
                .load(&topic, &consumer)
                .await
                .expect("load")
                .is_empty()
        );

        store
            .save(
                &topic,
                &consumer,
                &[(0, StreamId::new(50, 0)), (2, StreamId::new(60, 0))],
            )
            .await
            .expect("save");
        store
            .reset(&topic, &consumer, StreamId::new(10, 0), None)
            .await
            .expect("reset all");

        let records = store
            .load_records(&topic, &consumer)
            .await
            .expect("records");
        assert_eq!(records.len(), 2, "only recorded partitions move");
        for record in records.values() {
            assert_eq!(record.id, StreamId::new(10, 0));
            assert_eq!(record.instance_id, ADMIN_INSTANCE_ID);
        }

        let mut raw = conn(redis).await.shared();
        let marker: Option<String> = redis::cmd("HGET")
            .arg(keys().positions(&topic, &consumer))
            .arg("__reset:2")
            .query_async(&mut raw)
            .await
            .expect("marker");
        let marker = marker.expect("marker written with the reset");
        assert!(
            marker.starts_with("10-0|") && marker.ends_with("+00:00"),
            "{marker}"
        );

        store
            .reset(&topic, &consumer, StreamId::new(20, 0), Some(7))
            .await
            .expect("reset one");
        let records = store
            .load_records(&topic, &consumer)
            .await
            .expect("records");
        assert_eq!(records[&7].id, StreamId::new(20, 0));
        assert_eq!(
            records.len(),
            3,
            "a named partition is added, the others untouched"
        );
    }
}
