//! [`StreamPublisher`], the direct publisher. **Owner: B1** (port of `StreamPublisher.cs`; topic meta
//! via `admin::meta`). The struct and its trait impl are part of the frozen surface; the private
//! fields and the constructor's internals are B1's.

use bytes::Bytes;

use super::{PublishOptions, Publisher};
use crate::admin::meta::ensure_topic_meta;
use crate::config::TopicOptions;
use crate::connection::Connection;
use crate::diagnostics::activity::{current_traceparent, start_publish};
use crate::diagnostics::instruments::record_published;
use crate::error::{Error, Result};
use crate::wire::codec::{self, EntryFields};
use crate::wire::{HeaderBlock, Keys, RoundRobin, StreamId, partition_for_key};

/// Publishes straight to Redis: one `XADD` per message (pipelined for a batch) on the shared
/// connection, routed by key. Ensures the topic meta (`m:{topic}`) on construction, refusing a
/// partition shrink.
///
/// Cancel-safety: dropping a publish future before it resolves leaves it unknown whether the entry
/// was written, as with a cancelled C# call. There is no retry.
#[derive(Debug)]
pub struct StreamPublisher {
    conn: Connection,
    options: TopicOptions,
    router: Router,
    layout: Layout,
}

impl StreamPublisher {
    /// A publisher for `topic`, ensuring its meta hash first.
    ///
    /// # Errors
    /// [`Error::Config`](crate::Error::Config) when the configured partition count is below the
    /// recorded one or the recorded codec version is unsupported; [`Error::Redis`](crate::Error::Redis)
    /// when Redis fails.
    pub async fn new(
        conn: Connection,
        keys: Keys,
        topic: impl Into<String>,
        options: TopicOptions,
    ) -> Result<Self> {
        let topic = topic.into();
        ensure_topic_meta(&conn, &keys, &topic, &options).await?;
        let layout = Layout {
            streams: (0..options.partitions)
                .map(|p| keys.stream(&topic, p, options.co_locate_partitions))
                .collect(),
            trim: codec::max_len_args(options.trim, options.max_len()),
            topic: topic.clone(),
        };
        let router = Router {
            topic,
            partitions: options.partitions,
            round_robin: RoundRobin::new(),
        };
        Ok(Self {
            conn,
            options,
            router,
            layout,
        })
    }

    /// The topic this publisher writes to.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.layout.topic
    }

    /// The topic options in effect.
    #[must_use]
    pub fn options(&self) -> &TopicOptions {
        &self.options
    }
}

/// The routing rule for one topic (`WIRE.md` §6 and the explicit-partition override).
#[derive(Debug)]
struct Router {
    topic: String,
    partitions: u32,
    round_robin: RoundRobin,
}

impl Router {
    /// An explicit partition wins outright; otherwise a non-empty key hashes and an empty key
    /// round-robins.
    fn route(&self, partition_key: &str, explicit: Option<u32>) -> Result<u32> {
        route(
            &self.topic,
            self.partitions,
            &self.round_robin,
            partition_key,
            explicit,
        )
    }
}

/// The routing rule, shared with the outbox so an outbox publish and a plain publish of one key
/// land in one partition: an explicit partition wins outright (and must be in range); otherwise a
/// non-empty key hashes and an empty key round-robins.
pub(super) fn route(
    topic: &str,
    partitions: u32,
    round_robin: &RoundRobin,
    partition_key: &str,
    explicit: Option<u32>,
) -> Result<u32> {
    if let Some(chosen) = explicit {
        if chosen >= partitions {
            return Err(Error::invalid_argument(format!(
                "Topic '{topic}' has {partitions} partition(s), so an explicit partition must be in [0, {}].",
                partitions.saturating_sub(1)
            )));
        }
        return Ok(chosen);
    }
    if partition_key.is_empty() {
        return Ok(round_robin.next(partitions));
    }
    Ok(partition_for_key(partition_key.as_bytes(), partitions))
}

/// The stream names and the `XADD` shape: everything a command needs apart from the connection.
#[derive(Debug)]
struct Layout {
    topic: String,
    /// `s:{topic}:<p>` for each partition, computed once.
    streams: Vec<String>,
    /// The `MAXLEN` clause for every `XADD`; empty for `TrimMode::None`.
    trim: Vec<String>,
}

impl Layout {
    fn stream(&self, partition: u32) -> Option<&str> {
        usize::try_from(partition)
            .ok()
            .and_then(|i| self.streams.get(i))
            .map(String::as_str)
    }

    /// `XADD key [trim] * <fields>`.
    fn xadd(&self, partition: u32, fields: &EntryFields<'_>) -> Result<redis::Cmd> {
        let stream = self.stream(partition).ok_or_else(|| {
            Error::invalid_argument(format!(
                "partition {partition} is not a partition of topic '{}'",
                self.topic
            ))
        })?;
        let mut cmd = redis::cmd("XADD");
        cmd.arg(stream);
        for arg in &self.trim {
            cmd.arg(arg);
        }
        cmd.arg("*");
        fields.write_to(&mut cmd);
        Ok(cmd)
    }

    /// C# `Failure`: every publish failure surfaces as a transport error naming what did not land.
    fn failure(&self, partition: u32, count: u64, source: redis::RedisError) -> Error {
        Error::transport_with(
            format!(
                "Failed to publish {count} message(s) to topic '{}' partition {partition} ({}). The publisher does not retry — a silent retry would hide the outage and reorder messages within the key — so this is the caller's to handle.",
                self.topic,
                self.stream(partition).unwrap_or_default()
            ),
            source,
        )
    }
}

/// The per-call pieces every entry of one publish shares, computed once.
struct Prepared {
    partition: u32,
    headers: HeaderBlock,
    /// The caller's `traceparent`, taken before any span starts: the entry carries the caller's
    /// context, not the publish span's.
    trace_parent: Option<String>,
}

impl StreamPublisher {
    fn prepare(&self, partition_key: &str, options: &PublishOptions) -> Result<Prepared> {
        let partition = self.router.route(partition_key, options.partition)?;
        let headers = HeaderBlock::pack(options.headers.as_slice())?;
        let trace_parent = options.trace_parent.clone().or_else(current_traceparent);
        Ok(Prepared {
            partition,
            headers,
            trace_parent,
        })
    }

    async fn send_one(
        &self,
        prepared: &Prepared,
        partition_key: &str,
        body: &[u8],
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<StreamId> {
        let fields = entry_fields(prepared, partition_key, body, msg_type, options);
        let cmd = self.layout.xadd(prepared.partition, &fields)?;
        let mut conn = self.conn.shared();
        let reply: redis::RedisResult<String> = cmd.query_async(&mut conn).await;
        let reply = reply.map_err(|e| self.layout.failure(prepared.partition, 1, e))?;
        parse_entry_id(&reply)
    }

    async fn send_many(
        &self,
        prepared: &Prepared,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<()> {
        // One pipeline: every XADD is issued before any reply is read, so the batch costs one round
        // trip. `ignore_errors` returns each command's own result, so the landed entries can be
        // counted before a failure is reported.
        let mut pipe = redis::pipe();
        pipe.ignore_errors();
        for body in bodies {
            let fields = entry_fields(prepared, partition_key, body, msg_type, options);
            pipe.add_command(self.layout.xadd(prepared.partition, &fields)?);
        }
        let mut conn = self.conn.shared();
        let replies: redis::RedisResult<Vec<redis::Value>> = pipe.query_async(&mut conn).await;

        let (landed, failure) = match replies {
            Ok(values) => {
                let mut failure = None;
                let mut landed = 0_u64;
                for value in values {
                    match value {
                        redis::Value::ServerError(e) => {
                            failure.get_or_insert(redis::RedisError::from(e));
                        }
                        _ => landed += 1,
                    }
                }
                (landed, failure)
            }
            Err(e) => (0, Some(e)),
        };
        if landed > 0 {
            record_published(&self.layout.topic, prepared.partition, landed);
        }
        match failure {
            None => Ok(()),
            Some(e) => {
                let total = u64::try_from(bodies.len()).unwrap_or(u64::MAX);
                Err(self.layout.failure(prepared.partition, total - landed, e))
            }
        }
    }
}

/// One entry of a mixed-partition pipeline (crate-private, for the buffered publisher's flush).
pub(crate) struct PipelinedEntry<'a> {
    pub(crate) partition_key: &'a str,
    pub(crate) body: &'a [u8],
    pub(crate) msg_type: &'a str,
    /// Must carry the resolved partition (`options.partition = Some(p)`), so routing is not redone.
    pub(crate) options: &'a PublishOptions,
}

impl StreamPublisher {
    /// The partition `partition_key` / `explicit` routes to (the same rule `publish` uses).
    pub(crate) fn route(&self, partition_key: &str, explicit: Option<u32>) -> Result<u32> {
        self.router.route(partition_key, explicit)
    }

    /// Writes `entries` as one pipeline, in the given order, across any partitions, and returns
    /// each entry's own outcome (index for index). Redis runs a pipeline's commands in order on
    /// one connection, so entries sharing a partition land in the order given.
    pub(crate) async fn publish_pipelined(
        &self,
        entries: &[PipelinedEntry<'_>],
    ) -> Vec<Result<()>> {
        let mut outcomes: Vec<Option<Result<()>>> = Vec::with_capacity(entries.len());
        let mut issued: Vec<(usize, u32)> = Vec::with_capacity(entries.len());
        let mut pipe = redis::pipe();
        pipe.ignore_errors();
        for (i, entry) in entries.iter().enumerate() {
            let cmd = self
                .prepare(entry.partition_key, entry.options)
                .and_then(|prepared| {
                    let fields = entry_fields(
                        &prepared,
                        entry.partition_key,
                        entry.body,
                        entry.msg_type,
                        entry.options,
                    );
                    self.layout
                        .xadd(prepared.partition, &fields)
                        .map(|cmd| (cmd, prepared.partition))
                });
            match cmd {
                Ok((cmd, partition)) => {
                    pipe.add_command(cmd);
                    issued.push((i, partition));
                    outcomes.push(None);
                }
                Err(e) => outcomes.push(Some(Err(e))),
            }
        }
        if !issued.is_empty() {
            let mut conn = self.conn.shared();
            let replies: redis::RedisResult<Vec<redis::Value>> = pipe.query_async(&mut conn).await;
            let mut landed: Vec<(u32, u64)> = Vec::new();
            match replies {
                Ok(values) => {
                    for (&(i, partition), value) in issued.iter().zip(values) {
                        outcomes[i] = Some(if let redis::Value::ServerError(e) = value {
                            Err(self
                                .layout
                                .failure(partition, 1, redis::RedisError::from(e)))
                        } else {
                            match landed.iter_mut().find(|(p, _)| *p == partition) {
                                Some((_, n)) => *n += 1,
                                None => landed.push((partition, 1)),
                            }
                            Ok(())
                        });
                    }
                }
                Err(e) => {
                    for &(i, partition) in &issued {
                        outcomes[i] = Some(Err(self.layout.failure(
                            partition,
                            1,
                            redis::RedisError::from((e.kind(), "pipeline failed", e.to_string())),
                        )));
                    }
                }
            }
            for (partition, n) in landed {
                record_published(&self.layout.topic, partition, n);
            }
        }
        outcomes
            .into_iter()
            .map(|o| {
                o.unwrap_or_else(|| Err(Error::transport("XADD reply missing from the pipeline")))
            })
            .collect()
    }
}

impl Publisher for StreamPublisher {
    async fn publish(
        &self,
        partition_key: &str,
        body: Bytes,
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<StreamId> {
        let prepared = self.prepare(partition_key, options)?;
        let mut span = start_publish(self.topic(), prepared.partition, Some(msg_type), 1);
        let result = self
            .send_one(&prepared, partition_key, &body, msg_type, options)
            .await;
        match &result {
            Ok(id) => {
                span.set_message_id(*id);
                record_published(self.topic(), prepared.partition, 1);
            }
            Err(e) => span.set_error(&e.to_string()),
        }
        result
    }

    async fn publish_batch(
        &self,
        partition_key: &str,
        bodies: &[Bytes],
        msg_type: &str,
        options: &PublishOptions,
    ) -> Result<()> {
        if bodies.is_empty() {
            return Ok(());
        }
        let prepared = self.prepare(partition_key, options)?;
        let count = u64::try_from(bodies.len()).unwrap_or(u64::MAX);
        let mut span = start_publish(self.topic(), prepared.partition, Some(msg_type), count);
        let result = self
            .send_many(&prepared, partition_key, bodies, msg_type, options)
            .await;
        if let Err(e) = &result {
            span.set_error(&e.to_string());
        }
        result
    }
}

fn entry_fields<'a>(
    prepared: &'a Prepared,
    partition_key: &'a str,
    body: &'a [u8],
    msg_type: &'a str,
    options: &'a PublishOptions,
) -> EntryFields<'a> {
    EntryFields {
        body,
        msg_type,
        partition_key: Some(partition_key),
        correlation_id: options.correlation_id.as_deref(),
        trace_parent: prepared.trace_parent.as_deref(),
        headers: &prepared.headers,
    }
}

/// The id `XADD` returned.
fn parse_entry_id(reply: &str) -> Result<StreamId> {
    if reply.is_empty() {
        return Err(Error::transport(
            "XADD returned no entry id. The command was accepted but the reply was empty or nil.",
        ));
    }
    StreamId::parse(reply).map_err(|e| {
        Error::transport_with(
            format!("XADD returned an unparseable entry id '{reply}'"),
            e,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router(partitions: u32) -> Router {
        Router {
            topic: "t".to_owned(),
            partitions,
            round_robin: RoundRobin::new(),
        }
    }

    fn layout(trim: &[&str]) -> Layout {
        Layout {
            topic: "t".to_owned(),
            streams: vec!["s:{t}:0".to_owned(), "s:{t}:1".to_owned()],
            trim: trim.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    /// C# `ExplicitPartitionOverridesKeyHashing`: the explicit partition ignores the key.
    #[test]
    fn explicit_partition_overrides_key_hashing() {
        let r = router(8);
        let hashed = r.route("customer-42", None).unwrap();
        for p in 0..8 {
            assert_eq!(r.route("customer-42", Some(p)).unwrap(), p);
        }
        assert!(hashed < 8);
    }

    /// C# `ExplicitPartitionOutsideRangeThrows`. The negative case of the C# test cannot be
    /// expressed: the option is `u32`.
    #[test]
    fn explicit_partition_outside_range_is_invalid_argument() {
        let err = router(4).route("k", Some(4)).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(ref m) if m.contains("[0, 3]")));
    }

    #[test]
    fn empty_key_round_robins_and_a_key_hashes_stably() {
        let r = router(4);
        let spread: Vec<u32> = (0..4).map(|_| r.route("", None).unwrap()).collect();
        assert_eq!(spread, [1, 2, 3, 0]);
        let first = r.route("order-7", None).unwrap();
        assert!((0..50).all(|_| r.route("order-7", None).unwrap() == first));
    }

    #[test]
    fn one_partition_always_routes_to_zero() {
        let r = router(1);
        assert_eq!(r.route("anything", None).unwrap(), 0);
        assert_eq!(r.route("", None).unwrap(), 0);
    }

    /// `XADD key [MAXLEN ~ n] * b <body> t <type> k <key> [c] [p] [h]`: trim before the auto id,
    /// the canonical field order, optional fields omitted when empty, the packed headers last.
    #[test]
    fn xadd_writes_trim_then_auto_id_then_fields_in_order() {
        let headers = HeaderBlock::pack(&[("a", "1")]).unwrap();
        let fields = EntryFields {
            body: b"hi",
            msg_type: "T",
            partition_key: Some("k"),
            correlation_id: Some(""),
            trace_parent: None,
            headers: &headers,
        };
        let cmd = layout(&["MAXLEN", "~", "10"]).xadd(0, &fields).unwrap();
        let args = cmd_args(&cmd);
        let expected: Vec<&[u8]> = vec![
            b"XADD", b"s:{t}:0", b"MAXLEN", b"~", b"10", b"*", b"b", b"hi", b"t", b"T", b"k", b"k",
            b"h",
        ];
        assert_eq!(args.len(), 14);
        assert_eq!(args[..13], expected[..]);
        assert_eq!(args[13], headers.packed().as_ref());
    }

    #[test]
    fn xadd_without_trim_has_no_maxlen_and_selects_the_partition_stream() {
        let fields = EntryFields {
            body: b"x",
            msg_type: "T",
            partition_key: None,
            correlation_id: None,
            trace_parent: Some("00-x"),
            headers: &HeaderBlock::empty(),
        };
        let cmd = layout(&[]).xadd(1, &fields).unwrap();
        let expected: Vec<&[u8]> = vec![
            b"XADD", b"s:{t}:1", b"*", b"b", b"x", b"t", b"T", b"p", b"00-x",
        ];
        assert_eq!(cmd_args(&cmd), expected);
    }

    #[test]
    fn xadd_rejects_a_partition_outside_the_layout() {
        let fields = EntryFields {
            body: b"x",
            msg_type: "T",
            partition_key: Some(""),
            correlation_id: None,
            trace_parent: None,
            headers: &HeaderBlock::empty(),
        };
        assert!(matches!(
            layout(&[]).xadd(2, &fields),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn failure_is_a_transport_error_naming_what_did_not_land() {
        let err = layout(&[]).failure(0, 2, redis::RedisError::from(std::io::Error::other("gone")));
        match err {
            Error::Transport { message, source } => {
                assert!(
                    message.contains("Failed to publish 2 message(s)"),
                    "{message}"
                );
                assert!(message.contains("(s:{t}:0)"), "{message}");
                assert!(source.is_some());
            }
            other => panic!("expected a transport error, got {other:?}"),
        }
    }

    #[test]
    fn entry_ids_parse_and_empty_or_malformed_replies_are_transport_errors() {
        assert_eq!(
            parse_entry_id("1700000000000-7").unwrap(),
            StreamId::new(1_700_000_000_000, 7)
        );
        assert!(matches!(parse_entry_id(""), Err(Error::Transport { .. })));
        assert!(matches!(
            parse_entry_id("nope"),
            Err(Error::Transport { .. })
        ));
    }

    fn cmd_args(cmd: &redis::Cmd) -> Vec<&[u8]> {
        cmd.args_iter()
            .map(|a| match a {
                redis::Arg::Simple(b) => b,
                _ => &[][..],
            })
            .collect()
    }
}
