//! Integration tests for the direct publisher (`StreamPublisher`) and topic meta against a real
//! Redis. Port of the direct-publisher parts of `ProducerTests.cs`, `ProducerServiceTests.cs` and
//! `ProducerSpanTests.cs`, named after the C# test they port in `snake_case`; the C# tests that
//! need a consumer, the buffered publisher or the outbox are not here (see the PR for the list).
//! Skips without Redis (`require_redis!`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use redis::aio::ConnectionManager;
use redisevents::config::{TopicOptions, TrimMode};
use redisevents::producer::{PublishOptions, StreamPublisher};
use redisevents::wire::codec::{CODEC_VERSION, decode};
use redisevents::wire::{HeaderBlock, StreamId, partition_for_key};
use redisevents::{Error, Publisher};
use redisevents_test_support::RedisFixture;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Instrument, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

async fn raw(redis: &RedisFixture) -> ConnectionManager {
    redis.connect().await.shared()
}

async fn publisher(redis: &RedisFixture, topic: &str, options: TopicOptions) -> StreamPublisher {
    StreamPublisher::new(redis.connect().await, redis.keys(), topic, options)
        .await
        .expect("publisher")
}

fn stream_key(redis: &RedisFixture, topic: &str, partition: u32) -> String {
    redis.keys().stream(topic, partition, true)
}

async fn xlen(conn: &mut ConnectionManager, key: &str) -> i64 {
    redis::cmd("XLEN")
        .arg(key)
        .query_async(conn)
        .await
        .expect("XLEN")
}

/// Every entry of `key`, as `(id, field map)`.
async fn entries(
    conn: &mut ConnectionManager,
    key: &str,
) -> Vec<(StreamId, HashMap<String, Bytes>)> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
        .arg(key)
        .arg("-")
        .arg("+")
        .query_async(conn)
        .await
        .expect("XRANGE");
    reply
        .ids
        .into_iter()
        .map(|entry| {
            let id = StreamId::parse(&entry.id).expect("stream id");
            let map = entry
                .map
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        Bytes::from(redis::from_redis_value::<Vec<u8>>(v).expect("bytes")),
                    )
                })
                .collect();
            (id, map)
        })
        .collect()
}

fn body(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

// ---------------------------------------------------------------------------------------------
// ProducerTests (direct parts)
// ---------------------------------------------------------------------------------------------

/// C# `ExplicitPartitionOverridesKeyHashing`: the explicit partition picks the stream, and the key
/// still travels on the entry.
#[tokio::test]
async fn explicit_partition_overrides_key_hashing() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("explicit");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 8,
            ..TopicOptions::default()
        },
    )
    .await;

    let hashed = partition_for_key(b"customer-42", 8);
    publisher
        .publish(
            "customer-42",
            body("a"),
            "T",
            &PublishOptions::new().partition(3),
        )
        .await
        .unwrap();

    assert_eq!(xlen(&mut conn, &stream_key(redis, &topic, 3)).await, 1);
    assert_eq!(
        xlen(&mut conn, &stream_key(redis, &topic, hashed)).await,
        i64::from(hashed == 3)
    );
    let (_, fields) = &entries(&mut conn, &stream_key(redis, &topic, 3)).await[0];
    assert_eq!(fields["k"], "customer-42");
}

/// C# `ExplicitPartitionOutsideRangeThrows`, on the wire: nothing is written.
#[tokio::test]
async fn explicit_partition_outside_range_throws() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("range");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 4,
            ..TopicOptions::default()
        },
    )
    .await;

    let err = publisher
        .publish("k", body("a"), "T", &PublishOptions::new().partition(4))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    for p in 0..4 {
        assert_eq!(xlen(&mut conn, &stream_key(redis, &topic, p)).await, 0);
    }
}

/// C# `ANonRedisFailureIsStillWrappedAsATransportFailure`, adapted: the Rust publisher has no
/// stand-in database to throw from, so the failure is a real one. The partition stream is a string
/// key, so `XADD` fails with `WRONGTYPE`, and the caller still gets the documented transport error
/// with the Redis error as its cause.
#[tokio::test]
async fn a_failed_write_surfaces_as_a_transport_error_with_the_redis_cause() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("wrongtype");
    let mut conn = raw(redis).await;
    let publisher = publisher(redis, &topic, TopicOptions::default()).await;
    let _: () = redis::cmd("SET")
        .arg(stream_key(redis, &topic, 0))
        .arg("not a stream")
        .query_async(&mut conn)
        .await
        .unwrap();

    let err = publisher
        .publish("k", body("a"), "T", &PublishOptions::new())
        .await
        .unwrap_err();
    match &err {
        Error::Transport { message, source } => {
            assert!(
                message.contains("Failed to publish 1 message(s)"),
                "{message}"
            );
            assert!(
                source
                    .as_ref()
                    .is_some_and(|s| s.to_string().contains("WRONGTYPE"))
            );
        }
        other => panic!("expected a transport error, got {other:?}"),
    }
}

// SKIPPED, ProducerTests.cs:
// - DirectPublishDoesNotCopyTheBody: the body is a `Bytes` handed to the publisher, but redis-rs
//   serialises every command into its own buffer, so the copy is made by the client and cannot be
//   observed through the library's API.
// - APartiallyFailedBatchStillCountsTheEntriesThatLanded: every entry of a batch shares one stream
//   key, so a server fault fails the whole pipeline; there is no per-entry fault to inject without a
//   proxy. The counting logic is exercised on the replies in `send_many`.
// - APublisherOverAStandInDatabasePublishesWithoutReconciling: there is no stand-in database in the
//   Rust API; `StreamPublisher::new` always reconciles against the server it is given.

// ---------------------------------------------------------------------------------------------
// ProducerServiceTests (direct parts)
// ---------------------------------------------------------------------------------------------

/// C# `S1_publish_then_consume_preserves_every_field`, publish half: the entry on the wire decodes
/// back to the same body, type, key, correlation id, headers and id. The consume half needs the
/// consumer (C1/C2/C4), which is not on `main` yet.
#[tokio::test]
async fn s1_publish_preserves_every_field_on_the_wire() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("s1");
    let mut conn = raw(redis).await;
    let options = TopicOptions {
        partitions: 4,
        trim: TrimMode::None,
        ..TopicOptions::default()
    };
    let publisher = publisher(redis, &topic, options).await;

    let payload = "round trip payload — ünicode, and a \0 NUL byte";
    let key = "round-trip-key-42";
    let publish_options = PublishOptions::new()
        .correlation_id("corr-0f4a1c")
        .header("tenant", "acme")
        .header("schema", "v3");
    let before = time::OffsetDateTime::now_utc();
    let id = publisher
        .publish(key, body(payload), "S1.RoundTrip", &publish_options)
        .await
        .unwrap();
    let after = time::OffsetDateTime::now_utc();

    let partition = partition_for_key(key.as_bytes(), 4);
    let (wire_id, fields) = entries(&mut conn, &stream_key(redis, &topic, partition))
        .await
        .into_iter()
        .next()
        .expect("one entry");
    assert_eq!(wire_id, id, "the id XADD returned is the one stored");
    assert!(
        before - time::Duration::seconds(5) <= wire_id.timestamp()
            && wire_id.timestamp() <= after + time::Duration::seconds(5)
    );

    let msg = decode(wire_id, partition, fields, CODEC_VERSION).unwrap();
    assert_eq!(
        msg.body().as_ref(),
        payload.as_bytes(),
        "the body survives byte for byte, NUL included"
    );
    assert_eq!(msg.msg_type(), "S1.RoundTrip");
    assert_eq!(msg.partition_key(), key);
    assert_eq!(msg.correlation_id(), "corr-0f4a1c");
    assert_eq!(msg.partition(), partition);
    assert_eq!(
        msg.headers().get("tenant").unwrap().as_deref(),
        Some("acme")
    );
    assert_eq!(msg.headers().get("schema").unwrap().as_deref(), Some("v3"));
    assert!(msg.headers().get("absent").unwrap().is_none());
    assert!(
        msg.trace_parent().is_none(),
        "no traceparent without an ambient span or option"
    );
}

// SKIPPED, ProducerServiceTests.cs: S16 (buffered publisher, B2), S17 (outbox, B3) and S21b (outbox
// during a blocking consumer, B3) need work packages that are not on `main`.

// ---------------------------------------------------------------------------------------------
// Publishing on the wire
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn publish_writes_one_entry_per_message_with_the_expected_fields() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("one");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await;

    let first = publisher
        .publish("k", body("one"), "T", &PublishOptions::new())
        .await
        .unwrap();
    let second = publisher
        .publish("k", body("two"), "T", &PublishOptions::new())
        .await
        .unwrap();

    assert!(second > first, "ids are increasing");
    let all = entries(&mut conn, &stream_key(redis, &topic, 0)).await;
    assert_eq!(
        all.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [first, second]
    );
    let fields = &all[0].1;
    assert_eq!(fields["b"], "one");
    assert_eq!(fields["t"], "T");
    assert_eq!(fields["k"], "k");
    assert!(
        !fields.contains_key("c") && !fields.contains_key("p") && !fields.contains_key("h"),
        "optional fields are omitted when they have no value"
    );
}

#[tokio::test]
async fn publish_batch_is_one_ordered_pipeline_on_one_partition() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("batch");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 4,
            ..TopicOptions::default()
        },
    )
    .await;

    let bodies: Vec<Bytes> = (0..5).map(|i| body(&format!("m{i}"))).collect();
    publisher
        .publish_batch("order-9", &bodies, "T", &PublishOptions::new())
        .await
        .unwrap();

    let partition = partition_for_key(b"order-9", 4);
    let got: Vec<Vec<u8>> = entries(&mut conn, &stream_key(redis, &topic, partition))
        .await
        .iter()
        .map(|(_, f)| f["b"].to_vec())
        .collect();
    assert_eq!(got, bodies.iter().map(|b| b.to_vec()).collect::<Vec<_>>());
    for p in (0..4).filter(|p| *p != partition) {
        assert_eq!(xlen(&mut conn, &stream_key(redis, &topic, p)).await, 0);
    }
}

#[tokio::test]
async fn an_empty_batch_writes_nothing() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("empty-batch");
    let mut conn = raw(redis).await;
    let publisher = publisher(redis, &topic, TopicOptions::default()).await;

    publisher
        .publish_batch("k", &[], "T", &PublishOptions::new())
        .await
        .unwrap();
    assert_eq!(xlen(&mut conn, &stream_key(redis, &topic, 0)).await, 0);
}

#[tokio::test]
async fn empty_keys_round_robin_across_the_partitions() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("round-robin");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 4,
            ..TopicOptions::default()
        },
    )
    .await;

    for _ in 0..8 {
        publisher
            .publish("", body("x"), "T", &PublishOptions::new())
            .await
            .unwrap();
    }
    for p in 0..4 {
        assert_eq!(
            xlen(&mut conn, &stream_key(redis, &topic, p)).await,
            2,
            "partition {p}"
        );
    }
}

#[tokio::test]
async fn exact_trim_caps_the_stream_length() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("trim-exact");
    let mut conn = raw(redis).await;
    let options = TopicOptions {
        trim: TrimMode::Exact,
        max_len: Some(5),
        ..TopicOptions::default()
    };
    let publisher = publisher(redis, &topic, options).await;

    for i in 0..12 {
        publisher
            .publish("k", body(&format!("{i}")), "T", &PublishOptions::new())
            .await
            .unwrap();
    }
    assert_eq!(xlen(&mut conn, &stream_key(redis, &topic, 0)).await, 5);
}

#[tokio::test]
async fn trim_none_keeps_every_entry() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("trim-none");
    let mut conn = raw(redis).await;
    let options = TopicOptions {
        trim: TrimMode::None,
        max_len: Some(5),
        ..TopicOptions::default()
    };
    let publisher = publisher(redis, &topic, options).await;

    for i in 0..12 {
        publisher
            .publish("k", body(&format!("{i}")), "T", &PublishOptions::new())
            .await
            .unwrap();
    }
    assert_eq!(xlen(&mut conn, &stream_key(redis, &topic, 0)).await, 12);
}

#[tokio::test]
async fn correlation_id_trace_parent_and_headers_are_written() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("optional");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await;
    let trace = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    let options = PublishOptions::new()
        .correlation_id("c-1")
        .trace_parent(trace)
        .header("a", "1")
        .header("b:x", "2:y");
    publisher
        .publish("k", body("x"), "T", &options)
        .await
        .unwrap();

    let (_, fields) = &entries(&mut conn, &stream_key(redis, &topic, 0)).await[0];
    assert_eq!(fields["c"], "c-1");
    assert_eq!(fields["p"], trace);
    let headers = HeaderBlock::from_packed(fields["h"].clone());
    assert_eq!(
        headers.to_vec().unwrap(),
        [
            ("a".to_owned(), "1".to_owned()),
            ("b:x".to_owned(), "2:y".to_owned())
        ]
    );
}

#[tokio::test]
async fn too_many_headers_is_invalid_argument_and_writes_nothing() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("too-many");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await;

    let mut options = PublishOptions::new();
    for i in 0..=HeaderBlock::MAX_COUNT {
        options = options.header(format!("h{i}"), "v");
    }
    let err = publisher
        .publish("k", body("x"), "T", &options)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    assert_eq!(xlen(&mut conn, &stream_key(redis, &topic, 0)).await, 0);
}

// ---------------------------------------------------------------------------------------------
// Topic meta (`m:{topic}`, WIRE.md section 3)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn topic_meta_records_partitions_version_created_and_max_len() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("meta");
    let mut conn = raw(redis).await;
    let _ = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 3,
            max_len: Some(777),
            ..TopicOptions::default()
        },
    )
    .await;

    let meta_key = redis.keys().topic_meta(&topic);
    let fields: HashMap<String, String> = redis::cmd("HGETALL")
        .arg(&meta_key)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(fields["partitions"], "3");
    assert_eq!(fields["v"], "1");
    assert_eq!(fields["maxLen"], "777");
    let created = &fields["createdUtc"];
    // .NET "O": yyyy-MM-ddTHH:mm:ss.fffffff+00:00
    assert_eq!(created.len(), 33, "{created}");
    assert!(
        created.ends_with("+00:00")
            && created.as_bytes()[10] == b'T'
            && created.as_bytes()[19] == b'.',
        "{created}"
    );
}

#[tokio::test]
async fn a_second_publisher_keeps_the_creation_breadcrumbs() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("breadcrumbs");
    let mut conn = raw(redis).await;
    let meta_key = redis.keys().topic_meta(&topic);

    let _ = publisher(
        redis,
        &topic,
        TopicOptions {
            max_len: Some(100),
            ..TopicOptions::default()
        },
    )
    .await;
    let created_first: String = redis::cmd("HGET")
        .arg(&meta_key)
        .arg("createdUtc")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _ = publisher(
        redis,
        &topic,
        TopicOptions {
            max_len: Some(200),
            ..TopicOptions::default()
        },
    )
    .await;

    let max_len: String = redis::cmd("HGET")
        .arg(&meta_key)
        .arg("maxLen")
        .query_async(&mut conn)
        .await
        .unwrap();
    let created_again: String = redis::cmd("HGET")
        .arg(&meta_key)
        .arg("createdUtc")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(max_len, "100", "HSETNX never rewrites the creation maxLen");
    assert_eq!(created_again, created_first);
}

#[tokio::test]
async fn growing_the_partition_count_overwrites_it_and_creates_the_new_streams() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("grow");
    let mut conn = raw(redis).await;

    let _ = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 2,
            ..TopicOptions::default()
        },
    )
    .await;
    let _ = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 4,
            ..TopicOptions::default()
        },
    )
    .await;

    let recorded: String = redis::cmd("HGET")
        .arg(redis.keys().topic_meta(&topic))
        .arg("partitions")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(recorded, "4");
    for p in 0..4 {
        let exists: bool = redis::cmd("EXISTS")
            .arg(stream_key(redis, &topic, p))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(exists, "partition {p} stream exists");
    }
}

#[tokio::test]
async fn shrinking_the_partition_count_is_refused() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("shrink");
    let _ = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 4,
            ..TopicOptions::default()
        },
    )
    .await;

    let err = StreamPublisher::new(
        redis.connect().await,
        redis.keys(),
        topic.clone(),
        TopicOptions {
            partitions: 2,
            ..TopicOptions::default()
        },
    )
    .await
    .unwrap_err();
    match &err {
        Error::Config(message) => {
            assert!(message.contains("recorded with 4 partitions"), "{message}");
        }
        other => panic!("expected a config error, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unsupported_recorded_codec_version_is_refused() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("version");
    let mut conn = raw(redis).await;
    // Planted before any publisher reconciles the topic: the reconcile is guarded per process, so a
    // topic this process already reconciled is not checked again (as in C#).
    let _: () = redis::cmd("HSET")
        .arg(redis.keys().topic_meta(&topic))
        .arg("v")
        .arg(2)
        .query_async(&mut conn)
        .await
        .unwrap();

    let err = StreamPublisher::new(
        redis.connect().await,
        redis.keys(),
        topic,
        TopicOptions::default(),
    )
    .await
    .unwrap_err();
    match &err {
        Error::Config(message) => assert!(message.contains("codec version 2"), "{message}"),
        other => panic!("expected a config error, got {other:?}"),
    }
}

#[tokio::test]
async fn zero_partitions_is_refused() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("zero");
    let err = StreamPublisher::new(
        redis.connect().await,
        redis.keys(),
        topic,
        TopicOptions {
            partitions: 0,
            ..TopicOptions::default()
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err:?}");
}

// ---------------------------------------------------------------------------------------------
// ProducerSpanTests: the publish span from the diagnostics module (B6)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SpanRecord {
    id: u64,
    parent: Option<u64>,
    fields: HashMap<String, String>,
}

#[derive(Default)]
struct FieldMap(HashMap<String, String>);

impl Visit for FieldMap {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

/// The Rust counterpart of C#'s `ActivityListener`: records every span with its fields and parent.
#[derive(Clone, Default)]
struct SpanCapture(Arc<Mutex<Vec<SpanRecord>>>);

impl SpanCapture {
    /// The `streams.publish {topic}` spans for one topic.
    fn for_topic(&self, topic: &str) -> Vec<SpanRecord> {
        let name = format!("streams.publish {topic}");
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.fields.get("otel.name") == Some(&name))
            .cloned()
            .collect()
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for SpanCapture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = FieldMap::default();
        attrs.record(&mut fields);
        let parent = if attrs.is_contextual() {
            ctx.current_span().id().map(Id::into_u64)
        } else {
            attrs.parent().map(Id::into_u64)
        };
        self.0.lock().unwrap().push(SpanRecord {
            id: id.into_u64(),
            parent,
            fields: fields.0,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut fields = FieldMap::default();
        values.record(&mut fields);
        if let Some(span) = self
            .0
            .lock()
            .unwrap()
            .iter_mut()
            .find(|s| s.id == id.into_u64())
        {
            span.fields.extend(fields.0);
        }
    }
}

/// The span capture, installed once for this test binary as the global subscriber. Installing a
/// dispatcher per test instead races tracing's global callsite-interest cache under parallel tests
/// (a span is intermittently missing); one global subscriber never changes, so the cache is stable.
/// Each span test selects its own spans by topic, which is unique per test.
fn capture() -> &'static SpanCapture {
    static CAPTURE: std::sync::OnceLock<SpanCapture> = std::sync::OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture = SpanCapture::default();
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(capture.clone()),
        );
        capture
    })
}

/// C# `Publish_emits_a_streams_publish_span_in_the_callers_trace`.
#[tokio::test]
async fn publish_emits_a_streams_publish_span_in_the_callers_trace() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("span");
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 4,
            ..TopicOptions::default()
        },
    )
    .await;
    let capture = capture();

    let root = tracing::info_span!("caller");
    let root_id = root.id().expect("root span").into_u64();
    let id = publisher
        .publish(
            "customer-42",
            body("hello"),
            "OrderPlaced",
            &PublishOptions::new(),
        )
        .instrument(root)
        .await
        .unwrap();

    let spans = capture.for_topic(&topic);
    let span = match spans.as_slice() {
        [one] => one,
        other => panic!("one publish is one span, got {}", other.len()),
    };
    assert_eq!(span.fields["otel.name"], format!("streams.publish {topic}"));
    assert_eq!(span.fields["otel.kind"], "producer");
    assert_eq!(
        span.parent,
        Some(root_id),
        "it hangs off the work that asked for the publish"
    );
    assert_eq!(span.fields["messaging.system"], "redis_streams");
    assert_eq!(span.fields["messaging.destination"], topic);
    assert_eq!(
        span.fields["messaging.partition"],
        partition_for_key(b"customer-42", 4).to_string()
    );
    assert_eq!(span.fields["messaging.message.type"], "OrderPlaced");
    assert_eq!(
        span.fields["messaging.message_id"],
        id.to_string(),
        "the id XADD returned is stamped after the write"
    );
    assert!(
        !span.fields.contains_key("messaging.batch.message_count"),
        "a single publish is not a batch"
    );
}

/// C# `A_batch_publish_emits_one_span_carrying_its_message_count`.
#[tokio::test]
async fn a_batch_publish_emits_one_span_carrying_its_message_count() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("span-batch");
    let publisher = publisher(redis, &topic, TopicOptions::default()).await;
    let capture = capture();

    let bodies = [body("a"), body("b"), body("c")];
    publisher
        .publish_batch("k", &bodies, "T", &PublishOptions::new())
        .instrument(tracing::info_span!("caller"))
        .await
        .unwrap();

    let spans = capture.for_topic(&topic);
    let span = match spans.as_slice() {
        [one] => one,
        other => panic!("a pipelined batch is one span, got {}", other.len()),
    };
    assert_eq!(span.fields["messaging.batch.message_count"], "3");
    assert_eq!(span.fields["messaging.message.type"], "T");
}

/// C# `A_failed_publish_marks_its_span_as_an_error`, with a real failure (`WRONGTYPE`).
#[tokio::test]
async fn a_failed_publish_marks_its_span_as_an_error() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("span-fail");
    let mut conn = raw(redis).await;
    let publisher = publisher(redis, &topic, TopicOptions::default()).await;
    let _: () = redis::cmd("SET")
        .arg(stream_key(redis, &topic, 0))
        .arg("x")
        .query_async(&mut conn)
        .await
        .unwrap();
    let capture = capture();

    let result = publisher
        .publish("k", body("x"), "T", &PublishOptions::new())
        .instrument(tracing::info_span!("caller"))
        .await;
    assert!(matches!(result, Err(Error::Transport { .. })));

    let spans = capture.for_topic(&topic);
    let span = match spans.as_slice() {
        [one] => one,
        other => panic!("one span, got {}", other.len()),
    };
    assert_eq!(span.fields["otel.status_code"], "ERROR");
}

/// C# `Publishing_does_not_leave_its_span_current`.
#[tokio::test]
async fn publishing_does_not_leave_its_span_current() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("span-current");
    let publisher = publisher(redis, &topic, TopicOptions::default()).await;
    let _ = capture();

    let before = tracing::Span::current().id();
    let _ = publisher
        .publish("k", body("x"), "T", &PublishOptions::new())
        .instrument(tracing::info_span!("caller"))
        .await
        .unwrap();
    assert_eq!(
        tracing::Span::current().id(),
        before,
        "the publish span is not left current"
    );
}

/// C# `With_no_listener_no_span_is_created`, in spirit. The span capture is a global subscriber
/// that other tests in this binary install, so whether anything is listening depends on test order:
/// this asserts only what holds either way, that no span is left current and no traceparent is
/// stamped without an ambient context.
#[tokio::test]
async fn with_no_listener_no_span_is_created() {
    let redis = redisevents_test_support::require_redis!();
    let topic = redis.new_topic("span-none");
    let mut conn = raw(redis).await;
    let publisher = publisher(
        redis,
        &topic,
        TopicOptions {
            partitions: 1,
            ..TopicOptions::default()
        },
    )
    .await;

    let before = tracing::Span::current().id();
    let _ = publisher
        .publish("k", body("x"), "T", &PublishOptions::new())
        .await
        .unwrap();
    assert_eq!(tracing::Span::current().id(), before);
    let (_, fields) = &entries(&mut conn, &stream_key(redis, &topic, 0)).await[0];
    assert!(
        !fields.contains_key("p"),
        "with no ambient activity there is no traceparent to stamp"
    );
}

// SKIPPED, ProducerSpanTests.cs: The_entrys_traceparent_still_names_the_caller_not_the_publish_span
// needs the `otel` feature, the only build in which the caller's OpenTelemetry context becomes a
// `traceparent`; the default build has no ambient W3C context to stamp.
