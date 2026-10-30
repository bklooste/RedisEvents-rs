//! Port of the C# `WireTests` (`tst/RedisEvents.UnitTests/WireTests.cs`), one test per C# test,
//! named after it in `snake_case`. Tests whose C# subject has no Rust equivalent are listed in the
//! PR description as skipped, with the reason; each one is noted in a comment at its C# origin.

use std::collections::BTreeSet;

use bytes::Bytes;
use redisevents::error::Error;
use redisevents::wire::codec::{CODEC_VERSION, EntryFields, decode, ensure_supported_version};
use redisevents::wire::{HeaderBlock, Keys, RoundRobin, StreamId, StreamMsg, partition_for_key};
use time::{OffsetDateTime, UtcOffset};

const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// The field names and bytes a `write_to` call produces, in order.
fn written(fields: &EntryFields<'_>) -> Vec<(String, Vec<u8>)> {
    let mut cmd = redis::Cmd::new();
    fields.write_to(&mut cmd);
    let args: Vec<Vec<u8>> = cmd
        .args_iter()
        .map(|a| match a {
            redis::Arg::Simple(b) => b.to_vec(),
            _ => panic!("no cursor args expected"),
        })
        .collect();
    args.chunks(2)
        .map(|p| (String::from_utf8(p[0].clone()).expect("name"), p[1].clone()))
        .collect()
}

fn names(fields: &[(String, Vec<u8>)]) -> Vec<&str> {
    fields.iter().map(|(n, _)| n.as_str()).collect()
}

/// Decodes what `write_to` produced, as the server would hand it back.
fn round_trip(id: StreamId, partition: u32, fields: &EntryFields<'_>) -> StreamMsg {
    let stored = written(fields)
        .into_iter()
        .map(|(n, v)| (n, Bytes::from(v)));
    decode(id, partition, stored, CODEC_VERSION).expect("decodes")
}

fn no_headers() -> HeaderBlock {
    HeaderBlock::empty()
}

fn fields_with<'a>(
    body: &'a [u8],
    msg_type: &'a str,
    key: Option<&'a str>,
    headers: &'a HeaderBlock,
) -> EntryFields<'a> {
    EntryFields {
        body,
        msg_type,
        partition_key: key,
        correlation_id: None,
        trace_parent: None,
        headers,
    }
}

// ---------------------------------------------------------------- StreamId

#[test]
fn stream_id_format_parse_round_trips() {
    let cases = [
        (0, 0),
        (1, 0),
        (1_726_000_000_000, 42),
        (StreamId::MAX_PART, 0),
        (0, StreamId::MAX_PART),
        (StreamId::MAX_PART, StreamId::MAX_PART),
    ];
    for (ms, seq) in cases {
        let id = StreamId::new(ms, seq);
        let text = id.to_string();
        let parsed = StreamId::parse(&text).expect("parses");
        assert_eq!(parsed, id);
        assert_eq!(text, format!("{ms}-{seq}"));
    }
}

#[test]
fn stream_id_min_is_zero_dash_zero() {
    assert_eq!(StreamId::MIN.to_string(), "0-0");
    assert_eq!(StreamId::parse("0-0").expect("parses"), StreamId::MIN);
    assert_eq!(StreamId::MIN.ms, 0);
    assert_eq!(StreamId::MIN.seq, 0);
}

#[test]
fn stream_id_max_round_trips_at_long_max_value() {
    let text = StreamId::MAX.to_string();
    assert_eq!(text, "9223372036854775807-9223372036854775807");
    assert_eq!(StreamId::parse(&text).expect("parses"), StreamId::MAX);
    assert!(StreamId::MAX > StreamId::new(StreamId::MAX_PART, StreamId::MAX_PART - 1));
}

#[test]
fn stream_id_to_string_matches_format() {
    // C# `ToString` and `Format` are one method here: `Display`, and the allocation-free `format_into`.
    let id = StreamId::new(1234, 5);
    let mut buf = [0u8; 41];
    assert_eq!(id.to_string(), "1234-5");
    assert_eq!(id.format_into(&mut buf), id.to_string());
}

#[test]
fn stream_id_parse_rejects_malformed_text() {
    let cases = [
        "",
        "123",
        "-5",
        "5-",
        "abc-1",
        "1-abc",
        "1-2-3",
        " 1-2",
        "+1-2",
        "-1--2",
        "99999999999999999999-0",
        "9223372036854775808-0",
    ];
    for text in cases {
        assert!(StreamId::parse(text).is_err(), "{text:?} must be rejected");
        assert!(
            StreamId::parse_bytes(text.as_bytes()).is_err(),
            "{text:?} bytes"
        );
        assert!(matches!(
            text.parse::<StreamId>(),
            Err(e) if e.input() == text
        ));
    }
}

#[test]
fn stream_id_from_time_matches_unix_milliseconds() {
    // 2026-09-06T11:22:33.444Z, the C# test's `DateTimeOffset(2026, 9, 6, 11, 22, 33, 444)`.
    let when =
        OffsetDateTime::from_unix_timestamp_nanos(1_788_693_753_444_000_000).expect("in range");

    let id = StreamId::from_time(when);

    assert_eq!(id.ms, 1_788_693_753_444);
    assert_eq!(id.seq, 0);
    assert_eq!(id.timestamp(), when);
}

#[test]
fn stream_id_from_time_ignores_offset_and_uses_instant() {
    // 2026-09-06T00:00:00Z.
    let utc = OffsetDateTime::from_unix_timestamp(1_788_652_800).expect("in range");
    let same_instant_elsewhere = utc.to_offset(UtcOffset::from_hms(10, 0, 0).expect("offset"));

    assert_eq!(
        StreamId::from_time(same_instant_elsewhere),
        StreamId::from_time(utc)
    );
}

#[test]
fn stream_id_ordering_sorts_by_ms_then_seq() {
    let ids = [
        StreamId::new(2, 0),
        StreamId::new(1, 9),
        StreamId::new(1, 0),
        StreamId::new(1, 10), // same ms, higher seq
        StreamId::MAX,
        StreamId::MIN,
    ];

    let sorted: Vec<StreamId> = ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    assert_eq!(
        sorted,
        vec![
            StreamId::MIN,
            StreamId::new(1, 0),
            StreamId::new(1, 9),
            StreamId::new(1, 10),
            StreamId::new(2, 0),
            StreamId::MAX,
        ]
    );
}

#[test]
fn stream_id_same_millisecond_compares_by_sequence() {
    let earlier = StreamId::new(1_726_000_000_000, 1);
    let later = StreamId::new(1_726_000_000_000, 2);

    assert!(earlier.cmp(&later).is_lt());
    assert!(earlier < later);
    assert!(earlier <= later);
    assert!(later > earlier);
    assert!(later >= earlier);
    let same = StreamId::new(1_726_000_000_000, 1);
    assert!(earlier.cmp(&same).is_eq());
    assert!(earlier <= same);
    assert!(earlier >= same);
}

// -------------------------------------------------------------- EntryCodec

#[test]
fn entry_codec_encode_decode_round_trips_every_field() {
    let body = br#"{"betId":"abc"}"#;
    let headers = HeaderBlock::pack(&[("brand", "orange"), ("attempt", "3")]).expect("packs");
    let fields = EntryFields {
        body,
        msg_type: "Orange.Contracts.BetPlaced",
        partition_key: Some("cust-42"),
        correlation_id: Some("corr-99"),
        trace_parent: Some(TRACE_PARENT),
        headers: &headers,
    };

    let id = StreamId::new(1_726_000_000_000, 7);
    let msg = round_trip(id, 3, &fields);

    assert_eq!(msg.body().as_ref(), body);
    assert_eq!(msg.msg_type(), "Orange.Contracts.BetPlaced");
    assert_eq!(msg.partition_key(), "cust-42");
    assert_eq!(msg.correlation_id(), "corr-99");
    assert_eq!(msg.trace_parent(), Some(TRACE_PARENT));
    assert_eq!(msg.id(), id);
    assert_eq!(msg.partition(), 3);
    assert_eq!(msg.enqueued_time(), id.timestamp());
    assert_eq!(
        msg.headers().to_vec().expect("headers"),
        vec![
            ("brand".into(), "orange".into()),
            ("attempt".into(), "3".into())
        ]
    );
}

#[test]
fn stream_msg_positional_constructor_carries_trace_parent_and_computes_enqueued_time() {
    // C# guards that the trace parent is passed through construction. Here: the builder sets it,
    // and the enqueue time is computed from the id, never stored.
    let id = StreamId::new(1_726_000_000_123, 4);

    let msg = StreamMsg::new(id, 2, "T", Bytes::new())
        .with_partition_key("cust-1")
        .with_correlation_id("corr-1")
        .with_trace_parent(Some(TRACE_PARENT.to_owned()))
        .with_headers(no_headers());

    assert_eq!(msg.trace_parent(), Some(TRACE_PARENT));
    assert_eq!(msg.partition_key(), "cust-1");
    assert_eq!(msg.correlation_id(), "corr-1");
    assert_eq!(msg.partition(), 2);
    assert_eq!(msg.enqueued_time(), id.timestamp());
}

#[test]
fn entry_codec_encode_always_writes_body_type_and_key() {
    let headers = no_headers();
    let fields = fields_with(b"", "T", Some(""), &headers);

    assert_eq!(names(&written(&fields)), ["b", "t", "k"]);
}

#[test]
fn entry_codec_encode_state_writes_only_body_and_type() {
    let headers = no_headers();
    let fields = fields_with(b"x", "T", None, &headers);

    assert_eq!(names(&written(&fields)), ["b", "t"]);
}

#[test]
fn entry_codec_encode_state_with_metadata_never_writes_partition_key() {
    let headers = HeaderBlock::pack(&[("es-version", "3")]).expect("packs");
    let fields = EntryFields {
        body: b"x",
        msg_type: "T",
        partition_key: None,
        correlation_id: Some("corr-7"),
        trace_parent: Some(TRACE_PARENT),
        headers: &headers,
    };

    let pairs = written(&fields);
    assert_eq!(names(&pairs), ["b", "t", "c", "p", "h"]);

    let msg = round_trip(StreamId::MIN, 0, &fields);
    assert_eq!(msg.partition_key(), "");
    assert_eq!(msg.correlation_id(), "corr-7");
    assert_eq!(msg.trace_parent(), Some(TRACE_PARENT));
    assert_eq!(
        msg.headers().get("es-version").expect("valid").as_deref(),
        Some("3")
    );
}

#[test]
fn entry_codec_encode_state_decodes_under_the_current_codec_version() {
    let headers = no_headers();
    let fields = fields_with(b"body", "T", None, &headers);

    let msg = round_trip(StreamId::new(4, 2), 0, &fields);

    assert_eq!(msg.body().as_ref(), b"body");
    assert_eq!(msg.msg_type(), "T");
    assert_eq!(msg.partition_key(), "");
    assert_eq!(msg.correlation_id(), "");
    assert_eq!(msg.trace_parent(), None);
    assert!(msg.headers().is_empty());
}

#[test]
fn entry_codec_empty_body_round_trips() {
    let headers = no_headers();
    let fields = fields_with(b"", "T", Some(""), &headers);

    let msg = round_trip(StreamId::MIN, 0, &fields);

    assert!(msg.body().is_empty());
    assert_eq!(msg.msg_type(), "T");
    assert_eq!(msg.partition_key(), "");
    assert_eq!(msg.correlation_id(), "");
    assert_eq!(msg.trace_parent(), None);
    assert!(msg.headers().is_empty());
}

#[test]
fn entry_codec_one_mebibyte_body_round_trips_byte_for_byte() {
    // Deterministic pseudo-random bytes instead of C#'s `new Random(1234)`.
    let mut state: u32 = 1234;
    let body: Vec<u8> = (0..1024 * 1024)
        .map(|_| {
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
            (state >> 16).to_le_bytes()[0]
        })
        .collect();
    let headers = no_headers();
    let fields = fields_with(&body, "Big", Some(""), &headers);

    let msg = round_trip(StreamId::new(9, 9), 1, &fields);

    assert_eq!(msg.body().len(), body.len());
    assert_eq!(msg.body().as_ref(), body.as_slice());
}

#[test]
fn entry_codec_no_headers_omits_header_field() {
    let headers = HeaderBlock::pack::<&str, &str>(&[]).expect("packs");
    let fields = fields_with(b"x", "T", Some(""), &headers);

    assert!(!names(&written(&fields)).contains(&"h"));
    assert_eq!(
        round_trip(StreamId::MIN, 0, &fields)
            .headers()
            .count()
            .expect("valid"),
        0
    );
}

#[test]
fn entry_codec_single_header_round_trips() {
    let headers = HeaderBlock::pack(&[("only", "one")]).expect("packs");
    let fields = fields_with(b"x", "T", Some(""), &headers);

    let block = round_trip(StreamId::MIN, 0, &fields).headers().clone();

    assert_eq!(block.count().expect("valid"), 1);
    assert_eq!(block.get("only").expect("valid").as_deref(), Some("one"));
    assert_eq!(block.get("missing").expect("valid"), None);
}

#[test]
fn entry_codec_sixty_four_headers_round_trip() {
    let owned: Vec<(String, String)> = (0..HeaderBlock::MAX_COUNT)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    let headers = HeaderBlock::pack(&owned).expect("64 headers pack");
    let fields = fields_with(b"x", "T", Some(""), &headers);

    let block = round_trip(StreamId::MIN, 0, &fields).headers().clone();

    assert_eq!(block.count().expect("valid"), HeaderBlock::MAX_COUNT);
    assert_eq!(block.to_vec().expect("valid"), owned);
}

#[test]
fn header_block_pack_over_count_cap_throws() {
    let owned: Vec<(String, String)> = (0..=HeaderBlock::MAX_COUNT)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();

    let result = HeaderBlock::pack(&owned);

    match result {
        Err(Error::InvalidArgument(message)) => assert!(message.contains("64"), "{message}"),
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

#[test]
fn header_block_pack_over_byte_cap_throws() {
    // Nine headers of ~1 KiB values exceed the 8 KiB block cap.
    let owned: Vec<(String, String)> = (0..9)
        .map(|i| (format!("k{i}"), "v".repeat(1024)))
        .collect();

    let result = HeaderBlock::pack(&owned);

    match result {
        Err(Error::InvalidArgument(message)) => {
            assert!(
                message.contains(&HeaderBlock::MAX_BYTES.to_string()),
                "{message}"
            );
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
}

// Skipped: `HeaderBlock_Pack_NullKeyOrValue_Throws` and `EntryCodec_Encode_NullType_Throws`.
// Rust's `&str` keys, values and message types cannot be null.

#[test]
fn header_block_pack_empty_is_empty() {
    let none: [(&str, &str); 0] = [];
    assert!(HeaderBlock::pack(&none).expect("packs").is_empty());
    assert!(
        HeaderBlock::pack::<&str, &str>(&[])
            .expect("packs")
            .is_empty()
    );
    assert_eq!(HeaderBlock::empty().count().expect("valid"), 0);
    assert_eq!(HeaderBlock::empty().get("anything").expect("valid"), None);
    // Skipped: C# `ToString()` on an empty block; no Rust counterpart.
}

/// The packing format is `len ':' bytes`, so keys and values with `:`, digits or multi-byte UTF-8
/// must round-trip: the length prefix decides where a field ends, never a delimiter search.
#[test]
fn header_block_delimiter_and_digits_in_keys_and_values_round_trip() {
    let cases = [
        ("a:b", "c:d"),
        (":", ":"),
        ("::::", "5:notalength"),
        ("12:34", "56:78"),
        ("3:abc", "3:abc"),
        ("0:", "0:"),
        ("999999999:x", "1:y"),
        ("42", "7"),
        ("", ""),
        ("", "value-for-empty-key"),
        ("key-with-empty-value", ""),
        ("héllo:wörld", "ünïcode:2"),
    ];
    for (key, value) in cases {
        let block = HeaderBlock::pack(&[(key, value)]).expect("packs");
        let decoded = HeaderBlock::from_packed(block.packed().clone());

        assert_eq!(decoded.count().expect("valid"), 1, "{key:?}={value:?}");
        assert_eq!(decoded.get(key).expect("valid").as_deref(), Some(value));
        assert_eq!(
            decoded.to_vec().expect("valid"),
            vec![(key.to_owned(), value.to_owned())]
        );
    }
}

#[test]
fn header_block_adversarial_headers_survive_the_full_entry_round_trip() {
    let headers = HeaderBlock::pack(&[
        ("3:abc", "3:abc"),
        ("", ""),
        ("12", "34:56"),
        (":leading", "trailing:"),
    ])
    .expect("packs");
    let fields = fields_with(b"body", "T", Some(""), &headers);

    let block = round_trip(StreamId::new(5, 5), 0, &fields)
        .headers()
        .clone();

    assert_eq!(block.count().expect("valid"), 4);
    assert_eq!(
        block.to_vec().expect("valid"),
        vec![
            ("3:abc".into(), "3:abc".into()),
            (String::new(), String::new()),
            ("12".into(), "34:56".into()),
            (":leading".into(), "trailing:".into()),
        ]
    );
}

#[test]
fn header_block_enumerator_yields_keys_and_values_in_order() {
    let block = HeaderBlock::pack(&[("a", "1"), ("b", "2")]).expect("packs");

    let seen: Vec<String> = block
        .iter()
        .map(|h| {
            let h = h.expect("valid");
            format!("{}={}", h.key(), h.value())
        })
        .collect();

    assert_eq!(seen, ["a=1", "b=2"]);
    // Skipped: C# `ToString()` renders "a=1; b=2"; the Rust type has no such rendering.
}

#[test]
fn header_block_malformed_is_an_error() {
    // A key field with no matching value.
    let truncated = HeaderBlock::from_packed(Bytes::from_static(b"1:a"));
    assert!(matches!(truncated.count(), Err(Error::Transport { .. })));

    // A non-digit length prefix.
    let garbage = HeaderBlock::from_packed(Bytes::from_static(b"x:a1:b"));
    assert!(matches!(garbage.count(), Err(Error::Transport { .. })));
}

#[test]
fn header_block_equality_is_by_packed_bytes() {
    let a = HeaderBlock::pack(&[("k", "v")]).expect("packs");
    let b = HeaderBlock::pack(&[("k", "v")]).expect("packs");
    let c = HeaderBlock::pack(&[("k", "w")]).expect("packs");

    assert_eq!(a, b);
    assert_eq!(hash_of(&a), hash_of(&b));
    assert_ne!(a, c);
}

fn hash_of<T: std::hash::Hash>(value: &T) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn entry_codec_decode_unknown_codec_version_throws() {
    let headers = no_headers();
    let fields = fields_with(b"x", "T", Some(""), &headers);
    let stored = written(&fields)
        .into_iter()
        .map(|(n, v)| (n, Bytes::from(v)));

    let result = decode(StreamId::MIN, 0, stored, 2);

    match result {
        Err(Error::Transport { message, .. }) => {
            assert!(message.contains("version 2"), "{message}");
        }
        other => panic!("expected Transport, got {other:?}"),
    }
    assert!(ensure_supported_version(2).is_err());
    assert!(ensure_supported_version(CODEC_VERSION).is_ok());
}

#[test]
fn entry_codec_decode_ignores_unknown_fields_within_the_same_version() {
    let headers = no_headers();
    let fields = fields_with(b"x", "T", Some(""), &headers);
    let mut stored: Vec<(String, Bytes)> = written(&fields)
        .into_iter()
        .map(|(n, v)| (n, Bytes::from(v)))
        .collect();
    stored.push(("z".into(), Bytes::from_static(b"future")));

    let msg = decode(StreamId::MIN, 0, stored, CODEC_VERSION).expect("decodes");

    assert_eq!(msg.msg_type(), "T");
}

// ---------------------------------------------------------- PartitionRouter

#[test]
fn partition_router_for_key_is_deterministic() {
    let key = b"customer-12345";
    let first = partition_for_key(key, 8);
    for _ in 0..1000 {
        assert_eq!(partition_for_key(key, 8), first);
    }
}

#[test]
fn partition_router_single_partition_always_zero() {
    for partitions in [0, 1] {
        assert_eq!(partition_for_key(b"anything", partitions), 0);
        assert_eq!(partition_for_key(b"", partitions), 0);
        let rr = RoundRobin::new();
        for _ in 0..10 {
            assert_eq!(rr.next(partitions), 0);
        }
    }
    // Skipped: C# `-4`; partition counts are `u32` in this crate (CONTRACTS.md rule 8).
}

#[test]
fn partition_router_for_key_stays_in_range() {
    for partitions in [2, 8, 16, 3, 10] {
        for i in 0..5_000 {
            let p = partition_for_key(format!("key-{i}").as_bytes(), partitions);
            assert!(p < partitions, "{p} out of range for {partitions}");
        }
    }
}

#[test]
fn partition_router_for_key_distributes_within_ten_percent() {
    const KEYS: u32 = 100_000;
    for partitions in [8u32, 16] {
        let mut counts = vec![0u32; partitions as usize];
        for i in 0..KEYS {
            counts
                [partition_for_key(format!("customer-{i:06}").as_bytes(), partitions) as usize] +=
                1;
        }

        let expected = f64::from(KEYS) / f64::from(partitions);
        let tolerance = expected * 0.10;
        for count in counts {
            assert!(count > 0);
            assert!(
                (f64::from(count) - expected).abs() < tolerance,
                "partition count {count} not within 10% of {expected}"
            );
        }
    }
}

#[test]
fn partition_router_round_robin_spreads_evenly_and_never_goes_negative() {
    const PARTITIONS: u32 = 8;
    let rr = RoundRobin::new();
    let mut counts = [0u32; PARTITIONS as usize];

    for _ in 0..8_000 {
        let p = rr.next(PARTITIONS);
        assert!(p < PARTITIONS);
        counts[p as usize] += 1;
    }

    assert!(counts.iter().all(|&c| c == 1000));
}

#[test]
fn partition_router_for_key_empty_key_is_stable_but_not_spread() {
    // An empty key hashes like any other input; spreading empty keys is the publisher's job
    // (`RoundRobin`), not `partition_for_key`'s.
    let first = partition_for_key(b"", 8);
    assert_eq!(partition_for_key(b"", 8), first);
    assert!(first < 8);
}

// Skipped (unit tests, in `wire/router.rs`, because they need the private counter):
// `PartitionRouter_RoundRobin_WrapsAtUintOverflow`, `PartitionRouter_RoundRobin_NonPowerOfTwo_NeverNegativeAcrossOverflow`.

// ----------------------------------------------------------------- StreamKeys

fn default_keys() -> Keys {
    // The C# tests read `KeyNamespace.Prefix()`, the fixed `re:` with nothing configured; Rust's
    // default is the same.
    Keys::from_lookup(|_| None)
}

#[test]
fn stream_keys_use_literal_braces_around_the_topic_as_hash_tag() {
    let keys = default_keys();
    let ns = keys.prefix().to_owned();
    assert_eq!(ns, "re:");

    assert_eq!(
        keys.stream("offer_odds", 0, true),
        format!("{ns}s:{{offer_odds}}:0")
    );
    assert_eq!(
        keys.stream("offer_odds", 15, true),
        format!("{ns}s:{{offer_odds}}:15")
    );
    assert_eq!(
        keys.positions("offer_odds", "offer-products"),
        format!("{ns}p:{{offer_odds}}:offer-products")
    );
    assert_eq!(
        keys.positions_meta("offer_odds", "offer-products"),
        format!("{ns}p:{{offer_odds}}:offer-products:meta")
    );
    assert_eq!(
        keys.ownership("offer_odds", "offer-products"),
        format!("{ns}o:{{offer_odds}}:offer-products")
    );
    assert_eq!(
        keys.topic_meta("offer_odds"),
        format!("{ns}m:{{offer_odds}}")
    );
    assert_eq!(
        keys.positions_pattern("offer_odds"),
        format!("{ns}p:{{offer_odds}}:*")
    );
    assert_eq!(
        keys.stream_pattern("offer_odds", true),
        format!("{ns}s:{{offer_odds}}:*")
    );
}

#[test]
fn stream_keys_co_locate_false_drops_the_hash_tag() {
    let keys = default_keys();
    let ns = keys.prefix().to_owned();

    assert_eq!(
        keys.stream("offer_odds", 3, false),
        format!("{ns}s:offer_odds:3")
    );
    assert_eq!(
        keys.stream_pattern("offer_odds", false),
        format!("{ns}s:offer_odds:*")
    );
}

#[test]
fn stream_keys_colocated_keys_share_one_hash_tag() {
    let keys = default_keys();
    let all = [
        keys.stream("bet_state", 1, true),
        keys.positions("bet_state", "bet-processing"),
        keys.ownership("bet_state", "bet-processing"),
        keys.topic_meta("bet_state"),
    ];
    for key in all {
        let open = key.find('{').expect("hash tag");
        let close = key.find('}').expect("hash tag");
        assert_eq!(&key[open + 1..close], "bet_state", "{key}");
    }
}

#[test]
fn stream_keys_long_topic_name_exceeding_the_stack_buffer_still_builds() {
    let keys = default_keys();
    let topic = "t".repeat(400);

    assert_eq!(
        keys.stream(&topic, 7, true),
        format!("{}s:{{{topic}}}:7", keys.prefix())
    );
}

// Prefix resolution (`KeyNamespace.Prefix`) is covered by `wire_vectors.rs` (keys.json) and by
// `wire/keys.rs` unit tests (the Rust rule), through `Keys::from_lookup`. No test mutates the process environment:
// edition 2024 makes `std::env::set_var` unsafe, and this crate forbids `unsafe`.
