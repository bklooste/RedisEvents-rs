//! Port of `BlockFetchParseTests.cs` (1:1), feeding hand-built `redis::Value` replies in both the
//! RESP2 nesting and the flattened RESP3 map layout, plus the redis-rs RESP3 `Map` form.

#![allow(clippy::assert_is_empty)]

use redis::Value;

use super::{RawEntry, SliceKey, StreamId, parse, parse_all};
use crate::Error;

fn v(text: &str) -> Value {
    Value::BulkString(text.as_bytes().to_vec())
}

fn arr(items: Vec<Value>) -> Value {
    Value::Array(items)
}

fn resp2_stream(key: &str, entries: Value) -> Value {
    arr(vec![v(key), entries])
}

fn entry(id: &str, fields: &[(&str, &str)]) -> Value {
    let flat = fields.iter().flat_map(|(f, x)| [v(f), v(x)]).collect();
    arr(vec![v(id), arr(flat)])
}

fn id(s: &str) -> StreamId {
    StreamId::parse(s).unwrap()
}

fn field(e: &RawEntry, i: usize) -> (&[u8], &[u8]) {
    (&e.fields[i].0, &e.fields[i].1)
}

fn transport_message(r: crate::Result<impl std::fmt::Debug>) -> String {
    match r {
        Err(e @ Error::Transport { .. }) => e.to_string(),
        other => panic!("expected a transport error, got {other:?}"),
    }
}

const K0: &[u8] = b"s:t:0";

#[test]
fn a_nil_reply_is_the_idle_case_not_an_error() {
    assert!(parse(Value::Nil, K0).unwrap().is_empty());
    assert!(
        parse(arr(vec![]), K0).unwrap().is_empty(),
        "some server versions send an empty array where others send nil"
    );
    assert!(
        parse(Value::SimpleString("OK".into()), K0)
            .unwrap()
            .is_empty(),
        "a non-array reply carries no entries"
    );
    assert!(parse(Value::Okay, K0).unwrap().is_empty());
}

#[test]
fn a_stream_with_an_empty_entries_array_parses_as_empty() {
    let reply = arr(vec![resp2_stream("s:t:0", arr(vec![]))]);
    assert!(parse(reply, K0).unwrap().is_empty());
}

#[test]
fn a_resp2_reply_yields_every_entry_with_its_fields() {
    let reply = arr(vec![resp2_stream(
        "s:t:0",
        arr(vec![
            entry("100-0", &[("b", "one"), ("t", "Order")]),
            entry("100-1", &[("b", "two"), ("t", "Order")]),
        ]),
    )]);
    let batch = parse(reply, K0).unwrap();
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].id, id("100-0"));
    assert_eq!(batch[1].id, id("100-1"));
    assert_eq!(batch[0].fields.len(), 2);
    assert_eq!(field(&batch[0], 0), (&b"b"[..], &b"one"[..]));
    assert_eq!(field(&batch[1], 0).1, b"two");
}

#[test]
fn a_resp3_map_reply_is_walked_by_shape_and_the_right_stream_is_picked() {
    let reply = arr(vec![
        v("s:t:0"),
        arr(vec![entry("1-0", &[("b", "mine")])]),
        v("s:t:1"),
        arr(vec![entry("2-0", &[("b", "theirs")])]),
    ]);
    let batch = parse(reply, b"s:t:1").unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, id("2-0"));
    assert_eq!(
        field(&batch[0], 0).1,
        b"theirs",
        "the key match must pick the stream, not the position in the reply"
    );
}

/// Rust-only: redis-rs hands a RESP3 reply over as `Value::Map`, not flattened.
#[test]
fn a_resp3_value_map_reply_is_walked_too() {
    let reply = Value::Map(vec![
        (v("s:t:0"), arr(vec![entry("1-0", &[("b", "mine")])])),
        (v("s:t:1"), arr(vec![entry("2-0", &[("b", "theirs")])])),
    ]);
    let batch = parse(reply, b"s:t:1").unwrap();
    assert_eq!(batch[0].id, id("2-0"));
}

#[test]
fn a_resp3_key_with_no_entries_after_it_throws() {
    let reply = arr(vec![
        v("s:t:0"),
        arr(vec![entry("1-0", &[("b", "x")])]),
        v("s:t:1"),
    ]);
    assert!(transport_message(parse(reply, K0)).contains("no entries element"));
}

#[test]
fn a_multi_stream_reply_returns_the_requested_stream() {
    let reply = arr(vec![
        resp2_stream("s:t:0", arr(vec![entry("1-0", &[("b", "zero")])])),
        resp2_stream("s:t:1", arr(vec![entry("9-9", &[("b", "one")])])),
    ]);
    let batch = parse(reply, b"s:t:1").unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, id("9-9"));
}

#[test]
fn a_single_stream_reply_is_accepted_even_when_the_key_text_differs() {
    let reply = arr(vec![resp2_stream(
        "prefix:s:t:0",
        arr(vec![entry("4-0", &[("b", "x")])]),
    )]);
    assert_eq!(parse(reply, K0).unwrap().len(), 1);
}

#[test]
fn several_streams_and_none_requested_throws_rather_than_guessing() {
    let reply = arr(vec![
        resp2_stream("s:other:0", arr(vec![entry("1-0", &[("b", "x")])])),
        resp2_stream("s:other:1", arr(vec![entry("2-0", &[("b", "y")])])),
    ]);
    assert!(transport_message(parse(reply, K0)).contains("2 streams"));
}

#[test]
fn a_nil_entry_is_dropped_and_its_neighbours_survive() {
    let reply = arr(vec![resp2_stream(
        "s:t:0",
        arr(vec![Value::Nil, entry("7-0", &[("b", "kept")]), Value::Nil]),
    )]);
    let batch = parse(reply, K0).unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].id, id("7-0"));
}

#[test]
fn an_all_nil_entry_list_throws_because_the_cursor_could_not_advance() {
    let reply = arr(vec![resp2_stream(
        "s:t:0",
        arr(vec![Value::Nil, Value::Nil]),
    )]);
    assert!(transport_message(parse(reply, K0)).contains("nowhere to advance"));
}

#[test]
fn a_malformed_entry_throws_and_names_what_was_wrong() {
    for shape in ["arity", "no-id", "odd-fields", "pair-arity"] {
        let entries = match shape {
            "arity" => arr(vec![arr(vec![v("1-0"), arr(vec![]), v("extra")])]),
            "no-id" => arr(vec![arr(vec![Value::Nil, arr(vec![v("b"), v("x")])])]),
            _ => arr(vec![arr(vec![v("1-0"), arr(vec![v("b")])])]),
        };
        let reply = if shape == "pair-arity" {
            arr(vec![arr(vec![v("s:t:0"), entries, v("surplus")])])
        } else {
            arr(vec![resp2_stream("s:t:0", entries)])
        };
        let message = transport_message(parse(reply, K0));
        assert!(
            message.contains("XREAD reply was not in the expected shape"),
            "{shape}: {message}"
        );
    }
}

#[test]
fn an_entry_with_a_nil_field_list_still_arrives() {
    let reply = arr(vec![resp2_stream(
        "s:t:0",
        arr(vec![arr(vec![v("3-0"), Value::Nil])]),
    )]);
    let batch = parse(reply, K0).unwrap();
    assert_eq!(batch.len(), 1);
    assert!(batch[0].fields.is_empty());
}

// ------------------------------------------------------------------ parse_all

const KEYS3: [&[u8]; 3] = [b"s:t:0", b"s:t:1", b"s:t:2"];
const KEYS2: [&[u8]; 2] = [b"s:t:0", b"s:t:1"];

#[test]
fn parse_all_splits_a_reply_by_key_whatever_order_the_streams_arrive_in() {
    let reply = arr(vec![
        resp2_stream("s:t:2", arr(vec![entry("30-0", &[("b", "two")])])),
        resp2_stream(
            "s:t:0",
            arr(vec![
                entry("10-0", &[("b", "zero")]),
                entry("10-1", &[("b", "zero-b")]),
            ]),
        ),
        resp2_stream("s:t:1", arr(vec![entry("20-0", &[("b", "one")])])),
    ]);
    let slices = parse_all(reply, &KEYS3).unwrap();
    assert_eq!(slices.len(), 3);
    assert_eq!(slices[0].key, SliceKey::Requested(2));
    assert_eq!(slices[0].entries.len(), 1);
    assert_eq!(slices[1].key, SliceKey::Requested(0));
    assert_eq!(
        slices[1].entries.len(),
        2,
        "a stream's own entries must not leak into a neighbour's slice"
    );
    assert_eq!(slices[2].key, SliceKey::Requested(1));
    assert_eq!(field(&slices[2].entries[0], 0).1, b"one");
}

#[test]
fn parse_all_returns_only_the_streams_that_carried_entries() {
    let reply = arr(vec![
        resp2_stream("s:t:0", arr(vec![])),
        resp2_stream("s:t:1", arr(vec![entry("5-0", &[("b", "x")])])),
    ]);
    let slices = parse_all(reply, &KEYS2).unwrap();
    assert_eq!(
        slices.len(),
        1,
        "an empty slice would be handed to the loop as a partition with no work"
    );
    assert_eq!(slices[0].key, SliceKey::Requested(1));
}

#[test]
fn parse_all_handles_the_resp3_map_layout() {
    let reply = arr(vec![
        v("s:t:0"),
        arr(vec![entry("1-0", &[("b", "zero")])]),
        v("s:t:1"),
        arr(vec![entry("2-0", &[("b", "one")])]),
    ]);
    let slices = parse_all(reply, &KEYS2).unwrap();
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].key, SliceKey::Requested(0));
    assert_eq!(slices[1].key, SliceKey::Requested(1));
    assert_eq!(slices[1].entries[0].id, id("2-0"));
}

#[test]
fn parse_all_treats_a_nil_or_empty_reply_as_idle() {
    let keys: [&[u8]; 1] = [K0];
    assert!(parse_all(Value::Nil, &keys).unwrap().is_empty());
    assert!(parse_all(arr(vec![]), &keys).unwrap().is_empty());
    assert!(
        parse_all(Value::SimpleString("OK".into()), &keys)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn parse_all_never_attributes_an_unmatched_key_to_a_requested_slot() {
    let reply = arr(vec![
        resp2_stream("s:t:0", arr(vec![entry("1-0", &[("b", "mine")])])),
        resp2_stream(
            "s:elsewhere:4",
            arr(vec![entry("999-0", &[("b", "foreign")])]),
        ),
    ]);
    let slices = parse_all(reply, &KEYS2).unwrap();
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].key, SliceKey::Requested(0));
    assert_eq!(
        slices[1].key,
        SliceKey::Foreign(b"s:elsewhere:4".to_vec()),
        "the foreign key is reported as it arrived, so the loop can warn"
    );
}

#[test]
fn parse_all_matches_slots_after_the_hint_has_wrapped() {
    let reply = arr(vec![
        resp2_stream("s:t:2", arr(vec![entry("3-0", &[("b", "c")])])),
        resp2_stream("s:t:1", arr(vec![entry("2-0", &[("b", "b")])])),
        resp2_stream("s:t:0", arr(vec![entry("1-0", &[("b", "a")])])),
    ]);
    let keys: Vec<_> = parse_all(reply, &KEYS3)
        .unwrap()
        .into_iter()
        .map(|s| s.key)
        .collect();
    assert_eq!(
        keys,
        [
            SliceKey::Requested(2),
            SliceKey::Requested(1),
            SliceKey::Requested(0)
        ]
    );
}

#[test]
fn parse_all_throws_when_any_stream_in_the_reply_is_malformed() {
    let reply = arr(vec![
        resp2_stream("s:t:0", arr(vec![entry("1-0", &[("b", "ok")])])),
        resp2_stream("s:t:1", arr(vec![arr(vec![v("2-0"), arr(vec![v("b")])])])),
    ]);
    assert!(transport_message(parse_all(reply, &KEYS2)).contains("even field/value list"));
}

/// Rust-only: the single-key rescue of `ParseAll` (one key requested, one unmatched stream).
#[test]
fn parse_all_single_key_rescue_attributes_a_lone_rewritten_key() {
    let keys: [&[u8]; 1] = [K0];
    let reply = arr(vec![resp2_stream(
        "prefix:s:t:0",
        arr(vec![entry("4-0", &[("b", "x")])]),
    )]);
    assert_eq!(
        parse_all(reply, &keys).unwrap()[0].key,
        SliceKey::Requested(0)
    );
}

/// Rust-only: bodies are moved out of the reply, not copied.
#[test]
fn bodies_take_over_the_reply_buffer_without_copying() {
    let body = b"payload".to_vec();
    let ptr = body.as_ptr();
    let reply = arr(vec![resp2_stream(
        "s:t:0",
        arr(vec![arr(vec![
            v("1-0"),
            arr(vec![v("b"), Value::BulkString(body)]),
        ])]),
    )]);
    let batch = parse(reply, K0).unwrap();
    assert_eq!(batch[0].fields[0].1.as_ptr(), ptr);
}

#[test]
fn backoff_ladder_matches_csharp() {
    use super::backoff_next;
    let mut d = 0;
    let mut seen = vec![];
    for _ in 0..8 {
        d = backoff_next(d, 50);
        seen.push(d);
    }
    assert_eq!(seen, [1, 2, 4, 8, 16, 32, 50, 50]);
    assert_eq!(backoff_next(10, 0), 0);
}
