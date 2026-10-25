//! Golden vectors from W0.1 (`wire-spec/vectors/`), checked against the A1 wire layer.
//!
//! Every case in `routing.json`, `entries.json`, `headers.json`, `keys.json` and the stream-id part
//! of `formats.json` must pass. A failing case is a bug in this crate, never in the vector.
//!
//! Not covered here, by design: `formats.json` position, reset-marker, owner and datetime fields
//! (B4/B5), and `messagepack.json` (A3/E1).

use std::path::PathBuf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use redisevents::error::Error;
use redisevents::wire::codec::{CODEC_VERSION, EntryFields, decode};
use redisevents::wire::{HeaderBlock, Keys, StreamId, partition_for_key};
use serde_json::Value;

fn vectors(file: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../wire-spec/vectors")
        .join(file);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("cannot parse {}: {e}", path.display()))
}

fn cases<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v[key]
        .as_array()
        .unwrap_or_else(|| panic!("vector key `{key}` is not an array"))
}

fn s<'a>(v: &'a Value, key: &str) -> &'a str {
    v[key]
        .as_str()
        .unwrap_or_else(|| panic!("`{key}` is not a string in {v}"))
}

fn b64(text: &str) -> Vec<u8> {
    STANDARD
        .decode(text)
        .unwrap_or_else(|e| panic!("bad base64 `{text}`: {e}"))
}

/// The `[k, v]` pairs of a JSON array of two-element arrays.
fn pairs(v: &Value) -> Vec<(String, String)> {
    v.as_array()
        .expect("header list is an array")
        .iter()
        .map(|p| {
            (
                p[0].as_str().expect("header key").to_owned(),
                p[1].as_str().expect("header value").to_owned(),
            )
        })
        .collect()
}

/// Packs `headers` the way a publisher does, for the vector's `headers` input.
fn pack_pairs(headers: &[(String, String)]) -> HeaderBlock {
    HeaderBlock::pack(headers).expect("vector headers are within the caps")
}

// ------------------------------------------------------------------ routing.json

#[test]
fn routing_routes_match_partition_for_key() {
    let v = vectors("routing.json");
    let routes = cases(&v, "routes");
    assert_ne!(routes.len(), 0);
    for route in routes {
        let key = s(route, "key");
        let partitions = u32::try_from(route["partitions"].as_u64().expect("partitions"))
            .expect("partitions fit u32");
        let expected = u32::try_from(route["partition"].as_u64().expect("partition"))
            .expect("partition fits u32");
        assert_eq!(
            partition_for_key(key.as_bytes(), partitions),
            expected,
            "route key={key:?} partitions={partitions}"
        );
    }
}

#[test]
fn routing_raw_xxh3_matches_vector() {
    let v = vectors("routing.json");
    let hashes = cases(&v, "xxh3");
    assert_ne!(hashes.len(), 0);
    for case in hashes {
        let key = s(case, "key");
        let expected: u64 = s(case, "hash_u64_decimal_string")
            .parse()
            .expect("decimal u64");
        assert_eq!(
            xxhash_rust::xxh3::xxh3_64(key.as_bytes()),
            expected,
            "xxh3 key={key:?}"
        );
    }
}

// ------------------------------------------------------------------ entries.json

#[test]
fn entries_write_fields_and_decode_round_trip() {
    let v = vectors("entries.json");
    // entries.json is a top-level array, not an object.
    let entries = v.as_array().expect("entries.json is an array");
    assert_ne!(entries.len(), 0);
    for case in entries {
        let name = s(case, "name");
        let input = &case["input"];
        let body = b64(s(input, "body_b64"));
        let msg_type = s(input, "type");
        let state = input["state"].as_bool().expect("state flag");
        let headers = if input["headers"].is_null() {
            HeaderBlock::empty()
        } else {
            pack_pairs(&pairs(&input["headers"]))
        };
        let partition_key = if state {
            None
        } else {
            // A null partitionKey on a topic entry is written as an empty `k`.
            Some(input["partitionKey"].as_str().unwrap_or(""))
        };
        let correlation = input["correlationId"].as_str();
        let trace = input["traceParent"].as_str();

        let fields = EntryFields {
            body: &body,
            msg_type,
            partition_key,
            correlation_id: correlation,
            trace_parent: trace,
            headers: &headers,
        };

        // What `write_to` appends, as the (name, bytes) pairs the server would store.
        let mut cmd = redis::Cmd::new();
        fields.write_to(&mut cmd);
        let written: Vec<(String, Vec<u8>)> = written_pairs(&cmd);

        let expected = cases_fields(case);
        assert_eq!(written, expected, "entry `{name}`: written fields");

        // Decoding the written fields gives back the input.
        let id = StreamId::new(1_726_000_000_000, 7);
        let stored: Vec<(String, Bytes)> = written
            .into_iter()
            .map(|(n, v)| (n, Bytes::from(v)))
            .collect();
        let msg = decode(id, 2, stored, CODEC_VERSION)
            .unwrap_or_else(|e| panic!("entry `{name}` failed to decode: {e}"));
        assert_eq!(msg.body().as_ref(), body.as_slice(), "entry `{name}`: body");
        assert_eq!(msg.msg_type(), msg_type, "entry `{name}`: type");
        assert_eq!(
            msg.partition_key(),
            partition_key.unwrap_or(""),
            "entry `{name}`: partition key"
        );
        assert_eq!(
            msg.correlation_id(),
            correlation.unwrap_or(""),
            "entry `{name}`: correlation id"
        );
        assert_eq!(msg.trace_parent(), trace, "entry `{name}`: traceparent");
        let expected_headers = if input["headers"].is_null() {
            Vec::new()
        } else {
            pairs(&input["headers"])
        };
        assert_eq!(
            msg.headers().to_vec().expect("headers decode"),
            expected_headers,
            "entry `{name}`: headers"
        );
        assert_eq!(msg.id(), id);
        assert_eq!(msg.partition(), 2);
    }
}

/// The vector's expected `fields`, as `(name, bytes)` with values base64-decoded.
fn cases_fields(case: &Value) -> Vec<(String, Vec<u8>)> {
    case["fields"]
        .as_array()
        .expect("fields")
        .iter()
        .map(|f| {
            (
                f[0].as_str().expect("field name").to_owned(),
                b64(f[1].as_str().expect("field value")),
            )
        })
        .collect()
}

/// The field/value pairs a `Cmd` built by `write_to` carries.
fn written_pairs(cmd: &redis::Cmd) -> Vec<(String, Vec<u8>)> {
    let args: Vec<Vec<u8>> = cmd
        .args_iter()
        .map(|a| match a {
            redis::Arg::Simple(bytes) => bytes.to_vec(),
            _ => panic!("write_to never emits a cursor"),
        })
        .collect();
    assert!(
        args.len().is_multiple_of(2),
        "write_to emits name/value pairs"
    );
    args.chunks(2)
        .map(|p| {
            (
                String::from_utf8(p[0].clone()).expect("ascii name"),
                p[1].clone(),
            )
        })
        .collect()
}

// ------------------------------------------------------------------ headers.json

#[test]
fn headers_valid_pack_and_read_back() {
    let v = vectors("headers.json");
    let valid = cases(&v, "valid");
    assert_ne!(valid.len(), 0);
    for case in valid {
        let name = s(case, "name");
        let headers = pairs(&case["headers"]);
        let expected_bytes = b64(s(case, "packed_b64"));

        let block = pack_pairs(&headers);
        assert_eq!(
            block.packed().as_ref(),
            expected_bytes.as_slice(),
            "case `{name}`: packed bytes"
        );

        // Reading the packed bytes back, through `from_packed`, gives the same headers in order.
        let read = HeaderBlock::from_packed(Bytes::from(expected_bytes));
        assert_eq!(
            read.to_vec().expect("valid block"),
            headers,
            "case `{name}`: iter"
        );
        assert_eq!(
            read.count().expect("valid block"),
            headers.len(),
            "case `{name}`: count"
        );

        // Lookup returns the first match for each key.
        for (key, _) in &headers {
            let first = headers
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.as_str());
            let got = read.get(key).expect("valid block");
            assert_eq!(got.as_deref(), first, "case `{name}`: get({key:?})");
        }
    }
}

#[test]
fn headers_invalid_blocks_are_reported_not_read() {
    let v = vectors("headers.json");
    let invalid = cases(&v, "invalid");
    assert_ne!(invalid.len(), 0);
    for case in invalid {
        let name = s(case, "name");
        let block = HeaderBlock::from_packed(Bytes::from(b64(s(case, "packed_b64"))));

        // Iteration yields exactly one error, a transport error, then ends.
        let items: Vec<_> = block.iter().collect();
        let errors: Vec<&Error> = items.iter().filter_map(|i| i.as_ref().err()).collect();
        assert_eq!(errors.len(), 1, "case `{name}`: one error from iter");
        assert!(
            matches!(errors[0], Error::Transport { .. }),
            "case `{name}`: error is Transport, got {:?}",
            errors[0]
        );
        assert!(
            matches!(items.last(), Some(Err(_))),
            "case `{name}`: the error ends the iteration"
        );
        assert!(
            matches!(block.count(), Err(Error::Transport { .. })),
            "case `{name}`: count fails"
        );
    }
}

#[test]
fn headers_pack_rejects_over_the_caps() {
    let v = vectors("headers.json");
    let rejects = cases(&v, "pack_rejects");
    assert_ne!(rejects.len(), 0);
    for case in rejects {
        let name = s(case, "name");
        let headers = pairs(&case["headers"]);
        let result = HeaderBlock::pack(&headers);
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "case `{name}`: pack must reject with InvalidArgument"
        );
    }
}

// ------------------------------------------------------------------ keys.json

#[test]
fn keys_names_match_vector() {
    let v = vectors("keys.json");
    let keys = Keys::with_prefix(s(&v, "prefix"));
    let cases = cases(&v, "keys");
    assert_ne!(cases.len(), 0);
    for case in cases {
        let name = s(case, "name");
        let expected = s(case, "key");
        let (topic, shape) = name.split_once(':').expect("name is topic:shape");
        let got = match shape {
            "stream:p0:colocated" => keys.stream(topic, 0, true),
            "stream:p3:spread" => keys.stream(topic, 3, false),
            "positions" => keys.positions(topic, "svc"),
            "positions-meta" => keys.positions_meta(topic, "svc"),
            "ownership" => keys.ownership(topic, "svc"),
            "topic-meta" => keys.topic_meta(topic),
            "state" => keys.state(topic, "es:Order:42"),
            "dedupe" => keys.dedupe(topic, "evt-1").expect("valid scope"),
            other => panic!("no builder mapped for shape `{other}` in case `{name}`"),
        };
        assert_eq!(got, expected, "key case `{name}`");
    }
}

#[test]
fn keys_prefix_resolution_matches_vector() {
    let v = vectors("keys.json");
    let cases = cases(&v, "prefix_resolution");
    assert_ne!(cases.len(), 0);
    for case in cases {
        let name = s(case, "name");
        let env = &case["env"];
        let lookup = |var: &str| env[var].as_str().map(str::to_owned);
        let keys = Keys::from_lookup(lookup);
        assert_eq!(keys.prefix(), s(case, "prefix"), "prefix case `{name}`");
    }
}

// ------------------------------------------------------------------ formats.json (stream ids)

#[test]
fn formats_stream_id_format_matches_vector() {
    let v = vectors("formats.json");
    let cases = cases(&v, "stream_id_format");
    assert_ne!(cases.len(), 0);
    for case in cases {
        let id = StreamId::new(
            s(case, "ms").parse().expect("ms"),
            s(case, "seq").parse().expect("seq"),
        );
        let expected = s(case, "formatted");
        assert_eq!(id.to_string(), expected, "format {expected}");
        let mut buf = [0u8; 41];
        assert_eq!(id.format_into(&mut buf), expected, "format_into {expected}");
    }
}

#[test]
fn formats_stream_id_parse_matches_vector() {
    let v = vectors("formats.json");
    let cases = cases(&v, "stream_id_parse");
    assert_ne!(cases.len(), 0);
    for case in cases {
        let input = s(case, "input");
        let valid = case["valid"].as_bool().expect("valid flag");
        let parsed = StreamId::parse(input);
        let parsed_bytes = StreamId::parse_bytes(input.as_bytes());
        assert_eq!(parsed.is_ok(), valid, "parse {input:?}");
        assert_eq!(parsed_bytes.is_ok(), valid, "parse_bytes {input:?}");
        if valid {
            let id = parsed.expect("valid");
            assert_eq!(id.ms.to_string(), s(case, "ms"), "ms of {input:?}");
            assert_eq!(id.seq.to_string(), s(case, "seq"), "seq of {input:?}");
        }
    }
}
