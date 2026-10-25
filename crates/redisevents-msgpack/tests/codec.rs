//! Golden-vector and ported C# tests for the MessagePack + LZ4 codec.
#![allow(missing_docs, clippy::unwrap_used)]

use base64::Engine;
use proptest::prelude::*;
use redisevents::PayloadCodec;
use redisevents_msgpack::{
    Compression, Layout, MessagePack, MessagePackMap, Options, decode, decompress, encode_plain,
    encode_with, frame_block, frame_block_array, frame_block_array_with,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    id: i32,
    name: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Large {
    id: i32,
    lines: Vec<String>,
}

fn large() -> Large {
    Large {
        id: 1,
        lines: (0..200)
            .map(|i| format!("line-{i:03}-the quick brown fox jumps"))
            .collect(),
    }
}

fn vector(name: &str) -> Vec<u8> {
    let v: serde_json::Value =
        serde_json::from_str(include_str!("../../../wire-spec/vectors/messagepack.json")).unwrap();
    let c = v["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == name)
        .unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(c["body_b64"].as_str().unwrap())
        .unwrap()
}

const ARRAY: &str = "large-keyed-int-publish-lz4blockarray";
const BLOCK: &str = "large-keyed-int-lz4block";

// ---- golden vectors ----

#[test]
fn small_keyed_array_matches_vector() {
    let body = vector("small-keyed-int");
    let o = Order {
        id: 7,
        name: "seven".into(),
    };
    assert_eq!(&MessagePack::encode(&o).unwrap()[..], &body[..]);
    assert_eq!(decode::<Order>(&body).unwrap(), o);
}

#[test]
fn small_string_keyed_map_matches_vector() {
    let body = vector("small-string-keyed-map");
    let o = Order {
        id: 8,
        name: "eight".into(),
    };
    assert_eq!(&MessagePackMap::encode(&o).unwrap()[..], &body[..]);
    assert_eq!(decode::<Order>(&body).unwrap(), o);
}

#[test]
fn both_lz4_vectors_decode_to_the_object() {
    for name in [ARRAY, BLOCK] {
        assert_eq!(decode::<Large>(&vector(name)).unwrap(), large(), "{name}");
        let body = vector(name);
        let plain = decompress(&body).unwrap();
        assert_eq!(plain.len(), 7205, "{name}");
        assert_eq!(
            &plain[..],
            &encode_plain(&large(), Layout::Array).unwrap()[..]
        );
    }
}

#[test]
fn block_array_framing_is_byte_identical_to_csharp_apart_from_lz4_output() {
    let theirs = vector(ARRAY);
    let plain = encode_plain(&large(), Layout::Array).unwrap();
    // C# chunked this body 4001 + 3204 (its writer's buffer segments).
    let mine = frame_block_array_with(&plain, &[4001, 3204]);
    // Header: array(3), ext8 len 6 type 98, uint16 4001, uint16 3204, then bin32 (0xc6).
    let header = [
        0x93, 0xc7, 0x06, 0x62, 0xcd, 0x0f, 0xa1, 0xcd, 0x0c, 0x84, 0xc6,
    ];
    assert_eq!(&theirs[..11], &header);
    assert_eq!(&mine[..11], &header);
    assert_eq!(decompress(&mine).unwrap(), decompress(&theirs).unwrap());
}

#[test]
fn lz4_block_framing_matches_csharp_header() {
    let theirs = vector(BLOCK);
    let plain = encode_plain(&large(), Layout::Array).unwrap();
    let mine = frame_block(&plain);
    // ext16, type 99, int32 7205.
    assert_eq!(&theirs[..1], &mine[..1]);
    assert_eq!(&theirs[3..9], &mine[3..9]);
    assert_eq!(&theirs[3..9], &[0x63, 0xd2, 0x00, 0x00, 0x1c, 0x25]);
    assert_eq!(decompress(&mine).unwrap(), decompress(&theirs).unwrap());
}

// ---- ported from MessagePackPublishConsumeTests ----

#[test]
fn publish_round_trips_a_small_message_uncompressed() {
    let o = Order {
        id: 7,
        name: "seven".into(),
    };
    let body = MessagePack::encode(&o).unwrap();
    assert_eq!(&body[..], &encode_plain(&o, Layout::Array).unwrap()[..]);
    assert_eq!(MessagePack::decode::<Order>(&body).unwrap(), o);
}

#[test]
fn publish_compresses_a_large_message_and_decodes_transparently() {
    let o = Order {
        id: 1,
        name: "a".repeat(4000),
    };
    let body = MessagePack::encode(&o).unwrap();
    let plain = encode_plain(&o, Layout::Array).unwrap();
    assert!(body.len() < plain.len());
    assert_eq!(
        body[1..4],
        [0xc7, 0x03, 0x62][..],
        "ext 98 follows the array header"
    );
    assert_eq!(decode::<Order>(&body).unwrap(), o);
}

#[test]
fn batch_round_trips_every_message_in_order() {
    let msgs = [
        Order {
            id: 1,
            name: "one".into(),
        },
        Order {
            id: 2,
            name: "two".into(),
        },
        Order {
            id: 3,
            name: "three".into(),
        },
    ];
    let back: Vec<Order> = msgs
        .iter()
        .map(|m| MessagePack::encode(m).unwrap())
        .map(|b| decode(&b).unwrap())
        .collect();
    assert_eq!(back, msgs);
}

#[test]
fn deserialize_decodes_a_compressed_body() {
    let o = Order {
        id: 1,
        name: "b".repeat(4000),
    };
    let body = frame_block_array(&encode_plain(&o, Layout::Array).unwrap(), 64 * 1024);
    assert_eq!(decode::<Order>(&body).unwrap(), o);
}

// ---- ported from MessagePackCompressionOptionsTests ----

fn opts(compression: Compression) -> Options {
    Options {
        layout: Layout::Array,
        compression,
    }
}

#[test]
fn disabled_never_compresses_regardless_of_size() {
    let o = Order {
        id: 1,
        name: "a".repeat(4000),
    };
    let body = encode_with(&o, &opts(Compression::DISABLED)).unwrap();
    assert_eq!(&body[..], &encode_plain(&o, Layout::Array).unwrap()[..]);
}

#[test]
fn a_lower_threshold_compresses_what_the_default_would_not() {
    let o = Order {
        id: 1,
        name: "a".repeat(300),
    };
    let plain = encode_plain(&o, Layout::Array).unwrap();
    assert_eq!(
        &encode_with(&o, &Options::default()).unwrap()[..],
        &plain[..]
    );
    let low = Compression {
        threshold: 100,
        ..Compression::DEFAULT
    };
    assert_ne!(&encode_with(&o, &opts(low)).unwrap()[..], &plain[..]);
}

#[test]
fn a_higher_threshold_leaves_a_large_message_uncompressed() {
    let o = Order {
        id: 1,
        name: "a".repeat(4000),
    };
    let plain = encode_plain(&o, Layout::Array).unwrap();
    let c = Compression {
        threshold: plain.len(),
        ..Compression::DEFAULT
    };
    assert_eq!(
        &encode_with(&o, &opts(c)).unwrap()[..],
        &plain[..],
        "exactly at threshold: no compression"
    );
    let c = Compression {
        threshold: plain.len() - 1,
        ..Compression::DEFAULT
    };
    assert_ne!(&encode_with(&o, &opts(c)).unwrap()[..], &plain[..]);
}

#[test]
fn default_is_enabled_with_a_1024_byte_threshold() {
    let d = Compression::default();
    assert!(d.enabled);
    assert_eq!(d.threshold, 1024);
}

// ---- robustness ----

#[test]
fn malformed_framing_is_an_error_not_a_panic() {
    let good = vector(ARRAY);
    for cut in [1, 5, 11, 100, good.len() - 1] {
        assert!(decode::<Large>(&good[..cut]).is_err(), "cut {cut}");
    }
    // Declared length far beyond the cap.
    let mut bad = vec![0xc7, 0x06, 0x63, 0xce, 0xff, 0xff, 0xff, 0xff];
    bad.extend_from_slice(&[0, 0]);
    assert!(decompress(&bad).is_err());
}

// ---- property tests ----

proptest! {
    #[test]
    fn round_trips_around_the_threshold(n in 900usize..1200, map in any::<bool>()) {
        let o = Order { id: 5, name: "x".repeat(n) };
        let layout = if map { Layout::Map } else { Layout::Array };
        let body = encode_with(&o, &Options { layout, ..Options::default() }).unwrap();
        prop_assert_eq!(decode::<Order>(&body).unwrap(), o);
    }

    #[test]
    fn round_trips_across_chunk_boundaries(n in 1usize..9000, chunk in 1usize..3000, tail in 0u8..255) {
        let o = Order { id: 9, name: format!("{}{}", "ab".repeat(n), tail) };
        let plain = encode_plain(&o, Layout::Array).unwrap();
        let body = frame_block_array(&plain, chunk);
        let inflated = decompress(&body).unwrap();
        prop_assert_eq!(&inflated[..], &plain[..]);
        prop_assert_eq!(decode::<Order>(&body).unwrap(), o);
        let single = decompress(&frame_block(&plain)).unwrap().into_owned();
        prop_assert_eq!(single, plain);
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..300)) {
        let _ = decode::<Order>(&bytes);
    }
}
