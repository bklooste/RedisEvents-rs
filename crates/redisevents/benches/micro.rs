//! Micro-benchmarks, a one-to-one port of the C# `StreamsMicroBenchmarks`
//! (`tst/RedisEvents.UnitTests/StreamsMicroBenchmarks.cs`): the entry codec, the partition router
//! and stream-id parsing/formatting, with the same inputs.
//!
//! Run with `cargo bench -p redisevents --bench micro`. End-to-end throughput against a real Redis
//! lives in `tests/perf.rs`.

#![allow(missing_docs)]

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use redisevents::wire::codec::{self, CODEC_VERSION, EntryFields};
use redisevents::wire::router::{RoundRobin, partition_for_key};
use redisevents::wire::{HeaderBlock, StreamId};

/// C# `EntryCodec.Encode(...)` returns the field array; the Rust equivalent appends the same fields
/// to an `XADD` command, so the benchmark builds the command (key and `*` included) per call.
/// Built exactly as the publisher builds it: presized from `arg_count` / `data_len`.
fn encode(fields: &EntryFields<'_>) -> redis::Cmd {
    const STREAM: &str = "s:{perf}:0";
    let mut cmd = redis::Cmd::with_capacity(
        3 + fields.arg_count(),
        4 + STREAM.len() + 1 + fields.data_len(),
    );
    cmd.arg("XADD").arg(STREAM).arg("*");
    fields.write_to(&mut cmd);
    cmd
}

fn entry_codec(c: &mut Criterion) {
    let body = vec![b'x'; 256];
    let headers = [("tenant", "au"), ("source", "feed-kalshi")];
    let empty = HeaderBlock::empty();

    // Encode: headers are packed per publish call in the publisher, so they are packed here too,
    // matching the C# Encode that packs its headers argument on every call.
    let mut g = c.benchmark_group("EntryCodec");
    g.bench_function("Encode", |b| {
        b.iter(|| {
            let packed = HeaderBlock::pack(black_box(&headers)).expect("pack");
            let fields = EntryFields {
                body: black_box(&body),
                msg_type: "BetPlaced",
                partition_key: Some("customer-1"),
                correlation_id: Some("corr-1"),
                trace_parent: None,
                headers: &packed,
            };
            encode(&fields)
        });
    });
    g.bench_function("EncodeMinimal", |b| {
        b.iter(|| {
            let fields = EntryFields {
                body: black_box(&body),
                msg_type: "BetPlaced",
                partition_key: Some(""),
                correlation_id: None,
                trace_parent: None,
                headers: &empty,
            };
            encode(&fields)
        });
    });

    // Decode: the field/value pairs of one entry as the fetch path hands them over.
    let packed = HeaderBlock::pack(&headers).expect("pack");
    let entry: Vec<(Bytes, Bytes)> = vec![
        (Bytes::from_static(b"b"), Bytes::from(body.clone())),
        (Bytes::from_static(b"t"), Bytes::from_static(b"BetPlaced")),
        (Bytes::from_static(b"k"), Bytes::from_static(b"customer-1")),
        (Bytes::from_static(b"c"), Bytes::from_static(b"corr-1")),
        (Bytes::from_static(b"h"), packed.packed().clone()),
    ];
    let id = StreamId::new(1_700_000_000_000, 42);
    g.bench_function("Decode", |b| {
        b.iter(|| codec::decode(id, 3, black_box(&entry).iter().cloned(), CODEC_VERSION));
    });
    g.finish();

    // The smoke check the C# MicroBenchmarkTests does: the bodies measure what they claim.
    let m = codec::decode(id, 3, entry.iter().cloned(), CODEC_VERSION).expect("decode");
    assert_eq!(m.msg_type(), "BetPlaced");
    assert_eq!(m.partition_key(), "customer-1");
    assert_eq!(m.partition(), 3);
    assert_eq!(m.id(), id);
    assert_eq!(m.body().len(), 256);
}

fn partition_router(c: &mut Criterion) {
    let key = b"customer-01HZY3W7Q4K8N2";
    let rr = RoundRobin::new();
    let mut g = c.benchmark_group("PartitionRouter");
    g.bench_function("ForKeyPowerOfTwo", |b| {
        b.iter(|| partition_for_key(black_box(key), black_box(8)));
    });
    g.bench_function("ForKeyOddCount", |b| {
        b.iter(|| partition_for_key(black_box(key), black_box(6)));
    });
    g.bench_function("ForKeySinglePartition", |b| {
        b.iter(|| partition_for_key(black_box(key), black_box(1)));
    });
    g.bench_function("RoundRobin", |b| b.iter(|| rr.next(black_box(8))));
    g.finish();
}

fn stream_id(c: &mut Criterion) {
    const TEXT: &str = "1700000000000-42";
    let id = StreamId::new(1_700_000_000_000, 42);
    let mut buf = [0u8; 41];
    let mut g = c.benchmark_group("StreamId");
    // Rust has one fallible parse; C# Parse and TryParse both map onto it.
    g.bench_function("Parse", |b| {
        b.iter(|| StreamId::parse(black_box(TEXT)).expect("valid"));
    });
    g.bench_function("TryParse", |b| {
        b.iter(|| StreamId::parse(black_box(TEXT)).is_ok());
    });
    g.bench_function("TryFormat", |b| {
        b.iter(|| black_box(id).format_into(&mut buf).len());
    });
    g.bench_function("Format", |b| b.iter(|| black_box(id).to_string()));
    g.finish();
    assert_eq!(id.to_string(), TEXT);
}

criterion_group!(benches, entry_codec, partition_router, stream_id);
criterion_main!(benches);
