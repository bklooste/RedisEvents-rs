# Interop harness

Cross-language checks for the shared wire contract (`wire-spec/WIRE.md`, `docs/PLAN.md` section 5).

| Piece | Where |
| --- | --- |
| C# host on the RedisEvents NuGet (or source) | `dotnet/InteropHost` |
| Rust host, same CLI | `rust-host` |
| Scenario runner (Python 3 stdlib) | `scenarios/run.py` |

## Host CLI (identical in both hosts)

Output is JSON lines on stdout; logs go to stderr. `REDIS_URL` selects Redis and `STREAMS_KEY_PREFIX`
the key prefix, exactly as the libraries do.

```
publish --topic T --partitions N --key K [--key K2 ...] --type TYPE --body-b64 B64
        [--header k=v ...] [--correlation C] [--traceparent TP] [--count N] [--seq]
        [--batch | --buffered] [--no-warmup]
consume --topic T --consumer C --partitions N --expect N --timeout SECONDS
positions --topic T --consumer C
admin-reset --topic T --consumer C [--partition P] --to-id ID
meta --topic T
```

Exit codes: `0` ok, `1` usage/unexpected, `2` timeout, `3` not implemented (the runner SKIPs),
`4` the library refused (shrink, codec version, config).

## Running locally

```
dotnet build interop/dotnet/InteropHost -c Release -o interop/dotnet/out
#   against C# source instead of the NuGet:  -p:UseLocalRedisEvents=/path/to/RedisEvents
#   another package version:                 -p:RedisEventsVersion=0.2.37   (default pinned in dotnet/Versions.props)
cargo build -p interop-rust-host
python3 interop/scenarios/run.py --csharp "dotnet interop/dotnet/out/InteropHost.dll" \
        --rust target/debug/interop-rust-host            # needs Redis 7.4+ on $REDIS_URL
python3 interop/scenarios/run.py --list                  # scenarios and their gates
```

## Gates and scenarios

Directional scenarios run for all four (producer, consumer) pairs. Rust to C# is gate G1, C# to Rust is G2,
and same-language pairs are the `baseline`. A pair is SKIPPED when a host returns exit code 3.

| Gate | Scenarios | Rust verbs needed |
| --- | --- | --- |
| G1/G2 | `keyed_publish_{1,4,7,16}_partitions`, `headers_correlation_traceparent`, `optional_fields_omitted`, `unicode_empty_long_keys`, `batch_publish`, `buffered_publish`, `positions_resume`, `partition_grow_refuse_shrink`, `codec_version_guard` | publish (B1..), consume (C4), positions, meta |
| G3 | `admin_reset` (pair = admin language x consumer language) | admin-reset (D1, Rust side implemented), positions, consume |
| G4, G5 | not yet: JSON/MessagePack/LZ4 payloads, outbox/ES (E1, E3); the CQRS-ES sample matrix runs separately (E5d) | later WPs add verbs/scenarios |

`buffered_publish_cold_order` is an expected failure (XFAIL): a cold C# `BufferedStreamPublisher` reorders
its first flush.

## Adding a Rust verb

Replace the body of the verb in `rust-host/src/main.rs` (keep flags and JSON line shapes as in
`InteropHost/Program.cs`), and stop returning 3 for it. The pairs that need it start running on the next CI
run; there is no list to edit. New scenarios go in `scenarios/run.py` with `@scenario(name, gate)`.
