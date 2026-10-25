# RedisEvents wire format, codec v1

This is the contract shared by the C# library ([bklooste/RedisEvents](https://github.com/bklooste/RedisEvents))
and this Rust port. Two implementations are compatible exactly when they agree on everything in this
document. Internal design is free to differ.

The C# code is the reference implementation. Each section names the source file it was taken from.
Where this document and the C# code disagree, the code wins and this document has a bug.

"MUST", "MUST NOT" and "SHOULD" are used as in RFC 2119.

## 1. Key namespace
*Source: `src/RedisEvents/Config/KeyNamespace.cs`*

Every key below starts with a prefix, resolved in this order:

1. `STREAMS_KEY_PREFIX`, when set and not blank, used **verbatim**.
2. Otherwise `"<env>:re:"`, where `<env>` is the lowercased value of the first set variable among
   `ASPNETCORE_ENVIRONMENT` and `DOTNET_ENVIRONMENT`, defaulting to `production`.

Implementations MUST resolve the prefix this way, including the two .NET variable names, so that a
C# and a Rust service in the same environment read the same keys. An implementation MAY accept
further variables, but only ranked below these.

The prefix always goes *outside* any `{…}` hash tag, so it never changes a key's cluster slot.

## 2. Keys
*Source: `Wire/StreamKeys.cs`, `Producer/Outbox.cs` (`StateKey`), `Producer/Idempotency.cs`, `RedisEvents.Projections/RedisViewStore.cs`*

`{` and `}` are literal Redis Cluster hash tags. `<prefix>` is §1.

| Purpose | Key | Type |
| --- | --- | --- |
| Partition stream | `<prefix>s:{<topic>}:<p>` | stream |
| Partition stream, co-location off | `<prefix>s:<topic>:<p>` | stream |
| Positions | `<prefix>p:{<topic>}:<consumer>` | hash |
| Positions meta | `<prefix>p:{<topic>}:<consumer>:meta` | hash |
| Ownership / leases | `<prefix>o:{<topic>}:<consumer>` | hash |
| Topic meta | `<prefix>m:{<topic>}` | hash |
| State stream | `<prefix>{<topic>}:state:<name>` | stream |
| Event-sourced aggregate | state stream with `<name>` = `es:<Aggregate>:<id>` | stream |
| View | `<prefix>{<topic>}:view:<owner>:<viewName>` | hash |
| Dedupe claim | `<prefix>dedupe:{<scope>}:<id>` | string, `SET NX PX` |

`<p>` is the partition index in invariant decimal, `0`-based. A dedupe `<scope>` MUST NOT contain
`{` or `}`.

## 3. Topic meta
*Source: `Admin/StreamAdmin.cs`*

`m:{topic}` fields, each written with `HSETNX` the first time a topic is used:

| Field | Value |
| --- | --- |
| `partitions` | partition count, decimal |
| `v` | codec version, `1` |
| `createdUtc` | ISO-8601 round-trip (§9) |
| `maxLen` | approximate `MAXLEN` used when trimming, decimal |

- A process configured with **more** partitions than recorded MUST overwrite `partitions` and log a
  warning. Nothing is lost, but per-key ordering is broken until the backlog on the old partitions
  drains.
- A process configured with **fewer** MUST refuse to start, because shrinking orphans unread
  entries.
- A reader MUST refuse to decode entries when `v` is anything other than a version it supports.

## 4. Stream entries
*Source: `Wire/EntryCodec.cs`*

Field names are one byte:

| Field | Meaning | Presence |
| --- | --- | --- |
| `b` | body, the publisher's bytes verbatim | always |
| `t` | message type string | always |
| `k` | partition key, UTF-8, may be empty | always on a topic entry; never on a state entry |
| `c` | correlation id | optional |
| `p` | W3C `traceparent` | optional |
| `h` | packed headers (§5) | optional |

Rules:
- A writer MUST omit an optional field that has no value, rather than write it empty.
- Writers emit fields in the order above. Readers MUST NOT depend on the order.
- A reader MUST ignore field names it does not know: new fields are additive within a codec
  version.
- There is no envelope and no timestamp field. Enqueue time is the stream id's millisecond part.
- A state entry (written by the outbox or event sourcing) is `b` and `t`, plus `c`/`p`/`h` only when
  the topic keeps metadata on state entries. It never has `k`.

### Stream ids
`<ms>-<seq>`, both non-negative decimal integers no larger than **2^63 − 1** (C# stores them as `long`, so it rejects `9223372036854775808-0` even though Redis allows up to 2^64 − 1). Implementations MUST reject ids above that limit, so both sides accept the same set. A stored position is *exclusive*: the consumer
resumes strictly after it.

### Type strings
`t` is free text, but cross-language topics MUST use explicit, stable type strings. The C# typed
API defaults to `typeof(T).FullName`, which no other language can reproduce.

## 5. Header block (`h`)
*Source: `Wire/HeaderBlock.cs`*

```
len(key) ':' key  len(value) ':' value  len(key) ':' key  len(value) ':' value  …
```

- `len` is the ASCII decimal count of **UTF-8 bytes**. Keys and values may contain `:` and digits:
  the length decides where a field ends, never a delimiter search.
- Keys are compared byte for byte. Order is preserved, and duplicates are allowed (lookup returns
  the first).
- Caps: at most **64** headers, and at most **8192** packed bytes. A writer MUST error when a cap is
  exceeded and MUST NOT truncate.
- A reader MUST error on a non-digit in a length, an empty or unterminated length, a length past
  the end of the block, or a key with no value.

## 6. Partition routing
*Source: `Wire/PartitionRouter.cs`*

For `n` partitions and partition key `key`:

```
if n <= 1:          partition = 0
elif key is empty:  round-robin (implementation-defined; no ordering promise)
else:
    h = xxh3_64(utf8(key), seed = 0)          # XXH3 64-bit, as System.IO.Hashing.XxHash3.HashToUInt64
    partition = h & (n - 1)  if n is a power of two
                h % n        otherwise
```

Keys are hashed exactly as given, with no Unicode normalisation and no case folding.

## 7. Positions
*Source: `Positions/RedisPositionStore.cs`, `Admin/StreamAdmin.Reset.cs`, `Positions/ResetMarker.cs`*

`p:{topic}:<consumer>`, one field per partition:

| Field | Value |
| --- | --- |
| `<p>` | `<streamId>\|<instanceId>`. A value with no `\|` is read as a bare stream id. |
| `__reset:<p>` | reset marker: `<targetId>\|<issuedUtc>`. The time is optional on read. |

Fields that do not parse as a non-negative integer are not positions, and readers MUST skip them.

- `<instanceId>` is a GUID, lowercase hyphenated (.NET `"D"`).
- The admin identity **`6f72616e-6765-4164-6d69-6e5265736574`** marks an operator rewind. A
  consumer that finds it in a partition it owns MUST treat it as a rewind and keep the partition. A
  different, non-admin id means another writer: the consumer MUST check the ownership hash (§8) and
  stand the partition down only if that id holds a live claim.
- **Reset protocol:** an admin writes the position and the `__reset:<p>` marker together. A running
  consumer notices the marker on its next flush tick, rewinds its read cursor to `targetId`, and
  deletes the marker **only after** the read loop has actually rewound. A consumer starting cold
  applies every pending marker for the partitions it owns before its first read.

`p:{topic}:<consumer>:meta` is diagnostic only and nothing reads it. Per partition:
`<p>:updatedUtc` (§9) and `<p>:instanceId`.

## 8. Ownership
*Source: `Ownership/OwnershipRegistry.cs`*

`o:{topic}:<consumer>`:

| Field | Value | TTL |
| --- | --- | --- |
| `<p>` | owner: `<instanceId>\|<podName>` | `HEXPIRE` lease TTL |
| `i:<instanceId>` | the same owner value (presence) | `HEXPIRE` lease TTL |
| `__hexpire-probe:<instanceId>` | `1`, briefly at startup | deleted after the probe |

- `<podName>` is `POD_NAME` (trimmed), or empty.
- Per-field TTLs need **Redis 7.4 or later**. An implementation MUST probe for `HEXPIRE` at startup
  and refuse Lease mode without it.
- Readers MUST skip fields that are not a partition number.

### Lease mode (default)
Every renew interval, each instance runs [`lua/lease.lua`](lua/lease.lua) with
`KEYS[1]` = the ownership key and
`ARGV` = `[ownerValue, ttlSeconds, partitionCount, "i:<instanceId>"]`. The script returns the
sorted partitions this instance now holds. On shutdown it runs [`lua/release.lua`](lua/release.lua)
with `ARGV` = `[ownerValue, field…]`, which deletes only fields still holding `ownerValue`.

Implementations MUST send these scripts **byte for byte** (CI enforces this with
`scripts/check_wire_drift.py`). The script alone decides who owns what, so instances of different
languages under one consumer name then share partitions fairly.

Defaults: TTL 30 s, with the renew interval below the TTL.

### Static mode
Ownership is computed from `STREAMS_INSTANCE_INDEX` / `STREAMS_INSTANCE_COUNT`, or from the
`-<n>` ordinal suffix of `POD_NAME`. The hash is written for observation only and never decides
ownership.

## 9. Text formats

| Value | Format |
| --- | --- |
| GUID | lowercase hyphenated, 36 chars (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) |
| Timestamp written | .NET `"O"`: `yyyy-MM-ddTHH:mm:ss.fffffff+00:00` (7 fractional digits, numeric offset) |
| Timestamp read | any RFC 3339 / ISO-8601 instant, with any number of fractional digits, `Z` or an offset |
| Integers | invariant decimal, no separators |

## 10. Atomic writes
*Source: `Producer/Outbox.cs`, `Producer/StreamStore.cs`, `RedisEvents.EventSourcing/RedisEventRepository.cs`*

- **Outbox:** the caller's state commands, then the topic `XADD`, in one `MULTI`/`EXEC`.
- **Event sourcing:** the aggregate version is the state stream's `XLEN`. A save with expected
  version `v` succeeds only if `XLEN == v`. It then appends every event to the state stream (no
  `MAXLEN`, ever) and publishes each one to the topic, all atomically. The C# side uses `WATCH` +
  `MULTI`. Any mechanism that leaves the same data, all-or-nothing, is compatible.
- Both need the topic's keys co-located in one slot (the default).

## 11. Payload codecs (above the wire)
These do not affect entry decoding, but both sides must agree on them for typed messages.

- **JSON:** property casing MUST match on both sides. System.Text.Json defaults to PascalCase and
  serde to field names as written, so a Rust type for a C# message is normally
  `#[serde(rename_all = "PascalCase")]`. A mismatch is **silent in C#**: System.Text.Json fills
  unmatched members with defaults (`Guid.Empty`, `0`) and raises no error. Pin every
  cross-language message with golden bodies produced by the real C# types, as
  `samples/inventory/inventory-domain/tests/csharp_wire.rs` does.
- **MessagePack:** MessagePack-CSharp `[Key(int)]` types are arrays, and `[Key("name")]` types
  are maps.
- **MessagePack LZ4:** bodies whose uncompressed encoding exceeds **1024** bytes (configurable) are
  written in MessagePack-CSharp `Lz4BlockArray` form: a MessagePack array whose first element is
  extension type **98** containing the uncompressed length of each chunk (as MessagePack ints),
  followed by one `bin` per chunk of raw LZ4 *block* data (not the LZ4 frame format). Readers MUST
  also accept `Lz4Block` (extension type **99**: a MessagePack int32 holding the uncompressed
  length, then one raw LZ4 block). Readers detect compression from the bytes, never from a header.

## 12. Changing this document
A change to anything above is a codec change. It needs a spec PR with updated golden vectors
(`vectors/`) landed in **both** repositories before either side releases. A change that is not
additive bumps `v`.
