# RedisEvents wire format, codec v1

This is the contract shared by the C# library ([bklooste/RedisEvents](https://github.com/bklooste/RedisEvents))
and this Rust port. It defines **what is in Redis**: key names, entry fields, value formats, scripts
and the protocols built on them. Two implementations are compatible exactly when they agree on
everything in this document, so C# and Rust services can share topics and read and write each other's
data. Everything else, including configuration, defaults, APIs and internal design, is up to each
library and documented there.

For the bytes in Redis, the C# code is the reference. Each section names the source file it was taken
from. Where this document and the C# code disagree about what is written to Redis, the code wins and
this document has a bug.

"MUST", "MUST NOT" and "SHOULD" are used as in RFC 2119.

## 1. Key namespace

Every key below starts with a **prefix**: the fixed `re:`, or `STREAMS_KEY_PREFIX` verbatim when it is
set and not blank. Both libraries use exactly this rule and read no environment name
(`ASPNETCORE_ENVIRONMENT`, `DOTNET_ENVIRONMENT`, `STREAMS_ENVIRONMENT`), so C# and Rust agree with
nothing configured. The prefix goes *outside* any `{…}` hash tag, so it never changes a key's cluster
slot.

- Topic and consumer names are opaque, and they are the only isolation. Streams, topic meta and state
  belong to the topic; positions, ownership leases and the work-queue group belong to the consumer.
- Several environments on one Redis are the application's to compose: a shared feed is one topic
  (`np.mult_feed_events`), and each environment's service is its own consumer (`dev.my-service`,
  `tst.my-service`). Two consumer names on one topic never share a key.
- A dedupe scope used by a consumer must include the consumer name; a view owner must be distinct per
  deployment. Neither is derived from an environment.
- `STREAMS_KEY_PREFIX` is for the rare case of unrelated applications sharing one Redis. Services that
  share a topic MUST use the same prefix.

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
| Work-queue consumer group | on the partition stream, group name `<consumer>` (§9) | stream group |

`<p>` is the partition index in invariant decimal, `0`-based. A dedupe `<scope>` MUST NOT contain
`{` or `}`.

## 3. Topic meta
*Source: `Admin/StreamAdmin.cs`*

`m:{topic}` fields, each written with `HSETNX` the first time a topic is used:

| Field | Value |
| --- | --- |
| `partitions` | partition count, decimal |
| `v` | codec version, `1` |
| `createdUtc` | ISO-8601 round-trip (§10) |
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
`<p>:updatedUtc` (§10) and `<p>:instanceId`.

## 8. Ownership
*Source: `Ownership/OwnershipRegistry.cs`*

`o:{topic}:<consumer>`:

| Field | Value | TTL |
| --- | --- | --- |
| `<p>` | owner: `<instanceId>\|<podName>` | `HEXPIRE` lease TTL |
| `i:<instanceId>` | the same owner value (presence) | `HEXPIRE` lease TTL |
| `__hexpire-probe:<instanceId>` | `1`, briefly at startup | deleted after the probe |

- `<podName>` is free text naming the process (for example its pod or host name), possibly empty.
  Readers display it and MUST NOT interpret it.
- Per-field TTLs need **Redis 7.4 or later**. An implementation MUST probe for `HEXPIRE` at startup
  and refuse Lease mode without it.
- Readers MUST skip fields that are not a partition number.

### Lease mode (default)
Every renew interval, each instance runs [`lua/lease.lua`](lua/lease.lua) with
`KEYS[1]` = the ownership key and
`ARGV` = `[ownerValue, ttlSeconds, partitionCount, "i:<instanceId>", quarantined…]`. The optional
trailing `quarantined` arguments are partition numbers (decimal) this instance has stood down
because it could not make progress on them. With any present, the script deletes the instance's
`i:<instanceId>` presence field instead of refreshing it (the instance leaves the fair share, so the
others' cap covers the whole topic), deletes its claims on those partitions, and never claims them;
it still renews its other claims. With no trailing arguments the script behaves exactly as before.
Both implementations send them for a quarantine window after a stall and stop once it ends, at which
point the presence field returns on the next cycle. The script returns the
sorted partitions this instance now holds. On shutdown it runs [`lua/release.lua`](lua/release.lua)
with `ARGV` = `[ownerValue, field…]`, which deletes only fields still holding `ownerValue`.

Implementations MUST send these scripts **byte for byte** (CI enforces this with
`scripts/check_wire_drift.py`). The script alone decides who owns what, so instances of different
languages under one consumer name then share partitions fairly.

Defaults: TTL 30 s, with the renew interval below the TTL.

### Static mode
Each instance is assigned its partitions by its own configuration. It writes the same `<p>` and
`i:<instanceId>` fields for observation only, and the hash never decides ownership.

## 9. Consumer groups (work-queue delivery)
*Source: `Consumer/WorkQueueFetch.cs`, `Consumer/StreamConsumerHost.cs` (`InstanceName`, `GroupAcknowledger`)*

A consumer configured for work-queue delivery (C# `Delivery = WorkQueue`, Rust
`delivery = "work_queue"`) reads with Redis consumer groups instead of positions. C# and Rust
members of one consumer MUST agree on everything below, because they sit in the same group and
share its pending-entries list (PEL).

- **Group.** One group per partition stream (§2), named exactly the consumer name. It is created
  with `XGROUP CREATE <stream> <consumer> <id> MKSTREAM`, where `<id>` is `$` for a consumer that
  starts from now and otherwise the exclusive id its start position resolves to (`0-0` for the
  beginning). `BUSYGROUP` is success, and the existing group's last-delivered id MUST NOT be moved
  by a starting consumer. A read or claim answered with `NOGROUP` re-creates the group the same way.
- **Member.** Each instance reads as group consumer `<member>`: the pod name (`POD_NAME`) when set,
  else the host name (.NET `Environment.MachineName`). It MUST be stable across a restart of that
  instance and unique among live instances. It is free text to other members, which MUST NOT
  interpret it.
- **Reads.** `XREADGROUP GROUP <consumer> <member> [BLOCK <ms>] COUNT <n> STREAMS <stream>… >…`.
  An implementation MAY also re-read its own PEL (`… STREAMS <stream> <id>`); it MUST NOT read
  another member's.
- **Acknowledgement.** `XACK <stream> <consumer> <id>…` for every entry of a batch, sent **after**
  the batch was handled and only where a position-based consumer would advance its position (§7):
  success, an error policy that skips, and entries a type filter dropped. A batch whose handler
  blocks, stops the partition or fails the host is not acknowledged, nor is one interrupted by a
  shutdown or a lost partition. Only whole batches are acknowledged, in fetch order.
- **Claims.** A member takes over another member's entries only with
  `XAUTOCLAIM <stream> <consumer> <member> 30000 <cursor> COUNT 100`: **min-idle-time 30 000 ms**
  (an entry delivered or claimed less than 30 s ago belongs to a live member), the cursor starting
  at `0-0`, carried from sweep to sweep and restarted at `0-0` when Redis returns it. Members MUST
  NOT use a shorter min-idle-time in a shared group (the threshold is the only thing that tells a
  slow sibling from a dead one) and MUST NOT `XCLAIM` with a smaller idle time. Entries a sweep
  returns that were deleted from the stream carry no fields; a member acknowledges them.
- **No other state.** There is no delivery-count limit and nothing is dead-lettered: an
  unacknowledged entry is redelivered until a member acknowledges it. Nothing is written to
  `p:{topic}:<consumer>` and reset markers are not read in this mode. Ownership (§8) still decides
  which partitions an instance reads; members compete on a partition only where several instances
  read it (Static mode, or Lease mode around a hand-over).

## 10. Text formats

| Value | Format |
| --- | --- |
| GUID | lowercase hyphenated, 36 chars (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) |
| Timestamp written | .NET `"O"`: `yyyy-MM-ddTHH:mm:ss.fffffff+00:00` (7 fractional digits, numeric offset) |
| Timestamp read | any RFC 3339 / ISO-8601 instant, with any number of fractional digits, `Z` or an offset |
| Integers | invariant decimal, no separators |

## 11. Atomic writes
*Source: `Producer/Outbox.cs`, `Producer/StreamStore.cs`, `RedisEvents.EventSourcing/RedisEventRepository.cs`*

- **Outbox:** the caller's state commands, then the topic `XADD`, in one `MULTI`/`EXEC`.
- **Event sourcing:** the aggregate version is the state stream's `XLEN`. A save with expected
  version `v` succeeds only if `XLEN == v`. It then appends every event to the state stream (no
  `MAXLEN`, ever) and publishes each one to the topic, all atomically. The C# side uses `WATCH` +
  `MULTI`. Any mechanism that leaves the same data, all-or-nothing, is compatible.
- Both need the topic's keys co-located in one slot (the default).

## 12. Payload codecs (above the wire)
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

## 13. Changing this document
A change to anything above is a codec change. It needs a spec PR with updated golden vectors
(`vectors/`) landed in **both** repositories before either side releases. A change that is not
additive bumps `v`.
