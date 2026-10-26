//! Outbox and state store. **Owner: B3** (port of `Outbox.cs`, `StreamStore.cs`, `IStreamStore.cs`,
//! `StateEvent.cs`, `Idempotency.cs`, `TopicForwarder.cs`).
//!
//! - [`Outbox`]: the caller's state commands plus the topic `XADD`(s) in one `MULTI`/`EXEC`, on a
//!   dedicated connection (`WATCH`/`MULTI` never go on the shared multiplexed connection).
//! - [`StreamStore`]: append-and-publish with optimistic concurrency (version = `XLEN` of
//!   `{topic}:state:<name>`). E3 builds event sourcing on it.
//! - [`Idempotency`]: one `SET NX PX` on `<prefix>dedupe:{scope}:<id>`.
//! - [`TopicForwarder`]: exactly-once-in-effect republishing onto another topic.
//!
//! **Conditional writes use a Lua script, not `WATCH`.** The C# store and forwarder issue
//! `WATCH` + `MULTI`/`EXEC` (SE.Redis `Condition`s). Here the checks and the writes run in one
//! server-side script (`GUARDED_SCRIPT`) on the shared connection, which writes exactly the same
//! entries, so it is wire-compatible with the C# writers and their `WATCH`es (a script is atomic, so
//! a concurrent C# `WATCH` on the key sees the script's write and aborts). Why:
//!
//! - **One round trip, no extra socket.** `WATCH`/`MULTI` has to own a connection for
//!   `WATCH`…`EXEC`, i.e. a dedicated socket per concurrent save (or a pool), plus the `WATCH` round
//!   trip. The script is one `EVALSHA` on the shared, auto-reconnecting connection.
//! - **No lost-`WATCH` retries.** A `WATCH` fails on *any* touch of the key, even a no-op one; the
//!   script compares the actual value (`XLEN == expected`), so it never reports a false conflict.
//! - **Cluster.** Every key is passed in `KEYS` and all share the topic's hash tag (checked before
//!   sending), which is exactly what a cluster requires of a script, as of a transaction.
//! - Neither rolls back: a runtime error inside the script leaves earlier commands applied, the same
//!   as `EXEC`. The argument checks here make that a caller-bug-only path.
//!
//! The plain [`Outbox`] keeps `MULTI`/`EXEC` because its state writes are arbitrary caller
//! commands on a [`redis::Pipeline`] (the agreed API shape, `docs/PLAN.md` §3).

use std::fmt;
use std::sync::LazyLock;
use std::time::Duration;

use bytes::Bytes;
use redis::Value;
use redis::aio::MultiplexedConnection;
use tokio::sync::Mutex;

use super::PublishOptions;
use super::publisher::route;
use crate::config::{TopicOptions, TrimMode};
use crate::connection::Connection;
use crate::diagnostics::activity::{current_traceparent, start_publish};
use crate::diagnostics::instruments::record_published;
use crate::error::{Error, Result};
use crate::wire::codec::{self, CODEC_VERSION, EntryFields};
use crate::wire::{HeaderBlock, Keys, RoundRobin, StreamId, StreamMsg};

// ------------------------------------------------------------------------------------------------
// Outbox
// ------------------------------------------------------------------------------------------------

/// One publish inside an outbox transaction.
#[derive(Debug, Clone, Copy)]
pub struct OutboxPublish<'a> {
    /// The topic. Every publish of one transaction must land in one hash slot.
    pub topic: &'a str,
    /// The routing key; empty round-robins.
    pub partition_key: &'a str,
    /// The body.
    pub body: &'a [u8],
    /// The message type; required.
    pub msg_type: &'a str,
    /// Correlation id, headers, explicit partition, `traceparent`.
    pub options: &'a PublishOptions,
    /// The topic's configured options: a wrong partition count mis-routes the key.
    pub topic_options: &'a TopicOptions,
}

/// Writes state and publishes the events announcing it as one `MULTI`/`EXEC`.
///
/// **`MULTI`/`EXEC` is isolation, not rollback**: a queued command failing at run time (say
/// `WRONGTYPE`) does not stop the others. No other client sees a half-applied state, and a crash
/// cannot separate the state write from the publish; handlers must still be idempotent.
///
/// The transaction runs on one dedicated connection ([`Connection::dedicated_with_timeout`]),
/// opened on first use and reused; calls on one `Outbox` are serialised on it. A failed call drops
/// it and the next call opens a new one.
///
/// **Same slot.** Every key of the transaction must share the stream's hash tag `{topic}`; declared
/// state keys are checked before anything is sent ([`Outbox::ensure_same_slot`]); build them with
/// [`Outbox::state_key`].
pub struct Outbox {
    conn: Connection,
    keys: Keys,
    response_timeout: Duration,
    dedicated: Mutex<Option<MultiplexedConnection>>,
    round_robin: RoundRobin,
}

impl fmt::Debug for Outbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbox")
            .field("conn", &self.conn)
            .field("keys", &self.keys)
            .field("response_timeout", &self.response_timeout)
            .finish_non_exhaustive()
    }
}

impl Outbox {
    /// The response timeout of the dedicated connection unless [`Outbox::with_response_timeout`].
    pub const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

    /// An outbox on `conn`, with key prefix from `keys`.
    #[must_use]
    pub fn new(conn: Connection, keys: Keys) -> Self {
        Self {
            conn,
            keys,
            response_timeout: Self::DEFAULT_RESPONSE_TIMEOUT,
            dedicated: Mutex::new(None),
            round_robin: RoundRobin::new(),
        }
    }

    /// Sets the dedicated connection's response timeout.
    #[must_use]
    pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// A state key sharing `topic`'s slot: `<prefix>{topic}:state:<name>`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank `topic` or `name`; [`Error::Config`] when `topic`
    /// contains a brace.
    pub fn state_key(keys: &Keys, topic: &str, name: &str) -> Result<String> {
        require_non_blank(topic, "topic")?;
        require_non_blank(name, "name")?;
        require_plain_topic(topic)?;
        Ok(keys.state(topic, name))
    }

    /// Checks that every state key hashes to the slot of `topic`'s `partition` stream (C#
    /// `EnsureSameSlot`). Compares hash tags, so it answers the same on a single node as on a cluster.
    ///
    /// # Errors
    /// [`Error::Config`] when a key would land in another slot or the topic contains a brace.
    pub fn ensure_same_slot(
        keys: &Keys,
        topic: &str,
        state_keys: &[&str],
        co_locate_partitions: bool,
        partition: u32,
    ) -> Result<()> {
        require_non_blank(topic, "topic")?;
        require_plain_topic(topic)?;
        require_state_keys_in_slot(
            &keys.stream(topic, partition, co_locate_partitions),
            topic,
            co_locate_partitions,
            state_keys,
        )
    }

    /// Queues `state_writes` and one `XADD` in one `MULTI`/`EXEC`.
    /// `state_writes` adds commands to a pipeline already in atomic mode; it must not run them.
    ///
    /// # Errors
    /// As [`Outbox::write_and_publish_many`].
    pub async fn write_and_publish<F>(
        &self,
        state_writes: F,
        publish: &OutboxPublish<'_>,
        state_keys: &[&str],
    ) -> Result<StreamId>
    where
        F: FnOnce(&mut redis::Pipeline),
    {
        let ids = self
            .write_and_publish_many(state_writes, std::slice::from_ref(publish), state_keys)
            .await?;
        ids.into_iter()
            .next()
            .ok_or_else(|| Error::transport("The outbox transaction returned no entry id."))
    }

    /// Queues `state_writes` and one `XADD` per publish, in order, in one `MULTI`/`EXEC` (C#
    /// `WriteAndPublishManyAsync`). Returns the ids in publish order.
    ///
    /// Cancel-safety: dropping the future after `EXEC` was sent leaves it unknown whether it ran
    /// (it ran completely or not at all).
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for no publishes, a publish with no topic or type, or a partition
    /// out of range; [`Error::Config`] when the keys span slots or the topic options are unusable;
    /// [`Error::Transport`] when Redis fails the transaction.
    pub async fn write_and_publish_many<F>(
        &self,
        state_writes: F,
        publishes: &[OutboxPublish<'_>],
        state_keys: &[&str],
    ) -> Result<Vec<StreamId>>
    where
        F: FnOnce(&mut redis::Pipeline),
    {
        let trace = current_traceparent();
        let plans = plan_publishes(
            &self.keys,
            &self.round_robin,
            publishes,
            state_keys,
            trace.as_deref(),
        )?;
        let first = &plans[0];
        let mut pipe = redis::pipe();
        pipe.atomic();
        state_writes(&mut pipe);
        for (plan, publish) in plans.iter().zip(publishes) {
            let fields = plan.fields(publish);
            pipe.add_command(xadd(&plan.stream, &plan.trim, &fields));
        }

        let count = u64::try_from(plans.len()).unwrap_or(u64::MAX);
        let msg_type = (plans.len() == 1).then_some(publishes[0].msg_type);
        let mut span = start_publish(publishes[0].topic, first.partition, msg_type, count);
        let result = match self.exec(&pipe).await {
            Ok(values) => last_ids(&values, plans.len(), publishes[0].topic, first.partition),
            Err(e) => Err(outbox_failure(
                publishes[0].topic,
                first.partition,
                plans.len(),
                e,
            )),
        };
        match &result {
            Ok(ids) => {
                span.set_message_id(ids[0]);
                for (plan, publish) in plans.iter().zip(publishes) {
                    record_published(publish.topic, plan.partition, 1);
                }
            }
            Err(e) => span.set_error(&e.to_string()),
        }
        result
    }

    async fn exec(&self, pipe: &redis::Pipeline) -> Result<Vec<Value>> {
        let mut guard = self.dedicated.lock().await;
        if guard.is_none() {
            *guard = Some(
                self.conn
                    .dedicated_with_timeout(self.response_timeout)
                    .await?,
            );
        }
        let conn = guard.as_mut().expect("opened above");
        match pipe.query_async::<Vec<Value>>(conn).await {
            Ok(values) => Ok(values),
            Err(e) => {
                // The socket's state is unknown after a failure (a timeout may leave a reply in
                // flight); the next call opens a fresh one.
                *guard = None;
                Err(e.into())
            }
        }
    }
}

fn plan_publishes(
    keys: &Keys,
    round_robin: &RoundRobin,
    publishes: &[OutboxPublish<'_>],
    state_keys: &[&str],
    trace: Option<&str>,
) -> Result<Vec<Planned>> {
    if publishes.is_empty() {
        return Err(Error::invalid_argument(
            "write_and_publish_many was given no publishes. A transaction that only writes state is just a write — issue it on the connection directly.",
        ));
    }
    let mut plans = Vec::with_capacity(publishes.len());
    for (i, p) in publishes.iter().enumerate() {
        if p.topic.trim().is_empty() {
            return Err(Error::invalid_argument(format!(
                "Outbox: publish {i} names no topic."
            )));
        }
        if p.msg_type.is_empty() {
            return Err(Error::invalid_argument(format!(
                "Outbox: publish {i} to topic '{}' names no type; consumers filter on it.",
                p.topic
            )));
        }
        plans.push(Planned::new(
            keys,
            round_robin,
            p.topic,
            p.topic_options,
            p.partition_key,
            p.options,
            trace,
        )?);
    }
    require_stream_keys_in_one_slot(&plans, publishes)?;
    require_state_keys_in_slot(
        &plans[0].stream,
        publishes[0].topic,
        publishes[0].topic_options.co_locate_partitions,
        state_keys,
    )?;
    Ok(plans)
}

#[derive(Debug)]
/// One routed publish: everything an `XADD` needs apart from the body and type.
struct Planned {
    partition: u32,
    stream: String,
    trim: Vec<String>,
    headers: HeaderBlock,
    trace_parent: Option<String>,
}

impl Planned {
    fn new(
        keys: &Keys,
        round_robin: &RoundRobin,
        topic: &str,
        topic_options: &TopicOptions,
        partition_key: &str,
        options: &PublishOptions,
        ambient_trace: Option<&str>,
    ) -> Result<Self> {
        validate_topic(topic, topic_options)?;
        let partition = route(
            topic,
            topic_options.partitions,
            round_robin,
            partition_key,
            options.partition,
        )?;
        Ok(Self {
            partition,
            stream: keys.stream(topic, partition, topic_options.co_locate_partitions),
            trim: codec::max_len_args(topic_options.trim, topic_options.max_len()),
            headers: HeaderBlock::pack(options.headers.as_slice())?,
            trace_parent: options
                .trace_parent
                .clone()
                .or_else(|| ambient_trace.map(str::to_owned)),
        })
    }

    fn fields<'a>(&'a self, p: &OutboxPublish<'a>) -> EntryFields<'a> {
        EntryFields {
            body: p.body,
            msg_type: p.msg_type,
            partition_key: Some(p.partition_key),
            correlation_id: p.options.correlation_id.as_deref(),
            trace_parent: self.trace_parent.as_deref(),
            headers: &self.headers,
        }
    }
}

fn xadd(stream: &str, trim: &[String], fields: &EntryFields<'_>) -> redis::Cmd {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream);
    for a in trim {
        cmd.arg(a);
    }
    cmd.arg("*");
    fields.write_to(&mut cmd);
    cmd
}

/// The ids of the last `n` replies (the `XADD`s queued after the caller's state writes).
fn last_ids(values: &[Value], n: usize, topic: &str, partition: u32) -> Result<Vec<StreamId>> {
    if values.len() < n {
        return Err(Error::transport(format!(
            "The outbox transaction on topic '{topic}' partition {partition} committed, but returned {} replies for {n} XADD(s). The state writes in that transaction did apply.",
            values.len()
        )));
    }
    values[values.len() - n..]
        .iter()
        .map(|v| {
            id_of(v).ok_or_else(|| {
                Error::transport(format!(
                    "The outbox transaction on topic '{topic}' partition {partition} committed, but XADD returned no entry id. The state writes in that transaction did apply."
                ))
            })?
        })
        .collect()
}

fn id_of(value: &Value) -> Option<Result<StreamId>> {
    let text = match value {
        Value::BulkString(b) => std::str::from_utf8(b).ok()?,
        Value::SimpleString(s) => s.as_str(),
        _ => return None,
    };
    if text.is_empty() {
        return None;
    }
    Some(StreamId::parse(text).map_err(|e| {
        Error::transport_with(format!("XADD returned an unparseable entry id '{text}'"), e)
    }))
}

fn outbox_failure(topic: &str, partition: u32, count: usize, source: Error) -> Error {
    Error::transport_with(
        format!(
            "The outbox transaction failed: {count} message(s) to topic '{topic}' partition {partition}, plus its state writes. MULTI/EXEC hands the whole batch to the server as one unit, so it either never ran or ran completely; retrying the call is safe as long as the handler is idempotent. The outbox does not retry for you — a silent retry would hide the outage."
        ),
        source,
    )
}

// ------------------------------------------------------------------------------------------------
// Slot and option checks
// ------------------------------------------------------------------------------------------------

fn require_non_blank(value: &str, what: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::invalid_argument(format!(
            "'{what}' must not be empty or whitespace."
        )));
    }
    Ok(())
}

fn require_plain_topic(topic: &str) -> Result<()> {
    if topic.contains(['{', '}']) {
        return Err(Error::config(format!(
            "Outbox: topic '{topic}' contains a brace. Stream keys are hash-tagged as s:{{topic}}:<p>, so a brace in the name yields a hash tag nobody intended and silently splits the topic across cluster slots. Rename the topic."
        )));
    }
    Ok(())
}

fn validate_topic(topic: &str, options: &TopicOptions) -> Result<()> {
    require_plain_topic(topic)?;
    if options.partitions < 1 {
        return Err(Error::config(format!(
            "Streams:Topics:{topic}:Partitions is {}; it must be >= 1.",
            options.partitions
        )));
    }
    if options.trim != TrimMode::None && options.max_len() < 1 {
        return Err(Error::config(format!(
            "Streams:Topics:{topic}:MaxLen is {} with Trim={:?}; XADD MAXLEN 0 would empty the stream on every publish. Set MaxLen >= 1, or Trim=None.",
            options.max_len(),
            options.trim
        )));
    }
    Ok(())
}

/// The effective hash tag of a key (Redis's rule): the text between the first `{` and the first
/// `}` after it, or the whole key when there is no non-empty tag.
fn hash_tag(key: &str) -> &str {
    let Some(open) = key.find('{') else {
        return key;
    };
    let rest = &key[open + 1..];
    match rest.find('}') {
        Some(close) if close > 0 => &rest[..close],
        _ => key,
    }
}

fn require_stream_keys_in_one_slot(
    plans: &[Planned],
    publishes: &[OutboxPublish<'_>],
) -> Result<()> {
    let reference = &plans[0].stream;
    let tag = hash_tag(reference);
    for (i, plan) in plans.iter().enumerate().skip(1) {
        if hash_tag(&plan.stream) == tag {
            continue;
        }
        let same_topic = publishes[0].topic == publishes[i].topic;
        let reason = if same_topic {
            format!(
                "Topic '{}' has CoLocatePartitions=false, so its partitions are deliberately spread across slots: publish one partition per transaction, or co-locate the topic's partitions.",
                publishes[i].topic
            )
        } else {
            "Stream keys are hash-tagged per topic, so a transaction cannot span topics: publish each topic in its own transaction, or publish one event and let a consumer fan out.".to_owned()
        };
        return Err(Error::config(format!(
            "Outbox: publish 0 ('{}', key '{reference}') and publish {i} ('{}', key '{}') hash to different Redis Cluster slots, and one MULTI/EXEC covers one slot. {reason}",
            publishes[0].topic, publishes[i].topic, plan.stream
        )));
    }
    Ok(())
}

fn require_state_keys_in_slot(
    stream_key: &str,
    topic: &str,
    co_locate_partitions: bool,
    state_keys: &[&str],
) -> Result<()> {
    let tag = hash_tag(stream_key);
    for (i, key) in state_keys.iter().enumerate() {
        if key.is_empty() {
            return Err(Error::config(format!(
                "Outbox: state key {i} is empty. Declare the keys the state-write delegate actually touches, or declare none at all."
            )));
        }
        if hash_tag(key) == tag {
            continue;
        }
        let spread = if co_locate_partitions {
            String::new()
        } else {
            format!(
                "Note that topic '{topic}' has CoLocatePartitions=false, so its partition streams are spread across slots and no state key can join them. "
            )
        };
        return Err(Error::config(format!(
            "Outbox: state key '{key}' (hash tag '{}') is not in the same Redis Cluster slot as topic '{topic}' (stream key '{stream_key}', hash tag '{tag}'), and every key in one MULTI/EXEC must hash to one slot. Tag the state key on the topic — Outbox::state_key(keys, \"{topic}\", \"…\") gives '{{{topic}}}:state:…'. {spread}Where the state genuinely cannot share a slot, drop the transaction: write the state, then publish, and make the consumer idempotent.",
            hash_tag(key)
        )));
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// The guarded script (state store, forwarder)
// ------------------------------------------------------------------------------------------------

/// Checks conditions, then runs commands, atomically. `ARGV` is `ncond, cond…, ncmd, cmd…`, each
/// item `name, key_index, argc, arg…`. Conditions: `xlen <n>` (`XLEN == n`), `hnil <field>`,
/// `heq <field> <value>`, `nokey`. Returns `false` (nil) when one fails, having written nothing,
/// else the array of command replies.
pub(crate) const GUARDED_SCRIPT: &str = r"
local i = 1
local function item()
  local name = ARGV[i]
  local key = KEYS[tonumber(ARGV[i + 1])]
  local n = tonumber(ARGV[i + 2])
  local args = {}
  for j = 1, n do args[j] = ARGV[i + 2 + j] end
  i = i + 3 + n
  return name, key, args
end
local nconds = tonumber(ARGV[i]); i = i + 1
for _ = 1, nconds do
  local name, key, a = item()
  if name == 'xlen' then
    if redis.call('XLEN', key) ~= tonumber(a[1]) then return false end
  elseif name == 'hnil' then
    if redis.call('HEXISTS', key, a[1]) == 1 then return false end
  elseif name == 'heq' then
    if redis.call('HGET', key, a[1]) ~= a[2] then return false end
  elseif name == 'nokey' then
    if redis.call('EXISTS', key) == 1 then return false end
  else
    return redis.error_reply('redisevents: unknown condition ' .. tostring(name))
  end
end
local ncmds = tonumber(ARGV[i]); i = i + 1
local out = {}
for c = 1, ncmds do
  local name, key, a = item()
  out[c] = redis.call(name, key, unpack(a))
end
return out
";

static GUARDED: LazyLock<redis::Script> = LazyLock::new(|| redis::Script::new(GUARDED_SCRIPT));

/// Builds one invocation of [`GUARDED_SCRIPT`] as a single `EVALSHA`, writing each argument
/// (bodies included) once.
struct Guarded {
    keys: Vec<String>,
    conds: Vec<(&'static str, usize, Vec<Vec<u8>>)>,
    cmds: Vec<GuardedCmd>,
}

enum GuardedCmd {
    Plain(&'static str, usize, Vec<Vec<u8>>),
    Xadd {
        key: usize,
        trim: Vec<String>,
        fields: OwnedFields,
    },
}

/// The fields of one `XADD`, owned so a [`Guarded`] can hold them until it is sent.
struct OwnedFields {
    body: Bytes,
    msg_type: String,
    partition_key: Option<String>,
    correlation_id: Option<String>,
    trace_parent: Option<String>,
    headers: HeaderBlock,
}

impl OwnedFields {
    fn as_fields(&self) -> EntryFields<'_> {
        EntryFields {
            body: &self.body,
            msg_type: &self.msg_type,
            partition_key: self.partition_key.as_deref(),
            correlation_id: self.correlation_id.as_deref(),
            trace_parent: self.trace_parent.as_deref(),
            headers: &self.headers,
        }
    }
}

impl Guarded {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            conds: Vec::new(),
            cmds: Vec::new(),
        }
    }

    /// The 1-based `KEYS` index of `key`, adding it once.
    fn key(&mut self, key: &str) -> usize {
        if let Some(i) = self.keys.iter().position(|k| k == key) {
            return i + 1;
        }
        self.keys.push(key.to_owned());
        self.keys.len()
    }

    fn cond(&mut self, name: &'static str, key: &str, args: Vec<Vec<u8>>) {
        let k = self.key(key);
        self.conds.push((name, k, args));
    }

    fn cmd(&mut self, name: &'static str, key: &str, args: Vec<Vec<u8>>) {
        let k = self.key(key);
        self.cmds.push(GuardedCmd::Plain(name, k, args));
    }

    fn xadd(&mut self, key: &str, trim: Vec<String>, fields: OwnedFields) {
        let k = self.key(key);
        self.cmds.push(GuardedCmd::Xadd {
            key: k,
            trim,
            fields,
        });
    }

    fn to_cmd(&self, verb: &str, script: &str) -> redis::Cmd {
        let mut cmd = redis::cmd(verb);
        cmd.arg(script).arg(self.keys.len());
        for k in &self.keys {
            cmd.arg(k);
        }
        cmd.arg(self.conds.len());
        for (name, k, args) in &self.conds {
            cmd.arg(*name).arg(*k).arg(args.len());
            for a in args {
                cmd.arg(a.as_slice());
            }
        }
        cmd.arg(self.cmds.len());
        for c in &self.cmds {
            match c {
                GuardedCmd::Plain(name, k, args) => {
                    cmd.arg(*name).arg(*k).arg(args.len());
                    for a in args {
                        cmd.arg(a.as_slice());
                    }
                }
                GuardedCmd::Xadd { key, trim, fields } => {
                    let fields = fields.as_fields();
                    cmd.arg("XADD")
                        .arg(*key)
                        .arg(trim.len() + 1 + fields.arg_count());
                    for a in trim {
                        cmd.arg(a);
                    }
                    cmd.arg("*");
                    fields.write_to(&mut cmd);
                }
            }
        }
        cmd
    }

    /// Runs the script: `None` when a condition failed (nothing written), else the replies.
    async fn run(&self, conn: &Connection) -> Result<Option<Vec<Value>>> {
        let mut shared = conn.shared();
        let hash = GUARDED.get_hash();
        let reply: redis::RedisResult<Value> =
            self.to_cmd("EVALSHA", hash).query_async(&mut shared).await;
        let reply = match reply {
            Err(e) if e.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) => {
                self.to_cmd("EVAL", GUARDED_SCRIPT)
                    .query_async(&mut shared)
                    .await?
            }
            other => other?,
        };
        match reply {
            Value::Nil | Value::Boolean(false) => Ok(None),
            Value::Array(values) => Ok(Some(values)),
            other => Err(Error::transport(format!(
                "The state-store script returned an unexpected reply: {other:?}"
            ))),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// State store
// ------------------------------------------------------------------------------------------------

/// One event to append to a state stream and publish to its topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateEvent {
    /// The body.
    pub body: Bytes,
    /// The type string; required.
    pub msg_type: String,
    /// Correlation id, headers, `traceparent`; an explicit partition is honoured for the topic
    /// publish.
    pub options: PublishOptions,
}

impl StateEvent {
    /// An event with no options.
    pub fn new(body: impl Into<Bytes>, msg_type: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            msg_type: msg_type.into(),
            options: PublishOptions::default(),
        }
    }

    /// Sets the options.
    #[must_use]
    pub fn with_options(mut self, options: PublishOptions) -> Self {
        self.options = options;
        self
    }
}

/// A state stream per name, written atomically with the topic publish announcing it (C#
/// `StreamStore` / `IStreamStore`). The key is `<prefix>{topic}:state:<name>`.
///
/// **Version is length**: `XLEN` of the state stream. A missing stream has length 0, so creating
/// one is `expected_len = 0`. Never trim a state stream; nothing here does (no `MAXLEN`, ever).
/// State entries are `b`, `t` (plus `c`/`p`/`h` when [`TopicOptions::state_metadata`]), never `k`.
///
/// Like C#, construction does not ensure the topic meta `m:{topic}`; a writer-only service should
/// ensure it at startup.
#[derive(Debug)]
pub struct StreamStore {
    conn: Connection,
    keys: Keys,
    topic: String,
    options: TopicOptions,
    round_robin: RoundRobin,
}

impl StreamStore {
    /// A store bound to `topic`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic; [`Error::Config`] when the topic has fewer
    /// than one partition, a brace in its name, or partitions that are not co-located.
    pub fn new(
        conn: Connection,
        keys: Keys,
        topic: impl Into<String>,
        options: TopicOptions,
    ) -> Result<Self> {
        let topic = topic.into();
        require_non_blank(&topic, "topic")?;
        validate_topic(&topic, &options)?;
        Self::require_co_located_partitions(&topic, &options)?;
        Ok(Self {
            conn,
            keys,
            topic,
            options,
            round_robin: RoundRobin::new(),
        })
    }

    /// Refuses a topic whose partitions are spread across cluster slots (C#
    /// `RequireCoLocatedPartitions`): no state key can share a transaction with them.
    ///
    /// # Errors
    /// [`Error::Config`] when `options.co_locate_partitions` is false.
    pub fn require_co_located_partitions(topic: &str, options: &TopicOptions) -> Result<()> {
        if options.co_locate_partitions {
            return Ok(());
        }
        Err(Error::config(format!(
            "Streams:Topics:{topic}:CoLocatePartitions is false, so the topic's partition streams are spread across Redis Cluster slots and no state key can share one with them — but a state store's whole guarantee is that the append to '{{{topic}}}:state:<name>' and the publish to 's:{topic}:<p>' are one MULTI/EXEC. Co-locate the topic's partitions, or use a topic that does."
        )))
    }

    /// The topic every append publishes to.
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The topic's options.
    #[must_use]
    pub fn options(&self) -> &TopicOptions {
        &self.options
    }

    /// The state stream key for `name`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank name.
    pub fn state_key(&self, name: &str) -> Result<String> {
        Outbox::state_key(&self.keys, &self.topic, name)
    }

    /// Reads up to `max` entries of `name` with an id strictly after `after`, oldest first. A
    /// missing stream reads as empty. Entries have an empty partition key and partition 0.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank name or `max == 0`; [`Error::Redis`] when Redis fails.
    pub async fn read(&self, name: &str, after: StreamId, max: usize) -> Result<Vec<StreamMsg>> {
        let key = self.state_key(name)?;
        if max == 0 {
            return Err(Error::invalid_argument("'max' must be positive."));
        }
        let mut conn = self.conn.shared();
        let reply: Value = redis::cmd("XRANGE")
            .arg(&key)
            .arg(format!("({after}"))
            .arg("+")
            .arg("COUNT")
            .arg(max)
            .query_async(&mut conn)
            .await?;
        decode_range(reply)
    }

    /// Appends `events` to `name` and publishes them to the topic, both in order, atomically, if
    /// and only if the state stream's length is still `expected_len`.
    ///
    /// Returns the **state-stream** entry ids, or `None` when the length check failed, in which
    /// case nothing was written (neither stream).
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank name, no events, an event with no type or a partition
    /// out of range; [`Error::Redis`] / [`Error::Transport`] when Redis fails.
    pub async fn append_and_publish(
        &self,
        name: &str,
        expected_len: u64,
        partition_key: &str,
        events: &[StateEvent],
    ) -> Result<Option<Vec<StreamId>>> {
        self.append(name, Some(expected_len), partition_key, events)
            .await
    }

    /// [`StreamStore::append_and_publish`] with no version check: concurrent callers can interleave
    /// silently. For single-writer aggregates.
    ///
    /// # Errors
    /// As [`StreamStore::append_and_publish`].
    pub async fn append_and_publish_unchecked(
        &self,
        name: &str,
        partition_key: &str,
        events: &[StateEvent],
    ) -> Result<Vec<StreamId>> {
        self.append(name, None, partition_key, events)
            .await?
            .ok_or_else(|| Error::transport("An unconditional state append reported a conflict."))
    }

    /// The script for one append: the state half (lean entries, no `MAXLEN`), then the topic half.
    /// Returns it with each topic publish's partition.
    fn build_append(
        &self,
        name: &str,
        expected_len: Option<u64>,
        partition_key: &str,
        events: &[StateEvent],
    ) -> Result<(Guarded, Vec<u32>)> {
        let key = self.state_key(name)?;
        if events.is_empty() {
            return Err(Error::invalid_argument(format!(
                "append_and_publish was given no events for '{name}' on topic '{}'. An append of nothing is not an append: there is no state change to record and nothing to announce.",
                self.topic
            )));
        }
        let trace = current_traceparent();
        let mut script = Guarded::new();
        script.key(&key);
        if let Some(expected) = expected_len {
            script.cond("xlen", &key, vec![expected.to_string().into_bytes()]);
        }
        let mut topic_half = Vec::with_capacity(events.len());
        for (i, e) in events.iter().enumerate() {
            if e.msg_type.is_empty() {
                return Err(Error::invalid_argument(format!(
                    "append_and_publish: event {i} for '{name}' on topic '{}' names no type; consumers filter on it.",
                    self.topic
                )));
            }
            let plan = Planned::new(
                &self.keys,
                &self.round_robin,
                &self.topic,
                &self.options,
                partition_key,
                &e.options,
                trace.as_deref(),
            )?;
            let meta = self.options.state_metadata;
            script.xadd(
                &key,
                Vec::new(), // no MAXLEN on a state stream, ever: its length is its version
                OwnedFields {
                    body: e.body.clone(),
                    msg_type: e.msg_type.clone(),
                    partition_key: None,
                    correlation_id: e.options.correlation_id.clone().filter(|_| meta),
                    trace_parent: plan.trace_parent.clone().filter(|_| meta),
                    headers: if meta {
                        plan.headers.clone()
                    } else {
                        HeaderBlock::empty()
                    },
                },
            );
            topic_half.push((plan, e));
        }
        for (plan, e) in &topic_half {
            script.xadd(
                &plan.stream,
                plan.trim.clone(),
                OwnedFields {
                    body: e.body.clone(),
                    msg_type: e.msg_type.clone(),
                    partition_key: Some(partition_key.to_owned()),
                    correlation_id: e.options.correlation_id.clone(),
                    trace_parent: plan.trace_parent.clone(),
                    headers: plan.headers.clone(),
                },
            );
        }
        let partitions = topic_half.iter().map(|(plan, _)| plan.partition).collect();
        Ok((script, partitions))
    }

    async fn append(
        &self,
        name: &str,
        expected_len: Option<u64>,
        partition_key: &str,
        events: &[StateEvent],
    ) -> Result<Option<Vec<StreamId>>> {
        let (script, partitions) = self.build_append(name, expected_len, partition_key, events)?;
        let first_partition = partitions[0];
        let count = u64::try_from(events.len()).unwrap_or(u64::MAX);
        let msg_type = (events.len() == 1).then_some(events[0].msg_type.as_str());
        let mut span = start_publish(&self.topic, first_partition, msg_type, count);
        let replies = match script.run(&self.conn).await {
            Ok(r) => r,
            Err(e) => {
                let e = Error::transport_with(
                    format!(
                        "The state append for '{name}' on topic '{}' failed. The script either never ran or ran completely; retrying with the same expected length tells which.",
                        self.topic
                    ),
                    e,
                );
                span.set_error(&e.to_string());
                return Err(e);
            }
        };
        let Some(replies) = replies else {
            return Ok(None);
        };
        if replies.len() != events.len() * 2 {
            return Err(Error::transport(format!(
                "The state append for '{name}' committed, but returned {} replies for {} XADD(s).",
                replies.len(),
                events.len() * 2
            )));
        }
        let mut ids = Vec::with_capacity(events.len());
        for (i, v) in replies[..events.len()].iter().enumerate() {
            ids.push(id_of(v).ok_or_else(|| {
                Error::transport(format!(
                    "The state append for '{name}' committed, but XADD {i} returned no entry id. The transaction did apply: the events are on the state stream and on the topic, and only their ids are lost."
                ))
            })??);
        }
        if let Some(Some(Ok(id))) = replies.get(events.len()).map(id_of) {
            span.set_message_id(id);
        }
        for partition in partitions {
            record_published(&self.topic, partition, 1);
        }
        Ok(Some(ids))
    }
}

/// Decodes an `XRANGE` reply without copying bodies.
fn decode_range(reply: Value) -> Result<Vec<StreamMsg>> {
    let malformed = || Error::transport("XRANGE returned a malformed reply.");
    let Value::Array(entries) = reply else {
        return match reply {
            Value::Nil => Ok(Vec::new()),
            _ => Err(malformed()),
        };
    };
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let Value::Array(mut parts) = entry else {
            return Err(malformed());
        };
        if parts.len() != 2 {
            return Err(malformed());
        }
        let fields = parts.pop().ok_or_else(malformed)?;
        let id = match parts.pop() {
            Some(Value::BulkString(b)) => StreamId::parse_bytes(&b)
                .map_err(|e| Error::transport_with("XRANGE returned an unparseable entry id", e))?,
            Some(Value::SimpleString(s)) => StreamId::parse(&s)
                .map_err(|e| Error::transport_with("XRANGE returned an unparseable entry id", e))?,
            _ => return Err(malformed()),
        };
        let Value::Array(flat) = fields else {
            return Err(malformed());
        };
        let mut pairs = Vec::with_capacity(flat.len() / 2);
        let mut it = flat.into_iter();
        while let (Some(n), Some(v)) = (it.next(), it.next()) {
            pairs.push((
                bytes_of(n).ok_or_else(malformed)?,
                bytes_of(v).ok_or_else(malformed)?,
            ));
        }
        out.push(codec::decode(id, 0, pairs, CODEC_VERSION)?);
    }
    Ok(out)
}

fn bytes_of(value: Value) -> Option<Bytes> {
    match value {
        Value::BulkString(b) => Some(Bytes::from(b)),
        Value::SimpleString(s) => Some(Bytes::from(s)),
        _ => None,
    }
}

// ------------------------------------------------------------------------------------------------
// Idempotency
// ------------------------------------------------------------------------------------------------

/// Dedupe markers for at-least-once handlers. A marker, not a lock: it never
/// clears on failure.
#[derive(Debug, Clone, Copy)]
pub struct Idempotency;

impl Idempotency {
    /// `<prefix>dedupe:{scope}:<id>`, hash-tagged on the scope.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank scope or id; [`Error::Config`] when the scope
    /// contains a brace.
    pub fn key(keys: &Keys, scope: &str, id: &str) -> Result<String> {
        require_non_blank(scope, "scope")?;
        require_non_blank(id, "id")?;
        if scope.contains(['{', '}']) {
            return Err(Error::config(format!(
                "Idempotency: scope '{scope}' contains a brace. Dedupe keys are hash-tagged as dedupe:{{scope}}:<id>, so a brace in the scope yields a hash tag nobody intended and scatters the scope's markers across cluster slots. Rename the scope."
            )));
        }
        keys.dedupe(scope, id)
    }

    /// Marks `id` seen in `scope` for `ttl`: `SET <key> 1 NX PX <ttl_ms>`, one round trip. `true`
    /// on the first sighting, `false` if already marked.
    ///
    /// # Errors
    /// As [`Idempotency::key`]; [`Error::InvalidArgument`] when `ttl` is under one millisecond;
    /// [`Error::Redis`] when Redis fails.
    pub async fn try_begin(
        conn: &Connection,
        keys: &Keys,
        scope: &str,
        id: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let cmd = Self::try_begin_cmd(keys, scope, id, ttl)?;
        let mut shared = conn.shared();
        let reply: Option<String> = cmd.query_async(&mut shared).await?;
        Ok(reply.is_some())
    }

    fn try_begin_cmd(keys: &Keys, scope: &str, id: &str, ttl: Duration) -> Result<redis::Cmd> {
        let key = Self::key(keys, scope, id)?;
        let ms = ttl.as_millis();
        if ms < 1 {
            return Err(Error::invalid_argument(
                "Idempotency: the dedupe marker's TTL must be at least one millisecond — Redis rejects PX 0, and a marker with no expiry would grow the keyspace without bound. Pass the replay window, e.g. Duration::from_secs(24 * 3600).",
            ));
        }
        let mut cmd = redis::cmd("SET");
        cmd.arg(key)
            .arg("1")
            .arg("NX")
            .arg("PX")
            .arg(u64::try_from(ms).unwrap_or(u64::MAX));
        Ok(cmd)
    }
}

// ------------------------------------------------------------------------------------------------
// Topic forwarder
// ------------------------------------------------------------------------------------------------

/// One message a [`TopicForwarder`] publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardedMessage {
    /// The routing key on the target topic.
    pub partition_key: String,
    /// The body.
    pub body: Bytes,
    /// The type string.
    pub msg_type: String,
}

impl ForwardedMessage {
    /// A message.
    pub fn new(
        partition_key: impl Into<String>,
        body: impl Into<Bytes>,
        msg_type: impl Into<String>,
    ) -> Self {
        Self {
            partition_key: partition_key.into(),
            body: body.into(),
            msg_type: msg_type.into(),
        }
    }
}

/// Republishes events read from one topic onto another, exactly once in effect (C#
/// `TopicForwarder`). Guards written atomically with the publish, both tagged on the target topic:
/// a high-water mark per source (`{target}:state:<name>:high-water`, field = source, value = the
/// last forwarded position; compared as stream ids) and a dedupe marker per dedupe id.
///
/// A source must be delivered in order: one partition (`"{topic}:{partition}"`) or a one-partition
/// topic.
#[derive(Debug)]
pub struct TopicForwarder {
    conn: Connection,
    keys: Keys,
    target: String,
    options: TopicOptions,
    high_water_key: String,
    marker_ttl: Duration,
    round_robin: RoundRobin,
}

impl TopicForwarder {
    /// How long a dedupe marker lives unless [`TopicForwarder::with_marker_ttl`]: a week.
    pub const DEFAULT_MARKER_TTL: Duration = Duration::from_hours(7 * 24);

    /// A forwarder onto `target_topic` with high-water name `"forward"`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank topic; [`Error::Config`] for a brace in it.
    pub fn new(
        conn: Connection,
        keys: Keys,
        target_topic: impl Into<String>,
        options: TopicOptions,
    ) -> Result<Self> {
        let target = target_topic.into();
        let high_water_key = Outbox::state_key(&keys, &target, "forward:high-water")?;
        Ok(Self {
            conn,
            keys,
            target,
            options,
            high_water_key,
            marker_ttl: Self::DEFAULT_MARKER_TTL,
            round_robin: RoundRobin::new(),
        })
    }

    /// Names the high-water state `{target}:state:<name>:high-water`. Forwarders sharing a name
    /// share marks.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank name.
    pub fn with_name(mut self, name: &str) -> Result<Self> {
        require_non_blank(name, "name")?;
        self.high_water_key =
            Outbox::state_key(&self.keys, &self.target, &format!("{name}:high-water"))?;
        Ok(self)
    }

    /// Sets how long a dedupe marker lives.
    #[must_use]
    pub fn with_marker_ttl(mut self, ttl: Duration) -> Self {
        self.marker_ttl = ttl;
        self
    }

    /// The target topic.
    #[must_use]
    pub fn target_topic(&self) -> &str {
        &self.target
    }

    /// How long a dedupe marker lives.
    #[must_use]
    pub fn marker_ttl(&self) -> Duration {
        self.marker_ttl
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "private; the pieces of one forward"
    )]
    fn build_forward(
        &self,
        source: &str,
        stored: Option<&str>,
        marker: &str,
        position: StreamId,
        plans: &[Planned],
        messages: &[ForwardedMessage],
        correlation_id: Option<&str>,
    ) -> Guarded {
        let mut script = Guarded::new();
        match stored {
            None => script.cond(
                "hnil",
                &self.high_water_key,
                vec![source.as_bytes().to_vec()],
            ),
            Some(s) => script.cond(
                "heq",
                &self.high_water_key,
                vec![source.as_bytes().to_vec(), s.as_bytes().to_vec()],
            ),
        }
        script.cond("nokey", marker, Vec::new());
        let ttl_ms = u64::try_from(self.marker_ttl.as_millis().max(1)).unwrap_or(u64::MAX);
        script.cmd(
            "SET",
            marker,
            vec![Vec::new(), b"PX".to_vec(), ttl_ms.to_string().into_bytes()],
        );
        script.cmd(
            "HSET",
            &self.high_water_key,
            vec![
                source.as_bytes().to_vec(),
                position.to_string().into_bytes(),
            ],
        );
        for (plan, m) in plans.iter().zip(messages) {
            script.xadd(
                &plan.stream,
                plan.trim.clone(),
                OwnedFields {
                    body: m.body.clone(),
                    msg_type: m.msg_type.clone(),
                    partition_key: Some(m.partition_key.clone()),
                    correlation_id: correlation_id.map(str::to_owned),
                    trace_parent: plan.trace_parent.clone(),
                    headers: HeaderBlock::empty(),
                },
            );
        }
        script
    }

    /// Publishes `messages` onto the target unless `source` already forwarded `position` or later,
    /// or `dedupe_id` is still marked. `true` when published; `false` when skipped or a concurrent
    /// forward from the same source moved the mark first (nothing written either way).
    ///
    /// # Errors
    /// [`Error::InvalidArgument`] for a blank source or dedupe id or no messages; [`Error::Config`]
    /// when the target's partitions are not co-located; [`Error::Redis`] when Redis fails.
    pub async fn forward(
        &self,
        source: &str,
        dedupe_id: &str,
        position: StreamId,
        messages: &[ForwardedMessage],
        correlation_id: Option<&str>,
    ) -> Result<bool> {
        require_non_blank(source, "source")?;
        require_non_blank(dedupe_id, "dedupe_id")?;
        if messages.is_empty() {
            return Err(Error::invalid_argument(
                "A forward needs at least one message.",
            ));
        }

        let mut shared = self.conn.shared();
        let stored: Option<String> = redis::cmd("HGET")
            .arg(&self.high_water_key)
            .arg(source)
            .query_async(&mut shared)
            .await?;
        if let Some(Ok(high_water)) = stored.as_deref().map(StreamId::parse)
            && high_water >= position
        {
            return Ok(false);
        }

        let marker = Idempotency::key(&self.keys, &self.target, dedupe_id)?;
        let trace = current_traceparent();
        let options = PublishOptions {
            correlation_id: correlation_id.map(str::to_owned),
            ..PublishOptions::default()
        };
        let views: Vec<OutboxPublish<'_>> = messages
            .iter()
            .map(|m| OutboxPublish {
                topic: &self.target,
                partition_key: &m.partition_key,
                body: &m.body,
                msg_type: &m.msg_type,
                options: &options,
                topic_options: &self.options,
            })
            .collect();
        let plans = plan_publishes(
            &self.keys,
            &self.round_robin,
            &views,
            &[&marker, &self.high_water_key],
            trace.as_deref(),
        )?;

        let script = self.build_forward(
            source,
            stored.as_deref(),
            &marker,
            position,
            &plans,
            messages,
            options.correlation_id.as_deref(),
        );
        let count = u64::try_from(messages.len()).unwrap_or(u64::MAX);
        let mut span = start_publish(&self.target, plans[0].partition, None, count);
        match script.run(&self.conn).await {
            Ok(Some(_)) => {
                for plan in &plans {
                    record_published(&self.target, plan.partition, 1);
                }
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(e) => {
                span.set_error(&e.to_string());
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "outbox-tests";

    fn keys() -> Keys {
        Keys::with_prefix("dev:re:")
    }

    fn opts(partitions: u32, co_locate: bool) -> TopicOptions {
        TopicOptions {
            partitions,
            co_locate_partitions: co_locate,
            ..TopicOptions::default()
        }
    }

    fn plan(topic: &str, o: &TopicOptions, key: &str, p: &PublishOptions) -> Result<Planned> {
        Planned::new(&keys(), &RoundRobin::new(), topic, o, key, p, None)
    }

    fn publish<'a>(
        topic: &'a str,
        o: &'a TopicOptions,
        p: &'a PublishOptions,
        t: &'a str,
    ) -> OutboxPublish<'a> {
        OutboxPublish {
            topic,
            partition_key: "k",
            body: b"a",
            msg_type: t,
            options: p,
            topic_options: o,
        }
    }

    /// Port of the routing half of `WriteAndPublishManyAsync_routes_each_publish_and_uses_the_tagged_stream_keys`.
    #[test]
    fn write_and_publish_many_routes_each_publish_and_uses_the_tagged_stream_keys() {
        let o = opts(4, true);
        let explicit = plan(TOPIC, &o, "customer-1", &PublishOptions::new().partition(2)).unwrap();
        assert_eq!(explicit.stream, format!("dev:re:s:{{{TOPIC}}}:2"));
        let routed = crate::wire::partition_for_key(b"customer-1", 4);
        let hashed = plan(TOPIC, &o, "customer-1", &PublishOptions::new()).unwrap();
        assert_eq!(hashed.stream, format!("dev:re:s:{{{TOPIC}}}:{routed}"));
    }

    #[test]
    fn a_publish_naming_a_partition_outside_the_topic_is_rejected() {
        let err = plan(
            TOPIC,
            &opts(4, true),
            "k",
            &PublishOptions::new().partition(9),
        );
        assert!(matches!(err, Err(Error::InvalidArgument(_))));
    }

    fn outbox_plan(publishes: &[OutboxPublish<'_>], state_keys: &[&str]) -> Result<Vec<Planned>> {
        plan_publishes(&keys(), &RoundRobin::new(), publishes, state_keys, None)
    }

    #[test]
    fn an_empty_publish_list_is_refused_with_an_explanation() {
        let err = outbox_plan(&[], &[]).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(ref m) if m.contains("only writes state")));
    }

    #[test]
    fn publishes_to_two_topics_are_refused_before_anything_is_queued() {
        let o = opts(4, true);
        let p = PublishOptions::new();
        let err = outbox_plan(
            &[
                publish(TOPIC, &o, &p, "T"),
                publish("another-topic", &o, &p, "T"),
            ],
            &[],
        )
        .unwrap_err();
        let Error::Config(m) = err else {
            panic!("{err:?}")
        };
        assert!(
            m.contains(TOPIC) && m.contains("another-topic") && m.contains("cannot span topics"),
            "{m}"
        );
    }

    #[test]
    fn two_partitions_of_a_non_co_located_topic_are_refused_with_that_named_as_the_reason() {
        let o = opts(4, false);
        let (p0, p1) = (
            PublishOptions::new().partition(0),
            PublishOptions::new().partition(1),
        );
        let err = outbox_plan(
            &[publish(TOPIC, &o, &p0, "T"), publish(TOPIC, &o, &p1, "T")],
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, Error::Config(ref m) if m.contains("CoLocatePartitions=false")));
    }

    #[test]
    fn two_publishes_to_one_partition_of_a_non_co_located_topic_are_accepted() {
        let o = opts(4, false);
        let p = PublishOptions::new().partition(1);
        let plans = outbox_plan(
            &[publish(TOPIC, &o, &p, "T"), publish(TOPIC, &o, &p, "T")],
            &[],
        )
        .unwrap();
        assert!(
            plans
                .iter()
                .all(|pl| pl.stream == format!("dev:re:s:{TOPIC}:1"))
        );
    }

    #[test]
    fn write_and_publish_many_checks_the_declared_state_keys_against_the_stream_slot() {
        let o = opts(4, true);
        let p = PublishOptions::new();
        let err = outbox_plan(&[publish(TOPIC, &o, &p, "T")], &["customer:123"]).unwrap_err();
        assert!(
            matches!(err, Error::Config(ref m) if m.contains("customer:123") && m.contains(TOPIC))
        );
        let ok_key = Outbox::state_key(&keys(), TOPIC, "1").unwrap();
        outbox_plan(&[publish(TOPIC, &o, &p, "T")], &[&ok_key]).unwrap();
    }

    #[test]
    fn a_publish_missing_its_topic_or_type_is_reported_by_index() {
        let o = opts(4, true);
        let p = PublishOptions::new();
        for second in [publish(" ", &o, &p, "T"), publish(TOPIC, &o, &p, "")] {
            let err = outbox_plan(&[publish(TOPIC, &o, &p, "T"), second], &[]).unwrap_err();
            assert!(matches!(err, Error::InvalidArgument(ref m) if m.contains("publish 1")));
        }
    }

    #[test]
    fn state_key_is_tagged_on_the_topic_and_refuses_braces() {
        assert_eq!(
            Outbox::state_key(&keys(), "bets", "abc").unwrap(),
            "dev:re:{bets}:state:abc"
        );
        assert!(matches!(
            Outbox::state_key(&keys(), "b{e}ts", "x"),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            Outbox::state_key(&keys(), " ", "x"),
            Err(Error::InvalidArgument(_))
        ));
        assert!(Outbox::ensure_same_slot(&keys(), "bets", &["dev:re:{bets}:x"], true, 0).is_ok());
        assert!(Outbox::ensure_same_slot(&keys(), "bets", &["dev:re:{bets}:x"], false, 0).is_err());
    }

    #[test]
    fn hash_tag_follows_the_redis_rule() {
        assert_eq!(hash_tag("a{b}c"), "b");
        assert_eq!(hash_tag("a{}c"), "a{}c");
        assert_eq!(hash_tag("abc"), "abc");
        assert_eq!(hash_tag("a{b"), "a{b");
        assert_eq!(hash_tag("{x}{y}"), "x");
    }

    #[test]
    fn unusable_topic_options_are_config_errors() {
        assert!(matches!(
            validate_topic("t", &opts(0, true)),
            Err(Error::Config(_))
        ));
        let zero = TopicOptions {
            max_len: Some(0),
            trim: TrimMode::Approx,
            ..TopicOptions::default()
        };
        assert!(
            matches!(validate_topic("t", &zero), Err(Error::Config(ref m)) if m.contains("MAXLEN 0"))
        );
    }

    // Idempotency (port of `IdempotencyTests`).

    #[test]
    fn key_is_hash_tagged_on_scope() {
        assert_eq!(
            Idempotency::key(&keys(), "bets", "abc").unwrap(),
            "dev:re:dedupe:{bets}:abc"
        );
        assert_eq!(
            hash_tag(&Idempotency::key(&keys(), "bets", "abc").unwrap()),
            "bets"
        );
        assert_eq!(
            hash_tag(&Outbox::state_key(&keys(), "bets", "abc").unwrap()),
            "bets"
        );
    }

    #[test]
    fn keys_are_distinct_per_scope_and_id() {
        let k = |s, i| Idempotency::key(&keys(), s, i).unwrap();
        assert_ne!(k("a", "1"), k("b", "1"));
        assert_ne!(k("a", "1"), k("a", "2"));
    }

    #[test]
    fn braced_scope_is_refused() {
        assert!(
            matches!(Idempotency::key(&keys(), "be{ts}", "1"), Err(Error::Config(ref m)) if m.contains("brace"))
        );
    }

    #[test]
    fn null_or_empty_arguments_are_refused() {
        let ttl = Duration::from_secs(60);
        assert!(matches!(
            Idempotency::try_begin_cmd(&keys(), "  ", "1", ttl),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            Idempotency::try_begin_cmd(&keys(), "s", "", ttl),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn ttl_below_one_millisecond_is_refused_before_the_command() {
        for ttl in [Duration::ZERO, Duration::from_micros(400)] {
            assert!(matches!(
                Idempotency::try_begin_cmd(&keys(), "s", "1", ttl),
                Err(Error::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn issues_set_nx_with_the_ttl() {
        let cmd =
            Idempotency::try_begin_cmd(&keys(), "bet-placed", "bet-123", Duration::from_hours(24))
                .unwrap();
        let args: Vec<Vec<u8>> = cmd
            .args_iter()
            .map(|a| match a {
                redis::Arg::Simple(b) => b.to_vec(),
                _ => Vec::new(),
            })
            .collect();
        let expected: Vec<Vec<u8>> = [
            "SET",
            "dev:re:dedupe:{bet-placed}:bet-123",
            "1",
            "NX",
            "PX",
            "86400000",
        ]
        .iter()
        .map(|s| s.as_bytes().to_vec())
        .collect();
        assert_eq!(args, expected);
    }

    #[test]
    fn guarded_xadd_argc_matches_what_write_to_writes() {
        let mut g = Guarded::new();
        g.xadd(
            "dev:re:{t}:state:x",
            vec!["MAXLEN".into(), "~".into(), "5".into()],
            OwnedFields {
                body: Bytes::from_static(b"b"),
                msg_type: "T".into(),
                partition_key: Some(String::new()),
                correlation_id: Some("c".into()),
                trace_parent: Some(String::new()),
                headers: HeaderBlock::empty(),
            },
        );
        let cmd = g.to_cmd("EVALSHA", "h");
        let args: Vec<&[u8]> = cmd
            .args_iter()
            .map(|a| match a {
                redis::Arg::Simple(b) => b,
                _ => &[][..],
            })
            .collect();
        // EVALSHA h 1 key 0(conds) 1(cmds) XADD 1 argc ...
        assert_eq!(args[6], b"XADD");
        let count: usize = std::str::from_utf8(args[8]).unwrap().parse().unwrap();
        assert_eq!(args.len() - 9, count);
        assert_eq!(&args[9..13], [&b"MAXLEN"[..], b"~", b"5", b"*"]);
    }
}
