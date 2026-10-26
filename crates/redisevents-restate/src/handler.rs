use std::fmt::Write as _;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use futures_util::stream::{self, StreamExt};
use redisevents::consumer::{HandleError, HandleResult, PartitionContext, TypedBatchHandler};
use redisevents::{BoxError, TypedMsg};
use reqwest::header::{CONTENT_TYPE, HeaderValue};
use reqwest::{Client, StatusCode};
use serde::Serialize;

use crate::blocked::RestateStartBlocked;
use crate::options::{RestateConfigError, RestateStartOptions};

/// At most this much of a rejected response's body goes into the failure reason.
const MAX_REASON_BODY_CHARS: usize = 300;
const PREVIOUSLY_ACCEPTED: &str = "PreviouslyAccepted";

/// The workflow to start for a kept message: what `map` returns.
#[derive(Debug, Clone)]
pub struct RestateStart {
    workflow: String,
    key: String,
    input: Input,
}

#[derive(Debug, Clone)]
enum Input {
    None,
    Body(Vec<u8>),
}

impl RestateStart {
    /// Starts `workflow` with the workflow key `key` (the business id; the only dedupe) and no body.
    #[must_use]
    pub fn new(workflow: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            workflow: workflow.into(),
            key: key.into(),
            input: Input::None,
        }
    }

    /// Sends `input` serialized as the JSON body.
    ///
    /// # Errors
    /// The serialization error, so a `map` closure can propagate it with `?` (the message then
    /// fails and blocks the batch).
    pub fn with_input<T: Serialize + ?Sized>(
        mut self,
        input: &T,
    ) -> Result<Self, serde_json::Error> {
        self.input = Input::Body(serde_json::to_vec(input)?);
        Ok(self)
    }
}

/// A [`TypedBatchHandler`] that starts one Restate workflow per kept message and acks the batch only
/// once every start is accepted. See the [crate docs](crate).
///
/// One instance is shared by every partition, keeps no per-call state and takes no lock.
pub struct RestateStartHandler<T, K, M> {
    http: Client,
    options: RestateStartOptions,
    keep: K,
    map: M,
    ingress_base: String,
    handler_segment: String,
    _marker: std::marker::PhantomData<fn(&T)>,
}

impl<T, K, M> RestateStartHandler<T, K, M>
where
    K: Fn(&TypedMsg<T>) -> bool + Send + Sync + 'static,
    M: Fn(&TypedMsg<T>) -> Result<RestateStart, BoxError> + Send + Sync + 'static,
{
    /// Creates the handler.
    ///
    /// `keep` is an in-memory filter (no I/O); a panic fails that message and blocks the batch.
    /// `map` turns a kept message into the workflow to start; an `Err` or panic, or an empty
    /// workflow or key, fails that message and blocks the batch. `http` is long-lived and shared.
    ///
    /// # Errors
    /// [`RestateConfigError`] for an invalid option or a non-http(s) ingress.
    pub fn new(
        http: Client,
        options: RestateStartOptions,
        keep: K,
        map: M,
    ) -> Result<Self, RestateConfigError> {
        options.validate()?;
        Ok(Self {
            http,
            ingress_base: options.ingress.as_str().trim_end_matches('/').to_owned(),
            handler_segment: escape(&options.handler),
            options,
            keep,
            map,
            _marker: std::marker::PhantomData,
        })
    }

    /// Filters and maps the batch synchronously, copying everything the submits need.
    fn prepare(&self, batch: &[TypedMsg<T>]) -> Vec<Item> {
        let mut items = Vec::new();
        for msg in batch {
            let id = msg.envelope.id().to_string();
            match std::panic::catch_unwind(AssertUnwindSafe(|| (self.keep)(msg))) {
                Err(p) => {
                    items.push(Item::failed_before_key(
                        id,
                        format!("Keep panicked: {}", panic_text(p.as_ref())),
                    ));
                }
                Ok(false) => {}
                Ok(true) => {
                    items.push(self.map_one(msg, id));
                }
            }
        }
        items
    }

    fn map_one(&self, msg: &TypedMsg<T>, id: String) -> Item {
        let start = match std::panic::catch_unwind(AssertUnwindSafe(|| (self.map)(msg))) {
            Err(p) => {
                return Item::failed_before_key(
                    id,
                    format!("Map panicked: {}", panic_text(p.as_ref())),
                );
            }
            Ok(Err(e)) => return Item::failed_before_key(id, format!("Map failed: {e}")),
            Ok(Ok(start)) => start,
        };
        if start.workflow.is_empty() || start.key.is_empty() {
            return Item::failed_before_key(id, "Map returned an empty workflow or key".to_owned());
        }
        let label = format!("{}/{}", start.workflow, start.key);
        let mut item = Item {
            label,
            workflow: start.workflow,
            stream_id: id,
            url: String::new(),
            body: None,
            trace_parent: msg.envelope.trace_parent().map(str::to_owned),
            outcome: Outcome::Pending,
            already_started: false,
            last_reason: None,
        };
        match start.input {
            Input::None => {}
            Input::Body(b) => item.body = Some(b),
        }
        item.url = format!(
            "{}/{}/{}/{}/send",
            self.ingress_base,
            escape(&item.workflow),
            escape(&start.key),
            self.handler_segment
        );
        item
    }

    /// Submits one item with in-place retry. Never fails; the outcome is recorded on the item.
    async fn submit(&self, mut item: Item, ctx: &PartitionContext) -> Item {
        if item.outcome != Outcome::Pending {
            return item;
        }
        let span = tracing::debug_span!(
            "restate.start",
            workflow = %item.workflow,
            key = %item.label,
            handler = %self.options.handler,
            id = %item.stream_id
        );
        let _enter = span.enter();
        let mut delay = self.options.initial_retry_delay;
        let mut attempt = 1_u32;
        loop {
            if ctx.is_cancelled() {
                item.fail("cancelled before it was accepted".to_owned());
                return item;
            }
            let result = self.attempt(&mut item, ctx).await;
            match result {
                Attempt::Cancelled => {
                    item.fail("cancelled before it was accepted".to_owned());
                    return item;
                }
                Attempt::Accepted | Attempt::AlreadyStarted => {
                    item.outcome = Outcome::Accepted;
                    item.already_started = matches!(result, Attempt::AlreadyStarted);
                    return item;
                }
                Attempt::Permanent => {
                    let r = item
                        .last_reason
                        .take()
                        .unwrap_or_else(|| "not accepted".to_owned());
                    item.fail(r);
                    return item;
                }
                Attempt::Transient => {
                    if attempt >= self.options.max_attempts.get() {
                        let r = item
                            .last_reason
                            .take()
                            .unwrap_or_else(|| "not accepted".to_owned());
                        item.fail(r);
                        return item;
                    }
                }
            }
            tracing::debug!(
                handler = "RestateStartHandler",
                attempt,
                max_attempts = self.options.max_attempts.get(),
                reason = item.last_reason.as_deref().unwrap_or(""),
                delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                "Restate: start of {} (stream id {}) failed, retrying",
                item.label,
                item.stream_id
            );
            if !delay.is_zero() {
                tokio::select! {
                    biased;
                    () = ctx.cancellation().cancelled() => {
                        item.fail("cancelled before it was accepted".to_owned());
                        return item;
                    }
                    () = tokio::time::sleep(delay) => {}
                }
            }
            delay = (delay * 2).min(self.options.max_retry_delay);
            attempt += 1;
        }
    }

    /// One submit. Classifies every way it can end.
    async fn attempt(&self, item: &mut Item, ctx: &PartitionContext) -> Attempt {
        let mut request = self.http.post(&item.url);
        if let Some(body) = &item.body {
            request = request
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(body.clone());
        }
        if let Some(tp) = item.trace_parent.as_deref().filter(|t| !t.is_empty()) {
            request = request.header("traceparent", tp);
        }
        let io = async {
            let response = request.send().await?;
            let status = response.status();
            let bytes = response.bytes().await;
            Ok::<_, reqwest::Error>((status, bytes))
        };
        let outcome = tokio::select! {
            biased;
            () = ctx.cancellation().cancelled() => return Attempt::Cancelled,
            r = tokio::time::timeout(self.options.attempt_timeout, io) => r,
        };
        let (status, bytes) = match outcome {
            Err(_elapsed) => {
                item.last_reason = Some("timed out (attempt timeout)".to_owned());
                return Attempt::Transient;
            }
            Ok(Err(e)) => {
                item.last_reason = Some(if e.is_timeout() {
                    "timed out (client timeout)".to_owned()
                } else {
                    format!("transport failure: {e}")
                });
                return Attempt::Transient;
            }
            Ok(Ok(pair)) => pair,
        };
        if status.is_success() {
            // Any 2xx is accepted. The body only tells the already-started metric apart; a body
            // that cannot be read or parsed does not un-accept the start.
            let previously = bytes.is_ok_and(|b| is_previously_accepted(&b));
            return if previously {
                Attempt::AlreadyStarted
            } else {
                Attempt::Accepted
            };
        }
        let snippet = bytes.map(|b| snippet(&b)).unwrap_or_default();
        let code = status.as_u16();
        item.last_reason = Some(if snippet.is_empty() {
            format!("HTTP {code}")
        } else {
            format!("HTTP {code}: {snippet}")
        });
        if is_transient(status) {
            Attempt::Transient
        } else {
            Attempt::Permanent
        }
    }

    async fn run(&self, batch: &[TypedMsg<T>], ctx: &PartitionContext) -> HandleResult {
        let items = self.prepare(batch);
        if items.is_empty() {
            return Ok(());
        }
        let total = items.len();
        // Every item runs to completion (cancellation is checked per item, not on the loop), so each
        // ends Accepted or Failed and none is abandoned mid-flight with no outcome.
        let done: Vec<Item> = stream::iter(items)
            .map(|item| self.submit(item, ctx))
            .buffered(self.options.max_parallelism.get())
            .collect()
            .await;
        let mut keys = Vec::new();
        let mut reasons = Vec::new();
        for item in done.iter().filter(|i| i.outcome != Outcome::Accepted) {
            keys.push(item.label.clone());
            reasons.push(match item.outcome {
                Outcome::Pending => "not attempted".to_owned(),
                _ => item
                    .last_reason
                    .clone()
                    .unwrap_or_else(|| "not accepted".to_owned()),
            });
        }
        if keys.is_empty() {
            return Ok(());
        }
        Err(Self::blocked(keys, total, &reasons, ctx))
    }

    fn blocked(
        keys: Vec<String>,
        kept: usize,
        reasons: &[String],
        ctx: &PartitionContext,
    ) -> HandleError {
        HandleError::block(RestateStartBlocked::new(
            keys,
            kept,
            reasons,
            ctx.is_cancelled(),
        ))
    }
}

impl<T, K, M> TypedBatchHandler<T> for RestateStartHandler<T, K, M>
where
    T: Send + Sync + 'static,
    K: Fn(&TypedMsg<T>) -> bool + Send + Sync + 'static,
    M: Fn(&TypedMsg<T>) -> Result<RestateStart, BoxError> + Send + Sync + 'static,
{
    /// Returns `Ok` only when every kept message was accepted (or none was kept); otherwise
    /// `Err(Block)` carrying a [`RestateStartBlocked`]. Never `Skip`.
    async fn handle_batch(&self, batch: &[TypedMsg<T>], ctx: &PartitionContext) -> HandleResult {
        // Defence in depth: everything below already records failures per item, so a panic here is a
        // bug. It must not reach the worker as a panic (treated as Skip, losing starts).
        match AssertUnwindSafe(self.run(batch, ctx)).catch_unwind().await {
            Ok(result) => result,
            Err(p) => Err(Self::blocked(
                vec!["(batch)".to_owned()],
                batch.len(),
                &[format!("panic: {}", panic_text(p.as_ref()))],
                ctx,
            )),
        }
    }
}

#[derive(Clone, Copy)]
enum Attempt {
    Accepted,
    AlreadyStarted,
    Transient,
    Permanent,
    Cancelled,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Pending,
    Accepted,
    Failed,
}

/// One kept message's start, copied out of the batch.
struct Item {
    label: String,
    workflow: String,
    stream_id: String,
    url: String,
    body: Option<Vec<u8>>,
    trace_parent: Option<String>,
    outcome: Outcome,
    already_started: bool,
    last_reason: Option<String>,
}

impl Item {
    fn failed_before_key(id: String, reason: String) -> Self {
        let mut item = Self {
            label: format!("stream id {id}"),
            workflow: String::new(),
            stream_id: id,
            url: String::new(),
            body: None,
            trace_parent: None,
            outcome: Outcome::Pending,
            already_started: false,
            last_reason: None,
        };
        item.fail(reason);
        item
    }

    fn fail(&mut self, reason: String) {
        self.outcome = Outcome::Failed;
        self.last_reason = Some(reason);
    }
}

fn is_transient(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 425 | 429 | 500..)
}

fn is_previously_accepted(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("status").and_then(|s| s.as_str().map(str::to_owned)))
        .is_some_and(|s| s == PREVIOUSLY_ACCEPTED)
}

fn snippet(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body).replace(['\r', '\n'], " ");
    let text = text.trim();
    if text.chars().count() <= MAX_REASON_BODY_CHARS {
        text.to_owned()
    } else {
        let mut s: String = text.chars().take(MAX_REASON_BODY_CHARS).collect();
        s.push('\u{2026}');
        s
    }
}

/// Percent-encodes a path segment: everything but the RFC 3986 unreserved characters is percent-encoded.
fn escape(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for b in segment.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn panic_text(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "(non-string panic)".to_owned())
}
