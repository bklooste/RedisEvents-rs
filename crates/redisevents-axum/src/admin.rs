//! Stream administration routes: preview and apply a position reset, and read the ownership map.
//!
//! The port of C# `RedisEvents.Web.StreamAdminEndpoints`. Routes, verbs, query parameters, status
//! codes and bodies match the C# ones; see the crate docs for the table.
//!
//! These routes can rewind and fast-forward production consumers, so they are only ever mounted
//! through [`stream_admin_router`], which takes an authorization layer that every route carries.
//! There is no unauthenticated variant: a caller who wants none passes an explicit pass-through
//! layer, where the C# equivalent is `RequireAuthorization = false`.

use std::collections::{BTreeSet, HashMap};
use std::convert::Infallible;
use std::fmt::Write as _;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{Route, get, post};
use redis::RedisError;
use redisevents::admin::stream_admin::{
    DEFAULT_MAX_COUNT_SCAN, ResetOptions, ResetPreview, ResetTarget, StreamAdmin,
};
use redisevents::config::{StartFrom, TopicOptions};
use redisevents::ownership::OwnershipMap;
use redisevents::wire::{Keys, StreamId};
use redisevents::{Connection, Error};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tower::{Layer, Service};

/// Longest topic or consumer name accepted in a route (C# `StreamAdminEndpoints.MaxNameLength`).
pub const MAX_NAME_LENGTH: usize = 128;

/// Options for [`stream_admin_router`]. The defaults are the safe ones.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamAdminOptions {
    /// Whether a reset may be applied while the consumer still holds partition claims (default
    /// `false`). Left `false`, an apply against a live consumer answers 409 and names the owners;
    /// a caller who means it passes `force=true`. C# `AllowResetWhileRunning`.
    pub allow_reset_while_running: bool,
}

#[derive(Clone)]
struct AdminState {
    conn: Connection,
    keys: Keys,
    options: StreamAdminOptions,
}

/// The stream administration routes, to be nested under a prefix of the caller's choosing (C#
/// `app.MapGroup("/admin/streams")`).
///
/// Mapped routes:
///
/// - `GET  {topic}/{consumer}/reset/preview?from=…|id=…|to=start|end[&partition=N][&maxCountScan=N][&whenMissing=beginning|now]`
/// - `POST {topic}/{consumer}/reset?from=…|id=…|to=start|end[&partition=N][&force=true]`
/// - `GET  {topic}/ownership`
///
/// Every route passes through `authorize` (applied with `route_layer`, so it runs only for a
/// matched route; an unknown path is a plain 404). The layer is required on purpose: the router
/// never answers without an explicit decision about who may reset a consumer.
///
/// ```no_run
/// # async fn demo(conn: redisevents::Connection) {
/// use axum::middleware::{self, Next};
/// use axum::extract::Request;
/// use axum::response::Response;
///
/// async fn require_admin(request: Request, next: Next) -> Response {
///     // Check the caller here; reject with 401/403 before calling `next`.
///     next.run(request).await
/// }
///
/// let admin = redisevents_axum::stream_admin_router(
///     conn,
///     redisevents::wire::Keys::from_env(),
///     redisevents_axum::StreamAdminOptions::default(),
///     middleware::from_fn(require_admin),
/// );
/// let app = axum::Router::new().nest("/admin/streams", admin);
/// # let _ = app;
/// # }
/// ```
pub fn stream_admin_router<L>(
    conn: Connection,
    keys: Keys,
    options: StreamAdminOptions,
    authorize: L,
) -> Router
where
    L: Layer<Route> + Clone + Send + Sync + 'static,
    L::Service: Service<Request> + Clone + Send + Sync + 'static,
    <L::Service as Service<Request>>::Response: IntoResponse + 'static,
    <L::Service as Service<Request>>::Error: Into<Infallible> + 'static,
    <L::Service as Service<Request>>::Future: Send + 'static,
{
    Router::new()
        .route("/{topic}/{consumer}/reset/preview", get(preview_route))
        .route("/{topic}/{consumer}/reset", post(apply_route))
        .route("/{topic}/ownership", get(ownership_route))
        .route_layer(authorize)
        .with_state(Arc::new(AdminState {
            conn,
            keys,
            options,
        }))
}

type Failure = (StatusCode, String);

/// The parsed query of a reset, preview or apply.
#[derive(Debug)]
struct ResetRequest {
    target: ResetTarget,
    partition: Option<i64>,
    max_count_scan: Option<u64>,
    when_missing: StartFrom,
    force: bool,
}

async fn preview_route(
    State(state): State<Arc<AdminState>>,
    Path((topic, consumer)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    respond(preview_reset(&state, &topic, &consumer, &query).await)
}

async fn apply_route(
    State(state): State<Arc<AdminState>>,
    Path((topic, consumer)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    respond(apply_reset(&state, &topic, &consumer, &query).await)
}

async fn ownership_route(
    State(state): State<Arc<AdminState>>,
    Path(topic): Path<String>,
) -> Response {
    respond(ownership_text(&state, &topic).await)
}

fn respond(result: Result<String, Failure>) -> Response {
    let (status, body) = result.map_or_else(|failure| failure, |body| (StatusCode::OK, body));
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
        .into_response()
}

fn bad_request(message: impl Into<String>) -> Failure {
    (StatusCode::BAD_REQUEST, message.into())
}

/// Maps a core error to the status the C# handlers answer with.
fn core_failure(error: Error) -> Failure {
    match error {
        Error::Config(message) | Error::InvalidArgument(message) => {
            (StatusCode::BAD_REQUEST, format!("Error: {message}\n"))
        }
        Error::Redis(error) if is_connection_failure(&error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Error: Redis unavailable: {error}\n"),
        ),
        Error::Redis(error) => (
            StatusCode::BAD_GATEWAY,
            format!("Error: Redis rejected the command: {error}\n"),
        ),
        other => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Error: {other}\n"),
        ),
    }
}

fn is_connection_failure(error: &RedisError) -> bool {
    error.is_connection_dropped()
        || error.is_connection_refusal()
        || error.is_timeout()
        || error.is_io_error()
}

async fn preview_reset(
    state: &AdminState,
    topic: &str,
    consumer: &str,
    query: &HashMap<String, String>,
) -> Result<String, Failure> {
    let request = parse_reset(query, false)?;
    let (admin, layout, running) = prepare(state, topic, consumer, &request).await?;

    let options = ResetOptions {
        partition: partition_of(&request),
        topic: Some(layout.clone()),
        max_count_scan: request.max_count_scan.unwrap_or(DEFAULT_MAX_COUNT_SCAN),
        start_from_when_missing: request.when_missing,
    };
    let preview = admin
        .preview_reset(topic, consumer, request.target, &options)
        .await
        .map_err(core_failure)?;

    let mut body = describe_preview(&preview);
    if let Some((map, owned)) = &running {
        body.push_str(&running_consumer_text(map, *owned, false));
    }
    Ok(body)
}

async fn apply_reset(
    state: &AdminState,
    topic: &str,
    consumer: &str,
    query: &HashMap<String, String>,
) -> Result<String, Failure> {
    let request = parse_reset(query, true)?;
    let (admin, layout, running) = prepare(state, topic, consumer, &request).await?;

    if let Some((map, owned)) = &running
        && !request.force
        && !state.options.allow_reset_while_running
    {
        return Err((
            StatusCode::CONFLICT,
            running_consumer_text(map, *owned, true),
        ));
    }

    let options = ResetOptions {
        partition: partition_of(&request),
        topic: Some(layout.clone()),
        ..ResetOptions::default()
    };
    let preview = admin
        .preview_reset(topic, consumer, request.target, &options)
        .await
        .map_err(core_failure)?;
    let mut body = describe_preview(&preview);
    if let Some((map, owned)) = &running {
        body.push_str(&running_consumer_text(map, *owned, false));
    }

    let applied = admin
        .reset_position(topic, consumer, request.target, &options)
        .await
        .map_err(core_failure)?;
    let _ = writeln!(body, "\nApplied reset to {} partition(s).", applied.len());
    Ok(body)
}

/// The validation and checks shared by preview and apply, in the C# order: names, target, when
/// missing, scan cap, then the topic's layout (404 when it has none), partition range, and the
/// ownership of the consumer. Returns the running-owner description when any partition is held.
async fn prepare(
    state: &AdminState,
    topic: &str,
    consumer: &str,
    request: &ResetRequest,
) -> Result<(StreamAdmin, TopicOptions, Option<(String, usize)>), Failure> {
    let admin = StreamAdmin::new(state.conn.clone(), state.keys.clone());
    let layout = admin.read_topic_layout(topic).await.map_err(core_failure)?;
    if layout.partitions < 1 {
        return Err((
            StatusCode::NOT_FOUND,
            format!(
                "Error: topic '{topic}' has no recorded partition count in m:{{{topic}}}; it has never been created by a publisher or consumer\n"
            ),
        ));
    }
    if let Some(one) = request.partition
        && (one < 0 || one >= i64::from(layout.partitions))
    {
        return Err(bad_request(format!(
            "Error: topic '{topic}' has {} partition(s), numbered 0..{}; partition {one} does not exist\n",
            layout.partitions,
            layout.partitions - 1
        )));
    }

    let ownership = admin
        .get_ownership(topic, consumer, layout.partitions)
        .await
        .map_err(core_failure)?;
    let owned = ownership.owners.len();
    let running = (owned > 0).then(|| (describe_map(&ownership, layout.partitions), owned));
    Ok((admin, layout, running))
}

/// Validates the query. `preview` reads `maxCountScan` and `whenMissing`; the apply route does not
/// take them, so they are ignored there, as C# does.
fn parse_reset(query: &HashMap<String, String>, apply: bool) -> Result<ResetRequest, Failure> {
    let get = |name: &str| query.get(name).map(String::as_str);
    let from = get("from").filter(|v| !v.trim().is_empty());
    let id = get("id").filter(|v| !v.trim().is_empty());
    let to = get("to").filter(|v| !v.trim().is_empty());
    let target = parse_target(from, id, to)?;

    let partition = match get("partition") {
        None => None,
        Some(value) => Some(
            value
                .parse::<i64>()
                .map_err(|_| bad_request("Error: partition must be an integer\n"))?,
        ),
    };

    let force = match get("force") {
        None => false,
        Some(value) if value.eq_ignore_ascii_case("true") => true,
        Some(value) if value.eq_ignore_ascii_case("false") => false,
        Some(_) => return Err(bad_request("Error: force must be true or false\n")),
    };

    if apply {
        return Ok(ResetRequest {
            target,
            partition,
            max_count_scan: None,
            when_missing: StartFrom::Beginning,
            force,
        });
    }

    let when_missing = parse_when_missing(get("whenMissing"))?;
    let max_count_scan = match get("maxCountScan") {
        None => None,
        Some(value) => match value.parse::<i64>() {
            Ok(n) if n > 0 => Some(u64::try_from(n).unwrap_or(u64::MAX)),
            Ok(_) => {
                return Err(bad_request(
                    "Error: maxCountScan must be greater than zero\n",
                ));
            }
            Err(_) => return Err(bad_request("Error: maxCountScan must be an integer\n")),
        },
    };
    Ok(ResetRequest {
        target,
        partition,
        max_count_scan,
        when_missing,
        force,
    })
}

/// Resolves exactly one of `from`, `id` and `to` into a target (C# `TryResolveTarget`).
fn parse_target(
    from: Option<&str>,
    id: Option<&str>,
    to: Option<&str>,
) -> Result<ResetTarget, Failure> {
    let supplied = [from, id, to].iter().filter(|v| v.is_some()).count();
    if supplied != 1 {
        return Err(bad_request(if supplied == 0 {
            "Error: exactly one target is required: from=<ISO-8601 instant>, id=<ms-seq>, to=start or to=end\n"
        } else {
            "Error: from, id and to are mutually exclusive; supply exactly one\n"
        }));
    }
    if let Some(from) = from {
        return OffsetDateTime::parse(from, &Rfc3339)
            .map(ResetTarget::Time)
            .map_err(|_| {
                bad_request(
                    "Error: from must be a valid ISO-8601 instant, e.g. 2026-09-01T00:00:00Z\n",
                )
            });
    }
    if let Some(id) = id {
        return StreamId::parse(id).map(ResetTarget::Id).map_err(|_| {
            bad_request("Error: id must be a Redis stream id, e.g. 1757000000000-0\n")
        });
    }
    match to {
        Some(value) if value.eq_ignore_ascii_case("start") => Ok(ResetTarget::Start),
        Some(value) if value.eq_ignore_ascii_case("end") => Ok(ResetTarget::End),
        _ => Err(bad_request("Error: to must be either start or end\n")),
    }
}

/// Parses `whenMissing` (C# `TryParseWhenMissing`).
fn parse_when_missing(value: Option<&str>) -> Result<StartFrom, Failure> {
    match value.map(str::trim) {
        None | Some("") => Ok(StartFrom::Beginning),
        Some(v) if v.eq_ignore_ascii_case("beginning") => Ok(StartFrom::Beginning),
        Some(v) if v.eq_ignore_ascii_case("now") => Ok(StartFrom::Now),
        Some(_) => Err(bad_request(
            "Error: whenMissing must be either beginning or now (the consumer's StartFromWhenMissing)\n",
        )),
    }
}

async fn ownership_text(state: &AdminState, topic: &str) -> Result<String, Failure> {
    if let Some(error) = name_error(topic, "topic") {
        return Err(bad_request(error));
    }
    let admin = StreamAdmin::new(state.conn.clone(), state.keys.clone());
    let meta = admin.read_topic_meta(topic).await.map_err(core_failure)?;
    let partitions = meta
        .get("partitions")
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|count| *count > 0)
        .unwrap_or(0);

    let consumers = scan_consumers(state, topic)
        .await
        .map_err(|e| core_failure(Error::Redis(e)))?;

    let mut text = String::new();
    let _ = writeln!(text, "Ownership map for topic {topic}");
    let _ = writeln!(text, "Recorded partitions: {partitions}");
    for consumer in &consumers {
        let ownership = admin
            .get_ownership(topic, consumer, partitions)
            .await
            .map_err(core_failure)?;
        let total = if partitions > 0 {
            partitions
        } else {
            ownership.owners.keys().map(|p| p + 1).max().unwrap_or(0)
        };
        text.push('\n');
        let _ = writeln!(text, "Consumer: {consumer}");
        let _ = writeln!(text, "  Total partitions: {total}");
        let _ = writeln!(text, "  Owned: {}", ownership.owners.len());
        let _ = writeln!(text, "  Unowned: {}", ownership.unowned.len());
        let _ = writeln!(text, "  Contested: {}", ownership.contested.len());
        for (partition, owner) in &ownership.owners {
            let _ = writeln!(text, "    {partition} -> {}", owner.format());
        }
        if !ownership.unowned.is_empty() {
            let unowned: Vec<String> = ownership.unowned.iter().map(u32::to_string).collect();
            let _ = writeln!(text, "  Unowned partitions: {}", unowned.join(", "));
        }
    }
    if consumers.is_empty() {
        text.push_str("No consumers found for this topic.\n");
    }
    Ok(text)
}

/// Every consumer of `topic` that has an ownership hash, found with `SCAN MATCH o:{topic}:*`. The
/// braces are the Redis Cluster hash tag and are part of the key, so they are kept literal here.
async fn scan_consumers(state: &AdminState, topic: &str) -> Result<Vec<String>, RedisError> {
    let prefix = state.keys.ownership(topic, "");
    let pattern = state.keys.ownership(topic, "*");
    let mut shared = state.conn.shared();
    let mut cursor: u64 = 0;
    let mut names = BTreeSet::new();
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut shared)
            .await?;
        for key in keys {
            if let Some(name) = key.strip_prefix(&prefix)
                && !name.is_empty()
            {
                names.insert(name.to_owned());
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(names.into_iter().collect())
}

/// `full map: 0->pod-0 1->(none)` (C# `OwnershipMap.Describe`).
fn describe_map(ownership: &OwnershipMap, partitions: u32) -> String {
    let mut text = String::from("full map:");
    for partition in 0..partitions {
        match ownership.owners.get(&partition) {
            Some(owner) => {
                let _ = write!(text, " {partition}->{}", owner.format());
            }
            None => {
                let _ = write!(text, " {partition}->(none)");
            }
        }
    }
    text
}

fn describe_preview(preview: &ResetPreview) -> String {
    preview.describe()
}

/// The partition a validated request names. `prepare` has already checked it is in range.
fn partition_of(request: &ResetRequest) -> Option<u32> {
    request.partition.and_then(|p| u32::try_from(p).ok())
}

/// The text a reset appends while the consumer still holds claims (C# `RunningConsumerText`).
fn running_consumer_text(map: &str, owned: usize, blocked: bool) -> String {
    let mut text = String::from("\n");
    let _ = writeln!(
        text,
        "{}this consumer still holds {owned} live partition claim(s), so at least one worker is running.",
        if blocked { "Error: " } else { "Warning: " }
    );
    let _ = writeln!(text, "  {map}");
    text.push_str("  The documented procedure is scale to zero, reset, scale up: a running worker picks the reset up on its\n");
    text.push_str(
        "  next flush tick and rewinds mid-batch, which redelivers whatever it had in flight.\n",
    );
    if blocked {
        text.push_str(
            "  Nothing was written. Re-issue with ?force=true if that is what you meant.\n",
        );
    }
    text
}

/// Validates a topic or consumer name (C# `StreamAdminEndpoints.NameError`). The accepted alphabet
/// is letters, digits, `_`, `-`, `.` and `:`: braces re-tag the key and glob characters would turn
/// the ownership SCAN into a pattern.
pub(crate) fn name_error(value: &str, what: &str) -> Option<String> {
    if value.trim().is_empty() {
        return Some(format!("Error: {what} is required\n"));
    }
    if value.len() > MAX_NAME_LENGTH {
        return Some(format!(
            "Error: {what} is longer than {MAX_NAME_LENGTH} characters\n"
        ));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
    {
        return Some(format!(
            "Error: {what} may only contain letters, digits, '_', '-', '.' and ':'\n"
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC: &str = "feed_source_racing";

    fn query(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn ownership_scan_glob_keeps_the_literal_hash_tag_braces() {
        let keys = Keys::with_prefix("test:");
        let pattern = keys.ownership(TOPIC, "*");

        assert_eq!(pattern, format!("test:o:{{{TOPIC}}}:*"));
        assert_ne!(
            pattern,
            format!("test:o:{TOPIC}:*"),
            "interpolating the topic into the braces is the bug this test exists for"
        );
    }

    #[test]
    fn ownership_key_prefix_strips_back_to_the_consumer_name() {
        let keys = Keys::with_prefix("test:");
        let prefix = keys.ownership(TOPIC, "");
        let key = keys.ownership(TOPIC, "svc-0");

        assert_eq!(prefix, format!("test:o:{{{TOPIC}}}:"));
        assert_eq!(key.strip_prefix(&prefix), Some("svc-0"));
    }

    #[test]
    fn ordinary_names_are_accepted() {
        for name in ["feed_source_racing", "orders-v2", "svc.worker:0", "A0"] {
            assert_eq!(name_error(name, "topic"), None, "{name}");
        }
    }

    #[test]
    fn a_name_with_braces_is_refused() {
        for name in ["{evil}", "orders}", "{orders"] {
            assert!(name_error(name, "topic").is_some(), "{name}");
        }
    }

    #[test]
    fn a_name_with_glob_metacharacters_is_refused() {
        for name in ["orders*", "or?ers", "orders[0-9]", "orders\\x"] {
            assert!(name_error(name, "topic").is_some(), "{name}");
        }
    }

    #[test]
    fn an_empty_name_is_refused() {
        for name in ["", "   "] {
            let error = name_error(name, "consumer").expect("refused");
            assert!(error.contains("consumer"), "{error}");
        }
    }

    #[test]
    fn the_name_length_limit_is_inclusive() {
        assert_eq!(name_error(&"a".repeat(MAX_NAME_LENGTH), "topic"), None);
        assert!(name_error(&"a".repeat(MAX_NAME_LENGTH + 1), "topic").is_some());
    }

    #[test]
    fn a_reset_with_no_target_is_refused() {
        let (status, body) = parse_reset(&query(&[]), false).expect_err("no target");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("exactly one"), "{body}");
    }

    #[test]
    fn two_targets_at_once_are_refused() {
        let (status, body) = parse_reset(
            &query(&[("from", "2026-09-01T00:00:00Z"), ("id", "5-0")]),
            false,
        )
        .expect_err("two targets");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("mutually exclusive"), "{body}");
    }

    #[test]
    fn a_date_target_resolves_to_that_instant() {
        let request = parse_reset(&query(&[("from", "2026-09-01T00:00:00Z")]), false).expect("ok");
        assert_eq!(
            request.target,
            ResetTarget::Time(OffsetDateTime::parse("2026-09-01T00:00:00Z", &Rfc3339).unwrap())
        );
    }

    #[test]
    fn an_unparseable_date_is_refused() {
        let (_, body) = parse_reset(&query(&[("from", "yesterday")]), false).expect_err("bad date");
        assert!(body.contains("ISO-8601"), "{body}");
    }

    #[test]
    fn an_exact_id_target_resolves() {
        let request = parse_reset(&query(&[("id", "1757000000000-4")]), false).expect("ok");
        assert_eq!(
            request.target,
            ResetTarget::Id(StreamId::parse("1757000000000-4").unwrap())
        );
    }

    #[test]
    fn an_unparseable_id_is_refused() {
        let (_, body) = parse_reset(&query(&[("id", "not-an-id")]), false).expect_err("bad id");
        assert!(body.contains("stream id"), "{body}");
    }

    #[test]
    fn to_start_and_to_end_resolve_case_insensitively() {
        let start = parse_reset(&query(&[("to", "start")]), false).expect("start");
        let end = parse_reset(&query(&[("to", "END")]), false).expect("end");
        assert_eq!(start.target, ResetTarget::Start);
        assert_eq!(end.target, ResetTarget::End);
    }

    #[test]
    fn an_unknown_to_value_is_refused() {
        let (_, body) = parse_reset(&query(&[("to", "middle")]), false).expect_err("bad to");
        assert!(body.contains("start or end"), "{body}");
    }

    #[test]
    fn when_missing_parses() {
        assert_eq!(parse_when_missing(None).unwrap(), StartFrom::Beginning);
        assert_eq!(parse_when_missing(Some("")).unwrap(), StartFrom::Beginning);
        assert_eq!(
            parse_when_missing(Some("beginning")).unwrap(),
            StartFrom::Beginning
        );
        assert_eq!(parse_when_missing(Some("NOW")).unwrap(), StartFrom::Now);
    }

    #[test]
    fn an_unknown_when_missing_is_refused() {
        let (status, body) = parse_when_missing(Some("stored")).expect_err("bad");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("beginning or now"), "{body}");
    }

    #[test]
    fn max_count_scan_must_be_positive() {
        let (_, body) =
            parse_reset(&query(&[("to", "end"), ("maxCountScan", "0")]), false).expect_err("zero");
        assert!(body.contains("greater than zero"), "{body}");
        let ok = parse_reset(&query(&[("to", "end"), ("maxCountScan", "10")]), false).expect("ok");
        assert_eq!(ok.max_count_scan, Some(10));
    }

    #[test]
    fn apply_ignores_the_preview_only_parameters_as_the_post_route_does() {
        let request = parse_reset(
            &query(&[
                ("to", "start"),
                ("whenMissing", "bogus"),
                ("maxCountScan", "-1"),
            ]),
            true,
        )
        .expect("apply does not read whenMissing or maxCountScan");
        assert_eq!(request.max_count_scan, None);
    }

    #[test]
    fn force_must_be_a_boolean() {
        let (_, body) =
            parse_reset(&query(&[("to", "start"), ("force", "yes")]), true).expect_err("bad force");
        assert!(body.contains("force"), "{body}");
        let forced = parse_reset(&query(&[("to", "start"), ("force", "TRUE")]), true).expect("ok");
        assert!(forced.force);
    }

    #[test]
    fn the_running_consumer_text_says_what_it_refused() {
        let blocked = running_consumer_text("full map: 0->pod-0", 1, true);
        assert!(
            blocked.starts_with("\nError: this consumer still holds 1 live partition claim(s)")
        );
        assert!(blocked.contains("scale to zero"));
        assert!(blocked.contains("?force=true"));

        let warning = running_consumer_text("full map: 0->pod-0", 1, false);
        assert!(warning.contains("Warning:"));
        assert!(!warning.contains("?force=true"));
    }
}
