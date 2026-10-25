//! The stream readiness endpoint: a port of C# `RedisEvents.HealthChecks.StreamsHealthCheck`.
//!
//! The rules live in `StreamHealth`; this module only
//! maps the report to an HTTP answer, as the C# check maps its report to a `HealthCheckResult`.
//! Status mapping, as ASP.NET's `MapHealthChecks` defaults: Healthy 200, Degraded 200, Unhealthy
//! 503.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use redisevents::Connection;
use redisevents::admin::health::{
    HealthValue, StreamHealth, StreamHealthReport, StreamHealthStatus,
};
use serde_json::{Map, Value, json};

/// The health check's name and tag (C# `StreamsHealthCheck.Name`).
pub const CHECK_NAME: &str = "streams";

struct HealthState {
    connection: Option<Connection>,
    counts: Box<dyn Fn() -> (u32, u32) + Send + Sync>,
}

/// The readiness endpoint, to be mounted at the path of the caller's choice (C#
/// `app.MapHealthChecks("/health/ready", ...)` filtered to the `streams` tag).
///
/// `GET /` answers with the report as JSON:
///
/// ```text
/// {"status":"Healthy","entries":{"streams":{"status":"Healthy","description":"…","data":{…},"tags":["streams"]}}}
/// ```
///
/// The shape follows ASP.NET's health report. Durations are not included: the report is graded
/// from monitor snapshots, not timed.
///
/// `connection` is the shared Redis connection, pinged with a 1 s bound; `None` skips the ping. The
/// closure returns `(consumers, consumers_started)` for the consumer host, `(0, 0)` when there is
/// none; it is called on every request.
///
/// ```no_run
/// # async fn demo(conn: redisevents::Connection) {
/// let health = redisevents_axum::health_router(Some(conn), || (0, 0));
/// let app = axum::Router::new().nest("/health/ready", health);
/// # let _ = app;
/// # }
/// ```
pub fn health_router<F>(connection: Option<Connection>, counts: F) -> Router
where
    F: Fn() -> (u32, u32) + Send + Sync + 'static,
{
    Router::new()
        .route("/", get(readiness))
        .with_state(Arc::new(HealthState {
            connection,
            counts: Box::new(counts),
        }))
}

async fn readiness(State(state): State<Arc<HealthState>>) -> Response {
    let (consumers, started) = (state.counts)();
    let report = StreamHealth::check(state.connection.as_ref(), consumers, started).await;
    let (status, body) = render(&report);
    (status, Json(body)).into_response()
}

/// The HTTP status and JSON body for a report.
fn render(report: &StreamHealthReport) -> (StatusCode, Value) {
    let (code, label) = match report.status {
        StreamHealthStatus::Healthy => (StatusCode::OK, "Healthy"),
        StreamHealthStatus::Degraded => (StatusCode::OK, "Degraded"),
        // Unhealthy, and any status a later core release adds: fail closed.
        _ => (StatusCode::SERVICE_UNAVAILABLE, "Unhealthy"),
    };
    let mut data = Map::new();
    for (key, value) in &report.data {
        data.insert(key.clone(), health_value(value));
    }
    let body = json!({
        "status": label,
        "entries": {
            CHECK_NAME: {
                "status": label,
                "description": report.description,
                "data": data,
                "tags": [CHECK_NAME],
            }
        }
    });
    (code, body)
}

fn health_value(value: &HealthValue) -> Value {
    match value {
        HealthValue::Int(n) => json!(n),
        HealthValue::Float(f) => {
            serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number)
        }
        HealthValue::Bool(b) => json!(b),
        HealthValue::Text(t) => json!(t),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redisevents::admin::health::{StreamHealthReport, StreamHealthRule};
    use std::collections::BTreeMap;

    fn report(status: StreamHealthStatus) -> StreamHealthReport {
        StreamHealthReport {
            status,
            rule: StreamHealthRule::None,
            description: "described".to_owned(),
            data: BTreeMap::from([
                ("consumers".to_owned(), HealthValue::Int(2)),
                ("worstLagMs".to_owned(), HealthValue::Float(1.5)),
                ("ownershipDegraded".to_owned(), HealthValue::Bool(true)),
                ("rule".to_owned(), HealthValue::Text("Lagging".to_owned())),
            ]),
            partition: None,
        }
    }

    /// C# `StreamsHealthCheck` maps by name, never by cast: Healthy 200, Degraded 200 (the default
    /// ASP.NET mapping), Unhealthy 503.
    #[test]
    fn each_status_maps_to_the_asp_net_default_code() {
        assert_eq!(
            render(&report(StreamHealthStatus::Healthy)).0,
            StatusCode::OK
        );
        assert_eq!(
            render(&report(StreamHealthStatus::Degraded)).0,
            StatusCode::OK
        );
        assert_eq!(
            render(&report(StreamHealthStatus::Unhealthy)).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn the_body_carries_the_status_description_and_data_under_the_streams_entry() {
        let (_, body) = render(&report(StreamHealthStatus::Degraded));

        assert_eq!(body["status"], "Degraded");
        let entry = &body["entries"][CHECK_NAME];
        assert_eq!(entry["status"], "Degraded");
        assert_eq!(entry["description"], "described");
        assert_eq!(entry["tags"], serde_json::json!(["streams"]));
        assert_eq!(entry["data"]["consumers"], 2);
        assert_eq!(entry["data"]["worstLagMs"], 1.5);
        assert_eq!(entry["data"]["ownershipDegraded"], true);
        assert_eq!(entry["data"]["rule"], "Lagging");
    }

    #[test]
    fn a_non_finite_measurement_is_null_not_a_broken_document() {
        assert_eq!(health_value(&HealthValue::Float(f64::NAN)), Value::Null);
    }
}
