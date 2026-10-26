//! The stream readiness endpoint.
//!
//! The grading rules live in [`redisevents::admin::health`]; this module maps the report to an HTTP
//! answer: `healthy` and `degraded` are 200 (a restart would not help), `unhealthy` is 503.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use redisevents::Connection;
use redisevents::admin::health::{self, StreamHealthReport, StreamHealthStatus};
use serde::Serialize;

struct HealthState {
    connection: Option<Connection>,
    counts: Box<dyn Fn() -> (u32, u32) + Send + Sync>,
}

/// The readiness endpoint, to be mounted at the path of the caller's choice.
///
/// `GET /` answers with the report as JSON:
///
/// ```text
/// {"status":"healthy","rule":"none","description":"…","details":{"consumers":2,…}}
/// ```
///
/// `status` is `healthy`, `degraded` or `unhealthy`; `rule` names the rule that fired (snake case);
/// `details` holds the structured facts of [`StreamHealthDetails`](redisevents::admin::health::StreamHealthDetails), omitting those that do not
/// apply.
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
    let report = health::check(state.connection.as_ref(), consumers, started).await;
    let (status, body) = render(&report);
    (status, Json(body)).into_response()
}

/// The JSON body of the readiness answer.
#[derive(Serialize)]
struct Body<'a> {
    status: StreamHealthStatus,
    rule: redisevents::admin::health::StreamHealthRule,
    description: &'a str,
    details: &'a redisevents::admin::health::StreamHealthDetails,
}

/// The HTTP status and JSON body for a report.
fn render(report: &StreamHealthReport) -> (StatusCode, Body<'_>) {
    let code = match report.status {
        StreamHealthStatus::Healthy | StreamHealthStatus::Degraded => StatusCode::OK,
        // Unhealthy, and any status a later core release adds: fail closed.
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    let body = Body {
        status: report.status,
        rule: report.rule,
        description: &report.description,
        details: &report.details,
    };
    (code, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redisevents::admin::health::{StreamHealthDetails, StreamHealthRule};

    fn report(status: StreamHealthStatus) -> StreamHealthReport {
        let mut details = StreamHealthDetails::default();
        details.consumers = Some(2);
        details.worst_lag_ms = Some(1500);
        details.ownership_degraded = Some(true);
        StreamHealthReport::new(status, StreamHealthRule::Lagging, "described")
            .with_details(details)
    }

    /// Healthy 200, Degraded 200 (a restart would not help), Unhealthy 503.
    #[test]
    fn each_status_maps_to_its_http_code() {
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
    fn the_body_carries_status_rule_description_and_present_details_only() {
        let degraded = report(StreamHealthStatus::Degraded);
        let (_, body) = render(&degraded);
        let json = serde_json::to_value(&body).expect("serialises");

        assert_eq!(json["status"], "degraded");
        assert_eq!(json["rule"], "lagging");
        assert_eq!(json["description"], "described");
        assert_eq!(json["details"]["consumers"], 2);
        assert_eq!(json["details"]["worst_lag_ms"], 1500);
        assert_eq!(json["details"]["ownership_degraded"], true);
        assert!(json["details"].get("connection").is_none());
    }
}
