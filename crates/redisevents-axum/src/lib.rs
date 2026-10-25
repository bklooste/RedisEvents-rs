//! Admin endpoints and health checks for redisevents, on axum.
//!
//! The axum port of `RedisEvents.Web` (`StreamAdminEndpoints`) and `RedisEvents.HealthChecks`
//! (`StreamsHealthCheck`). Both are routers to nest under a path of the caller's choosing.
//!
//! | C# route | Rust route (nest under the caller's prefix) |
//! | --- | --- |
//! | `GET  {topic}/{consumer}/reset/preview` | [`stream_admin_router`] `GET /{topic}/{consumer}/reset/preview` |
//! | `POST {topic}/{consumer}/reset` | [`stream_admin_router`] `POST /{topic}/{consumer}/reset` |
//! | `GET  {topic}/ownership` | [`stream_admin_router`] `GET /{topic}/ownership` |
//! | `MapHealthChecks(path, tag "streams")` | [`health_router`] `GET /` |
//!
//! The admin router is opt-in by construction: nothing is mounted unless the caller calls
//! [`stream_admin_router`], and it requires an authorization layer for every route.

#![warn(missing_docs)]

pub mod admin;
pub mod health;

pub use admin::{MAX_NAME_LENGTH, StreamAdminOptions, stream_admin_router};
pub use health::{CHECK_NAME, health_router};
