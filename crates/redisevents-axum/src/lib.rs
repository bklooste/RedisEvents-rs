//! Admin endpoints and health checks for redisevents, on axum.
//!
//! Two routers to nest under a path of the caller's choosing: stream administration
//! ([`stream_admin_router`]) and a readiness check ([`health_router`]).
//!
//! | Router | Routes (relative to where it is nested) |
//! | --- | --- |
//! | [`stream_admin_router`] | `GET /{topic}/{consumer}/reset/preview`, `POST /{topic}/{consumer}/reset`, `GET /{topic}/ownership` |
//! | [`health_router`] | `GET /` |
//!
//! The admin router is opt-in by construction: nothing is mounted unless the caller calls
//! [`stream_admin_router`], and it requires an authorization layer for every route.

#![warn(missing_docs)]

pub mod admin;
pub mod health;

pub use admin::{MAX_NAME_LENGTH, StreamAdminOptions, stream_admin_router};
pub use health::health_router;
