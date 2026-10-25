//! Operator and startup operations on a topic. Files:
//!
//! | File | WP | Contents |
//! | --- | --- | --- |
//! | `meta.rs` | B1 | topic meta `m:{topic}`: `HSETNX` of `partitions`, `v`, `createdUtc`, `maxLen`; grow = warn + overwrite, shrink = refuse |
//! | `stream_admin.rs` | D1 | `StreamAdmin`: reset/preview, ownership view, `read_topic_tail` |
//! | `health.rs` | D2 | `StreamHealth`, `StreamLag`, `StreamStatus`, `StreamPartitionStatus` |
//!
//! `StreamAdmin` and the health types are public API added by D1/D2 (lead review per
//! `docs/CONTRACTS.md`); `redisevents-axum` (D3) consumes them.

pub mod health;
pub mod meta;
pub mod stream_admin;
