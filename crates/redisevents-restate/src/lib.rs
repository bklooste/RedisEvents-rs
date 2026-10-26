//! Batched Restate workflow starts from a redisevents stream.
//!
//! [`RestateStartHandler`] is a [`TypedBatchHandler`](redisevents::consumer::TypedBatchHandler): wrap it in
//! [`Typed`](redisevents::consumer::Typed) to use it as a batch handler. It starts one Restate
//! workflow per kept message, keyed by a business id, and acks the batch only once every start has
//! been accepted.
//!
//! # Why a typed batch handler
//!
//! The core `Typed` adapter already decodes bodies (dropping and logging malformed ones), so the
//! handler stays codec-agnostic and works with any `PayloadCodec`.
//!
//! # The submit
//!
//! `POST {ingress}/{workflow}/{key}/{handler}/send`, the input as the JSON body, the message's W3C
//! `traceparent`, and deliberately **no** `Idempotency-Key` (Restate 1.8 answers `400` to it on a
//! workflow handler and never starts the run). The workflow key is the dedupe: a second submit of
//! one key answers `202` with `"status":"PreviouslyAccepted"`. Kept items go out bounded-parallel
//! ([`RestateStartOptions::max_parallelism`]).
//!
//! # The ack rule
//!
//! Each item is retried in place ([`RestateStartOptions::max_attempts`], doubling delay). Any 2xx is
//! accepted; a timeout, a transport failure, `408`, `425`, `429` and any 5xx are retried; any other
//! status is not. If any item is still not accepted the handler returns
//! [`HandleError::Block`](redisevents::consumer::HandleError::Block) carrying a
//! [`RestateStartBlocked`]: the partition retries the whole batch and accepted starts dedupe. It
//! never returns `Skip`, not for a filter or map failure, a panic, or cancellation (a cancelled
//! `PartitionContext` also blocks; the worker stops without advancing).
#![allow(clippy::module_name_repetitions)]

mod blocked;
mod handler;
mod options;

pub use blocked::RestateStartBlocked;
pub use handler::{RestateStart, RestateStartHandler};
pub use options::{RestateConfigError, RestateStartOptions};
