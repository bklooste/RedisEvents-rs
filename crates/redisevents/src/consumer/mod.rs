//! The consumer side. **Contracts owned by W0.2**: the handler traits ([`handler`]), the error
//! contract ([`error`]) and [`PartitionContext`] ([`context`]). The machinery behind them is
//! filled in by later WPs, one file each:
//!
//! | File | WP | Contents |
//! | --- | --- | --- |
//! | `fetch.rs` | C1 | blocking/polling reads on dedicated connections, raw reply parsing, `StreamBatch` |
//! | `worker.rs` | C2 | the per-partition state machine, skip/block, back-off, adapters' bodies |
//! | `host.rs` | C4 | `Streams` / `StreamsHost`: wiring workers, ownership, flusher and trimmer |
//! | `builder.rs` | C4b | registration surface over `host.rs` |
//! | `trimmer.rs` | C5 | the background trimmer |
//! | `work_queue.rs` | C6 | `XREADGROUP` delivery (deferred) |

pub mod builder;
pub mod context;
pub mod error;
pub mod fetch;
pub mod handler;
pub mod host;
pub(crate) mod shared;
pub mod trimmer;
pub mod work_queue;
pub(crate) mod worker;

pub use context::PartitionContext;
pub use error::{BLOCK_BACKOFF, HandleError, HandleResult, ResultExt};
pub use handler::{
    BatchHandler, MessageHandler, PerMessage, Typed, TypedBatchHandler, TypedHandler,
};
/// Re-exported so handlers and harnesses need no direct `tokio-util` dependency.
pub use tokio_util::sync::CancellationToken;
