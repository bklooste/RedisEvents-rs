//! The per-partition worker: read loop hand-off, skip/block error contract, back-off
//! ([`BLOCK_BACKOFF`](super::BLOCK_BACKOFF)), panic-as-skip, reset hand-off with the flusher.
//! **Owner: C2** (port of `PartitionWorker.*`, `HandlerAdapter.cs`, `Backoff.cs`). Crate-private.
