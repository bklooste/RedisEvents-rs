//! In-memory test doubles (feature `testing`). **Owner: D4** (port of `RedisEvents/Testing/*`):
//! in-memory [`Publisher`](crate::Publisher) / [`BufferedPublisher`](crate::BufferedPublisher), an
//! in-memory [`PositionStore`](crate::PositionStore) and a handler harness that drives a
//! [`BatchHandler`](crate::BatchHandler) through the C2 error contract without Redis.
//!
//! A real Redis for integration tests is not here: it is the `redisevents-test-support` crate.
