//! Outbox and state store. **Owner: B3** (port of `Outbox.cs`, `StreamStore.cs`, `IStreamStore.cs`,
//! `StateEvent.cs`, `Idempotency.cs`, `TopicForwarder.cs`).
//!
//! Shape agreed in `docs/PLAN.md` §3: `Outbox::write_and_publish(|pipe| { … })` taking a
//! `redis::Pipeline` in atomic mode, run on a [`Connection::dedicated`](crate::Connection::dedicated)
//! connection (a `WATCH`/`MULTI` on the shared multiplexed connection is unsafe). E3 builds its
//! optimistic concurrency on this module.
