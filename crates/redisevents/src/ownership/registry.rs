//! The ownership registry: Static and Lease modes, the renew loop, the `HEXPIRE` probe, contested
//! detection and release on shutdown. **Owner: B5.** Crate-private; C3, C4, D1 and D2 consume it.
//! Embeds `wire-spec/lua/lease.lua` and `release.lua` byte for byte (`include_str!`).
