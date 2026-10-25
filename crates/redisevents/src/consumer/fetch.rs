//! The fetch layer: blocking `XREAD` on dedicated reader connections
//! ([`Connection::dedicated`](crate::Connection::dedicated)), polling reads, parsing the raw reply
//! into [`StreamMsg`](crate::StreamMsg)s through [`wire::codec::decode`](crate::wire::codec::decode)
//! without copying bodies, and cancelling a parked read. **Owner: C1** (port of `BlockFetch.cs`,
//! `PollFetch.cs`, `StreamReaderConnection.cs`, `StreamBatch.cs`). Items here are `pub(crate)`
//! unless the lead agrees otherwise.
