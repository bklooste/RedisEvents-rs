//! The consumer host: `Streams::builder(conn).topic("orders").handler(h).spawn()` returning a
//! `StreamsHost` handle with graceful shutdown. Wires workers, ownership, flusher and trimmer;
//! startup checks (topic meta, codec version, partition shrink). **Owner: C4** (port of
//! `StreamConsumerHost.cs`).
