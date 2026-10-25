//! Configuration binding and validation (port of `Config/StreamConfigBinder.cs`). **Owner: A2.**
//!
//! A2 adds here: binding the `Streams` section through the `config` crate with the C# key names,
//! the validation rules and messages, the legacy-key mapping (`UseConsumerGroup`), and
//! translating the C# connection string (`host:port,abortConnect=false,password=…`) to a redis-rs
//! URL for [`Connection::connect`](crate::Connection::connect).
