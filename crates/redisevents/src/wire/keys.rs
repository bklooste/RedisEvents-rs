//! [`Keys`], the Redis key layout (`WIRE.md` §1–2). **API: W0.2. Bodies: A1** (port of
//! `Config/KeyNamespace.cs` and `Wire/StreamKeys.cs`).
//!
//! Unlike C#, the prefix is a value rather than a process-wide static: [`Keys::from_env`] resolves it
//! the C# way, and tests use [`Keys::with_prefix`] instead of setting environment variables.

use std::sync::Arc;

/// Builds every key the library touches, under one prefix. Cheap to clone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Keys {
    prefix: Arc<str>,
}

impl Keys {
    /// The prefix from the environment, in the C# order: `STREAMS_KEY_PREFIX` verbatim when set and
    /// not blank; otherwise `"<env>:re:"` with `<env>` the lowercased first set of
    /// `ASPNETCORE_ENVIRONMENT`, `DOTNET_ENVIRONMENT`, then (Rust-only, ranked last)
    /// `STREAMS_ENVIRONMENT`, defaulting to `production`.
    #[must_use]
    pub fn from_env() -> Self {
        todo!("A1")
    }

    /// Resolves the prefix from `var` instead of the process environment (for tests).
    #[must_use]
    pub fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Self {
        let _ = var;
        todo!("A1")
    }

    /// Uses `prefix` verbatim, as `STREAMS_KEY_PREFIX` would.
    #[must_use]
    pub fn with_prefix(prefix: impl Into<Arc<str>>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// The resolved prefix.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Partition stream: `<prefix>s:{topic}:<p>`, or `<prefix>s:topic:<p>` when `co_locate` is off.
    #[must_use]
    pub fn stream(&self, topic: &str, partition: u32, co_locate: bool) -> String {
        let _ = (topic, partition, co_locate);
        todo!("A1")
    }

    /// Positions hash: `<prefix>p:{topic}:<consumer>`.
    #[must_use]
    pub fn positions(&self, topic: &str, consumer: &str) -> String {
        let _ = (topic, consumer);
        todo!("A1")
    }

    /// Positions meta hash: `<prefix>p:{topic}:<consumer>:meta`.
    #[must_use]
    pub fn positions_meta(&self, topic: &str, consumer: &str) -> String {
        let _ = (topic, consumer);
        todo!("A1")
    }

    /// Ownership / lease hash: `<prefix>o:{topic}:<consumer>`.
    #[must_use]
    pub fn ownership(&self, topic: &str, consumer: &str) -> String {
        let _ = (topic, consumer);
        todo!("A1")
    }

    /// Topic meta hash: `<prefix>m:{topic}`.
    #[must_use]
    pub fn topic_meta(&self, topic: &str) -> String {
        let _ = topic;
        todo!("A1")
    }

    /// State stream (outbox / event sourcing): `<prefix>{topic}:state:<name>`.
    #[must_use]
    pub fn state(&self, topic: &str, name: &str) -> String {
        let _ = (topic, name);
        todo!("A1")
    }

    /// View hash: `<prefix>{topic}:view:<owner>:<view_name>`.
    #[must_use]
    pub fn view(&self, topic: &str, owner: &str, view_name: &str) -> String {
        let _ = (topic, owner, view_name);
        todo!("A1")
    }

    /// Dedupe claim: `<prefix>dedupe:{scope}:<id>`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`](crate::Error::InvalidArgument) when `scope` contains `{` or `}`.
    pub fn dedupe(&self, scope: &str, id: &str) -> crate::Result<String> {
        let _ = (scope, id);
        todo!("A1")
    }

    /// `SCAN` pattern for every positions hash of a topic: `<prefix>p:{topic}:*`.
    #[must_use]
    pub fn positions_pattern(&self, topic: &str) -> String {
        let _ = topic;
        todo!("A1")
    }

    /// `SCAN` pattern for every partition stream of a topic: `<prefix>s:{topic}:*`.
    #[must_use]
    pub fn stream_pattern(&self, topic: &str, co_locate: bool) -> String {
        let _ = (topic, co_locate);
        todo!("A1")
    }
}
