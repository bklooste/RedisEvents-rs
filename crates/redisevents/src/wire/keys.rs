//! [`Keys`], the Redis key layout (`WIRE.md` §1–2). **API: W0.2. Bodies: A1** (port of
//! `Config/KeyNamespace.cs` and `Wire/StreamKeys.cs`).
//!
//! Unlike C#, the prefix is a value rather than a process-wide static: [`Keys::from_env`] resolves it
//! the C# way, and tests use [`Keys::with_prefix`] instead of setting environment variables.

use std::sync::Arc;

const OVERRIDE_VARIABLE: &str = "STREAMS_KEY_PREFIX";
/// Checked in order; the first one set wins.
const ENVIRONMENT_VARIABLES: [&str; 3] = [
    "ASPNETCORE_ENVIRONMENT",
    "DOTNET_ENVIRONMENT",
    "STREAMS_ENVIRONMENT",
];
const DEFAULT_ENVIRONMENT: &str = "production";
const MARKER: &str = "re";

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
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Resolves the prefix from `var` instead of the process environment (for tests).
    #[must_use]
    pub fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Self {
        if let Some(prefix) = var(OVERRIDE_VARIABLE).filter(|p| !p.trim().is_empty()) {
            return Self::with_prefix(prefix);
        }
        let environment = ENVIRONMENT_VARIABLES
            .iter()
            .find_map(|name| var(name))
            .unwrap_or_else(|| DEFAULT_ENVIRONMENT.to_owned());
        Self::with_prefix(format!("{}:{MARKER}:", environment.to_lowercase()))
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
        if co_locate {
            format!("{}s:{{{topic}}}:{partition}", self.prefix)
        } else {
            format!("{}s:{topic}:{partition}", self.prefix)
        }
    }

    /// Positions hash: `<prefix>p:{topic}:<consumer>`.
    #[must_use]
    pub fn positions(&self, topic: &str, consumer: &str) -> String {
        format!("{}p:{{{topic}}}:{consumer}", self.prefix)
    }

    /// Positions meta hash: `<prefix>p:{topic}:<consumer>:meta`.
    #[must_use]
    pub fn positions_meta(&self, topic: &str, consumer: &str) -> String {
        format!("{}p:{{{topic}}}:{consumer}:meta", self.prefix)
    }

    /// Ownership / lease hash: `<prefix>o:{topic}:<consumer>`.
    #[must_use]
    pub fn ownership(&self, topic: &str, consumer: &str) -> String {
        format!("{}o:{{{topic}}}:{consumer}", self.prefix)
    }

    /// Topic meta hash: `<prefix>m:{topic}`.
    #[must_use]
    pub fn topic_meta(&self, topic: &str) -> String {
        format!("{}m:{{{topic}}}", self.prefix)
    }

    /// State stream (outbox / event sourcing): `<prefix>{topic}:state:<name>`.
    #[must_use]
    pub fn state(&self, topic: &str, name: &str) -> String {
        format!("{}{{{topic}}}:state:{name}", self.prefix)
    }

    /// View hash: `<prefix>{topic}:view:<owner>:<view_name>`.
    #[must_use]
    pub fn view(&self, topic: &str, owner: &str, view_name: &str) -> String {
        format!("{}{{{topic}}}:view:{owner}:{view_name}", self.prefix)
    }

    /// Dedupe claim: `<prefix>dedupe:{scope}:<id>`.
    ///
    /// # Errors
    /// [`Error::InvalidArgument`](crate::Error::InvalidArgument) when `scope` contains `{` or `}`.
    pub fn dedupe(&self, scope: &str, id: &str) -> crate::Result<String> {
        if scope.contains(['{', '}']) {
            return Err(crate::Error::invalid_argument(format!(
                "A dedupe scope must not contain '{{' or '}}': '{scope}'."
            )));
        }
        Ok(format!("{}dedupe:{{{scope}}}:{id}", self.prefix))
    }

    /// `SCAN` pattern for every positions hash of a topic: `<prefix>p:{topic}:*`.
    #[must_use]
    pub fn positions_pattern(&self, topic: &str) -> String {
        format!("{}p:{{{topic}}}:*", self.prefix)
    }

    /// `SCAN` pattern for every partition stream of a topic: `<prefix>s:{topic}:*`.
    #[must_use]
    pub fn stream_pattern(&self, topic: &str, co_locate: bool) -> String {
        if co_locate {
            format!("{}s:{{{topic}}}:*", self.prefix)
        } else {
            format!("{}s:{topic}:*", self.prefix)
        }
    }
}
