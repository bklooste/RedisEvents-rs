//! [`Keys`], the Redis key layout (`WIRE.md` §1–2). **API: W0.2. Bodies: A1** (port of
//! `Config/KeyNamespace.cs` and `Wire/StreamKeys.cs`).
//!
//! The prefix is the fixed `re:`, or `STREAMS_KEY_PREFIX` verbatim, the same rule as C#. The library
//! knows nothing about environments: topic and consumer names are opaque, and they are the only
//! isolation. An application running several environments on one Redis composes them itself (topic
//! `np.mult_feed_events`, consumers `dev.my-service` / `tst.my-service`).
//!
//! Unlike C#, the prefix is a value rather than a process-wide static: [`Keys::from_env`] reads the
//! override, and tests use [`Keys::with_prefix`] instead of setting environment variables.

use std::sync::Arc;

const OVERRIDE_VARIABLE: &str = "STREAMS_KEY_PREFIX";
/// The prefix when `STREAMS_KEY_PREFIX` is unset or blank; the same as C#.
const DEFAULT_PREFIX: &str = "re:";

/// Builds every key the library touches, under one prefix. Cheap to clone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Keys {
    prefix: Arc<str>,
}

impl Keys {
    /// The prefix from the environment: `STREAMS_KEY_PREFIX` verbatim when set and not blank,
    /// otherwise the fixed `"re:"`. No environment name is read
    /// (`ASPNETCORE_ENVIRONMENT`, `DOTNET_ENVIRONMENT`, `STREAMS_ENVIRONMENT` are all ignored), so
    /// C# and Rust agree with nothing configured. Logs the result at info.
    #[must_use]
    pub fn from_env() -> Self {
        let keys = Self::from_lookup(|name| std::env::var(name).ok());
        tracing::info!(prefix = keys.prefix(), "redis key prefix resolved");
        keys
    }

    /// Resolves the prefix from `var` instead of the process environment (for tests).
    #[must_use]
    pub fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Self {
        var(OVERRIDE_VARIABLE)
            .filter(|p| !p.trim().is_empty())
            .map_or_else(|| Self::with_prefix(DEFAULT_PREFIX), Self::with_prefix)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn key_prefix_is_used_verbatim() {
        let k = Keys::from_lookup(lookup(&[("STREAMS_KEY_PREFIX", "custom:")]));
        assert_eq!(k.prefix(), "custom:");
    }

    #[test]
    fn blank_key_prefix_is_ignored() {
        let k = Keys::from_lookup(lookup(&[("STREAMS_KEY_PREFIX", "   ")]));
        assert_eq!(k.prefix(), "re:");
    }

    #[test]
    fn nothing_set_is_the_fixed_prefix() {
        assert_eq!(Keys::from_lookup(lookup(&[])).prefix(), "re:");
    }

    #[test]
    fn environment_names_are_ignored() {
        let k = Keys::from_lookup(lookup(&[
            ("ASPNETCORE_ENVIRONMENT", "Development"),
            ("DOTNET_ENVIRONMENT", "QA"),
            ("STREAMS_ENVIRONMENT", "prod"),
        ]));
        assert_eq!(k.prefix(), "re:");
    }

    #[test]
    fn composed_names_are_opaque() {
        let k = Keys::from_lookup(lookup(&[]));
        assert_eq!(
            k.positions("np.mult_feed_events", "dev.svc"),
            "re:p:{np.mult_feed_events}:dev.svc"
        );
        assert_eq!(
            k.positions("np.mult_feed_events", "tst.svc"),
            "re:p:{np.mult_feed_events}:tst.svc"
        );
    }
}
