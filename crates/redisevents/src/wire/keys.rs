//! [`Keys`], the Redis key layout (`WIRE.md` §1–2). **API: W0.2. Bodies: A1** (port of
//! `Config/KeyNamespace.cs` and `Wire/StreamKeys.cs`).
//!
//! Unlike C#, the prefix is a value rather than a process-wide static: [`Keys::from_env`] resolves it
//! by the Rust rule (which differs from C# when nothing is configured), and tests use [`Keys::with_prefix`] instead of setting environment variables.

use std::sync::Arc;

const OVERRIDE_VARIABLE: &str = "STREAMS_KEY_PREFIX";
const ENVIRONMENT_VARIABLE: &str = "STREAMS_ENVIRONMENT";
/// Used when neither variable is set. Deliberately not `production`; C# defaults to `production`.
const DEFAULT_ENVIRONMENT: &str = "local";
const MARKER: &str = "re";

/// Builds every key the library touches, under one prefix. Cheap to clone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Keys {
    prefix: Arc<str>,
}

impl Keys {
    /// The prefix from the environment: `STREAMS_KEY_PREFIX` verbatim when set and not blank;
    /// otherwise `"<env>:re:"` with `<env>` the lowercased `STREAMS_ENVIRONMENT`; otherwise
    /// `"local:re:"`. `ASPNETCORE_ENVIRONMENT` and `DOTNET_ENVIRONMENT` are never read, and there
    /// is no `production` default (C# has both), so a cross-language deployment must set
    /// `STREAMS_KEY_PREFIX` on both sides. Logs the result at info, and the fallback with the
    /// variable names.
    #[must_use]
    pub fn from_env() -> Self {
        let keys = Self::from_lookup(|name| std::env::var(name).ok());
        if Self::is_fallback(|name| std::env::var(name).ok()) {
            tracing::info!(
                prefix = keys.prefix(),
                "no {OVERRIDE_VARIABLE} or {ENVIRONMENT_VARIABLE} set: using the default key prefix `{}`",
                keys.prefix()
            );
        } else {
            tracing::info!(prefix = keys.prefix(), "redis key prefix resolved");
        }
        keys
    }

    fn is_fallback(var: impl Fn(&str) -> Option<String>) -> bool {
        var(OVERRIDE_VARIABLE).is_none_or(|p| p.trim().is_empty())
            && var(ENVIRONMENT_VARIABLE).is_none_or(|e| e.trim().is_empty())
    }

    /// Resolves the prefix from `var` instead of the process environment (for tests).
    #[must_use]
    pub fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Self {
        if let Some(prefix) = var(OVERRIDE_VARIABLE).filter(|p| !p.trim().is_empty()) {
            return Self::with_prefix(prefix);
        }
        let environment = var(ENVIRONMENT_VARIABLE)
            .filter(|e| !e.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_ENVIRONMENT.to_owned());
        Self::with_prefix(format!("{}:{MARKER}:", environment.trim().to_lowercase()))
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
    fn key_prefix_is_used_verbatim_and_wins() {
        let k = Keys::from_lookup(lookup(&[
            ("STREAMS_KEY_PREFIX", "custom:"),
            ("STREAMS_ENVIRONMENT", "Staging"),
        ]));
        assert_eq!(k.prefix(), "custom:");
    }

    #[test]
    fn blank_key_prefix_is_ignored() {
        let k = Keys::from_lookup(lookup(&[
            ("STREAMS_KEY_PREFIX", "   "),
            ("STREAMS_ENVIRONMENT", "QA"),
        ]));
        assert_eq!(k.prefix(), "qa:re:");
    }

    #[test]
    fn streams_environment_is_lowercased() {
        let k = Keys::from_lookup(lookup(&[("STREAMS_ENVIRONMENT", "Staging")]));
        assert_eq!(k.prefix(), "staging:re:");
    }

    #[test]
    fn nothing_set_is_local_not_production() {
        let k = Keys::from_lookup(lookup(&[]));
        assert_eq!(k.prefix(), "local:re:");
        assert!(Keys::is_fallback(lookup(&[])));
        assert!(!Keys::is_fallback(lookup(&[("STREAMS_ENVIRONMENT", "x")])));
        assert!(!Keys::is_fallback(lookup(&[("STREAMS_KEY_PREFIX", "x:")])));
    }

    #[test]
    fn dotnet_variables_are_ignored() {
        let k = Keys::from_lookup(lookup(&[
            ("ASPNETCORE_ENVIRONMENT", "Development"),
            ("DOTNET_ENVIRONMENT", "QA"),
        ]));
        assert_eq!(k.prefix(), "local:re:");
        let k = Keys::from_lookup(lookup(&[
            ("ASPNETCORE_ENVIRONMENT", "Development"),
            ("STREAMS_ENVIRONMENT", "prod"),
        ]));
        assert_eq!(k.prefix(), "prod:re:");
    }
}
