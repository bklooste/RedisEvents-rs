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

use std::borrow::Cow;
use std::sync::Arc;

const OVERRIDE_VARIABLE: &str = "STREAMS_KEY_PREFIX";
/// The prefix when `STREAMS_KEY_PREFIX` is unset or blank; the same as C#.
const DEFAULT_PREFIX: &str = "re:";
const HASH_TAG_VARIABLE: &str = "STREAMS_HASH_TAG";

/// Whether a key family wraps the topic in a `{...}` cluster hash tag, from `STREAMS_HASH_TAG`: on
/// unless the value is a recognised falsey word. The same rule and variable as C#
/// (`KeyNamespace.HashTagTopics`). Hash tags force a topic's keys to one cluster slot, which single-instance
/// Redis does not need and which breaks the `:` key hierarchy, so a single-node deployment sets it off.
#[must_use]
fn hash_tag_enabled(value: Option<&str>) -> bool {
    match value.map(str::trim) {
        None | Some("") => true,
        Some(v) => !matches!(
            v,
            "false" | "False" | "FALSE" | "0" | "no" | "No" | "NO" | "off" | "Off" | "OFF"
        ),
    }
}

/// Builds every key the library touches, under one prefix. Cheap to clone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Keys {
    prefix: Arc<str>,
    hash_tag: bool,
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

    /// Resolves the prefix and the hash-tag setting from `var` instead of the process environment (for tests).
    #[must_use]
    pub fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Self {
        let hash_tag = hash_tag_enabled(var(HASH_TAG_VARIABLE).as_deref());
        var(OVERRIDE_VARIABLE)
            .filter(|p| !p.trim().is_empty())
            .map_or_else(|| Self::with_prefix(DEFAULT_PREFIX), Self::with_prefix)
            .with_hash_tag(hash_tag)
    }

    /// Uses `prefix` verbatim, as `STREAMS_KEY_PREFIX` would. Hash tags are on, the default; use
    /// [`Keys::with_hash_tag`] to turn them off.
    #[must_use]
    pub fn with_prefix(prefix: impl Into<Arc<str>>) -> Self {
        Self {
            prefix: prefix.into(),
            hash_tag: true,
        }
    }

    /// The same keys with hash-tagging set as given: `true` wraps each topic in a `{...}` cluster hash tag,
    /// `false` leaves the bare topic, for single-instance Redis. See [`hash_tag_enabled`] and `STREAMS_HASH_TAG`.
    #[must_use]
    pub fn with_hash_tag(mut self, hash_tag: bool) -> Self {
        self.hash_tag = hash_tag;
        self
    }

    /// The resolved prefix.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Whether this [`Keys`] wraps topics in a cluster hash tag.
    #[must_use]
    pub fn hash_tag(&self) -> bool {
        self.hash_tag
    }

    /// A topic as it appears inside a key: `{topic}` when hash-tagging is on, the bare `topic` when off.
    /// A service that builds its own keys alongside a topic's streams uses this so they co-locate (or not)
    /// exactly as the library's own keys do.
    #[must_use]
    pub fn tag<'a>(&self, topic: &'a str) -> Cow<'a, str> {
        if self.hash_tag {
            Cow::Owned(format!("{{{topic}}}"))
        } else {
            Cow::Borrowed(topic)
        }
    }

    /// Partition stream: `<prefix>s:{topic}:<p>`, or `<prefix>s:topic:<p>` when `co_locate` is off or
    /// hash-tagging is off.
    #[must_use]
    pub fn stream(&self, topic: &str, partition: u32, co_locate: bool) -> String {
        if co_locate && self.hash_tag {
            format!("{}s:{{{topic}}}:{partition}", self.prefix)
        } else {
            format!("{}s:{topic}:{partition}", self.prefix)
        }
    }

    /// Positions hash: `<prefix>p:{topic}:<consumer>`.
    #[must_use]
    pub fn positions(&self, topic: &str, consumer: &str) -> String {
        format!("{}p:{}:{consumer}", self.prefix, self.tag(topic))
    }

    /// Positions meta hash: `<prefix>p:{topic}:<consumer>:meta`.
    #[must_use]
    pub fn positions_meta(&self, topic: &str, consumer: &str) -> String {
        format!("{}p:{}:{consumer}:meta", self.prefix, self.tag(topic))
    }

    /// Ownership / lease hash: `<prefix>o:{topic}:<consumer>`.
    #[must_use]
    pub fn ownership(&self, topic: &str, consumer: &str) -> String {
        format!("{}o:{}:{consumer}", self.prefix, self.tag(topic))
    }

    /// Topic meta hash: `<prefix>m:{topic}`.
    #[must_use]
    pub fn topic_meta(&self, topic: &str) -> String {
        format!("{}m:{}", self.prefix, self.tag(topic))
    }

    /// State stream (outbox / event sourcing): `<prefix>{topic}:state:<name>`.
    #[must_use]
    pub fn state(&self, topic: &str, name: &str) -> String {
        format!("{}{}:state:{name}", self.prefix, self.tag(topic))
    }

    /// View hash: `<prefix>{topic}:view:<owner>:<view_name>`.
    #[must_use]
    pub fn view(&self, topic: &str, owner: &str, view_name: &str) -> String {
        format!(
            "{}{}:view:{owner}:{view_name}",
            self.prefix,
            self.tag(topic)
        )
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
        Ok(format!("{}dedupe:{}:{id}", self.prefix, self.tag(scope)))
    }

    /// `SCAN` pattern for every positions hash of a topic: `<prefix>p:{topic}:*`.
    #[must_use]
    pub fn positions_pattern(&self, topic: &str) -> String {
        format!("{}p:{}:*", self.prefix, self.tag(topic))
    }

    /// `SCAN` pattern for every partition stream of a topic: `<prefix>s:{topic}:*`.
    #[must_use]
    pub fn stream_pattern(&self, topic: &str, co_locate: bool) -> String {
        if co_locate && self.hash_tag {
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

    #[test]
    fn hash_tag_on_by_default_wraps_every_family() {
        let k = Keys::with_prefix("re:");
        assert!(k.hash_tag());
        assert_eq!(
            k.stream("dev:bet:core-es", 0, true),
            "re:s:{dev:bet:core-es}:0"
        );
        assert_eq!(
            k.positions("dev:bet:core-es", "c"),
            "re:p:{dev:bet:core-es}:c"
        );
        assert_eq!(
            k.state("dev:bet:core-es", "Bet:42"),
            "re:{dev:bet:core-es}:state:Bet:42"
        );
        assert_eq!(
            k.view("dev:bet:core-es", "o", "v"),
            "re:{dev:bet:core-es}:view:o:v"
        );
        assert_eq!(
            k.dedupe("dev:bet:core-es", "evt-1").unwrap(),
            "re:dedupe:{dev:bet:core-es}:evt-1"
        );
    }

    #[test]
    fn hash_tag_off_drops_the_brace_from_every_family() {
        let k = Keys::from_lookup(lookup(&[("STREAMS_HASH_TAG", "false")]));
        assert!(!k.hash_tag());
        let topic = "dev:bet:core-es";
        let all = [
            k.stream(topic, 0, true),
            k.stream_pattern(topic, true),
            k.positions(topic, "c"),
            k.positions_meta(topic, "c"),
            k.positions_pattern(topic),
            k.ownership(topic, "c"),
            k.topic_meta(topic),
            k.state(topic, "Bet:42"),
            k.view(topic, "o", "v"),
            k.dedupe(topic, "evt-1").unwrap(),
            k.tag(topic).into_owned(),
        ];
        for key in &all {
            assert!(!key.contains('{') && !key.contains('}'), "brace in {key}");
        }
        assert_eq!(k.stream(topic, 0, true), "re:s:dev:bet:core-es:0");
        assert_eq!(k.state(topic, "Bet:42"), "re:dev:bet:core-es:state:Bet:42");
        assert_eq!(k.view(topic, "o", "v"), "re:dev:bet:core-es:view:o:v");
        assert_eq!(
            k.dedupe(topic, "evt-1").unwrap(),
            "re:dedupe:dev:bet:core-es:evt-1"
        );
    }
}
