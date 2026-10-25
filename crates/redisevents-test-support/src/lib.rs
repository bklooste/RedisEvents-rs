//! Test support for redisevents integration tests (port of `RedisEvents.TestSupport`).
//!
//! - [`redis()`] gives a test a Redis: `REDIS_URL` when set (CI runs `redis:8-alpine` as a service),
//!   otherwise a `redis:8-alpine` container through testcontainers. When neither is available it
//!   returns `None`, and [`require_redis!`] skips the test with a message. Set
//!   `REDISEVENTS_REQUIRE_REDIS=1` to make that a failure instead.
//! - [`RecordingHandler`] records what a consumer delivers and lets a test wait for it
//!   (C# `TestHandler`).
//! - [`wait_until`] / [`wait_until_async`] poll a condition with a short back-off.
//!
//! The container is started once per test binary and is **reused across runs** (named
//! `redisevents-test-redis`, testcontainers `ReuseDirective::Always`), so repeated local runs do not
//! leak containers and do not pay the start-up each time. Tests isolate themselves with
//! [`RedisFixture::new_topic`] / [`RedisFixture::new_consumer`], never by flushing.
//!
//! Each `#[tokio::test]` has its own runtime, and a redis-rs connection is tied to the runtime that
//! opened it, so the fixture hands out a URL and fresh connections ([`RedisFixture::connect`]),
//! never a shared connection.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use redisevents::Connection;
use redisevents::wire::Keys;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, ReuseDirective};
use tokio::sync::OnceCell;

mod recording;

pub use recording::RecordingHandler;

/// The image used when `REDIS_URL` is not set. Redis 7.4+ is required (`HEXPIRE`).
pub const IMAGE: (&str, &str) = ("redis", "8-alpine");

const CONTAINER_NAME: &str = "redisevents-test-redis";
const REDIS_PORT: u16 = 6379;

static FIXTURE: OnceCell<Option<RedisFixture>> = OnceCell::const_new();
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A Redis to test against. Obtain with [`redis()`].
#[derive(Debug)]
pub struct RedisFixture {
    url: String,
    source: Source,
    _container: Option<ContainerAsync<GenericImage>>,
}

/// Where the fixture's Redis came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// The `REDIS_URL` environment variable.
    Env,
    /// A testcontainers `redis:8-alpine` container.
    Container,
}

/// The shared Redis for this test binary, or `None` when no Redis is available (see the crate docs).
///
/// # Panics
/// When no Redis is available and `REDISEVENTS_REQUIRE_REDIS` is set.
pub async fn redis() -> Option<&'static RedisFixture> {
    let fixture = FIXTURE.get_or_init(RedisFixture::start).await.as_ref();
    assert!(
        fixture.is_some() || std::env::var_os("REDISEVENTS_REQUIRE_REDIS").is_none(),
        "REDISEVENTS_REQUIRE_REDIS is set but no Redis is available: set REDIS_URL or start Docker"
    );
    fixture
}

/// Binds a [`RedisFixture`] or returns from the test early, printing why it was skipped.
///
/// ```no_run
/// #[tokio::test]
/// async fn ping() {
///     let redis = redisevents_test_support::require_redis!();
///     let conn = redis.connect().await;
///     # let _ = conn;
/// }
/// ```
#[macro_export]
macro_rules! require_redis {
    () => {
        match $crate::redis().await {
            Some(fixture) => fixture,
            None => {
                eprintln!(
                    "skipping {}: no Redis (set REDIS_URL or start Docker)",
                    module_path!()
                );
                return;
            }
        }
    };
}

impl RedisFixture {
    async fn start() -> Option<Self> {
        if let Some(url) = std::env::var("REDIS_URL")
            .ok()
            .filter(|u| !u.trim().is_empty())
        {
            return Some(Self {
                url,
                source: Source::Env,
                _container: None,
            });
        }
        let started = GenericImage::new(IMAGE.0, IMAGE.1)
            .with_exposed_port(REDIS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_container_name(CONTAINER_NAME)
            .with_reuse(ReuseDirective::Always)
            .start()
            .await;
        let container = match started {
            Ok(c) => c,
            Err(e) => {
                eprintln!("redisevents-test-support: could not start {IMAGE:?}: {e}");
                return None;
            }
        };
        let host = container.get_host().await.ok()?;
        let port = container.get_host_port_ipv4(REDIS_PORT).await.ok()?;
        Some(Self {
            url: format!("redis://{host}:{port}"),
            source: Source::Container,
            _container: Some(container),
        })
    }

    /// The redis-rs URL, for example `redis://localhost:6379`.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Where this Redis came from.
    #[must_use]
    pub fn source(&self) -> Source {
        self.source
    }

    /// A new [`Connection`] on the calling test's runtime.
    ///
    /// # Panics
    /// When Redis cannot be reached.
    pub async fn connect(&self) -> Connection {
        Connection::connect(&self.url)
            .await
            .unwrap_or_else(|e| panic!("connect to {}: {e}", self.url))
    }

    /// A raw redis-rs client, for assertions that go around the library.
    ///
    /// # Panics
    /// When the URL does not parse.
    #[must_use]
    pub fn client(&self) -> redis::Client {
        redis::Client::open(self.url.as_str()).expect("valid REDIS_URL")
    }

    /// Keys under a fixed test prefix (`test:re:`), independent of the environment.
    #[must_use]
    pub fn keys(&self) -> Keys {
        Keys::with_prefix("test:re:")
    }

    /// A topic name unique to this run: `<hint>-<n>-<random>`, sanitised to `[a-z0-9-]`.
    #[must_use]
    pub fn new_topic(&self, hint: &str) -> String {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}-{n}-{}", sanitise(hint, "topic"), short_id())
    }

    /// A consumer name unique to this run: `<hint>-<random>`.
    #[must_use]
    pub fn new_consumer(&self, hint: &str) -> String {
        format!("{}-{}", sanitise(hint, "consumer"), short_id())
    }

    /// `FLUSHALL`. Only for tests that own the whole server; prefer unique topics.
    ///
    /// # Panics
    /// When the command fails.
    pub async fn flush_all(&self) {
        let mut conn = self.connect().await.shared();
        redis::cmd("FLUSHALL")
            .query_async::<()>(&mut conn)
            .await
            .expect("FLUSHALL");
    }
}

fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..6].to_owned()
}

fn sanitise(text: &str, fallback: &str) -> String {
    let cleaned: String = text
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let cleaned = cleaned.trim_matches('-');
    match cleaned.len() {
        0 => fallback.to_owned(),
        n if n <= 40 => cleaned.to_owned(),
        _ => cleaned[..40].to_owned(),
    }
}

/// Polls `condition` until it holds, starting at 2 ms and backing off to ~32 ms.
///
/// # Panics
/// When `timeout` (default 30 s) passes first; the message names `because`.
pub async fn wait_until(
    mut condition: impl FnMut() -> bool,
    timeout: Option<Duration>,
    because: &str,
) {
    wait_until_async(|| std::future::ready(condition()), timeout, because).await;
}

/// [`wait_until`] with an async condition.
///
/// # Panics
/// As [`wait_until`].
pub async fn wait_until_async<F: Future<Output = bool>>(
    mut condition: impl FnMut() -> F,
    timeout: Option<Duration>,
    because: &str,
) {
    let limit = timeout.unwrap_or(Duration::from_secs(30));
    let deadline = Instant::now() + limit;
    let mut wait = Duration::from_millis(2);
    while !condition().await {
        assert!(
            Instant::now() < deadline,
            "timed out after {limit:?} waiting for {because}"
        );
        tokio::time::sleep(wait).await;
        if wait < Duration::from_millis(25) {
            wait *= 2;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitises_names() {
        assert_eq!(sanitise("My Test::Case", "topic"), "my-test--case");
        assert_eq!(sanitise("::", "topic"), "topic");
        assert_eq!(sanitise(&"a".repeat(50), "topic").len(), 40);
    }
}
