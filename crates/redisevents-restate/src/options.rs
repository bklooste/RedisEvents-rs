use std::fmt;
use std::time::Duration;

use reqwest::Url;

/// Settings for a [`RestateStartHandler`](crate::RestateStartHandler) (C# `RestateStartOptions`).
#[derive(Debug, Clone)]
pub struct RestateStartOptions {
    /// The Restate ingress base URL, for example `http://restate:8080`. Required (reqwest has no
    /// client base address).
    pub ingress: Option<Url>,
    /// The workflow's handler segment: `{ingress}/{workflow}/{key}/{handler}/send`. Default `run`.
    pub handler: String,
    /// Starts in flight at once. Default 8, at least 1.
    pub max_parallelism: usize,
    /// Attempts per item before it counts as failed. Default 5, at least 1.
    pub max_attempts: u32,
    /// Delay before the first retry; doubles each retry. Default 200 ms.
    pub initial_retry_delay: Duration,
    /// Cap on the retry delay. Default 5 s, at least `initial_retry_delay`.
    pub max_retry_delay: Duration,
    /// Bound on one attempt, request and body. Default 10 s, above zero.
    pub attempt_timeout: Duration,
    /// Names this handler in logs and metrics tags; defaults to `RestateStartHandler`.
    pub name: Option<String>,
}

impl RestateStartOptions {
    /// The default handler segment.
    pub const DEFAULT_HANDLER: &'static str = "run";

    /// The defaults with `ingress` set.
    #[must_use]
    pub fn new(ingress: Url) -> Self {
        Self {
            ingress: Some(ingress),
            ..Self::default()
        }
    }

    pub(crate) fn validate(&self) -> Result<(), RestateConfigError> {
        let bad = |m: &str| Err(RestateConfigError(m.to_owned()));
        if self.handler.trim().is_empty() {
            return bad("Restate: handler must not be blank");
        }
        if self.max_parallelism < 1 {
            return bad("Restate: max_parallelism must be at least 1");
        }
        if self.max_attempts < 1 {
            return bad("Restate: max_attempts must be at least 1");
        }
        if self.max_retry_delay < self.initial_retry_delay {
            return bad("Restate: max_retry_delay must not be below initial_retry_delay");
        }
        if self.attempt_timeout.is_zero() {
            return bad("Restate: attempt_timeout must be above zero");
        }
        Ok(())
    }
}

impl Default for RestateStartOptions {
    fn default() -> Self {
        Self {
            ingress: None,
            handler: Self::DEFAULT_HANDLER.to_owned(),
            max_parallelism: 8,
            max_attempts: 5,
            initial_retry_delay: Duration::from_millis(200),
            max_retry_delay: Duration::from_secs(5),
            attempt_timeout: Duration::from_secs(10),
            name: None,
        }
    }
}

/// The handler was constructed with unusable settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestateConfigError(pub(crate) String);

impl fmt::Display for RestateConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RestateConfigError {}
