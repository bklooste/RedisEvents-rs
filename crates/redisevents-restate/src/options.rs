use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use reqwest::Url;

/// Settings for a [`RestateStartHandler`](crate::RestateStartHandler). Build with
/// [`new`](Self::new) and override fields with struct update syntax.
#[derive(Debug, Clone)]
pub struct RestateStartOptions {
    /// The Restate ingress base URL, for example `http://restate:8080`. Must be http or https.
    pub ingress: Url,
    /// The workflow's handler segment: `{ingress}/{workflow}/{key}/{handler}/send`. Default `run`.
    pub handler: String,
    /// Starts in flight at once. Default 8.
    pub max_parallelism: NonZeroUsize,
    /// Attempts per item before it counts as failed. Default 5.
    pub max_attempts: NonZeroU32,
    /// Delay before the first retry; doubles each retry. Default 200 ms.
    pub initial_retry_delay: Duration,
    /// Cap on the retry delay. Default 5 s, at least `initial_retry_delay`.
    pub max_retry_delay: Duration,
    /// Bound on one attempt, request and body. Default 10 s, above zero.
    pub attempt_timeout: Duration,
}

impl RestateStartOptions {
    /// The default handler segment.
    pub const DEFAULT_HANDLER: &'static str = "run";

    /// The defaults for the Restate at `ingress`.
    #[must_use]
    pub fn new(ingress: Url) -> Self {
        Self {
            ingress,
            handler: Self::DEFAULT_HANDLER.to_owned(),
            max_parallelism: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
            max_attempts: NonZeroU32::new(5).unwrap_or(NonZeroU32::MIN),
            initial_retry_delay: Duration::from_millis(200),
            max_retry_delay: Duration::from_secs(5),
            attempt_timeout: Duration::from_secs(10),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), RestateConfigError> {
        let bad = |m: String| Err(RestateConfigError(m));
        if !matches!(self.ingress.scheme(), "http" | "https") {
            return bad(format!(
                "Restate: the ingress address '{}' must be an http or https URL",
                self.ingress
            ));
        }
        if self.handler.trim().is_empty() {
            return bad("Restate: handler must not be blank".to_owned());
        }
        if self.max_retry_delay < self.initial_retry_delay {
            return bad(
                "Restate: max_retry_delay must not be below initial_retry_delay".to_owned(),
            );
        }
        if self.attempt_timeout.is_zero() {
            return bad("Restate: attempt_timeout must be above zero".to_owned());
        }
        Ok(())
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
