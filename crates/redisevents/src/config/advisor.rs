//! Configuration advice logged at startup (port of `Config/StreamsConfigurationAdvisor.cs`).
//! **Owner: A2.**
//!
//! The C# advisor re-runs validation with a real logger once the host exists, so the advisory lines
//! reach the log. Here the caller builds the advisor once at startup and calls
//! [`StreamsConfigurationAdvisor::log`]; the advisory lines themselves come from
//! `binder::advise`.

use tracing::Level;

use super::StreamOptions;
use super::binder;
use crate::error::Result;

/// One advisory line: informational, or a warning about a configuration that will work but is
/// almost certainly not what the operator wanted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Advisory {
    /// [`Level::WARN`] or [`Level::INFO`].
    pub level: Level,
    /// The log line, naming the `Streams:…` keys involved.
    pub message: String,
}

/// Validates the options and holds the advisory lines for them, ready to log at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsConfigurationAdvisor {
    advisories: Vec<Advisory>,
}

impl StreamsConfigurationAdvisor {
    /// Validates `options` for `environment` and `sync_timeout_ms`, then collects the advisory lines.
    ///
    /// # Errors
    ///
    /// Whatever [`binder::validate`] rejects. Validation failures are fatal at startup.
    pub fn new(options: &StreamOptions, environment: &str, sync_timeout_ms: u32) -> Result<Self> {
        binder::validate(options, environment, sync_timeout_ms)?;
        Ok(Self {
            advisories: binder::advise(options),
        })
    }

    /// The advisory lines, in the order they were found.
    #[must_use]
    pub fn advisories(&self) -> &[Advisory] {
        &self.advisories
    }

    /// Emits every advisory line through `tracing`, at its level.
    pub fn log(&self) {
        for advisory in &self.advisories {
            if advisory.level == Level::WARN {
                tracing::warn!("{}", advisory.message);
            } else {
                tracing::info!("{}", advisory.message);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::binder::bind_json;

    #[test]
    fn advisor_emits_the_advisory_lines_at_startup() {
        let options = bind_json(
            r#"{ "Streams": {
              "Topics": { "orders": { "Partitions": 8, "CoLocatePartitions": false } },
              "Consumers": [ { "Topic": "orders" } ]
            } }"#,
        )
        .unwrap();

        let advisor = StreamsConfigurationAdvisor::new(&options, "Production", 5000).unwrap();

        assert!(
            advisor.advisories().iter().any(|a| a.level == Level::WARN),
            "{:?}",
            advisor.advisories()
        );
        advisor.log();
    }

    /// C# `Advisor_WithoutALogger_DoesNothing`. Rust has no logger argument: with nothing configured
    /// there is nothing to say, and logging it is a no-op.
    #[test]
    fn advisor_without_a_logger_does_nothing() {
        let advisor =
            StreamsConfigurationAdvisor::new(&StreamOptions::default(), "Production", 5000)
                .unwrap();

        assert_eq!(advisor.advisories().len(), 0);
        advisor.log();
    }
}
