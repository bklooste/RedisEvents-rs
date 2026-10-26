use std::fmt;

/// At least one kept message was not started, so the batch must not be acked. Returned inside `HandleError::Block`; recover it with
/// `err.into_error().downcast::<RestateStartBlocked>()`.
#[derive(Debug, Clone)]
pub struct RestateStartBlocked {
    failed_keys: Vec<String>,
    kept: usize,
    shutting_down: bool,
    message: String,
}

impl RestateStartBlocked {
    /// At most this many failures are named in the message.
    pub const MAX_NAMED_IN_MESSAGE: usize = 20;

    pub(crate) fn new(
        failed_keys: Vec<String>,
        kept: usize,
        reasons: &[String],
        shutting_down: bool,
    ) -> Self {
        let message = build_message(&failed_keys, kept, reasons, shutting_down);
        Self {
            failed_keys,
            kept,
            shutting_down,
            message,
        }
    }

    /// `workflow/key` of each failed start (`stream id N-M` when the message never got a key).
    #[must_use]
    pub fn failed_keys(&self) -> &[String] {
        &self.failed_keys
    }

    /// How many messages the filter kept in the batch.
    #[must_use]
    pub fn kept(&self) -> usize {
        self.kept
    }

    /// `true` when the handler's cancellation token was cancelled: a shutdown, not an outage.
    #[must_use]
    pub fn shutting_down(&self) -> bool {
        self.shutting_down
    }
}

fn build_message(keys: &[String], kept: usize, reasons: &[String], shutting_down: bool) -> String {
    use std::fmt::Write;
    let mut s = format!(
        "Restate: {} of {} kept start(s) were not accepted",
        keys.len(),
        kept
    );
    if shutting_down {
        s.push_str(" before the handler's token was cancelled");
    }
    s.push_str("; the batch is not acked and is retried whole (accepted starts dedupe on their workflow keys). Failed: ");
    let named = keys.len().min(RestateStartBlocked::MAX_NAMED_IN_MESSAGE);
    for (i, key) in keys.iter().take(named).enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(key);
        if let Some(r) = reasons.get(i).filter(|r| !r.is_empty()) {
            let _ = write!(s, " ({r})");
        }
    }
    if keys.len() > named {
        let _ = write!(s, ", and {} more", keys.len() - named);
    }
    s.push('.');
    s
}

impl fmt::Display for RestateStartBlocked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RestateStartBlocked {}

#[cfg(test)]
mod tests {
    use super::RestateStartBlocked;

    #[test]
    fn the_blocked_error_names_at_most_twenty_keys_then_counts_the_rest() {
        let keys: Vec<String> = (0..25).map(|i| format!("W/k{i}")).collect();
        let reasons = vec!["HTTP 503".to_owned(); 25];

        let ex = RestateStartBlocked::new(keys, 30, &reasons, false);

        let m = ex.to_string();
        assert!(m.contains("25 of 30"));
        assert!(m.contains("W/k19 (HTTP 503)"));
        assert!(!m.contains("W/k20"));
        assert!(m.contains("and 5 more"));
        assert_eq!(ex.failed_keys().len(), 25);
    }
}
