//! Client and thread names (C# `StreamNames`). Every component is sanitised to `[A-Za-z0-9._:-]`
//! (Redis `CLIENT SETNAME` rejects spaces and control characters) and the whole name is capped at
//! 256 bytes.

/// Byte cap on a produced name.
const MAX_NAME_LENGTH: usize = 256;

/// Thread name for a reader connection: `streams-rd:{consumer}:i{index}`.
pub(crate) fn reader_thread_name(consumer: &str, index: u32) -> String {
    truncate(format!("streams-rd:{}:i{index}", sanitise(consumer)))
}

/// Client name for a dedicated reader: `streams-rd:{service}:{consumer}:i{index}:{pod}`.
pub(crate) fn reader_client_name(service: &str, consumer: &str, index: u32, pod: &str) -> String {
    truncate(format!(
        "streams-rd:{}:{}:i{index}:{}",
        sanitise(service),
        sanitise(consumer),
        sanitise(pod)
    ))
}

/// The service name in client names (C# `Assembly.GetEntryAssembly().GetName().Name`): the
/// running executable's file stem, or `unknown`.
pub(crate) fn service_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "unknown".to_owned())
}

/// Client name for the shared (admin and write) connection: `streams:{service}:{pod}`.
pub(crate) fn shared_client_name(service: &str, pod: &str) -> String {
    truncate(format!("streams:{}:{}", sanitise(service), sanitise(pod)))
}

/// Task name for a consumer host: `streams-host:{consumer}`.
pub(crate) fn consumer_host_task_name(consumer: &str) -> String {
    truncate(format!("streams-host:{}", sanitise(consumer)))
}

/// Replaces every character outside `[A-Za-z0-9._:-]` with `-`; empty input becomes `unknown`.
fn sanitise(input: &str) -> String {
    if input.is_empty() {
        return "unknown".to_owned();
    }
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Caps at [`MAX_NAME_LENGTH`] bytes. The name is ASCII after sanitising, so the cut is on a char
/// boundary.
fn truncate(mut name: String) -> String {
    name.truncate(MAX_NAME_LENGTH);
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsafe_characters_become_dashes_and_empty_becomes_unknown() {
        assert_eq!(
            shared_client_name("my service", ""),
            "streams:my-service:unknown"
        );
        assert_eq!(consumer_host_task_name("a\nb"), "streams-host:a-b");
    }

    #[test]
    fn names_are_capped_at_256_bytes() {
        let long = "x".repeat(400);
        assert_eq!(consumer_host_task_name(&long).len(), MAX_NAME_LENGTH);
    }

    #[test]
    fn reader_names_have_their_documented_shape() {
        assert_eq!(reader_thread_name("orders", 2), "streams-rd:orders:i2");
        assert_eq!(
            reader_client_name("svc", "orders", 0, "pod-1"),
            "streams-rd:svc:orders:i0:pod-1"
        );
    }
}
