//! StackExchange.Redis connection strings, for deployments that share one Redis endpoint setting
//! between the C# library and this one.
//!
//! The Rust library connects with a `redis://` / `rediss://` URL
//! ([`StreamOptions::redis_url`](crate::config::StreamOptions::redis_url)); this module is the
//! explicit, opt-in bridge for a value written in the C# client's format:
//!
//! ```
//! let url = redisevents::connection::stackexchange::to_redis_url(
//!     "cache:6380,password=secret,ssl=true,defaultDatabase=2",
//! )?;
//! assert_eq!(url, "rediss://:secret@cache:6380/2");
//! # Ok::<(), redisevents::Error>(())
//! ```

use std::fmt::Write as _;

use crate::error::{Error, Result};

/// The default Redis port when an endpoint omits one.
const DEFAULT_PORT: u16 = 6379;

/// Keys StackExchange.Redis accepts that tune its own client and have no effect here. They are
/// accepted and ignored; anything else is refused rather than silently dropped.
const IGNORED_KEYS: &[&str] = &[
    "abortconnect",
    "connecttimeout",
    "connectretry",
    "keepalive",
    "allowadmin",
    "clientname",
    "synctimeout",
];

/// The string with every `password=` value masked, for error messages.
fn redact(connection_string: &str) -> String {
    connection_string
        .split(',')
        .map(|part| match part.split_once('=') {
            Some((key, _)) if key.trim().eq_ignore_ascii_case("password") => format!("{key}=***"),
            _ => part.to_owned(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Translates a StackExchange.Redis connection string (`host:port,password=…,ssl=true,…`) into a
/// redis URL for [`Connection::connect`](crate::Connection::connect).
///
/// Supported keys (case-insensitive, as StackExchange.Redis reads them): `password`, `user`, `ssl`,
/// `defaultDatabase`, and the accepted-but-ignored client tuning keys (`abortConnect`,
/// `connectTimeout`, `connectRetry`, `keepAlive`, `allowAdmin`, `clientName`, `syncTimeout`).
/// Other keys are refused. A single endpoint is supported: cluster and sentinel are not wired.
///
/// # Errors
///
/// [`Error::Config`] when the string does not parse. The message never echoes the password.
pub fn to_redis_url(connection_string: &str) -> Result<String> {
    let parsed = parse_connection_string(connection_string)?;
    let scheme = if parsed.tls { "rediss" } else { "redis" };
    let mut url = format!("{scheme}://");
    match (&parsed.user, &parsed.password) {
        (Some(user), Some(password)) => {
            let _ = write!(
                url,
                "{}:{}@",
                percent_encode(user),
                percent_encode(password)
            );
        }
        (None, Some(password)) => {
            let _ = write!(url, ":{}@", percent_encode(password));
        }
        (Some(user), None) => {
            let _ = write!(url, "{}@", percent_encode(user));
        }
        (None, None) => {}
    }
    let _ = write!(url, "{}/{}", parsed.host_port, parsed.database.unwrap_or(0));
    Ok(url)
}

/// What the connection string said, before it is rendered as a URL.
#[derive(Debug, Default)]
struct ParsedConnection {
    host_port: String,
    user: Option<String>,
    password: Option<String>,
    tls: bool,
    database: Option<u32>,
}

fn parse_connection_string(connection_string: &str) -> Result<ParsedConnection> {
    parse_inner(connection_string).map_err(|detail| {
        Error::config(format!(
            "'{}' is not a valid StackExchange.Redis connection string: {detail}",
            redact(connection_string)
        ))
    })
}

fn parse_inner(connection_string: &str) -> std::result::Result<ParsedConnection, String> {
    let mut parsed = ParsedConnection::default();
    let mut endpoints = Vec::new();

    for part in connection_string
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
    {
        let Some((raw_key, value)) = part.split_once('=') else {
            endpoints.push(part);
            continue;
        };
        let key = raw_key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "password" => parsed.password = Some(value.to_owned()),
            "user" => parsed.user = Some(value.to_owned()),
            "ssl" => {
                parsed.tls = match value.to_ascii_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(format!("ssl must be true or false, not '{value}'")),
                };
            }
            "defaultdatabase" => {
                parsed.database = Some(value.parse::<u32>().map_err(|_| {
                    format!("defaultDatabase must be a non-negative integer, not '{value}'")
                })?);
            }
            k if IGNORED_KEYS.contains(&k) => {}
            _ => return Err(format!("unsupported option '{}'", raw_key.trim())),
        }
    }

    match endpoints.as_slice() {
        [] => return Err("no endpoint (expected host:port)".to_owned()),
        [one] => parsed.host_port = normalise_endpoint(one)?,
        _ => {
            return Err(
                "more than one endpoint; only a single node is supported (cluster and sentinel are not wired)".to_owned(),
            );
        }
    }
    Ok(parsed)
}

/// `host`, `host:port`, `[v6]` or `[v6]:port`, rendered with brackets where needed and the default
/// port filled in.
fn normalise_endpoint(endpoint: &str) -> std::result::Result<String, String> {
    let (host, port) = if let Some(rest) = endpoint.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .ok_or_else(|| format!("unclosed '[' in endpoint '{endpoint}'"))?;
        let port =
            match after {
                "" => None,
                other => Some(other.strip_prefix(':').ok_or_else(|| {
                    format!("unexpected text after ']' in endpoint '{endpoint}'")
                })?),
            };
        (format!("[{host}]"), port)
    } else {
        match endpoint.matches(':').count() {
            0 => (endpoint.to_owned(), None),
            1 => {
                let (host, port) = endpoint.split_once(':').unwrap_or((endpoint, ""));
                (host.to_owned(), Some(port))
            }
            _ => {
                return Err(format!(
                    "IPv6 endpoints must be bracketed, e.g. [::1]:6379: '{endpoint}'"
                ));
            }
        }
    };

    if host.is_empty() || host == "[]" {
        return Err(format!("endpoint '{endpoint}' has no host"));
    }

    let port = match port {
        None => DEFAULT_PORT,
        Some(text) => text
            .parse::<u16>()
            .map_err(|_| format!("port '{text}' is not a valid port number"))?,
    };
    Ok(format!("{host}:{port}"))
}

/// Percent-encodes everything outside the RFC 3986 unreserved set, so a password with `@`, `:` or
/// `/` survives the URL.
fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url_of(connection_string: &str) -> String {
        to_redis_url(connection_string).expect("translates")
    }

    #[test]
    fn endpoint_without_port_gets_the_default_port() {
        assert_eq!(url_of("cache"), "redis://cache:6379/0");
        assert_eq!(url_of("cache:6380"), "redis://cache:6380/0");
    }

    #[test]
    fn bracketed_ipv6_endpoint_is_kept_bracketed() {
        assert_eq!(url_of("[::1]:6380"), "redis://[::1]:6380/0");
        assert_eq!(url_of("[::1]"), "redis://[::1]:6379/0");
    }

    #[test]
    fn unbracketed_ipv6_endpoint_is_refused() {
        let err = to_redis_url("::1:6379").expect_err("needs brackets");
        assert!(
            err.to_string()
                .contains("StackExchange.Redis connection string"),
            "{err}"
        );
    }

    #[test]
    fn password_is_carried_as_the_redis_password() {
        assert_eq!(
            url_of("localhost:6380,password=s3cret"),
            "redis://:s3cret@localhost:6380/0"
        );
    }

    #[test]
    fn user_and_password_are_both_carried() {
        assert_eq!(
            url_of("localhost:6380,user=bob,password=p"),
            "redis://bob:p@localhost:6380/0"
        );
    }

    #[test]
    fn ssl_true_selects_rediss() {
        assert_eq!(
            url_of("localhost:6380,ssl=true"),
            "rediss://localhost:6380/0"
        );
        assert_eq!(
            url_of("localhost:6380,ssl=false"),
            "redis://localhost:6380/0"
        );
    }

    #[test]
    fn default_database_becomes_the_url_path() {
        assert_eq!(
            url_of("localhost:6379,defaultDatabase=3"),
            "redis://localhost:6379/3"
        );
    }

    #[test]
    fn keys_are_case_insensitive_and_whitespace_tolerant() {
        assert_eq!(
            url_of(" localhost:6379 , SSL = TRUE "),
            "rediss://localhost:6379/0"
        );
    }

    #[test]
    fn ignored_stackexchange_keys_do_not_change_the_url() {
        assert_eq!(
            url_of(
                "localhost:6379,abortConnect=false,syncTimeout=9000,connectTimeout=3000,clientName=svc"
            ),
            "redis://localhost:6379/0"
        );
    }

    #[test]
    fn special_characters_in_the_password_are_percent_encoded_and_round_trip() {
        let password = "p@ss:w/rd #%~ü";
        let url = url_of(&format!("localhost:6379,password={password}"));

        let info = redis::IntoConnectionInfo::into_connection_info(url.as_str())
            .expect("the URL parses in redis-rs");
        assert_eq!(info.redis_settings().password(), Some(password));
    }

    #[test]
    fn unsupported_key_is_refused_naming_the_key() {
        let err = to_redis_url("localhost:6379,bogusOption=1").expect_err("unknown key");
        let message = err.to_string();
        assert!(
            message.contains("StackExchange.Redis connection string"),
            "{message}"
        );
        assert!(message.contains("bogusOption"), "{message}");
    }

    #[test]
    fn multiple_endpoints_are_refused() {
        let err = to_redis_url("a:6379,b:6379").expect_err("single node only");
        assert!(err.to_string().contains("more than one endpoint"));
    }

    #[test]
    fn missing_endpoint_is_refused() {
        assert!(to_redis_url("password=x").is_err());
    }

    #[test]
    fn bad_port_is_refused() {
        assert!(to_redis_url("localhost:notaport").is_err());
    }

    #[test]
    fn error_message_never_echoes_the_password() {
        let err = to_redis_url("localhost:notaport,password=hunter2").expect_err("bad port");
        assert!(!err.to_string().contains("hunter2"));
    }

    #[test]
    fn sync_timeout_is_accepted_and_ignored() {
        assert_eq!(
            url_of("localhost:6379,syncTimeout=9000"),
            "redis://localhost:6379/0"
        );
    }
}
