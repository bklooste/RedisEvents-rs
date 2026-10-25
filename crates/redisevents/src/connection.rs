//! [`Connection`], the wrapper over redis-rs every component takes. **Owner: W0.2** (complete).
//! B6 adds `tracing` spans around it; A2 turns the C# connection string into the URL it takes.
//!
//! Two kinds of connection, because redis-rs multiplexes:
//!
//! - [`Connection::shared`]: one [`ConnectionManager`] per `Connection`, cloned freely, reconnecting
//!   on its own. Use it for ordinary commands and pipelines (publish, positions, ownership, admin).
//!   Clones share one socket, so it **must not** carry connection-state commands.
//! - [`Connection::dedicated`]: a new socket each call, owned by the caller. Required for anything
//!   that holds connection state or the socket itself: `WATCH`/`MULTI`/`EXEC` (B3, E3; on a shared
//!   connection another task's command would land inside the transaction) and blocking
//!   `XREAD … BLOCK` (C1; it would stall every other command queued behind it, and its wait
//!   exceeds the shared response timeout).

use std::fmt;
use std::time::Duration;

use redis::aio::{ConnectionManager, MultiplexedConnection};
use redis::{AsyncConnectionConfig, Client};

use crate::error::{Error, Result};

/// A Redis endpoint: a client plus one shared, auto-reconnecting connection. Cheap to clone.
#[derive(Clone)]
pub struct Connection {
    client: Client,
    shared: ConnectionManager,
}

impl Connection {
    /// Connects to `url` (a redis-rs URL such as `redis://host:6379/0`).
    ///
    /// # Errors
    /// [`Error::Config`] when `url` does not parse; [`Error::Transport`] when the first connect fails.
    pub async fn connect(url: &str) -> Result<Self> {
        let client = Client::open(url).map_err(|e| {
            Error::config(format!("Streams: '{url}' is not a valid Redis URL: {e}"))
        })?;
        Self::from_client(client).await
    }

    /// Connects to the endpoint a bound `Streams` config names: its `ConnectionString` (a
    /// StackExchange.Redis string such as `host:6379,password=...,ssl=true`, translated by
    /// [`to_redis_url`](crate::config::binder::to_redis_url)), or the C# default when unset. One
    /// config file can then serve both the C# and the Rust runtime.
    ///
    /// `ssl=true` yields a `rediss://` URL, which needs the `tls-rustls` cargo feature.
    ///
    /// # Errors
    /// [`Error::Config`] naming `Streams:ConnectionString` when it does not parse;
    /// [`Error::Transport`] when the first connect fails.
    pub async fn from_options(options: &crate::config::StreamOptions) -> Result<Self> {
        let url = Self::url_from_options(options)?;
        Self::connect(&url).await
    }

    /// The redis-rs URL [`Connection::from_options`] connects to, without connecting.
    ///
    /// # Errors
    /// [`Error::Config`] naming `Streams:ConnectionString` when it does not parse.
    pub fn url_from_options(options: &crate::config::StreamOptions) -> Result<String> {
        crate::config::binder::to_redis_url(&crate::config::binder::resolve_connection_string(
            options,
        ))
    }

    /// Wraps an existing client, opening its shared connection.
    ///
    /// # Errors
    /// [`Error::Transport`] when the connect fails.
    pub async fn from_client(client: Client) -> Result<Self> {
        use tracing::Instrument as _;

        let addr = client.get_connection_info().addr().to_string();
        let span = tracing::debug_span!("redis.connect", "server.address" = %addr);
        let shared = client
            .get_connection_manager()
            .instrument(span)
            .await
            .map_err(|e| {
                Error::transport_with(
                    format!("Streams: failed to connect to Redis at '{addr}'."),
                    e,
                )
            })?;
        Ok(Self { client, shared })
    }

    /// The shared connection (a clone of the one manager; cheap).
    #[must_use]
    pub fn shared(&self) -> ConnectionManager {
        self.shared.clone()
    }

    /// Opens a new connection for the caller's exclusive use, with **no response timeout** so a
    /// blocking read can wait as long as its `BLOCK` argument says. Callers own reconnecting it.
    ///
    /// # Errors
    /// [`Error::Redis`] when the connect fails.
    pub async fn dedicated(&self) -> Result<MultiplexedConnection> {
        self.dedicated_with(&AsyncConnectionConfig::new().set_response_timeout(None))
            .await
    }

    /// [`Connection::dedicated`] with an explicit redis-rs connection config.
    ///
    /// # Errors
    /// [`Error::Redis`] when the connect fails.
    pub async fn dedicated_with(
        &self,
        config: &AsyncConnectionConfig,
    ) -> Result<MultiplexedConnection> {
        Ok(self
            .client
            .get_multiplexed_async_connection_with_config(config)
            .await?)
    }

    /// The underlying client.
    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Opens a dedicated connection with a bounded response timeout, for short exclusive work such
    /// as one `WATCH`/`MULTI`/`EXEC` round.
    ///
    /// # Errors
    /// [`Error::Redis`] when the connect fails.
    pub async fn dedicated_with_timeout(
        &self,
        response_timeout: Duration,
    ) -> Result<MultiplexedConnection> {
        self.dedicated_with(
            &AsyncConnectionConfig::new().set_response_timeout(Some(response_timeout)),
        )
        .await
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field(
                "addr",
                &self.client.get_connection_info().addr().to_string(),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod from_options_tests {
    use super::Connection;
    use crate::config::StreamOptions;

    fn with(connection_string: &str) -> StreamOptions {
        StreamOptions {
            connection_string: Some(connection_string.to_owned()),
            ..StreamOptions::default()
        }
    }

    #[test]
    fn translates_the_csharp_connection_string() {
        let url = Connection::url_from_options(&with(
            "cache:6380,password=p@ss,ssl=true,defaultDatabase=2",
        ))
        .unwrap();
        assert_eq!(url, "rediss://:p%40ss@cache:6380/2");
    }

    #[test]
    fn unset_uses_the_csharp_default() {
        let url = Connection::url_from_options(&StreamOptions::default()).unwrap();
        assert_eq!(url, "redis://redis-db.infra:6379/0");
    }

    #[test]
    fn a_bad_string_is_a_config_error() {
        let err = Connection::url_from_options(&with("host,bogusKey=1")).unwrap_err();
        assert!(matches!(err, crate::Error::Config(_)), "{err}");
    }
}
