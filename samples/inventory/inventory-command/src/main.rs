//! The Inventory write-side service. Reads `REDIS_URL` (default `redis://127.0.0.1:6379`) and
//! `PORT` (default 8080). Exits on SIGTERM or Ctrl+C after finishing in-flight requests.

use std::error::Error;
use std::time::Duration;

use inventory_command::{TOPIC, repository, router};
use redisevents::Connection;
use redisevents::admin::meta::ensure_topic_meta;
use redisevents::config::TopicOptions;
use redisevents::producer::StreamStore;
use redisevents::wire::Keys;

const DEFAULT_PORT: u16 = 8080;
const CONNECT_ATTEMPTS: u32 = 30;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let port = match std::env::var("PORT") {
        Ok(port) => port.parse()?,
        Err(_) => DEFAULT_PORT,
    };

    let conn = connect_with_retry(&url).await?;
    let keys = Keys::from_env();
    let options = TopicOptions::default();
    // A writer-only service ensures the topic meta at startup (as C# does).
    ensure_topic_meta(&conn, &keys, TOPIC, &options).await?;
    let store = StreamStore::new(conn, keys, TOPIC, options)?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    axum::serve(listener, router(repository(store)))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Connects, retrying so the service can start before Redis is up (compose has no health gate).
async fn connect_with_retry(url: &str) -> Result<Connection, redisevents::Error> {
    let mut attempt = 1;
    loop {
        match Connection::connect(url).await {
            Ok(conn) => return Ok(conn),
            Err(e) if attempt < CONNECT_ATTEMPTS => {
                eprintln!("inventory-command: Redis not ready ({e}), retrying");
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Resolves on SIGTERM (container stop) or Ctrl+C.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install the Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install the SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
