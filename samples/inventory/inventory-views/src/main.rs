//! The inventory read-side service: starts the consumer host on the `inventory` topic, then serves
//! the views over HTTP. Configuration: `REDIS_URL` (default `redis://127.0.0.1:6379`), `PORT`
//! (default 8080). SIGTERM or Ctrl-C shuts down: HTTP drains first, then the consumer.

use std::env;
use std::error::Error;

use inventory_domain::TOPIC;
use inventory_views::{CONSUMER, Views, projector, router};
use redisevents::Connection;
use redisevents::consumer::host::Streams;
use redisevents::wire::Keys;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt().init();

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let port: u16 = env::var("PORT").map_or(Ok(8080), |p| p.parse())?;

    let conn = Connection::connect(&redis_url).await?;
    let keys = Keys::from_env();
    let views = Views::new(&conn, &keys, TOPIC)?;
    let host = Streams::builder(conn)
        .keys(keys)
        .topic(TOPIC)
        .consumer(CONSUMER)
        .handler(projector(&views, TOPIC)?)
        .spawn()
        .await?;

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "inventory-views listening");
    axum::serve(listener, router(&views))
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("http stopped, stopping the consumer");
    host.shutdown().await?;
    Ok(())
}

async fn shutdown_signal() {
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
