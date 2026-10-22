//! journald-agent: ship systemd journal entries to an Apache Iggy topic.
//!
//! Correctness invariant: a journal cursor is only persisted *after* Iggy has
//! acked the batch containing that entry. On restart we resume from the last
//! persisted cursor, giving at-least-once delivery (a crash between ack and
//! persist replays the last batch).

mod config;
mod journal;
mod shipper;
mod state;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, watch};

use config::{Config, StartAt};
use journal::{Entry, JournalReader};
use shipper::Shipper;
use state::CursorStore;

const DEFAULT_CONFIG: &str = "/etc/rave/journald-agent.toml";
const CHANNEL_CAPACITY: usize = 10_000;
const READER_WAIT: Duration = Duration::from_secs(1);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG));
    let cfg = Config::load(&config_path)?;

    let store = CursorStore::new(cfg.state.path.clone());
    let stored_cursor = store.load().context("loading stored cursor")?;
    tracing::info!(
        resuming = stored_cursor.is_some(),
        start_at = ?cfg.journal.start_at,
        "journald-agent starting"
    );

    let (tx, rx) = mpsc::channel::<Entry>(CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Journal reading is blocking; run it on a dedicated OS thread.
    let reader_handle = {
        let start_at = cfg.journal.start_at;
        let exclude = cfg.journal.exclude_units.clone();
        let shutdown_rx = shutdown_rx.clone();
        std::thread::Builder::new()
            .name("journal-reader".into())
            .spawn(move || reader_loop(start_at, stored_cursor, exclude, tx, shutdown_rx))
            .context("spawning journal reader thread")?
    };

    // Translate OS signals into a shutdown broadcast.
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("shutdown signal received; draining");
        let _ = shutdown_tx.send(true);
    });

    let shipper = Shipper::connect(
        &cfg.iggy,
        store,
        cfg.batch.clone(),
        cfg.backoff.clone(),
    )
    .await?;

    shipper.run(rx, shutdown_rx).await?;

    // Shipper has stopped; the reader will observe shutdown (or the closed
    // channel) and exit. Join it so we don't leak the thread.
    if let Err(e) = reader_handle.join() {
        tracing::error!(?e, "journal reader thread panicked");
    }
    tracing::info!("journald-agent stopped");
    Ok(())
}

/// Blocking loop: read journal entries and hand them to the shipper.
///
/// Backpressure is intentional: when Iggy is down the shipper stops draining
/// the channel, `blocking_send` blocks, and the journal's own on-disk storage
/// becomes the buffer until Iggy recovers.
fn reader_loop(
    start_at: StartAt,
    stored_cursor: Option<String>,
    exclude_units: Vec<String>,
    tx: mpsc::Sender<Entry>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let mut reader =
        match JournalReader::open(start_at, stored_cursor.as_deref(), exclude_units) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "failed to open journal; shutting down");
                return; // dropping tx closes the channel and stops the shipper
            }
        };

    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        match reader.read_next(READER_WAIT) {
            Ok(Some(entry)) => {
                if reader.is_excluded(&entry) {
                    continue;
                }
                if tx.blocking_send(entry).is_err() {
                    break; // shipper gone
                }
            }
            Ok(None) => {} // idle timeout; loop to re-check shutdown
            Err(e) => {
                tracing::error!(error = %e, "journal read error; retrying");
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
    tracing::debug!("journal reader thread exiting");
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to install SIGTERM handler");
            // Fall back to ctrl_c only.
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
