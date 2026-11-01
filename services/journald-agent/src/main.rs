//! journald-agent: ship systemd journal entries straight to VictoriaLogs.
//!
//! Correctness invariant: a journal cursor is only persisted *after* VictoriaLogs
//! has 2xx-acked the batch containing that entry. On restart we resume from the
//! last persisted cursor, giving at-least-once delivery (a crash between ack and
//! persist replays the last batch).

mod backoff;
mod config;
mod journal;
mod sink;
mod state;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{mpsc, watch};

use config::{BackoffConfig, BatchConfig, Config, StartAt};
use journal::{Entry, JournalReader};
use sink::{build_ndjson, Sink};
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
        account_id = cfg.victorialogs.account_id,
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

    let sink = Sink::new(&cfg.victorialogs)?;
    run(&sink, &store, &cfg.batch, &cfg.backoff, rx, shutdown_rx).await?;

    // The sink loop has stopped; the reader will observe shutdown (or the closed
    // channel) and exit. Join it so we don't leak the thread.
    if let Err(e) = reader_handle.join() {
        tracing::error!(?e, "journal reader thread panicked");
    }
    tracing::info!("journald-agent stopped");
    Ok(())
}

/// Batch journal entries and POST each batch to VictoriaLogs, advancing the
/// durable cursor only after a 2xx.
async fn run(
    sink: &Sink,
    store: &CursorStore,
    batch_cfg: &BatchConfig,
    backoff_cfg: &BackoffConfig,
    mut rx: mpsc::Receiver<Entry>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        // Block for the first entry of the next batch (or shutdown).
        let first = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; } else { continue; }
            }
            v = rx.recv() => v,
        };
        let Some(first) = first else { break }; // reader gone

        let mut batch = vec![first];
        let linger = tokio::time::sleep(batch_cfg.linger());
        tokio::pin!(linger);
        while batch.len() < batch_cfg.max_entries {
            tokio::select! {
                _ = &mut linger => break,
                v = rx.recv() => match v {
                    Some(e) => batch.push(e),
                    None => break,
                },
            }
        }

        let last_cursor = batch.last().expect("non-empty batch").cursor.clone();
        let payloads: Vec<Vec<u8>> = batch
            .iter()
            .map(|e| serde_json::to_vec(&e.payload).expect("journal payload is valid json"))
            .collect();
        let body = build_ndjson(&payloads);

        if post_with_retry(sink, &body, backoff_cfg, &mut shutdown).await.is_err() {
            break; // shutdown during retry; cursor NOT advanced, batch replays next start
        }

        store.store(&last_cursor).context("persisting acked cursor")?;
        tracing::debug!(count = batch.len(), cursor = %last_cursor, "ingested batch");
    }
    Ok(())
}

/// POST to VictoriaLogs, retrying with exponential backoff until a 2xx. Returns
/// `Err(())` only if shutdown is requested mid-retry.
async fn post_with_retry(
    sink: &Sink,
    body: &[u8],
    cfg: &BackoffConfig,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<(), ()> {
    let mut delay_ms = cfg.initial_ms;
    loop {
        match sink.post(body.to_vec()).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let sleep_ms = backoff::jittered(delay_ms, cfg.jitter);
                tracing::warn!(error = %e, retry_in_ms = sleep_ms, "victorialogs post failed; backing off");
                if sleep_or_shutdown(sleep_ms, shutdown).await {
                    return Err(());
                }
                delay_ms = backoff::next_delay(delay_ms, cfg);
            }
        }
    }
}

/// Sleep for `ms`, returning `true` if shutdown was signalled instead.
async fn sleep_or_shutdown(ms: u64, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(ms)) => false,
        _ = shutdown.changed() => *shutdown.borrow(),
    }
}

/// Blocking loop: read journal entries and hand them to the sink loop.
///
/// Backpressure is intentional: when VictoriaLogs is down the sink loop stops
/// draining the channel, `blocking_send` blocks, and the journal's own on-disk
/// storage becomes the buffer until VictoriaLogs recovers.
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
                return; // dropping tx closes the channel and stops the sink loop
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
                    break; // sink loop gone
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
