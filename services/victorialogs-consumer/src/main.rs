//! victorialogs-consumer: drain an Iggy topic into VictoriaLogs.
//!
//! Manual offset tracking (auto_commit=false): the offset file is the sole
//! source of truth. Correctness invariant — poll at offset(n) -> POST the batch
//! to VictoriaLogs -> await HTTP 2xx -> only then persist next_offset. A crash
//! between 2xx and persist replays the last batch (at-least-once).

mod backoff;
mod config;
mod offsets;
mod sink;
mod source;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;

use config::Config;
use offsets::OffsetStore;
use sink::{build_ndjson, Sink};
use source::Source;

const DEFAULT_CONFIG: &str = "/etc/rave/victorialogs-consumer.toml";

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

    let store = OffsetStore::new(cfg.state.path.clone());
    let mut offsets = store.load().context("loading stored offsets")?;
    tracing::info!(
        partitions = ?cfg.iggy.partitions,
        account_id = cfg.victorialogs.account_id,
        resuming = !offsets.is_empty(),
        "victorialogs-consumer starting"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("shutdown signal received");
        let _ = shutdown_tx.send(true);
    });

    let source = Source::connect(&cfg.iggy).await?;
    let sink = Sink::new(&cfg.victorialogs)?;

    run(&cfg, &source, &sink, &store, &mut offsets, shutdown_rx).await?;

    tracing::info!("victorialogs-consumer stopped");
    Ok(())
}

async fn run(
    cfg: &Config,
    source: &Source,
    sink: &Sink,
    store: &OffsetStore,
    offsets: &mut offsets::Offsets,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let count = cfg.batch.max_messages;
    let idle = cfg.batch.idle_poll_interval_ms;

    loop {
        if *shutdown.borrow() {
            break;
        }
        let mut progressed = false;

        for &partition in &cfg.iggy.partitions {
            if *shutdown.borrow() {
                break;
            }
            let next = offsets.get(&partition).copied().unwrap_or(0);

            // Poll with backoff until success or shutdown.
            let entries = match retry(cfg, &mut shutdown, "poll", || {
                source.poll(partition, next, count)
            })
            .await
            {
                Some(e) => e,
                None => break, // shutdown during retry
            };

            if entries.is_empty() {
                continue;
            }
            progressed = true;

            let last_offset = entries.last().expect("non-empty").offset;
            // Derive a `level` field from journald PRIORITY so VL shows real levels.
            let payloads: Vec<Vec<u8>> =
                entries.into_iter().map(|e| sink::enrich_level(&e.payload)).collect();
            let body = build_ndjson(&payloads);

            // Ingest with backoff until 2xx or shutdown.
            if retry(cfg, &mut shutdown, "ingest", || sink.post(body.clone()))
                .await
                .is_none()
            {
                break; // shutdown during retry; offset NOT advanced
            }

            // Ack received: safe to advance the durable offset.
            offsets.insert(partition, last_offset + 1);
            store.store(offsets).context("persisting offsets")?;
            tracing::debug!(
                partition,
                count = payloads.len(),
                next_offset = last_offset + 1,
                "ingested batch"
            );
        }

        if !progressed && sleep_or_shutdown(idle, &mut shutdown).await {
            break;
        }
    }
    Ok(())
}

/// Retry an async op with exponential backoff until it succeeds, or return
/// `None` if shutdown is requested mid-retry.
async fn retry<F, Fut, T>(
    cfg: &Config,
    shutdown: &mut watch::Receiver<bool>,
    op_name: &str,
    mut op: F,
) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay_ms = cfg.backoff.initial_ms;
    loop {
        match op().await {
            Ok(v) => return Some(v),
            Err(e) => {
                let sleep_ms = backoff::jittered(delay_ms, cfg.backoff.jitter);
                tracing::warn!(error = %e, op = op_name, retry_in_ms = sleep_ms, "retrying");
                if sleep_or_shutdown(sleep_ms, shutdown).await {
                    return None;
                }
                delay_ms = backoff::next_delay(delay_ms, &cfg.backoff);
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

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to install SIGTERM handler");
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
