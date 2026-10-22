//! Iggy producer side: batch journal entries, publish with retry/backoff, and
//! only advance the persisted cursor once the server has acked the batch.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context, Result};
use iggy::prelude::*;
use tokio::sync::{mpsc, watch};

use crate::config::{BackoffConfig, BatchConfig, IggyConfig};
use crate::journal::Entry;
use crate::state::CursorStore;

pub struct Shipper {
    // Kept alive so the producer's connection stays open.
    _client: IggyClient,
    producer: IggyProducer,
    store: CursorStore,
    batch: BatchConfig,
    backoff: BackoffConfig,
}

impl Shipper {
    pub async fn connect(
        cfg: &IggyConfig,
        store: CursorStore,
        batch: BatchConfig,
        backoff: BackoffConfig,
    ) -> Result<Self> {
        let client = IggyClient::from_connection_string(&cfg.connection_string)
            .context("building iggy client from connection string")?;
        client.connect().await.context("connecting to iggy")?;

        let producer = client
            .producer(&cfg.stream, &cfg.topic)
            .context("creating iggy producer")?
            .build();
        producer.init().await.context("initializing iggy producer")?;

        tracing::info!(stream = %cfg.stream, topic = %cfg.topic, "connected to iggy");
        Ok(Self { _client: client, producer, store, batch, backoff })
    }

    /// Consume entries, batch, publish, and persist cursors until the channel
    /// closes or shutdown is signalled.
    pub async fn run(
        &self,
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
            let linger = tokio::time::sleep(self.batch.linger());
            tokio::pin!(linger);

            while batch.len() < self.batch.max_entries {
                tokio::select! {
                    _ = &mut linger => break,
                    v = rx.recv() => match v {
                        Some(e) => batch.push(e),
                        None => break,
                    },
                }
            }

            if self.send_with_retry(&batch, &mut shutdown).await.is_err() {
                // Only error path is shutdown-during-retry: cursor not advanced,
                // batch will be re-read next start. Exit cleanly.
                break;
            }

            // Ack received: now it is safe to advance the durable cursor.
            let last = batch.last().expect("non-empty batch");
            self.store
                .store(&last.cursor)
                .context("persisting acked cursor")?;
            tracing::debug!(count = batch.len(), cursor = %last.cursor, "shipped batch");
        }
        Ok(())
    }

    /// Publish, retrying forever with exponential backoff until the server
    /// acks. Returns `Err` only if shutdown is requested mid-retry.
    async fn send_with_retry(
        &self,
        batch: &[Entry],
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let mut delay_ms = self.backoff.initial_ms;
        loop {
            // IggyMessage isn't Clone, so rebuild the batch each attempt.
            let messages = build_messages(batch)?;
            match self.producer.send(messages).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let sleep_ms = jittered(delay_ms, self.backoff.jitter);
                    tracing::warn!(
                        error = %e,
                        retry_in_ms = sleep_ms,
                        "iggy send failed; backing off"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                anyhow::bail!("shutdown requested during send retry");
                            }
                        }
                    }
                    delay_ms = next_delay(delay_ms, &self.backoff);
                }
            }
        }
    }
}

fn build_messages(batch: &[Entry]) -> Result<Vec<IggyMessage>> {
    batch
        .iter()
        .map(|e| {
            let json = serde_json::to_string(&e.payload).context("serializing journal entry")?;
            IggyMessage::from_str(&json)
                .map_err(|e| anyhow::anyhow!("building iggy message: {e}"))
        })
        .collect()
}

/// Next backoff delay: multiply, clamp to `max_ms`, never below 1ms.
fn next_delay(current_ms: u64, cfg: &BackoffConfig) -> u64 {
    ((current_ms as f64 * cfg.multiplier) as u64)
        .min(cfg.max_ms)
        .max(1)
}

/// Apply a [0.5, 1.0) multiplicative jitter so retrying agents don't synchronize.
fn jittered(ms: u64, jitter: bool) -> u64 {
    if !jitter || ms == 0 {
        return ms;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let frac = 0.5 + (nanos % 1_000_000) as f64 / 1_000_000.0 * 0.5;
    ((ms as f64) * frac) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> BackoffConfig {
        BackoffConfig { initial_ms: 500, max_ms: 30_000, multiplier: 2.0, jitter: true }
    }

    #[test]
    fn backoff_grows_then_caps() {
        let c = cfg();
        let mut d = c.initial_ms;
        let seq: Vec<u64> = (0..8)
            .map(|_| {
                d = next_delay(d, &c);
                d
            })
            .collect();
        // 500 -> 1000, 2000, 4000, 8000, 16000, 30000 (capped), 30000
        assert_eq!(seq, vec![1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
    }

    #[test]
    fn backoff_never_stalls_at_zero() {
        // Even a zero/degenerate start advances to a positive delay.
        let c = BackoffConfig { initial_ms: 0, max_ms: 1000, multiplier: 2.0, jitter: false };
        assert_eq!(next_delay(0, &c), 1);
    }

    #[test]
    fn jitter_within_half_to_full() {
        for _ in 0..1000 {
            let j = jittered(1000, true);
            assert!((500..=1000).contains(&j), "jittered value {j} out of range");
        }
        // Disabled jitter is a no-op.
        assert_eq!(jittered(1234, false), 1234);
    }
}
