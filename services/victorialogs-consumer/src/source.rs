//! Iggy poll source with explicit, manual offset selection.

use anyhow::{Context, Result};
use iggy::prelude::*;

use crate::config::IggyConfig;

/// One polled message: its partition offset and raw JSON payload bytes.
pub struct PolledEntry {
    pub offset: u64,
    pub payload: Vec<u8>,
}

pub struct Source {
    // Kept alive so the connection stays open.
    _client: IggyClient,
    stream: Identifier,
    topic: Identifier,
    consumer: Consumer,
}

impl Source {
    pub async fn connect(cfg: &IggyConfig) -> Result<Self> {
        let client = IggyClient::from_connection_string(&cfg.connection_string)
            .context("building iggy client from connection string")?;
        client.connect().await.context("connecting to iggy")?;

        let stream = Identifier::named(&cfg.stream)
            .with_context(|| format!("invalid stream name {}", cfg.stream))?;
        let topic = Identifier::named(&cfg.topic)
            .with_context(|| format!("invalid topic name {}", cfg.topic))?;
        let consumer = Consumer::new(
            Identifier::numeric(cfg.consumer_id).context("invalid consumer id")?,
        );

        tracing::info!(stream = %cfg.stream, topic = %cfg.topic, "connected to iggy");
        Ok(Self { _client: client, stream, topic, consumer })
    }

    /// Poll up to `count` messages from `partition_id` starting at `offset`.
    /// `auto_commit=false`: nothing is stored server-side — our offset file is
    /// the sole source of truth.
    pub async fn poll(&self, partition_id: u32, offset: u64, count: u32) -> Result<Vec<PolledEntry>> {
        let polled = self
            ._client
            .poll_messages(
                &self.stream,
                &self.topic,
                Some(partition_id),
                &self.consumer,
                &PollingStrategy::offset(offset),
                count,
                false,
            )
            .await
            .context("polling iggy messages")?;

        Ok(polled
            .messages
            .into_iter()
            .map(|m| PolledEntry { offset: m.header.offset, payload: m.payload.to_vec() })
            .collect())
    }
}
