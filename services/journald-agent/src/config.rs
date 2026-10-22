//! Configuration loaded from a TOML file.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub iggy: IggyConfig,
    #[serde(default)]
    pub journal: JournalConfig,
    #[serde(default)]
    pub batch: BatchConfig,
    pub state: StateConfig,
    #[serde(default)]
    pub backoff: BackoffConfig,
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        let cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("parsing config file {}", path.display()))?;
        Ok(cfg)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct IggyConfig {
    /// e.g. "iggy://user:pass@10.20.0.10:8090"
    pub connection_string: String,
    pub stream: String,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct JournalConfig {
    pub start_at: StartAt,
    /// `_SYSTEMD_UNIT` values to skip (e.g. the agent's own unit).
    pub exclude_units: Vec<String>,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self { start_at: StartAt::Tail, exclude_units: Vec::new() }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StartAt {
    /// Start from "now" on first run (no stored cursor). Default.
    #[default]
    Tail,
    /// Backfill the entire retained journal on first run.
    Head,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    pub max_entries: usize,
    pub linger_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self { max_entries: 500, linger_ms: 1000 }
    }
}

impl BatchConfig {
    pub fn linger(&self) -> Duration {
        Duration::from_millis(self.linger_ms)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    /// File holding the last Iggy-acked journal cursor.
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BackoffConfig {
    pub initial_ms: u64,
    pub max_ms: u64,
    pub multiplier: f64,
    pub jitter: bool,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self { initial_ms: 500, max_ms: 30_000, multiplier: 2.0, jitter: true }
    }
}
