//! Configuration loaded from a TOML file.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub iggy: IggyConfig,
    pub victorialogs: VictoriaLogsConfig,
    #[serde(default)]
    pub batch: BatchConfig,
    pub state: StateConfig,
    #[serde(default)]
    pub backoff: BackoffConfig,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parsing config file {}", path.display()))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IggyConfig {
    pub connection_string: String,
    pub stream: String,
    pub topic: String,
    /// Partitions to consume; dev topic has a single partition 0.
    pub partitions: Vec<u32>,
    pub consumer_id: u32,
}

impl Default for IggyConfig {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            stream: String::new(),
            topic: String::new(),
            partitions: vec![0],
            consumer_id: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VictoriaLogsConfig {
    /// Base URL, e.g. "http://10.20.0.11:9428".
    pub url: String,
    /// Tenant selector (default 0/0). Dev=1, prod=2 by project convention.
    pub account_id: u32,
    pub project_id: u32,
    pub msg_field: String,
    pub time_field: String,
    pub stream_fields: Vec<String>,
    pub ignore_fields: Vec<String>,
}

impl Default for VictoriaLogsConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            account_id: 0,
            project_id: 0,
            msg_field: "MESSAGE".to_string(),
            time_field: "_SOURCE_REALTIME_TIMESTAMP".to_string(),
            stream_fields: vec![
                "_HOSTNAME".to_string(),
                "_SYSTEMD_UNIT".to_string(),
                "_SYSTEMD_USER_UNIT".to_string(),
            ],
            ignore_fields: vec!["__cursor".to_string()],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    pub max_messages: u32,
    pub idle_poll_interval_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self { max_messages: 500, idle_poll_interval_ms: 1000 }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    /// File holding per-partition next offsets (JSON map).
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
