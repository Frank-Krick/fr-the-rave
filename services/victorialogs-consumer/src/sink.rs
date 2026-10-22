//! VictoriaLogs ingestion sink: POST newline-delimited JSON to `/insert/jsonline`.

use anyhow::{Context, Result};
use reqwest::Client;

use crate::config::VictoriaLogsConfig;

pub struct Sink {
    client: Client,
    /// Fully-built insert URL including the field-mapping query params.
    url: reqwest::Url,
    account_id: String,
    project_id: String,
}

impl Sink {
    pub fn new(cfg: &VictoriaLogsConfig) -> Result<Self> {
        let mut url = reqwest::Url::parse(&cfg.url)
            .with_context(|| format!("parsing victorialogs url {}", cfg.url))?;
        url.set_path("/insert/jsonline");
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("_msg_field", &cfg.msg_field);
            q.append_pair("_time_field", &cfg.time_field);
            if !cfg.stream_fields.is_empty() {
                q.append_pair("_stream_fields", &cfg.stream_fields.join(","));
            }
            if !cfg.ignore_fields.is_empty() {
                q.append_pair("ignore_fields", &cfg.ignore_fields.join(","));
            }
        }

        let client = Client::builder().build().context("building http client")?;
        tracing::info!(
            url = %url,
            account_id = %cfg.account_id,
            project_id = %cfg.project_id,
            "victorialogs sink ready"
        );
        Ok(Self {
            client,
            url,
            account_id: cfg.account_id.to_string(),
            project_id: cfg.project_id.to_string(),
        })
    }

    /// POST one ndjson batch into the configured tenant. Errors on non-2xx so
    /// the caller retries without advancing the offset.
    pub async fn post(&self, body: Vec<u8>) -> Result<()> {
        let resp = self
            .client
            .post(self.url.clone())
            .header("AccountID", &self.account_id)
            .header("ProjectID", &self.project_id)
            .header(reqwest::header::CONTENT_TYPE, "application/stream+json")
            .body(body)
            .send()
            .await
            .context("posting to victorialogs")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("victorialogs returned {status}: {text}");
        }
        Ok(())
    }
}

/// Map a syslog PRIORITY (journald `PRIORITY`, "0".."7") to a VictoriaLogs-recognized
/// level name. Returns `None` for unmappable values.
fn priority_to_level(priority: &str) -> Option<&'static str> {
    match priority.trim() {
        "0" => Some("emerg"),
        "1" => Some("alert"),
        "2" => Some("crit"),
        "3" => Some("error"),
        "4" => Some("warn"),
        "5" => Some("notice"),
        "6" => Some("info"),
        "7" => Some("debug"),
        _ => None,
    }
}

/// Add a `level` field derived from journald `PRIORITY` so VictoriaLogs shows a real
/// log level instead of "OTHER". Returns the payload unchanged when it isn't a JSON
/// object or has no mappable `PRIORITY` (VL then just treats it as level-less).
pub fn enrich_level(payload: &[u8]) -> Vec<u8> {
    let Ok(serde_json::Value::Object(mut map)) =
        serde_json::from_slice::<serde_json::Value>(payload)
    else {
        return payload.to_vec();
    };
    let level = map
        .get("PRIORITY")
        .and_then(|v| v.as_str())
        .and_then(priority_to_level);
    match level {
        Some(level) => {
            map.insert("level".to_string(), serde_json::Value::String(level.to_string()));
            serde_json::to_vec(&serde_json::Value::Object(map)).unwrap_or_else(|_| payload.to_vec())
        }
        None => payload.to_vec(),
    }
}

/// Build an ndjson body from raw one-line JSON payloads (each gets a trailing newline).
pub fn build_ndjson(payloads: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = payloads.iter().map(|p| p.len() + 1).sum();
    let mut body = Vec::with_capacity(total);
    for p in payloads {
        body.extend_from_slice(p);
        body.push(b'\n');
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ndjson_joins_with_newlines() {
        let payloads = vec![b"{\"a\":1}".to_vec(), b"{\"b\":2}".to_vec()];
        let body = build_ndjson(&payloads);
        assert_eq!(body, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn ndjson_empty() {
        assert!(build_ndjson(&[]).is_empty());
    }

    #[test]
    fn enrich_maps_priority_to_level() {
        let out = enrich_level(br#"{"MESSAGE":"hi","PRIORITY":"3"}"#);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["level"], "error");
        assert_eq!(v["MESSAGE"], "hi"); // original fields preserved
    }

    #[test]
    fn enrich_passthrough_without_priority() {
        let input = br#"{"MESSAGE":"hi"}"#;
        assert_eq!(enrich_level(input), input.to_vec());
    }

    #[test]
    fn enrich_passthrough_on_bad_priority_or_non_object() {
        assert_eq!(enrich_level(br#"{"PRIORITY":"9"}"#), br#"{"PRIORITY":"9"}"#.to_vec());
        assert_eq!(enrich_level(b"not json"), b"not json".to_vec());
    }

    #[test]
    fn priority_table() {
        for (p, lvl) in [
            ("0", "emerg"), ("1", "alert"), ("2", "crit"), ("3", "error"),
            ("4", "warn"), ("5", "notice"), ("6", "info"), ("7", "debug"),
        ] {
            assert_eq!(priority_to_level(p), Some(lvl));
        }
        assert_eq!(priority_to_level("8"), None);
    }
}
