//! Exponential-backoff math shared by the poll and ingest retry loops.

use crate::config::BackoffConfig;

/// Next backoff delay: multiply, clamp to `max_ms`, never below 1ms.
pub fn next_delay(current_ms: u64, cfg: &BackoffConfig) -> u64 {
    ((current_ms as f64 * cfg.multiplier) as u64)
        .min(cfg.max_ms)
        .max(1)
}

/// Apply a [0.5, 1.0) multiplicative jitter so retrying consumers don't synchronize.
pub fn jittered(ms: u64, jitter: bool) -> u64 {
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
        let seq: Vec<u64> = (0..8).map(|_| { d = next_delay(d, &c); d }).collect();
        assert_eq!(seq, vec![1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
    }

    #[test]
    fn jitter_within_half_to_full() {
        for _ in 0..1000 {
            let j = jittered(1000, true);
            assert!((500..=1000).contains(&j), "jittered value {j} out of range");
        }
        assert_eq!(jittered(1234, false), 1234);
    }
}
