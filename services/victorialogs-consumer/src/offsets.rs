//! Durable, crash-safe per-partition offset storage.
//!
//! The whole map is written atomically (temp file + rename) on every advance,
//! so an interrupted write can never corrupt the stored offsets. This mirrors
//! the journald-agent's cursor store.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};

/// partition id -> next offset to poll.
pub type Offsets = BTreeMap<u32, u64>;

pub struct OffsetStore {
    path: PathBuf,
    tmp: PathBuf,
}

impl OffsetStore {
    pub fn new(path: PathBuf) -> Self {
        let tmp = path.with_extension("tmp");
        Self { path, tmp }
    }

    /// Load stored offsets, or an empty map on first run (file absent).
    pub fn load(&self) -> Result<Offsets> {
        match std::fs::read(&self.path) {
            Ok(bytes) if bytes.is_empty() => Ok(Offsets::new()),
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing offsets file {}", self.path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Offsets::new()),
            Err(e) => Err(e).with_context(|| format!("reading offsets {}", self.path.display())),
        }
    }

    /// Atomically persist the full offset map. Durable after return.
    pub fn store(&self, offsets: &Offsets) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        let bytes = serde_json::to_vec(offsets).context("serializing offsets")?;
        {
            let mut f = std::fs::File::create(&self.tmp)
                .with_context(|| format!("creating temp offsets {}", self.tmp.display()))?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&self.tmp, &self.path)
            .with_context(|| format!("renaming offsets into place {}", self.path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
fn tmp_path_of(p: &std::path::Path) -> PathBuf {
    p.with_extension("tmp")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = std::env::temp_dir().join("vl-consumer-offsets-test");
        let _ = std::fs::remove_dir_all(&dir);
        let store = OffsetStore::new(dir.join("offsets.json"));

        assert!(store.load().unwrap().is_empty(), "absent file -> empty map");

        let mut offsets = Offsets::new();
        offsets.insert(0, 42);
        offsets.insert(3, 7);
        store.store(&offsets).unwrap();

        let loaded = store.load().unwrap();
        assert_eq!(loaded.get(&0), Some(&42));
        assert_eq!(loaded.get(&3), Some(&7));

        // advance partition 0
        offsets.insert(0, 43);
        store.store(&offsets).unwrap();
        assert_eq!(store.load().unwrap().get(&0), Some(&43));

        // no stray temp file left behind
        assert!(!tmp_path_of(&dir.join("offsets.json")).exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
