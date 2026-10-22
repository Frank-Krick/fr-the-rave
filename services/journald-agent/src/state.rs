//! Durable, crash-safe storage of the last Iggy-acked journal cursor.
//!
//! The cursor is written atomically (temp file + rename) so an interrupted
//! write can never leave a half-written or empty cursor file behind.

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};

pub struct CursorStore {
    path: PathBuf,
    tmp: PathBuf,
}

impl CursorStore {
    pub fn new(path: PathBuf) -> Self {
        let tmp = path.with_extension("tmp");
        Self { path, tmp }
    }

    /// Returns the stored cursor, or `None` on first run (file absent).
    pub fn load(&self) -> Result<Option<String>> {
        match std::fs::read_to_string(&self.path) {
            Ok(s) => {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed.to_string()))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading cursor {}", self.path.display())),
        }
    }

    /// Atomically persist `cursor`. Durable after return (fsync + rename).
    pub fn store(&self, cursor: &str) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state dir {}", parent.display()))?;
        }
        {
            let mut f = std::fs::File::create(&self.tmp)
                .with_context(|| format!("creating temp cursor {}", self.tmp.display()))?;
            f.write_all(cursor.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&self.tmp, &self.path)
            .with_context(|| format!("renaming cursor into place {}", self.path.display()))?;
        Ok(())
    }
}

/// Exposed only for the module's unit test helper path.
#[cfg(test)]
pub(crate) fn tmp_path_of(p: &std::path::Path) -> PathBuf {
    p.with_extension("tmp")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = std::env::temp_dir().join("journald-agent-state-test");
        let _ = std::fs::remove_dir_all(&dir);
        let store = CursorStore::new(dir.join("cursor"));

        assert_eq!(store.load().unwrap(), None, "absent file reads as None");

        store.store("s=abc;i=1").unwrap();
        assert_eq!(store.load().unwrap().as_deref(), Some("s=abc;i=1"));

        // overwrite advances
        store.store("s=abc;i=2").unwrap();
        assert_eq!(store.load().unwrap().as_deref(), Some("s=abc;i=2"));

        // no stray temp file left behind
        assert!(!tmp_path_of(&dir.join("cursor")).exists());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
