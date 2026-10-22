//! Blocking reader over the systemd journal.
//!
//! libsystemd's journal API is synchronous and blocks the calling thread while
//! waiting for new entries, so this is driven from a dedicated OS thread (see
//! `main::spawn_reader`) rather than from the async runtime.

// `JournalFiles`/`Journal::open` are deprecated in favour of `OpenOptions`, but
// the plain All/false/false form is exactly what we want and remains stable.
#![allow(deprecated)]

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result};
use systemd::journal::{Journal, JournalFiles, JournalRecord};

use crate::config::StartAt;

/// One journal entry ready to ship: its fields plus its opaque cursor.
pub struct Entry {
    pub cursor: String,
    pub payload: serde_json::Value,
    /// Unit identifiers used for exclusion filtering: `_SYSTEMD_UNIT` (system
    /// units) and `_SYSTEMD_USER_UNIT` (user units). A user unit's own logs
    /// carry `_SYSTEMD_UNIT=user@<uid>.service` plus the real name in
    /// `_SYSTEMD_USER_UNIT`, so both must be checked.
    pub units: Vec<String>,
}

pub struct JournalReader {
    journal: Journal,
    exclude_units: HashSet<String>,
}

impl JournalReader {
    /// Open the journal and position it so the next read yields the first
    /// entry we still need to ship.
    ///
    /// - With a stored cursor: seek to it, then step once so we resume *after*
    ///   the last acked entry. If the cursor has been vacuumed away, fall back
    ///   to the tail and warn (bounded gap beats a crash loop).
    /// - Without a stored cursor (first run): honour `start_at`.
    pub fn open(
        start_at: StartAt,
        stored_cursor: Option<&str>,
        exclude_units: Vec<String>,
    ) -> Result<Self> {
        #[allow(deprecated)]
        let mut journal = Journal::open(JournalFiles::All, false, false)
            .context("opening systemd journal")?;

        match stored_cursor {
            Some(cursor) => match journal.seek_cursor(cursor) {
                Ok(()) => {
                    // seek_cursor points *at* the acked entry; step past it.
                    journal.next().context("stepping past stored cursor")?;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "stored cursor not found (vacuumed?); resuming from journal tail"
                    );
                    seek_to_tail(&mut journal)?;
                }
            },
            None => match start_at {
                StartAt::Tail => seek_to_tail(&mut journal)?,
                StartAt::Head => {
                    journal.seek_head().context("seeking journal head")?;
                }
            },
        }

        Ok(Self { journal, exclude_units: exclude_units.into_iter().collect() })
    }

    /// Advance to and return the next entry. Blocks up to `wait` for a new
    /// entry to appear if the journal is currently drained; returns `Ok(None)`
    /// if nothing arrived within `wait`.
    pub fn read_next(&mut self, wait: Duration) -> Result<Option<Entry>> {
        let record = match self.journal.next_entry().context("advancing journal")? {
            Some(record) => Some(record),
            None => self
                .journal
                .await_next_entry(Some(wait))
                .context("awaiting new journal entry")?,
        };

        match record {
            Some(record) => {
                let cursor = self.journal.cursor().context("reading journal cursor")?;
                Ok(Some(self.to_entry(record, cursor)))
            }
            None => Ok(None),
        }
    }

    pub fn is_excluded(&self, entry: &Entry) -> bool {
        if self.exclude_units.is_empty() {
            return false;
        }
        entry.units.iter().any(|u| self.exclude_units.contains(u))
    }

    fn to_entry(&self, record: JournalRecord, cursor: String) -> Entry {
        let units = ["_SYSTEMD_UNIT", "_SYSTEMD_USER_UNIT"]
            .iter()
            .filter_map(|k| record.get(*k).cloned())
            .collect();
        let mut map = serde_json::Map::with_capacity(record.len() + 1);
        for (k, v) in record {
            map.insert(k, serde_json::Value::String(v));
        }
        map.insert("__cursor".to_string(), serde_json::Value::String(cursor.clone()));
        Entry { cursor, payload: serde_json::Value::Object(map), units }
    }
}

/// Seek to the tail so only *new* entries are read.
///
/// libsystemd quirk: after `sd_journal_seek_tail()` the read pointer sits past
/// the end without being anchored on a real entry, and a bare `next()` from
/// there returns 0 and fails to follow appended entries. Calling `previous()`
/// once anchors on the last existing entry; the reader's first `next()` then
/// steps off it onto the first genuinely new entry.
fn seek_to_tail(journal: &mut Journal) -> Result<()> {
    journal.seek_tail().context("seeking journal tail")?;
    journal.previous().context("anchoring at journal tail")?;
    Ok(())
}
