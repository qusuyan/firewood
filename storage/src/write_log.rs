// Copyright (C) 2023, Ava Labs, Inc. All rights reserved.
// See the file LICENSE.md for licensing terms.

//! Optional CSV logging of every write the nodestore issues to the underlying
//! storage, for debugging and analyzing I/O patterns.
//!
//! Enable it by setting the `FIREWOOD_WRITE_LOG` environment variable to the
//! path of the log file to produce (e.g. `FIREWOOD_WRITE_LOG=/tmp/writes.csv`).
//! If the variable is unset or empty, logging is disabled and has no I/O cost.
//! The file is created (truncated if it exists) on the first write.
//!
//! One CSV row is appended per write, after the write succeeded, with the
//! columns:
//!
//! ```csv
//! group,purpose,offset,len,area_size
//! ```
//!
//! - `group` - which persist-worker event-loop iteration the write belongs to;
//!   writes with the same group happened together in one iteration (reaps of
//!   old revisions followed by persisting the latest revision). `0` means the
//!   write happened outside the event loop (e.g. header setup at startup, or
//!   storage-level unit tests).
//! - `purpose` - `trie-node`, `free-area`, `free-truncate` (checker repairs),
//!   `freelist-heads`, or `header`.
//! - `offset`, `len` - file offset and number of bytes actually written.
//! - `area_size` - for `trie-node`, `free-area`, and `free-truncate`: the full
//!   size of the allocated area (so `area_size - len` is padding for nodes,
//!   and free-area markers only overwrite the first few bytes of the freed
//!   area). Empty for `freelist-heads` and `header` writes.
//!
//! All numbers are decimal.
//!
//! This intentionally does not use the `logger` module: it is driven purely by
//! the environment variable, so it behaves identically whether or not the
//! `logger` feature is compiled in or a logger is configured. Rows are written
//! under a mutex with a single `write_all`, so rows from concurrent writers
//! never interleave.

use std::fmt;
use std::fs::File;
use std::io::Write;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// The environment variable holding the log file path.
pub(crate) const ENV_VAR: &str = "FIREWOOD_WRITE_LOG";

/// The CSV header row written when the log file is created.
const HEADER_ROW: &str = "group,purpose,offset,len,area_size\n";

/// The current write group, incremented by [`begin_group`].
static GROUP: AtomicU64 = AtomicU64::new(0);

/// Purpose-specific columns of a log row. The variant determines the `purpose`
/// column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Details {
    /// A serialized trie node written into an area of `area_size` bytes.
    TrieNode {
        /// The full size of the allocated area, if the serialized node's area
        /// index byte was valid.
        area_size: Option<u64>,
    },
    /// A freed area: the area index and the `0xff` free marker, followed by
    /// the pointer to the next free area (the previous freelist head).
    FreeArea {
        /// The full size of the freed area.
        area_size: u64,
    },
    /// A free area rewritten by the checker to truncate a broken freelist.
    FreeAreaTruncate {
        /// The full size of the freed area.
        area_size: u64,
    },
    /// The freelist heads array inside the header.
    FreeListHeads,
    /// The full nodestore header.
    Header,
}

/// Displays an optional number as an empty CSV cell or the decimal value.
struct OptionalCell(Option<u64>);

impl fmt::Display for OptionalCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => write!(f, "{value}"),
            None => Ok(()),
        }
    }
}

/// Returns the log file writer, initialized from [`ENV_VAR`] on first use.
fn writer() -> Option<&'static Mutex<File>> {
    static WRITER: OnceLock<Option<Mutex<File>>> = OnceLock::new();
    WRITER
        .get_or_init(|| {
            let path = std::path::PathBuf::from(std::env::var_os(ENV_VAR)?);
            if path.as_os_str().is_empty() {
                return None;
            }
            match File::create(&path) {
                Ok(mut file) => {
                    // A failed header write is reported on the first row write.
                    let _ = file.write_all(HEADER_ROW.as_bytes());
                    Some(Mutex::new(file))
                }
                Err(error) => {
                    eprintln!(
                        "[firewood-write] cannot create write log file {}: {error}; write logging disabled",
                        path.display(),
                    );
                    None
                }
            }
        })
        .as_ref()
}

/// Returns true if write logging is enabled, i.e. [`ENV_VAR`] names a log file
/// that could be created. Call sites should check this before doing any work
/// to assemble row details.
#[must_use]
pub fn enabled() -> bool {
    writer().is_some()
}

/// Start a new write group.
///
/// The persist worker calls this at the start of each event-loop iteration so
/// that every row logged afterwards carries the iteration's number in its
/// `group` column, making it visible which writes happened together. Rows
/// logged before the first call have group `0`.
pub fn begin_group() {
    GROUP.fetch_add(1, Ordering::Relaxed);
}

/// Append one CSV row for a completed write.
///
/// Only call this when [`enabled`] returns true and after the write succeeded,
/// so the log only contains writes that actually happened. Write errors on the
/// log file itself are ignored.
pub(crate) fn record(offset: u64, len: usize, details: Details) {
    let Some(writer) = writer() else {
        return;
    };

    let (purpose, area_size) = match details {
        Details::TrieNode { area_size } => ("trie-node", area_size),
        Details::FreeArea { area_size } => ("free-area", Some(area_size)),
        Details::FreeAreaTruncate { area_size } => ("free-truncate", Some(area_size)),
        Details::FreeListHeads => ("freelist-heads", None),
        Details::Header => ("header", None),
    };

    let group = GROUP.load(Ordering::Relaxed);
    let row = format!(
        "{group},{purpose},{offset},{len},{}\n",
        OptionalCell(area_size),
    );
    let _ = writer.lock().write_all(row.as_bytes());
}
