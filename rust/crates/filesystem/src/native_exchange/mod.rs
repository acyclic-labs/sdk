//! Crash-recoverable whole-tree publication for native consumers.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

use acyclic_native_runtime::durable_rename;

const NATIVE_EXCHANGE_JOURNAL_VERSION: u32 = 1;

/// Durable whole-tree exchange phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum NativeExchangePhase {
    /// Excluded paths are moving from the live tree into the prepared tree.
    Carrying,
    /// Whole-tree exchange is in flight.
    Exchanging,
}

/// Complete recovery state for one whole-tree publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeExchangeJournal {
    /// Version of the durable journal encoding.
    pub version: u32,
    /// Stable identity of the caller-visible publication request.
    pub operation: crate::IdempotencyKey,
    /// Opaque caller recovery bytes retained with the durable request.
    pub recovery: Vec<u8>,
    /// Current live root.
    pub live: PathBuf,
    /// Prepared tree before publication and displaced tree after publication.
    pub prepared: PathBuf,
    /// Exact relative paths carried from the live tree.
    pub carried: Vec<PathBuf>,
    /// Stable identities of the live and prepared roots before exchange.
    pub roots: [[u8; 16]; 2],
    /// Current durable phase.
    pub phase: NativeExchangePhase,
}

/// Result of a successful or recovered exchange.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeExchangeOutcome {
    /// Whether the prepared tree was published.
    pub published: bool,
    /// Complete displaced tree when publication completed.
    pub displaced: Option<PathBuf>,
    /// Opaque consumer state stored with the recovered publication request.
    pub recovery: Vec<u8>,
}

/// Whole-tree exchange failure.
#[derive(Debug, Error)]
pub enum NativeExchangeError {
    /// Paths are not two distinct sibling directories.
    #[error("native exchange paths must be distinct sibling directories")]
    InvalidLayout,
    /// Durable journal is malformed or does not match its requested roots.
    #[error("native exchange journal is incompatible")]
    IncompatibleJournal,
    /// Durable journal was written by an unsupported format version.
    #[error("native exchange journal version {0} is unsupported")]
    UnsupportedJournalVersion(u32),
    /// Both copies of one carried path exist, so recovery cannot discard either.
    #[error("both copies of carried path exist: {0}")]
    CarriedPathConflict(PathBuf),
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
mod tree;
use tree::*;
pub use tree::{prepare_native_exchange, publish_native_exchange, recover_native_exchange};
mod entry;
pub(crate) use entry::{exchange_native_entries, recover_native_entry_exchange};
#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::*;
#[cfg(test)]
mod tests;

impl From<NativeExchangeError> for crate::demand::DemandError {
    fn from(error: NativeExchangeError) -> Self {
        match error {
            NativeExchangeError::Io(error) => Self::Io(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}
