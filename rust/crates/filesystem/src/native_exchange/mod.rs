//! Crash-recoverable entry exchanges through original held parents.

use std::path::Path;
use thiserror::Error;

/// Held native entry exchange failure.
#[derive(Debug, Error)]
enum NativeExchangeError {
    /// An entry name or physical layout is invalid.
    #[error("native exchange entry or physical layout is invalid")]
    InvalidLayout,
    /// Durable journal is malformed or does not match its requested roots.
    #[error("native exchange journal is incompatible")]
    IncompatibleJournal,
}
mod entry;
pub(crate) use entry::exchange_native_entries;
#[cfg(any(windows, test, feature = "native-mount"))]
pub(crate) use entry::recover_native_entry_exchange;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::*;
#[cfg(test)]
mod tests;

impl From<NativeExchangeError> for crate::demand::DemandError {
    fn from(error: NativeExchangeError) -> Self {
        Self::Io(std::io::Error::other(error))
    }
}
