//! Crash-recoverable whole-tree publication for native consumers.
use acyclic_native_runtime::durable_rename;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
mod journal;
use journal::*;
pub use journal::{
    NativeExchangeError, NativeExchangeJournal, NativeExchangeOutcome, NativeExchangePhase,
    prepare_native_exchange,
};
mod publication;
use publication::*;
pub use publication::{publish_native_exchange, recover_native_exchange};
mod entry;
use entry::*;
pub(crate) use entry::{exchange_native_entries, recover_native_entry_exchange};
#[cfg(windows)]
mod windows;
#[cfg(windows)]
use windows::*;
#[cfg(windows)]
mod windows_io;
#[cfg(windows)]
use windows_io::*;
#[cfg(test)]
mod tests;

impl From<crate::demand::DemandError> for NativeExchangeError {
    fn from(error: crate::demand::DemandError) -> Self {
        match error {
            crate::demand::DemandError::Cancelled => Self::Cancelled,
            crate::demand::DemandError::Work(error) => Self::Work(error),
            crate::demand::DemandError::Io(error) => exchange_io_error(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

impl From<crate::materializer::NativeTreeMaterializationError> for NativeExchangeError {
    fn from(error: crate::materializer::NativeTreeMaterializationError) -> Self {
        match error {
            crate::materializer::NativeTreeMaterializationError::Cancelled => Self::Cancelled,
            crate::materializer::NativeTreeMaterializationError::Work(error) => Self::Work(error),
            crate::materializer::NativeTreeMaterializationError::Io(error) => {
                exchange_io_error(error)
            }
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

impl From<NativeExchangeError> for crate::demand::DemandError {
    fn from(error: NativeExchangeError) -> Self {
        match error {
            NativeExchangeError::Cancelled => Self::Cancelled,
            NativeExchangeError::Work(error) => Self::Work(error),
            NativeExchangeError::Io(error) => Self::Io(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

impl From<NativeExchangeError> for crate::materializer::NativeTreeMaterializationError {
    fn from(error: NativeExchangeError) -> Self {
        match error {
            NativeExchangeError::Cancelled => Self::Cancelled,
            NativeExchangeError::Work(error) => Self::Work(error),
            NativeExchangeError::Io(error) => Self::Io(error),
            error => Self::Io(std::io::Error::other(error)),
        }
    }
}

pub(super) fn exchange_io_error(error: std::io::Error) -> NativeExchangeError {
    if error
        .get_ref()
        .is_some_and(|inner| inner.is::<NativeExchangeError>())
    {
        match error.into_inner() {
            Some(inner) => match inner.downcast::<NativeExchangeError>() {
                Ok(error) => *error,
                Err(inner) => NativeExchangeError::Io(std::io::Error::other(inner)),
            },
            None => {
                NativeExchangeError::Io(std::io::Error::other("exchange error lost its payload"))
            }
        }
    } else {
        NativeExchangeError::Io(error)
    }
}
