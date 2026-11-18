//! Canonical authority epoch and sequence values.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Monotonic authority incarnation used to fence stale writers.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Epoch(u64);

impl Epoch {
    /// Genesis authority epoch.
    pub const GENESIS: Self = Self(1);

    /// Constructs a non-zero epoch.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::ZeroEpoch`] when `value` is zero.
    pub const fn new(value: u64) -> Result<Self, IdentityError> {
        if value == 0 {
            Err(IdentityError::ZeroEpoch)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the wire integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Dense one-based sequence within an authority history; zero denotes genesis.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sequence(u64);

impl Sequence {
    /// Empty stream position.
    pub const GENESIS: Self = Self(0);

    /// Constructs a sequence from its wire integer.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wire integer.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next sequence, failing closed at exhaustion.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError::SequenceExhausted`] at `u64::MAX`.
    pub const fn checked_next(self) -> Result<Self, IdentityError> {
        match self.0.checked_add(1) {
            Some(value) => Ok(Self(value)),
            None => Err(IdentityError::SequenceExhausted),
        }
    }
}

/// Errors constructing portable identities.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IdentityError {
    /// Epoch zero is reserved and never valid.
    #[error("authority epoch must be non-zero")]
    ZeroEpoch,
    /// No further sequence can be represented.
    #[error("authority sequence exhausted")]
    SequenceExhausted,
}
