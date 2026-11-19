//! Portable identities and authority contracts.

pub(crate) use acyclic_fs_format::identity::{
    AuthorityId, CheckoutId, Digest, FileId, MountId, OperationId, VolumeId, WatchId,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Widens one native collection size into the durable 64-bit accounting domain.
///
/// Rust supports pointer widths no larger than 64 bits, so this conversion is
/// an invariant rather than a recoverable filesystem outcome on every native
/// and WebAssembly target supported by this crate.
#[inline]
pub(crate) fn usize_to_u64(value: usize) -> u64 {
    value as u64
}
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

/// Durable authority head.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Head {
    /// Current fencing epoch.
    pub epoch: Epoch,
    /// Last committed sequence.
    pub sequence: Sequence,
    /// Hash-chain head, or [`Digest::ZERO`] at genesis.
    pub digest: Digest,
}

impl Head {
    /// Empty head for a new authority.
    #[must_use]
    pub const fn genesis(epoch: Epoch) -> Self {
        Self {
            epoch,
            sequence: Sequence::GENESIS,
            digest: Digest::ZERO,
        }
    }
}

/// One canonical opaque operation submitted to authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposedCommit {
    /// Kernel-authenticated namespace changes committed atomically with a volume publication.
    pub publication: Option<crate::PublicationChanges>,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Digest of the complete semantic command and caller-visible preconditions.
    pub fingerprint: Digest,
    /// Canonical schema-versioned operation bytes.
    pub payload: Bytes,
}

/// One recovered or newly durable authority commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableCommit {
    /// Writer-fencing epoch that admitted this commit.
    pub epoch: Epoch,
    /// Assigned dense sequence.
    pub sequence: Sequence,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Original command fingerprint.
    pub fingerprint: Digest,
    /// Previous hash-chain head.
    pub previous_digest: Digest,
    /// Commit hash-chain head.
    pub digest: Digest,
    /// Canonical schema-versioned operation bytes.
    pub payload: Bytes,
    /// Authoritative Stream settlement time; absent for offline and legacy records.
    /// This metadata is not serialized into the filesystem record or its digest.
    pub settled_at_micros: Option<u64>,
}

/// Canonical non-payload bytes hashed into one authority commit identity.
pub const AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES: u64 = 151;

/// Computes the canonical hash-chain digest for one durable authority commit.
///
/// This function is the backend conformance boundary: every authority store
/// must produce this exact digest for the same canonical operation.
#[must_use]
pub fn authority_commit_digest(
    authority_id: AuthorityId,
    epoch: Epoch,
    sequence: Sequence,
    operation_id: OperationId,
    fingerprint: Digest,
    previous_digest: Digest,
    payload: &[u8],
) -> Digest {
    const DOMAIN: &[u8] = b"acyclic-fs-authority-commit-v1\0";
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    hasher.update(&authority_id.into_bytes());
    hasher.update(&epoch.get().to_le_bytes());
    hasher.update(&sequence.get().to_le_bytes());
    hasher.update(&operation_id.into_bytes());
    hasher.update(fingerprint.as_bytes());
    hasher.update(previous_digest.as_bytes());
    hasher.update(
        &u64::try_from(payload.len())
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hasher.update(payload);
    Digest::from_bytes(*hasher.finalize().as_bytes())
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

#[cfg(test)]
#[path = "tests/foundation.rs"]
#[cfg(any(not(unit_test_split), unit_test_foundation))]
mod tests;
