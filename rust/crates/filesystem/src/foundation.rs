//! Portable identities and authority contracts.

pub(crate) use acyclic_fs_authority_record::DurableCommit;
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
pub(crate) use acyclic_fs_format::authority::{Epoch, Sequence};

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

#[cfg(test)]
#[path = "tests/foundation.rs"]
mod tests;
