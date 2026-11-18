//! Canonical filesystem authority record framing for Stream storage.

use acyclic_fs_format::authority::{Epoch, Sequence};
use acyclic_fs_format::identity::{Digest, OperationId};
use bytes::Bytes;

pub mod streams_record;

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
