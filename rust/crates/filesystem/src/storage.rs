//! Pluggable authority and immutable object storage interfaces.

use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::performance::{MeasuredResult, OperationFailure, WorkCounters};
use acyclic_stream::OwnedBytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical bytes hashed in addition to one object's body.
pub const OBJECT_DIGEST_ENVELOPE_BYTES: u64 = b"acyclic-fs-object-v1\0".len() as u64 + 1 + 8;

/// Bounds one replay request before storage allocates or returns bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayLimit {
    /// Maximum records returned.
    pub records: u32,
    /// Maximum cumulative payload bytes returned.
    pub payload_bytes: u64,
}

/// Idempotent authority creation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateAuthorityOutcome {
    /// A new empty authority was durably created.
    Created(Head),
    /// The authority already existed and was not changed.
    Existing(Head),
}

/// Successful or rejected compare-and-append result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    /// The proposed operation became durable at this commit.
    Committed(DurableCommit),
    /// The same operation and fingerprint was already durable.
    AlreadyCommitted(DurableCommit),
    /// The expected head was stale; no bytes were appended.
    Conflict {
        /// Linearizable current head.
        actual: Head,
    },
    /// The caller epoch was stale; no bytes were appended.
    Fenced {
        /// Active authority epoch.
        actual_epoch: Epoch,
    },
    /// The operation identity exists with a different fingerprint.
    IdempotencyConflict {
        /// Fingerprint already bound to the operation identity.
        committed_fingerprint: Digest,
    },
}

/// Optional authority-side admission condition for one publication.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PublicationPermit {
    /// No operation-window admission is required.
    Unrestricted,
    /// The exact durable tool lease must still be active.
    Lease {
        /// Authority derived from the admitted workspace identity.
        authority_id: [u8; 16],
        /// Workspace whose mount admitted the operation.
        workspace_id: [u8; 16],
        /// Unique durable lease identity.
        lease_id: [u8; 16],
        /// Provider-evaluated exclusive publication deadline.
        expires_at_millis: u64,
    },
    /// The exact durable multi-authority publication reservation is active.
    Reservation {
        /// Stable logical publication identity.
        operation_id: [u8; 16],
        /// Tail produced when the authority gate was reserved.
        gate_tail: u64,
        /// Exact authority head authenticated when the gate was acquired.
        expected: Head,
    },
}

/// Durable exclusive authority reservation used by multi-root publication.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublicationReservation {
    /// Authority protected by the reservation.
    pub authority_id: AuthorityId,
    /// Stable logical publication identity.
    pub operation_id: OperationId,
    /// Exact authority head validated while acquiring the gate.
    pub expected: Head,
    /// Gate tail that authenticates the reservation holder.
    pub gate_tail: u64,
}

/// Result of atomically reserving an authority at an exact head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationOutcome {
    /// This operation owns the exclusive gate.
    Reserved(PublicationReservation),
    /// The identical reservation was already acquired.
    AlreadyReserved(PublicationReservation),
    /// The authority head or gate changed before acquisition.
    Conflict {
        /// Linearizable head observed while acquisition failed.
        actual: Head,
    },
}

/// Complete atomic authority append request, including any admission permit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuardedAppend {
    /// Authority receiving the operation.
    pub authority_id: AuthorityId,
    /// Writer epoch expected by the caller.
    pub epoch: Epoch,
    /// Complete expected authority head.
    pub expected: Head,
    /// Canonical durable operation.
    pub commit: ProposedCommit,
    /// Optional operation-window admission requirement.
    pub permit: PublicationPermit,
}

/// Successful or rejected compare-and-fence result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FenceOutcome {
    /// The exact expected head was atomically advanced to a fresh writer epoch.
    Advanced(Head),
    /// The authority changed before ownership could be acquired.
    Conflict {
        /// Linearizable current head; no fence was advanced.
        actual: Head,
    },
}


/// One failed authority operation retaining exact spent work.
pub type AuthorityFailure = OperationFailure<AuthorityStoreError>;

/// Receipt-bearing authority result.
pub type AuthorityResult<T> = MeasuredResult<crate::OperationReceipt<T>, AuthorityStoreError>;

/// Typed authority storage failures.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum AuthorityStoreError {
    /// Operation was cancelled before durable authority work began.
    #[error("authority operation was cancelled")]
    Cancelled,
    /// This custom provider cannot fund the canonical producer in the original native allocation ledger.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("authority provider does not support original native work")]
    NativeOriginalUnsupported,
    /// A failure from the caller's actual original native work owner.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeOriginal(#[from] crate::materializer::NativeTreeMaterializationError),
    /// Canonical SDK failure without a formatted diagnostic allocation.
    #[error(transparent)]
    NativeStream(#[from] acyclic_stream::StreamError),
    /// Canonical durable record validation preserves its actual typed cause.
    #[error(transparent)]
    DurableCodec(#[from] crate::streams_record::StreamsAuthorityRecordError),
    /// Fixed canonical corruption reason needs no owned diagnostic buffer.
    #[error("authority storage is corrupt: {0}")]
    CorruptStatic(&'static str),
    /// Fixed canonical rejection reason needs no owned diagnostic buffer.
    #[error("authority backend rejected operation: {0}")]
    RejectedStatic(&'static str),
    /// The authority does not exist.
    #[error("authority does not exist")]
    Missing,
    /// The authority was released for good with its deleted workspace.
    #[error("authority was retired with its deleted workspace")]
    Retired,
    /// A configured replay bound is invalid.
    #[error("replay limit must have non-zero record and payload bounds")]
    InvalidReplayLimit,
    /// The memory authority mutex cannot safely expose its state.
    #[error("authority mutex poisoned")]
    MemoryAuthorityPoisoned,
    /// Replay starts beyond the canonical durable head.
    #[error("replay cursor is ahead of authority head")]
    ReplayCursorAhead {
        /// Requested exclusive replay cursor.
        after: Sequence,
        /// Current durable sequence.
        head: Sequence,
    },
    /// Replay's sequence cannot be represented by this process.
    #[error("replay cursor does not fit process")]
    ReplayCursorProcessOverflow {
        /// Requested exclusive replay cursor.
        after: Sequence,
    },
    /// The actual replay metadata allocator rejected its admitted reservation.
    #[error("memory replay allocation failed: {0}")]
    MemoryReplayAllocationFailed(#[source] std::collections::TryReserveError),
    /// One authority record cannot fit into the requested replay page.
    #[error("next authority record has {observed} payload bytes; replay maximum is {maximum}")]
    ReplayRecordTooLarge {
        /// Next record payload bytes.
        observed: u64,
        /// Requested page payload bound.
        maximum: u64,
    },
    /// One submitted authority payload exceeds the backend admission bound.
    #[error("authority payload has {observed} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Submitted payload bytes.
        observed: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// Epoch arithmetic cannot represent another fence.
    #[error("authority epoch exhausted")]
    EpochExhausted,
    /// Sequence arithmetic cannot represent another authority fact.
    #[error("authority sequence exhausted")]
    SequenceExhausted,
    /// A surviving reservation identity lacks authenticated original head evidence.
    #[error("original publication reservation head is unavailable for authority {authority_id:?}, operation {operation_id:?}, gate {gate_tail}")]
    ReservationOriginalHeadUnavailable {
        /// Original authority selected by the reservation.
        authority_id: AuthorityId,
        /// Original caller-owned operation identity.
        operation_id: OperationId,
        /// Observed retained gate tail, not a newly generated proof.
        gate_tail: u64,
    },
    /// Persistent state is corrupt and cannot be safely interpreted.
    #[error("authority storage is corrupt: {0}")]
    Corrupt(String),
    /// Persistent I/O failed.
    #[error("authority storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Durable state may have changed, so the caller must reopen and resolve by identity.
    #[error("authority {operation} outcome is indeterminate: {source}")]
    Indeterminate {
        /// Durable operation whose acknowledgement was lost.
        operation: &'static str,
        /// Underlying storage failure after mutation may have begun.
        #[source]
        source: std::io::Error,
    },
    /// Backend rejected an operation for a stable implementation-specific reason.
    #[error("authority backend rejected operation: {0}")]
    Rejected(String),
    /// Backend work could not be admitted within the caller's hard budget.
    #[error(transparent)]
    Work(#[from] crate::performance::WorkError),
}

/// Immutable object class. Hashes are scoped by both kind and canonical bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObjectKind {
    /// Authenticated blob-index page.
    Blob,
    /// Fixed-size immutable content chunk addressed by its complete bytes.
    BlobChunk,
    /// Authenticated namespace page.
    TreePage,
    /// Authenticated file-extent page.
    ExtentPage,
    /// Authenticated file-table page.
    FileTablePage,
    /// Composite immutable generation root.
    GenerationRoot,
    /// Canonical metadata object.
    Metadata,
    /// Authenticated named extended-attribute/stream page.
    AttributePage,
}

impl ObjectKind {
    /// Returns the stable canonical object-class tag.
    #[must_use]
    pub const fn canonical_tag(self) -> u8 {
        match self {
            Self::Blob => 1,
            Self::BlobChunk => 2,
            Self::TreePage => 3,
            Self::ExtentPage => 4,
            Self::FileTablePage => 5,
            Self::GenerationRoot => 6,
            Self::Metadata => 7,
            Self::AttributePage => 8,
        }
    }

    /// Parses one stable canonical object-class tag.
    pub const fn from_canonical_tag(tag: u8) -> Result<Self, ObjectKindError> {
        match tag {
            1 => Ok(Self::Blob),
            2 => Ok(Self::BlobChunk),
            3 => Ok(Self::TreePage),
            4 => Ok(Self::ExtentPage),
            5 => Ok(Self::FileTablePage),
            6 => Ok(Self::GenerationRoot),
            7 => Ok(Self::Metadata),
            8 => Ok(Self::AttributePage),
            value => Err(ObjectKindError::Unknown(value)),
        }
    }
}

/// Canonical object-kind parsing failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectKindError {
    /// A newer or corrupt object used an unknown kind tag.
    #[error("unknown object kind tag {0}")]
    Unknown(u8),
}

/// Typed object identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectId {
    /// Object class.
    pub kind: ObjectKind,
    /// Digest over object class and canonical bytes.
    pub digest: Digest,
}

impl ObjectId {
    /// Length of the canonical `kind tag || digest` byte encoding.
    pub const ENCODED_LEN: usize = 33;

    /// Returns the canonical `kind tag || digest` byte encoding.
    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0; Self::ENCODED_LEN];
        bytes[0] = self.kind.canonical_tag();
        bytes[1..].copy_from_slice(self.digest.as_bytes());
        bytes
    }
}

impl TryFrom<&[u8]> for ObjectId {
    type Error = ObjectIdError;

    /// Parses the canonical `kind tag || digest` byte encoding.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let Some((&tag, digest)) = bytes.split_first() else {
            return Err(ObjectIdError::Length);
        };
        let digest = <[u8; 32]>::try_from(digest).map_err(|_| ObjectIdError::Length)?;
        Ok(Self {
            kind: ObjectKind::from_canonical_tag(tag)?,
            digest: Digest::from_bytes(digest),
        })
    }
}

/// Canonical object-identity byte parsing failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectIdError {
    /// The encoding is not exactly one tag byte and one 32-byte digest.
    #[error("object identity must be exactly 33 bytes")]
    Length,
    /// The tag byte names no known object class.
    #[error(transparent)]
    Kind(#[from] ObjectKindError),
}

/// Computes the canonical digest for one typed immutable object.
#[must_use]
pub fn object_digest(kind: ObjectKind, bytes: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-object-v1\0");
    hasher.update(&[kind.canonical_tag()]);
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

/// Canonical immutable object bytes and the identity their hash proves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashedObject {
    object_id: ObjectId,
    bytes: acyclic_stream::OwnedBytes,
}


impl HashedObject {
    /// Hashes `bytes` as one object of `kind`.
    #[must_use]
    pub fn new(kind: ObjectKind, bytes: acyclic_stream::OwnedBytes) -> Self {
        Self {
            object_id: ObjectId {
                kind,
                digest: object_digest(kind, &bytes),
            },
            bytes,
        }
    }

    /// Hashes an existing immutable owner without claiming its body again.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        kind: ObjectKind,
        bytes: acyclic_stream::OwnedBytes,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        let hash = WorkCounters {
            bytes_hashed: u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(crate::WorkError::Overflow)?,
            ..WorkCounters::default()
        };
        work.admit(hash)?;
        let object_id = ObjectId { kind, digest: object_digest(kind, &bytes) };
        work.record_admitted(hash)?;
        work.admit(WorkCounters::default())?;
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        Ok(Self { object_id, bytes })
    }

    /// Transfers a guarded vector into the physical immutable byte owner.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_native_vec(
        kind: ObjectKind,
        bytes: &mut Vec<u8>,
        allocation: &mut crate::materializer::NativeAllocation,
        work: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        let capacity = u64::try_from(bytes.capacity()).map_err(|_| crate::WorkError::Overflow)?;
        if !work.shares_ledger(allocation.work()) || allocation.bytes() < capacity {
            return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout.into());
        }
        let hash = WorkCounters {
            bytes_hashed: u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(crate::WorkError::Overflow)?,
            ..WorkCounters::default()
        };
        work.admit(hash)?;
        let object_id = ObjectId { kind, digest: object_digest(kind, bytes) };
        work.record_admitted(hash)?;
        work.admit(WorkCounters::default())?;
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        let payload_allocation = allocation.split_off(capacity)?;
        let payload_original = payload_allocation.work().clone();
        let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            std::mem::take(bytes), payload_allocation, payload_original.runtime_meter(),
        ).map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => payload_original.runtime_error(error).into(),
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            => {
                ObjectStoreError::Native(crate::NativeTreeMaterializationError::InvalidLayout)
            }
        })?;
        Ok(Self { object_id, bytes })
    }

    /// Admits `bytes` under `object_id` only if they hash to it.
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) fn verify(object_id: ObjectId, bytes: acyclic_stream::OwnedBytes) -> Option<Self> {
        (object_digest(object_id.kind, &bytes) == object_id.digest)
            .then_some(Self { object_id, bytes })
    }

    /// The identity the bytes hash to.
    #[must_use]
    pub const fn object_id(&self) -> ObjectId {
        self.object_id
    }

    /// Exact canonical length.
    #[must_use]
    pub fn length(&self) -> u64 {
        u64::try_from(self.bytes.len()).unwrap_or(u64::MAX)
    }

    /// The canonical bytes.
    #[must_use]
    pub const fn bytes(&self) -> &acyclic_stream::OwnedBytes {
        &self.bytes
    }

    /// Releases the identity and bytes.
    #[must_use]
    pub fn into_parts(self) -> (ObjectId, acyclic_stream::OwnedBytes) {
        (self.object_id, self.bytes)
    }
}

/// Exact bounded logical byte range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    /// Inclusive start offset.
    pub offset: u64,
    /// Exact requested byte count.
    pub length: u64,
}

/// How the bytes returned by one immutable-object read remain resident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectReadRetention {
    /// The returned view shares backend-owned immutable storage and adds no operation-owned byte buffer.
    Shared,
    /// The backend allocated this exact logical byte capacity and transfers ownership to the caller until the value is dropped.
    Owned {
        /// Exact requested capacity retained by the returned value.
        logical_bytes: u64,
    },
}

/// Immutable bytes plus explicit retention evidence for zero-copy and peak allocation accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRead {
    /// Complete canonical object bytes.
    pub bytes: OwnedBytes,
    /// Whether those bytes are a shared view or an owned returned buffer.
    pub retention: ObjectReadRetention,
}

/// One complete immutable object requested as part of an ordered batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectReadRequest {
    /// Exact typed immutable identity.
    pub object_id: ObjectId,
    /// Maximum canonical bytes admitted for this object.
    pub maximum_bytes: u64,
}

/// One canonical immutable object submitted as part of an ordered write batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectWrite {
    /// Exact typed immutable identity.
    pub object_id: ObjectId,
    /// Canonical bytes authenticated by `object_id`.
    pub bytes: OwnedBytes,
}

impl std::ops::Deref for ObjectRead {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

/// One failed immutable-object operation and all work completed before failure.
pub type ObjectFailure = OperationFailure<ObjectStoreError>;

/// Receipt-bearing immutable-object result. Failure never erases spent work.
pub type ObjectResult<T> = MeasuredResult<crate::OperationReceipt<T>, ObjectStoreError>;


/// Typed immutable-object failures.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum ObjectStoreError {
    /// Operation was cancelled before immutable-object work began.
    #[error("object operation was cancelled")]
    Cancelled,
    /// A simulator emitted its actual static interceptor failure.
    #[error("simulation: {0}")]
    Simulation(&'static str),
    /// Exact typed object is absent.
    #[error("object is missing")]
    Missing,
    /// Supplied bytes do not match the typed object digest.
    #[error("object digest does not match canonical bytes")]
    DigestMismatch,
    /// An immutable write batch contains no objects.
    #[error("object write batch is empty")]
    EmptyWriteBatch,
    /// An immutable read batch contains no requests.
    #[error("object read batch is empty")]
    EmptyReadBatch,
    /// SDK buffer custody failed without an allocator-provided error.
    #[error("SDK buffer allocation or custody failed")]
    SdkAllocationFailed,
    /// The resident table or read-result allocator rejected the exact reserve.
    #[error("memory object allocation failed: {0}")]
    MemoryTableAllocationFailed(#[source] std::collections::TryReserveError),
    /// Object exceeds the backend's declared bound.
    #[error("object is {observed} bytes; maximum is {maximum}")]
    TooLarge {
        /// Observed canonical byte length.
        observed: u64,
        /// Configured maximum canonical byte length.
        maximum: u64,
    },
    /// Requested range is invalid or outside the object.
    #[error("object range is outside the admitted object")]
    InvalidRange,
    /// Stored bytes fail verification.
    #[error("stored object is corrupt")]
    Corrupt,
    /// Corrupt bytes could not be isolated from the live object namespace.
    #[error("corrupt object quarantine failed: {0}")]
    QuarantineFailed(#[source] std::io::Error),
    /// Storage I/O failed.
    #[error("object storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Backend rejected an operation for a stable implementation-specific reason.
    #[error("object backend rejected operation: {0}")]
    Rejected(String),
    /// Backend work could not be admitted without exceeding the caller budget.
    #[error(transparent)]
    Work(#[from] crate::performance::WorkError),
    /// This custom provider has no original-native accounting capability.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("object provider does not support original native accounting")]
    NativeUnavailable,
    /// The original native journal rejected or failed the operation.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Native(#[from] crate::materializer::NativeTreeMaterializationError),
    /// The original Objects failure, including every acknowledged prefix and intrinsically owned diagnostic or failed request packet.
    #[cfg(all(feature = "distributed", not(target_arch = "wasm32")))]
    #[error(transparent)]
    NativeObjects(#[from] acyclic_objects::v1::NativeObjectsError),
    /// The logical Objects provider returned its canonical typed category.
    #[cfg(feature = "distributed")]
    #[error(transparent)]
    LogicalObjects(#[from] acyclic_objects::v1::Error),
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod tests;

#[cfg(test)]
mod object_id_codec_tests {
    use super::*;

    #[test]
    fn object_id_bytes_round_trip_and_reject_malformed_input() {
        let id = ObjectId {
            kind: ObjectKind::TreePage,
            digest: Digest::from_bytes([7; 32]),
        };
        let bytes = id.to_bytes();
        assert_eq!(bytes[0], 3);
        assert_eq!(&bytes[1..], &[7; 32]);
        assert_eq!(ObjectId::try_from(&bytes[..]), Ok(id));
        for length in [0, 1, 32, 34] {
            assert_eq!(
                ObjectId::try_from(&vec![1; length][..]),
                Err(ObjectIdError::Length)
            );
        }
        let mut unknown = bytes;
        unknown[0] = 0;
        let error = ObjectId::try_from(&unknown[..]);
        assert_eq!(error, Err(ObjectIdError::Kind(ObjectKindError::Unknown(0))));
        assert_eq!(
            ObjectIdError::Length.to_string(),
            "object identity must be exactly 33 bytes"
        );
    }
}
