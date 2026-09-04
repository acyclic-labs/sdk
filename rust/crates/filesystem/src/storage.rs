//! Pluggable authority and immutable object storage interfaces.

use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::performance::{MeasuredResult, OperationFailure, WorkBudget, WorkCounters};
use bytes::Bytes;
use std::sync::Arc;
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

/// Storage-only authority interface.
///
/// Implementations own ordering, fencing, idempotency, durable acknowledgement,
/// and bounded replay. They do not interpret commit payloads.
pub trait AuthorityStore: Send + Sync {
    /// Creates one authority at the supplied non-zero genesis epoch.
    ///
    /// # Errors
    ///
    /// Returns a typed backend error if creation cannot be made durable.
    fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
    ) -> AuthorityResult<CreateAuthorityOutcome>;

    /// Returns the linearizable current head.
    ///
    /// # Errors
    ///
    /// Returns a typed backend error when the authority is missing, corrupt,
    /// unavailable, or cannot be read durably.
    fn head(&self, authority_id: AuthorityId, budget: WorkBudget) -> AuthorityResult<Head>;

    /// Atomically checks epoch/head/idempotency and durably appends one operation.
    ///
    /// # Errors
    ///
    /// Returns a typed backend error when durable evaluation cannot complete.
    /// Semantic conflicts are returned as [`AppendOutcome`] variants.
    fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
    ) -> AuthorityResult<AppendOutcome>;

    /// Replays a contiguous bounded page after `sequence`.
    ///
    /// # Errors
    ///
    /// Returns a typed backend error for invalid limits, missing/corrupt
    /// authority state, or storage failure.
    fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
    ) -> AuthorityResult<Vec<DurableCommit>>;

    /// Atomically compares the complete head, advances its epoch, and
    /// permanently fences prior writers.
    ///
    /// # Errors
    ///
    /// A stale expected head is a semantic [`FenceOutcome::Conflict`], while a
    /// backend failure means the fence could not be durably resolved.
    fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
    ) -> AuthorityResult<FenceOutcome>;

    /// Looks up a durable operation for exact retry resolution.
    ///
    /// # Errors
    ///
    /// Returns a typed backend error when the authority cannot be read safely.
    fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
    ) -> AuthorityResult<Option<DurableCommit>>;
}

impl<T: AuthorityStore + ?Sized> AuthorityStore for Arc<T> {
    fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        (**self).create_authority(authority_id, genesis_epoch, budget)
    }

    fn head(&self, authority_id: AuthorityId, budget: WorkBudget) -> AuthorityResult<Head> {
        (**self).head(authority_id, budget)
    }

    fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
    ) -> AuthorityResult<AppendOutcome> {
        (**self).compare_and_append(authority_id, epoch, expected, commit, budget)
    }

    fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        (**self).replay(authority_id, after, limit, budget)
    }

    fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
    ) -> AuthorityResult<FenceOutcome> {
        (**self).fence(authority_id, expected, budget)
    }

    fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
    ) -> AuthorityResult<Option<DurableCommit>> {
        (**self).find_operation(authority_id, operation_id, budget)
    }
}

/// One successful authority operation and its exact backend work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityReceipt<T> {
    /// Operation value.
    pub value: T,
    /// Exact backend work.
    pub work: WorkCounters,
}

/// One failed authority operation retaining exact spent work.
pub type AuthorityFailure = OperationFailure<AuthorityStoreError>;

/// Receipt-bearing authority result.
pub type AuthorityResult<T> = MeasuredResult<AuthorityReceipt<T>, AuthorityStoreError>;

/// Typed authority storage failures.
#[derive(Debug, Error)]
pub enum AuthorityStoreError {
    /// Operation was cancelled before durable authority work began.
    #[error("authority operation was cancelled")]
    Cancelled,
    /// The authority does not exist.
    #[error("authority does not exist")]
    Missing,
    /// A configured replay bound is invalid.
    #[error("replay limit must have non-zero record and payload bounds")]
    InvalidReplayLimit,
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
    ///
    /// # Errors
    ///
    /// Rejects unknown tags instead of guessing a future object class.
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

/// Exact bounded logical byte range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    /// Inclusive start offset.
    pub offset: u64,
    /// Exact requested byte count.
    pub length: u64,
}

/// One successful immutable-object operation and its exact physical work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReceipt<T> {
    /// Operation value.
    pub value: T,
    /// Exact backend work, including zero-copy versus copied-byte behavior.
    pub work: WorkCounters,
}

/// How the bytes returned by one immutable-object read remain resident.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectReadRetention {
    /// The returned view shares backend-owned immutable storage and adds no
    /// operation-owned byte buffer.
    Shared,
    /// The backend allocated this exact logical byte capacity and transfers
    /// ownership to the caller until the value is dropped.
    Owned {
        /// Exact requested capacity retained by the returned value.
        logical_bytes: u64,
    },
}

/// Immutable bytes plus explicit retention evidence for zero-copy and peak
/// allocation accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRead {
    /// Complete canonical object bytes.
    pub bytes: Bytes,
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

impl std::ops::Deref for ObjectRead {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

/// One failed immutable-object operation and all work completed before failure.
pub type ObjectFailure = OperationFailure<ObjectStoreError>;

/// Receipt-bearing immutable-object result. Failure never erases spent work.
pub type ObjectResult<T> = MeasuredResult<ObjectReceipt<T>, ObjectStoreError>;

/// Immutable digest-addressed object storage interface.
///
/// Objects are deliberately read whole. Pages and blob chunks have hard size
/// limits; logical range reads are built over authenticated chunk indexes. This
/// prevents a backend from hiding a whole-object scan behind a tiny range read.
pub trait ObjectStore: Send + Sync {
    /// Admits canonical bytes under their verified identity.
    ///
    /// # Errors
    ///
    /// Returns a typed error for digest mismatch or storage failure.
    fn put(&self, object_id: ObjectId, bytes: Bytes, budget: WorkBudget) -> ObjectResult<()>;

    /// Reads exactly the requested range or returns a typed error.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the object is absent, corrupt, unavailable,
    /// or the range is invalid.
    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead>;

    /// Reads an ordered batch of complete bounded objects.
    ///
    /// The result position always matches the request position. Implementations
    /// should coalesce physical transactions or I/O submission where their
    /// backend supports it. The default preserves exact semantics and work
    /// accounting without claiming fewer backend operations.
    ///
    /// # Errors
    ///
    /// Returns the first typed object failure and all work completed before it.
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
    ) -> ObjectResult<Vec<ObjectRead>>;

    /// Returns whether the exact typed object is present and verified.
    ///
    /// # Errors
    ///
    /// Returns a typed error when presence cannot be checked safely.
    fn contains(&self, object_id: ObjectId, budget: WorkBudget) -> ObjectResult<bool>;
}

impl<T: ObjectStore + ?Sized> ObjectStore for Arc<T> {
    fn put(&self, object_id: ObjectId, bytes: Bytes, budget: WorkBudget) -> ObjectResult<()> {
        (**self).put(object_id, bytes, budget)
    }

    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
    ) -> ObjectResult<ObjectRead> {
        (**self).read(object_id, maximum_bytes, budget)
    }

    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
    ) -> ObjectResult<Vec<ObjectRead>> {
        (**self).read_many(requests, budget)
    }

    fn contains(&self, object_id: ObjectId, budget: WorkBudget) -> ObjectResult<bool> {
        (**self).contains(object_id, budget)
    }
}

/// Typed immutable-object failures.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// Operation was cancelled before immutable-object work began.
    #[error("object operation was cancelled")]
    Cancelled,
    /// Exact typed object is absent.
    #[error("object is missing")]
    Missing,
    /// Supplied bytes do not match the typed object digest.
    #[error("object digest does not match canonical bytes")]
    DigestMismatch,
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
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod tests;
