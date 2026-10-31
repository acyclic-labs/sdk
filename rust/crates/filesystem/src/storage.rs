//! Pluggable authority and immutable object storage interfaces.

use crate::foundation::{
    AuthorityId, Digest, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
};
use crate::performance::{MeasuredResult, OperationFailure};
use acyclic_stream::OwnedBytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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
#[cfg_attr(test, derive(strum::EnumCount))]
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
    #[error(
        "original publication reservation head is unavailable for authority {authority_id:?}, operation {operation_id:?}, gate {gate_tail}"
    )]
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
    /// BLAKE3 of canonical plaintext bytes.
    pub digest: Digest,
    /// Authenticated physical association, carried by the referring object.
    pub storage: Option<ObjectStorage>,
}

/// Physical location of lineage-sealed bytes. The referring object's hash binds
/// this association; the plaintext digest remains the filesystem identity.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct ObjectStorage {
    /// BLAKE3 of the exact sealed CAS payload.
    pub locator: Digest,
    /// Root directory identity selecting the sealing lineage.
    pub root_file_id: crate::FileId,
    /// Original generation authenticated by the seal, preserved when reused.
    pub origin_generation: Digest,
}

impl ObjectId {
    /// Largest canonical object-reference encoding.
    pub const ENCODED_LEN: usize = 113;

    /// Canonical bytes occupied by this reference.
    #[must_use]
    pub const fn encoded_len(self) -> usize {
        if self.storage.is_some() { 113 } else { 33 }
    }

    /// Parses one complete canonical reference, leaving subsequent fields unread.
    ///
    /// The returned suffix borrows the original input. Parsing performs no
    /// allocation and preserves the complete sealed storage association.
    pub fn from_prefix(bytes: &[u8]) -> Result<(Self, &[u8]), ObjectIdError> {
        let mut reader = ObjectPrefixReader { bytes };
        let object = Self::read_from(&mut reader)?;
        Ok((object, reader.bytes))
    }

    pub(crate) fn read_from<R: ObjectIdRead>(reader: &mut R) -> Result<Self, R::Error> {
        let tag = reader.tag()?;
        let digest = Digest::from_bytes(reader.fixed::<32>()?);
        let kind = ObjectKind::from_canonical_tag(tag & 127).map_err(|_| R::unknown_kind(tag))?;
        let storage = if tag & 128 == 0 {
            None
        } else {
            Some(ObjectStorage {
                locator: Digest::from_bytes(reader.fixed::<32>()?),
                root_file_id: crate::FileId::from_bytes(reader.fixed::<16>()?),
                origin_generation: Digest::from_bytes(reader.fixed::<32>()?),
            })
        };
        Ok(Self {
            kind,
            digest,
            storage,
        })
    }

    /// Returns the canonical typed reference, including its sealed association.
    #[must_use]
    pub fn to_bytes(self) -> ObjectIdBytes {
        let mut bytes = [0; Self::ENCODED_LEN];
        bytes[0] = self.kind.canonical_tag() | if self.storage.is_some() { 128 } else { 0 };
        bytes[1..33].copy_from_slice(self.digest.as_bytes());
        if let Some(storage) = self.storage {
            bytes[33..65].copy_from_slice(storage.locator.as_bytes());
            bytes[65..81].copy_from_slice(&storage.root_file_id.into_bytes());
            bytes[81..113].copy_from_slice(storage.origin_generation.as_bytes());
        }
        ObjectIdBytes {
            bytes,
            length: self.encoded_len(),
        }
    }

    /// Fixed-width index key from the canonical encoding, padded with zeros.
    pub(crate) fn index_key(self) -> [u8; Self::ENCODED_LEN] {
        self.to_bytes().bytes
    }
}

/// Stack-owned canonical object reference, with no allocation for serialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectIdBytes {
    bytes: [u8; ObjectId::ENCODED_LEN],
    length: usize,
}

impl ObjectIdBytes {
    /// Borrows the initialized canonical encoding.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}

impl std::ops::Deref for ObjectIdBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl TryFrom<&[u8]> for ObjectId {
    type Error = ObjectIdError;

    /// Parses a canonical typed reference and its optional sealed association.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let (object, remaining) = Self::from_prefix(bytes)?;
        if !remaining.is_empty() {
            return Err(ObjectIdError::Length);
        }
        Ok(object)
    }
}

/// Fixed-field admission belongs to the reader; identity framing belongs here.
pub(crate) trait ObjectIdRead {
    type Error;

    fn tag(&mut self) -> Result<u8, Self::Error> {
        self.fixed::<1>().map(|[tag]| tag)
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Self::Error>;
    fn unknown_kind(tag: u8) -> Self::Error;
}

struct ObjectPrefixReader<'a> {
    bytes: &'a [u8],
}

impl ObjectIdRead for ObjectPrefixReader<'_> {
    type Error = ObjectIdError;

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Self::Error> {
        let (field, remaining) = self
            .bytes
            .split_at_checked(N)
            .ok_or(ObjectIdError::Length)?;
        let value = field.try_into().map_err(|_| ObjectIdError::Length)?;
        self.bytes = remaining;
        Ok(value)
    }

    fn unknown_kind(tag: u8) -> Self::Error {
        ObjectKindError::Unknown(tag & 127).into()
    }
}

/// Canonical object-identity byte parsing failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ObjectIdError {
    /// The encoding has an invalid object-reference length.
    #[error("invalid object reference length")]
    Length,
    /// The tag byte names no known object class.
    #[error(transparent)]
    Kind(#[from] ObjectKindError),
}

/// Computes the canonical digest for one immutable object: the BLAKE3 of its
/// plaintext bytes. The kind and sealed storage association are part of [`ObjectId`].
#[must_use]
pub fn object_digest(bytes: &[u8]) -> Digest {
    Digest::from_bytes(acyclic_stream::content_digest(bytes))
}

/// Canonical immutable object bytes and the identity their hash proves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashedObject {
    kind: ObjectKind,
    sealed: acyclic_stream::SealedBytes,
    storage: Option<(ObjectStorage, acyclic_stream::SealedBytes)>,
}

impl HashedObject {
    /// Hashes `bytes` as one object of `kind`.
    #[must_use]
    pub fn new(kind: ObjectKind, bytes: acyclic_stream::OwnedBytes) -> Self {
        Self {
            kind,
            storage: None,
            sealed: acyclic_stream::SealedBytes::new(bytes),
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
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        let hash = crate::WorkCounters {
            bytes_hashed: u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?,
            ..crate::WorkCounters::default()
        };
        work.admit(hash)?;
        let sealed = acyclic_stream::SealedBytes::new(bytes);
        work.record_admitted(hash)?;
        work.admit(crate::WorkCounters::default())?;
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        Ok(Self {
            kind,
            sealed,
            storage: None,
        })
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
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        let capacity = u64::try_from(bytes.capacity()).map_err(|_| crate::WorkError::Overflow)?;
        if !work.shares_ledger(allocation.work()) || allocation.bytes() < capacity {
            return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout.into());
        }
        let hash = crate::WorkCounters {
            bytes_hashed: u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?,
            ..crate::WorkCounters::default()
        };
        work.admit(hash)?;
        let payload_allocation = allocation.split_off(capacity)?;
        let payload_original = payload_allocation.work().clone();
        let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            std::mem::take(bytes),
            payload_allocation,
            payload_original.runtime_meter(),
        )
        .map_err(|failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                payload_original.runtime_error(error).into()
            }
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                ObjectStoreError::Native(crate::NativeTreeMaterializationError::InvalidLayout)
            }
        })?;
        let sealed = acyclic_stream::SealedBytes::new(bytes);
        work.record_admitted(hash)?;
        work.admit(crate::WorkCounters::default())?;
        cancellation
            .check()
            .map_err(|_| ObjectStoreError::Cancelled)?;
        Ok(Self {
            kind,
            sealed,
            storage: None,
        })
    }

    /// Admits `bytes` under `object_id` only if they hash to it.
    #[cfg(all(
        any(test, feature = "local", feature = "conformance"),
        not(target_arch = "wasm32")
    ))]
    pub(crate) fn verify(object_id: ObjectId, bytes: acyclic_stream::OwnedBytes) -> Option<Self> {
        let object = Self::new(object_id.kind, bytes);
        (object.object_id() == object_id).then_some(object)
    }

    /// The identity the bytes hash to.
    #[must_use]
    pub const fn object_id(&self) -> ObjectId {
        ObjectId {
            kind: self.kind,
            storage: match &self.storage {
                Some((storage, _)) => Some(*storage),
                None => None,
            },
            digest: Digest::from_bytes(*self.sealed.digest()),
        }
    }

    /// Exact canonical length.
    #[must_use]
    pub fn length(&self) -> u64 {
        u64::try_from(self.sealed.bytes().len()).unwrap_or(u64::MAX)
    }

    /// The canonical bytes.
    #[must_use]
    pub const fn bytes(&self) -> &acyclic_stream::OwnedBytes {
        self.sealed.bytes()
    }

    /// The bytes with the digest their hash proved, for layers that reuse it.
    #[must_use]
    pub const fn sealed(&self) -> &acyclic_stream::SealedBytes {
        match &self.storage {
            Some((_, sealed)) => sealed,
            None => &self.sealed,
        }
    }

    /// Releases the identity and the sealed bytes.
    #[must_use]
    pub fn into_sealed(self) -> (ObjectId, acyclic_stream::SealedBytes) {
        (
            self.object_id(),
            match self.storage {
                Some((_, sealed)) => sealed,
                None => self.sealed,
            },
        )
    }

    /// Exact bytes submitted to CAS after optional lineage sealing.
    #[must_use]
    pub fn stored_bytes(&self) -> &acyclic_stream::OwnedBytes {
        self.sealed().bytes()
    }

    /// Binds a physical hash proof to this already authenticated plaintext.
    #[must_use]
    pub fn seal(
        mut self,
        sealed: acyclic_stream::SealedBytes,
        root_file_id: crate::FileId,
        origin_generation: crate::GenerationId,
    ) -> Self {
        self.storage = Some((
            ObjectStorage {
                locator: Digest::from_bytes(*sealed.digest()),
                root_file_id,
                origin_generation: origin_generation.digest(),
            },
            sealed,
        ));
        self
    }

    /// Releases the identity and bytes.
    #[must_use]
    pub fn into_parts(self) -> (ObjectId, acyclic_stream::OwnedBytes) {
        (self.object_id(), self.sealed.into_bytes())
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
    #[cfg(all(any(test, feature = "distributed"), not(target_arch = "wasm32")))]
    #[error(transparent)]
    NativeObjects(#[from] acyclic_objects::NativeObjectsError),
    /// The logical Objects provider returned its canonical typed category.
    #[cfg(any(test, feature = "distributed"))]
    #[error(transparent)]
    LogicalObjects(#[from] acyclic_objects::Error),
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod tests;

#[cfg(test)]
mod object_id_codec_tests {
    use super::*;

    #[test]
    fn canonical_object_prefix_keeps_every_kind_seal_and_following_field()
    -> Result<(), ObjectIdError> {
        for kind in [
            ObjectKind::Blob,
            ObjectKind::BlobChunk,
            ObjectKind::TreePage,
            ObjectKind::ExtentPage,
            ObjectKind::FileTablePage,
            ObjectKind::GenerationRoot,
            ObjectKind::Metadata,
            ObjectKind::AttributePage,
        ] {
            for storage in [
                None,
                Some(ObjectStorage {
                    locator: Digest::from_bytes([11; 32]),
                    root_file_id: crate::FileId::from_bytes([13; 16]),
                    origin_generation: Digest::from_bytes([17; 32]),
                }),
            ] {
                let id = ObjectId {
                    kind,
                    digest: Digest::from_bytes([7; 32]),
                    storage,
                };
                let bytes = id.to_bytes();
                assert_eq!(ObjectId::try_from(bytes.as_slice()), Ok(id));
                for length in 0..bytes.len() {
                    assert_eq!(
                        ObjectId::from_prefix(&bytes[..length]),
                        Err(ObjectIdError::Length)
                    );
                }
                let following = id.to_bytes();
                let record = [bytes.as_slice(), following.as_slice()].concat();
                let (decoded, remaining) = ObjectId::from_prefix(&record)?;
                assert_eq!(decoded, id);
                assert_eq!(remaining, following.as_slice());
                assert_eq!(
                    ObjectId::try_from(record.as_slice()),
                    Err(ObjectIdError::Length)
                );

                let mut unknown = bytes.to_vec();
                unknown[0] &= 128;
                assert_eq!(
                    ObjectId::from_prefix(&unknown),
                    Err(ObjectIdError::Kind(ObjectKindError::Unknown(0)))
                );
                if let Some(mut resealed) = storage {
                    // A plain digest cannot stand in for its authenticated association.
                    let plain = ObjectId {
                        storage: None,
                        ..id
                    };
                    assert_ne!(ObjectId::from_prefix(plain.to_bytes().as_slice())?.0, id);
                    resealed.locator = Digest::from_bytes([19; 32]);
                    let other = ObjectId {
                        storage: Some(resealed),
                        ..id
                    };
                    assert_ne!(ObjectId::try_from(other.to_bytes().as_slice())?, id);
                }
            }
        }
        Ok(())
    }
}
