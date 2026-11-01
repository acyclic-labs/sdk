//! Whole-generation authentication before authority publication.

use super::{
    AttributeName, BlobNode, DecodeLimits, ExtentKind, ExtentPage, FilePayload, FileRecord,
    FileTablePage, GenerationRoot, LogicalName, MetadataField, TreeEntry, TreePage,
    decode_file_metadata,
};
use crate::CancellationToken;
use crate::foundation::{FileId, GenerationId, usize_to_u64};
use crate::model::FilesystemProfile;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ObjectKind, ObjectStoreError};
use std::mem::size_of;
use std::sync::Mutex;
use thiserror::Error;

/// Canonical shape and semantic capabilities required before publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClosureLimits {
    /// Canonical decoder/page limits.
    pub decode: DecodeLimits,
    /// Exact namespace profile whose representable kinds are admitted.
    pub profile: FilesystemProfile,
    /// Whether symbolic-link records are admitted.
    pub symbolic_links: bool,
    /// Whether more than one namespace binding may reference a non-directory.
    pub hard_links: bool,
    /// Whether holes or allocated-zero extents are admitted.
    pub sparse_files: bool,
}

/// Successful whole-generation proof.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationProof {
    /// Decoded immutable root.
    pub root: GenerationRoot,
    /// Content-addressed generation identity.
    pub generation_id: GenerationId,
    /// Reachable path-independent file records.
    pub file_count: u64,
    /// Logical regular-file bytes, counting each file identity once, including holes.
    pub logical_file_bytes: u128,
    /// Immutable objects authenticated by this proof. Ordinary proofs use sorted
    /// identities; publication proofs order dependencies before their parents.
    pub objects: Vec<ObjectId>,
    /// Exact proof work.
    pub work: WorkCounters,
}

/// Generation-closure failure retaining all work spent before rejection.
pub type GenerationProofFailure = OperationFailure<ClosureError>;

/// Whole-generation proof failures.
#[derive(Debug, Error)]
pub enum ClosureError {
    /// Typed object reference has the wrong class.
    #[error("generation closure object has the wrong kind")]
    WrongObjectKind,
    /// Same object was encountered twice where the graph requires a tree.
    #[error("routing graph contains a cycle or alias")]
    RoutingAlias,
    /// Parent lower/upper bounds do not match authenticated child content.
    #[error("routing bounds do not match authenticated child content")]
    RoutingBoundsMismatch,
    /// The sum of logical regular-file sizes cannot be represented.
    #[error("generation logical file byte total overflowed")]
    LogicalBytesOverflow,
    /// File table contains the same stable identity twice.
    #[error("duplicate file record {0:?}")]
    DuplicateFileRecord(FileId),
    /// Root file record is absent.
    #[error("root file record is missing")]
    MissingRootFile,
    /// Root file is not a directory.
    #[error("root file record is not a directory")]
    RootIsNotDirectory,
    /// A record or extent exceeds the volume's declared semantic capabilities.
    #[error("generation closure uses semantics unsupported by its volume configuration")]
    UnsupportedVolumeSemantics,
    /// Namespace references an absent file record.
    #[error("namespace references missing file record {0:?}")]
    MissingFileRecord(FileId),
    /// Namespace entry kind differs from its file record.
    #[error("namespace kind differs from file record {0:?}")]
    NamespaceKindMismatch(FileId),
    /// Recorded hard-link count differs from exact namespace references.
    #[error("file link count is incorrect for {0:?}")]
    LinkCountMismatch(FileId),
    /// Authenticated direct bindings differ from this generation's namespace.
    #[error("file binding relation is incorrect for {0:?}")]
    BindingRelationMismatch(FileId),
    /// Directories cannot have hard links.
    #[error("directory has multiple namespace links {0:?}")]
    DirectoryHardLink(FileId),
    /// Directory record has no proven namespace tree.
    #[error("directory record has no namespace tree {0:?}")]
    MissingDirectoryTree(FileId),
    /// File table contains records unreachable from the root.
    #[error("file table contains unreachable records")]
    UnreachableFileRecords,
    /// Blob logical length differs from its declared use.
    #[error("blob logical length does not match its declaration")]
    BlobLengthMismatch,
    /// Extent references bytes outside its blob.
    #[error("extent content span exceeds its blob")]
    BlobSpanOutsideObject,
    /// Offset/count arithmetic overflowed.
    #[error("generation closure range arithmetic overflowed")]
    RangeOverflow,
    /// Immutable object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical object failed decoding or semantic validation.
    #[error(transparent)]
    Decode(#[from] crate::kernel::CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// A native original accounting leaf failed with its original typed cause.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Native(#[from] crate::NativeTreeMaterializationError),
    /// An authenticated extent decoder retained its typed read failure.
    #[error(transparent)]
    FileRead(#[from] crate::kernel::FileRangeReadError),
    /// An authenticated blob decoder retained its typed read failure.
    #[error(transparent)]
    BlobRead(#[from] crate::kernel::BlobReadError),
}
pub(crate) fn closure_error(error: crate::FsError) -> ClosureError {
    match error {
        crate::FsError::Closure(error) => error,
        crate::FsError::Object(error) => ClosureError::Storage(error),
        crate::FsError::Decode(error) => ClosureError::Decode(error),
        crate::FsError::Work(error) => ClosureError::Work(error),
        crate::FsError::Cancelled(_) => ClosureError::Storage(ObjectStoreError::Cancelled),
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => ClosureError::Native(error),
        crate::FsError::FileRead(crate::kernel::FileRangeReadError::Extent(
            crate::kernel::ExtentReadError::Decode(error),
        )) => ClosureError::Decode(error),
        crate::FsError::BlobRead(crate::kernel::BlobReadError::Decode(error)) => {
            ClosureError::Decode(error)
        }
        crate::FsError::FileRead(error) => ClosureError::FileRead(error),
        crate::FsError::BlobRead(error) => ClosureError::BlobRead(error),
        _ => unreachable!("closure accounting emitted an unrelated filesystem error"),
    }
}

mod memory;
pub(crate) use memory::{CachedProofs, ObservedProof, ProofGuard, ProofMeter};
mod published;
use crate::kernel::allocation::retained::RetainedArc;
use memory::{ProofMap, ProofSet, ProofVec, map_get, map_insert, set_insert};
