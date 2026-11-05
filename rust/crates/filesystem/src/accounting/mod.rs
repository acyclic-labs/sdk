//! Bounded, fail-closed projection of durable filesystem authority evidence.
//!
//! The owner must retain the authenticated generation closure for every unread
//! publication or retention. Stream history alone does not preserve those
//! objects after collection. Missing proof is an error, never a zero-byte fact;
//! this projection establishes no archival custody or permission to collect.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, OperationId, Sequence, VolumeId, authority_commit_digest,
};
use crate::kernel::allocation::retained::Retained;
use crate::kernel::{
    ClosureLimits, DecodeLimits, RetentionKind, decode_published_generation,
    decode_retention_created, decode_volume_created, decode_workspace_deleted,
    retention_authority_id, volume_authority_id,
};
use crate::model::VolumeConfig;
use crate::performance::{WorkCounters, WorkError};
use crate::storage::{ObjectId, ReplayLimit};
use thiserror::Error;

/// Exact next position in one authority hash chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountingCursor {
    /// Last consumed dense sequence.
    pub sequence: Sequence,
    /// Digest at that sequence.
    pub digest: Digest,
    /// Last accepted replicated settlement time, or zero at genesis.
    pub settled_at_micros: u64,
}

impl AccountingCursor {
    /// Empty authority position.
    #[must_use]
    pub const fn genesis() -> Self {
        Self {
            sequence: Sequence::GENESIS,
            digest: Digest::ZERO,
            settled_at_micros: 0,
        }
    }
}

/// Semantic transition proven by one durable FS record.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(
    tag = "transition",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum AccountingTransition {
    /// A generation became the initial or later published workspace version.
    Published {
        /// Workspace holding this publication.
        volume_id: VolumeId,
        /// Content-addressed generation identity.
        generation_root: ObjectId,
        /// Authenticated logical file size in this generation.
        logical_regular_file_bytes: u128,
    },
    /// A distinct retention authority began keeping a generation alive.
    Retained {
        /// Volume whose generation is retained.
        volume_id: VolumeId,
        /// Workspace whose deletion releases this retention.
        owner_volume_id: VolumeId,
        /// Retention reason.
        kind: RetentionKind,
        /// Content-addressed generation identity.
        generation_root: ObjectId,
        /// Authenticated logical file size in this generation.
        logical_regular_file_bytes: u128,
    },
    /// A workspace ceased retaining its generation history.
    WorkspaceDeleted {
        /// Workspace whose publications and owned retentions are released.
        volume_id: VolumeId,
    },
}

/// Whether a durable fact can represent one customer operation after regional top-level product attribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountingOperationClass {
    /// One successful durable FS mutation candidate.
    CustomerMutation,
    /// Supporting retention created as part of a workspace fork.
    InternalForkRetention,
}

/// One authenticated authority fact.
///
/// Retains the authority selected by the original proven page. A generation
/// can be published and retained by distinct authorities; neither its root nor
/// its volume identifies the authority that produced this fact. Logical file
/// bytes do not establish physical allocation custody or deduplication savings.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountingFact {
    /// Original authority whose hash chain authenticated this record.
    pub authority_id: AuthorityId,
    /// Dense sequence within the authority.
    pub sequence: Sequence,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Authenticated FS commit digest.
    pub commit_digest: Digest,
    /// Replicated settlement time.
    pub settled_at_micros: u64,
    /// Whether this fact can count as one durable FS mutation.
    pub operation_class: AccountingOperationClass,
    /// Canonically decoded state transition.
    pub transition: AccountingTransition,
}

/// One contiguous page ending at a pinned authority head.
pub struct AccountingPage {
    pub(crate) facts: Retained<Vec<AccountingFact>>,
    /// Exact cursor after the last fact.
    pub next: AccountingCursor,
    /// Last sequence at the pinned read head.
    pub watermark_sequence: Sequence,
    /// Digest of the pinned read head.
    pub watermark_digest: Digest,
    /// Replicated time of the pinned head.
    pub watermark_settled_at_micros: u64,
    /// True only if the cursor reached the pinned head; no earlier records remain.
    pub complete: bool,
    /// Original request's completed work through this read, including its prefix.
    pub work: WorkCounters,
}

impl AccountingPage {
    /// Proven facts borrowed from the page that retains their original funding.
    #[must_use]
    pub fn facts(&self) -> &[AccountingFact] {
        &self.facts.value
    }
}

impl std::fmt::Debug for AccountingPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AccountingPage")
            .field("facts", &self.facts())
            .field("next", &self.next)
            .field("watermark_sequence", &self.watermark_sequence)
            .field("watermark_digest", &self.watermark_digest)
            .field(
                "watermark_settled_at_micros",
                &self.watermark_settled_at_micros,
            )
            .field("complete", &self.complete)
            .field("work", &self.work)
            .finish()
    }
}

/// Fail-closed projection error.
#[derive(Debug, Error)]
pub enum AccountingError {
    /// Authority backend failed.
    #[error("authority read failed: {0}")]
    Authority(#[source] crate::AuthorityStoreError),
    /// Protected original generation evidence was malformed or mismatched.
    #[error("generation proof failed: {0}")]
    Proof(#[from] crate::kernel::PublicationEvidenceError),
    /// The original object operation failed before a proof was decoded.
    #[error(transparent)]
    Object(crate::ObjectStoreError),
    /// Original allocation or closure work failed.
    #[error(transparent)]
    Filesystem(crate::FsError),
    /// Dense chain, cursor, payload, or authority identity is invalid.
    #[error("accounting authority is malformed or has a replay gap")]
    InvalidHistory,
    /// Backend supplied no replicated settlement time.
    #[error("accounting authority has no trusted settlement time")]
    MissingTime,
    /// Caller requested an invalid page.
    #[error("accounting page bounds are invalid")]
    InvalidBounds,
    /// A finite admitted work allowance was exhausted or overflowed.
    #[error(transparent)]
    Work(#[from] WorkError),
}

mod proof;
mod read;

use proof::{prove_bytes, record_digest, verify_record};
pub use read::accounting_page;

impl From<crate::kernel::CanonicalDecodeError> for AccountingError {
    fn from(error: crate::kernel::CanonicalDecodeError) -> Self {
        Self::Proof(error.into())
    }
}

impl From<crate::kernel::GenerationExportManifestError> for AccountingError {
    fn from(error: crate::kernel::GenerationExportManifestError) -> Self {
        Self::Proof(error.into())
    }
}

impl From<crate::kernel::VolumeCreatedError> for AccountingError {
    fn from(error: crate::kernel::VolumeCreatedError) -> Self {
        Self::Proof(error.into())
    }
}

impl From<crate::kernel::RetentionCreatedError> for AccountingError {
    fn from(error: crate::kernel::RetentionCreatedError) -> Self {
        Self::Proof(error.into())
    }
}

impl From<crate::AuthorityStoreError> for AccountingError {
    fn from(error: crate::AuthorityStoreError) -> Self {
        match error {
            crate::AuthorityStoreError::Work(error) => Self::Work(error),
            #[cfg(not(target_arch = "wasm32"))]
            crate::AuthorityStoreError::NativeOriginal(
                crate::NativeTreeMaterializationError::Work(error),
            ) => Self::Work(error),
            error => Self::Authority(error),
        }
    }
}

impl From<crate::ObjectStoreError> for AccountingError {
    fn from(error: crate::ObjectStoreError) -> Self {
        match error {
            crate::ObjectStoreError::Work(error) => Self::Work(error),
            #[cfg(not(target_arch = "wasm32"))]
            crate::ObjectStoreError::Native(crate::NativeTreeMaterializationError::Work(error)) => {
                Self::Work(error)
            }
            error => Self::Object(error),
        }
    }
}

impl From<crate::FsError> for AccountingError {
    fn from(error: crate::FsError) -> Self {
        match error {
            crate::FsError::Work(error)
            | crate::FsError::Object(crate::ObjectStoreError::Work(error))
            | crate::FsError::Closure(crate::kernel::ClosureError::Work(error))
            | crate::FsError::Closure(crate::kernel::ClosureError::Storage(
                crate::ObjectStoreError::Work(error),
            )) => Self::Work(error),
            #[cfg(not(target_arch = "wasm32"))]
            crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Work(error))
            | crate::FsError::Closure(crate::kernel::ClosureError::Native(
                crate::NativeTreeMaterializationError::Work(error),
            )) => Self::Work(error),
            error => Self::Filesystem(error),
        }
    }
}
