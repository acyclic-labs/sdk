//! Bounded, fail-closed projection of durable filesystem authority evidence.
//!
//! The owner must retain the authenticated generation closure for every unread
//! publication or retention. Stream history alone does not preserve those
//! objects after collection. Missing proof is an error, never a zero-byte fact;
//! this projection establishes no archival custody or permission to collect.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, OperationId, Sequence, VolumeId, authority_commit_digest,
};
use crate::kernel::{
    ClosureLimits, DecodeLimits, RetentionKind, decode_published_generation,
    decode_retention_created, decode_volume_created, decode_workspace_deleted,
    prove_generation_closure, retention_authority_id, volume_authority_id,
};
use crate::model::VolumeConfig;
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ReplayLimit};
use thiserror::Error;

const MAX_EVENT_BYTES: u64 = 4096;

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
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountingOperationClass {
    /// One successful durable FS mutation candidate.
    CustomerMutation,
    /// Supporting retention created as part of a workspace fork.
    InternalForkRetention,
}

/// One authenticated authority fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountingFact {
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountingPage {
    /// Proven facts after the supplied cursor.
    pub facts: Vec<AccountingFact>,
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
    /// Measured authority and immutable-proof work performed by this read.
    pub work: WorkCounters,
}

/// Fail-closed projection error.
#[derive(Debug, Error)]
pub enum AccountingError {
    /// Authority backend failed.
    #[error("authority read failed: {0}")]
    Authority(String),
    /// Generation closure was absent, corrupt, or outside admitted bounds.
    #[error("generation proof failed: {0}")]
    Proof(String),
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

use proof::{prove_bytes, prove_published, verify_record};
use read::AccountingReadWork;
pub use read::accounting_page;
pub(crate) use read::accounting_page_at_head;

#[cfg(test)]
mod tests;
