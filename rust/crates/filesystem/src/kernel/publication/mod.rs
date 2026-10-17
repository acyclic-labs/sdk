//! Atomic generation publication after bounded whole-closure authentication.

use super::{
    CanonicalDecodeError, ClosureError, ClosureLimits, GenerationProof, codec::Decoder,
    volume_authority_id,
};
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, Epoch, Head, OperationId, ProposedCommit, VolumeId, usize_to_u64,
};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    AppendOutcome, AuthorityStoreError, GuardedAppend, ObjectId, ObjectKind, PublicationPermit,
};
use acyclic_stream::ProviderTask;
use bytes::Bytes;
use thiserror::Error;

use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::kernel::closure::CachedProofs;
use crate::kernel::closure::ProofGuard;
use crate::kernel::persistent_point::Owned;
use std::sync::Mutex;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type PublicationRootPreparation<'a, E> = &'a mut (
            dyn FnMut(
    &super::GenerationRoot,
    WorkBudget,
) -> crate::MeasuredResult<crate::OperationReceipt<u64>, E>
                + Send
        );
#[cfg(target_arch = "wasm32")]
pub(crate) type PublicationRootPreparation<'a, E> =
    &'a mut dyn FnMut(
        &super::GenerationRoot,
        WorkBudget,
    ) -> crate::MeasuredResult<crate::OperationReceipt<u64>, E>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type NativePublicationRootPreparation<'a, E> = &'a mut (
            dyn FnMut(
    &super::GenerationRoot,
    &mut crate::NativeAllocation,
    &crate::NativeMaterializationWork,
) -> Result<(), E>
                + Send
        );

/// Complete caller preconditions for publishing one immutable generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishGenerationRequest {
    /// Storage authority that owns this volume's ordered head.
    pub authority_id: AuthorityId,
    /// Volume expected inside the authenticated generation root.
    pub volume_id: VolumeId,
    /// Active writer-fencing epoch.
    pub epoch: Epoch,
    /// Exact authority head observed by the caller.
    pub expected: Head,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Immutable generation-root object to authenticate and publish.
    pub generation_root: ObjectId,
}

/// Context, evidence and authorization for one publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationIntent {
    /// Optional semantic operation context.
    pub operation_context: Option<Digest>,
    /// Original operation evidence retained through publication.
    pub original_evidence: Option<acyclic_stream::OwnedBytes>,
    /// Original permit used when reconstructing a retry fingerprint.
    pub fingerprint_permit: Option<PublicationPermit>,
    /// Current publication authorization.
    pub permit: PublicationPermit,
}

impl PublicationIntent {
    /// Creates an intent without original evidence or a prior permit.
    pub const fn guarded(operation_context: Option<Digest>, permit: PublicationPermit) -> Self {
        Self {
            operation_context,
            original_evidence: None,
            fingerprint_permit: None,
            permit,
        }
    }
}

/// Canonical semantic fact stored in one generation-publication authority record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishedGeneration {
    /// Volume whose ordered authority admitted the generation.
    pub volume_id: VolumeId,
    /// Authenticated immutable generation-root object.
    pub generation_root: ObjectId,
}

/// Successful or semantically rejected generation publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationReceipt {
    /// Whole-generation authentication evidence.
    pub proof: GenerationProof,
    /// Atomic authority result, including retry/conflict/fencing outcomes.
    pub outcome: AppendOutcome,
    /// Exact closure, encoding, hashing, and authority work.
    pub work: WorkCounters,
}

/// Publication failure retaining every unit of work spent before rejection.
pub type PublicationFailure = OperationFailure<PublicationError>;

/// Fail-closed generation-publication errors.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum PublicationError {
    /// The request targeted an authority other than the volume's canonical authority.
    #[error("authority {actual:?} does not own volume {volume_id:?}; expected {expected:?}")]
    AuthorityMismatch {
        /// Requested volume.
        volume_id: VolumeId,
        /// Sole authority derived from the volume identity.
        expected: AuthorityId,
        /// Authority supplied by the caller.
        actual: AuthorityId,
    },
    /// The authenticated generation belongs to another volume.
    #[error("generation belongs to volume {actual:?}, expected {expected:?}")]
    VolumeMismatch {
        /// Requested volume.
        expected: VolumeId,
        /// Volume authenticated by the generation root.
        actual: VolumeId,
    },
    /// A physical projection still holds the prior policy revision.
    #[error("policy metadata cannot change while a native projection is active")]
    ActiveProjection,
    /// The current or candidate policy metadata could not be authenticated.
    #[error("projection policy validation failed: {0}")]
    ProjectionValidation(#[source] Box<crate::FsError>),
    /// Whole-generation authentication failed.
    #[error(transparent)]
    Closure(#[from] ClosureError),
    /// Staged immutable bodies could not be durably admitted before publication.
    #[error(transparent)]
    Objects(#[from] crate::storage::ObjectStoreError),
    /// Atomic authority evaluation failed.
    #[error(transparent)]
    Authority(#[from] AuthorityStoreError),
    /// Exact work overflowed or exceeded the admitted request budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

mod accounting;
use accounting::*;
mod codec;
pub(crate) use codec::decode_publication_evidence_with_accounting;
pub use codec::decode_published_generation;
use codec::*;
mod construction;
pub(crate) use construction::contextual_publication_fingerprint;
use construction::*;
mod engine;
mod projection;
use engine::*;
mod api;
pub use api::publish_generation;
pub(crate) use api::publish_generation_async_guarded;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use api::publish_generation_async_guarded_with_native_work;

#[cfg(all(test, feature = "memory"))]
#[path = "../tests/publication/mod.rs"]
mod tests;
