//! Parent-controlled publication of an inspected project merge.

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ConversationMessage, MessageKind, VolumeRef},
    core::{Authority, Scope},
    fork::{ForkSeed, ResourceRevision},
    resources::{GenerationRef, ProviderRef},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};

/// Stable provider-owned identity of one inspected conflict region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectConflict {
    /// Provider that interprets the conflict identity.
    pub provider: ProviderRef,
    /// Bounded stable identity within the exact inspected join plan.
    pub key: Vec<u8>,
}

impl ProjectConflict {
    /// Rejects empty or unbounded provider conflict identities.
    pub fn validate(&self) -> Result<()> {
        self.provider.validate(&mut None)?;
        if self.key.is_empty() {
            return Err(Error::Invalid("project conflict key is invalid".into()));
        }
        Ok(())
    }
}

/// Declarative choice of an immutable side, never an implicit write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectConflictSide {
    /// The common ancestor generation.
    Base,
    /// The current parent target generation.
    Target,
    /// The child source generation being joined.
    Source,
}

/// One exact conflict choice admitted by its owning inspected plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectConflictSelection {
    /// Provider-owned conflict identity from the inspected join plan.
    pub conflict: ProjectConflict,
    /// Immutable side whose value the parent selected.
    pub side: ProjectConflictSide,
}

/// Explicit result of an inspected project-only join attempt.
pub enum ProjectJoinOutcome {
    /// An immutable target generation was published and may be noticed in history.
    Applied(ProjectMergeReceipt),
    /// An exact retry of the same provider operation already published it.
    AlreadyApplied(ProjectMergeReceipt),
    /// The selected source introduced no changes to the target.
    NoChanges(GenerationRef),
    /// The target changed after inspection; callers must prepare a new plan.
    StaleTarget(GenerationRef),
    /// Exact conflicts need explicit parent choices or registered drivers.
    Conflicted {
        /// Provider-owned conflicts that still require a selection.
        conflicts: Vec<ProjectConflict>,
        /// Whether the provider omitted additional conflicts due to a bound.
        truncated: bool,
    },
    /// The target writer changed, so no publication occurred.
    Fenced,
    /// The provider retry identity was previously bound to different inputs.
    IdempotencyConflict,
}

/// Opaque provider-owned inspected plan.
pub trait ProjectJoinPlan: acyclic_stream::ProviderPlatform {
    /// Returns the exact child generation captured during inspection.
    fn source_generation(&self) -> &GenerationRef;
    /// Returns the target generation required by the compare-and-swap join.
    fn expected_target_generation(&self) -> &GenerationRef;
    /// Applies the inspected project-only join under the parent's authority.
    fn apply<'a>(
        &'a self,
        scope: &'a Scope,
        operation_id: OperationId,
        child: &'a Authority,
        notice: &'a ConversationMessage,
        selections: &'a [ProjectConflictSelection],
    ) -> BoxFuture<'a, Result<ProjectJoinOutcome>>;
}

/// Parent-bound project workspace operations.
pub trait ProjectWorkspaceProvider: acyclic_stream::ProviderPlatform {
    /// Returns the provider identity that owns workspace generations.
    fn provider(&self) -> &ProviderRef;
    /// Returns the parent-controlled project volume handled by this provider.
    fn project(&self) -> &VolumeRef;
    /// Creates a child project generation from one exact immutable source.
    fn fork_project<'a>(
        &'a self,
        scope: &'a Scope,
        source_generation: &'a GenerationRef,
        child: &'a VolumeRef,
        key: &'a IdempotencyKey,
    ) -> BoxFuture<'a, Result<GenerationRef>>;
    /// Inspects a pinned source for import into the writable destination.
    fn prepare_project_import<'a>(
        &'a self,
        scope: &'a Scope,
        destination: &'a crate::core::Reducer,
        source: &'a Authority,
        source_project: &'a VolumeRef,
        source_generation: &'a GenerationRef,
    ) -> BoxFuture<'a, Result<Box<dyn ProjectJoinPlan>>>;
}

/// Owning provider's immutable-generation proof, checked before Stream append.
pub trait ProjectMergeVerifier: acyclic_stream::ProviderPlatform {
    /// Verifies the immutable provider join before publication to history.
    fn verify<'a>(&'a self, receipt: &'a ProjectMergeReceipt) -> BoxFuture<'a, Result<()>>;
}

mod proof;
mod receipt;
pub use proof::ProviderJoinProof;
pub use receipt::ProjectMergeReceipt;
#[cfg(test)]
mod tests;
