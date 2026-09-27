//! Parent-controlled publication of an inspected project merge.
//!
//! Filesystem owns the generation transition. Harness records only its
//! immutable result and a ref-only conversation notice, in one Stream event.

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ConversationMessage, MessageKind, VolumeClass, VolumeRef},
    core::{Authority, Scope},
    fork::{ForkSeed, ResourceRevision},
    resources::{GenerationRef, ProviderRef},
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin};

/// Stable provider-owned identity of one inspected conflict region. Its bytes
/// are deliberately opaque to Harness; a provider validates every selection.
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
        self.provider.validate()?;
        if self.key.is_empty() || self.key.len() > 4_096 {
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

/// Opaque inspected plan. Its implementation and conflict interpretation stay
/// with the workspace provider; a lost acknowledgement must be reconciled by
/// the stable provider operation, never blindly retried with a new key.
pub trait ProjectJoinPlan: Send + Sync {
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

/// Parent-bound project workspace operations. A child cannot obtain this
/// binding merely by possessing a project or generation reference.
pub trait ProjectWorkspaceProvider: Send + Sync {
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
    /// Inspect against the caller's current parent reducer, so a provider
    /// bound before child publication cannot retain a stale fork registry.
    fn prepare_project_merge<'a>(
        &'a self,
        scope: &'a Scope,
        parent: &'a crate::core::Reducer,
        child: &'a Authority,
        child_project: &'a VolumeRef,
    ) -> BoxFuture<'a, Result<Box<dyn ProjectJoinPlan>>>;
}

/// Provider-owned metadata proving one exact immutable merge publication.
/// The statement is bounded and contains no message or file bodies; its owner
/// validates its meaning against durable operation state before Stream append.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderJoinProof {
    /// Provider that owns and verifies this statement.
    pub provider: ProviderRef,
    /// Versioned provider-specific proof format.
    pub format: String,
    /// Bounded immutable proof payload.
    pub statement: serde_json::Value,
}

impl ProviderJoinProof {
    /// Rejects an invalid or unbounded provider proof.
    pub fn validate(&self) -> Result<()> {
        self.provider.validate()?;
        if self.format.is_empty()
            || self.format.len() > 128
            || self.statement.is_null()
            || !proof_numbers_are_js_safe(&self.statement)
            || crate::contract::canonical_json_bytes(&self.statement)?.len() > 4_096
        {
            return Err(Error::Invalid(
                "provider merge proof is invalid or oversized".into(),
            ));
        }
        Ok(())
    }
}

fn proof_numbers_are_js_safe(value: &serde_json::Value) -> bool {
    let mut pending = vec![value];
    while let Some(value) = pending.pop() {
        match value {
            serde_json::Value::Number(number) => {
                if number
                    .as_i64()
                    .is_some_and(|integer| integer.unsigned_abs() > 9_007_199_254_740_991)
                    || number
                        .as_u64()
                        .is_some_and(|integer| integer > 9_007_199_254_740_991)
                {
                    return false;
                }
            }
            serde_json::Value::Array(items) => pending.extend(items),
            serde_json::Value::Object(fields) => pending.extend(fields.values()),
            _ => {}
        }
    }
    true
}

/// Exact result of a parent-authorized Filesystem join. The publication ID is
/// distinct from the Filesystem retry identity so a lost Stream acknowledgement
/// can be reconciled without reapplying the join.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectMergeReceipt {
    /// Stable Harness publication identity.
    pub operation_id: OperationId,
    /// Child conversation whose project branch was joined.
    pub child: Authority,
    /// Forked child project volume.
    pub source_project: VolumeRef,
    /// Exact child generation selected for the join.
    pub source_generation: GenerationRef,
    /// Parent-controlled target project volume.
    pub target_project: VolumeRef,
    /// Target generation required by the join's compare-and-swap.
    pub expected_target_generation: GenerationRef,
    /// Immutable generation produced by the join.
    pub result_generation: GenerationRef,
    /// Bounded provider-side idempotency identity for the join.
    pub provider_operation_id: Vec<u8>,
    /// Provider-owned proof binding this receipt to the join operation.
    pub provider_proof: ProviderJoinProof,
    /// Parent-conversation notice appended atomically with this receipt.
    pub notice: ConversationMessage,
}

impl ProjectMergeReceipt {
    /// Checks ref-only shape and provider identities without fork lineage.
    pub fn validate_shape(&self) -> Result<()> {
        if self.child.kind != crate::core::AggregateKind::Conversation
            || self.source_project.class() != VolumeClass::Project
            || self.target_project.class() != VolumeClass::Project
            || self.source_project == self.target_project
            || self.source_project.provider() != self.target_project.provider()
            || self.source_project.owner() != self.target_project.owner()
            || self.result_generation == self.expected_target_generation
            || self.source_generation.as_resource().provider() != self.source_project.provider()
            || self.expected_target_generation.as_resource().provider()
                != self.target_project.provider()
            || self.result_generation.as_resource().provider() != self.target_project.provider()
            || self.provider_proof.provider != *self.target_project.provider()
            || self.provider_operation_id.is_empty()
            || self.provider_operation_id.len() > 64
            || self.provider_operation_id.iter().all(|byte| *byte == 0)
        {
            return Err(Error::Invalid(
                "project merge receipt has inconsistent identities".into(),
            ));
        }
        self.child.stream_path()?;
        self.source_project.validate()?;
        self.target_project.validate()?;
        self.source_generation.validate()?;
        self.expected_target_generation.validate()?;
        self.result_generation.validate()?;
        self.provider_proof.validate()?;
        self.notice.validate()?;
        if self.notice.kind != MessageKind::Merge {
            return Err(Error::Invalid(
                "project merge notice has the wrong kind".into(),
            ));
        }
        Ok(())
    }

    /// Checks the receipt against the exact child fork selected by the parent.
    pub fn validate(&self, seed: &ForkSeed) -> Result<()> {
        self.validate_shape()?;
        if self.child != seed.child {
            return Err(Error::Invalid(
                "project merge child does not match fork seed".into(),
            ));
        }
        let captured = seed.resources.iter().any(|capture| {
            matches!(
                (&capture.source, &capture.revision),
                (ResourceRevision::Project { volume: parent, .. },
                 ResourceRevision::Project { volume: child, .. })
                    if parent == &self.target_project && child == &self.source_project
            )
        });
        if !captured {
            return Err(Error::Unauthorized(
                "project merge was not forked by this parent".into(),
            ));
        }
        Ok(())
    }
}

/// Owning provider's immutable-generation proof, checked before Stream append.
/// No reducer replay needs access to the live Filesystem head.
pub trait ProjectMergeVerifier: Send + Sync {
    /// Verifies the immutable provider join before publication to history.
    fn verify<'a>(
        &'a self,
        receipt: &'a ProjectMergeReceipt,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_project_merge_receipt_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/project-merge-receipt.json").trim();
        let receipt: ProjectMergeReceipt =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        let seed: ForkSeed = serde_json::from_str(include_str!("../fixtures/v2/fork-seed.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        receipt.validate(&seed)?;
        assert_eq!(
            serde_json::to_string(&receipt).map_err(|error| Error::Invalid(error.to_string()))?,
            fixture
        );
        let mut forged = receipt.clone();
        forged.child.id = "other-child".into();
        assert!(forged.validate(&seed).is_err());
        let mut unsafe_proof = receipt.provider_proof;
        unsafe_proof.statement = serde_json::json!({"unsafe": 9_007_199_254_740_992_u64});
        assert!(unsafe_proof.validate().is_err());
        Ok(())
    }
}
