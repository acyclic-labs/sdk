use super::proof::{merge_boundary, provider_operation_has_nonzero};
use super::*;

/// Exact result of a parent-authorized Filesystem join.
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
    pub fn validate_shape(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.child.kind != crate::core::AggregateKind::Conversation
            || self.source_project == self.target_project
            || self.source_project.provider() != self.target_project.provider()
            || self.result_generation == self.expected_target_generation
            || self.source_generation.as_resource().provider() != self.source_project.provider()
            || self.expected_target_generation.as_resource().provider()
                != self.target_project.provider()
            || self.result_generation.as_resource().provider() != self.target_project.provider()
            || self.provider_proof.provider != *self.target_project.provider()
            || self.provider_operation_id.is_empty()
            || !provider_operation_has_nonzero(&self.provider_operation_id, work)?
        {
            return Err(merge_boundary(
                work,
                "project merge receipt has inconsistent identities",
            ));
        }
        self.child.validate_with_work(work)?;
        self.source_project.validate(work)?;
        self.target_project.validate(work)?;
        self.source_generation.validate(work)?;
        self.expected_target_generation.validate(work)?;
        self.result_generation.validate(work)?;
        self.provider_proof.validate(work)?;
        self.notice.validate(work)?;
        if self.notice.kind != MessageKind::Merge {
            return Err(merge_boundary(
                work,
                "project merge notice has the wrong kind",
            ));
        }
        Ok(())
    }

    /// Checks the receipt against the exact child fork selected by the parent.
    pub fn validate(&self, seed: &ForkSeed) -> Result<()> {
        self.validate_shape(&mut None)?;
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

impl ProjectMergeReceipt {
    /// Copies canonical fields on the caller's original ledger.
    pub(crate) fn copy(&self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<Self> {
        self.validate_shape(quota.work())?;
        Ok(Self {
            operation_id: self.operation_id,
            child: crate::core::copy_authority(&self.child, quota)?,
            source_project: self.source_project.clone_in_quota(quota)?,
            source_generation: GenerationRef::from_resource_with_work(
                crate::core::copy_resource(self.source_generation.as_resource(), quota)?,
                quota.work(),
            )?,
            target_project: self.target_project.clone_in_quota(quota)?,
            expected_target_generation: GenerationRef::from_resource_with_work(
                crate::core::copy_resource(self.expected_target_generation.as_resource(), quota)?,
                quota.work(),
            )?,
            result_generation: GenerationRef::from_resource_with_work(
                crate::core::copy_resource(self.result_generation.as_resource(), quota)?,
                quota.work(),
            )?,
            provider_operation_id: crate::core::copy_copy_vector(
                &self.provider_operation_id,
                quota,
            )?,
            provider_proof: ProviderJoinProof {
                provider: crate::core::copy_provider(&self.provider_proof.provider, quota)?,
                format: crate::core::copy_text(&self.provider_proof.format, quota)?,
                statement: crate::core::copy_value(&self.provider_proof.statement, quota)?,
            },
            notice: self.notice.clone_in_quota(quota)?,
        })
    }
}
