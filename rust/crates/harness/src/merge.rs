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

/// Opaque inspected plan.
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
    /// Inspects a pinned readable source for import into this binding's writable destination.
    fn prepare_project_import<'a>(
        &'a self,
        scope: &'a Scope,
        destination: &'a crate::core::Reducer,
        source: &'a Authority,
        source_project: &'a VolumeRef,
        source_generation: &'a GenerationRef,
    ) -> BoxFuture<'a, Result<Box<dyn ProjectJoinPlan>>>;
}

/// Provider-owned metadata proving one exact immutable merge publication.
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
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        self.provider.validate(work)?;
        if self.format.is_empty()
            || self.statement.is_null()
            || !proof_numbers_are_js_safe(&self.statement, work)?
        {
            return Err(merge_boundary(work, "provider merge proof is invalid"));
        }
        Ok(())
    }
}

fn merge_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    let _ = work;
    Error::Invalid(reason.into())
}

fn proof_numbers_are_js_safe(
    value: &serde_json::Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    // FIRST: the borrowed traversal buffer drops before its original quota.
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let mut pending = crate::core::reserve_copy_vector::<&serde_json::Value>(1, &mut quota)?;
    pending.push(value);
    while let Some(value) = pending.pop() {
        if let Some(io) = quota.work().as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
        }
        match value {
            serde_json::Value::Number(number) => {
                if !crate::conversation::is_exact_js_integer(number) {
                    return Ok(false);
                }
            }
            serde_json::Value::Array(items) =>
                append_proof_values(&mut pending, items.iter(), items.len(), &mut quota)?,
            serde_json::Value::Object(fields) =>
                append_proof_values(&mut pending, fields.values(), fields.len(), &mut quota)?,
            _ => {}
        }
    }
    Ok(true)
}

fn append_proof_values<'a>(
    pending: &mut Vec<&'a serde_json::Value>,
    values: impl Iterator<Item = &'a serde_json::Value>, count: usize,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<()> {
    let required = pending.len().checked_add(count)
        .ok_or_else(|| merge_boundary(quota.work(), "provider merge proof traversal overflows"))?;
    if required > pending.capacity() {
        let mut replacement = crate::core::reserve_copy_vector::<&serde_json::Value>(required, quota)?;
        let copied = (pending.len() * std::mem::size_of::<&serde_json::Value>()) as u64;
        if let Some(io) = quota.work().as_deref_mut() {
            io.admit_copy(copied)?;
            io.copy_bytes(copied)?;
        }
        replacement.extend_from_slice(pending);
        let old = std::mem::replace(pending, replacement);
        let old_bytes = (old.capacity() * std::mem::size_of::<&serde_json::Value>()) as u64;
        drop(old);
        if quota.work().is_some() { quota.relinquish(old_bytes)?; }
    }
    pending.extend(values);
    Ok(())
}

fn provider_operation_has_nonzero(
    bytes: &[u8], work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    for byte in bytes {
        if let Some(io) = work.as_deref_mut() {
            io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
        }
        if *byte != 0 { return Ok(true); }
    }
    Ok(false)
}

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
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
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
            return Err(merge_boundary(work, "project merge receipt has inconsistent identities"));
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
            return Err(merge_boundary(work, "project merge notice has the wrong kind"));
        }
        Ok(())
    }

    /// Copies canonical fields on the caller's original ledger.
    pub(crate) fn clone_with_work_and_custody(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        if work.is_none() { return Ok((self.clone(), 0)); }
        self.validate_shape(work)?;
        // FIRST: all copied DTO fields drop before the quota on failure.
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let receipt = Self {
            operation_id: self.operation_id,
            child: crate::core::copy_authority(&self.child, &mut quota)?,
            source_project: self.source_project.clone_in_quota(&mut quota)?,
            source_generation: copy_generation(&self.source_generation, &mut quota)?,
            target_project: self.target_project.clone_in_quota(&mut quota)?,
            expected_target_generation: copy_generation(&self.expected_target_generation, &mut quota)?,
            result_generation: copy_generation(&self.result_generation, &mut quota)?,
            provider_operation_id: crate::core::copy_copy_vector(&self.provider_operation_id, &mut quota)?,
            provider_proof: ProviderJoinProof {
                provider: crate::core::copy_provider(&self.provider_proof.provider, &mut quota)?,
                format: crate::core::copy_text(&self.provider_proof.format, &mut quota)?,
                statement: crate::core::copy_value(&self.provider_proof.statement, &mut quota)?,
            },
            notice: self.notice.clone_in_quota(&mut quota)?,
        };
        let retained = quota.retained();
        quota.transfer();
        Ok((receipt, retained))
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

fn copy_generation(
    source: &GenerationRef, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<GenerationRef> {
    let source = source.as_resource();
    let provider = crate::core::copy_provider(source.provider(), quota)?;
    let key = crate::core::copy_copy_vector(source.key(), quota)?;
    let version = source.version().map(|value| crate::core::copy_text(value, quota)).transpose()?;
    let resource = crate::resources::ResourceRef::new(
        source.kind(), provider, key, version, quota.work())?;
    GenerationRef::from_resource_with_work(resource, quota.work())
}

/// Owning provider's immutable-generation proof, checked before Stream append.
pub trait ProjectMergeVerifier: acyclic_stream::ProviderPlatform {
    /// Verifies the immutable provider join before publication to history.
    fn verify<'a>(&'a self, receipt: &'a ProjectMergeReceipt) -> BoxFuture<'a, Result<()>>;
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
        assert!(unsafe_proof.validate(&mut None).is_err());
        Ok(())
    }

    fn field_copy_receipt() -> Result<ProjectMergeReceipt> {
        let mut receipt: ProjectMergeReceipt = serde_json::from_str(
            include_str!("../fixtures/v2/project-merge-receipt.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        receipt.provider_proof.statement = serde_json::json!({
            "state": [null, true, "original-state-ref", {"revision": 9_007_199_254_740_991_u64}],
            "notice": {"original": false, "count": -1},
        });
        receipt.notice.attachments = crate::conversation::ReferencedAttachments::Inline {
            items: vec![crate::conversation::Attachment {
                file: receipt.notice.content.clone(), label: Some("original notice attachment".into()),
            }],
        };
        receipt.notice.extensions.insert("merge-proof".into(), receipt.notice.content.clone());
        receipt.validate_shape(&mut None)?;
        Ok(receipt)
    }

    #[test]
    fn ordinary_merge_copy_preserves_every_canonical_field_and_policy() -> Result<()> {
        let receipt = field_copy_receipt()?;
        let (copied, retained) = receipt.clone_with_work_and_custody(&mut None)?;
        assert_eq!(copied, receipt);
        assert_eq!(retained, 0);
        for statement in [
            serde_json::Value::Null,
            serde_json::json!({"nested": [9_007_199_254_740_992_u64]}),
            serde_json::json!({"nested": [1.5]}),
        ] {
            let mut malformed = receipt.clone();
            malformed.provider_proof.statement = statement;
            assert!(malformed.validate_shape(&mut None).is_err());
        }
        let mut malformed = receipt;
        malformed.provider_operation_id.fill(0);
        assert!(malformed.validate_shape(&mut None).is_err());
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    fn field_copy_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 100_000, peak_allocation_bytes: 16_777_216,
            bytes_copied: 16_777_216, bytes_encoded: 16_777_216,
            bytes_hashed: 16_777_216, items_examined: 100_000,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn owned_merge_copy_uses_physical_fields_and_finite_inspection() -> Result<()> {
        use crate::store::history::{HistoryIo as _, HistoryWork};
        let receipt = field_copy_receipt()?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut io = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
            field_copy_budget(), &cancellation)?);
        let (copied, retained) = receipt.clone_with_work_and_custody(&mut Some(&mut io))?;
        assert_eq!(copied, receipt);
        assert!(retained > 0);
        drop(copied);
        io.release_owned(retained)?;
        let observed = io.finish(Ok(())).expect("physical owned receipt");
        assert!(observed.work.bytes_copied > 0);
        assert_eq!(observed.work.bytes_encoded, 0);
        assert_eq!(observed.work.bytes_hashed, 0);
        let mut denied =
            crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
            acyclic_fs::WorkBudget { items_examined: 0, ..field_copy_budget() }, &cancellation)?);
        assert!(receipt.validate_shape(&mut Some(&mut denied)).is_err());
        let mut partial =
            crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(
            acyclic_fs::WorkBudget { bytes_copied: receipt.child.id.len() as u64, ..field_copy_budget() },
            &cancellation,
            )?);
        assert!(receipt.clone_with_work_and_custody(&mut Some(&mut partial)).is_err());
        partial.admit_owned(field_copy_budget().peak_allocation_bytes)?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_merge_copy_transfers_original_custody_and_static_malformed_failures() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let receipt = field_copy_receipt()?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(field_copy_budget(), &cancellation)
            .expect("original merge ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native).expect("original merge observer"),
        );
        let (copied, retained) = receipt.clone_with_work_and_custody(&mut Some(&mut io))?;
        assert_eq!(copied, receipt);
        let guard = io.into_custody()?;
        assert_eq!(guard.bytes(), retained);
        assert!(guard.work().shares_ledger(&native));
        assert_eq!(native.receipt().bytes_encoded, 0);
        assert_eq!(native.receipt().bytes_hashed, 0);
        drop(copied);
        drop(guard);
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native).expect("same merge observer"),
        );
        let mut malformed = receipt.clone();
        malformed.source_project = malformed.target_project.clone();
        let before = native.receipt();
        assert!(matches!(malformed.clone_with_work_and_custody(&mut Some(&mut io)),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert_eq!(native.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(native.receipt().bytes_copied, before.bytes_copied);
        cancellation.cancel();
        assert!(receipt.clone_with_work_and_custody(&mut Some(&mut io)).is_err());
        assert_eq!(native.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(native.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(io.into_custody()?.bytes(), 0);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_partial_merge_copy_retires_new_fields_without_releasing_existing_custody() -> Result<()> {
        use acyclic_fs::{
            CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError,
        };
        use crate::store::history::HistoryIo as _;
        let receipt = field_copy_receipt()?;
        let baseline_token = CancellationToken::new();
        let baseline = NativeMaterializationWork::new(field_copy_budget(), &baseline_token)
            .expect("validation-only original ledger");
        let mut baseline_io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&baseline).expect("validation observer"),
        );
        receipt.validate_shape(&mut Some(&mut baseline_io))?;
        let validation_copies = baseline.receipt().bytes_copied;
        assert_eq!(baseline_io.into_custody()?.bytes(), 0);
        let cancellation = CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            bytes_copied: validation_copies + 4 + receipt.child.id.len() as u64,
            ..field_copy_budget()
        };
        let native = NativeMaterializationWork::new(budget, &cancellation)
            .expect("partial merge original ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native).expect("partial merge observer"),
        );
        let held = io.clone_text("held")?;
        assert!(matches!(receipt.clone_with_work_and_custody(&mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_copied", .. }), .. })));
        assert_eq!(native.receipt().bytes_copied, budget.bytes_copied);
        let guard = io.into_custody()?;
        assert_eq!(guard.bytes(), held.capacity() as u64);
        drop(held);
        drop(guard);
        native.admit_retained(budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }
}
