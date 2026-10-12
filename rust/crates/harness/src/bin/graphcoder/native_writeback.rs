//! Exact caller-owned approval for the SDK's original measured host publication.
#![allow(missing_docs, reason = "private native binary domain composition")]

use acyclic_fs::{GenerationId, MaterializationJournal, WorkBudget, WorkCounters};
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeRef},
    core::AuthorityIssuer,
    distributed::WorkLease,
    interaction::{InteractionResolution, InteractionTicket},
};
use serde::{Deserialize, Serialize};
use std::path::{Component, PathBuf};

use super::{native::AuthorityBinding, terminal::MAX_FRAME_BYTES};

/// Only the installed trusted producer supplies these paths and source facts.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationConfig {
    pub operation_id: OperationId,
    pub caller: TaskId,
    pub caller_lease: WorkLease,
    pub owner: AuthorityBinding,
    pub private_volume: VolumeRef,
    pub root: PathBuf,
    pub operation_directory: PathBuf,
    pub state_root: PathBuf,
    pub source_root_identity: [u8; 16],
    pub from_project: VolumeRef,
    pub to_project: VolumeRef,
    pub from: GenerationId,
    pub to: GenerationId,
    pub excluded_names: Vec<String>,
    pub budget: WorkBudget,
    pub maximum_directory_entries: u32,
    pub maximum_extent_spans: u32,
    pub transfer_bytes: u64,
}

impl NativePublicationConfig {
    pub fn validate(&self) -> Result<()> {
        for path in [&self.root, &self.operation_directory, &self.state_root] {
            if !path.is_absolute() || path.components().any(|part| matches!(part, Component::ParentDir)) {
                return Err(Error::Invalid("native publication paths must be explicit absolute locations".into()));
            }
        }
        if self.operation_id.into_bytes() == [0; 16] || self.source_root_identity == [0; 16]
            || !self.budget.has_finite_allowances() || self.budget.peak_allocation_bytes == 0
            || self.maximum_directory_entries == 0 || self.maximum_extent_spans == 0
            || self.transfer_bytes == 0 || self.transfer_bytes > MAX_FRAME_BYTES as u64
            || self.excluded_names.len() > 64
        {
            return Err(Error::Invalid("native publication requires original identities and finite allowances".into()));
        }
        for name in &self.excluded_names {
            if name.is_empty() || name.len() > 255 || name == "." || name == ".."
                || name.contains(['/', '\\', '\0'])
            {
                return Err(Error::Invalid("publication exclusion must be one root-level name".into()));
            }
        }
        self.private_volume.validate(&mut None)?;
        self.from_project.validate(&mut None)?;
        self.to_project.validate(&mut None)?;
        if self.private_volume.class() != VolumeClass::AgentPrivate
            || self.from_project.class() != VolumeClass::Project || self.to_project.class() != VolumeClass::Project
            || self.from_project.provider() != self.to_project.provider()
            || self.private_volume.provider() != self.to_project.provider()
        {
            return Err(Error::Invalid("publication source selections must belong to the original filesystem provider".into()));
        }
        let verifier = self.owner.verifier()?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.from_project, VolumeOperation::Read, u64::MAX, &mut None)?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.to_project, VolumeOperation::Read, u64::MAX, &mut None)?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.to_project, VolumeOperation::Write, u64::MAX, &mut None)?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.private_volume, VolumeOperation::Write, u64::MAX, &mut None)?;
        Ok(())
    }
}

/// Untrusted input cannot name a new path, generation, grant or plan.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationRequest {
    pub operation_id: OperationId,
    pub caller_lease: WorkLease,
}

#[derive(Serialize)]
pub struct NativePublicationReceipt {
    pub journal: MaterializationJournal,
    /// The SDK's actual invocation receipt, not a synthesized cumulative total.
    pub work: WorkCounters,
}

#[derive(Serialize)]
pub struct NativePublicationFailure { pub message: String, pub work: WorkCounters }

#[derive(Serialize)]
pub struct NativePublicationResult {
    pub operation_id: OperationId,
    pub caller: TaskId,
    pub status: &'static str,
    pub interaction: Option<InteractionTicket>,
    pub resolution: Option<InteractionResolution>,
    pub action_digest: Option<[u8; 32]>,
    pub publication: Option<NativePublicationReceipt>,
    pub failure: Option<NativePublicationFailure>,
    pub approved_frame_hex: Option<String>,
}

#[cfg(feature = "native-execution")]
#[path = "native_writeback/approval.rs"]
mod approval;

#[cfg(feature = "native-execution")]
mod enabled {
use super::*;
use acyclic_fs::MaterializationPhase;
use acyclic_harness::{
    interaction::InteractionOutcome,
    runtime::DurableTaskHost as _,
    scheduler::LeaseFence,
    tool::ToolApprovalVerifier,
};
use super::super::native::NativeSession;
use super::approval::*;

impl NativeSession {
    pub async fn publication_open(&self, request: &NativePublicationRequest) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        self.runtime.task_host().verify_dispatch_owner(binding.caller, LeaseFence::from(&request.caller_lease.reservation)).await?;
        let (journal, interactions) = self.publication_journal(binding, request).await?;
        let state = acyclic_fs::LocalCoreStateStore::new(&binding.state_root);
        let cancellation = acyclic_fs::CancellationToken::new();
        let existing = acyclic_fs::load_native_materialization_journal(
            &state, fs_operation(binding.operation_id), binding.budget, &cancellation,
        ).await;
        let receipt = match existing {
            Ok(receipt) => receipt,
            Err((error, work)) => return Ok(publication_failure(binding, error.to_string(), work)),
        };
        if let Some(original) = receipt.value {
            if !matches!(original.phase, MaterializationPhase::Preparing) {
                if original.phase == MaterializationPhase::Prepared {
                    self.publication_open_approval(binding, &journal, &original).await?;
                }
                return self.publication_result(binding, &interactions, Some(NativePublicationReceipt { journal: original, work: receipt.work })).await;
            }
        }
        let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&self.config.filesystem_root)).await
            .map_err(storage)?;
        let from_workspace = filesystem.open_workspace(binding.from_project.storage_name(&mut None)?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let to_workspace = filesystem.open_workspace(binding.to_project.storage_name(&mut None)?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let from = from_workspace.generation(binding.from, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let to = to_workspace.generation(binding.to, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let excluded = binding.excluded_names.iter().map(String::as_str).collect::<Vec<_>>();
        let options = publication_options(binding);
        self.runtime.task_host().verify_dispatch_owner(binding.caller, LeaseFence::from(&request.caller_lease.reservation)).await?;
        match acyclic_fs::prepare_native_generation_transition(&from, &to, &state,
            publication(binding, &excluded, &options, &cancellation)).await
        {
            Ok(receipt) => {
                self.publication_open_approval(binding, &journal, &receipt.value).await?;
                self.publication_result(binding, &interactions, Some(NativePublicationReceipt { journal: receipt.value, work: receipt.work })).await
            }
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }

    pub async fn publication_observe(&self, request: &NativePublicationRequest) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        let (_, interactions) = self.publication_journal(binding, request).await?;
        let state = acyclic_fs::LocalCoreStateStore::new(&binding.state_root);
        let cancellation = acyclic_fs::CancellationToken::new();
        match acyclic_fs::load_native_materialization_journal(&state, fs_operation(binding.operation_id), binding.budget, &cancellation).await {
            Ok(receipt) => self.publication_result(binding, &interactions, receipt.value.map(|journal| NativePublicationReceipt { journal, work: receipt.work })).await,
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }

    /// Reconciliation observes original progress.
    pub async fn publication_reconcile(&self, request: &NativePublicationRequest) -> Result<NativePublicationResult> {
        let observed = self.publication_observe(request).await?;
        if observed.publication.as_ref().is_some_and(|receipt|
            matches!(receipt.journal.phase, MaterializationPhase::Preparing | MaterializationPhase::Prepared))
            && observed.interaction.is_none()
        {
            // The SDK resumes its original captured preparation; it must never plan a second operation just because the approval open was lost.
            return self.publication_open(request).await;
        }
        if matches!(observed.status, "approved" | "recovering")
            && observed.resolution.as_ref().is_some_and(|value| value.outcome == InteractionOutcome::Approved)
        {
            return self.publication_apply(request).await;
        }
        Ok(observed)
    }

    pub async fn publication_apply(&self, request: &NativePublicationRequest) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        let (journal, interactions) = self.publication_journal(binding, request).await?;
        let observed = self.publication_observe(request).await?;
        let original = observed.publication.as_ref().ok_or_else(|| Error::Conflict("original publication has not been prepared".into()))?;
        let digest = publication_digest(binding, &original.journal)?;
        journal.verify(interaction_id(binding), binding.operation_id, digest).await?;
        self.runtime.task_host().verify_dispatch_owner(binding.caller, LeaseFence::from(&request.caller_lease.reservation)).await?;
        if original.journal.phase == MaterializationPhase::Applied { return Ok(observed); }
        if matches!(original.journal.phase, MaterializationPhase::Preparing | MaterializationPhase::RollingBack | MaterializationPhase::RolledBack) {
            return Err(Error::Conflict("publication is not an approved prepared forward operation".into()));
        }
        let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&self.config.filesystem_root)).await.map_err(storage)?;
        let from_workspace = filesystem.open_workspace(binding.from_project.storage_name(&mut None)?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let to_workspace = filesystem.open_workspace(binding.to_project.storage_name(&mut None)?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let from = from_workspace.generation(binding.from, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let to = to_workspace.generation(binding.to, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let excluded = binding.excluded_names.iter().map(String::as_str).collect::<Vec<_>>();
        let options = publication_options(binding);
        let state = acyclic_fs::LocalCoreStateStore::new(&binding.state_root);
        let dispatch_owner = self.runtime.task_host().journal_owner(
            binding.caller, LeaseFence::from(&request.caller_lease.reservation),
        ).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = match acyclic_fs::NativeMaterializationWork::new(binding.budget, &cancellation) {
            Ok(work) => work,
            Err((error, work)) => return Ok(publication_failure(binding, error.to_string(), work)),
        };
        let guard_work = work.clone();
        let guard_journal = &journal;
        let guard_owner = &dispatch_owner;
        let guard_cancellation = &cancellation;
        let before_effect = || {
            let work = guard_work.clone();
            async move {
                for (volume, operation) in [
                    (&binding.from_project, VolumeOperation::Read),
                    (&binding.to_project, VolumeOperation::Read),
                    (&binding.to_project, VolumeOperation::Write),
                ] {
                    let mut grant_work = acyclic_harness::store::HistoryWork::new(work.remaining()?, guard_cancellation);
                    let grant = grant_work.preflight().and_then(|()|
                        guard_owner.require_volume_grant(volume, operation, &mut Some(&mut grant_work)));
                    account_guard(&work, grant_work.finish(grant))?;
                }
                let approval = guard_journal.verify_measured(
                    interaction_id(binding), binding.operation_id, digest,
                    work.remaining()?, guard_cancellation,
                ).await;
                account_guard(&work, approval)?;
                let mut owner_work = acyclic_harness::store::HistoryWork::new(work.remaining()?, guard_cancellation);
                let owner = async {
                    owner_work.preflight()?;
                    guard_owner.verify(false, &mut Some(&mut owner_work)).await
                }.await;
                account_guard(&work, owner_work.finish(owner))
            }
        };
        match acyclic_fs::publish_native_generation_transition(&from, &to, &state,
            publication(binding, &excluded, &options, &cancellation), work, before_effect).await
        {
            Ok(receipt) => self.publication_result(binding, &interactions, Some(NativePublicationReceipt { journal: receipt.value, work: receipt.work })).await,
            Err((error, work)) => Ok(publication_failure(binding, error.to_string(), work)),
        }
    }


}

fn publication_options(binding: &NativePublicationConfig) -> acyclic_fs::MaterializeOptions {
    acyclic_fs::MaterializeOptions { destination: binding.operation_directory.join("target"), expected_root_identity: None,
        maximum_directory_entries: binding.maximum_directory_entries, maximum_extent_spans: binding.maximum_extent_spans, transfer_bytes: binding.transfer_bytes }
}
fn publication<'a>(binding: &'a NativePublicationConfig, excluded: &'a [&'a str], options: &'a acyclic_fs::MaterializeOptions, cancellation: &'a acyclic_fs::CancellationToken) -> acyclic_fs::NativeWorkspacePublication<'a> {
    acyclic_fs::NativeWorkspacePublication { root: &binding.root, operation_directory: &binding.operation_directory,
        source_root_identity: acyclic_fs::NativeRootIdentity::from_bytes(binding.source_root_identity),
        operation_id: fs_operation(binding.operation_id), from: binding.from, to: binding.to,
        excluded_names: excluded, options, budget: binding.budget, cancellation }
}
fn storage(error: impl std::fmt::Display) -> Error { Error::Storage(error.to_string()) }
fn publication_guard_error(error: Error) -> acyclic_fs::NativeWorkspacePublicationError {
    acyclic_fs::NativeWorkspacePublicationError::Native(
        acyclic_fs::NativeTreeMaterializationError::Io(std::io::Error::other(error)),
    )
}
fn account_guard<T>(
    work: &acyclic_fs::NativeMaterializationWork,
    result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error>,
) -> std::result::Result<T, acyclic_fs::NativeWorkspacePublicationError> {
    match result {
        Ok(receipt) => {
            work.spent(receipt.work)?;
            Ok(receipt.value)
        }
        Err(failure) => {
            work.spent(*failure.work)?;
            Err(publication_guard_error(failure.error))
        }
    }
}

}
