//! Exact caller-owned approval for the SDK's original measured host publication.
#![allow(missing_docs, reason = "private native binary domain composition")]

use acyclic_fs::{GenerationId, MaterializationJournal, WorkBudget, WorkCounters};
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::{ContentGrant, VolumeClass, VolumeOperation, VolumeRef},
    core::{AuthorityIssuer, AuthorityVerifier},
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

fn verifier(binding: &AuthorityBinding) -> Result<AuthorityVerifier> {
    let verifier = AuthorityIssuer::new(&binding.issuer, binding.key, binding.authority.clone()).verifier(&mut None).expect("verifier copy");
    verifier.verify(&binding.scope)?;
    Ok(verifier)
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
        self.private_volume.validate()?;
        self.from_project.validate()?;
        self.to_project.validate()?;
        if self.private_volume.class() != VolumeClass::AgentPrivate
            || self.from_project.class() != VolumeClass::Project || self.to_project.class() != VolumeClass::Project
            || self.from_project.provider() != self.to_project.provider()
            || self.private_volume.provider() != self.to_project.provider()
        {
            return Err(Error::Invalid("publication source selections must belong to the original filesystem provider".into()));
        }
        let verifier = verifier(&self.owner)?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.from_project, VolumeOperation::Read)?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.to_project, VolumeOperation::Read)?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.to_project, VolumeOperation::Write)?;
        ContentGrant::verify(&verifier, &self.owner.scope, &self.private_volume, VolumeOperation::Write)?;
        Ok(())
    }
}

/// Untrusted input cannot name a new path, generation, grant or plan.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationRequest {
    pub operation_id: OperationId,
    pub caller: TaskId,
    pub caller_lease: WorkLease,
}

#[derive(Serialize)]
pub struct NativePublicationSource {
    pub project: VolumeRef,
    pub workspace: String,
    pub generation: GenerationId,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativePublicationStatus { Unopened, Pending, Approved, Rejected, Recovering, Applied, Failed }

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
    pub status: NativePublicationStatus,
    pub interaction: Option<InteractionTicket>,
    pub resolution: Option<InteractionResolution>,
    pub action_digest: Option<[u8; 32]>,
    pub before: NativePublicationSource,
    pub proposed: NativePublicationSource,
    pub source_root_identity: [u8; 16],
    pub publication: Option<NativePublicationReceipt>,
    pub failure: Option<NativePublicationFailure>,
    pub approved_frame_hex: Option<String>,
    pub declined_frame_hex: Option<String>,
}

#[cfg(feature = "native-execution")]
mod enabled {
use super::*;
use acyclic_fs::MaterializationPhase;
use acyclic_harness::{
    Capabilities, InteractionId,
    core::{Action, SchemaRegistry},
    executor::ExecutionJournal,
    filesystem::{FilesystemExecutionJournal, FilesystemInteractionHost},
    interaction::{Interaction, InteractionOutcome},
    runtime::DurableTaskHost as _,
    scheduler::LeaseFence,
    store::StreamAggregate,
    tool::ToolApprovalVerifier,
    wire, wire_api::current_protocol, wire_codec::encode_authority,
};
use prost::Message as _;
use super::super::native::NativeSession;

type Journal = FilesystemExecutionJournal<acyclic_stream::LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
type Interactions = FilesystemInteractionHost<acyclic_stream::LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

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
            Err(failure) => return publication_failure(binding, failure.error.to_string(), *failure.work),
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
        let from_workspace = filesystem.open_workspace(binding.from_project.storage_name()?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let to_workspace = filesystem.open_workspace(binding.to_project.storage_name()?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
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
            Err(failure) => publication_failure(binding, failure.error.to_string(), *failure.work),
        }
    }

    pub async fn publication_observe(&self, request: &NativePublicationRequest) -> Result<NativePublicationResult> {
        let binding = self.publication_binding(request).await?;
        let (_, interactions) = self.publication_journal(binding, request).await?;
        let state = acyclic_fs::LocalCoreStateStore::new(&binding.state_root);
        let cancellation = acyclic_fs::CancellationToken::new();
        match acyclic_fs::load_native_materialization_journal(&state, fs_operation(binding.operation_id), binding.budget, &cancellation).await {
            Ok(receipt) => self.publication_result(binding, &interactions, receipt.value.map(|journal| NativePublicationReceipt { journal, work: receipt.work })).await,
            Err(failure) => publication_failure(binding, failure.error.to_string(), *failure.work),
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
        if matches!(observed.status, NativePublicationStatus::Approved | NativePublicationStatus::Recovering)
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
        let from_workspace = filesystem.open_workspace(binding.from_project.storage_name()?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
        let to_workspace = filesystem.open_workspace(binding.to_project.storage_name()?, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(storage)?;
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
            Err(failure) => return publication_failure(binding, failure.error.to_string(), *failure.work),
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
                    let grant = guard_owner.require_volume_grant_measured(
                        volume, operation, work.remaining()?, guard_cancellation,
                    );
                    account_guard(&work, grant)?;
                }
                let approval = guard_journal.verify_measured(
                    interaction_id(binding), binding.operation_id, digest,
                    work.remaining()?, guard_cancellation,
                ).await;
                account_guard(&work, approval)?;
                let owner = guard_owner.verify_dispatch_measured(work.remaining()?, guard_cancellation).await;
                account_guard(&work, owner)
            }
        };
        match acyclic_fs::publish_native_generation_transition(&from, &to, &state,
            publication(binding, &excluded, &options, &cancellation), work, before_effect).await
        {
            Ok(receipt) => self.publication_result(binding, &interactions, Some(NativePublicationReceipt { journal: receipt.value, work: receipt.work })).await,
            Err(failure) => publication_failure(binding, failure.error.to_string(), *failure.work),
        }
    }

    async fn publication_binding<'a>(&'a self, request: &NativePublicationRequest) -> Result<&'a NativePublicationConfig> {
        let mut selected = self.config.native_publications.iter().filter(|entry| entry.operation_id == request.operation_id);
        let binding = selected.next().ok_or_else(|| Error::Unauthorized("publication operation is not configured".into()))?;
        if selected.next().is_some() || binding.caller != request.caller
            || canonical_bytes(&binding.caller_lease)? != canonical_bytes(&request.caller_lease)?
        {
            return Err(Error::Unauthorized("publication request differs from its original caller and lease".into()));
        }
        binding.validate()?;
        if binding.to_project.provider() != &self.config.filesystem_provider {
            return Err(Error::Unauthorized("publication selects another installed provider".into()));
        }
        let context = self.caller_context(binding.caller, &request.caller_lease).await?;
        for grant in [binding.from_project.capability(VolumeOperation::Read)?,
            binding.to_project.capability(VolumeOperation::Read)?, binding.to_project.capability(VolumeOperation::Write)?,
            "interaction:route".to_owned()]
        {
            if !context.scope().grants().contains(&grant) {
                return Err(Error::Unauthorized("current original task scope does not authorize publication".into()));
            }
        }
        Ok(binding)
    }

    async fn publication_journal(&self, binding: &NativePublicationConfig, request: &NativePublicationRequest) -> Result<(Journal, Interactions)> {
        let owner = self.runtime.task_host().journal_owner(binding.caller, LeaseFence::from(&request.caller_lease.reservation)).await?;
        let admission = self.runtime.task_host().observe_admission(binding.caller).await?;
        let maximum_bytes = (MAX_FRAME_BYTES as u64).min(admission.limits.file_bytes);
        let interaction_verifier = verifier(&binding.owner)?;
        let journal = Journal::for_task(owner, binding.operation_id, binding.operation_id, self.files.clone(),
            self.config.private_volume.clone(), verifier(&self.config.owner)?, self.config.owner.scope.clone(), maximum_bytes)?
            .with_interaction_owner(interaction_verifier.clone(), SchemaRegistry::default(), binding.owner.scope.clone(), binding.private_volume.clone())?;
        let interactions = Interactions::new(self.stream.clone(), self.files.clone(), &binding.owner.authority,
            &interaction_verifier, &SchemaRegistry::default(), &binding.owner.scope, &binding.private_volume, maximum_bytes, None)?;
        Ok((journal, interactions))
    }

    async fn publication_open_approval(&self, binding: &NativePublicationConfig, journal: &Journal, original: &MaterializationJournal) -> Result<()> {
        let digest = publication_digest(binding, original)?;
        if original.phase == MaterializationPhase::Preparing {
            return Err(Error::Conflict("approval requires the complete captured original plan".into()));
        }
        journal.open_interaction(interaction_id(binding), Interaction::Approval {
            prompt: "Approve the exact retained native root generation publication; display text grants no authority.".into(),
            operation_id: binding.operation_id, action_digest: digest,
        }).await
    }

    async fn publication_result(&self, binding: &NativePublicationConfig, interactions: &Interactions, receipt: Option<NativePublicationReceipt>) -> Result<NativePublicationResult> {
        let mut result = empty_result(binding)?;
        if let Some(receipt) = receipt {
            if receipt.journal.phase != MaterializationPhase::Preparing {
                result.action_digest = Some(publication_digest(binding, &receipt.journal)?);
            }
            result.status = match receipt.journal.phase {
                MaterializationPhase::Applied => NativePublicationStatus::Applied,
                MaterializationPhase::Applying => NativePublicationStatus::Recovering,
                MaterializationPhase::Preparing => NativePublicationStatus::Recovering,
                MaterializationPhase::RollingBack | MaterializationPhase::RolledBack => NativePublicationStatus::Rejected,
                MaterializationPhase::Prepared => NativePublicationStatus::Pending,
            };
            result.publication = Some(receipt);
        }
        if let Some((ticket, resolution)) = interactions.read(interaction_id(binding)).await? {
            let digest = result.action_digest.ok_or_else(|| Error::Conflict("approval lacks the original publication journal".into()))?;
            if ticket.id != uuid::Uuid::from_bytes(interaction_id(binding).into_bytes())
                || !ticket.approval.as_ref().is_some_and(|approval| approval.operation_id == binding.operation_id && approval.action_digest == digest)
            {
                return Err(Error::Unauthorized("retained approval differs from the complete original publication plan".into()));
            }
            let original_request = interactions.read_request(interaction_id(binding)).await?
                .ok_or_else(|| Error::Storage("publication approval request is missing".into()))?;
            if !matches!(original_request, Interaction::Approval { operation_id, action_digest, .. }
                if operation_id == binding.operation_id && action_digest == digest)
            {
                return Err(Error::Unauthorized("approval request differs from original publication".into()));
            }
            if let Some(resolution) = &resolution {
                resolution.validate(&ticket)?;
                result.status = match &resolution.outcome {
                    InteractionOutcome::Approved if matches!(result.status, NativePublicationStatus::Applied | NativePublicationStatus::Recovering) => result.status,
                    InteractionOutcome::Approved => NativePublicationStatus::Approved,
                    InteractionOutcome::Indeterminate { .. } => NativePublicationStatus::Pending,
                    _ => NativePublicationStatus::Rejected,
                };
            } else if binding.owner.scope.capabilities().contains("interaction:resolve") {
                result.approved_frame_hex = Some(self.publication_decision_frame(binding, &ticket, true).await?);
                result.declined_frame_hex = Some(self.publication_decision_frame(binding, &ticket, false).await?);
            }
            result.interaction = Some(ticket);
            result.resolution = resolution;
        }
        Ok(result)
    }

    async fn publication_decision_frame(&self, binding: &NativePublicationConfig, ticket: &InteractionTicket, approved: bool) -> Result<String> {
        // The trusted configured issuer issues only this ticket's responder grant.
        self.runtime.task_host().verify_dispatch_owner(binding.caller, LeaseFence::from(&binding.caller_lease.reservation)).await?;
        let issuer = AuthorityIssuer::new(&binding.owner.issuer, binding.owner.key, binding.owner.authority.clone());
        let capabilities = Capabilities::new(["interaction:resolve".to_owned(), ticket.responder_grant()]);
        let label = format!("native-publication-responder:{}", ticket.id);
        let scope = match binding.owner.scope.agent() {
            Some(agent) => issuer.root_for_agent(agent, label, capabilities),
            None => issuer.root(label, capabilities),
        };
        let aggregate = StreamAggregate::open(&self.stream, binding.owner.authority.clone(), issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::default()).await?;
        let operation = decision_operation(binding.operation_id, approved);
        let action = Action::ResolveInteraction { resolution: InteractionResolution {
            id: ticket.id, expected_version: 1,
            outcome: if approved { InteractionOutcome::Approved } else { InteractionOutcome::Declined }, detail: None,
        } };
        #[derive(Deserialize)]
        struct SignedScope { id: String, capabilities: Capabilities, agent: Option<acyclic_harness::AgentId>, issuer: String, parent_proof: Option<[u8; 32]>, proof: [u8; 32] }
        let parts: SignedScope = serde_json::from_value(serde_json::to_value(&scope).map_err(invalid)?).map_err(invalid)?;
        let envelope = wire::CommandEnvelope {
            protocol: Some(current_protocol()), authority: Some(encode_authority(&binding.owner.authority)),
            operation: Some(wire::OperationIdentity { operation_id: operation.to_string(), idempotency_key: format!("native-publication-decision:{operation}") }),
            expected_revision: aggregate.reducer().revision(),
            scope: Some(wire::Scope { id: parts.id, capabilities: parts.capabilities.iter().map(ToOwned::to_owned).collect(),
                issuer: parts.issuer, agent_id: parts.agent.map_or_else(String::new, |agent| agent.to_string()),
                parent_proof: parts.parent_proof.map_or_else(Vec::new, |proof| proof.to_vec()), proof: parts.proof.to_vec() }),
            action_type: "resolve_interaction".into(), canonical_action_json: canonical_bytes(&action)?,
            ..Default::default()
        };
        Ok(hex::encode(wire::ClientFrame { frame: Some(wire::client_frame::Frame::Command(envelope)) }.encode_to_vec()))
    }
}

fn publication_digest(binding: &NativePublicationConfig, journal: &MaterializationJournal) -> Result<[u8; 32]> {
    if journal.plan.operation_id != fs_operation(binding.operation_id)
        || journal.plan.intent != (acyclic_fs::MaterializationIntent::GenerationTransition { from: binding.from, to: binding.to })
        || journal.root_identity != Some(binding.source_root_identity)
        || journal.preparation_identity.is_none()
        || (journal.phase != MaterializationPhase::Preparing && journal.preimages.len() != journal.plan.edits.len())
    {
        return Err(Error::Conflict("original publication journal differs from configured source, target or real root identity".into()));
    }
    Ok(*blake3::hash(&canonical_bytes(&("graphcoder/native-publication/v1", binding,
        &journal.plan, journal.root_identity, journal.preparation_identity, &journal.preimages))?).as_bytes())
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    struct Limit(u64);
    impl std::io::Write for Limit {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.checked_sub(bytes.len() as u64).ok_or_else(|| std::io::Error::other("publication JSON exceeds finite terminal bound"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    serde_json::to_writer(&mut Limit(MAX_FRAME_BYTES as u64), value).map_err(invalid)?;
    let mut value = serde_json::to_value(value).map_err(invalid)?;
    value.sort_all_objects();
    serde_json::to_vec(&value).map_err(invalid)
}

fn fs_operation(operation: OperationId) -> acyclic_fs::OperationId { acyclic_fs::OperationId::from_bytes(operation.into_bytes()) }
fn interaction_id(binding: &NativePublicationConfig) -> InteractionId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"graphcoder/native-publication/interaction/v1\0");
    hash.update(&binding.caller.into_bytes());
    hash.update(&binding.operation_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    InteractionId::from_bytes(bytes)
}
fn decision_operation(operation: OperationId, approved: bool) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"graphcoder/native-publication/decision/v1\0");
    hash.update(&operation.into_bytes());
    hash.update(&[u8::from(approved)]);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
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
fn empty_result(binding: &NativePublicationConfig) -> Result<NativePublicationResult> {
    Ok(NativePublicationResult { operation_id: binding.operation_id, caller: binding.caller, status: NativePublicationStatus::Unopened,
        interaction: None, resolution: None, action_digest: None,
        before: NativePublicationSource { project: binding.from_project.clone(), workspace: binding.from_project.storage_name()?, generation: binding.from },
        proposed: NativePublicationSource { project: binding.to_project.clone(), workspace: binding.to_project.storage_name()?, generation: binding.to },
        source_root_identity: binding.source_root_identity, publication: None, failure: None, approved_frame_hex: None, declined_frame_hex: None })
}
fn invalid(error: impl std::fmt::Display) -> Error { Error::Invalid(error.to_string()) }
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
fn publication_failure(binding: &NativePublicationConfig, message: String, work: WorkCounters) -> Result<NativePublicationResult> {
    let mut result = empty_result(binding)?;
    result.status = NativePublicationStatus::Failed;
    result.failure = Some(NativePublicationFailure { message, work });
    Ok(result)
}
}
