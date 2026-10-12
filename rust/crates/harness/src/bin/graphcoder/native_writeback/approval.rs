//! Caller binding, approval identity and publication result projection.
#![allow(missing_docs, reason = "private native binary domain composition")]

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
    wire, wire_api::current_protocol, wire_codec::encode_authority,
};
use prost::Message as _;
use super::super::native::NativeSession;

type Journal = FilesystemExecutionJournal<acyclic_stream::LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
type Interactions = FilesystemInteractionHost<acyclic_stream::LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

impl NativeSession {
    pub(super) async fn publication_binding<'a>(&'a self, request: &NativePublicationRequest) -> Result<&'a NativePublicationConfig> {
        let mut selected = self.config.native_publications.iter().filter(|entry| entry.operation_id == request.operation_id);
        let binding = selected.next().ok_or_else(|| Error::Unauthorized("publication operation is not configured".into()))?;
        if selected.next().is_some()
            || canonical_bytes(&binding.caller_lease)? != canonical_bytes(&request.caller_lease)?
        {
            return Err(Error::Unauthorized("publication request differs from its original caller and lease".into()));
        }
        binding.validate()?;
        if binding.to_project.provider() != &self.config.filesystem_provider {
            return Err(Error::Unauthorized("publication selects another installed provider".into()));
        }
        let context = self.caller_context(binding.caller, &request.caller_lease).await?;
        for grant in [binding.from_project.capability(VolumeOperation::Read, &mut None)?,
            binding.to_project.capability(VolumeOperation::Read, &mut None)?, binding.to_project.capability(VolumeOperation::Write, &mut None)?,
            "interaction:route".to_owned()]
        {
            if !context.scope().grants().contains(&grant) {
                return Err(Error::Unauthorized("current original task scope does not authorize publication".into()));
            }
        }
        Ok(binding)
    }

    pub(super) async fn publication_journal(&self, binding: &NativePublicationConfig, request: &NativePublicationRequest) -> Result<(Journal, Interactions)> {
        let owner = self.runtime.task_host().journal_owner(binding.caller, LeaseFence::from(&request.caller_lease.reservation)).await?;
        let admission = self.runtime.task_host().observe_admission(binding.caller).await?;
        let maximum_bytes = (MAX_FRAME_BYTES as u64).min(admission.limits.file_bytes);
        let interaction_verifier = binding.owner.verifier()?;
        let journal = Journal::for_task(owner, binding.operation_id, binding.operation_id, self.files.clone(),
            &self.config.private_volume, &self.config.owner.verifier()?, &self.config.owner.scope, maximum_bytes, None)?
            .with_interaction_owner(&interaction_verifier, &SchemaRegistry::default(), &binding.owner.scope, &binding.private_volume, None)?;
        let interactions = Interactions::new(self.stream.clone(), self.files.clone(), &binding.owner.authority,
            &interaction_verifier, &SchemaRegistry::default(), &binding.owner.scope, &binding.private_volume, maximum_bytes, None)?;
        Ok((journal, interactions))
    }

    pub(super) async fn publication_open_approval(&self, binding: &NativePublicationConfig, journal: &Journal, original: &MaterializationJournal) -> Result<()> {
        let digest = publication_digest(binding, original)?;
        if original.phase == MaterializationPhase::Preparing {
            return Err(Error::Conflict("approval requires the complete captured original plan".into()));
        }
        journal.open_interaction(interaction_id(binding), Interaction::Approval {
            prompt: "Approve the exact retained native root generation publication; display text grants no authority.".into(),
            operation_id: binding.operation_id, action_digest: digest,
        }).await
    }

    pub(super) async fn publication_result(&self, binding: &NativePublicationConfig, interactions: &Interactions, receipt: Option<NativePublicationReceipt>) -> Result<NativePublicationResult> {
        let mut result = empty_result(binding);
        if let Some(receipt) = receipt {
            if receipt.journal.phase != MaterializationPhase::Preparing {
                result.action_digest = Some(publication_digest(binding, &receipt.journal)?);
            }
            result.status = match receipt.journal.phase {
                MaterializationPhase::Applied => "applied",
                MaterializationPhase::Applying => "recovering",
                MaterializationPhase::Preparing => "recovering",
                MaterializationPhase::RollingBack | MaterializationPhase::RolledBack => "rejected",
                MaterializationPhase::Prepared => "pending",
            };
            result.publication = Some(receipt);
        }
        if let Some((ticket, resolution)) = interactions.read(interaction_id(binding), None).await? {
            let digest = result.action_digest.ok_or_else(|| Error::Conflict("approval lacks the original publication journal".into()))?;
            if ticket.id != uuid::Uuid::from_bytes(interaction_id(binding).into_bytes())
                || !ticket.approval.as_ref().is_some_and(|approval| approval.operation_id == binding.operation_id && approval.action_digest == digest)
            {
                return Err(Error::Unauthorized("retained approval differs from the complete original publication plan".into()));
            }
            let original_request = interactions.read_request(interaction_id(binding), None).await?
                .ok_or_else(|| Error::Storage("publication approval request is missing".into()))?;
            if !matches!(original_request, Interaction::Approval { operation_id, action_digest, .. }
                if operation_id == binding.operation_id && action_digest == digest)
            {
                return Err(Error::Unauthorized("approval request differs from original publication".into()));
            }
            if let Some(resolution) = &resolution {
                resolution.validate(&ticket)?;
                result.status = match &resolution.outcome {
                    InteractionOutcome::Approved if matches!(result.status, "applied" | "recovering") => result.status,
                    InteractionOutcome::Approved => "approved",
                    InteractionOutcome::Indeterminate { .. } => "pending",
                    _ => "rejected",
                };
            } else if binding.owner.scope.capabilities().contains("interaction:resolve") {
                result.approved_frame_hex = Some(self.publication_approval_frame(binding, &ticket).await?);
            }
            result.interaction = Some(ticket);
            result.resolution = resolution;
        }
        Ok(result)
    }

    async fn publication_approval_frame(&self, binding: &NativePublicationConfig, ticket: &InteractionTicket) -> Result<String> {
        // The trusted configured issuer issues only this ticket's responder grant.
        self.runtime.task_host().verify_dispatch_owner(binding.caller, LeaseFence::from(&binding.caller_lease.reservation)).await?;
        let issuer = AuthorityIssuer::new(&binding.owner.issuer, binding.owner.key, binding.owner.authority.clone());
        let capabilities = Capabilities::new(["interaction:resolve".to_owned(), ticket.responder_grant()]);
        let label = format!("native-publication-responder:{}", ticket.id);
        let scope = issuer.issue(&label, &capabilities, binding.owner.scope.agent(), None, &mut None).expect("scope proof");
        let aggregate = StreamAggregate::open(&self.stream, binding.owner.authority.clone(), issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::default(), None, acyclic_harness::store::default_projection_read_limits(), acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await?;
        let operation = approval_operation(binding.operation_id);
        let action = Action::ResolveInteraction { resolution: InteractionResolution {
            id: ticket.id, expected_version: 1,
            outcome: InteractionOutcome::Approved, detail: None,
        } };
        let proof: [u8; 32] = serde_json::from_value(
            serde_json::to_value(&scope).map_err(invalid)?["proof"].take(),
        ).map_err(invalid)?;
        let envelope = wire::CommandEnvelope {
            protocol: Some(current_protocol()), authority: Some(encode_authority(&binding.owner.authority)),
            operation: Some(wire::OperationIdentity { operation_id: operation.to_string(), idempotency_key: format!("native-publication-decision:{operation}") }),
            expected_revision: aggregate.reducer().revision(),
            scope: Some(wire::Scope { id: scope.id().into(), capabilities: scope.capabilities().iter().map(ToOwned::to_owned).collect(),
                issuer: issuer.id().into(), agent_id: scope.agent().map_or_else(String::new, |agent| agent.to_string()),
                parent_proof: Vec::new(), proof: proof.to_vec() }),
            action_type: "resolve_interaction".into(), canonical_action_json: canonical_bytes(&action)?,
            ..Default::default()
        };
        Ok(hex::encode(wire::ClientFrame { frame: Some(wire::client_frame::Frame::Command(envelope)) }.encode_to_vec()))
    }
}

pub(super) fn publication_digest(binding: &NativePublicationConfig, journal: &MaterializationJournal) -> Result<[u8; 32]> {
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
    acyclic_harness::canonical_json_bytes_bounded(value, acyclic_harness::JsonBounds {
        maximum_bytes: MAX_FRAME_BYTES as u64,
        maximum_items: u64::MAX,
        maximum_depth: u32::MAX,
    }, None, &mut |_| Ok(()))
}

pub(super) fn fs_operation(operation: OperationId) -> acyclic_fs::OperationId { acyclic_fs::OperationId::from_bytes(operation.into_bytes()) }
pub(super) fn interaction_id(binding: &NativePublicationConfig) -> InteractionId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"graphcoder/native-publication/interaction/v1\0");
    hash.update(&binding.caller.into_bytes());
    hash.update(&binding.operation_id.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    InteractionId::from_bytes(bytes)
}
fn approval_operation(operation: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"graphcoder/native-publication/decision/v1\0");
    hash.update(&operation.into_bytes());
    hash.update(&[1]);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
fn empty_result(binding: &NativePublicationConfig) -> NativePublicationResult {
    NativePublicationResult { operation_id: binding.operation_id, caller: binding.caller, status: "unopened",
        interaction: None, resolution: None, action_digest: None,
        publication: None, failure: None, approved_frame_hex: None }
}
fn invalid(error: impl std::fmt::Display) -> Error { Error::Invalid(error.to_string()) }
pub(super) fn publication_failure(binding: &NativePublicationConfig, message: String, work: WorkCounters) -> NativePublicationResult {
    let mut result = empty_result(binding);
    result.status = "failed";
    result.failure = Some(NativePublicationFailure { message, work });
    result
}
