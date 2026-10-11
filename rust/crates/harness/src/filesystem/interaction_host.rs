//! One Filesystem/Stream bridge for the conversation-owned interaction ledger.

use super::{FilesystemContentVerifier, FilesystemHost, InternalContentClass};
use super::fork_preparer_io::{PreparationIo, UploadRecord};
use crate::contract::capability;
use crate::contract::next_revision;
use crate::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, ApplyResult, Authority, AuthorityVerifier, Command, SchemaRegistry, Scope},
    interaction::{
        ApprovalBinding, Interaction, InteractionKind, InteractionOutcome, InteractionResolution,
        InteractionResponse, InteractionTicket, ResolutionReceipt,
    },
    runtime::InteractionResolver,
    store::StreamAggregate,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{StreamClient, StreamProvider};
use std::sync::Arc;
use uuid::Uuid;

/// Stages participant data privately and commits only references to one conversation history.
pub struct FilesystemInteractionHost<P, A, O> {
    stream: StreamClient<P>,
    host: Arc<FilesystemHost<A, O>>,
    authority: Authority,
    verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    owner_scope: Scope,
    private_volume: VolumeRef,
    maximum_bytes: u64,
}

// Decode only the closed approval wire, never an arbitrary request's JSON
// schema. These are the existing approval enum's exact serialized fields.
#[derive(serde::Deserialize)]
enum ApprovalTag {
    #[serde(rename = "approval")]
    Approval,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalRequestWire {
    #[serde(rename = "type")]
    _kind: ApprovalTag,
    prompt: String,
    operation_id: OperationId,
    action_digest: [u8; 32],
}

impl UploadRecord for ApprovalRequestWire {
    fn logical_owned_bytes(&self) -> Result<u64> { Ok(self.prompt.capacity() as u64) }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ApprovalResponseWire {
    #[serde(rename = "type")]
    _kind: ApprovalTag,
    approved: bool,
    reason: Option<String>,
}

impl UploadRecord for ApprovalResponseWire {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(self.reason.as_ref().map_or(0, String::capacity) as u64)
    }
}

impl<P, A, O> FilesystemInteractionHost<P, A, O> {
    pub(super) fn verify_responder_scope(&self, id: InteractionId, scope: &Scope) -> Result<()> {
        self.verifier.verify(scope)?;
        if !scope.capabilities().contains(capability::INTERACTION_RESOLVE)
            || !scope.capabilities().contains(&capability::interaction_respond(id)) {
            return Err(Error::Unauthorized("responder lacks the exact interaction grant".into()));
        }
        Ok(())
    }
}

impl<P, A, O> InteractionResolver for FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn inspect<'a>(
        &'a self,
        scope: Scope,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<(InteractionTicket, Option<InteractionResolution>)>>> {
        Box::pin(async move {
            self.verifier.verify(&scope)?;
            let result = self.read(id).await?;
            if let Some((ticket, _)) = &result
                && !scope.capabilities().contains(&ticket.viewer_grant())
                && !scope.capabilities().contains(&ticket.responder_grant())
            {
                return Err(Error::Unauthorized(
                    "scope lacks interaction view grant".into(),
                ));
            }
            Ok(result)
        })
    }

    fn resolve_answer<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>> {
        Box::pin(FilesystemInteractionHost::resolve_answer(
            self,
            operation_id,
            scope,
            id,
            expected_version,
            response,
        ))
    }

    fn resolve_approval<'a>(
        &'a self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> BoxFuture<'a, Result<ResolutionReceipt>> {
        Box::pin(FilesystemInteractionHost::resolve_approval(
            self,
            operation_id,
            scope,
            id,
            expected_version,
            approved,
            reason,
        ))
    }
}

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds the exact conversation authority and an agent-private content owner.
    #[allow(
        clippy::too_many_arguments,
        reason = "binds each conversation, volume, and signed authority boundary"
    )]
    pub fn new(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        authority: Authority,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        owner_scope: Scope,
        private_volume: VolumeRef,
        maximum_bytes: u64,
    ) -> Result<Self> {
        Self::new_with_io(
            stream, host, authority, verifier, schemas, owner_scope,
            private_volume, maximum_bytes, None,
        )
    }

    // Incoming owned metadata is already covered by the constructing
    // execution journal's original ledger and intrinsic, last-field custody.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_io(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        authority: Authority,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        owner_scope: Scope,
        private_volume: VolumeRef,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            verifier.verify_audience_with_native_work(&authority, work)?;
        } else {
            verifier.verify_audience(&authority)?;
        }
        #[cfg(target_arch = "wasm32")]
        verifier.verify_audience(&authority)?;
        if authority.kind != crate::core::AggregateKind::Conversation
            || private_volume.class() != VolumeClass::AgentPrivate
            || private_volume.provider() != &host.provider
            || maximum_bytes == 0
        {
            return Err(super::fork_preparer_io::invalid(
                io,
                "interaction host requires a conversation, private volume, and positive limit",
            ));
        }
        if let Some(io) = io {
            ContentGrant::verify_original_volume_with_io(
                &verifier, &owner_scope, &private_volume, VolumeOperation::Read, io,
            )?;
            ContentGrant::verify_original_volume_with_io(
                &verifier, &owner_scope, &private_volume, VolumeOperation::Write, io,
            )?;
        } else {
            ContentGrant::verify(
                &verifier, &owner_scope, &private_volume, VolumeOperation::Read,
            )?;
            ContentGrant::verify(
                &verifier, &owner_scope, &private_volume, VolumeOperation::Write,
            )?;
        }
        Ok(Self {
            stream,
            host,
            authority,
            verifier,
            schemas,
            owner_scope,
            private_volume,
            maximum_bytes,
        })
    }

    pub(crate) fn owner_scope(&self) -> Scope {
        self.owner_scope.clone()
    }

    /// Stages the exact JSON request before admission; a staged orphan is reclaimable.
    pub async fn stage_request(
        &self,
        id: InteractionId,
        request: &Interaction,
        deadline_unix_ms: Option<u64>,
    ) -> Result<InteractionTicket> {
        request.validate()?;
        let approval = match request {
            Interaction::Approval {
                operation_id,
                action_digest,
                ..
            } => Some(ApprovalBinding {
                operation_id: *operation_id,
                action_digest: *action_digest,
            }),
            _ => None,
        };
        let reference = self.stage(id, "request", request).await?;
        let ticket = InteractionTicket {
            id: interaction_uuid(id)?,
            kind: request.kind(),
            request: reference,
            deadline_unix_ms,
            approval,
        };
        ticket.validate()?;
        Ok(ticket)
    }

    /// Stages a proposed answer; admission revalidates it against the pinned request.
    pub async fn stage_answer(
        &self,
        id: InteractionId,
        expected_version: u64,
        operation_id: OperationId,
        response: &crate::interaction::InteractionResponse,
    ) -> Result<FileRef> {
        if expected_version == 0 {
            return Err(Error::Invalid(
                "interaction answer version must be positive".into(),
            ));
        }
        self.stage(
            id,
            &format!("answer-{expected_version}-{operation_id}"),
            response,
        )
        .await
    }

    /// Replays the exact conversation and admits one typed open event.
    pub async fn open(
        &self,
        operation_id: OperationId,
        scope: Scope,
        ticket: InteractionTicket,
    ) -> Result<ApplyResult> {
        self.execute(operation_id, scope, Action::OpenInteraction { ticket })
            .await
    }

    /// Admits through the same conversation reducer under this exact task lease.
    /// The caller binds this host to the task owner's Stream provider.
    pub(crate) async fn open_owned(
        &self,
        operation_id: OperationId,
        scope: Scope,
        ticket: InteractionTicket,
        owner: &crate::durable_host::TaskJournalOwner<P>,
    ) -> Result<ApplyResult> {
        let mut aggregate = self.aggregate().await?;
        let expected_revision = match aggregate.operation_revision(operation_id).await? {
            Some(revision) => revision
                .checked_sub(1)
                .ok_or_else(|| Error::Invalid("committed interaction revision is zero".into()))?,
            None => aggregate.reducer().revision(),
        };
        aggregate
            .execute_task_command(
                Command {
                    operation_id,
                    idempotency_key: IdempotencyKey::new(format!("interaction:{operation_id}"))?,
                    expected_revision,
                    scope,
                    causal_parent: None,
                    action: Action::OpenInteraction { ticket },
                },
                owner,
            )
            .await
    }

    /// Resolves only with the exact interaction responder grant and current version.
    pub async fn resolve(
        &self,
        operation_id: OperationId,
        scope: Scope,
        resolution: InteractionResolution,
    ) -> Result<ApplyResult> {
        self.execute(
            operation_id,
            scope,
            Action::ResolveInteraction { resolution },
        )
        .await
    }

    /// Authenticates a responder, validates one typed answer against the
    /// admitted request, stages its bytes, then CAS-resolves the conversation.
    /// A lost acknowledgement is retried with the same operation ID.
    pub async fn resolve_answer(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> Result<ResolutionReceipt> {
        let ticket = self.open_resolution(&scope, id, expected_version).await?;
        if ticket.kind == InteractionKind::Approval {
            return Err(Error::Invalid("approval requires a decision".into()));
        }
        let request = self
            .read_request(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        request.validate_response(&response)?;
        let answer = self
            .stage_answer(id, expected_version, operation_id, &response)
            .await?;
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version,
            outcome: InteractionOutcome::Answered {
                answer: Box::new(answer),
            },
            detail: None,
        };
        resolution.validate(&ticket)?;
        ResolutionReceipt::from_apply_result(self.resolve(operation_id, scope, resolution).await?)
    }

    /// Resolves an approval of the ticket's exact operation and argument digest.
    /// The complete decision, including an optional explanation, is staged as
    /// a ref so exact retries can compare it without guessing intent.
    pub async fn resolve_approval(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> Result<ResolutionReceipt> {
        let ticket = self.open_resolution(&scope, id, expected_version).await?;
        if ticket.kind != InteractionKind::Approval {
            return Err(Error::Invalid("interaction is not an approval".into()));
        }
        let outcome = if approved {
            InteractionOutcome::Approved
        } else {
            InteractionOutcome::Declined
        };
        let response = InteractionResponse::Approval { approved, reason };
        let detail = Some(
            self.stage_answer(id, expected_version, operation_id, &response)
                .await?,
        );
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version,
            outcome,
            detail,
        };
        resolution.validate(&ticket)?;
        ResolutionReceipt::from_apply_result(self.resolve(operation_id, scope, resolution).await?)
    }

    async fn open_resolution(
        &self,
        scope: &Scope,
        id: InteractionId,
        expected_version: u64,
    ) -> Result<InteractionTicket> {
        self.verifier.verify(scope)?;
        if !scope
            .capabilities()
            .contains(capability::INTERACTION_RESOLVE)
        {
            return Err(Error::Unauthorized(
                "scope lacks interaction:resolve".into(),
            ));
        }
        let (ticket, prior) = self
            .read(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        if !scope.capabilities().contains(&ticket.responder_grant()) {
            return Err(Error::Unauthorized(
                "scope lacks interaction responder grant".into(),
            ));
        }
        let next = match &prior {
            Some(prior) if prior.outcome.is_terminal() => prior.expected_version,
            Some(prior) => next_revision(prior.expected_version)?,
            None => 1,
        };
        if expected_version != next {
            return Err(Error::Conflict(
                "interaction resolution version mismatch".into(),
            ));
        }
        Ok(ticket)
    }

    /// Reads the authoritative ticket and latest resolution from its conversation.
    pub async fn read(
        &self,
        id: InteractionId,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        let aggregate = self.aggregate().await?;
        Ok(aggregate
            .reducer()
            .interaction(&interaction_uuid(id)?)
            .cloned())
    }

    /// Loads and validates the request under the owner's explicit read grant.
    pub async fn read_request(&self, id: InteractionId) -> Result<Option<Interaction>> {
        let Some((ticket, _)) = self.read(id).await? else {
            return Ok(None);
        };
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            ticket.request.volume(),
            VolumeOperation::Read,
        )?;
        let bytes = self
            .host
            .read_content(&ticket.request, &grant, self.maximum_bytes)
            .await?;
        Ok(Some(ticket.validate_request_bytes(&bytes)?))
    }

    /// Resolves only the currently admitted answer under the owner's read grant.
    pub async fn read_answer(&self, id: InteractionId) -> Result<Option<Vec<u8>>> {
        let Some((_, Some(resolution))) = self.read(id).await? else {
            return Ok(None);
        };
        let InteractionOutcome::Answered { answer } = resolution.outcome else {
            return Ok(None);
        };
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            answer.volume(),
            VolumeOperation::Read,
        )?;
        Ok(Some(
            self.host
                .read_content(&answer, &grant, self.maximum_bytes)
                .await?
                .to_vec(),
        ))
    }

    /// Reads the admitted approval decision artifact, when one was recorded.
    pub async fn read_decision_detail(&self, id: InteractionId) -> Result<Option<Vec<u8>>> {
        let Some((_, Some(resolution))) = self.read(id).await? else {
            return Ok(None);
        };
        let Some(detail) = resolution.detail else {
            return Ok(None);
        };
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            detail.volume(),
            VolumeOperation::Read,
        )?;
        Ok(Some(
            self.host
                .read_content(&detail, &grant, self.maximum_bytes)
                .await?
                .to_vec(),
        ))
    }

    async fn execute(
        &self,
        operation_id: OperationId,
        scope: Scope,
        action: Action,
    ) -> Result<ApplyResult> {
        let mut aggregate = self.aggregate().await?;
        let expected_revision = match aggregate.operation_revision(operation_id).await? {
            Some(revision) => revision
                .checked_sub(1)
                .ok_or_else(|| Error::Invalid("committed interaction revision is zero".into()))?,
            None => aggregate.reducer().revision(),
        };
        let command = Command {
            operation_id,
            idempotency_key: IdempotencyKey::new(format!("interaction:{operation_id}"))?,
            expected_revision,
            scope,
            causal_parent: None,
            action,
        };
        aggregate.execute(command).await
    }

    pub(super) async fn verify_approval(
        &self, id: InteractionId, operation: OperationId, digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        // Declared before the aggregate: its conservative logical allowance
        // is released only after the borrowed replay result has been dropped.
        let _replay_hold;
        let aggregate = if let Some(io) = io {
            io.claim(self.authority.id.len() as u64)?;
            let verifier = self.verifier.clone_for_preparation(io)?;
            let result = StreamAggregate::open_measured(
                &self.stream, self.authority.clone(), verifier, self.schemas.clone(),
                io.remaining()?, &io.cancellation,
            ).await;
            let peak = result.as_ref().map_or(0, |receipt| receipt.work.peak_allocation_bytes);
            let aggregate = io.receive_content(result)?;
            // The original replay decoder already admitted this bounded result.
            // Transfer its logical high-water allowance while borrowing it,
            // not an invented Reducer/RSS/allocator-node footprint.
            _replay_hold = Some(io.reserve_decode(peak)?);
            aggregate
        } else {
            _replay_hold = None;
            self.aggregate().await?
        };
        let agent = aggregate.reducer().conversation()
            .and_then(|conversation| conversation.agent)
            .ok_or_else(|| Error::Conflict("interaction conversation is not bound to an agent".into()))?;
        if self.private_volume.owner() != &VolumeOwner::Agent(agent) {
            return Err(Error::Unauthorized("interaction private volume belongs to another agent".into()));
        }
        let Some((ticket, Some(resolution))) = aggregate.reducer().interaction(interaction_uuid(id)?) else {
            return Err(Error::Unauthorized("tool installation lacks a resolved approval".into()));
        };
        if !matches!(&resolution.outcome, InteractionOutcome::Approved)
            || !ticket.approval.as_ref().is_some_and(|binding|
                binding.operation_id == operation && binding.action_digest == digest) {
            return Err(Error::Unauthorized("approval does not authorize this tool definition".into()));
        }
        let bytes = self.read_approval_artifact(&ticket.request, io).await?;
        let request = if let Some(io) = io {
            let wire: ApprovalRequestWire = io.decode_json(&bytes)?;
            let request = Interaction::Approval {
                prompt: wire.prompt, operation_id: wire.operation_id, action_digest: wire.action_digest,
            };
            ticket.validate_decoded_request(&request)?;
            request
        } else { ticket.validate_request_bytes(&bytes)? };
        if let Some(detail) = &resolution.detail {
            if detail.descriptor().media_type() != "application/json" {
                return Err(Error::Invalid("approval decision artifact is invalid".into()));
            }
            let bytes = self.read_approval_artifact(detail, io).await?;
            if let Some(io) = io {
                let wire: ApprovalResponseWire = io.decode_json(&bytes)?;
                let response = InteractionResponse::Approval { approved: wire.approved, reason: wire.reason };
                ticket.validate_decoded_decision(&request, &resolution.outcome, &response)?;
            } else { ticket.validate_decision_bytes(&request, &resolution.outcome, detail, &bytes)?; }
        }
        if let Some(io) = io { io.remaining()?; }
        Ok(())
    }

    async fn read_approval_artifact(
        &self, reference: &FileRef, io: Option<&PreparationIo>,
    ) -> Result<bytes::Bytes> {
        if let Some(io) = io {
            ContentGrant::verify_original_volume_with_io(
                &self.verifier, &self.owner_scope, reference.volume(), VolumeOperation::Read, io)?;
            self.host.read_pinned_io(reference, self.maximum_bytes, Some(io)).await
        } else {
            let grant = ContentGrant::verify(&self.verifier, &self.owner_scope,
                reference.volume(), VolumeOperation::Read)?;
            Ok(self.host.read_content(reference, &grant, self.maximum_bytes).await?.into())
        }
    }

    async fn aggregate(&self) -> Result<StreamAggregate<P>> {
        let verifier = FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            self.verifier.clone(),
            self.owner_scope.clone(),
            self.maximum_bytes,
        )?;
        let aggregate = StreamAggregate::open(
            &self.stream,
            self.authority.clone(),
            self.verifier.clone(),
            self.schemas.clone(),
        )
        .await?
        .with_content_verifier(Arc::new(verifier));
        let agent = aggregate
            .reducer()
            .conversation()
            .and_then(|conversation| conversation.agent)
            .ok_or_else(|| {
                Error::Conflict("interaction conversation is not bound to an agent".into())
            })?;
        if self.private_volume.owner() != &VolumeOwner::Agent(agent) {
            return Err(Error::Unauthorized(
                "interaction private volume belongs to another agent".into(),
            ));
        }
        Ok(aggregate)
    }

    async fn stage<T: serde::Serialize>(
        &self,
        id: InteractionId,
        kind: &str,
        value: &T,
    ) -> Result<FileRef> {
        let bytes = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() as u64 > self.maximum_bytes {
            return Err(Error::Invalid("interaction payload exceeds limit".into()));
        }
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            &self.private_volume,
            VolumeOperation::Write,
        )?;
        self.host
            .put_internal_content(
                &self.private_volume,
                &grant,
                &format!(".system/interactions/{id}/{kind}.json"),
                &bytes,
                "application/json",
                &format!("{kind}.json"),
                self.maximum_bytes,
                &IdempotencyKey::new(format!("interaction:{id}:{kind}"))?,
                InternalContentClass::Interaction,
            )
            .await
    }
}

fn interaction_uuid(id: InteractionId) -> Result<Uuid> {
    Ok(Uuid::from_bytes(id.into_bytes()))
}
