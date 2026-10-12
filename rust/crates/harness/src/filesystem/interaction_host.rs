//! One Filesystem/Stream bridge for the conversation-owned interaction ledger.

use super::{FilesystemContentVerifier, FilesystemHost, InternalContentClass};
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
use acyclic_stream::StreamProvider;
use super::{PreparationIo, PreparationRecord, DecodeReservation};
use super::fork_preparer_io::preparation_slot_bytes;
use crate::wire::ErrorCode;

use std::sync::Arc;
use uuid::Uuid;

/// Stages participant data privately and commits only references to one conversation history.
pub struct FilesystemInteractionHost<P, A, O> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    verifier: AuthorityVerifier,
    schemas: SchemaRegistry,
    owner_scope: Scope,
    private_volume: VolumeRef,
    maximum_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}


const DIAGNOSTIC_LABELS: &[&str] = &[
    "Interaction", "InteractionResponse", "ChoiceOption", "type", "question", "choice",
    "form", "approval", "prompt", "response_schema", "options", "min_selections",
    "max_selections", "title", "schema", "operation_id", "action_digest",
    "id", "label", "description", "value", "option_ids", "approved", "reason",
];
impl PreparationRecord for Interaction {
    fn maximum_slot_bytes() -> u64 {
        preparation_slot_bytes::<Self>().max(std::mem::size_of::<crate::interaction::ChoiceOption>() as u64)
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
            + DIAGNOSTIC_LABELS.iter().map(|label| label.len() as u64 + 4).sum::<u64>()
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema { crate::contract::JsonValueSchema::Interaction }
}
impl PreparationRecord for InteractionResponse {
    fn diagnostic_label_bytes() -> u64 { <Interaction as PreparationRecord>::diagnostic_label_bytes() }
    fn json_value_schema() -> crate::contract::JsonValueSchema { crate::contract::JsonValueSchema::InteractionResponse }
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
            self.verifier.verify(&scope, &mut None)?;
            let result = self.read(id, None).await?;
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
where P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    /// Binds the authenticated conversation and private content owner.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: Arc<P>, host: Arc<FilesystemHost<A, O>>, authority: &Authority,
        verifier: &AuthorityVerifier, schemas: &SchemaRegistry, owner_scope: &Scope,
        private_volume: &VolumeRef, maximum_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (verifier, schemas, owner_scope, private_volume) = crate::executor::with_history_io(io, |work| {
            verifier.verify_audience(authority, work)?;
            if authority.kind != crate::core::AggregateKind::Conversation
                || private_volume.class() != VolumeClass::AgentPrivate
                || private_volume.provider() != &host.provider || maximum_bytes == 0 {
                return Err(interaction_boundary(io, ErrorCode::Invalid,
                    "interaction host requires a conversation, private volume, and positive limit"));
            }
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                ContentGrant::authorize_volume(verifier, owner_scope, private_volume, operation, u64::MAX, work)?;
            }
            Ok((verifier.copy(work)?, schemas.copy(work)?,
                owner_scope.copy(work)?, private_volume.copy(work)?))
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => {
                io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                Some(io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| interaction_boundary(Some(io), ErrorCode::Invalid, "interaction host custody underflows"))?)?)
            }
            None => None,
        };
        let _ = before;
        Ok(Self { stream, host, verifier, schemas, owner_scope, private_volume,
            maximum_bytes, #[cfg(not(target_arch = "wasm32"))] _native_metadata })
    }

    pub(super) fn verify_responder_scope_with_io(
        &self, id: InteractionId, scope: &Scope, io: Option<&PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| self.verifier.verify(scope, work))?;
        let grant_hold;
        let grant = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("interaction:respond:{id}"), work))?;
        grant_hold = io.map(|io| io.guard_owned(grant.capacity() as u64)).transpose()?;
        let examined = acyclic_fs::WorkCounters { items_examined: 2, ..Default::default() };
        if let Some(io) = io { io.preflight(examined)?; }
        let allowed = scope.capabilities().contains(capability::INTERACTION_RESOLVE)
            && scope.capabilities().contains(&grant);
        if let Some(io) = io { io.charge(examined)?; }
        drop(grant);
        drop(grant_hold);
        if !allowed { return Err(interaction_boundary(io, ErrorCode::Unauthorized, "responder lacks the exact interaction grant")); }
        Ok(())
    }

    pub(crate) async fn stage_request_with_io(
        &self, id: InteractionId, request: &Interaction, deadline_unix_ms: Option<u64>, io: Option<&PreparationIo>,
    ) -> Result<InteractionTicket> {
        if id.into_bytes() == [0; 16] || deadline_unix_ms == Some(0) {
            return Err(interaction_boundary(io, ErrorCode::Invalid, "interaction ticket identity or deadline is invalid"));
        }
        crate::executor::with_history_io(io, |work| request.validate(work))?;
        let approval = match request {
            Interaction::Approval { operation_id, action_digest, .. } => Some(ApprovalBinding {
                operation_id: *operation_id, action_digest: *action_digest,
            }), _ => None,
        };
        let ticket = InteractionTicket { id: Uuid::from_bytes(id.into_bytes()), kind: request.kind(),
            request: self.stage(id, "request", request, io).await?, deadline_unix_ms, approval };
        crate::executor::with_history_io(io, |work| ticket.validate(work))?;
        Ok(ticket)
    }

    /// Stages an answer against its pinned interaction version.
    pub async fn stage_answer(
        &self, id: InteractionId, expected_version: u64, operation_id: OperationId,
        response: &InteractionResponse, io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        if expected_version == 0 { return Err(interaction_boundary(io, ErrorCode::Invalid, "interaction answer version must be positive")); }
        let kind_hold;
        let kind = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("answer-{expected_version}-{operation_id}"), work))?;
        kind_hold = io.map(|io| io.guard_owned(kind.capacity() as u64)).transpose()?;
        let result = self.stage(id, &kind, response, io).await;
        drop(kind);
        drop(kind_hold);
        result
    }

    /// Admits one open event under the optional original task lease.
    pub async fn open(
        &self, operation_id: OperationId, scope: Option<&Scope>, ticket: &InteractionTicket,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>, io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        loop {
            let ticket_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let copied = crate::executor::with_history_io(io, |work|
                crate::wire_codec::CodecQuota::produce(work, |copy_quota| crate::core::copy_ticket(ticket, copy_quota)).map(|(ticket, _)| ticket))?;
            ticket_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| interaction_boundary(Some(io), ErrorCode::Invalid, "interaction ticket custody underflows"))?)).transpose()?;
            let result = self.execute(operation_id, scope.unwrap_or(&self.owner_scope),
                Action::OpenInteraction { ticket: copied }, owner, io).await;
            drop(ticket_hold);
            match result {
                Err(error) if error.code() == ErrorCode::Conflict => {
                    let request_hold;
                    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                    let request = self.read_request(InteractionId::from_bytes(ticket.id.into_bytes()), io).await?;
                    request_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                        .ok_or_else(|| interaction_boundary(Some(io), ErrorCode::Invalid, "interaction request custody underflows"))?)).transpose()?;
                    let opened = request.is_some();
                    drop(request);
                    drop(request_hold);
                    if opened { return Err(error); }
                }
                result => return result,
            }
        }
    }

    /// Resolves with the exact responder grant and pinned version.
    pub async fn resolve(
        &self, operation_id: OperationId, scope: &Scope, resolution: InteractionResolution,
        io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        self.execute(operation_id, scope, Action::ResolveInteraction { resolution }, None, io).await
    }
    /// Authenticates a responder, validates one typed answer against the
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
            .read_request(id, None)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        request.validate_response(&response, &mut None)?;
        let answer = self
            .stage_answer(id, expected_version, operation_id, &response, None)
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
        ResolutionReceipt::from_apply_result(self.resolve(operation_id, &scope, resolution, None).await?)
    }

    /// Resolves an approval of the ticket's exact operation and argument digest.
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
            self.stage_answer(id, expected_version, operation_id, &response, None)
                .await?,
        );
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version,
            outcome,
            detail,
        };
        resolution.validate(&ticket)?;
        ResolutionReceipt::from_apply_result(self.resolve(operation_id, &scope, resolution, None).await?)
    }

    async fn open_resolution(
        &self,
        scope: &Scope,
        id: InteractionId,
        expected_version: u64,
    ) -> Result<InteractionTicket> {
        self.verifier.verify(scope, &mut None)?;
        if !scope
            .capabilities()
            .contains(capability::INTERACTION_RESOLVE)
        {
            return Err(Error::Unauthorized(
                "scope lacks interaction:resolve".into(),
            ));
        }
        let (ticket, prior) = self
            .read(id, None)
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


    /// Reads the authoritative ticket and latest resolution.
    pub async fn read(&self, id: InteractionId, io: Option<&PreparationIo>)
        -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        let aggregate = self.aggregate(io).await?;
        crate::executor::with_history_io(io, |work| {
            aggregate.0.reducer().interaction(&Uuid::from_bytes(id.into_bytes())).map(|(ticket, resolution)| {
                Ok((crate::wire_codec::CodecQuota::produce(work, |copy_quota| crate::core::copy_ticket(ticket, copy_quota))?.0,
                    resolution.as_ref().map(|resolution| crate::wire_codec::CodecQuota::produce(work, |copy_quota| crate::core::copy_resolution(resolution, copy_quota)).map(|(value, _)| value)).transpose()?))
            }).transpose()
        })
    }

    pub(crate) async fn read_outcome_with_io(&self, id: InteractionId, io: Option<&PreparationIo>)
        -> Result<Option<InteractionOutcome>> {
        let aggregate = self.aggregate(io).await?;
        crate::executor::with_history_io(io, |work| {
            aggregate.0.reducer().interaction(&Uuid::from_bytes(id.into_bytes()))
                .and_then(|(_, resolution)| resolution.as_ref()).map(|resolution|
                    crate::wire_codec::CodecQuota::produce(work, |copy_quota| crate::core::copy_interaction_outcome(&resolution.outcome, copy_quota))
                        .map(|(value, _)| value)).transpose()
        })
    }

    /// Loads the pinned request under the owner read grant.
    pub async fn read_request(&self, id: InteractionId, io: Option<&PreparationIo>) -> Result<Option<Interaction>> {
        let aggregate = self.aggregate(io).await?;
        let Some((ticket, _)) = aggregate.0.reducer().interaction(&Uuid::from_bytes(id.into_bytes())) else { return Ok(None); };
        let bytes_hold;
        let bytes = self.read_file(&ticket.request, io).await?;
        bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let request = match io {
            Some(io) => {
                let decoded = io.decode(std::marker::PhantomData::<Interaction>, &bytes, ErrorCode::Storage)?;
                crate::executor::with_history_io(Some(io), |work| ticket.validate_decoded_request(&decoded.0, work))?;
                crate::filesystem::retain_decoded(decoded)
            }
            None => ticket.validate_request_bytes(&bytes, &mut None)?.0,
        };
        drop(bytes);
        drop(bytes_hold);
        Ok(Some(request))
    }

    pub(crate) async fn read_resolution_file(
        &self, id: InteractionId, detail: bool, io: Option<&PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let aggregate = self.aggregate(io).await?;
        let Some((_, Some(resolution))) = aggregate.0.reducer().interaction(&Uuid::from_bytes(id.into_bytes())) else { return Ok(None); };
        let reference = if detail { resolution.detail.as_ref() } else {
            match &resolution.outcome { InteractionOutcome::Answered { answer } => Some(answer.as_ref()), _ => None }
        };
        match reference { Some(reference) => self.read_file(reference, io).await.map(Some), None => Ok(None) }
    }

    async fn read_file(&self, reference: &FileRef, io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
        let maximum = io.map(|io| io.remaining().map(|budget| budget.bytes_encoded)).transpose()?.unwrap_or(u64::MAX);
        crate::executor::with_history_io(io, |work| {
            reference.validate(work)?;
            ContentGrant::authorize_volume(&self.verifier, &self.owner_scope, reference.volume(), VolumeOperation::Read, maximum, work)
        })?;
        let bytes = self.host.read_pinned_io(reference, self.maximum_bytes, io).await?;
        if io.is_none() { self.host.retain_file_generation(reference).await?; }
        Ok(bytes)
    }

    pub(super) async fn verify_approval_with_io(
        &self, id: InteractionId, operation: OperationId, digest: [u8; 32], io: Option<&PreparationIo>,
    ) -> Result<()> {
        let aggregate = self.aggregate(io).await?;
        let Some((ticket, Some(resolution))) = aggregate.0.reducer().interaction(&Uuid::from_bytes(id.into_bytes())) else {
            return Err(interaction_boundary(io, ErrorCode::Unauthorized, "tool installation lacks a resolved approval"));
        };
        if !matches!(&resolution.outcome, InteractionOutcome::Approved)
            || !ticket.approval.as_ref().is_some_and(|binding| binding.operation_id == operation && binding.action_digest == digest) {
            return Err(interaction_boundary(io, ErrorCode::Unauthorized, "approval does not authorize this tool definition"));
        }
        let bytes_hold;
        let bytes = self.read_file(&ticket.request, io).await?;
        bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let request = match io {
            Some(io) => {
                let request = io.decode(std::marker::PhantomData::<Interaction>, &bytes, ErrorCode::Storage)?;
                crate::executor::with_history_io(Some(io), |work| ticket.validate_decoded_request(&request.0, work))?;
                (request.0, Some(request.1))
            }
            None => (ticket.validate_request_bytes(&bytes, &mut None)?.0, None),
        };
        drop(bytes);
        drop(bytes_hold);
        if let Some(detail) = &resolution.detail {
            if detail.descriptor().media_type() != "application/json" {
                return Err(interaction_boundary(io, ErrorCode::Invalid, "approval decision artifact is invalid"));
            }
            let bytes_hold;
            let bytes = self.read_file(detail, io).await?;
            bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            match io {
                Some(io) => {
                    let response = io.decode(std::marker::PhantomData::<InteractionResponse>, &bytes, ErrorCode::Storage)?;
                    crate::executor::with_history_io(Some(io), |work| ticket.validate_decision(
                        &request.0, &resolution.outcome, detail, &response.0, work))?;
                }
                None => ticket.validate_decision_bytes(&request.0, &resolution.outcome, detail, &bytes, &mut None)?,
            }
            drop(bytes);
            drop(bytes_hold);
        }
        Ok(())
    }

    async fn execute(
        &self, operation_id: OperationId, scope: &Scope, action: Action,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>, io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        let command_hold;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some(owner) = owner { owner.require_interaction_grant(&mut work)?; owner.verify(false, &mut work).await?; }
        let mut aggregate = self.aggregate(io).await?;
        let committed_hold;
        let (committed, _) = crate::store::operations::bounded_operation_with_retained_quota(
            &self.stream, self.verifier.audience(), &self.verifier, operation_id,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64, true, &mut work).await?;
        committed_hold = io.map(|io| io.guard_owned(committed.as_ref().map_or(0, |(_, bytes)| *bytes))).transpose()?;
        let committed = (committed, committed_hold);
        let expected_revision = match &committed.0 {
            Some((event, _)) => event.revision.checked_sub(1).ok_or_else(|| interaction_boundary(io, ErrorCode::Invalid, "committed interaction revision is zero"))?,
            None => aggregate.0.reducer().revision(),
        };
        drop(committed);
        let after_aggregate = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let scope = scope.copy(&mut work)?;
        let idempotency_key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("interaction:{operation_id}"), work))?)?;
        command_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(after_aggregate)
            .ok_or_else(|| interaction_boundary(Some(io), ErrorCode::Invalid, "interaction command custody underflows"))?)).transpose()?;
        let command = Command { operation_id, idempotency_key, expected_revision, scope, causal_parent: None, action };
        let result = aggregate.0.execute(command, owner, &mut work).await;
        drop(aggregate);
        drop(command_hold);
        result
    }

    async fn aggregate<'a>(&self, io: Option<&'a PreparationIo>)
        -> Result<(StreamAggregate<P>, Option<DecodeReservation<'a>>)> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|work| work as &mut dyn crate::store::history::HistoryIo);
        let authority = match work.as_deref_mut() { Some(work) => work.clone_authority(self.verifier.audience())?, None => self.verifier.audience().clone() };
        let verifier = self.verifier.copy(&mut work)?;
        let schemas = self.schemas.copy(&mut work)?;
        let content = FilesystemContentVerifier::new(Arc::clone(&self.host), &self.verifier, &self.owner_scope, self.maximum_bytes, io)?;
        let aggregate = StreamAggregate::open(&self.stream, authority, verifier, schemas, None,
            crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut work)
            .await?.with_content_verifier(content);
        let agent = aggregate.reducer().conversation().and_then(|conversation| conversation.agent)
            .ok_or_else(|| interaction_boundary(io, ErrorCode::Conflict, "interaction conversation is not bound to an agent"))?;
        if self.private_volume.owner() != &VolumeOwner::Agent(agent) {
            return Err(interaction_boundary(io, ErrorCode::Unauthorized, "interaction private volume belongs to another agent"));
        }
        let guard = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), ErrorCode::Invalid, "interaction aggregate custody underflows"))?)).transpose()?;
        Ok((aggregate, guard))
    }

    async fn stage<T: serde::Serialize>(
        &self, id: InteractionId, kind: &str, value: &T, io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        let bytes_hold;
        let bytes = match io { Some(io) => io.encode(value, self.maximum_bytes, false)?,
            None => serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?.into() };
        bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if bytes.len() as u64 > self.maximum_bytes { return Err(interaction_boundary(io, ErrorCode::Invalid, "interaction payload exceeds limit")); }
        let names_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let path = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!(".system/interactions/{id}/{kind}.json"), work))?;
        let name = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("{kind}.json"), work))?;
        let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("interaction:{id}:{kind}"), work))?)?;
        names_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), ErrorCode::Invalid, "interaction name custody underflows"))?)).transpose()?;
        let grant = crate::executor::with_history_io(io, |work| ContentGrant::verify(
            &self.verifier, &self.owner_scope, &self.private_volume, VolumeOperation::Write,
            io.map_or(Ok(u64::MAX), |io| io.remaining().map(|budget| budget.bytes_encoded))?, work))?;
        let grant_hold = io.map(|io| io.guard_owned(grant.logical_owned_bytes()?)).transpose()?;
        let result = self.host.put_internal_content(&self.private_volume, &grant, &path, &bytes,
            "application/json", &name, self.maximum_bytes, &key, InternalContentClass::Interaction, io).await;
        drop(grant); drop(grant_hold);
        drop(key); drop(name); drop(path); drop(names_hold); drop(bytes); drop(bytes_hold);
        result
    }
}

pub(super) fn interaction_boundary(io: Option<&PreparationIo>, code: ErrorCode, reason: &'static str) -> Error {
    if code == ErrorCode::NotFound && io.is_none_or(|io| io.original_native_work().is_none()) {
        Error::NotFound(reason.into())
    } else { stock_boundary(io, code, reason) }
}

use crate::executor::{stock_boundary};
