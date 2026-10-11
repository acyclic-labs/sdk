//! One Filesystem/Stream bridge for the conversation-owned interaction ledger.

use super::{FilesystemContentVerifier, FilesystemHost, InternalContentClass};
use super::fork_preparer_io::{PreparationIo, PreparationRecord, preparation_slot_bytes};
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
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

const INTERACTION_DIAGNOSTIC_LABEL_BYTES: u64 = {
    let labels = [
        "Interaction", "InteractionResponse", "ChoiceOption", "type", "question", "choice",
        "form", "approval", "prompt", "response_schema", "options", "min_selections",
        "max_selections", "title", "schema", "operation_id", "action_digest",
        "id", "label", "description", "value", "option_ids", "approved", "reason",
    ];
    let mut bytes = 0;
    let mut index = 0;
    while index < labels.len() {
        bytes += labels[index].len() as u64 + 4;
        index += 1;
    }
    bytes
};

impl PreparationRecord for Interaction {
    fn maximum_slot_bytes() -> u64 {
        preparation_slot_bytes::<Self>()
            .max(std::mem::size_of::<crate::interaction::ChoiceOption>() as u64)
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes() + INTERACTION_DIAGNOSTIC_LABEL_BYTES
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::Interaction
    }
}

impl PreparationRecord for InteractionResponse {
    fn maximum_slot_bytes() -> u64 { preparation_slot_bytes::<Self>() }
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes() + INTERACTION_DIAGNOSTIC_LABEL_BYTES
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::InteractionResponse
    }
}

impl<P, A, O> FilesystemInteractionHost<P, A, O> {

    pub(super) fn verify_responder_scope_with_io(
        &self, id: InteractionId, scope: &Scope, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let mut borrowed = io.map(PreparationIo::history_io);
        self.verifier.verify_with_work(scope, &mut borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo))?;
        let grant_hold;
        let grant = interaction_text("interaction:respond:", Some(id.into_bytes()), "", "", "", io)?;
        grant_hold = io.map(|io| io.guard_owned(grant.capacity() as u64)).transpose()?;
        let examined = acyclic_fs::WorkCounters { items_examined: 2, ..Default::default() };
        if let Some(io) = io { io.preflight(examined)?; }
        let route = scope.capabilities().contains(capability::INTERACTION_RESOLVE);
        let exact = scope.capabilities().contains(&grant);
        let allowed = route && exact;
        if let Some(io) = io { io.charge(examined)?; }
        drop(grant);
        drop(grant_hold);
        if !allowed {
            return Err(interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "responder lacks the exact interaction grant"));
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
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata: None,
        })
    }

    /// Binds the same conversation owner on the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "the existing interaction host owner contract")]
    pub fn new_with_native_work(
        stream: StreamClient<P>, host: Arc<FilesystemHost<A, O>>,
        authority: &Authority, verifier: &AuthorityVerifier, schemas: &SchemaRegistry,
        owner_scope: &Scope, private_volume: &VolumeRef, maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let io = PreparationIo::from_native_work(work)?;
        let mut borrowed = io.history_io();
        let authority = crate::store::history::HistoryIo::clone_authority(&mut borrowed, authority)?;
        let (verifier, custody) = verifier.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let (scope, custody) = owner_scope.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let volume = private_volume.clone_with_io(&io)?;
        io.claim(std::mem::size_of::<Self>() as u64)?;
        let mut interaction_host = Self::new_with_io(
            stream, host, authority, verifier, schemas.clone(), scope,
            volume, maximum_bytes, Some(&io),
        )?;
        let retained = io.owned_bytes()?;
        interaction_host._native_metadata = Some(io.into_native_custody(retained)?);
        Ok(interaction_host)
    }

    pub(crate) async fn stage_request_with_io(
        &self, id: InteractionId, request: &Interaction, deadline_unix_ms: Option<u64>,
        io: Option<&PreparationIo>,
    ) -> Result<InteractionTicket> {
        if id.into_bytes() == [0; 16] || deadline_unix_ms == Some(0) {
            return Err(interaction_boundary(io, crate::wire::ErrorCode::Invalid,
                "interaction ticket identity or deadline is invalid"));
        }
        let mut borrowed = io.map(PreparationIo::history_io);
        request.validate_with_work(&mut borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo))?;
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
        let reference = self.stage(id, "request", request, io).await?;
        let ticket = InteractionTicket {
            id: interaction_uuid(id)?,
            kind: request.kind(),
            request: reference,
            deadline_unix_ms,
            approval,
        };
        ticket.validate_with_work(&mut borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo))?;
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
        self.stage_answer_with_io(id, expected_version, operation_id, response, None).await
    }

    pub(crate) async fn stage_answer_with_io(
        &self, id: InteractionId, expected_version: u64, operation_id: OperationId,
        response: &InteractionResponse, io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        if expected_version == 0 {
            return Err(interaction_boundary(io, crate::wire::ErrorCode::Invalid,
                "interaction answer version must be positive"));
        }
        let kind_hold;
        let length = 7 + expected_version.ilog10() as usize + 1 + 1 + 36;
        let copied = acyclic_fs::WorkCounters { bytes_copied: length as u64, ..Default::default() };
        if let Some(io) = io {
            io.preflight(copied)?;
            io.claim(length as u64)?;
            io.allocation_attempt(length as u64)?;
        }
        let mut kind = String::with_capacity(length);
        use std::fmt::Write as _;
        write!(&mut kind, "answer-{expected_version}-{operation_id}")
            .map_err(|_| interaction_boundary(io, crate::wire::ErrorCode::Invalid,
                "interaction answer identity cannot be formatted"))?;
        if let Some(io) = io { io.charge(copied)?; }
        kind_hold = io.map(|io| io.guard_owned(kind.capacity() as u64)).transpose()?;
        let result = self.stage(id, &kind, response, io).await;
        drop(kind);
        drop(kind_hold);
        result
    }

    /// Replays the exact conversation and admits one typed open event.
    pub async fn open(
        &self,
        operation_id: OperationId,
        scope: Scope,
        ticket: InteractionTicket,
    ) -> Result<ApplyResult> {
        self.execute_open_with_io(operation_id, &scope, &ticket, None, None).await
    }

    pub(crate) async fn open_request_with_io(
        &self, operation_id: OperationId, ticket: &InteractionTicket,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>, io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.execute_open_with_io(operation_id, &self.owner_scope, ticket, owner, io)
            .await.map(|_| ())
    }

    async fn execute_open_with_io(
        &self, operation_id: OperationId, scope: &Scope, ticket: &InteractionTicket,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>, io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        loop {
            let attempt_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let mut borrowed = io.map(PreparationIo::history_io);
            let mut work = borrowed.as_mut().map(
                |work| work as &mut dyn crate::store::history::HistoryIo);
            if let Some(owner) = owner {
                owner.require_interaction_grant_with_work(&mut work)?;
                owner.verify_with_work(false, &mut work).await?;
            }
            let mut aggregate = self.aggregate(io).await?;
            let (committed, _) = crate::store::operations::find_operation_bounded_with_work(
                &self.stream, &self.authority, &self.verifier, operation_id,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64, true, &mut work,
            ).await?;
            let expected_revision = match committed {
                Some(event) => event.revision.checked_sub(1).ok_or_else(|| interaction_boundary(
                    io, crate::wire::ErrorCode::Invalid, "committed interaction revision is zero"))?,
                None => aggregate.reducer().revision(),
            };
            let scope = self.clone_scope_with_io(scope, io)?;
            let command_ticket = ticket.clone_with_work(&mut work)?;
            let idempotency_key = IdempotencyKey::new(interaction_text(
                "interaction:", Some(operation_id.into_bytes()), "", "", "", io)?)?;
            attempt_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                    "interaction attempt custody underflows"))?)).transpose()?;
            let command = Command { operation_id, idempotency_key, expected_revision,
                scope, causal_parent: None, action: Action::OpenInteraction { ticket: command_ticket } };
            let result = match owner {
                Some(owner) => aggregate.execute_task_command_with_work(command, owner, &mut work).await,
                None => aggregate.execute_with_work(command, &mut work).await,
            };
            drop(aggregate);
            drop(attempt_hold);
            match result {
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {
                    let request_hold;
                    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                    let original = self.read_request_with_io(
                        InteractionId::from_bytes(ticket.id.into_bytes()), io).await?;
                    request_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                        .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                            "interaction request custody underflows"))?)).transpose()?;
                    let opened = original.is_some();
                    drop(original);
                    drop(request_hold);
                    if opened { return Err(error); }
                }
                result => return result,
            }
        }
    }

    fn clone_scope_with_io(&self, scope: &Scope, io: Option<&PreparationIo>) -> Result<Scope> {
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        scope.clone_with_work(&mut work)
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
            Action::ResolveInteraction { resolution }, None)
        .await
    }

    pub(crate) async fn resolve_with_io(
        &self, operation_id: OperationId, scope: &Scope,
        resolution: InteractionResolution, io: Option<&PreparationIo>,
    ) -> Result<()> {
        let scope = self.clone_scope_with_io(scope, io)?;
        self.execute(operation_id, scope, Action::ResolveInteraction { resolution }, io)
            .await.map(|_| ())
    }

    /// Authenticates a responder, validates one typed answer against the admitted request, stages its bytes, then CAS-resolves the conversation.
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
        self.read_with_io(id, None).await
    }

    /// Inspects the same authoritative DTO while retaining its original native custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn read_with_native_work<'a>(
        &'a self, id: InteractionId, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (
        Option<(InteractionTicket, Option<InteractionResolution>)>, acyclic_fs::NativeAllocation,
    )>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let inspection = self.read_with_io(id, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((inspection, custody))
        })
    }

    pub(crate) async fn read_with_io(
        &self, id: InteractionId, io: Option<&PreparationIo>,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        let aggregate_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let aggregate = self.aggregate(io).await?;
        aggregate_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction aggregate custody underflows"))?)).transpose()?;
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let value = aggregate.reducer().interaction(&interaction_uuid(id)?)
            .map(|value| crate::core::native_interaction_state_clone_with_custody(value, &mut work)
                .map(|(value, _)| value)).transpose()?;
        drop(aggregate);
        drop(aggregate_hold);
        Ok(value)
    }

    pub(crate) async fn read_outcome_with_io(
        &self, id: InteractionId, io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        let aggregate_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let aggregate = self.aggregate(io).await?;
        aggregate_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction aggregate custody underflows"))?)).transpose()?;
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let outcome = aggregate.reducer().interaction(&interaction_uuid(id)?)
            .and_then(|(_, resolution)| resolution.as_ref())
            .map(|resolution| resolution.outcome.clone_with_work(&mut work))
            .transpose()?;
        drop(aggregate);
        drop(aggregate_hold);
        Ok(outcome)
    }

    /// Loads and validates the request under the owner's explicit read grant.
    pub async fn read_request(&self, id: InteractionId) -> Result<Option<Interaction>> {
        self.read_request_with_io(id, None).await
    }

    /// Reads and validates the exact pinned request on the caller's original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn read_request_with_native_work<'a>(
        &'a self, id: InteractionId, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<Interaction>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let request = self.read_request_with_io(id, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((request, custody))
        })
    }

    pub(crate) async fn read_request_with_io(
        &self, id: InteractionId, io: Option<&PreparationIo>,
    ) -> Result<Option<Interaction>> {
        let aggregate_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let aggregate = self.aggregate(io).await?;
        aggregate_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction aggregate custody underflows"))?)).transpose()?;
        let Some((ticket, _)) = aggregate.reducer().interaction(&interaction_uuid(id)?) else {
            return Ok(None);
        };
        let request = match io {
            Some(io) => {
                ContentGrant::verify_original_volume_with_io(&self.verifier, &self.owner_scope,
                    ticket.request.volume(), VolumeOperation::Read, io)?;
                let bytes_hold;
                let bytes = self.host.read_pinned_io(&ticket.request, self.maximum_bytes, Some(io)).await?;
                bytes_hold = io.guard_bytes(&bytes)?;
                let request = io.decode_preparation::<Interaction>(&bytes)?;
                let mut borrowed = io.history_io();
                ticket.validate_decoded_request_with_work(&request, &mut Some(&mut borrowed))?;
                let request = request.into_retained();
                drop(bytes);
                drop(bytes_hold);
                request
            }
            None => {
                let grant = ContentGrant::verify(&self.verifier, &self.owner_scope,
                    ticket.request.volume(), VolumeOperation::Read)?;
                let bytes = self.host.read_content(&ticket.request, &grant, self.maximum_bytes).await?;
                ticket.validate_request_bytes(&bytes)?
            }
        };
        drop(aggregate);
        drop(aggregate_hold);
        Ok(Some(request))
    }

    pub(crate) async fn read_answer_with_io(
        &self, id: InteractionId, io: Option<&PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let aggregate_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let aggregate = self.aggregate(io).await?;
        aggregate_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction aggregate custody underflows"))?)).transpose()?;
        let Some((_, Some(resolution))) = aggregate.reducer().interaction(&interaction_uuid(id)?) else {
            return Ok(None);
        };
        let InteractionOutcome::Answered { answer } = &resolution.outcome else { return Ok(None); };
        let bytes = self.read_resolution_content_with_io(answer, io).await?;
        drop(aggregate);
        drop(aggregate_hold);
        Ok(Some(bytes))
    }

    pub(crate) async fn read_decision_detail_with_io(
        &self, id: InteractionId, io: Option<&PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let aggregate_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let aggregate = self.aggregate(io).await?;
        aggregate_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction aggregate custody underflows"))?)).transpose()?;
        let Some((_, Some(resolution))) = aggregate.reducer().interaction(&interaction_uuid(id)?) else {
            return Ok(None);
        };
        let Some(detail) = &resolution.detail else { return Ok(None); };
        let bytes = self.read_resolution_content_with_io(detail, io).await?;
        drop(aggregate);
        drop(aggregate_hold);
        Ok(Some(bytes))
    }

    async fn read_resolution_content_with_io(
        &self, reference: &FileRef, io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        match io {
            Some(io) => {
                ContentGrant::verify_original_volume_with_io(&self.verifier, &self.owner_scope,
                    reference.volume(), VolumeOperation::Read, io)?;
                self.host.read_pinned_io(reference, self.maximum_bytes, Some(io)).await
            }
            None => {
                let grant = ContentGrant::verify(&self.verifier, &self.owner_scope,
                    reference.volume(), VolumeOperation::Read)?;
                self.host.read_content(reference, &grant, self.maximum_bytes).await
            }
        }
    }


    async fn execute(
        &self, operation_id: OperationId, scope: Scope, action: Action, io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        let aggregate_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let mut aggregate = self.aggregate(io).await?;
        aggregate_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction aggregate custody underflows"))?)).transpose()?;
        let command_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        let (committed, _) = crate::store::operations::find_operation_bounded_with_work(
            &self.stream, &self.authority, &self.verifier, operation_id,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64, true, &mut work,
        ).await?;
        let expected_revision = match committed {
            Some(event) => event.revision.checked_sub(1).ok_or_else(|| interaction_boundary(
                io, crate::wire::ErrorCode::Invalid, "committed interaction revision is zero"))?,
            None => aggregate.reducer().revision(),
        };
        let idempotency_key = IdempotencyKey::new(interaction_text(
            "interaction:", Some(operation_id.into_bytes()), "", "", "", io)?)?;
        command_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction command custody underflows"))?)).transpose()?;
        let command = Command {
            operation_id, idempotency_key, expected_revision, scope, causal_parent: None, action,
        };
        let result = aggregate.execute_with_work(command, &mut work).await;
        drop(aggregate);
        drop(aggregate_hold);
        drop(command_hold);
        result
    }

    pub(super) async fn verify_approval_with_io(
        &self, id: InteractionId, operation: OperationId, digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let replay_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let aggregate = self.aggregate(io).await?;
        replay_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "approval replay custody underflows"))?)).transpose()?;
        let Some((ticket, Some(resolution))) = aggregate.reducer().interaction(&interaction_uuid(id)?) else {
            return Err(interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "tool installation lacks a resolved approval"));
        };
        if !matches!(&resolution.outcome, InteractionOutcome::Approved)
            || !ticket.approval.as_ref().is_some_and(|binding|
                binding.operation_id == operation && binding.action_digest == digest) {
            return Err(interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "approval does not authorize this tool definition"));
        }
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        let bytes_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let bytes = self.read_resolution_content_with_io(&ticket.request, io).await?;
        bytes_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "approval request bytes custody underflows"))?)).transpose()?;
        let request_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let request = match io {
            Some(io) => {
                let request = io.decode_preparation::<Interaction>(&bytes)?;
                ticket.validate_decoded_request_with_work(&request, &mut work)?;
                request.into_retained()
            }
            None => ticket.validate_request_bytes(&bytes)?,
        };
        request_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "approval request custody underflows"))?)).transpose()?;
        drop(bytes);
        drop(bytes_hold);
        if let Some(detail) = &resolution.detail {
            if detail.descriptor().media_type() != "application/json" {
                return Err(interaction_boundary(io, crate::wire::ErrorCode::Invalid,
                    "approval decision artifact is invalid"));
            }
            let detail_bytes_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let bytes = self.read_resolution_content_with_io(detail, io).await?;
            detail_bytes_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                    "approval decision bytes custody underflows"))?)).transpose()?;
            match io {
                Some(io) => {
                    let response = io.decode_preparation::<InteractionResponse>(&bytes)?;
                    ticket.validate_decoded_decision_with_work(
                        &request, &resolution.outcome, detail, &response, &mut work)?;
                }
                None => ticket.validate_decision_bytes(
                    &request, &resolution.outcome, detail, &bytes)?,
            }
            drop(bytes);
            drop(detail_bytes_hold);
        }
        drop(request);
        drop(request_hold);
        drop(aggregate);
        drop(replay_hold);
        Ok(())
    }


    async fn aggregate(&self, io: Option<&PreparationIo>) -> Result<StreamAggregate<P>> {
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(
            |work| work as &mut dyn crate::store::history::HistoryIo);
        let authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(&self.authority)?,
            None => self.authority.clone(),
        };
        let aggregate_verifier = self.verifier.clone_with_work(&mut work)?;
        let content_authority = self.verifier.clone_with_work(&mut work)?;
        let scope = self.owner_scope.clone_with_work(&mut work)?;
        if let Some(io) = io {
            io.claim(std::mem::size_of::<FilesystemContentVerifier<A, O>>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<FilesystemContentVerifier<A, O>>() as u64)?;
        }
        let content_verifier = FilesystemContentVerifier::new_with_io(
            Arc::clone(&self.host), content_authority, scope, self.maximum_bytes, io)?;
        let content_verifier = Arc::new(content_verifier);
        let aggregate = StreamAggregate::open_with_read_limits_with_work(
            &self.stream, authority, aggregate_verifier, self.schemas.clone(), crate::store::default_projection_read_limits(), &mut work).await?.with_content_verifier(content_verifier);
        let agent = aggregate
            .reducer()
            .conversation()
            .and_then(|conversation| conversation.agent)
            .ok_or_else(|| interaction_boundary(io, crate::wire::ErrorCode::Conflict,
                "interaction conversation is not bound to an agent"))?;
        if self.private_volume.owner() != &VolumeOwner::Agent(agent) {
            return Err(interaction_boundary(io, crate::wire::ErrorCode::Unauthorized,
                "interaction private volume belongs to another agent"));
        }
        Ok(aggregate)
    }


    async fn stage<T: serde::Serialize>(
        &self, id: InteractionId, kind: &str, value: &T, io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        let bytes_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let bytes = match io {
            Some(io) => io.encode_json(value, self.maximum_bytes, false)?,
            None => serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?.into(),
        };
        bytes_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction encoding custody underflows"))?)).transpose()?;
        if bytes.len() as u64 > self.maximum_bytes {
            return Err(interaction_boundary(io, crate::wire::ErrorCode::Invalid,
                "interaction payload exceeds limit"));
        }
        let names_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let path = interaction_text(".system/interactions/", Some(id.into_bytes()), "/", kind, ".json", io)?;
        let name = interaction_text("", None, "", kind, ".json", io)?;
        let key = IdempotencyKey::new(interaction_text("interaction:", Some(id.into_bytes()), ":", kind, "", io)?)?;
        names_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "interaction name custody underflows"))?)).transpose()?;
        let result = match io {
            Some(io) => self.host.put_internal_content_original_with_io(
                &self.private_volume, &self.verifier, &self.owner_scope, &path, &bytes,
                "application/json", &name, self.maximum_bytes, &key,
                InternalContentClass::Interaction, io).await,
            None => {
                let grant = ContentGrant::verify(
                    &self.verifier, &self.owner_scope, &self.private_volume, VolumeOperation::Write)?;
                self.host.put_internal_content(&self.private_volume, &grant, &path, &bytes,
                    "application/json", &name, self.maximum_bytes, &key,
                    InternalContentClass::Interaction).await
            }
        };
        drop(key);
        drop(name);
        drop(path);
        drop(names_hold);
        drop(bytes);
        drop(bytes_hold);
        result
    }
}

fn interaction_uuid(id: InteractionId) -> Result<Uuid> {
    Ok(Uuid::from_bytes(id.into_bytes()))
}

pub(super) fn interaction_boundary(
    io: Option<&PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str,
) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::NotFound => Error::NotFound(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

fn interaction_text(
    prefix: &str, id: Option<[u8; 16]>, separator: &str, kind: &str, suffix: &str,
    io: Option<&PreparationIo>,
) -> Result<String> {
    let length = prefix.len().checked_add(if id.is_some() { 36 } else { 0 })
        .and_then(|bytes| bytes.checked_add(separator.len()))
        .and_then(|bytes| bytes.checked_add(kind.len()))
        .and_then(|bytes| bytes.checked_add(suffix.len()))
        .ok_or_else(|| interaction_boundary(io, crate::wire::ErrorCode::Invalid,
            "interaction metadata length overflows"))?;
    let copied = acyclic_fs::WorkCounters { bytes_copied: length as u64, ..Default::default() };
    if let Some(io) = io {
        io.preflight(copied)?;
        io.claim(length as u64)?;
        io.allocation_attempt(length as u64)?;
    }
    let mut text = String::with_capacity(length);
    text.push_str(prefix);
    if let Some(id) = id {
        let mut encoded = [0_u8; 36];
        text.push_str(Uuid::from_bytes(id).hyphenated().encode_lower(&mut encoded));
    }
    text.push_str(separator);
    text.push_str(kind);
    text.push_str(suffix);
    if let Some(io) = io { io.charge(copied)?; }
    Ok(text)
}
