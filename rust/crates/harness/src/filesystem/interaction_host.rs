//! One Filesystem/Stream bridge for the conversation-owned interaction ledger.

use super::{FilesystemContentVerifier, FilesystemHost, InternalContentClass};
use crate::{
    Capabilities, Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{
        ContentGrant, ContentResidencyVerifier, FileRef, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{
        Action, ApplyResult, Authority, AuthorityIssuer, AuthorityVerifier, Command,
        SchemaRegistry, Scope,
    },
    interaction::{
        ApprovalBinding, Interaction, InteractionKind, InteractionOutcome, InteractionResolution,
        InteractionResponse, InteractionTicket, ResolutionReceipt,
    },
    runtime::InteractionResolver,
    store::StreamAggregate,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{StreamClient, StreamProvider};
use std::sync::Arc;
use uuid::Uuid;

// Private to interaction admission; never installed on a model reader.
struct InteractionContentVerifier<A, O> {
    ordinary: FilesystemContentVerifier<A, O>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
    maximum_bytes: u64,
}

impl<A, O> ContentResidencyVerifier for InteractionContentVerifier<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn verify<'a>(&'a self, reference: &'a FileRef) -> futures::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.read(reference).await.map(|_| ()) })
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> futures::future::BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            if reference.path().starts_with(".system/interactions/") {
                self.host
                    .read_internal_content(
                        reference,
                        &self.volume,
                        &self.grant,
                        InternalContentClass::Interaction,
                        self.maximum_bytes,
                    )
                    .await
                    .map(|bytes| bytes.to_vec())
            } else {
                self.ordinary.read(reference).await
            }
        })
    }
}

/// Exact pending approval identity presented to a host operator boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InteractionApprovalAuthorization {
    /// Address of the pending interaction.
    pub interaction_id: InteractionId,
    /// Operation bound by the admitted approval ticket.
    pub operation_id: OperationId,
    /// Digest of the exact action and arguments bound by the ticket.
    pub action_digest: [u8; 32],
    /// Explicit operator decision selected by the host UI.
    pub approved: bool,
}

impl InteractionApprovalAuthorization {
    fn validate(&self) -> Result<()> {
        if self.interaction_id.into_bytes() == [0; 16]
            || self.operation_id.into_bytes() == [0; 16]
            || self.action_digest == [0; 32]
        {
            return Err(Error::Invalid(
                "interaction approval identity cannot be empty".into(),
            ));
        }
        Ok(())
    }
}

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

/// Host-only signer for one explicit decision on one durable approval ticket.
///
/// The signer retains the durable interaction host and revalidates the ticket
/// before issuing a scope. Model tools receive neither this signer nor its
/// issuer key.
pub struct InteractionOperatorAuthorizer<P, A, O> {
    host: Arc<FilesystemInteractionHost<P, A, O>>,
    issuer: AuthorityIssuer,
}

impl<P, A, O> InteractionOperatorAuthorizer<P, A, O>
where
    P: StreamProvider + Send + Sync + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(crate) fn new(
        host: Arc<FilesystemInteractionHost<P, A, O>>,
        issuer: AuthorityIssuer,
    ) -> Self {
        Self { host, issuer }
    }

    /// Revalidates the durable ticket and issues the smallest decision scope.
    pub async fn issue_scope(
        &self,
        authorization: &InteractionApprovalAuthorization,
    ) -> Result<Scope> {
        authorization.validate()?;
        let Some((ticket, resolution)) = self.host.read(authorization.interaction_id).await?
        else {
            return Err(Error::NotFound(format!(
                "interaction {}",
                authorization.interaction_id
            )));
        };
        let binding = ticket.approval.as_ref().ok_or_else(|| {
            Error::Invalid("interaction is not an approval ticket".into())
        })?;
        if binding.operation_id != authorization.operation_id
            || binding.action_digest != authorization.action_digest
        {
            return Err(Error::Unauthorized(
                "approval authorization does not match the durable ticket".into(),
            ));
        }
        if resolution.is_some() {
            return Err(Error::Conflict(
                "approval is no longer pending operator choice".into(),
            ));
        }
        let digest = authorization
            .action_digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let decision = if authorization.approved {
            "approved"
        } else {
            "declined"
        };
        Ok(self.issuer.root(
            format!("interaction-operator:{}", authorization.interaction_id),
            Capabilities::new([
                "interaction:resolve".to_owned(),
                ticket.responder_grant(),
                format!(
                    "interaction:decision:{}:{decision}",
                    authorization.interaction_id
                ),
                format!(
                    "interaction:approve:{}:{}:{digest}",
                    authorization.interaction_id, authorization.operation_id
                ),
            ]),
        ))
    }
}

impl<P, A, O> InteractionResolver for FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn inspect<'a>(
        &'a self,
        scope: Scope,
        id: InteractionId,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<(InteractionTicket, Option<InteractionResolution>)>>,
    > {
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
    ) -> futures::future::BoxFuture<'a, Result<ResolutionReceipt>> {
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
    ) -> futures::future::BoxFuture<'a, Result<ResolutionReceipt>> {
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
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
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
        verifier.verify_audience(&authority)?;
        if authority.kind != crate::core::AggregateKind::Conversation
            || private_volume.class() != VolumeClass::AgentPrivate
            || private_volume.provider() != &host.provider
            || maximum_bytes == 0
        {
            return Err(Error::Invalid(
                "interaction host requires a conversation, private volume, and positive limit"
                    .into(),
            ));
        }
        ContentGrant::verify(
            &verifier,
            &owner_scope,
            &private_volume,
            VolumeOperation::Read,
        )?;
        ContentGrant::verify(
            &verifier,
            &owner_scope,
            &private_volume,
            VolumeOperation::Write,
        )?;
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
        self.validate_internal_ref(&reference, id, "request", None, None)?;
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
        let reference = self
            .stage(
                id,
                &format!("answer-{expected_version}-{operation_id}"),
                response,
            )
            .await?;
        self.validate_internal_ref(
            &reference,
            id,
            "answer",
            Some(expected_version),
            Some(operation_id),
        )?;
        Ok(reference)
    }

    /// Replays the exact conversation and admits one typed open event.
    pub async fn open(
        &self,
        operation_id: OperationId,
        scope: Scope,
        ticket: InteractionTicket,
    ) -> Result<ApplyResult> {
        self.validate_ticket_admission(&ticket).await?;
        self.execute(operation_id, scope, Action::OpenInteraction { ticket })
            .await
    }

    /// Resolves only with the exact interaction responder grant and current version.
    pub async fn resolve(
        &self,
        operation_id: OperationId,
        scope: Scope,
        resolution: InteractionResolution,
    ) -> Result<ApplyResult> {
        self.validate_resolution_admission(operation_id, &scope, &resolution)
            .await?;
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
        if !scope.capabilities().contains("interaction:resolve") {
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
            Some(prior) => prior
                .expected_version
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("interaction revision exhausted".into()))?,
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
        self.validate_internal_ref(&ticket.request, id, "request", None, None)?;
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            ticket.request.volume(),
            VolumeOperation::Read,
        )?;
        let bytes = self
            .host
            .read_internal_content(
                &ticket.request,
                &self.private_volume,
                &grant,
                super::InternalContentClass::Interaction,
                self.maximum_bytes,
            )
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
        self.validate_internal_ref(
            &answer,
            id,
            "answer",
            Some(resolution.expected_version),
            None,
        )?;
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            answer.volume(),
            VolumeOperation::Read,
        )?;
        Ok(Some(
            self.host
                .read_internal_content(
                    &answer,
                    &self.private_volume,
                    &grant,
                    super::InternalContentClass::Interaction,
                    self.maximum_bytes,
                )
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
        self.validate_internal_ref(
            &detail,
            id,
            "answer",
            Some(resolution.expected_version),
            None,
        )?;
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            detail.volume(),
            VolumeOperation::Read,
        )?;
        Ok(Some(
            self.host
                .read_internal_content(
                    &detail,
                    &self.private_volume,
                    &grant,
                    super::InternalContentClass::Interaction,
                    self.maximum_bytes,
                )
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
        let expected_revision = match aggregate.reducer().operation_revision(operation_id) {
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

    fn validate_internal_ref(
        &self,
        reference: &FileRef,
        id: InteractionId,
        role: &str,
        expected_version: Option<u64>,
        expected_operation: Option<OperationId>,
    ) -> Result<()> {
        reference.validate()?;
        if reference.volume() != &self.private_volume
            || reference.descriptor().media_type() != "application/json"
        {
            return Err(Error::Unauthorized(
                "interaction artifact is outside its authenticated private volume".into(),
            ));
        }
        let prefix = format!(".system/interactions/{id}/");
        let valid_path = match (role, expected_version) {
            ("request", None) => reference.path() == format!("{prefix}request.json"),
            ("answer", Some(version)) => {
                let Some(suffix) = reference
                    .path()
                    .strip_prefix(&format!("{prefix}answer-{version}-"))
                else {
                    return Err(Error::Unauthorized(
                        "interaction answer artifact has an invalid operation path".into(),
                    ));
                };
                let Some(operation) = suffix.strip_suffix(".json") else {
                    return Err(Error::Unauthorized(
                        "interaction answer artifact is not canonical JSON".into(),
                    ));
                };
                let parsed_operation = OperationId::parse(operation).map_err(|_| {
                    Error::Unauthorized(
                        "interaction answer artifact has an invalid operation identity".into(),
                    )
                })?;
                if expected_operation.is_some_and(|expected| parsed_operation != expected) {
                    return Err(Error::Unauthorized(
                        "interaction answer artifact has an invalid operation identity".into(),
                    ));
                }
                true
            }
            _ => false,
        };
        if !valid_path {
            return Err(Error::Unauthorized(format!(
                "interaction {role} artifact has an invalid id, role, or version path"
            )));
        }
        Ok(())
    }

    async fn validate_ticket_admission(&self, ticket: &InteractionTicket) -> Result<()> {
        ticket.validate()?;
        let id = InteractionId::parse(&ticket.id.to_string())?;
        self.validate_internal_ref(&ticket.request, id, "request", None, None)?;
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            ticket.request.volume(),
            VolumeOperation::Read,
        )?;
        let bytes = self
            .host
            .read_internal_content(
                &ticket.request,
                ticket.request.volume(),
                &grant,
                InternalContentClass::Interaction,
                self.maximum_bytes,
            )
            .await?;
        ticket.validate_request_bytes(&bytes)?;
        Ok(())
    }

    async fn validate_resolution_admission(
        &self,
        operation_id: OperationId,
        scope: &Scope,
        resolution: &InteractionResolution,
    ) -> Result<()> {
        self.verifier.verify(scope)?;
        if !scope.capabilities().contains("interaction:resolve")
            || !scope
                .capabilities()
                .contains(&format!("interaction:respond:{}", resolution.id))
        {
            return Err(Error::Unauthorized(
                "scope lacks the exact interaction resolution grant".into(),
            ));
        }
        let id = InteractionId::parse(&resolution.id.to_string())?;
        let Some((ticket, _)) = self.read(id).await? else {
            return Err(Error::NotFound(format!("interaction {id}")));
        };
        self.validate_internal_ref(&ticket.request, id, "request", None, None)?;
        resolution.validate(&ticket)?;
        let request_grant = ContentGrant::verify(
            &self.verifier,
            &self.owner_scope,
            ticket.request.volume(),
            VolumeOperation::Read,
        )?;
        let request_bytes = self
            .host
            .read_internal_content(
                &ticket.request,
                ticket.request.volume(),
                &request_grant,
                InternalContentClass::Interaction,
                self.maximum_bytes,
            )
            .await?;
        let request = ticket.validate_request_bytes(&request_bytes)?;
        match (&resolution.outcome, &resolution.detail) {
            (InteractionOutcome::Answered { answer }, None) => {
                self.validate_internal_ref(
                    answer,
                    id,
                    "answer",
                    Some(resolution.expected_version),
                    Some(operation_id),
                )?;
                let answer_grant = ContentGrant::verify(
                    &self.verifier,
                    &self.owner_scope,
                    answer.volume(),
                    VolumeOperation::Read,
                )?;
                let answer_bytes = self
                    .host
                    .read_internal_content(
                        answer,
                        answer.volume(),
                        &answer_grant,
                        InternalContentClass::Interaction,
                        self.maximum_bytes,
                    )
                    .await?;
                ticket.validate_answer_bytes(&request, answer, &answer_bytes)?;
            }
            (_, Some(detail)) => {
                self.validate_internal_ref(
                    detail,
                    id,
                    "answer",
                    Some(resolution.expected_version),
                    Some(operation_id),
                )?;
                let detail_grant = ContentGrant::verify(
                    &self.verifier,
                    &self.owner_scope,
                    detail.volume(),
                    VolumeOperation::Read,
                )?;
                let detail_bytes = self
                    .host
                    .read_internal_content(
                        detail,
                        detail.volume(),
                        &detail_grant,
                        InternalContentClass::Interaction,
                        self.maximum_bytes,
                    )
                    .await?;
                ticket.validate_decision_bytes(
                    &request,
                    &resolution.outcome,
                    detail,
                    &detail_bytes,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn aggregate(&self) -> Result<StreamAggregate<P>> {
        let verifier = InteractionContentVerifier {
            ordinary: FilesystemContentVerifier::new(
                Arc::clone(&self.host),
                self.verifier.clone(),
                self.owner_scope.clone(),
                self.maximum_bytes,
            )?,
            host: Arc::clone(&self.host),
            volume: self.private_volume.clone(),
            grant: ContentGrant::verify(
                &self.verifier,
                &self.owner_scope,
                &self.private_volume,
                VolumeOperation::Read,
            )?,
            maximum_bytes: self.maximum_bytes,
        };
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
    Uuid::parse_str(&id.to_string()).map_err(|error| Error::Invalid(error.to_string()))
}
