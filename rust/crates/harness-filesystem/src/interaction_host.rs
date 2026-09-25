//! One Filesystem/Stream bridge for the conversation-owned interaction ledger.

use super::{FilesystemContentVerifier, FilesystemHost, InternalContentClass};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_harness::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, ApplyResult, Authority, AuthorityVerifier, Command, SchemaRegistry, Scope},
    interaction::{
        ApprovalBinding, Interaction, InteractionOutcome, InteractionResolution, InteractionTicket,
    },
    store::StreamAggregate,
};
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

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Binds the exact conversation authority and an agent-private content owner.
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
        if authority.kind != acyclic_harness::core::AggregateKind::Conversation
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
        response: &acyclic_harness::interaction::InteractionResponse,
    ) -> Result<FileRef> {
        if expected_version == 0 {
            return Err(Error::Invalid(
                "interaction answer version must be positive".into(),
            ));
        }
        self.stage(id, &format!("answer-{expected_version}"), response)
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
        let command = Command {
            operation_id,
            idempotency_key: IdempotencyKey::new(format!("interaction:{operation_id}"))?,
            expected_revision: aggregate.reducer().revision(),
            scope,
            causal_parent: None,
            action,
        };
        aggregate.execute(command).await
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
    Uuid::parse_str(&id.to_string()).map_err(|error| Error::Invalid(error.to_string()))
}
