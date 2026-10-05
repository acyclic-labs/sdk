//! Stream-backed execution observations with private Filesystem payloads.

use super::{
    FilesystemHost, FilesystemInteractionHost, InternalContentClass, is_host_owned_internal_path,
};
use crate::{
    Error, IdempotencyKey, InteractionId, OperationId, Result, SessionId,
    conversation::{
        ContentGrant, ContentResidencyVerifier, FileRef, VolumeClass, VolumeOperation, VolumeRef,
    },
    core::{AuthorityVerifier, SchemaRegistry, Scope},
    executor::{ExecutionEvent, ExecutionJournal, ExecutionRecord, load_json},
    host_execution::{ExecutionApproval, ExecutionApprovalContext, ExecutionApprovalVerifier},
    interaction::{Interaction, InteractionOutcome, InteractionResolution, InteractionResponse},
    projection::{SelectedModelContext, select_model_context},
    store::StreamAggregate,
    tool::ToolApprovalVerifier,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};

const MAX_RECORDS: u64 = 1_000_000;

/// Returns the canonical host-only path for one idempotent execution payload.
pub(crate) fn execution_content_path(
    operation_id: OperationId,
    idempotency_key: &str,
) -> String {
    let digest = blake3::hash(format!("{operation_id}:{idempotency_key}").as_bytes());
    format!(".system/execution/{operation_id}/{}.json", digest.to_hex())
}

/// Hidden metadata linking a model-visible rejection to the authoritative
/// execution journal that produced it. The visible envelope is never trusted
/// as the source of evidence on a later turn.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RejectionJournalBinding {
    pub(crate) operation_id: OperationId,
    pub(crate) step: u32,
    pub(crate) call_id: String,
    pub(crate) message_id: uuid::Uuid,
    pub(crate) reply_to: uuid::Uuid,
    pub(crate) invocation_digest: [u8; 32],
}

pub(crate) const REJECTION_JOURNAL_BINDING: &str = "acyclic.model.rejection-journal";

/// The only inherited prefix a child journal may use for membership proof.
/// This is installed from a seed after the parent publication and allocation
/// claims have been verified; callers cannot select an arbitrary file ref.
#[derive(Clone, Debug)]
pub(crate) struct AuthenticatedInheritedPrefix {
    pub(crate) file: FileRef,
    pub(crate) parent: crate::core::Authority,
    pub(crate) parent_revision: u64,
    pub(crate) parent_agent: crate::AgentId,
    pub(crate) through_sequence: u64,
    pub(crate) attached_agents: Vec<crate::AgentId>,
}

pub(crate) fn authenticated_inherited_prefix(
    seed: &crate::fork::ForkSeed,
    parent_agent: crate::AgentId,
) -> Result<Option<AuthenticatedInheritedPrefix>> {
    seed.validate()?;
    if seed.inherited_through_sequence == 0 {
        if !seed.inherited_context.is_empty() {
            return Err(Error::Invalid(
                "fork has inherited files without an inherited prefix".into(),
            ));
        }
        return Ok(None);
    }
    let prefix = seed
        .inherited_context
        .first()
        .cloned()
        .ok_or_else(|| Error::Invalid("fork is missing its inherited prefix".into()))?;
    if prefix.volume() != &seed.child_private_volume
        || prefix.version() != hex::encode(seed.child_private_generation.as_resource().key())
        || prefix.path() != ".system/inherited-conversation/prefix.json"
        || prefix.display_name() != "inherited-conversation.json"
        || prefix.descriptor().media_type()
            != "application/vnd.acyclic.harness.inherited-conversation+json"
        || parent_agent == seed.child_agent
    {
        return Err(Error::Conflict(
            "fork inherited prefix is not bound to the published child generation".into(),
        ));
    }
    Ok(Some(AuthenticatedInheritedPrefix {
        file: prefix,
        parent: seed.parent.clone(),
        parent_revision: seed.captured_history_revision()?,
        parent_agent,
        through_sequence: seed.inherited_through_sequence,
        attached_agents: seed.attached_agents.clone(),
    }))
}

/// Loads rejection evidence for selected historical messages from the journal
/// records named by authenticated hidden conversation metadata. The message
/// and its call are checked against the same journal invocation, so callers
/// cannot pair a valid rejection with a different set of arguments.
pub(crate) async fn selected_rejection_evidence_from_journal(
    journal: &dyn ExecutionJournal,
    historical: &crate::conversation::ConversationState,
    selection: &crate::conversation::ModelContextSelection,
    limits: crate::conversation::Limits,
    inherited_prefix: Option<&AuthenticatedInheritedPrefix>,
) -> Result<Vec<crate::tool::ToolRejectionFeedback>> {
    let inherited_ids = if let Some(prefix_ref) = inherited_prefix {
        let bytes = journal.load(&prefix_ref.file).await?;
        if prefix_ref.file.path() != ".system/inherited-conversation/prefix.json"
            || prefix_ref.file.display_name() != "inherited-conversation.json"
            || prefix_ref.file.descriptor().media_type()
            != "application/vnd.acyclic.harness.inherited-conversation+json"
        {
            return Err(Error::Conflict("inherited conversation prefix has an invalid media type".into()));
        }
        let expected_descriptor = crate::conversation::FileDescriptor::from_bytes(
            &bytes,
            "application/vnd.acyclic.harness.inherited-conversation+json",
        )?;
        if prefix_ref.file.descriptor() != &expected_descriptor {
            return Err(Error::Conflict(
                "inherited conversation prefix descriptor is not content bound".into(),
            ));
        }
        let prefix: crate::fork::InheritedConversationPrefix = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Storage(format!("inherited conversation prefix is invalid: {error}")))?;
        if prefix.canonical_bytes()? != bytes {
            return Err(Error::Conflict("inherited conversation prefix is not canonical".into()));
        }
        if prefix.through_sequence != prefix.messages.len() as u64 {
            return Err(Error::Conflict("inherited conversation prefix sequence is invalid".into()));
        }
        if prefix.parent != prefix_ref.parent
            || prefix.parent_revision != prefix_ref.parent_revision
            || prefix.parent_agent != prefix_ref.parent_agent
            || prefix.through_sequence != prefix_ref.through_sequence
            || prefix.attached_agents != prefix_ref.attached_agents
        {
            return Err(Error::Conflict(
                "inherited conversation prefix differs from the published fork boundary".into(),
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        for (index, inherited) in prefix.messages.iter().enumerate() {
            inherited.validate()?;
            if inherited.sequence != index as u64 + 1
                || !ids.insert(inherited.id)
                || historical.messages.iter().find(|message| message.id == inherited.id)
                    .is_some_and(|message| message != inherited)
            {
                return Err(Error::Conflict("historical message differs from frozen inherited prefix".into()));
            }
        }
        Some(ids)
    } else {
        None
    };
    let mut evidence = Vec::new();
    for message_id in &selection.message_ids {
        let Some(message) = historical.messages.iter().find(|message| &message.id == message_id)
        else {
            return Err(Error::Conflict("selected rejection message is absent".into()));
        };
        let Some(binding_ref) = message.extensions.get(REJECTION_JOURNAL_BINDING) else {
            continue;
        };
        if message.kind != crate::conversation::MessageKind::ToolResult {
            return Err(Error::Conflict("rejection binding is not attached to a tool result".into()));
        }
        // A recursive child carries the parent's hidden binding refs in its
        // authenticated frozen prefix. Its immutable rejection evidence is
        // supplied by the inherited boundary; only child-owned suffix refs
        // are resolved against this journal.
        if inherited_ids.as_ref().is_some_and(|ids| ids.contains(&message.id)) {
            continue;
        }
        let binding_bytes = journal.load(binding_ref).await?;
        let binding: RejectionJournalBinding = serde_json::from_slice(&binding_bytes)
            .map_err(|error| Error::Storage(format!("rejection journal binding is invalid: {error}")))?;
        if binding.message_id != message.id {
            return Err(Error::Conflict("rejection binding message identity differs from history".into()));
        }
        if message.tool_call_id.as_deref() != Some(binding.call_id.as_str()) {
            return Err(Error::Conflict("rejection binding call identity differs from conversation result".into()));
        }
        let call_id = message.reply_to.ok_or_else(|| {
            Error::Conflict("rejection result has no tool-call reply target".into())
        })?;
        let call = historical.messages.iter().find(|candidate| candidate.id == call_id)
            .ok_or_else(|| Error::Conflict("rejection result reply target is absent".into()))?;
        if call.kind != crate::conversation::MessageKind::ToolCall
            || call.tool_call_id.as_deref() != Some(binding.call_id.as_str())
            || call.id != binding.reply_to
        {
            return Err(Error::Conflict("rejection result reply target is not its tool call".into()));
        }
        let records = journal.replay(binding.operation_id).await?;
        let message_invocation: crate::tool::ToolInvocation = serde_json::from_slice(
            &journal.load(&call.content).await?,
        )
        .map_err(|error| Error::Storage(format!("tool-call invocation is invalid: {error}")))?;
        if message_invocation.call_id != binding.call_id {
            return Err(Error::Conflict("tool-call content identity differs from binding".into()));
        }
        if crate::contract::canonical_json_digest(&message_invocation)? != binding.invocation_digest {
            return Err(Error::Conflict("rejection binding invocation digest differs from call".into()));
        }
        let mut found = None;
        for record in records {
            let crate::executor::ExecutionEvent::ToolAdmissionRejected {
                step,
                invocation,
                reason: crate::executor::ToolRejectionKind::InvalidArguments,
                feedback: Some(feedback),
            } = record.event
            else {
                continue;
            };
            if step != binding.step {
                continue;
            }
            let invocation: crate::tool::ToolInvocation = load_json(journal, &invocation).await?;
            if invocation.call_id != binding.call_id {
                continue;
            }
            if invocation != message_invocation {
                return Err(Error::Conflict("tool-call content differs from authoritative invocation".into()));
            }
            if found.is_some() {
                return Err(Error::Storage("rejection journal binding is duplicated".into()));
            }
            let feedback: crate::tool::ToolRejectionFeedback = load_json(journal, &feedback).await?;
            if feedback.call_id != invocation.call_id || feedback.name != invocation.name {
                return Err(Error::Conflict("rejection journal evidence changed identity".into()));
            }
            found = Some(feedback);
        }
        let feedback = found.ok_or_else(|| {
            Error::Conflict("rejection journal binding has no authoritative record".into())
        })?;
        evidence.push(feedback);
    }
    if evidence.len() > limits.context_messages {
        return Err(Error::Invalid("rejection evidence exceeds context limit".into()));
    }
    Ok(evidence)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    operation_id: OperationId,
    retry_digest: String,
    event: ExecutionEvent,
}

async fn verify_selected_rejection_evidence(
    journal: &dyn ExecutionJournal,
    historical: &crate::conversation::ConversationState,
    selected: &SelectedModelContext,
    inherited_prefix: Option<&AuthenticatedInheritedPrefix>,
) -> Result<()> {
    let authoritative = selected_rejection_evidence_from_journal(
        journal,
        historical,
        &selected.selection,
        crate::conversation::Limits {
            context_messages: selected.selection.message_ids.len(),
            ..crate::conversation::Limits::default()
        },
        inherited_prefix,
    )
    .await?;
    if authoritative != selected.rejection_evidence {
        return Err(Error::Conflict(
            "selected rejection evidence differs from authoritative journal".into(),
        ));
    }
    Ok(())
}

/// Durable, ref-only journal for one exact agent-private volume.
pub struct FilesystemExecutionJournal<P, A, O> {
    stream: StreamClient<P>,
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    schemas: SchemaRegistry,
    maximum_payload_bytes: u64,
    input_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    interactions: FilesystemInteractionHost<P, A, O>,
    /// Exact child-owned materialization proving which conversation messages
    /// belong to the frozen inherited prefix.
    inherited_prefix: Option<AuthenticatedInheritedPrefix>,
    /// Session identity authenticated by the composition that owns this
    /// journal.  A journal without this binding cannot authorize execution.
    session_id: Option<SessionId>,
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O> {
    /// Binds an exact private volume and authenticated read/write grant.
    pub fn new(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self>
    where
        P: StreamProvider + Send + Sync,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        Self::new_with_schemas(
            stream,
            host,
            volume,
            verifier,
            SchemaRegistry::new(),
            scope,
            maximum_payload_bytes,
        )
    }

    /// Binds the exact pinned extension registry used by the conversation reducer.
    pub fn new_with_schemas(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self>
    where
        P: StreamProvider + Send + Sync,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_payload_bytes == 0
        {
            return Err(Error::Invalid(
                "execution journal requires a private volume and positive limit".into(),
            ));
        }
        ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Read)?;
        ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write)?;
        let interactions = FilesystemInteractionHost::new(
            stream.clone(),
            Arc::clone(&host),
            verifier.audience().clone(),
            verifier.clone(),
            schemas.clone(),
            scope.clone(),
            volume.clone(),
            maximum_payload_bytes,
        )?;
        Ok(Self {
            stream,
            host,
            volume,
            verifier,
            scope,
            schemas,
            maximum_payload_bytes,
            input_verifier: None,
            interactions,
            inherited_prefix: None,
            session_id: None,
        })
    }

    /// Binds the journal to the authenticated session that owns its
    /// interaction ledger.  Production execution must use this binding;
    /// leaving it unset makes every execution approval fail closed.
    pub fn with_session_id(mut self, session_id: SessionId) -> Result<Self> {
        if session_id.into_bytes() == [0; 16] {
            return Err(Error::Invalid(
                "execution journal session identity cannot be zero".into(),
            ));
        }
        self.session_id = Some(session_id);
        Ok(self)
    }

    /// Routes turn-input attachment admission through an explicitly bound provider set.
    #[must_use]
    pub fn with_input_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.input_verifier = Some(verifier);
        self
    }

    pub(crate) fn with_authenticated_inherited_prefix(
        mut self,
        prefix: Option<AuthenticatedInheritedPrefix>,
    ) -> Self {
        self.inherited_prefix = prefix;
        self
    }

    /// Binds the authenticated child-owned inherited conversation materialization.
    fn path(&self, operation_id: OperationId) -> Result<acyclic_stream::Stream<P>>
    where
        P: StreamProvider,
    {
        self.stream
            .stream(format!("harness/v2/execution/{operation_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Resolves through the conversation-owned ledger with an exact responder grant.
    #[allow(
        clippy::too_many_lines,
        reason = "one ordered interaction resolution transaction"
    )]
    pub async fn resolve_interaction(
        &self,
        id: InteractionId,
        response: InteractionResponse,
        responder: &Scope,
    ) -> Result<()>
    where
        P: StreamProvider + Send + Sync,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        self.verifier.verify(responder)?;
        if !responder.capabilities().contains("interaction:resolve")
            || !responder
                .capabilities()
                .contains(&format!("interaction:respond:{id}"))
        {
            return Err(Error::Unauthorized(
                "responder lacks the exact interaction grant".into(),
            ));
        }
        let Some((ticket, prior)) = self.interactions.read(id).await? else {
            return Err(Error::NotFound(format!("interaction {id}")));
        };
        let request = self
            .interactions
            .read_request(id)
            .await?
            .ok_or_else(|| Error::Storage("admitted interaction has no request".into()))?;
        request.validate_response(&response)?;
        if let Some(previous) = &prior
            && previous.outcome.is_terminal()
        {
            let same = match (&previous.outcome, &response) {
                (
                    InteractionOutcome::Approved,
                    InteractionResponse::Approval { approved: true, .. },
                )
                | (
                    InteractionOutcome::Declined,
                    InteractionResponse::Approval {
                        approved: false, ..
                    },
                ) => {
                    let bytes = self
                        .interactions
                        .read_decision_detail(id)
                        .await?
                        .ok_or_else(|| {
                            Error::Storage("admitted approval detail is missing".into())
                        })?;
                    let original: InteractionResponse = serde_json::from_slice(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    original == response
                }
                (InteractionOutcome::Answered { .. }, _) => {
                    let bytes = self
                        .interactions
                        .read_answer(id)
                        .await?
                        .ok_or_else(|| Error::Storage("admitted answer is missing".into()))?;
                    let original: InteractionResponse = serde_json::from_slice(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))?;
                    original == response
                }
                _ => false,
            };
            return if same {
                Ok(())
            } else {
                Err(Error::Conflict("interaction was already resolved".into()))
            };
        }
        let expected_version = prior.map_or(Ok(1_u64), |value| {
            value
                .expected_version
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("interaction version exhausted".into()))
        })?;
        if let InteractionResponse::Approval { approved, reason } = &response {
            let approval_operation = ticket
                .approval
                .as_ref()
                .ok_or_else(|| Error::Invalid("approval ticket binding is missing".into()))?
                .operation_id;
            self.interactions
                .resolve_approval(
                    approval_operation,
                    responder.clone(),
                    id,
                    expected_version,
                    *approved,
                    reason.clone(),
                )
                .await?;
            return Ok(());
        }
        let resolution_operation =
            interaction_operation(id, &format!("resolve-{expected_version}"));
        let outcome = InteractionOutcome::Answered {
            answer: Box::new(
                self.interactions
                    .stage_answer(id, expected_version, resolution_operation, &response)
                    .await?,
            ),
        };
        self.interactions
            .resolve(
                resolution_operation,
                responder.clone(),
                InteractionResolution {
                    id: ticket.id,
                    expected_version,
                    outcome,
                    detail: None,
                },
            )
            .await
            .map(|_| ())
    }

    async fn verify_event_refs(
        &self,
        operation_id: OperationId,
        event: &ExecutionEvent,
    ) -> Result<()>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let refs: Vec<&FileRef> = match event {
            ExecutionEvent::ModelInputPrepared {
                manifest, request, ..
            } => vec![manifest, request],
            ExecutionEvent::ToolBatchCompleted { boundary, .. } => vec![boundary],
            ExecutionEvent::BatchPublicationStarted { publication, .. } => vec![publication],
            ExecutionEvent::Model { event, .. } => vec![event],
            ExecutionEvent::ToolStarted { invocation, .. } => vec![invocation],
            ExecutionEvent::ToolAdmissionRejected {
                invocation,
                feedback,
                ..
            } => {
                let mut refs = vec![invocation];
                if let Some(feedback) = feedback {
                    refs.push(feedback);
                }
                refs
            }
            ExecutionEvent::ToolCompleted {
                result, projection, ..
            } => vec![result, projection],
            ExecutionEvent::Started { .. }
            | ExecutionEvent::ModelStarted { .. }
            | ExecutionEvent::ToolFailed { .. }
            | ExecutionEvent::BatchPublicationCompleted { .. } => Vec::new(),
        };
        for reference in refs {
            if reference.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "journal event refers to another private volume".into(),
                ));
            }
            let operation_prefix = format!(".system/execution/{operation_id}/");
            if !reference.path().starts_with(&operation_prefix) {
                return Err(Error::Unauthorized(
                    "journal event refers to another operation's content".into(),
                ));
            }
            let grant = ContentGrant::verify(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
            )?;
            self.host
                .read_content(reference, &grant, self.maximum_payload_bytes)
                .await?;
        }
        Ok(())
    }
}

impl<P, A, O> ToolApprovalVerifier for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn verify<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let Some((ticket, Some(resolution))) = self.interactions.read(id).await? else {
                return Err(Error::Unauthorized(
                    "tool installation lacks a resolved approval".into(),
                ));
            };
            if !matches!(&resolution.outcome, InteractionOutcome::Approved)
                || !ticket.approval.as_ref().is_some_and(|binding| {
                    binding.operation_id == operation && binding.action_digest == digest
                })
            {
                return Err(Error::Unauthorized(
                    "approval does not authorize this tool definition".into(),
                ));
            }
            let request =
                self.interactions.read_request(id).await?.ok_or_else(|| {
                    Error::Storage("approved interaction request is missing".into())
                })?;
            if let Some(detail) = &resolution.detail {
                let bytes = self
                    .interactions
                    .read_decision_detail(id)
                    .await?
                    .ok_or_else(|| {
                        Error::Storage("approved interaction decision is missing".into())
                    })?;
                ticket.validate_decision_bytes(&request, &resolution.outcome, detail, &bytes)?;
            }
            Ok(())
        })
    }
}

impl<P, A, O> ExecutionApprovalVerifier for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn verify<'a>(
        &'a self,
        context: ExecutionApprovalContext<'a>,
        approval: &'a ExecutionApproval,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if context.session_id.into_bytes() == [0; 16]
                || context.interaction_id.into_bytes() == [0; 16]
                || context.session_id != approval.session_id
                || context.interaction_id != approval.interaction_id
                || context.operation_id != approval.operation_id
                || self.session_id != Some(context.session_id)
                || approval.request_locator_digest != Some(context.request_locator_digest)
            {
                return Err(Error::Unauthorized(
                    "execution approval is not bound to the authenticated owner session".into(),
                ));
            }
            let Some((ticket, Some(resolution))) =
                self.interactions.read(approval.interaction_id).await?
            else {
                return Err(Error::Unauthorized(
                    "execution approval lacks a resolved owner interaction".into(),
                ));
            };
            let expected_outcome = if approval.approved {
                InteractionOutcome::Approved
            } else {
                InteractionOutcome::Declined
            };
            if resolution.outcome != expected_outcome
                || !ticket.approval.as_ref().is_some_and(|binding| {
                    binding.operation_id == approval.operation_id
                        && binding.action_digest == context.request_digest
                })
            {
                return Err(Error::Unauthorized(
                    "owner interaction does not authorize this exact execution request".into(),
                ));
            }
            let request = self
                .interactions
                .read_request(approval.interaction_id)
                .await?
                .ok_or_else(|| Error::Storage("approved interaction request is missing".into()))?;
            if let Some(detail) = &resolution.detail {
                let bytes = self
                    .interactions
                    .read_decision_detail(approval.interaction_id)
                    .await?
                    .ok_or_else(|| {
                        Error::Storage("approved interaction decision is missing".into())
                    })?;
                ticket.validate_decision_bytes(&request, &resolution.outcome, detail, &bytes)?;
            }
            Ok(())
        })
    }
}

impl<P, A, O> ExecutionJournal for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>> {
        Box::pin(async move {
            let mut result = Vec::new();
            let mut keys = HashSet::new();
            let mut replay = self.path(operation_id)?.replay(0);
            while let Some(page) = replay.next_page().await? {
                for record in page {
                    if record.sequence >= MAX_RECORDS {
                        return Err(Error::Storage(
                            "execution journal sequence or bound is invalid".into(),
                        ));
                    }
                    let observation: Observation =
                        serde_json::from_slice(&record.value).map_err(|error| {
                            Error::Storage(format!("execution journal record is invalid: {error}"))
                        })?;
                    observation.event.validate_schema_version()?;
                    if observation.operation_id != operation_id
                        || !keys.insert(observation.retry_digest.clone())
                    {
                        return Err(Error::Storage(
                            "execution journal identity is invalid".into(),
                        ));
                    }
                    self.verify_event_refs(operation_id, &observation.event)
                        .await?;
                    result.push(ExecutionRecord {
                        operation_id,
                        sequence: record.sequence + 1,
                        idempotency_key: observation.retry_digest,
                        event: observation.event,
                    });
                }
            }
            Ok(result)
        })
    }

    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if idempotency_key.is_empty() || idempotency_key.len() > 256 {
                return Err(Error::Invalid(
                    "execution journal idempotency key is invalid".into(),
                ));
            }
            event.validate_schema_version()?;
            self.verify_event_refs(operation_id, &event).await?;
            let digest = blake3::hash(format!("{operation_id}:{idempotency_key}").as_bytes());
            let bytes = serde_json::to_vec(&Observation {
                operation_id,
                retry_digest: digest.to_hex().to_string(),
                event,
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "execution journal observation exceeds Stream limit".into(),
                ));
            }
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match self
                .path(operation_id)?
                .append_batch(vec![Bytes::from(bytes)], None, Some(key))
                .await
            {
                Ok(AppendOutcome::Committed(_)) => Ok(()),
                Ok(AppendOutcome::TailConflict { .. }) => {
                    Err(Error::Conflict("execution journal tail changed".into()))
                }
                Err(StreamError::IdempotencyMismatch) => Err(Error::Conflict(
                    "execution journal retry identity reused".into(),
                )),
                Err(error) => Err(Error::Storage(error.to_string())),
            }
        })
    }

    fn append_if_tail<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        claim_id: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async move {
            if claim_id.is_empty() || claim_id.len() > 256 || expected_tail >= MAX_RECORDS {
                return Err(Error::Invalid(
                    "execution journal compare-and-append is invalid".into(),
                ));
            }
            event.validate_schema_version()?;
            self.verify_event_refs(operation_id, &event).await?;
            let digest = blake3::hash(format!("{operation_id}:{claim_id}").as_bytes());
            let retry_digest = digest.to_hex().to_string();
            let bytes = serde_json::to_vec(&Observation {
                operation_id,
                retry_digest: retry_digest.clone(),
                event: event.clone(),
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "execution journal observation exceeds Stream limit".into(),
                ));
            }
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match self
                .path(operation_id)?
                .append_batch(vec![Bytes::from(bytes)], Some(expected_tail), Some(key))
                .await
            {
                Ok(AppendOutcome::Committed(receipt)) if receipt.start == expected_tail => Ok(true),
                Ok(AppendOutcome::Committed(_)) | Ok(AppendOutcome::TailConflict { .. }) => {
                    Ok(false)
                }
                Err(StreamError::IdempotencyMismatch) => Err(Error::Conflict(
                    "execution journal compare-and-append identity was reused".into(),
                )),
                Err(_) => {
                    let records = self
                        .replay(operation_id)
                        .await
                        .map_err(|_| Error::Indeterminate(operation_id))?;
                    if records.iter().any(|record| {
                        record.idempotency_key == retry_digest && record.event == event
                    }) {
                        Ok(true)
                    } else {
                        Err(Error::Indeterminate(operation_id))
                    }
                }
            }
        })
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            if bytes.len() as u64 > self.maximum_payload_bytes {
                return Err(Error::Invalid(
                    "execution journal payload exceeds limit".into(),
                ));
            }
            let digest = blake3::hash(format!("{operation_id}:{idempotency_key}").as_bytes());
            let path = execution_content_path(operation_id, &idempotency_key);
            let key = IdempotencyKey::new(format!("journal-{}", digest.to_hex()))?;
            let grant = ContentGrant::verify(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Write,
            )?;
            self.host
                .put_internal_content(
                    &self.volume,
                    &grant,
                    &path,
                    &bytes,
                    media_type,
                    "execution.json",
                    self.maximum_payload_bytes,
                    &key,
                    InternalContentClass::Execution,
                )
                .await
        })
    }

    fn load<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            if reference.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "journal content belongs to another private volume".into(),
                ));
            }
            let grant = ContentGrant::verify(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
            )?;
            let bytes = if reference.path().starts_with(".system/execution/") {
                self.host
                    .read_internal_content(
                        reference,
                        &self.volume,
                        &grant,
                        InternalContentClass::Execution,
                        self.maximum_payload_bytes,
                    )
                    .await?
            } else if reference.path().starts_with(".system/interactions/") {
                self.host
                    .read_internal_content(
                        reference,
                        &self.volume,
                        &grant,
                        InternalContentClass::Interaction,
                        self.maximum_payload_bytes,
                    )
                    .await?
            } else if reference.path().starts_with(".system/workflows/") {
                self.host
                    .read_internal_content(
                        reference,
                        &self.volume,
                        &grant,
                        InternalContentClass::Workflow,
                        self.maximum_payload_bytes,
                    )
                    .await?
            } else {
                self.host
                    .read_content(reference, &grant, self.maximum_payload_bytes)
                    .await?
            };
            Ok(bytes.to_vec())
        })
    }

    fn verify_input_file<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if reference.descriptor().byte_length() > self.maximum_payload_bytes {
                return Err(Error::Invalid(
                    "turn input file exceeds journal limit".into(),
                ));
            }
            if reference.volume().provider() != &self.host.provider {
                if let Some(verifier) = &self.input_verifier {
                    return verifier.verify(reference).await;
                }
                return Err(Error::Unsupported(
                    "turn input file belongs to another content provider".into(),
                ));
            }
            if is_host_owned_internal_path(reference.path()) {
                return Err(Error::Unauthorized(
                    "host-owned internal content cannot enter model context".into(),
                ));
            }
            let grant = if reference.volume().class() == VolumeClass::AgentPrivate
                && reference.volume().owner() != self.volume.owner()
            {
                ContentGrant::verify_file_read(&self.verifier, &self.scope, reference)?
            } else {
                ContentGrant::verify(
                    &self.verifier,
                    &self.scope,
                    reference.volume(),
                    VolumeOperation::Read,
                )?
            };
            self.host
                .read_content(reference, &grant, self.maximum_payload_bytes)
                .await?;
            Ok(())
        })
    }

    fn verify_selected_context<'a>(
        &'a self,
        operation_id: OperationId,
        selected: &'a SelectedModelContext,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let verifier = self.input_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("conversation content verifier is not bound".into())
            })?;
            let aggregate = StreamAggregate::open(
                &self.stream,
                self.verifier.audience().clone(),
                self.verifier.clone(),
                self.schemas.clone(),
            )
            .await?;
            let committed = aggregate
                .reducer()
                .context_selection_for_operation(operation_id)
                .ok_or_else(|| {
                    Error::Conflict("turn has no committed model-context selection".into())
                })?;
            if committed != &selected.selection {
                return Err(Error::Conflict(
                    "model-context selection does not match the committed turn".into(),
                ));
            }
            let mut historical = aggregate
                .reducer()
                .conversation()
                .ok_or_else(|| Error::Invalid("journal authority is not a conversation".into()))?
                .clone();
            let length = usize::try_from(committed.conversation_revision)
                .map_err(|_| Error::Storage("selection revision exceeds platform size".into()))?;
            if length > historical.messages.len() {
                return Err(Error::Storage(
                    "selection revision exceeds conversation history".into(),
                ));
            }
            historical.messages.truncate(length);
            let mut projected = select_model_context(
                &historical,
                committed.clone(),
                verifier.as_ref(),
                1_000_000,
                65_536,
                self.maximum_payload_bytes,
            )
            .await?;
            projected.rejection_evidence = selected.rejection_evidence.clone();
            if &projected != selected {
                return Err(Error::Conflict(
                    "model context differs from committed conversation projection".into(),
                ));
            }
            verify_selected_rejection_evidence(
                self,
                &historical,
                selected,
                self.inherited_prefix.as_ref(),
            )
            .await?;
            Ok(())
        })
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            interaction.validate()?;
            if let Some(original) = self.interactions.read_request(id).await? {
                return if original == interaction {
                    Ok(())
                } else {
                    Err(Error::Conflict("interaction identity reused".into()))
                };
            }
            let ticket = self
                .interactions
                .stage_request(id, &interaction, None)
                .await?;
            match self
                .interactions
                .open(
                    interaction_operation(id, "open"),
                    self.scope.clone(),
                    ticket,
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(Error::Conflict(_)) | Err(Error::Indeterminate(_)) => {
                    match self.interactions.read_request(id).await? {
                        Some(original) if original == interaction => Ok(()),
                        Some(_) => Err(Error::Conflict("interaction identity reused".into())),
                        None => Err(Error::Indeterminate(interaction_operation(id, "open"))),
                    }
                }
                Err(error) => Err(error),
            }
        })
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        Box::pin(async move {
            Ok(self
                .interactions
                .read(id)
                .await?
                .and_then(|(_, resolution)| resolution.map(|value| value.outcome)))
        })
    }
}

fn interaction_operation(id: InteractionId, phase: &str) -> OperationId {
    let digest = blake3::hash(format!("interaction:{id}:{phase}").as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
