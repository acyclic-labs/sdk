//! Stream-backed execution observations with private Filesystem payloads.

use super::{FilesystemHost, FilesystemInteractionHost, InternalContentClass};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_harness::{
    Error, IdempotencyKey, InteractionId, OperationId, Result,
    conversation::{
        ContentGrant, ContentResidencyVerifier, FileRef, VolumeClass, VolumeOperation, VolumeRef,
    },
    core::{AuthorityVerifier, SchemaRegistry, Scope},
    executor::{ExecutionEvent, ExecutionJournal, ExecutionRecord},
    interaction::{Interaction, InteractionOutcome, InteractionResolution, InteractionResponse},
    projection::{SelectedModelContext, select_model_context},
    store::StreamAggregate,
    tool::ToolApprovalVerifier,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::{TryStreamExt as _, future::BoxFuture};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};

const PAGE_SIZE: u32 = 1_024;
const MAX_RECORDS: u64 = 1_000_000;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    operation_id: OperationId,
    retry_digest: String,
    event: ExecutionEvent,
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
        })
    }

    /// Routes turn-input attachment admission through an explicitly bound provider set.
    #[must_use]
    pub fn with_input_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.input_verifier = Some(verifier);
        self
    }

    fn path(&self, operation_id: OperationId) -> Result<acyclic_stream::Stream<P>>
    where
        P: StreamProvider,
    {
        self.stream
            .stream(format!("harness/v2/execution/{operation_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Resolves through the conversation-owned ledger with an exact responder grant.
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
        if let Some(previous) = &prior {
            if previous.outcome.is_terminal() {
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
                        let bytes =
                            self.interactions.read_answer(id).await?.ok_or_else(|| {
                                Error::Storage("admitted answer is missing".into())
                            })?;
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
        }
        let expected_version = prior.map_or(Ok(1_u64), |value| {
            value
                .expected_version
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("interaction version exhausted".into()))
        })?;
        let detail = if matches!(&response, InteractionResponse::Approval { .. }) {
            Some(
                self.interactions
                    .stage_answer(id, expected_version, &response)
                    .await?,
            )
        } else {
            None
        };
        let outcome = match response {
            InteractionResponse::Approval { approved: true, .. } => InteractionOutcome::Approved,
            InteractionResponse::Approval {
                approved: false, ..
            } => InteractionOutcome::Declined,
            other => InteractionOutcome::Answered {
                answer: Box::new(
                    self.interactions
                        .stage_answer(id, expected_version, &other)
                        .await?,
                ),
            },
        };
        self.interactions
            .resolve(
                interaction_operation(id, &format!("resolve-{expected_version}")),
                responder.clone(),
                InteractionResolution {
                    id: ticket.id,
                    expected_version,
                    outcome,
                    detail,
                },
            )
            .await
            .map(|_| ())
    }

    async fn verify_event_refs(&self, event: &ExecutionEvent) -> Result<()>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let refs: Vec<&FileRef> = match event {
            ExecutionEvent::Model { event, .. } => vec![event],
            ExecutionEvent::ToolStarted { invocation, .. } => vec![invocation],
            ExecutionEvent::ToolCompleted {
                result, projection, ..
            } => vec![result, projection],
            ExecutionEvent::Started { .. }
            | ExecutionEvent::ModelStarted { .. }
            | ExecutionEvent::ToolFailed { .. } => Vec::new(),
        };
        for reference in refs {
            if reference.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "journal event refers to another private volume".into(),
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
            let stream = self.path(operation_id)?;
            let mut from = 0_u64;
            let mut result = Vec::new();
            let mut keys = HashSet::new();
            loop {
                let records = match stream.read(from, PAGE_SIZE).await {
                    Ok(records) => records,
                    Err(StreamError::NotFound) if from == 0 => break,
                    Err(error) => return Err(Error::Storage(error.to_string())),
                };
                let page = records
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if page.is_empty() {
                    break;
                }
                for record in page {
                    if record.sequence != from || from >= MAX_RECORDS {
                        return Err(Error::Storage(
                            "execution journal sequence or bound is invalid".into(),
                        ));
                    }
                    let observation: Observation =
                        serde_json::from_slice(&record.value).map_err(|error| {
                            Error::Storage(format!("execution journal record is invalid: {error}"))
                        })?;
                    if observation.operation_id != operation_id
                        || !keys.insert(observation.retry_digest.clone())
                    {
                        return Err(Error::Storage(
                            "execution journal identity is invalid".into(),
                        ));
                    }
                    self.verify_event_refs(&observation.event).await?;
                    result.push(ExecutionRecord {
                        operation_id,
                        sequence: from + 1,
                        idempotency_key: observation.retry_digest,
                        event: observation.event,
                    });
                    from += 1;
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
            self.verify_event_refs(&event).await?;
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
            self.verify_event_refs(&event).await?;
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
                    &format!(".system/execution/{operation_id}/{}.json", digest.to_hex()),
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
            Ok(self
                .host
                .read_content(reference, &grant, self.maximum_payload_bytes)
                .await?
                .to_vec())
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
            let projected = select_model_context(
                &historical,
                committed.clone(),
                verifier.as_ref(),
                1_000_000,
                65_536,
                self.maximum_payload_bytes,
            )
            .await?;
            if &projected != selected {
                return Err(Error::Conflict(
                    "model context differs from committed conversation projection".into(),
                ));
            }
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
