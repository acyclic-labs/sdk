//! Infrastructure-free Harness storage with the same Stream/Filesystem contracts.

use crate::{FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost};
use acyclic_fs::{
    Fs, MemoryAuthorityBackend, MemoryObjectBackend, WorkspaceDirectoryPage, kernel::LogicalName,
};
use acyclic_harness::{
    AgentId, Capabilities, ConversationId, Error, IdempotencyKey, InteractionId, OperationId,
    Result,
    conversation::{
        Attachment, ContentGrant, ContentPublisher, ContentResidencyVerifier, ConversationMessage,
        FileRef, Limits, MessageKind, ModelContextSelection, ReferencedAttachments, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry, Scope},
    executor::{ExecutionEvent, ExecutionJournal, TurnInput, TurnOutput},
    interaction::{InteractionOutcome, InteractionResponse},
    projection::select_model_context,
    resources::{GenerationRef, ProviderRef},
    runtime::ContentBindings,
    store::StreamAggregate,
};
use acyclic_stream::{MemoryStream, StreamClient};
use futures::future::BoxFuture;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

type MemoryHost = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;
type MemoryJournal =
    FilesystemExecutionJournal<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

/// A fully bound in-process journal and owner-private file volume. Its data is
/// deliberately ephemeral; durable deployments bind persistent providers.
pub struct MemoryHarnessStorage {
    journal: Arc<MemoryJournal>,
    content_verifier: Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>,
    publisher: Arc<MemoryContentPublisher>,
    host: Arc<MemoryHost>,
    volume: VolumeRef,
    read_capability: String,
    write_capability: String,
    write: ContentGrant,
    scope: Scope,
    issuer: AuthorityIssuer,
    stream: StreamClient<MemoryStream>,
    conversation: Authority,
    maximum_file_bytes: u64,
}

struct MemoryContentPublisher {
    host: Arc<MemoryHost>,
    volume: VolumeRef,
    write: ContentGrant,
    maximum_file_bytes: u64,
}

impl ContentPublisher for MemoryContentPublisher {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            let path_digest = blake3::hash(path.as_bytes());
            let retry = IdempotencyKey::new(format!(
                "local-upload:{operation_id}:{}",
                path_digest.to_hex()
            ))?;
            self.host
                .put_content(
                    &self.volume,
                    &self.write,
                    path,
                    bytes,
                    media_type,
                    display_name,
                    self.maximum_file_bytes,
                    &retry,
                )
                .await
        })
    }
}

impl MemoryHarnessStorage {
    /// Creates an agent-owned private volume and a bound conversation before
    /// any turn or interaction can publish a reference.
    pub async fn new(agent: AgentId, maximum_file_bytes: u64) -> Result<Self> {
        if maximum_file_bytes == 0 {
            return Err(Error::Invalid(
                "memory Harness file limit must be positive".into(),
            ));
        }
        let provider = ProviderRef::new("local", "filesystem", "2")?;
        let host: Arc<MemoryHost> = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
        let volume = VolumeRef::new(
            provider,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        host.create_volume(&volume).await?;
        let conversation = Authority {
            kind: AggregateKind::Conversation,
            id: ConversationId::new().to_string(),
        };
        let key = *blake3::hash(&OperationId::new().into_bytes()).as_bytes();
        let issuer = AuthorityIssuer::new("local-harness", key, conversation.clone());
        let read_capability = volume.capability(VolumeOperation::Read)?;
        let write_capability = volume.capability(VolumeOperation::Write)?;
        let scope = issuer.root_for_agent(
            agent,
            "owner",
            Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "conversation:select_context".to_owned(),
                "interaction:open".to_owned(),
                read_capability.clone(),
                write_capability.clone(),
            ]),
        );
        let write =
            ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut aggregate = StreamAggregate::open(
            &stream,
            conversation.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?;
        aggregate
            .execute(Command {
                operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new("local-conversation-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            })
            .await?;
        let input = Arc::new(FilesystemContentVerifier::new(
            Arc::clone(&host),
            issuer.verifier(),
            scope.clone(),
            maximum_file_bytes,
        )?);
        let journal = Arc::new(
            FilesystemExecutionJournal::new(
                stream.clone(),
                Arc::clone(&host),
                volume.clone(),
                issuer.verifier(),
                scope.clone(),
                maximum_file_bytes,
            )?
            .with_input_verifier(input.clone()),
        );
        let publisher = Arc::new(MemoryContentPublisher {
            host: Arc::clone(&host),
            volume: volume.clone(),
            write: write.clone(),
            maximum_file_bytes,
        });
        Ok(Self {
            journal,
            content_verifier: input,
            publisher,
            host,
            volume,
            read_capability,
            write_capability,
            write,
            scope,
            issuer,
            stream,
            conversation,
            maximum_file_bytes,
        })
    }

    /// Supplies the exact ref-only journal required by `HarnessBuilder`.
    #[must_use]
    pub fn journal(&self) -> Arc<dyn ExecutionJournal> {
        self.journal.clone()
    }

    /// Starts a runnable local composition with this owner-controlled journal.
    #[must_use]
    pub fn builder(&self) -> acyclic_harness::bundle::HarnessBuilder {
        acyclic_harness::bundle::HarnessBuilder::new()
            .journal(self.journal())
            .content(ContentBindings {
                reader: self.content_verifier.clone(),
                writer: Some(self.publisher.clone()),
            })
            .grant(self.read_capability.clone())
            .grant(self.write_capability.clone())
    }

    /// Runs a canonical conversation turn from already staged primary content
    /// and ordered attachments. The user record and model-context selection
    /// commit before model dispatch; the assistant record commits afterward.
    /// Retrying the same operation recovers both records and the exact
    /// previously selected context rather than silently selecting newer history.
    pub async fn run_conversation(
        &self,
        bundle: &acyclic_harness::bundle::HarnessBundle,
        operation_id: OperationId,
        content: FileRef,
        attachments: Vec<Attachment>,
        max_steps: u32,
    ) -> Result<TurnOutput> {
        let limits = bundle.limits();
        limits.validate_file(&content)?;
        if attachments.len() > limits.attachments {
            return Err(Error::Invalid(
                "turn attachments exceed harness limits".into(),
            ));
        }
        for attachment in &attachments {
            attachment.validate()?;
            limits.validate_file(&attachment.file)?;
        }
        let referenced = if attachments.len() <= 128 {
            ReferencedAttachments::Inline { items: attachments }
        } else {
            let manifest = self
                .host
                .put_attachment_manifest(
                    &self.volume,
                    &self.write,
                    &format!("turns/{operation_id}/attachments.json"),
                    &attachments,
                    self.maximum_file_bytes,
                    &IdempotencyKey::new(format!("conversation:{operation_id}:attachments"))?,
                )
                .await?;
            manifest
        };
        let user_operation = derived_operation_id(operation_id, b"conversation-user");
        let user_id = Uuid::from_bytes(user_operation.into_bytes());
        let mut aggregate = self.open_conversation(limits).await?;
        let state = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        if let Some(last_user) = state
            .messages
            .iter()
            .rfind(|message| message.kind == MessageKind::User)
            && last_user.id != user_id
            && !state.messages.iter().any(|message| {
                message.kind == MessageKind::Assistant && message.reply_to == Some(last_user.id)
            })
        {
            return Err(Error::Conflict(
                "previous conversation turn is unresolved; retry it before admitting another"
                    .into(),
            ));
        }
        if let Some(existing) = state.messages.iter().find(|message| message.id == user_id) {
            if existing.kind != MessageKind::User
                || existing.content != content
                || existing.attachments != referenced
            {
                return Err(Error::Conflict(
                    "turn identity belongs to another user message".into(),
                ));
            }
        } else {
            let message = ConversationMessage {
                id: user_id,
                sequence: state.messages.len() as u64 + 1,
                kind: MessageKind::User,
                content,
                attachments: referenced,
                reply_to: None,
                tool_call_id: None,
                extensions: BTreeMap::new(),
            };
            self.append_conversation(
                &mut aggregate,
                user_operation,
                "user",
                Action::AppendConversationMessage {
                    message: Box::new(message),
                },
            )
            .await?;
        }
        let selection_operation = operation_id;
        if aggregate
            .reducer()
            .context_selection_for_operation(selection_operation)
            .is_none()
        {
            let state = aggregate
                .reducer()
                .conversation()
                .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
            let current = state
                .messages
                .iter()
                .position(|message| message.id == user_id)
                .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?;
            let mut ids = state.messages[..=current]
                .iter()
                .filter(|message| {
                    matches!(
                        message.kind,
                        MessageKind::System
                            | MessageKind::User
                            | MessageKind::Assistant
                            | MessageKind::ToolCall
                            | MessageKind::ToolResult
                    )
                })
                .map(|message| message.id)
                .collect::<Vec<_>>();
            if ids.len() > limits.context_messages {
                ids.drain(..ids.len() - limits.context_messages);
            }
            // A bounded suffix can start inside a tool exchange. Never hand a
            // provider a result whose precise call fell outside the window.
            let included = ids.iter().copied().collect::<HashSet<_>>();
            let by_id = state.messages[..=current]
                .iter()
                .map(|message| (message.id, message))
                .collect::<HashMap<_, _>>();
            ids.retain(|id| {
                by_id.get(id).is_some_and(|message| {
                    message.kind != MessageKind::ToolResult
                        || message
                            .reply_to
                            .is_some_and(|call| included.contains(&call))
                })
            });
            let selection = ModelContextSelection {
                conversation_revision: state.messages.len() as u64,
                message_ids: ids,
            };
            self.append_conversation(
                &mut aggregate,
                selection_operation,
                "context",
                Action::SelectModelContext { selection },
            )
            .await?;
        }
        let selection = aggregate
            .reducer()
            .context_selection_for_operation(selection_operation)
            .cloned()
            .ok_or_else(|| Error::Storage("model context selection is missing".into()))?;
        let mut historical = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?
            .clone();
        let selected_revision = usize::try_from(selection.conversation_revision)
            .map_err(|_| Error::Storage("selection revision exceeds platform size".into()))?;
        if selected_revision > historical.messages.len() {
            return Err(Error::Storage(
                "selection revision exceeds conversation history".into(),
            ));
        }
        historical.messages.truncate(selected_revision);
        if selection.message_ids.last() != Some(&user_id) {
            return Err(Error::Conflict(
                "turn identity is bound to another context selection".into(),
            ));
        }
        let selected = select_model_context(
            &historical,
            selection,
            self.content_verifier.as_ref(),
            limits.context_messages,
            limits.attachments,
            limits.render_bytes,
        )
        .await?;
        let output = bundle
            .run(TurnInput::from_selected_context(
                operation_id,
                selected,
                max_steps,
            )?)
            .await?;
        self.append_assistant(operation_id, user_id, &output, limits)
            .await?;
        Ok(output)
    }

    /// Convenience over the canonical ref-only turn API for local text input.
    pub async fn run_prompt(
        &self,
        bundle: &acyclic_harness::bundle::HarnessBundle,
        prompt: &str,
    ) -> Result<TurnOutput> {
        let operation_id = OperationId::new();
        let content = self
            .stage(
                operation_id,
                &format!("turns/{operation_id}/user.txt"),
                prompt.as_bytes(),
                "text/plain",
                "prompt.txt",
            )
            .await?;
        let max_steps = u32::try_from(bundle.limits().model_steps)
            .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?;
        self.run_conversation(bundle, operation_id, content, Vec::new(), max_steps)
            .await
    }

    async fn open_conversation(&self, limits: Limits) -> Result<StreamAggregate<MemoryStream>> {
        StreamAggregate::open(
            &self.stream,
            self.conversation.clone(),
            self.issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?
        .with_content_verifier(self.content_verifier.clone())
        .with_limits(limits)
    }

    async fn append_conversation(
        &self,
        aggregate: &mut StreamAggregate<MemoryStream>,
        operation_id: OperationId,
        label: &str,
        action: Action,
    ) -> Result<()> {
        aggregate
            .execute(Command {
                operation_id,
                idempotency_key: IdempotencyKey::new(format!(
                    "conversation:{operation_id}:{label}"
                ))?,
                expected_revision: aggregate.reducer().revision(),
                scope: self.scope.clone(),
                causal_parent: None,
                action,
            })
            .await?;
        Ok(())
    }

    async fn append_assistant(
        &self,
        operation_id: OperationId,
        user_id: Uuid,
        output: &TurnOutput,
        limits: Limits,
    ) -> Result<()> {
        if output.attachments.len() > limits.attachments {
            return Err(Error::Invalid(
                "assistant attachments exceed harness limits".into(),
            ));
        }
        for attachment in &output.attachments {
            attachment.validate()?;
            limits.validate_file(&attachment.file)?;
        }
        let assistant_operation = derived_operation_id(operation_id, b"conversation-assistant");
        let assistant_id = Uuid::from_bytes(assistant_operation.into_bytes());
        let content = self
            .stage(
                operation_id,
                &format!("turns/{operation_id}/assistant.txt"),
                output.text.as_bytes(),
                "text/plain",
                "assistant.txt",
            )
            .await?;
        let metadata = serde_json::to_vec(&output.metadata)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let metadata = self
            .stage(
                operation_id,
                &format!("turns/{operation_id}/metadata.json"),
                &metadata,
                "application/json",
                "metadata.json",
            )
            .await?;
        let attachments = if output.attachments.len() <= 128 {
            ReferencedAttachments::Inline {
                items: output.attachments.clone(),
            }
        } else {
            self.host
                .put_attachment_manifest(
                    &self.volume,
                    &self.write,
                    &format!("turns/{operation_id}/assistant-attachments.json"),
                    &output.attachments,
                    self.maximum_file_bytes,
                    &IdempotencyKey::new(format!(
                        "conversation:{operation_id}:assistant-attachments"
                    ))?,
                )
                .await?
        };
        let mut aggregate = self.open_conversation(limits).await?;
        self.append_tool_history(&mut aggregate, operation_id, user_id)
            .await?;
        let state = aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
        let mut extensions = BTreeMap::new();
        extensions.insert("acyclic.model.metadata".to_owned(), metadata);
        if let Some(existing) = state
            .messages
            .iter()
            .find(|message| message.id == assistant_id)
        {
            if existing.content != content
                || existing.attachments != attachments
                || existing.extensions != extensions
                || existing.reply_to != Some(user_id)
                || existing.kind != MessageKind::Assistant
            {
                return Err(Error::Conflict(
                    "turn identity belongs to another assistant message".into(),
                ));
            }
            return Ok(());
        }
        let message = ConversationMessage {
            id: assistant_id,
            sequence: state.messages.len() as u64 + 1,
            kind: MessageKind::Assistant,
            content,
            attachments,
            reply_to: Some(user_id),
            tool_call_id: None,
            extensions,
        };
        self.append_conversation(
            &mut aggregate,
            assistant_operation,
            "assistant",
            Action::AppendConversationMessage {
                message: Box::new(message),
            },
        )
        .await
    }

    async fn append_tool_history(
        &self,
        aggregate: &mut StreamAggregate<MemoryStream>,
        operation_id: OperationId,
        user_id: Uuid,
    ) -> Result<()> {
        let mut calls = BTreeMap::new();
        for record in self.journal.replay(operation_id).await? {
            let (kind, content, attachments, reply_to, call_id, step) = match record.event {
                ExecutionEvent::ToolStarted {
                    step,
                    call_id,
                    invocation,
                } => (
                    MessageKind::ToolCall,
                    invocation,
                    ReferencedAttachments::Inline { items: Vec::new() },
                    Some(user_id),
                    call_id,
                    step,
                ),
                ExecutionEvent::ToolCompleted {
                    step,
                    call_id,
                    result,
                    projection,
                } => {
                    let call = calls
                        .get(&(step, call_id.clone()))
                        .copied()
                        .ok_or_else(|| Error::Storage("tool result has no durable start".into()))?;
                    (
                        MessageKind::ToolResult,
                        result,
                        ReferencedAttachments::Inline {
                            items: vec![Attachment {
                                file: projection,
                                label: Some("model_projection".into()),
                            }],
                        },
                        Some(call),
                        call_id,
                        step,
                    )
                }
                _ => continue,
            };
            let publication = derived_operation_id(
                operation_id,
                format!("conversation-tool:{}", record.sequence).as_bytes(),
            );
            let id = Uuid::from_bytes(publication.into_bytes());
            if kind == MessageKind::ToolCall && calls.insert((step, call_id.clone()), id).is_some()
            {
                return Err(Error::Storage(
                    "duplicate durable tool call identity".into(),
                ));
            }
            let state = aggregate
                .reducer()
                .conversation()
                .ok_or_else(|| Error::Storage("conversation projection is missing".into()))?;
            if let Some(existing) = state.messages.iter().find(|message| message.id == id) {
                if existing.kind != kind
                    || existing.content != content
                    || existing.attachments != attachments
                    || existing.reply_to != reply_to
                    || existing.tool_call_id.as_deref() != Some(call_id.as_str())
                {
                    return Err(Error::Conflict(
                        "tool publication identity belongs to another message".into(),
                    ));
                }
                continue;
            }
            let message = ConversationMessage {
                id,
                sequence: state.messages.len() as u64 + 1,
                kind,
                content,
                attachments,
                reply_to,
                tool_call_id: Some(call_id),
                extensions: BTreeMap::new(),
            };
            self.append_conversation(
                aggregate,
                publication,
                "tool",
                Action::AppendConversationMessage {
                    message: Box::new(message),
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Stages user content or an attachment before it is admitted to a turn.
    pub async fn stage(
        &self,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        self.publisher
            .stage(operation_id, path, bytes, media_type, display_name)
            .await
    }

    /// Reads a pinned owner-private version after verifying its descriptor.
    pub async fn read(&self, file: &FileRef) -> Result<Vec<u8>> {
        self.content_verifier.read(file).await
    }

    /// Publishes one owner-mediated exact-file read scope to another agent,
    /// independent of any fork relationship. The file must be resident now.
    pub async fn delegate_file_read(
        &self,
        reader: AgentId,
        id: impl Into<String>,
        file: &FileRef,
    ) -> Result<Scope> {
        self.read(file).await?;
        self.issuer
            .delegate_private_file_read(&self.scope, reader, id, file)
    }

    /// Resolves a pinned reference under an owner-issued signed read scope.
    pub async fn read_delegated(&self, scope: &Scope, file: &FileRef) -> Result<Vec<u8>> {
        self.delegated_reader(scope)?.read(file).await
    }

    /// Shares a private subtree with a named agent. An empty prefix grants
    /// root discovery but never exposes internal storage receipts.
    pub fn delegate_directory_read(
        &self,
        reader: AgentId,
        id: impl Into<String>,
        prefix: &str,
    ) -> Result<Scope> {
        self.issuer
            .delegate_private_directory_read(&self.scope, reader, id, &self.volume, prefix)
    }

    /// Lists a lazily opened owner-private directory under a signed grant.
    pub async fn list_delegated(
        &self,
        scope: &Scope,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        self.delegated_reader(scope)?
            .list_private_directory(
                &self.volume,
                granted_prefix,
                path,
                expected_generation,
                after,
                maximum_entries,
            )
            .await
    }

    /// Reads a named file from a delegated private directory and pins its
    /// observed version for later exact-reference use.
    pub async fn read_delegated_path(
        &self,
        scope: &Scope,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, Vec<u8>)> {
        self.delegated_reader(scope)?
            .read_private_path(&self.volume, granted_prefix, path, expected_generation)
            .await
    }

    fn delegated_reader(
        &self,
        scope: &Scope,
    ) -> Result<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>> {
        FilesystemContentVerifier::new(
            self.host.clone(),
            self.issuer.verifier(),
            scope.clone(),
            self.maximum_file_bytes,
        )
    }

    /// Resolves an addressable local interaction under a host-issued exact
    /// responder grant. This method belongs to the local host, not the agent.
    pub async fn resolve_interaction(
        &self,
        id: InteractionId,
        response: InteractionResponse,
    ) -> Result<InteractionOutcome> {
        let responder = self.issuer.root(
            format!("local-responder:{id}"),
            Capabilities::new([
                "interaction:resolve".to_owned(),
                format!("interaction:respond:{id}"),
            ]),
        );
        self.journal
            .resolve_interaction(id, response, &responder)
            .await?;
        self.journal
            .interaction_outcome(id)
            .await?
            .ok_or_else(|| Error::Storage("resolved interaction has no committed outcome".into()))
    }

    /// Returns the original agent's private volume identity.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Opens the bound conversation's exact Stream history to host adapters.
    #[must_use]
    pub fn stream(&self) -> &StreamClient<MemoryStream> {
        &self.stream
    }

    /// Returns the bound conversation aggregate identity.
    #[must_use]
    pub const fn conversation(&self) -> &Authority {
        &self.conversation
    }

    /// Returns the signed owner scope for composing additional local adapters.
    #[must_use]
    pub const fn owner_scope(&self) -> &Scope {
        &self.scope
    }

    /// Returns the trusted verifier without disclosing the signing key.
    #[must_use]
    pub fn verifier(&self) -> acyclic_harness::core::AuthorityVerifier {
        self.issuer.verifier()
    }
}

fn derived_operation_id(turn: OperationId, domain: &[u8]) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&turn.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        Outcome,
        executor::TurnInput,
        model::{Model, ModelAttempt, ModelEvent, ModelProvider, ModelRequest},
        runtime::{TaskDefinition, TaskRegistry},
    };
    use futures::{
        future::BoxFuture,
        stream::{self, BoxStream},
    };
    use serde_json::Value;
    use std::sync::Mutex;

    struct TextModel(Arc<Mutex<Vec<ModelRequest>>>);

    impl ModelProvider for TextModel {
        fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            Box::pin(stream::iter(vec![
                Ok(ModelEvent::Content {
                    delta: "local response".into(),
                }),
                Ok(ModelEvent::Completed {
                    metadata: Value::Null,
                }),
            ]))
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[tokio::test]
    async fn local_builder_runs_through_ref_only_storage() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::from_bytes([81; 16]), 4_096).await?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let bundle = storage
            .builder()
            .model(
                Model::new("test", "text", "1", Value::Null)?,
                Arc::new(TextModel(requests.clone())),
            )
            .grant("model:generate")
            .build()?;
        let operation_id = OperationId::from_bytes([82; 16]);
        let attachment = storage
            .stage(
                operation_id,
                "input/note.txt",
                b"private attachment body",
                "text/plain",
                "note.txt",
            )
            .await?;
        let user = storage
            .stage(
                operation_id,
                "input/prompt.txt",
                b"hello",
                "text/plain",
                "prompt.txt",
            )
            .await?;
        assert_eq!(
            storage.read(&attachment).await?.as_slice(),
            b"private attachment body"
        );
        let reader = AgentId::from_bytes([83; 16]);
        let delegated = storage
            .delegate_file_read(reader, "attached-reader", &attachment)
            .await?;
        assert_eq!(delegated.agent(), Some(reader));
        assert_eq!(
            storage
                .read_delegated(&delegated, &attachment)
                .await?
                .as_slice(),
            b"private attachment body"
        );
        let root_directory = storage.delegate_directory_read(reader, "attached-root", "")?;
        assert_eq!(
            storage
                .read_delegated(&root_directory, &attachment)
                .await?
                .as_slice(),
            b"private attachment body"
        );
        assert!(
            ContentGrant::verify(
                &storage.verifier(),
                &delegated,
                storage.volume(),
                VolumeOperation::Write
            )
            .is_err()
        );
        let directory = storage.delegate_directory_read(reader, "attached-directory", "input")?;
        let (generation, listed) = storage
            .list_delegated(&directory, "input", "input", None, None, 16)
            .await?;
        assert!(
            listed
                .entries
                .iter()
                .any(|entry| entry.name.unicode_text().as_deref() == Some("note.txt"))
        );
        let (from_directory, body) = storage
            .read_delegated_path(&directory, "input", "input/note.txt", Some(&generation))
            .await?;
        assert_eq!(body, b"private attachment body");
        assert_eq!(from_directory.path(), attachment.path());
        assert!(
            storage
                .read_delegated_path(&directory, "input", "inputs/note.txt", Some(&generation))
                .await
                .is_err()
        );
        storage
            .stage(
                OperationId::from_bytes([84; 16]),
                "input/new.txt",
                b"new",
                "text/plain",
                "new.txt",
            )
            .await?;
        assert!(
            storage
                .read_delegated_path(&directory, "input", "input/note.txt", Some(&generation))
                .await
                .is_err()
        );
        assert!(
            ContentGrant::verify(
                &storage.verifier(),
                &directory,
                storage.volume(),
                VolumeOperation::Write
            )
            .is_err()
        );
        let output = storage
            .run_conversation(
                &bundle,
                operation_id,
                user.clone(),
                vec![Attachment {
                    file: attachment.clone(),
                    label: None,
                }],
                2,
            )
            .await?;
        assert_eq!(output.text, "local response");
        let replayed = storage
            .run_conversation(
                &bundle,
                operation_id,
                user.clone(),
                vec![Attachment {
                    file: attachment.clone(),
                    label: None,
                }],
                2,
            )
            .await?;
        assert_eq!(replayed, output);
        let aggregate = storage.open_conversation(bundle.limits()).await?;
        assert_eq!(
            aggregate
                .reducer()
                .conversation()
                .ok_or_else(|| acyclic_harness::Error::Invalid("conversation not bound".into()))?
                .messages
                .len(),
            2
        );
        assert_eq!(aggregate.reducer().context_selections().len(), 1);
        storage.run_prompt(&bundle, "follow-up").await?;
        assert_eq!(
            storage
                .run_conversation(
                    &bundle,
                    operation_id,
                    user,
                    vec![Attachment {
                        file: attachment,
                        label: None
                    }],
                    2
                )
                .await?,
            output
        );
        let requests = requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].messages.len(), 1);
        assert_eq!(requests[1].messages.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn typed_tasks_stage_and_resolve_only_with_owner_bound_grants() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::from_bytes([91; 16]), 4_096).await?;
        let mut tasks = TaskRegistry::default();
        tasks.register(
            TaskDefinition::live("file_task", "1", |context, (): ()| async move {
                let file = context
                    .stage_file(
                        OperationId::from_bytes([92; 16]),
                        "task/result.txt",
                        b"pinned bytes",
                        "text/plain",
                        "result.txt",
                    )
                    .await?;
                let bytes = context.read_file(&file).await?;
                let narrowed =
                    context.scoped(Capabilities::new(Vec::<String>::new()), Limits::default())?;
                assert!(narrowed.read_file(&file).await.is_err());
                assert!(
                    narrowed
                        .stage_file(
                            OperationId::from_bytes([93; 16]),
                            "task/denied.txt",
                            b"denied",
                            "text/plain",
                            "denied.txt"
                        )
                        .await
                        .is_err()
                );
                String::from_utf8(bytes).map_err(|error| Error::Invalid(error.to_string()))
            })?
            .requires("content:write")?,
        )?;
        let bundle = storage
            .builder()
            .tasks(tasks)
            .model(
                Model::new("test", "text", "1", Value::Null)?,
                Arc::new(TextModel(Arc::new(Mutex::new(Vec::new())))),
            )
            .grant("model:generate")
            .build()?;
        let runtime = bundle.runtime();
        let task = runtime.task::<(), String>("file_task")?;
        assert_eq!(
            runtime.spawn(&task, ()).await?.result().await?,
            Outcome::Succeeded("pinned bytes".to_owned())
        );
        Ok(())
    }
}
