//! Infrastructure-free Harness storage with the same Stream/Filesystem contracts.

use crate::{FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemHost};
use acyclic_fs::{
    Fs, MemoryAuthorityBackend, MemoryObjectBackend, WorkspaceDirectoryPage,
    kernel::{FileKind, LogicalName, NameEncoding},
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
    model::{Model, ModelProvider},
    projection::select_model_context,
    resources::{GenerationRef, ProviderRef},
    runtime::{ContentBindings, RuntimeScope},
    store::StreamAggregate,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use acyclic_stream::{MemoryStream, StreamClient};
use futures::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Value, json};
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

/// Ready-to-run local Harness with an isolated conversation and private volume.
///
/// This preset is intentionally ephemeral. Its storage and bundle remain
/// accessible so callers can stage attachments or use the full typed turn API.
pub struct LocalHarness {
    storage: MemoryHarnessStorage,
    bundle: acyclic_harness::bundle::HarnessBundle,
}

impl LocalHarness {
    /// Starts a fresh local agent with the standard bounded runtime defaults.
    pub async fn new(model: Model, provider: Arc<dyn ModelProvider>) -> Result<Self> {
        Self::with_limits(AgentId::new(), Limits::default(), model, provider).await
    }

    /// Starts a named local agent with explicit admission and rendering bounds.
    pub async fn with_limits(
        agent: AgentId,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Self> {
        limits.validate()?;
        let storage = MemoryHarnessStorage::new(agent, limits.file_bytes).await?;
        let tools = storage.default_tools(limits)?;
        Self::from_storage(
            storage,
            limits,
            model,
            provider,
            tools,
            [
                "tool:call:acyclic.read_file".into(),
                "tool:call:acyclic.stage_file".into(),
                "tool:call:acyclic.list_files".into(),
            ],
        )
    }

    /// Starts a local agent with an explicitly selected, versioned tool set and
    /// matching capability grants. Merely registering a tool does not authorize
    /// its execution; callers must grant `tool:call:<name>` deliberately.
    pub async fn with_tools(
        agent: AgentId,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        tools: ToolRegistry,
        capabilities: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        limits.validate()?;
        let storage = MemoryHarnessStorage::new(agent, limits.file_bytes).await?;
        Self::from_storage(storage, limits, model, provider, tools, capabilities)
    }

    fn from_storage(
        storage: MemoryHarnessStorage,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        tools: ToolRegistry,
        capabilities: impl IntoIterator<Item = String>,
    ) -> Result<Self> {
        let mut builder = storage
            .builder()
            .model(model, provider)
            .grant("model:generate")
            .tools(tools)
            .limits(limits);
        for capability in capabilities {
            builder = builder.grant(capability);
        }
        let bundle = builder.build()?;
        Ok(Self { storage, bundle })
    }

    /// Runs a text prompt through the canonical ref-only conversation path.
    pub async fn run(&self, prompt: &str) -> Result<TurnOutput> {
        self.storage.run_prompt(&self.bundle, prompt).await
    }

    /// Runs text with ordered, already-staged attachment references.
    pub async fn run_with_attachments(
        &self,
        prompt: &str,
        attachments: Vec<Attachment>,
    ) -> Result<TurnOutput> {
        self.storage
            .run_prompt_with_attachments(&self.bundle, prompt, attachments)
            .await
    }

    /// Exposes staging, attachment admission, conversation replay, and grants.
    #[must_use]
    pub fn storage(&self) -> &MemoryHarnessStorage {
        &self.storage
    }

    /// Exposes the immutable runtime binding for typed execution.
    #[must_use]
    pub fn bundle(&self) -> &acyclic_harness::bundle::HarnessBundle {
        &self.bundle
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadFileInput {
    file: FileRef,
}

struct LocalReadFileTool {
    verifier: Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>,
    maximum_bytes: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StageFileInput {
    path: String,
    text: String,
    media_type: String,
    display_name: String,
}

struct LocalStageFileTool {
    publisher: Arc<MemoryContentPublisher>,
    maximum_bytes: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListFilesInput {
    path: String,
    expected_generation: Option<GenerationRef>,
    after: Option<String>,
    maximum_entries: u32,
}

struct LocalListFilesTool {
    verifier: Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>,
    volume: VolumeRef,
    maximum_bytes: u64,
    observed: tokio::sync::Mutex<HashMap<OperationId, (ToolInvocation, ToolResult)>>,
}

fn require_volume_grant(
    scope: Option<&RuntimeScope>,
    volume: &VolumeRef,
    operation: VolumeOperation,
) -> Result<()> {
    let scope =
        scope.ok_or_else(|| Error::Unauthorized("file tool requires a scoped caller".into()))?;
    let capability = volume.capability(operation)?;
    if !scope.grants().contains(&capability) {
        return Err(Error::Unauthorized(format!("scope lacks {capability}")));
    }
    Ok(())
}

impl ToolExecutor for LocalListFilesTool {
    fn authorize(&self, scope: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
        require_volume_grant(scope, &self.volume, VolumeOperation::Read)
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            if let Some((original, result)) = observed.get(&invocation.operation_id) {
                return if original == &invocation {
                    Ok(result.clone())
                } else {
                    Err(Error::Conflict(
                        "list_files operation identity changed".into(),
                    ))
                };
            }
            let input: ListFilesInput = serde_json::from_value(invocation.arguments.clone())
                .map_err(|error| Error::Invalid(format!("list_files input is invalid: {error}")))?;
            if input.maximum_entries == 0
                || input.maximum_entries > 64
                || (input.after.is_some() && input.expected_generation.is_none())
            {
                return Err(Error::Invalid(
                    "list_files requires a bounded, generation-pinned page".into(),
                ));
            }
            let after = input
                .after
                .map(|name| {
                    LogicalName::new(NameEncoding::Utf8, name.into_bytes(), 255)
                        .map_err(|error| Error::Invalid(error.to_string()))
                })
                .transpose()?;
            let (generation, page) = self
                .verifier
                .list_private_directory(
                    &self.volume,
                    "",
                    &input.path,
                    input.expected_generation.as_ref(),
                    after.as_ref(),
                    input.maximum_entries,
                )
                .await?;
            let entries =
                page.entries
                    .into_iter()
                    .map(|entry| {
                        let name = entry.name.unicode_text().ok_or_else(|| {
                            Error::Unsupported("local file name is not UTF-8".into())
                        })?;
                        let kind = match entry.kind {
                            FileKind::Regular => "file",
                            FileKind::Directory => "directory",
                            FileKind::SymbolicLink => "symlink",
                            FileKind::Fifo => "fifo",
                            FileKind::Socket => "socket",
                            FileKind::CharacterDevice => "character_device",
                            FileKind::BlockDevice => "block_device",
                            FileKind::ReparsePoint => "reparse_point",
                            FileKind::MountBoundary => "mount_boundary",
                        };
                        Ok(json!({"name": name, "kind": kind}))
                    })
                    .collect::<Result<Vec<_>>>()?;
            let next_after = entries
                .last()
                .and_then(|entry| entry["name"].as_str())
                .map(str::to_owned);
            let value = json!({
                "generation": generation,
                "entries": entries,
                "has_more": page.has_more,
                "next_after": next_after,
            });
            if serde_json::to_vec(&value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .len() as u64
                > self.maximum_bytes
            {
                return Err(Error::Invalid("directory page exceeds render limit".into()));
            }
            let result = ToolResult { value };
            observed.insert(invocation.operation_id, (invocation, result.clone()));
            Ok(result)
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let observed = self.observed.lock().await;
            match observed.get(&invocation.operation_id) {
                Some((original, result)) if original == &invocation => Ok(Some(result.clone())),
                Some(_) => Err(Error::Conflict(
                    "list_files operation identity changed".into(),
                )),
                None => Ok(None),
            }
        })
    }
}

impl ToolProjection for LocalListFilesTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

impl ToolExecutor for LocalStageFileTool {
    fn authorize(&self, scope: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
        require_volume_grant(scope, &self.publisher.volume, VolumeOperation::Write)
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let input: StageFileInput = serde_json::from_value(invocation.arguments)
                .map_err(|error| Error::Invalid(format!("stage_file input is invalid: {error}")))?;
            let retry =
                IdempotencyKey::new(format!("local-tool-stage:{}", invocation.operation_id))?;
            let file = self
                .publisher
                .host
                .put_content(
                    &self.publisher.volume,
                    &self.publisher.write,
                    &input.path,
                    input.text.as_bytes(),
                    &input.media_type,
                    &input.display_name,
                    self.maximum_bytes,
                    &retry,
                )
                .await?;
            Ok(ToolResult {
                value: json!({"file": file}),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        // put_content checks the atomic operation receipt before attempting a
        // write. Re-entering it with the same ID recovers the pinned result or
        // conflicts if the proposed bytes or metadata changed.
        Box::pin(async move { self.execute(invocation).await.map(Some) })
    }
}

impl ToolProjection for LocalStageFileTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(result.value.clone())
    }
}

impl ToolExecutor for LocalReadFileTool {
    fn authorize(&self, scope: Option<&RuntimeScope>, invocation: &ToolInvocation) -> Result<()> {
        let input: ReadFileInput = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("read_file input is invalid: {error}")))?;
        require_volume_grant(scope, input.file.volume(), VolumeOperation::Read)
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let input: ReadFileInput = serde_json::from_value(invocation.arguments)
                .map_err(|error| Error::Invalid(format!("read_file input is invalid: {error}")))?;
            if input.file.descriptor().byte_length() > self.maximum_bytes {
                return Err(Error::Invalid(
                    "file exceeds the tool rendering limit".into(),
                ));
            }
            let bytes = self.verifier.read(&input.file).await?;
            let text = String::from_utf8(bytes)
                .map_err(|_| Error::Unsupported("read_file requires UTF-8 content".into()))?;
            Ok(ToolResult {
                value: json!({"file": input.file, "text": text}),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        // The exact FileRef pins immutable bytes; repeating this read cannot
        // redispatch a write or observe a newer path version.
        Box::pin(async move { self.execute(invocation).await.map(Some) })
    }
}

impl ToolProjection for LocalReadFileTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let text = result
            .value
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Invalid("read_file result has no text".into()))?;
        let projection = Value::String(text.into());
        if serde_json::to_vec(&projection)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len() as u64
            > self.maximum_bytes
        {
            return Err(Error::Invalid(
                "read_file projection exceeds render limit".into(),
            ));
        }
        Ok(projection)
    }
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
    /// Builds the local ref-only file tools against this exact owner volume.
    /// Callers composing a custom builder can use this registry unchanged.
    pub fn default_tools(&self, limits: Limits) -> Result<ToolRegistry> {
        limits.validate()?;
        if limits.file_bytes > self.maximum_file_bytes {
            return Err(Error::Invalid(
                "tool file limit exceeds the bound storage volume".into(),
            ));
        }
        let mut tools = ToolRegistry::new();
        tools.register(self.read_file_tool(limits))?;
        tools.register(self.stage_file_tool(limits))?;
        tools.register(self.list_files_tool(limits))?;
        Ok(tools)
    }

    fn list_files_tool(&self, limits: Limits) -> Tool {
        let implementation = Arc::new(LocalListFilesTool {
            verifier: self.content_verifier.clone(),
            volume: self.volume.clone(),
            maximum_bytes: limits.render_bytes,
            observed: tokio::sync::Mutex::new(HashMap::new()),
        });
        Tool {
            definition: ToolDefinition {
                name: "acyclic.list_files".into(),
                revision: "1".into(),
                description: "List one bounded, generation-pinned page of the agent-private volume"
                    .into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "expected_generation": {"type": ["object", "null"]},
                        "after": {"type": ["string", "null"]},
                        "maximum_entries": {"type": "integer", "minimum": 1, "maximum": 64}
                    },
                    "required": ["path", "maximum_entries"],
                    "additionalProperties": false
                }),
                output_schema: json!({
                    "type": "object",
                    "properties": {
                        "generation": {"type": "object"},
                        "entries": {"type": "array", "items": {"type": "object"}},
                        "has_more": {"type": "boolean"},
                        "next_after": {"type": ["string", "null"]}
                    },
                    "required": ["generation", "entries", "has_more", "next_after"],
                    "additionalProperties": false
                }),
            },
            executor: implementation.clone(),
            projection: implementation,
        }
    }

    fn stage_file_tool(&self, limits: Limits) -> Tool {
        let implementation = Arc::new(LocalStageFileTool {
            publisher: self.publisher.clone(),
            maximum_bytes: limits.file_bytes,
        });
        Tool {
            definition: ToolDefinition {
                name: "acyclic.stage_file".into(),
                revision: "1".into(),
                description: "Stage a bounded UTF-8 file in the agent-private volume and return its immutable FileRef".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "text": {"type": "string"},
                        "media_type": {"type": "string"},
                        "display_name": {"type": "string"}
                    },
                    "required": ["path", "text", "media_type", "display_name"],
                    "additionalProperties": false
                }),
                output_schema: json!({
                    "type": "object",
                    "properties": {"file": {"type": "object"}},
                    "required": ["file"],
                    "additionalProperties": false
                }),
            },
            executor: implementation.clone(),
            projection: implementation,
        }
    }

    fn read_file_tool(&self, limits: Limits) -> Tool {
        let implementation = Arc::new(LocalReadFileTool {
            verifier: self.content_verifier.clone(),
            maximum_bytes: limits.render_bytes,
        });
        Tool {
            definition: ToolDefinition {
                name: "acyclic.read_file".into(),
                revision: "1".into(),
                description: "Read bounded UTF-8 bytes from an authorized immutable FileRef".into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"file": {"type": "object"}},
                    "required": ["file"],
                    "additionalProperties": false
                }),
                output_schema: json!({
                    "type": "object",
                    "properties": {"file": {"type": "object"}, "text": {"type": "string"}},
                    "required": ["file", "text"],
                    "additionalProperties": false
                }),
            },
            executor: implementation.clone(),
            projection: implementation,
        }
    }

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
            Uuid::new_v4().to_string(),
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

    /// Globally distinct identity of this local agent-private volume.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
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
    #[allow(
        clippy::too_many_lines,
        reason = "one complete idempotent conversation turn"
    )]
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
            self.host
                .put_attachment_manifest(
                    &self.volume,
                    &self.write,
                    &format!("turns/{operation_id}/attachments.json"),
                    &attachments,
                    self.maximum_file_bytes,
                    &IdempotencyKey::new(format!("conversation:{operation_id}:attachments"))?,
                )
                .await?
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
            let prefix = state
                .messages
                .get(..=current)
                .ok_or_else(|| Error::Storage("admitted user message is missing".into()))?;
            let mut ids = prefix
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
            let by_id = prefix
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
        self.run_prompt_with_attachments(bundle, prompt, Vec::new())
            .await
    }

    /// Text convenience with ordered staged file refs; canonical history still
    /// contains only refs and the complete list is manifest-backed if needed.
    pub async fn run_prompt_with_attachments(
        &self,
        bundle: &acyclic_harness::bundle::HarnessBundle,
        prompt: &str,
        attachments: Vec<Attachment>,
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
        self.run_conversation(bundle, operation_id, content, attachments, max_steps)
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

    /// Lazily discovers this agent's private files under its existing signed
    /// volume-read grant. Pages pin a generation; callers pass it back to
    /// detect a changed directory instead of silently mixing two heads.
    pub async fn list_private_directory(
        &self,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        self.content_verifier
            .list_private_directory(
                &self.volume,
                "",
                path,
                expected_generation,
                after,
                maximum_entries,
            )
            .await
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
        model::{Model, ModelAttempt, ModelEvent, ModelProvider, ModelRequest},
        runtime::{TaskDefinition, TaskRegistry},
    };
    use futures::{
        future::BoxFuture,
        stream::{self, BoxStream},
    };
    use serde_json::Value;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

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
    async fn default_read_file_tool_checks_owner_and_render_bound() -> Result<()> {
        let owner = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
        let other = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
        let file = owner
            .stage(
                OperationId::new(),
                "notes/one.txt",
                b"pinned text",
                "text/plain",
                "one.txt",
            )
            .await?;
        let mut limits = Limits {
            file_bytes: 4_096,
            render_bytes: 32,
            ..Limits::default()
        };
        let tool = owner
            .default_tools(limits)?
            .get("acyclic.read_file")
            .ok_or_else(|| Error::NotFound("default read tool".into()))?
            .clone();
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "read-1".into(),
            name: "acyclic.read_file".into(),
            arguments: json!({"file": file}),
        };
        let result = tool.executor.execute(invocation.clone()).await?;
        assert_eq!(result.value["text"], "pinned text");
        assert_eq!(
            tool.projection.project(&invocation, &result)?,
            json!("pinned text")
        );
        assert_eq!(
            tool.executor.reconcile(invocation.clone()).await?,
            Some(result)
        );
        assert!(matches!(
            other
                .default_tools(limits)?
                .get("acyclic.read_file")
                .ok_or_else(|| Error::NotFound("other read tool".into()))?
                .executor
                .execute(invocation.clone())
                .await,
            Err(Error::Unauthorized(_)) | Err(Error::NotFound(_))
        ));
        limits.render_bytes = 4;
        assert!(matches!(
            owner
                .default_tools(limits)?
                .get("acyclic.read_file")
                .ok_or_else(|| Error::NotFound("bounded read tool".into()))?
                .executor
                .execute(invocation)
                .await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn default_stage_file_reconciles_by_operation_and_rejects_changed_retry() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
        let limits = Limits {
            file_bytes: 4_096,
            render_bytes: 1_024,
            ..Limits::default()
        };
        let tool = storage
            .default_tools(limits)?
            .get("acyclic.stage_file")
            .ok_or_else(|| Error::NotFound("default stage tool".into()))?
            .clone();
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "write-1".into(),
            name: "acyclic.stage_file".into(),
            arguments: json!({
                "path": "notes/one.txt", "text": "saved text",
                "media_type": "text/plain", "display_name": "one.txt"
            }),
        };
        let first = tool.executor.execute(invocation.clone()).await?;
        assert_eq!(
            tool.executor.reconcile(invocation.clone()).await?,
            Some(first.clone())
        );
        let file: FileRef = serde_json::from_value(first.value["file"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(storage.read(&file).await?, b"saved text");
        let mut changed = invocation.clone();
        changed.arguments["text"] = json!("different text");
        assert!(matches!(
            tool.executor.reconcile(changed).await,
            Err(Error::Conflict(_))
        ));
        let mut changed_path = invocation;
        changed_path.arguments["path"] = json!("notes/another.txt");
        assert!(matches!(
            tool.executor.reconcile(changed_path).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn owner_private_directory_is_lazy_paged_and_generation_pinned() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
        storage
            .stage(
                OperationId::new(),
                "notes/one.txt",
                b"one",
                "text/plain",
                "one.txt",
            )
            .await?;
        let (generation, root) = storage.list_private_directory("", None, None, 8).await?;
        assert!(
            root.entries
                .iter()
                .any(|entry| entry.name.unicode_text().as_deref() == Some("notes"))
        );
        assert!(
            !root
                .entries
                .iter()
                .any(|entry| entry.name.unicode_text().as_deref() == Some(".system"))
        );
        let (_, notes) = storage
            .list_private_directory("notes", Some(&generation), None, 8)
            .await?;
        assert!(
            notes
                .entries
                .iter()
                .any(|entry| entry.name.unicode_text().as_deref() == Some("one.txt"))
        );
        let limits = Limits {
            file_bytes: 4_096,
            render_bytes: 1_024,
            ..Limits::default()
        };
        let tool = storage
            .default_tools(limits)?
            .get("acyclic.list_files")
            .ok_or_else(|| Error::NotFound("default list tool".into()))?
            .clone();
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "list-1".into(),
            name: "acyclic.list_files".into(),
            arguments: json!({"path": "notes", "maximum_entries": 8}),
        };
        let page = tool.executor.execute(invocation.clone()).await?;
        assert_eq!(page.value["entries"][0]["name"], "one.txt");
        storage
            .stage(
                OperationId::new(),
                "notes/two.txt",
                b"two",
                "text/plain",
                "two.txt",
            )
            .await?;
        assert_eq!(tool.executor.reconcile(invocation).await?, Some(page));
        let (_, pinned_notes) = storage
            .list_private_directory("notes", Some(&generation), None, 8)
            .await?;
        assert_eq!(pinned_notes.entries.len(), 1);
        assert_eq!(
            pinned_notes.entries[0].name.unicode_text().as_deref(),
            Some("one.txt")
        );
        let (_, current_notes) = storage
            .list_private_directory("notes", None, None, 8)
            .await?;
        assert_eq!(current_notes.entries.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn local_private_volume_identity_is_unique_per_conversation() -> Result<()> {
        let agent = AgentId::new();
        let first = MemoryHarnessStorage::new(agent, 4_096).await?;
        let second = MemoryHarnessStorage::new(agent, 4_096).await?;
        assert_ne!(first.volume(), second.volume());
        let file = first
            .stage(
                OperationId::new(),
                "notes/one.txt",
                b"private",
                "text/plain",
                "one.txt",
            )
            .await?;
        assert!(matches!(
            second.read(&file).await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn default_file_tools_require_caller_volume_grants() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
        let limits = Limits {
            file_bytes: 4_096,
            render_bytes: 4_096,
            ..Limits::default()
        };
        let tools = storage.default_tools(limits)?;
        let only_calls = RuntimeScope::new(
            Capabilities::new([
                "tool:call:acyclic.read_file",
                "tool:call:acyclic.list_files",
                "tool:call:acyclic.stage_file",
            ]),
            limits,
        )?;
        let file = storage
            .stage(
                OperationId::new(),
                "notes/one.txt",
                b"one",
                "text/plain",
                "one.txt",
            )
            .await?;
        for (name, arguments) in [
            ("acyclic.read_file", json!({"file": file})),
            (
                "acyclic.list_files",
                json!({"path": "", "maximum_entries": 8}),
            ),
            (
                "acyclic.stage_file",
                json!({"path": "notes/two.txt", "text": "two",
                "media_type": "text/plain", "display_name": "two.txt"}),
            ),
        ] {
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: name.into(),
                name: name.into(),
                arguments,
            };
            let tool = tools
                .get(name)
                .ok_or_else(|| Error::NotFound(name.into()))?;
            assert!(matches!(
                tool.executor.authorize(Some(&only_calls), &invocation),
                Err(Error::Unauthorized(_))
            ));
        }
        Ok(())
    }

    struct ReadFileModel {
        file: Arc<Mutex<Option<FileRef>>>,
        calls: AtomicUsize,
    }

    impl ModelProvider for ReadFileModel {
        fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let Some(file) = self
                    .file
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                else {
                    return Box::pin(stream::iter(vec![Err(Error::Invalid(
                        "file was not staged before model generation".into(),
                    ))]));
                };
                assert!(
                    request
                        .tools
                        .iter()
                        .any(|tool| tool.name == "acyclic.read_file")
                );
                Box::pin(stream::iter(vec![
                    Ok(ModelEvent::ToolCall {
                        call_id: "read".into(),
                        name: "acyclic.read_file".into(),
                        arguments: json!({"file": file}),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]))
            } else {
                assert!(request.messages.iter().any(|message| {
                    matches!(
                        &message.content,
                        acyclic_harness::model::ModelContent::Part(
                            acyclic_harness::model::ModelContentPart::ToolResult { .. }
                        )
                    )
                }));
                Box::pin(stream::iter(vec![
                    Ok(ModelEvent::Content {
                        delta: "read complete".into(),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: Value::Null,
                    }),
                ]))
            }
        }

        fn reconcile<'a>(
            &'a self,
            _: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            Box::pin(async { Ok(None) })
        }
    }

    #[tokio::test]
    async fn local_harness_advertises_and_executes_default_read_file() -> Result<()> {
        let file = Arc::new(Mutex::new(None));
        let model = Arc::new(ReadFileModel {
            file: file.clone(),
            calls: AtomicUsize::new(0),
        });
        let local =
            LocalHarness::new(Model::new("test", "file", "1", Value::Null)?, model.clone()).await?;
        let staged = local
            .storage()
            .stage(
                OperationId::new(),
                "notes/read.txt",
                b"read me",
                "text/plain",
                "read.txt",
            )
            .await?;
        *file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(staged);
        assert_eq!(local.run("read the file").await?.text, "read complete");
        assert_eq!(model.calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn local_harness_runs_without_manual_storage_wiring() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let local = LocalHarness::new(
            Model::new("test", "text", "1", Value::Null)?,
            Arc::new(TextModel(requests.clone())),
        )
        .await?;
        let output = local.run("hello").await?;
        assert_eq!(output.text, "local response");
        let file = local
            .storage()
            .stage(
                OperationId::new(),
                "input/note.txt",
                b"attached text",
                "text/plain",
                "note.txt",
            )
            .await?;
        let output = local
            .run_with_attachments(
                "read this",
                vec![Attachment {
                    file,
                    label: Some("note".into()),
                }],
            )
            .await?;
        assert_eq!(output.text, "local response");
        assert_eq!(
            requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2
        );
        assert!(matches!(
            local.storage().volume().owner(),
            VolumeOwner::Agent(_)
        ));
        Ok(())
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
                .is_ok()
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
