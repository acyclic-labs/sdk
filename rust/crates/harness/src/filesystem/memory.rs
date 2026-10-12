//! Infrastructure-free Harness storage with the same Stream/Filesystem contracts.

use crate::contract::capability;
use crate::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost,
};
use crate::{
    AgentId, Capabilities, ConversationId, Error, IdempotencyKey, InteractionId, OperationId,
    Result,
    conversation::{
        Attachment, ContentPublisher, ContentResidencyVerifier, FileRef, Limits, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry, Scope},
    executor::{ExecutionJournal, TurnOutput},
    interaction::{InteractionOutcome, InteractionResponse},
    model::{Model, ModelProvider},
    resources::{GenerationRef, ProviderRef},
    runtime::{ContentBindings, RuntimeScope},
    store::StreamAggregate,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
};
use acyclic_fs::{
    Fs, MemoryAuthorityBackend, MemoryObjectBackend, WorkspaceDirectoryPage,
    kernel::{FileKind, LogicalName, NameEncoding},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{MemoryStream, StreamClient};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
};
use uuid::Uuid;
#[cfg(test)]
use crate::{conversation::{ConversationMessage, MessageKind}, executor::TurnInput};

type MemoryHost = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;
type MemoryJournal =
    FilesystemExecutionJournal<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

fn new_memory_store(
    volume: &VolumeRef,
    maximum_file_bytes: u64,
) -> Result<Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>> {
    Ok(Arc::new(std::sync::Mutex::new(
        crate::memory_store::MemoryStore::new(
            volume.clone(),
            maximum_file_bytes,
            crate::conversation::MAX_PATH_BYTES as u64,
            u64::MAX,
            u64::MAX,
        )?,
    )))
}

/// A fully bound in-process journal and owner-private file volume.
pub struct MemoryHarnessStorage {
    journal: Arc<MemoryJournal>,
    content_verifier: Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>,
    publisher: Arc<MemoryContentPublisher>,
    host: Arc<MemoryHost>,
    volume: VolumeRef,
    read_capability: String,
    write_capability: String,
    scope: Scope,
    issuer: AuthorityIssuer,
    stream: StreamClient<MemoryStream>,
    conversation: Authority,
    maximum_file_bytes: u64,
    memory_store: Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
}

/// Ready-to-run local Harness with an isolated conversation and private volume.
pub struct LocalHarness {
    storage: MemoryHarnessStorage,
    bundle: crate::bundle::HarnessBundle,
}

impl LocalHarness {
    /// Starts a fresh local agent with the standard bounded runtime defaults.
    pub async fn new(model: Model, provider: Arc<dyn ModelProvider>) -> Result<Self> {
        Self::with_limits(
            AgentId::new(),
            Limits::default(),
            model,
            provider,
            crate::context::CompactionPolicy::default(),
        )
        .await
    }

    /// Starts a named local agent with explicit limits and compaction policy.
    pub async fn with_limits(
        agent: AgentId,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        compaction: crate::context::CompactionPolicy,
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
                "tool:call:acyclic.write_file".into(),
                "tool:call:acyclic.edit_file".into(),
                "tool:call:acyclic.list_files".into(),
            ],
            compaction,
        )
    }

    /// Starts a local agent with an explicitly selected, versioned tool set and matching capability grants and compaction policy.
    pub async fn with_tools(
        agent: AgentId,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        tools: ToolRegistry,
        capabilities: impl IntoIterator<Item = String>,
        compaction: crate::context::CompactionPolicy,
    ) -> Result<Self> {
        limits.validate()?;
        let storage = MemoryHarnessStorage::new(agent, limits.file_bytes).await?;
        Self::from_storage(
            storage,
            limits,
            model,
            provider,
            tools,
            capabilities,
            compaction,
        )
    }

    fn from_storage(
        storage: MemoryHarnessStorage,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        tools: ToolRegistry,
        capabilities: impl IntoIterator<Item = String>,
        compaction: crate::context::CompactionPolicy,
    ) -> Result<Self> {
        let mut builder = storage
            .builder()
            .model(model, provider)
            .grant(capability::MODEL_GENERATE)
            .tools(tools)
            .compaction(compaction)
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
    pub fn bundle(&self) -> &crate::bundle::HarnessBundle {
        &self.bundle
    }
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
    observed: tokio::sync::Mutex<BTreeMap<OperationId, ListFilesObservation>>,
}

struct ListFilesObservation {
    admission_digest: [u8; 32],
    result: ToolResult,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(serde::Serialize)]
struct ListFilesPageWire<'a> {
    generation: &'a GenerationRef,
    entries: ListFilesEntries<'a>,
    has_more: bool,
    next_after: Option<&'a str>,
}

struct ListFilesEntries<'a>(&'a [acyclic_fs::WorkspaceDirectoryEntry]);

impl serde::Serialize for ListFilesEntries<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeSeq};
        #[derive(serde::Serialize)]
        struct Entry<'a> {
            name: &'a str,
            kind: &'static str,
        }
        let mut entries = serializer.serialize_seq(Some(self.0.len()))?;
        for entry in self.0 {
            let name = std::str::from_utf8(entry.name.as_bytes())
                .map_err(|_| S::Error::custom("local file name is not UTF-8"))?;
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
            entries.serialize_element(&Entry { name, kind })?;
        }
        entries.end()
    }
}

fn list_files_page_wire<'a>(
    generation: &'a GenerationRef,
    page: &'a WorkspaceDirectoryPage,
) -> Result<ListFilesPageWire<'a>> {
    for entry in &page.entries {
        if entry.name.encoding() != NameEncoding::Utf8
            || std::str::from_utf8(entry.name.as_bytes()).is_err()
        {
            #[cfg(not(target_arch = "wasm32"))]
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "local file name is not UTF-8",
            });
            #[cfg(target_arch = "wasm32")]
            return Err(Error::Unsupported("local file name is not UTF-8".into()));
        }
    }
    Ok(ListFilesPageWire {
        generation,
        entries: ListFilesEntries(&page.entries),
        has_more: page.has_more,
        next_after: page.entries.last()
            .and_then(|entry| std::str::from_utf8(entry.name.as_bytes()).ok()),
    })
}

#[cfg(not(target_arch = "wasm32"))]
impl super::UploadRecord for ListFilesInput {
    fn diagnostic_label_bytes() -> u64 { crate::tool::tool_serde_diagnostic_label_bytes() }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = 0_u64;
        for size in [
            self.path.capacity() as u64,
            self.after.as_ref().map_or(0, |after| after.capacity() as u64),
            self.expected_generation.as_ref()
                .map(GenerationRef::logical_owned_bytes)
                .transpose()?.unwrap_or(0),
        ] {
            bytes = bytes.checked_add(size).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "list_files input ownership overflow",
            })?;
        }
        Ok(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeListFilesInput {
    input: ListFilesInput,
    _custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeListFilesPage {
    generation: GenerationRef,
    page: WorkspaceDirectoryPage,
    _custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeListFilesResult {
    result: ToolResult,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl LocalListFilesTool {
    fn native_admission_digest(
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<[u8; 32]> {
        let io = super::PreparationIo::from_native_work(work)?;
        let encoded_hold;
        let encoded = io.encode_json(invocation, io.remaining()?.bytes_encoded, true)?;
        encoded_hold = io.guard_bytes(&encoded)?;
        let charge = acyclic_fs::WorkCounters {
            bytes_hashed: encoded.len() as u64,
            ..Default::default()
        };
        io.preflight(charge)?;
        let digest = *blake3::hash(&encoded).as_bytes();
        io.charge(charge)?;
        drop(encoded);
        drop(encoded_hold);
        Ok(digest)
    }

    fn native_result_copy(
        result: &ToolResult,
        maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeListFilesResult> {
        let (value, custody) = crate::tool::clone_value_with_native_work_bounded(
            &result.value, maximum_bytes, work,
        )?;
        Ok(NativeListFilesResult { result: ToolResult { value }, custody })
    }

    async fn native_observe(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<NativeListFilesResult>> {
        let digest = Self::native_admission_digest(invocation, work)?;
        let observed = self.observed.lock().await;
        match observed.get(&invocation.operation_id) {
            Some(original) if original.admission_digest == digest =>
                Self::native_result_copy(&original.result,
                    self.maximum_bytes.min(context.effective_scope().limits().render_bytes),
                    work).map(Some),
            Some(_) => Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "list_files operation identity changed",
            }),
            None => Ok(None),
        }
    }

    async fn native_execute(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeListFilesResult> {
        let digest = Self::native_admission_digest(invocation, work)?;
        let limits = context.effective_scope().limits();
        let maximum_bytes = self.maximum_bytes.min(limits.render_bytes);
        let mut observed = self.observed.lock().await;
        if let Some(original) = observed.get(&invocation.operation_id) {
            return if original.admission_digest == digest {
                Self::native_result_copy(&original.result, maximum_bytes, work)
            } else {
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "list_files operation identity changed",
                })
            };
        }
        let (input, custody) = crate::tool::decode_with_native_work(&invocation.arguments, work)?;
        let input = NativeListFilesInput { input, _custody: custody };
        if input.input.maximum_entries == 0
            || (input.input.after.is_some() && input.input.expected_generation.is_none())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "list_files requires a bounded, generation-pinned page",
            });
        }
        let ((generation, page), custody) = self.verifier
            .list_private_directory_with_native_work(
                context.task(), &self.volume, "", &input.input.path,
                input.input.expected_generation.as_ref(), input.input.after.as_deref(),
                input.input.maximum_entries, work,
            )?.await?;
        let page = NativeListFilesPage { generation, page, _custody: custody };
        let wire = list_files_page_wire(&page.generation, &page.page)?;
        let (value, custody) = crate::tool::value_with_native_work_bounded(
            &wire, maximum_bytes, work,
        )?;
        let output = NativeListFilesResult { result: ToolResult { value }, custody };
        let mut cached = Self::native_result_copy(&output.result, maximum_bytes, work)?;
        let slot_bytes = (std::mem::size_of::<(OperationId, ListFilesObservation)>()
            - std::mem::size_of::<Value>()) as u64;
        let retained = cached.custody.bytes().checked_add(slot_bytes)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "list_files cache ownership overflow",
            })?;
        cached.custody.resize_retained(retained)
            .map_err(Error::from)?;
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1, ..Default::default()
        }).map_err(Error::from)?;
        observed.insert(invocation.operation_id, ListFilesObservation {
            admission_digest: digest,
            result: cached.result,
            _custody: Some(cached.custody),
        });
        Ok(output)
    }
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
            let admission_digest = crate::contract::canonical_json_digest(&invocation)?;
            if let Some(original) = observed.get(&invocation.operation_id) {
                return if original.admission_digest == admission_digest {
                    Ok(original.result.clone())
                } else {
                    Err(Error::Conflict(
                        "list_files operation identity changed".into(),
                    ))
                };
            }
            let input: ListFilesInput = serde_json::from_value(invocation.arguments.clone())
                .map_err(|error| Error::Invalid(format!("list_files input is invalid: {error}")))?;
            if input.maximum_entries == 0
                || (input.after.is_some() && input.expected_generation.is_none())
            {
                return Err(Error::Invalid(
                    "list_files requires a bounded, generation-pinned page".into(),
                ));
            }
            let after = input
                .after
                .map(|name| {
                    LogicalName::new(NameEncoding::Utf8, name.into_bytes(), u32::MAX)
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
            let wire = list_files_page_wire(&generation, &page)?;
            let value = serde_json::to_value(wire)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if serde_json::to_vec(&value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .len() as u64
                > self.maximum_bytes
            {
                return Err(Error::Invalid("directory page exceeds render limit".into()));
            }
            let result = ToolResult { value };
            observed.insert(invocation.operation_id, ListFilesObservation {
                admission_digest,
                result: result.clone(),
                #[cfg(not(target_arch = "wasm32"))]
                _custody: None,
            });
            Ok(result)
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let observed = self.observed.lock().await;
            let admission_digest = crate::contract::canonical_json_digest(&invocation)?;
            match observed.get(&invocation.operation_id) {
                Some(original) if original.admission_digest == admission_digest =>
                    Ok(Some(original.result.clone())),
                Some(_) => Err(Error::Conflict(
                    "list_files operation identity changed".into(),
                )),
                None => Ok(None),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.verifier.supports_native_directory_list(&self.volume, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn authorize_with_native_context(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.supports_native_execution(work)?;
        if context.operation_id() != invocation.operation_id || context.call_id() != invocation.call_id {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "list_files tool context identity mismatch",
            });
        }
        let maximum_path_bytes = context.effective_scope().limits().path_bytes;
        for key in ["path", "after"] {
            if invocation.arguments.get(key).and_then(Value::as_str)
                .is_some_and(|path| path.len() > maximum_path_bytes)
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "list_files path exceeds the effective scope path allowance",
                });
            }
        }
        let io = super::PreparationIo::from_native_work(work)?;
        if !io.contains_capability(context.effective_scope().grants(), &self.volume, "volume:read:")? {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "scope lacks the original volume read capability",
            });
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a crate::runtime::ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let output = self.native_execute(context, invocation, work).await?;
            Ok((output.result, output.custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a crate::runtime::ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>> {
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            match self.native_observe(context, invocation, work).await? {
                Some(mut output) => {
                    let retained = output.custody.bytes().checked_add(
                        (std::mem::size_of::<Option<ToolResult>>()
                            - std::mem::size_of::<ToolResult>()) as u64,
                    ).ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "list_files observation ownership overflow",
                    })?;
                    output.custody.resize_retained(retained)
                        .map_err(Error::from)?;
                    Ok((Some(output.result), output.custody))
                }
                None => Ok((None, work.retain_owned(std::mem::size_of::<Option<ToolResult>>() as u64)
                    .map_err(Error::from)?)),
            }
        })
    }
}

impl ToolProjection for LocalListFilesTool {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        Ok(serde_json::json!({"kind":"json","value":result.value}))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_projection(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining().map_err(Error::from)?;
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn project_with_native_work(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        #[derive(serde::Serialize)]
        struct Projection<'a> {
            kind: &'static str,
            value: &'a Value,
        }
        self.supports_native_projection(work)?;
        crate::tool::value_with_native_work(&Projection { kind: "json", value: &result.value }, work)
    }
}

struct MemoryContentPublisher {
    files: FilesystemContentPublisher<MemoryAuthorityBackend, MemoryObjectBackend>,
    memory_store: Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
}

impl ContentPublisher for MemoryContentPublisher {
    fn volume(&self) -> &VolumeRef {
        self.files.volume()
    }

    fn stage_once<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            let persisted = self
                .files
                .stage_once(operation_id, path, bytes, media_type, display_name)
                .await?;
            self.retain(persisted, bytes)
        })
    }

    fn observe_staged_once<'a>(
        &'a self, operation_id: OperationId, maximum_bytes: u64,
    ) -> BoxFuture<'a, Result<Option<FileRef>>> {
        self.files.observe_staged_once(operation_id, maximum_bytes)
    }

    fn observe_staged_once_measured<'a>(
        &'a self, operation_id: OperationId, maximum_bytes: u64,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error>> {
        self.files.observe_staged_once_measured(operation_id, maximum_bytes, budget, cancellation)
    }

    #[allow(clippy::too_many_arguments, reason = "same immutable upload identity")]
    fn stage_once_measured<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error>> {
        Box::pin(async move {
            let io = super::PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                let persisted = self.files.stage_once_with_io(
                    operation_id, path, bytes, media_type, display_name, Some(&io), None, None).await?;
                self.retain_with_io(&persisted, bytes, &io)?;
                Ok(persisted)
            }.await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_stage(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.files.supports_native_stage(work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stage_once_with_native_work<'a>(
        &'a self, operation_id: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            let result: Result<FileRef> = async {
                let persisted = self.files.stage_once_with_io(
                    operation_id, path, bytes, media_type, display_name, Some(&io), None, None).await?;
                self.retain_with_io(&persisted, bytes, &io)?;
                Ok(persisted)
            }.await;
            let persisted = result?;
            let custody = io.into_native_custody(super::UploadRecord::logical_owned_bytes(&persisted)?)?;
            Ok((persisted, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stage_at_with_native_work<'a>(
        &'a self, operation_id: OperationId, source: &'a FileRef, bytes: &'a [u8],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = super::PreparationIo::from_native_work(work)?;
            let persisted = self.files.stage_at_with_io(operation_id, source, bytes, Some(&io), None, None).await?;
            self.retain_with_io(&persisted, bytes, &io)?;
            let custody = io.into_native_custody(super::UploadRecord::logical_owned_bytes(&persisted)?)?;
            Ok((persisted, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observe_staged_once_with_native_work<'a>(
        &'a self, operation_id: OperationId, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>> {
        self.files.observe_staged_once_with_native_work(operation_id, maximum_bytes, work)
    }

    fn stage_at<'a>(
        &'a self,
        operation_id: OperationId,
        source: &'a FileRef,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            let persisted = self.files.stage_at(operation_id, source, bytes).await?;
            self.retain(persisted, bytes)
        })
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
            let persisted = self
                .files
                .stage(operation_id, path, bytes, media_type, display_name)
                .await?;
            self.retain(persisted, bytes)
        })
    }
}

impl MemoryContentPublisher {
    fn retain_with_io(
        &self, persisted: &FileRef, bytes: &[u8], io: &super::PreparationIo,
    ) -> Result<()> {
        self.memory_store.lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?
            .retain_reference_with_io(persisted, bytes, true, io)?;
        io.remaining()?;
        Ok(())
    }

    fn retain(&self, persisted: FileRef, bytes: &[u8]) -> Result<FileRef> {
        self.memory_store.lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?
            .retain_reference(&persisted, bytes, true)?;
        Ok(persisted)
    }
}

impl MemoryHarnessStorage {
    /// Assembles portable read/write/edit tools and this volume's directory tool.
    pub fn default_tools(&self, limits: Limits) -> Result<ToolRegistry> {
        limits.validate()?;
        if limits.file_bytes > self.maximum_file_bytes {
            return Err(Error::Invalid(
                "tool file limit exceeds the bound storage volume".into(),
            ));
        }
        let mut tools = ToolRegistry::new();
        tools.register(crate::tool::files::read_file()?)?;
        tools.register(crate::tool::files::write_file()?)?;
        tools.register(crate::tool::files::edit_file()?)?;
        tools.register(self.list_files_tool(limits))?;
        Ok(tools)
    }

    fn list_files_tool(&self, limits: Limits) -> Tool {
        let implementation = Arc::new(LocalListFilesTool {
            verifier: self.content_verifier.clone(),
            volume: self.volume.clone(),
            maximum_bytes: limits.render_bytes,
            observed: tokio::sync::Mutex::new(BTreeMap::new()),
        });
        Tool {
            definition: ToolDefinition {
                name: "acyclic.list_files".into(),
                revision: "2".into(),
                description: "List one bounded, generation-pinned page of the agent-private volume"
                    .into(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "expected_generation": {"type": ["object", "null"]},
                        "after": {"type": ["string", "null"]},
                        "maximum_entries": {"type": "integer", "minimum": 1, "maximum": u32::MAX}
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
                projection_schema: crate::tool::json_projection_schema(json!({
                    "type": "object",
                    "properties": {
                        "generation": {"type": "object"},
                        "entries": {"type": "array", "items": {"type": "object"}},
                        "has_more": {"type": "boolean"},
                        "next_after": {"type": ["string", "null"]}
                    },
                    "required": ["generation", "entries", "has_more", "next_after"],
                    "additionalProperties": false
                })),
            },
            executor: implementation.clone(),
            projection: implementation,
        }
    }

    /// Creates an agent-owned private volume and a bound conversation before any turn or interaction can publish a reference.
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
        let memory_store = new_memory_store(&volume, maximum_file_bytes)?;
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
                capability::CONVERSATION_BIND.to_owned(),
                capability::CONVERSATION_APPEND.to_owned(),
                capability::CONVERSATION_SELECT_CONTEXT.to_owned(),
                capability::INTERACTION_OPEN.to_owned(),
                read_capability.clone(),
                write_capability.clone(),
            ]),
        );
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
                issuer.verifier(), crate::core::SchemaRegistry::new(),
                scope.clone(),
                maximum_file_bytes,
            )?
            .with_input_verifier(input.clone()),
        );
        let publisher = Arc::new(MemoryContentPublisher {
            files: FilesystemContentPublisher::new(
                Arc::clone(&host),
                volume.clone(),
                &issuer.verifier(),
                &scope,
                maximum_file_bytes,
            )?,
            memory_store: memory_store.clone(),
        });
        Ok(Self {
            journal,
            content_verifier: input,
            publisher,
            host,
            volume,
            read_capability,
            write_capability,
            scope,
            issuer,
            stream,
            conversation,
            maximum_file_bytes,
            memory_store,
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
    pub fn builder(&self) -> crate::bundle::HarnessBuilder {
        crate::bundle::HarnessBuilder::new()
            .journal(self.journal())
            .content(ContentBindings {
                reader: self.content_verifier.clone(),
                writer: Some(self.publisher.clone()),
            })
            .grant(self.read_capability.clone())
            .grant(self.write_capability.clone())
    }

    /// Runs the shared canonical loop with this memory-backed journal.
    pub async fn run_conversation(
        &self,
        bundle: &crate::bundle::HarnessBundle,
        operation_id: OperationId,
        content: FileRef,
        attachments: Vec<Attachment>,
        max_steps: u32,
    ) -> Result<TurnOutput> {
        self.journal.run_conversation(bundle, operation_id, content, attachments, max_steps).await
    }

    /// Convenience over the canonical ref-only turn API for local text input.
    pub async fn run_prompt(
        &self,
        bundle: &crate::bundle::HarnessBundle,
        prompt: &str,
    ) -> Result<TurnOutput> {
        self.run_prompt_with_attachments(bundle, prompt, Vec::new())
            .await
    }

    /// Text convenience with ordered staged file refs; canonical history still contains only refs and the complete list is manifest-backed if needed.
    pub async fn run_prompt_with_attachments(
        &self,
        bundle: &crate::bundle::HarnessBundle,
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

    #[cfg(test)]
    async fn open_conversation(
        &self,
        limits: Limits,
    ) -> Result<tokio::sync::MappedMutexGuard<'_, StreamAggregate<MemoryStream>>> {
        self.journal.conversation_projection(limits).await
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
    pub async fn read(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        if let Ok(bytes) = self
            .memory_store
            .lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?
            .read(file)
        {
            return Ok(bytes.into());
        }
        self.content_verifier.read(file).await
    }

    /// Lazily discovers this agent's private files under its existing signed volume-read grant.
    pub async fn list_private_directory(
        &self,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        let page = self
            .content_verifier
            .list_private_directory(
                &self.volume,
                "",
                path,
                expected_generation,
                after,
                maximum_entries,
            )
            .await?;
        let after_text = after.and_then(LogicalName::unicode_text);
        let core = self
            .memory_store
            .lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?;
        let generation = core.generation();
        let _shared_page = core.list(
            path,
            Some(generation),
            after_text.as_deref(),
            maximum_entries as usize,
        )?;
        Ok(page)
    }

    /// Publishes one owner-mediated exact-file read scope to another agent, independent of any fork relationship.
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
    pub async fn read_delegated(&self, scope: &Scope, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        self.delegated_reader(scope)?.read(file).await
    }

    /// Shares a private subtree with a named agent.
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

    /// Reads a named file from a delegated private directory and pins its observed version for later exact-reference use.
    pub async fn read_delegated_path(
        &self,
        scope: &Scope,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let resolved = self
            .delegated_reader(scope)?
            .read_private_path(&self.volume, granted_prefix, path, expected_generation)
            .await?;
        let core = self
            .memory_store
            .lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?;
        let shared = core.read_path(path, None);
        match shared {
            Ok(reference) if reference == resolved.0 => {
                let bytes = core.read(&reference)?;
                Ok((reference, bytes.into()))
            }
            _ => Ok(resolved),
        }
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

    /// Resolves an addressable local interaction under a host-issued exact responder grant.
    pub async fn resolve_interaction(
        &self,
        id: InteractionId,
        response: InteractionResponse,
    ) -> Result<InteractionOutcome> {
        let responder = self.issuer.root(
            format!("local-responder:{id}"),
            Capabilities::new([
                capability::INTERACTION_RESOLVE.to_owned(),
                capability::interaction_respond(id),
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
    pub fn verifier(&self) -> crate::core::AuthorityVerifier {
        self.issuer.verifier()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::ContentGrant;
    use crate::conversation::ContentGrant;
    use crate::{
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

    fn finite_content_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            authority_records_read: 100_000, authority_records_appended: 100_000,
            authority_bytes_read: 16_000_000, authority_bytes_written: 16_000_000,
            object_probes: 100_000, backend_read_operations: 100_000, backend_write_operations: 100_000,
            durability_operations: 100_000, page_reads: 100_000, page_writes: 100_000,
            object_bytes_read: 16_000_000, object_bytes_written: 16_000_000,
            bytes_hashed: 16_000_000, bytes_copied: 16_000_000, bytes_encoded: 16_000_000,
            source_bytes_read: 16_000_000, source_path_components: 100_000, source_entries_visited: 100_000,
            output_bytes: 16_000_000, items_examined: 100_000, items_returned: 100_000,
            allocation_operations: 100_000, peak_allocation_bytes: 16_000_000, materializations: 100_000,
        }
    }

    #[tokio::test]
    async fn stock_publisher_finite_once_observes_original_metadata_after_head_changes() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
        let operation = OperationId::new();
        let cancellation = acyclic_fs::CancellationToken::new();
        let committed = storage.publisher.stage_once_measured(
            operation, "once.json", b"original", "application/json", "original.json",
            finite_content_budget(), &cancellation,
        ).await.map_err(|failure| failure.error)?;
        assert!(committed.work.backend_write_operations > 0);
        assert!(storage.memory_store.lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?
            .has(&committed.value)?);
        storage.publisher.stage_once(
            OperationId::new(), "once.json", b"later", "text/plain", "later.txt",
        ).await?;
        let recovered = storage.publisher.observe_staged_once_measured(
            operation, 1024, finite_content_budget(), &cancellation,
        ).await.map_err(|failure| failure.error)?;
        assert_eq!(recovered.value.as_ref(), Some(&committed.value));
        assert_eq!(storage.content_verifier.read_measured(
            &committed.value, 1024, finite_content_budget(), &cancellation,
        ).await.map_err(|failure| failure.error)?.value, b"original");
        assert!(storage.publisher.stage_once_measured(
            operation, "once.json", b"original", "text/plain", "changed.txt",
            finite_content_budget(), &cancellation,
        ).await.is_err());
        let cancelled = acyclic_fs::CancellationToken::new();
        cancelled.cancel();
        let failure = storage.publisher.stage_once_measured(
            OperationId::new(), "cancelled", b"never", "text/plain", "never",
            finite_content_budget(), &cancelled,
        ).await.unwrap_err();
        assert_eq!(*failure.work, acyclic_fs::WorkCounters::default());
        let denied_operation = OperationId::new();
        let mut denied = finite_content_budget();
        denied.peak_allocation_bytes = 0;
        assert!(storage.publisher.stage_once_measured(
            denied_operation, "denied", b"never", "text/plain", "never", denied, &cancellation,
        ).await.is_err());
        assert!(storage.publisher.observe_staged_once_measured(
            denied_operation, 1024, finite_content_budget(), &cancellation,
        ).await.map_err(|failure| failure.error)?.value.is_none());
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn stock_native_content_keeps_original_ledger_and_output_custody()
        -> Result<()>
    {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            finite_content_budget(), &cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let constructor_allocations = work.receipt().allocation_operations;
        let baseline = work.remaining().map_err(Error::from)?.peak_allocation_bytes;
        storage.publisher.supports_native_stage(&work)?;
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.remaining().map_err(Error::from)?
            .peak_allocation_bytes, baseline);
        struct OrdinaryOnlyPublisher {
            volume: VolumeRef,
            effects: Arc<AtomicUsize>,
        }
        impl ContentPublisher for OrdinaryOnlyPublisher {
            fn volume(&self) -> &VolumeRef { &self.volume }
            fn stage<'a>(
                &'a self, _: OperationId, _: &'a str, _: &'a [u8],
                _: &'a str, _: &'a str,
            ) -> BoxFuture<'a, Result<FileRef>> {
                self.effects.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(Error::Unsupported("ordinary publisher was invoked".into())) })
            }
        }
        let ordinary = OrdinaryOnlyPublisher {
            volume: storage.volume.clone(), effects: Arc::new(AtomicUsize::new(0)),
        };
        assert!(matches!(ordinary.supports_native_stage(&work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(ordinary.effects.load(Ordering::SeqCst), 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert!(matches!(ordinary.stage_once_with_native_work(
            OperationId::new(), "unsupported", b"never", "text/plain", "never", &work,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, .. })));
        assert_eq!(ordinary.effects.load(Ordering::SeqCst), 0);
        let mut frame_budget = finite_content_budget();
        frame_budget.allocation_operations = constructor_allocations;
        let frame_work = acyclic_fs::NativeMaterializationWork::new(
            frame_budget, &acyclic_fs::CancellationToken::new(),
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(matches!(storage.publisher.stage_once_with_native_work(
            OperationId::new(), "frame-denied", b"never", "text/plain", "never", &frame_work,
        ), Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(_), custody: None })));
        assert_eq!(frame_work.receipt().allocation_operations, constructor_allocations);
        assert_eq!(frame_work.receipt().bytes_copied, 0);
        assert_eq!(frame_work.receipt().backend_read_operations, 0);
        assert_eq!(frame_work.receipt().backend_write_operations, 0);
        let operation = OperationId::new();
        let (file, file_hold) = storage.publisher.stage_once_with_native_work(
            operation, "native.json", b"original", "application/json", "original.json", &work,
        )?.await?;
        assert_eq!(file_hold.bytes(), file.logical_owned_bytes()?);
        assert_eq!(work.remaining().map_err(Error::from)?.peak_allocation_bytes, baseline - file_hold.bytes());
        assert!(work.receipt().backend_write_operations > 0);
        let staged_allowance = work.remaining().map_err(Error::from)?.peak_allocation_bytes;
        let (bytes, bytes_hold) = storage.content_verifier.read_with_native_work(&file, 1024, &work)?.await?;
        assert_eq!(bytes, b"original");
        assert_eq!(bytes_hold.bytes(), bytes.capacity() as u64);
        assert_eq!(work.remaining().map_err(Error::from)?.peak_allocation_bytes, staged_allowance - bytes_hold.bytes());
        assert!(work.receipt().backend_read_operations > 0);
        drop(bytes);
        drop(bytes_hold);
        assert_eq!(work.remaining().map_err(Error::from)?.peak_allocation_bytes, staged_allowance);
        assert!(storage.publisher.stage_once_with_native_work(
            operation, "native.json", b"original", "text/plain", "changed", &work,
        )?.await.is_err());
        drop(file);
        drop(file_hold);
        assert_eq!(work.remaining().map_err(Error::from)?.peak_allocation_bytes, baseline);

        cancellation.cancel();
        assert!(matches!(storage.publisher.stage_once_with_native_work(
            OperationId::new(), "cancelled", b"never", "text/plain", "never", &work,
        ), Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })));
        let mut denied = finite_content_budget();
        denied.backend_write_operations = 0;
        let denied_work = acyclic_fs::NativeMaterializationWork::new(
            denied, &acyclic_fs::CancellationToken::new()).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(matches!(storage.publisher.stage_once_with_native_work(
            OperationId::new(), "denied", b"never", "text/plain", "never", &denied_work,
        )?.await, Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(_), custody: None })));
        assert_eq!(denied_work.receipt().backend_write_operations, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn scheduler_native_stage_preserves_original_key_receipt_and_finite_hash_guard()
        -> Result<()>
    {
        use crate::distributed::SchedulerPayloadStore as _;
        let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
        let scheduler = super::super::FilesystemSchedulerPayloadStore::new(
            storage.host.clone(), storage.volume.clone(), &storage.issuer.verifier(),
            &storage.scope, 1024)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            finite_content_budget(), &cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let operation = OperationId::new();
        let (file, hold) = scheduler.stage_with_native_work(
            operation, "canonical-original-key", br#"{"input":1}"#, &work)?.await?;
        assert_eq!(hold.bytes(), file.logical_owned_bytes()?);
        let (retry, retry_hold) = scheduler.stage_with_native_work(
            operation, "canonical-original-key", br#"{"input":1}"#, &work)?.await?;
        assert_eq!(retry, file);
        assert_eq!(scheduler.stage(operation, "canonical-original-key", br#"{"input":1}"#).await?, file);
        assert!(scheduler.stage_with_native_work(
            operation, "canonical-original-key", br#"{"input":2}"#, &work)?.await.is_err());
        drop(retry);
        drop(retry_hold);
        drop(file);
        drop(hold);
        let mut denied = finite_content_budget();
        denied.bytes_hashed = "canonical-original-key".len() as u64 - 1;
        let denied_work = acyclic_fs::NativeMaterializationWork::new(
            denied, &cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(matches!(scheduler.stage_with_native_work(
            OperationId::new(), "canonical-original-key", b"never", &denied_work)?.await,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(_), custody: None })));
        assert_eq!(denied_work.receipt().bytes_hashed, 0);
        assert_eq!(denied_work.receipt().backend_write_operations, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_host_original_key_recovers_immutable_upload_after_head_edit() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 1024).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(finite_content_budget(), &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let key = IdempotencyKey::new("acyclic.pi/input/root-task/original-operation/label")?;
        let mut path_budget = finite_content_budget();
        path_budget.allocation_operations = work.receipt().allocation_operations;
        let path_work = acyclic_fs::NativeMaterializationWork::new(path_budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let path_prefix = path_work.receipt();
        assert!(matches!(storage.host.put_content_with_native_work(
            &storage.volume, &storage.write, "../outside", b"never",
            "text/plain", "never", 1024, &key, &path_work,
        ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert!(matches!(storage.host.content_for_operation_with_native_work(
            &storage.volume, &storage.write, &key, ".system/private", 1024, &path_work,
        ).await, Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert_eq!(path_work.receipt(), path_prefix);
        let staged = storage.host.put_content_with_native_work(
            &storage.volume, &storage.write, "inputs/original.json", b"original",
            "application/json", "original.json", 1024, &key, &work,
        ).await?;
        assert_eq!(staged.1.bytes(), staged.0.logical_owned_bytes()?);
        storage.host.put_content(
            &storage.volume, &storage.write, "inputs/original.json", b"later",
            "text/plain", "later.txt", 1024, &IdempotencyKey::new("head-edit")?,
        ).await?;
        let recovered = storage.host.content_for_operation_with_native_work(
            &storage.volume, &storage.write, &key, "inputs/original.json", 1024, &work,
        ).await?.ok_or_else(|| Error::NotFound("original native operation receipt".into()))?;
        assert_eq!(recovered.0, staged.0);
        assert_eq!(recovered.1.bytes(), recovered.0.logical_owned_bytes()?);
        let content = storage.content_verifier.read_with_native_work(&recovered.0, 1024, &work)?.await?;
        assert_eq!(content.0, b"original");
        drop(content);
        let writes = work.receipt().backend_write_operations;
        assert!(storage.host.put_content_with_native_work(
            &storage.volume, &storage.write, "inputs/original.json", b"original",
            "text/plain", "changed.txt", 1024, &key, &work,
        ).await.is_err());
        assert_eq!(work.receipt().backend_write_operations, writes);
        assert!(storage.host.content_for_operation_with_native_work(
            &storage.volume, &storage.write, &key, "inputs/changed.json", 1024, &work,
        ).await.is_err());
        assert!(storage.host.content_for_operation_with_native_work(
            &storage.volume, &storage.write, &IdempotencyKey::new("proven-absent-operation")?,
            "inputs/original.json", 1024, &work,
        ).await?.is_none());
        drop(recovered);
        drop(staged);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let cancelled = acyclic_fs::CancellationToken::new();
        let cancelled_work = acyclic_fs::NativeMaterializationWork::new(finite_content_budget(), &cancelled)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let prefix = cancelled_work.receipt();
        cancelled.cancel();
        assert!(matches!(storage.host.content_for_operation_with_native_work(
            &storage.volume, &storage.write, &key, "inputs/original.json", 1024, &cancelled_work,
        ).await, Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(cancelled_work.receipt(), prefix);
        Ok(())
    }


    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn failed_native_custody_transfer_keeps_existing_guard_until_value_drops() -> Result<()> {
        let work = acyclic_fs::NativeMaterializationWork::new(
            finite_content_budget(), &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = super::super::PreparationIo::from_native_work(&work)?;
        let bytes = io.encode_json(&serde_json::Value::Null, 1024, true)?;
        let held = io.owned_bytes()?;
        let live_held = work.live_allocation_bytes() - baseline;
        let error = match io.into_native_custody(held + 1) {
            Err(error) => error,
            Ok(_) => panic!("unadmitted extra custody must fail"),
        };
        let failure = (bytes, error);
        assert!(matches!(&failure.1, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(_), custody: Some(_),
        }));
        assert_eq!(work.live_allocation_bytes(), baseline + live_held);
        let empty_alias = failure.0.slice(0..0);
        drop(failure);
        assert!(work.live_allocation_bytes() > baseline);
        drop(empty_alias);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn rejected_foreign_native_custody_preserves_original_owned_data_guard() -> Result<()> {
        let token = acyclic_fs::CancellationToken::new();
        let receiving = acyclic_fs::NativeMaterializationWork::new(finite_content_budget(), &token)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let producing = acyclic_fs::NativeMaterializationWork::new(finite_content_budget(), &token)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let receiver_io = super::super::PreparationIo::from_native_work(&receiving)?;
        let receiver_baseline = receiving.live_allocation_bytes();
        let producer_baseline = producing.live_allocation_bytes();
        let producer_io = super::super::PreparationIo::from_native_work(&producing)?;
        let bytes = producer_io.encode_json(&serde_json::Value::Null, 1024, true)?;
        drop(producer_io);
        let (metadata, incoming) = producing.display("source carrier metadata")
            .map_err(Error::from)?;
        let held = producing.live_allocation_bytes() - producer_baseline;
        let error = match receiver_io.absorb_native_custody(incoming) {
            Err(error) => error,
            Ok(()) => panic!("distinct original allocation ledgers must not merge"),
        };
        let failure = (bytes, metadata, error);
        assert!(matches!(&failure.2, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: Some(_),
        }));
        assert_eq!(receiving.live_allocation_bytes(), receiver_baseline);
        assert_eq!(producing.live_allocation_bytes(), producer_baseline + held);
        drop(failure);
        assert_eq!(producing.live_allocation_bytes(), producer_baseline);
        Ok(())
    }

    struct TextModel(Arc<Mutex<Vec<ModelRequest>>>);

    fn synthetic_token_count(
        request: &crate::model::PreparedModelRequest,
    ) -> Result<crate::context::ModelTokenCount> {
        let message_tokens = request
            .request()
            .messages
            .iter()
            .map(|message| {
                let bytes = message.content.file_refs().try_fold(
                    crate::contract::canonical_json_bytes(message)?.len() as u64,
                    |total, file| {
                        total
                            .checked_add(file.descriptor().byte_length())
                            .ok_or_else(|| Error::Invalid("synthetic token count overflows".into()))
                    },
                )?;
                u32::try_from(bytes)
                    .map_err(|_| Error::Invalid("synthetic token count exceeds u32".into()))
            })
            .collect::<Result<Vec<_>>>()?;
        let fixed = crate::contract::canonical_json_bytes(&request.request().tools)?.len();
        Ok(crate::context::ModelTokenCount {
            request_digest: request.manifest().request_digest,
            fixed_tokens: u32::try_from(fixed)
                .map_err(|_| Error::Invalid("synthetic framing exceeds u32".into()))?,
            message_tokens,
        })
    }

    impl ModelProvider for TextModel {
        fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
            Ok(crate::context::ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: 4_096,
            })
        }
        fn count_tokens(
            &self,
            request: &crate::model::PreparedModelRequest,
        ) -> Result<crate::context::ModelTokenCount> {
            synthetic_token_count(request)
        }
        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            _dispatch: crate::model::ModelDispatch,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request.request().clone());
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

    async fn run_admitted_file_case<F, Fut>(
        storage: &MemoryHarnessStorage,
        limits: Limits,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(crate::runtime::TaskContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        use crate::runtime::{Bindings, ContentBindings};
        let mut bindings = Bindings::local();
        bindings.scope = RuntimeScope::new(
            Capabilities::new([
                storage.read_capability.clone(),
                storage.write_capability.clone(),
                "task:spawn:file_tool_case@1".into(),
                "tool:call:acyclic.read_file".into(),
                "tool:call:acyclic.write_file".into(),
                "tool:call:acyclic.edit_file".into(),
                "tool:call:acyclic.list_files".into(),
            ]),
            limits,
        )?;
        bindings.content = Some(ContentBindings {
            reader: storage.content_verifier.clone(),
            writer: Some(storage.publisher.clone()),
        });
        bindings.tasks.register(TaskDefinition::live(
            "file_tool_case",
            "1",
            move |context, (): ()| handler(context),
        )?)?;
        let runtime = bindings.build()?;
        let task = runtime.task::<(), ()>("file_tool_case")?;
        assert_eq!(
            runtime.spawn(&task, ()).await?.result().await?,
            Outcome::Succeeded(())
        );
        Ok(())
    }

    struct ModelWithoutAccounting(TextModel);

    impl ModelProvider for ModelWithoutAccounting {
        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            dispatch: crate::model::ModelDispatch,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            self.0.generate(request, dispatch)
        }

        fn reconcile<'a>(
            &'a self,
            attempt: ModelAttempt,
        ) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
            self.0.reconcile(attempt)
        }
    }

    #[tokio::test]
    async fn local_convenience_consumers_replace_or_disable_compaction() -> Result<()> {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let policy =
            crate::context::CompactionPolicy::Threshold(crate::context::ThresholdCompaction {
                response_reserve_tokens: 1_024,
                recent_tokens: 2_048,
                ..crate::context::ThresholdCompaction::default()
            });
        let local = LocalHarness::with_limits(
            AgentId::new(),
            Limits::default(),
            Model::new("test", "text", "1", Value::Null)?,
            Arc::new(TextModel(requests.clone())),
            policy,
        )
        .await?;
        assert_eq!(local.run("replacement").await?.text, "local response");
        let disabled = LocalHarness::with_tools(
            AgentId::new(),
            Limits::default(),
            Model::new("test", "text", "1", Value::Null)?,
            Arc::new(ModelWithoutAccounting(TextModel(requests.clone()))),
            ToolRegistry::new(),
            Vec::<String>::new(),
            crate::context::CompactionPolicy::Disabled,
        )
        .await?;
        assert_eq!(disabled.run("disabled").await?.text, "local response");
        let requests = requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].max_output_tokens, Some(1_024));
        assert_eq!(requests[1].max_output_tokens, None);
        Ok(())
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
        let expected_projection = json!({"kind":"json","value":"pinned text"});
        let render_bytes = serde_json::to_vec(&expected_projection)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .len() as u64;
        let limits = Limits {
            file_bytes: 4_096,
            render_bytes,
            ..Limits::default()
        };
        let tool = owner
            .default_tools(limits)?
            .get("acyclic.read_file")
            .ok_or_else(|| Error::NotFound("read tool".into()))?
            .clone();
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "read-1".into(),
            name: "acyclic.read_file".into(),
            arguments: json!({"file":file}),
        };
        let own_tool = tool.clone();
        let own_invocation = invocation.clone();
        run_admitted_file_case(&owner, limits, move |task| {
            let tool = own_tool.clone();
            let invocation = own_invocation.clone();
            let expected_projection = expected_projection.clone();
            async move {
                let context = crate::runtime::ToolContext::new(
                    task.clone(),
                    invocation.operation_id,
                    &invocation.call_id,
                )?;
                let result = tool
                    .executor
                    .execute_with_context(context.clone(), invocation.clone())
                    .await?;
                assert_eq!(result.value, json!("pinned text"));
                let projection = tool.projection.project(&invocation, &result)?;
                assert_eq!(projection, expected_projection);
                let content = tool.definition.validate_projection(&projection)?;
                content.validate_limits(limits)?;
                assert!(matches!(
                    content.validate_limits(Limits {
                        render_bytes: render_bytes - 1,
                        ..limits
                    }),
                    Err(Error::Invalid(_))
                ));
                assert_eq!(
                    tool.executor
                        .reconcile_with_context(context, invocation.clone())
                        .await?,
                    Some(result)
                );
                let narrowed = task.scoped(
                    task.scope().grants().clone(),
                    Limits {
                        render_bytes: 4,
                        ..limits
                    },
                )?;
                assert!(matches!(
                    tool.executor
                        .execute_with_context(
                            crate::runtime::ToolContext::new(
                                narrowed,
                                invocation.operation_id,
                                &invocation.call_id
                            )?,
                            invocation
                        )
                        .await,
                    Err(Error::Invalid(_))
                ));
                Ok(())
            }
        })
        .await?;
        run_admitted_file_case(&other, limits, move |task| {
            let tool = tool.clone();
            let invocation = invocation.clone();
            async move {
                assert!(matches!(
                    tool.executor
                        .execute_with_context(
                            crate::runtime::ToolContext::new(
                                task,
                                invocation.operation_id,
                                &invocation.call_id
                            )?,
                            invocation
                        )
                        .await,
                    Err(Error::Unauthorized(_)) | Err(Error::NotFound(_))
                ));
                Ok(())
            }
        })
        .await
    }

    #[tokio::test]
    async fn default_write_reconciles_original_receipt_and_rejects_changed_retry() -> Result<()> {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 4_096).await?;
        let limits = Limits {
            file_bytes: 4_096,
            render_bytes: 4_096,
            ..Limits::default()
        };
        let tool = storage
            .default_tools(limits)?
            .get("acyclic.write_file")
            .ok_or_else(|| Error::NotFound("write tool".into()))?
            .clone();
        run_admitted_file_case(&storage, limits, move |task| {
            let tool = tool.clone();
            async move {
                let invocation = ToolInvocation {
                    operation_id: OperationId::new(), call_id: "write-1".into(), name: "acyclic.write_file".into(),
                    arguments: json!({"path":"notes/one.txt","text":"saved text","media_type":"text/plain","display_name":"one.txt"}),
                };
                let context = crate::runtime::ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
                let first = tool.executor.execute_with_context(context.clone(), invocation.clone()).await?;
                let file: FileRef = serde_json::from_value(first.value["file"].clone()).map_err(|error| Error::Invalid(error.to_string()))?;
                let user = task.stage_file(OperationId::new(), "notes/one.txt", b"later user edit", "text/plain", "one.txt").await?;
                assert_eq!(tool.executor.reconcile_with_context(context.clone(), invocation.clone()).await?, Some(first));
                assert_eq!(task.read_file(&file).await?.as_ref(), b"saved text".as_ref());
                assert_eq!(task.read_file(&user).await?.as_ref(), b"later user edit".as_ref());
                for (field, value) in [("text", "different text"), ("path", "notes/another.txt"), ("media_type", "text/markdown"), ("display_name", "other.txt")] {
                    let mut changed = invocation.clone(); changed.arguments[field] = json!(value);
                    assert!(matches!(tool.executor.reconcile_with_context(context.clone(), changed).await, Err(Error::Conflict(_))));
                }
                let ordinary = OperationId::new();
                for path in ["multi/a.txt", "multi/b.txt"] {
                    task.stage_file(ordinary, path, b"ordinary", "text/plain", "ordinary.txt").await?;
                }
                Ok(())
            }
        }).await
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
            arguments: json!({"path": "notes", "maximum_entries": 65}),
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
        let file = storage
            .stage(
                OperationId::new(),
                "notes/one.txt",
                b"one",
                "text/plain",
                "one.txt",
            )
            .await?;
        run_admitted_file_case(&storage, limits, move |task| {
            let tools = tools.clone(); let file = file.clone();
            async move {
                let task = task.scoped(Capabilities::new([
                    "tool:call:acyclic.read_file", "tool:call:acyclic.write_file", "tool:call:acyclic.edit_file", "tool:call:acyclic.list_files",
                ]), limits)?;
                for (name, arguments) in [
                    ("acyclic.read_file", json!({"file":file})),
                    ("acyclic.write_file", json!({"path":"notes/two.txt","text":"two","media_type":"text/plain","display_name":"two.txt"})),
                    ("acyclic.edit_file", json!({"file":file,"old_text":"one","new_text":"two"})),
                ] {
                    let invocation = ToolInvocation { operation_id: OperationId::new(), call_id: name.into(), name: name.into(), arguments };
                    let tool = tools.get(name).ok_or_else(|| Error::NotFound(name.into()))?;
                    assert!(matches!(tool.executor.execute_with_context(crate::runtime::ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?, invocation).await, Err(Error::Unauthorized(_))));
                }
                let invocation = ToolInvocation { operation_id: OperationId::new(), call_id: "list".into(), name: "acyclic.list_files".into(), arguments: json!({"path":"","maximum_entries":8}) };
                assert!(matches!(tools.get("acyclic.list_files").ok_or_else(|| Error::NotFound("list tool".into()))?.executor.authorize(Some(task.scope()), &invocation), Err(Error::Unauthorized(_))));
                Ok(())
            }
        }).await
    }

    struct ReadFileModel {
        file: Arc<Mutex<Option<FileRef>>>,
        calls: AtomicUsize,
    }

    impl ModelProvider for ReadFileModel {
        fn context_capacity(&self, _: &Model) -> Result<crate::context::ModelContextCapacity> {
            Ok(crate::context::ModelContextCapacity {
                context_tokens: 131_072,
                output_tokens: 4_096,
            })
        }
        fn count_tokens(
            &self,
            request: &crate::model::PreparedModelRequest,
        ) -> Result<crate::context::ModelTokenCount> {
            synthetic_token_count(request)
        }
        fn generate<'a>(
            &'a self,
            request: crate::model::PreparedModelRequest,
            _dispatch: crate::model::ModelDispatch,
        ) -> BoxStream<'a, Result<ModelEvent>> {
            let request = request.request();
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
                        crate::model::ModelContent::Part(
                            crate::model::ModelContentPart::ToolResult { .. }
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
            .compaction(crate::context::CompactionPolicy::Disabled)
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
        assert_eq!(body.as_ref(), b"private attachment body");
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
        let first_aggregate = storage.open_conversation(bundle.limits()).await?;
        let first_user = first_aggregate
            .reducer()
            .conversation()
            .and_then(|conversation| conversation.messages.first())
            .ok_or_else(|| Error::Invalid("conversation user message is missing".into()))?;
        assert_eq!(
            first_user.id,
            crate::turn::canonical_user_message_id(operation_id)
        );
        drop(first_aggregate);
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
        let mut aggregate = storage.open_conversation(bundle.limits()).await?;
        assert_eq!(
            aggregate
                .reducer()
                .conversation()
                .ok_or_else(|| crate::Error::Invalid("conversation not bound".into()))?
                .messages
                .len(),
            2
        );
        assert_eq!(aggregate.reducer().resident_context_selections().count(), 1);
        let expected_selection = aggregate
            .context_selection_for_operation(operation_id)
            .await?
            .ok_or_else(|| Error::Invalid("original context selection missing".into()))?;
        aggregate.set_resident_event_limit(1)?;
        assert_eq!(aggregate.reducer().resident_context_selections().count(), 0);
        assert!(
            aggregate
                .reducer()
                .context_selection_for_operation(operation_id)
                .is_none()
        );
        assert_eq!(
            aggregate
                .context_selection_for_operation(operation_id)
                .await?,
            Some(expected_selection)
        );
        drop(aggregate);
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
            .compaction(crate::context::CompactionPolicy::Disabled)
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

#[cfg(test)]
#[path = "memory_summary_tests.rs"]
mod summary_tests;
