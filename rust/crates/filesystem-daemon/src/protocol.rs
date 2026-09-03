//! Typed bounded daemon protocol and process-owned handle state.

use crate::mount_journal::MountJournal;
use acyclic_fs::kernel::{
    DecodeLimits, FileKind, FilePayload, FileRecord, LogicalName, NameEncoding, TreeEntry,
    decode_file_metadata,
};
use acyclic_fs::model::VolumeLimits;
use acyclic_fs::{
    ByteRange, CancellationToken, Digest, ForkOptions, GenerationId, IdempotencyKey,
    LocalAuthorityBackend, LocalFs, LocalObjectBackend, MergeConflict, MountId, OperationId,
    TransactionCommit, WatchInvalidationReason, WorkspaceRebase, probe_native_mount,
};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

/// One request-addressed protocol command.
#[derive(Debug, Deserialize)]
pub(crate) struct Request {
    /// Caller-selected response correlation identity.
    pub id: String,
    /// Strict typed command payload.
    #[serde(flatten)]
    command: Command,
}

impl Request {
    /// Whether this request asks the owning process to stop after replying.
    #[must_use]
    pub(crate) const fn requests_shutdown(&self) -> bool {
        matches!(&self.command, Command::Shutdown)
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
enum Command {
    Health,
    Shutdown,
    Capabilities,
    CreateWorkspace {
        name: String,
    },
    OpenWorkspace {
        name: String,
    },
    WorkspaceHead {
        workspace: String,
    },
    WorkspaceRead {
        workspace: String,
        generation_id: Option<String>,
        path: String,
        #[serde(deserialize_with = "deserialize_u64")]
        maximum_bytes: u64,
    },
    WorkspaceReadRange {
        workspace: String,
        generation_id: Option<String>,
        path: String,
        #[serde(deserialize_with = "deserialize_u64")]
        offset: u64,
        #[serde(deserialize_with = "deserialize_u64")]
        length: u64,
    },
    WorkspaceStat {
        workspace: String,
        generation_id: Option<String>,
        path: String,
    },
    WorkspaceListDirectory {
        workspace: String,
        generation_id: Option<String>,
        path: String,
        after: Option<WireLogicalName>,
        maximum_entries: u32,
    },
    WorkspaceReadSymbolicLink {
        workspace: String,
        generation_id: Option<String>,
        path: String,
    },
    WorkspacePlanExtents {
        workspace: String,
        generation_id: Option<String>,
        path: String,
        #[serde(deserialize_with = "deserialize_u64")]
        offset: u64,
        #[serde(deserialize_with = "deserialize_u64")]
        length: u64,
        maximum_spans: u32,
    },
    WorkspaceCheckpoint {
        workspace: String,
        label: String,
    },
    WorkspacePin {
        workspace: String,
        generation_id: Option<String>,
        identity: String,
    },
    WorkspaceDiff {
        workspace: String,
        from_generation_id: String,
        to_generation_id: String,
        maximum_changes: u32,
    },
    WorkspaceDelete {
        workspace: String,
        idempotency_key: Option<String>,
    },
    WorkspacePlanJoin {
        source: String,
        target: String,
        history: String,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    },
    WorkspaceApplyJoin {
        plan_id: String,
        if_target_generation_id: String,
        idempotency_key: Option<String>,
    },
    WorkspaceCloseJoinPlan {
        plan_id: String,
    },
    WorkspaceMount {
        workspace: String,
        destination: String,
        writable: bool,
        subdirectory: String,
        publication: String,
    },
    WorkspaceMountSync {
        mount_id: String,
    },
    AttachDirectory {
        name: String,
        path: String,
        mode: String,
        maximum_paths: u32,
        maximum_extent_spans: u32,
        maximum_queued_changes: u32,
    },
    SourceState {
        source_id: String,
    },
    SourceReconcile {
        source_id: String,
    },
    SourceRescan {
        source_id: String,
    },
    SourceSeal {
        source_id: String,
    },
    SourceClose {
        source_id: String,
    },
    S3HeadObject {
        workspace: String,
        object_key: String,
    },
    S3GetObject {
        workspace: String,
        object_key: String,
        #[serde(deserialize_with = "deserialize_u64")]
        maximum_bytes: u64,
    },
    S3GetObjectRange {
        workspace: String,
        object_key: String,
        #[serde(deserialize_with = "deserialize_u64")]
        offset: u64,
        #[serde(deserialize_with = "deserialize_u64")]
        length: u64,
    },
    S3PutObject {
        workspace: String,
        object_key: String,
        bytes_base64: String,
        idempotency_key: Option<String>,
    },
    S3DeleteObject {
        workspace: String,
        object_key: String,
        idempotency_key: Option<String>,
    },
    S3DeleteObjects {
        workspace: String,
        object_keys: Vec<String>,
        idempotency_key: Option<String>,
    },
    S3CopyObject {
        workspace: String,
        source_key: String,
        destination_key: String,
        idempotency_key: Option<String>,
    },
    S3ListObjects {
        workspace: String,
        prefix: String,
        delimiter: Option<char>,
        after: Option<String>,
        maximum_keys: u32,
        maximum_entries_examined: u32,
    },
    S3CreateMultipartUpload {
        workspace: String,
        object_key: String,
        maximum_parts: u32,
        #[serde(deserialize_with = "deserialize_u64")]
        maximum_part_bytes: u64,
        idempotency_key: Option<String>,
    },
    S3UploadPart {
        upload_id: String,
        part_number: u32,
        bytes_base64: String,
    },
    S3CompleteMultipartUpload {
        upload_id: String,
        ordered_parts: Vec<u32>,
    },
    S3AbortMultipartUpload {
        upload_id: String,
    },
    WorkspaceWrite {
        workspace: String,
        path: String,
        bytes_base64: String,
        idempotency_key: Option<String>,
    },
    WorkspaceRemove {
        workspace: String,
        path: String,
        idempotency_key: Option<String>,
    },
    WorkspaceTransaction {
        workspace: String,
        idempotency_key: Option<String>,
        operations: Vec<WireWorkspaceOperation>,
    },
    WorkspaceBeginTransaction {
        workspace: String,
        idempotency_key: Option<String>,
    },
    WorkspaceStageTransaction {
        transaction_id: String,
        operation: WireWorkspaceOperation,
    },
    WorkspaceCommitTransaction {
        transaction_id: String,
    },
    WorkspaceRebaseTransaction {
        transaction_id: String,
        maximum_conflicts: u32,
    },
    WorkspaceCloseTransaction {
        transaction_id: String,
    },
    WorkspaceFork {
        workspace: String,
        destination: String,
        generation_id: Option<String>,
    },
    WorkspaceLiveRebase {
        workspace: String,
        idempotency_key: Option<String>,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    },
    ObjectCacheStats,
    ClearObjectCache,
    Unmount {
        mount_id: String,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
struct WireLogicalName {
    encoding: String,
    bytes_base64: String,
}

fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ExactU64 {
        Number(u64),
        Decimal(String),
    }

    match ExactU64::deserialize(deserializer)? {
        ExactU64::Number(value) => Ok(value),
        ExactU64::Decimal(value) => {
            if value.is_empty()
                || (value.len() > 1 && value.starts_with('0'))
                || !value.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(serde::de::Error::custom(
                    "u64 decimal string is not canonical",
                ));
            }
            value.parse().map_err(serde::de::Error::custom)
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum WireWorkspaceOperation {
    CreateDirAll {
        path: String,
    },
    CreateDirectory {
        path: String,
    },
    CreateSymbolicLink {
        path: String,
        target_base64: String,
    },
    Write {
        path: String,
        bytes_base64: String,
    },
    Remove {
        path: String,
    },
    Copy {
        source: String,
        destination: String,
    },
    Rename {
        source: String,
        destination: String,
    },
    HardLink {
        source: String,
        destination: String,
    },
    WriteRange {
        path: String,
        #[serde(deserialize_with = "deserialize_u64")]
        offset: u64,
        bytes_base64: String,
    },
    Resize {
        path: String,
        #[serde(deserialize_with = "deserialize_u64")]
        logical_bytes: u64,
    },
    ZeroRange {
        path: String,
        #[serde(deserialize_with = "deserialize_u64")]
        offset: u64,
        #[serde(deserialize_with = "deserialize_u64")]
        length: u64,
        allocated: bool,
        extend: bool,
    },
    Preallocate {
        path: String,
        #[serde(deserialize_with = "deserialize_u64")]
        offset: u64,
        #[serde(deserialize_with = "deserialize_u64")]
        length: u64,
        keep_size: bool,
    },
    CloneRange {
        source: String,
        #[serde(deserialize_with = "deserialize_u64")]
        source_offset: u64,
        destination: String,
        #[serde(deserialize_with = "deserialize_u64")]
        destination_offset: u64,
        #[serde(deserialize_with = "deserialize_u64")]
        length: u64,
    },
    SetMetadata {
        path: String,
        canonical_bytes_base64: String,
    },
}

/// One closed success or typed failure response.
#[derive(Debug, Serialize)]
pub(crate) struct Response {
    id: String,
    ok: bool,
    result: Option<Value>,
    error: Option<RpcError>,
}

#[derive(Debug, Serialize)]
struct RpcError {
    code: &'static str,
    message: String,
}

impl RpcError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_argument",
            message: message.into(),
        }
    }

    fn absent(message: impl Into<String>) -> Self {
        Self {
            code: "not_found",
            message: message.into(),
        }
    }

    fn engine(error: impl std::fmt::Display) -> Self {
        Self {
            code: "engine_failure",
            message: error.to_string(),
        }
    }
}

type LocalMultipartUpload =
    acyclic_fs::S3MultipartUpload<LocalAuthorityBackend, LocalObjectBackend>;
type SharedMultipartUpload = Arc<tokio::sync::Mutex<Option<LocalMultipartUpload>>>;
type LocalTransaction = acyclic_fs::Transaction<LocalAuthorityBackend, LocalObjectBackend>;
type SharedTransaction = Arc<tokio::sync::Mutex<LocalTransaction>>;
type DaemonMount = acyclic_fs::Mount<LocalAuthorityBackend, LocalObjectBackend>;

/// Process-owned optional service state. Durable truth remains in the two SDK stores.
pub(crate) struct DaemonState {
    fs: Arc<LocalFs>,
    cancellation: CancellationToken,
    shutdown: CancellationToken,
    mounts: tokio::sync::Mutex<HashMap<MountId, DaemonMount>>,
    mount_journal: Option<MountJournal>,
    join_plans: tokio::sync::RwLock<
        HashMap<OperationId, Arc<acyclic_fs::JoinPlan<LocalAuthorityBackend, LocalObjectBackend>>>,
    >,
    multipart_uploads: tokio::sync::RwLock<HashMap<OperationId, SharedMultipartUpload>>,
    transactions: tokio::sync::RwLock<HashMap<OperationId, SharedTransaction>>,
    sources: tokio::sync::RwLock<
        HashMap<OperationId, Arc<acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>>>,
    >,
}

impl DaemonState {
    /// Creates empty derived handle state over one embedded engine.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn new(fs: Arc<LocalFs>) -> Self {
        Self {
            fs,
            cancellation: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            mounts: tokio::sync::Mutex::new(HashMap::new()),
            mount_journal: None,
            join_plans: tokio::sync::RwLock::new(HashMap::new()),
            multipart_uploads: tokio::sync::RwLock::new(HashMap::new()),
            transactions: tokio::sync::RwLock::new(HashMap::new()),
            sources: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Creates derived handles with durable native-mount restart recovery.
    #[must_use]
    pub(crate) fn with_mount_journal(fs: Arc<LocalFs>, mount_journal: MountJournal) -> Self {
        Self {
            fs,
            cancellation: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            mounts: tokio::sync::Mutex::new(HashMap::new()),
            mount_journal: Some(mount_journal),
            join_plans: tokio::sync::RwLock::new(HashMap::new()),
            multipart_uploads: tokio::sync::RwLock::new(HashMap::new()),
            transactions: tokio::sync::RwLock::new(HashMap::new()),
            sources: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Executes one parsed command and always returns one terminal response.
    pub(crate) async fn handle(&self, request: Request) -> Response {
        let result = self.dispatch(request.command).await;
        match result {
            Ok(result) => Response {
                id: request.id,
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(error) => Response {
                id: request.id,
                ok: false,
                result: None,
                error: Some(error),
            },
        }
    }

    /// Cancels callbacks and synchronously releases all process-owned mounts.
    pub(crate) async fn stop_all_mounts(&self) -> Result<(), String> {
        self.cancellation.cancel();
        self.sources.write().await.clear();
        self.transactions.write().await.clear();
        let sessions = {
            let mut mounts = self.mounts.lock().await;
            mounts.drain().collect::<Vec<_>>()
        };
        let mut failed = Vec::new();
        let mut first_error = None;
        for (mount_id, session) in sessions {
            let stopped = session.unmount().await.map_err(|error| error.to_string());
            let completed = stopped.and_then(|()| {
                self.mount_journal.as_ref().map_or(Ok(()), |journal| {
                    journal
                        .complete(mount_id)
                        .map_err(|error| error.to_string())
                })
            });
            if let Err(error) = completed {
                first_error.get_or_insert(error);
                failed.push((mount_id, session));
            }
        }
        if !failed.is_empty() {
            self.mounts.lock().await.extend(failed);
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Requests process shutdown after the current terminal response is flushed.
    pub(crate) fn request_shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Resolves once one loopback client has requested process shutdown.
    pub(crate) async fn shutdown_requested(&self) {
        self.shutdown.cancelled().await;
    }

    #[allow(clippy::too_many_lines)]
    fn dispatch(
        &self,
        command: Command,
    ) -> Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send + '_>> {
        Box::pin(async move {
            match command {
                Command::Health => Ok(json!({ "status": "ready" })),
                Command::Shutdown => Ok(json!({ "status": "stopping" })),
                Command::CreateWorkspace { name } => {
                    let workspace = self
                        .fs
                        .create_workspace(name)
                        .await
                        .map_err(RpcError::engine)?;
                    workspace_value(&workspace).await
                }
                Command::OpenWorkspace { name } => {
                    let workspace = self
                        .fs
                        .open_workspace(name)
                        .await
                        .map_err(RpcError::engine)?;
                    workspace_value(&workspace).await
                }
                Command::WorkspaceHead { workspace } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = workspace.head().await.map_err(RpcError::engine)?;
                    Ok(json!({
                        "generationId": hex::encode(generation.id().digest().into_bytes()),
                    }))
                }
                Command::WorkspaceRead {
                    workspace,
                    generation_id,
                    path,
                    maximum_bytes,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let bytes = Box::pin(generation.read(&path, maximum_bytes))
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(json!({
                        "bytesBase64": base64::engine::general_purpose::STANDARD.encode(bytes),
                    }))
                }
                Command::WorkspaceReadRange {
                    workspace,
                    generation_id,
                    path,
                    offset,
                    length,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let bytes = Box::pin(generation.read_range(&path, offset, length))
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(json!({
                        "bytesBase64": base64::engine::general_purpose::STANDARD.encode(bytes),
                    }))
                }
                Command::WorkspaceStat {
                    workspace,
                    generation_id,
                    path,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let stat = Box::pin(generation.stat(&path))
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_stat_value(&stat))
                }
                Command::WorkspaceListDirectory {
                    workspace,
                    generation_id,
                    path,
                    after,
                    maximum_entries,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let after = after.as_ref().map(parse_logical_name).transpose()?;
                    let page =
                        Box::pin(generation.list_directory(&path, after.as_ref(), maximum_entries))
                            .await
                            .map_err(RpcError::engine)?;
                    Ok(json!({
                        "entries": page.entries.into_iter().map(|entry| json!({
                            "name": name_value(&entry.name),
                            "fileId": hex::encode(entry.file_id.into_bytes()),
                            "fileKind": file_kind(entry.kind),
                        })).collect::<Vec<_>>(),
                        "hasMore": page.has_more,
                    }))
                }
                Command::WorkspaceReadSymbolicLink {
                    workspace,
                    generation_id,
                    path,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let target = generation
                        .read_symbolic_link(&path)
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(json!({
                        "targetBase64": base64::engine::general_purpose::STANDARD.encode(target),
                    }))
                }
                Command::WorkspacePlanExtents {
                    workspace,
                    generation_id,
                    path,
                    offset,
                    length,
                    maximum_spans,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let plan =
                        Box::pin(generation.plan_extents(&path, offset, length, maximum_spans))
                            .await
                            .map_err(RpcError::engine)?;
                    Ok(json!({
                        "spans": plan.spans.into_iter().map(workspace_extent_value).collect::<Vec<_>>(),
                    }))
                }
                Command::WorkspaceCheckpoint { workspace, label } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let checkpoint = workspace
                        .checkpoint(label)
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(json!({
                        "label": checkpoint.label().as_str(),
                        "generationId": hex::encode(checkpoint.generation().id().digest().into_bytes()),
                    }))
                }
                Command::WorkspacePin {
                    workspace,
                    generation_id,
                    identity,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let pin = generation.pin(identity).await.map_err(RpcError::engine)?;
                    Ok(json!({
                        "identity": pin.identity().as_str(),
                        "generationId": hex::encode(pin.generation().id().digest().into_bytes()),
                    }))
                }
                Command::WorkspaceDiff {
                    workspace,
                    from_generation_id,
                    to_generation_id,
                    maximum_changes,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let from = workspace
                        .generation(parse_generation_id(&from_generation_id)?)
                        .await
                        .map_err(RpcError::engine)?;
                    let to = workspace
                        .generation(parse_generation_id(&to_generation_id)?)
                        .await
                        .map_err(RpcError::engine)?;
                    let changes = workspace
                        .diff(&from, &to, maximum_changes)
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_change_set_value(&changes))
                }
                Command::WorkspaceDelete {
                    workspace,
                    idempotency_key,
                } => {
                    let outcome = self
                        .fs
                        .delete_workspace(
                            workspace,
                            parse_idempotency_key(idempotency_key.as_deref())?,
                        )
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(json!({
                        "status": match outcome {
                            acyclic_fs::WorkspaceDelete::Deleted => "deleted",
                            acyclic_fs::WorkspaceDelete::AlreadyDeleted => "already-deleted",
                            acyclic_fs::WorkspaceDelete::Conflict => "conflict",
                            acyclic_fs::WorkspaceDelete::IdempotencyConflict => "idempotency-conflict",
                        },
                    }))
                }
                Command::WorkspacePlanJoin {
                    source,
                    target,
                    history,
                    maximum_generations,
                    maximum_changes,
                    maximum_conflicts,
                } => {
                    let source = self
                        .fs
                        .open_workspace(source)
                        .await
                        .map_err(RpcError::engine)?;
                    let target = self
                        .fs
                        .open_workspace(target)
                        .await
                        .map_err(RpcError::engine)?;
                    let plan = source
                        .join_into(&target)
                        .history(parse_join_history(&history)?)
                        .bounds(maximum_generations, maximum_changes, maximum_conflicts)
                        .plan()
                        .await
                        .map_err(RpcError::engine)?;
                    let plan_id = OperationId::new();
                    let mut plans = self.join_plans.write().await;
                    if plans.len() >= 1_024 {
                        return Err(RpcError::invalid(
                            "daemon has reached its bounded join-plan capacity",
                        ));
                    }
                    let value = json!({
                        "planId": hex::encode(plan_id.into_bytes()),
                        "sourceGenerationId": hex::encode(plan.source_head().digest().into_bytes()),
                        "targetGenerationId": hex::encode(plan.target_head().digest().into_bytes()),
                        "commonAncestorGenerationId": hex::encode(plan.common_ancestor().digest().into_bytes()),
                    });
                    plans.insert(plan_id, Arc::new(plan));
                    Ok(value)
                }
                Command::WorkspaceApplyJoin {
                    plan_id,
                    if_target_generation_id,
                    idempotency_key,
                } => {
                    let plan_id = OperationId::from_bytes(parse_16(&plan_id, "join plan")?);
                    let plan = self
                        .join_plans
                        .read()
                        .await
                        .get(&plan_id)
                        .cloned()
                        .ok_or_else(|| RpcError::absent("join plan is absent or closed"))?;
                    let outcome = plan
                        .apply(acyclic_fs::ApplyOptions {
                            if_target: parse_generation_id(&if_target_generation_id)?,
                            idempotency_key: parse_idempotency_key(idempotency_key.as_deref())?,
                        })
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_join_value(outcome))
                }
                Command::WorkspaceCloseJoinPlan { plan_id } => {
                    let plan_id = OperationId::from_bytes(parse_16(&plan_id, "join plan")?);
                    let closed = self.join_plans.write().await.remove(&plan_id).is_some();
                    Ok(json!({ "closed": closed }))
                }
                Command::WorkspaceMount {
                    workspace,
                    destination,
                    writable,
                    subdirectory,
                    publication,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let destination = PathBuf::from(destination)
                        .canonicalize()
                        .map_err(RpcError::engine)?;
                    let mount_id = MountId::new();
                    if let Some(journal) = &self.mount_journal {
                        journal
                            .admit(mount_id, &destination)
                            .map_err(RpcError::engine)?;
                    }
                    let options = if writable {
                        acyclic_fs::MountOptions::read_write()
                    } else {
                        acyclic_fs::MountOptions::read_only()
                    }
                    .subdirectory(subdirectory)
                    .publication(parse_mount_publication(&publication)?);
                    let mounted = workspace.mount(destination, options).await;
                    let mount = match mounted {
                        Ok(mount) => mount,
                        Err(error) => {
                            if let Some(journal) = &self.mount_journal {
                                journal.complete(mount_id).map_err(RpcError::engine)?;
                            }
                            return Err(RpcError::engine(error));
                        }
                    };
                    self.mounts.lock().await.insert(mount_id, mount);
                    Ok(json!({ "mountId": hex::encode(mount_id.into_bytes()) }))
                }
                Command::WorkspaceMountSync { mount_id } => {
                    let mount_id = MountId::from_bytes(parse_16(&mount_id, "mount")?);
                    let mounts = self.mounts.lock().await;
                    let mount = mounts
                        .get(&mount_id)
                        .ok_or_else(|| RpcError::absent("mount is absent or stopped"))?;
                    mount.sync().await.map_err(RpcError::engine)?;
                    Ok(json!({ "synced": true }))
                }
                Command::AttachDirectory {
                    name,
                    path,
                    mode,
                    maximum_paths,
                    maximum_extent_spans,
                    maximum_queued_changes,
                } => {
                    let workspace = Box::pin(self.fs.attach_directory(
                        name,
                        path,
                        acyclic_fs::SourceOptions {
                            mode: parse_source_mode(&mode)?,
                            maximum_paths,
                            maximum_extent_spans,
                            maximum_queued_changes,
                        },
                    ))
                    .await
                    .map_err(RpcError::engine)?;
                    let source_id = OperationId::new();
                    let value = workspace_value(&workspace).await?;
                    let mut sources = self.sources.write().await;
                    if sources.len() >= 1_024 {
                        return Err(RpcError::invalid(
                            "daemon has reached its bounded source capacity",
                        ));
                    }
                    sources.insert(source_id, Arc::new(workspace));
                    Ok(json!({
                        "sourceId": hex::encode(source_id.into_bytes()),
                        "workspace": value,
                        "state": "clean",
                    }))
                }
                Command::SourceState { source_id } => {
                    let workspace = self.source_workspace(&source_id).await?;
                    let source = workspace
                        .source()
                        .ok_or_else(|| RpcError::invalid("workspace has no attached source"))?;
                    Ok(source_state_value(source.state().await))
                }
                Command::SourceReconcile { source_id } => {
                    let workspace = self.source_workspace(&source_id).await?;
                    let source = workspace
                        .source()
                        .ok_or_else(|| RpcError::invalid("workspace has no attached source"))?;
                    let outcome = Box::pin(source.reconcile())
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(source_reconcile_value(outcome))
                }
                Command::SourceRescan { source_id } => {
                    let workspace = self.source_workspace(&source_id).await?;
                    let source = workspace
                        .source()
                        .ok_or_else(|| RpcError::invalid("workspace has no attached source"))?;
                    let outcome = Box::pin(source.rescan()).await.map_err(RpcError::engine)?;
                    Ok(source_reconcile_value(outcome))
                }
                Command::SourceSeal { source_id } => {
                    let workspace = self.source_workspace(&source_id).await?;
                    let generation = Box::pin(workspace.seal()).await.map_err(RpcError::engine)?;
                    Ok(json!({
                        "status": "sealed",
                        "generationId": hex::encode(generation.id().digest().into_bytes()),
                    }))
                }
                Command::SourceClose { source_id } => {
                    let source_id = OperationId::from_bytes(parse_16(&source_id, "source handle")?);
                    let closed = self.sources.write().await.remove(&source_id).is_some();
                    Ok(json!({ "closed": closed }))
                }
                Command::S3HeadObject {
                    workspace,
                    object_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let head = workspace
                        .s3()
                        .head_object(&object_key)
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(s3_head_value(&head))
                }
                Command::S3GetObject {
                    workspace,
                    object_key,
                    maximum_bytes,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let bytes = Box::pin(workspace.s3().get_object(&object_key, maximum_bytes))
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(json!({
                        "bytesBase64": base64::engine::general_purpose::STANDARD.encode(bytes),
                    }))
                }
                Command::S3GetObjectRange {
                    workspace,
                    object_key,
                    offset,
                    length,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let bytes = Box::pin(
                        workspace
                            .s3()
                            .get_object_range(&object_key, ByteRange { offset, length }),
                    )
                    .await
                    .map_err(RpcError::engine)?;
                    Ok(json!({
                        "bytesBase64": base64::engine::general_purpose::STANDARD.encode(bytes),
                    }))
                }
                Command::S3PutObject {
                    workspace,
                    object_key,
                    bytes_base64,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let outcome = Box::pin(workspace.s3().put_object(
                        &object_key,
                        decode_bytes(&bytes_base64)?.into(),
                        parse_idempotency_key(idempotency_key.as_deref())?,
                    ))
                    .await
                    .map_err(RpcError::engine)?;
                    Ok(workspace_commit_value(outcome))
                }
                Command::S3DeleteObject {
                    workspace,
                    object_key,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let outcome = workspace
                        .s3()
                        .delete_object(
                            &object_key,
                            parse_idempotency_key(idempotency_key.as_deref())?,
                        )
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_commit_value(outcome))
                }
                Command::S3DeleteObjects {
                    workspace,
                    object_keys,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let outcome = workspace
                        .s3()
                        .delete_objects(
                            &object_keys,
                            parse_idempotency_key(idempotency_key.as_deref())?,
                        )
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_commit_value(outcome))
                }
                Command::S3CopyObject {
                    workspace,
                    source_key,
                    destination_key,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let outcome = Box::pin(workspace.s3().copy_object(
                        &source_key,
                        &destination_key,
                        parse_idempotency_key(idempotency_key.as_deref())?,
                    ))
                    .await
                    .map_err(RpcError::engine)?;
                    Ok(workspace_commit_value(outcome))
                }
                Command::S3ListObjects {
                    workspace,
                    prefix,
                    delimiter,
                    after,
                    maximum_keys,
                    maximum_entries_examined,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let list = Box::pin(workspace.s3().list_objects(acyclic_fs::S3ListOptions {
                        prefix,
                        delimiter,
                        after,
                        maximum_keys,
                        maximum_entries_examined,
                    }))
                    .await
                    .map_err(RpcError::engine)?;
                    Ok(json!({
                        "objects": list.objects.iter().map(s3_head_value).collect::<Vec<_>>(),
                        "commonPrefixes": list.common_prefixes,
                        "nextAfter": list.next_after,
                        "entriesExamined": list.entries_examined,
                    }))
                }
                Command::S3CreateMultipartUpload {
                    workspace,
                    object_key,
                    maximum_parts,
                    maximum_part_bytes,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let upload = workspace
                        .s3()
                        .create_multipart_upload(
                            &object_key,
                            acyclic_fs::S3MultipartOptions {
                                maximum_parts,
                                maximum_part_bytes,
                            },
                            parse_idempotency_key(idempotency_key.as_deref())?,
                        )
                        .await
                        .map_err(RpcError::engine)?;
                    let upload_id = OperationId::new();
                    let mut uploads = self.multipart_uploads.write().await;
                    if uploads.len() >= 1_024 {
                        return Err(RpcError::invalid(
                            "daemon has reached its bounded multipart-upload capacity",
                        ));
                    }
                    uploads.insert(upload_id, Arc::new(tokio::sync::Mutex::new(Some(upload))));
                    Ok(json!({ "uploadId": hex::encode(upload_id.into_bytes()) }))
                }
                Command::S3UploadPart {
                    upload_id,
                    part_number,
                    bytes_base64,
                } => {
                    let upload_id = OperationId::from_bytes(parse_16(&upload_id, "upload")?);
                    let upload = self
                        .multipart_uploads
                        .read()
                        .await
                        .get(&upload_id)
                        .cloned()
                        .ok_or_else(|| RpcError::absent("multipart upload is absent or aborted"))?;
                    let mut upload = upload.lock().await;
                    upload
                        .as_mut()
                        .ok_or_else(|| RpcError::absent("multipart upload is absent or aborted"))?
                        .upload_part(part_number, decode_bytes(&bytes_base64)?.into())
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(json!({ "accepted": true }))
                }
                Command::S3CompleteMultipartUpload {
                    upload_id,
                    ordered_parts,
                } => {
                    let upload_id = OperationId::from_bytes(parse_16(&upload_id, "upload")?);
                    let upload = self
                        .multipart_uploads
                        .read()
                        .await
                        .get(&upload_id)
                        .cloned()
                        .ok_or_else(|| RpcError::absent("multipart upload is absent or aborted"))?;
                    let mut upload = upload.lock().await;
                    let outcome = upload
                        .as_mut()
                        .ok_or_else(|| RpcError::absent("multipart upload is absent or aborted"))?
                        .complete(&ordered_parts)
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_commit_value(outcome))
                }
                Command::S3AbortMultipartUpload { upload_id } => {
                    let upload_id = OperationId::from_bytes(parse_16(&upload_id, "upload")?);
                    let upload = self.multipart_uploads.write().await.remove(&upload_id);
                    let Some(upload) = upload else {
                        return Ok(json!({ "aborted": false }));
                    };
                    let mut upload = upload.lock().await;
                    if let Some(upload) = upload.take() {
                        upload.abort();
                    }
                    Ok(json!({ "aborted": true }))
                }
                Command::WorkspaceWrite {
                    workspace,
                    path,
                    bytes_base64,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    Box::pin(apply_workspace_transaction(
                        &workspace,
                        parse_idempotency_key(idempotency_key.as_deref())?,
                        vec![WireWorkspaceOperation::Write { path, bytes_base64 }],
                    ))
                    .await
                }
                Command::WorkspaceRemove {
                    workspace,
                    path,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    Box::pin(apply_workspace_transaction(
                        &workspace,
                        parse_idempotency_key(idempotency_key.as_deref())?,
                        vec![WireWorkspaceOperation::Remove { path }],
                    ))
                    .await
                }
                Command::WorkspaceTransaction {
                    workspace,
                    idempotency_key,
                    operations,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    Box::pin(apply_workspace_transaction(
                        &workspace,
                        parse_idempotency_key(idempotency_key.as_deref())?,
                        operations,
                    ))
                    .await
                }
                Command::WorkspaceBeginTransaction {
                    workspace,
                    idempotency_key,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let transaction = workspace
                        .begin_transaction(parse_idempotency_key(idempotency_key.as_deref())?)
                        .await
                        .map_err(RpcError::engine)?;
                    let transaction_id = OperationId::new();
                    let mut transactions = self.transactions.write().await;
                    if transactions.len() >= 1_024 {
                        return Err(RpcError::invalid(
                            "daemon has reached its bounded transaction capacity",
                        ));
                    }
                    transactions.insert(
                        transaction_id,
                        Arc::new(tokio::sync::Mutex::new(transaction)),
                    );
                    Ok(json!({
                        "transactionId": hex::encode(transaction_id.into_bytes()),
                    }))
                }
                Command::WorkspaceStageTransaction {
                    transaction_id,
                    operation,
                } => {
                    let transaction = self.transaction(&transaction_id).await?;
                    let mut transaction = transaction.lock().await;
                    apply_workspace_operations(&mut transaction, [operation]).await?;
                    Ok(json!({ "staged": true }))
                }
                Command::WorkspaceCommitTransaction { transaction_id } => {
                    let transaction = self.transaction(&transaction_id).await?;
                    let outcome = transaction
                        .lock()
                        .await
                        .commit()
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_commit_value(outcome))
                }
                Command::WorkspaceRebaseTransaction {
                    transaction_id,
                    maximum_conflicts,
                } => {
                    let transaction = self.transaction(&transaction_id).await?;
                    let outcome = transaction
                        .lock()
                        .await
                        .rebase(maximum_conflicts)
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(transaction_rebase_value(outcome))
                }
                Command::WorkspaceCloseTransaction { transaction_id } => {
                    let transaction_id =
                        OperationId::from_bytes(parse_16(&transaction_id, "transaction")?);
                    let closed = self
                        .transactions
                        .write()
                        .await
                        .remove(&transaction_id)
                        .is_some();
                    Ok(json!({ "closed": closed }))
                }
                Command::WorkspaceFork {
                    workspace,
                    destination,
                    generation_id,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let generation = select_workspace_generation(&workspace, generation_id).await?;
                    let fork = workspace
                        .fork(destination, ForkOptions::from_generation(generation))
                        .await
                        .map_err(RpcError::engine)?;
                    workspace_value(&fork).await
                }
                Command::WorkspaceLiveRebase {
                    workspace,
                    idempotency_key,
                    maximum_generations,
                    maximum_changes,
                    maximum_conflicts,
                } => {
                    let workspace = self
                        .fs
                        .open_workspace(workspace)
                        .await
                        .map_err(RpcError::engine)?;
                    let outcome = workspace
                        .live_rebase(
                            parse_idempotency_key(idempotency_key.as_deref())?,
                            maximum_generations,
                            maximum_changes,
                            maximum_conflicts,
                        )
                        .await
                        .map_err(RpcError::engine)?;
                    Ok(workspace_rebase_value(outcome))
                }
                Command::Capabilities => {
                    let mount = probe_native_mount();
                    Ok(json!({
                        "version": env!("CARGO_PKG_VERSION"),
                        "nativeMount": {
                            "kind": format!("{:?}", mount.kind),
                            "available": mount.available,
                            "writable": mount.writable,
                            "unavailableReason": mount.unavailable_reason,
                        }
                    }))
                }
                Command::ObjectCacheStats => {
                    let stats = self.fs.object_cache_stats().map_err(RpcError::engine)?;
                    Ok(json!({
                        "hits": stats.hits,
                        "decodedHits": stats.decoded_hits,
                        "misses": stats.misses,
                        "coalescedReads": stats.coalesced_reads,
                        "evictions": stats.evictions,
                        "residentEntries": stats.resident_entries,
                        "residentBytes": stats.resident_bytes,
                        "residentCanonicalObjects": stats.resident_canonical_objects,
                        "residentCanonicalBytes": stats.resident_canonical_bytes,
                        "residentDecodedPages": stats.resident_decoded_pages,
                        "residentDecodedBytes": stats.resident_decoded_bytes,
                        "inFlight": stats.in_flight,
                    }))
                }
                Command::ClearObjectCache => {
                    self.fs.clear_object_cache().map_err(RpcError::engine)?;
                    Ok(json!({ "cleared": true }))
                }
                Command::Unmount { mount_id } => {
                    let mount_id = MountId::from_bytes(parse_16(&mount_id, "mount")?);
                    let mut mounts = self.mounts.lock().await;
                    let session = mounts.get_mut(&mount_id);
                    let Some(session) = session else {
                        return Ok(json!({ "stopped": false }));
                    };
                    session.unmount().await.map_err(RpcError::engine)?;
                    if let Some(journal) = &self.mount_journal {
                        journal.complete(mount_id).map_err(RpcError::engine)?;
                    }
                    mounts.remove(&mount_id);
                    Ok(json!({ "stopped": true }))
                }
                Command::Unknown => Err(RpcError {
                    code: "unknown_method",
                    message: "daemon method is not recognized".to_owned(),
                }),
            }
        })
    }

    async fn source_workspace(
        &self,
        value: &str,
    ) -> Result<Arc<acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>>, RpcError>
    {
        let source_id = OperationId::from_bytes(parse_16(value, "source handle")?);
        self.sources
            .read()
            .await
            .get(&source_id)
            .cloned()
            .ok_or_else(|| RpcError::absent("source handle is absent or closed"))
    }

    async fn transaction(&self, value: &str) -> Result<SharedTransaction, RpcError> {
        let transaction_id = OperationId::from_bytes(parse_16(value, "transaction")?);
        self.transactions
            .read()
            .await
            .get(&transaction_id)
            .cloned()
            .ok_or_else(|| RpcError::absent("transaction handle is absent or closed"))
    }
}

fn parse_generation_id(value: &str) -> Result<GenerationId, RpcError> {
    let bytes =
        hex::decode(value).map_err(|_| RpcError::invalid("generation identity is not hex"))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| RpcError::invalid("generation identity must be 32 bytes"))?;
    Ok(GenerationId::new(Digest::from_bytes(bytes)))
}

fn parse_16(value: &str, name: &str) -> Result<[u8; 16], RpcError> {
    let bytes =
        hex::decode(value).map_err(|_| RpcError::invalid(format!("{name} identity is not hex")))?;
    bytes
        .try_into()
        .map_err(|_| RpcError::invalid(format!("{name} identity must be 16 bytes")))
}

fn decode_bytes(value: &str) -> Result<Vec<u8>, RpcError> {
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| RpcError::invalid("bytesBase64 is malformed"))
}

fn name_value(name: &LogicalName) -> Value {
    json!({
        "encoding": match name.encoding() {
            NameEncoding::Utf8 => "utf8",
            NameEncoding::PosixBytes => "posix_bytes",
            NameEncoding::WindowsUtf16Le => "windows_utf16le",
        },
        "bytesBase64": base64::engine::general_purpose::STANDARD.encode(name.as_bytes()),
    })
}

fn merge_conflict_value(conflict: MergeConflict) -> Value {
    match conflict {
        MergeConflict::File(file_id) => json!({
            "kind": "file",
            "fileId": hex::encode(file_id.into_bytes()),
        }),
        MergeConflict::Binding { directory_id, name } => json!({
            "kind": "binding",
            "directoryId": hex::encode(directory_id.into_bytes()),
            "name": name_value(&name),
        }),
    }
}

#[allow(clippy::too_many_lines)]
async fn apply_workspace_transaction(
    workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    idempotency_key: IdempotencyKey,
    operations: Vec<WireWorkspaceOperation>,
) -> Result<Value, RpcError> {
    if operations.is_empty() {
        return Err(RpcError::invalid(
            "workspace transaction requires at least one operation",
        ));
    }
    let mut transaction = workspace
        .begin_transaction(idempotency_key)
        .await
        .map_err(RpcError::engine)?;
    apply_workspace_operations(&mut transaction, operations).await?;
    transaction
        .commit()
        .await
        .map(workspace_commit_value)
        .map_err(RpcError::engine)
}

#[allow(clippy::too_many_lines)]
async fn apply_workspace_operations(
    transaction: &mut LocalTransaction,
    operations: impl IntoIterator<Item = WireWorkspaceOperation>,
) -> Result<(), RpcError> {
    for operation in operations {
        match operation {
            WireWorkspaceOperation::CreateDirAll { path } => transaction
                .create_dir_all(&path)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::CreateDirectory { path } => transaction
                .create_directory(&path)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::CreateSymbolicLink {
                path,
                target_base64,
            } => transaction
                .create_symbolic_link(&path, decode_bytes(&target_base64)?.into())
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::Write { path, bytes_base64 } => transaction
                .write(&path, decode_bytes(&bytes_base64)?.into())
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::Remove { path } => {
                transaction.remove(&path).await.map_err(RpcError::engine)?;
            }
            WireWorkspaceOperation::Copy {
                source,
                destination,
            } => transaction
                .copy(&source, &destination)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::Rename {
                source,
                destination,
            } => transaction
                .rename(&source, &destination)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::HardLink {
                source,
                destination,
            } => transaction
                .hard_link(&source, &destination)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::WriteRange {
                path,
                offset,
                bytes_base64,
            } => transaction
                .write_range(&path, offset, decode_bytes(&bytes_base64)?.into())
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::Resize {
                path,
                logical_bytes,
            } => transaction
                .resize(&path, logical_bytes)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::ZeroRange {
                path,
                offset,
                length,
                allocated,
                extend,
            } => transaction
                .zero_range(&path, ByteRange { offset, length }, allocated, extend)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::Preallocate {
                path,
                offset,
                length,
                keep_size,
            } => transaction
                .preallocate(&path, ByteRange { offset, length }, keep_size)
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::CloneRange {
                source,
                source_offset,
                destination,
                destination_offset,
                length,
            } => transaction
                .clone_range(
                    &source,
                    source_offset,
                    &destination,
                    destination_offset,
                    length,
                )
                .await
                .map_err(RpcError::engine)?,
            WireWorkspaceOperation::SetMetadata {
                path,
                canonical_bytes_base64,
            } => {
                let metadata = decode_file_metadata(
                    &decode_bytes(&canonical_bytes_base64)?,
                    DecodeLimits::default(),
                )
                .map_err(RpcError::engine)?;
                transaction
                    .set_metadata(&path, metadata)
                    .await
                    .map_err(RpcError::engine)?;
            }
        }
    }
    Ok(())
}

async fn select_workspace_generation(
    workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    generation_id: Option<String>,
) -> Result<acyclic_fs::Generation<LocalAuthorityBackend, LocalObjectBackend>, RpcError> {
    match generation_id {
        Some(generation_id) => workspace
            .generation(parse_generation_id(&generation_id)?)
            .await
            .map_err(RpcError::engine),
        None => workspace.head().await.map_err(RpcError::engine),
    }
}

fn parse_logical_name(value: &WireLogicalName) -> Result<LogicalName, RpcError> {
    let encoding = match value.encoding.as_str() {
        "utf8" => NameEncoding::Utf8,
        "posix_bytes" => NameEncoding::PosixBytes,
        "windows_utf16le" => NameEncoding::WindowsUtf16Le,
        _ => return Err(RpcError::invalid("invalid logical-name encoding")),
    };
    LogicalName::new(
        encoding,
        decode_bytes(&value.bytes_base64)?,
        VolumeLimits::default().maximum_component_bytes,
    )
    .map_err(RpcError::engine)
}

fn workspace_stat_value(stat: &acyclic_fs::WorkspaceStat) -> Value {
    json!({
        "fileId": hex::encode(stat.file_id.into_bytes()),
        "fileKind": file_kind(stat.kind),
        "linkCount": stat.link_count,
        "logicalBytes": stat.logical_bytes,
        "metadata": {
            "posixMode": stat.metadata.posix_mode,
            "posixUid": stat.metadata.posix_uid,
            "posixGid": stat.metadata.posix_gid,
            "posixFlags": stat.metadata.posix_flags,
            "windowsAttributes": stat.metadata.windows_attributes,
            "createdNs": stat.metadata.created_ns,
            "modifiedNs": stat.metadata.modified_ns,
            "accessedNs": stat.metadata.accessed_ns,
            "changedNs": stat.metadata.changed_ns,
            "hasNamedAttributes": stat.metadata.has_named_attributes,
            "hasAcl": stat.metadata.has_acl,
            "hasSecurityDescriptor": stat.metadata.has_security_descriptor,
        },
    })
}

fn workspace_extent_value(span: acyclic_fs::WorkspaceExtentSpan) -> Value {
    json!({
        "offset": span.offset,
        "length": span.length,
        "sourceEnd": span.source_end,
        "kind": match span.kind {
            acyclic_fs::WorkspaceExtentKind::Hole => "hole",
            acyclic_fs::WorkspaceExtentKind::AllocatedZero => "allocated-zero",
            acyclic_fs::WorkspaceExtentKind::Content => "content",
        },
    })
}

fn workspace_change_set_value(
    changes: &acyclic_fs::ChangeSet<LocalAuthorityBackend, LocalObjectBackend>,
) -> Value {
    let change = changes.changes();
    json!({
        "fromGenerationId": hex::encode(changes.from().id().digest().into_bytes()),
        "toGenerationId": hex::encode(changes.to().id().digest().into_bytes()),
        "files": change.files.iter().map(|entry| json!({
            "fileId": hex::encode(entry.file_id.into_bytes()),
            "before": entry.before.as_ref().map(workspace_file_change_value),
            "after": entry.after.as_ref().map(workspace_file_change_value),
        })).collect::<Vec<_>>(),
        "bindings": change.bindings.iter().map(|entry| json!({
            "directoryId": hex::encode(entry.directory_id.into_bytes()),
            "name": name_value(&entry.name),
            "before": entry.before.as_ref().map(workspace_binding_value),
            "after": entry.after.as_ref().map(workspace_binding_value),
        })).collect::<Vec<_>>(),
        "truncated": change.truncated,
        "work": changes.work(),
    })
}

fn parse_join_history(value: &str) -> Result<acyclic_fs::JoinHistory, RpcError> {
    match value {
        "merge" => Ok(acyclic_fs::JoinHistory::Merge),
        "rebase" => Ok(acyclic_fs::JoinHistory::Rebase),
        "squash" => Ok(acyclic_fs::JoinHistory::Squash),
        "cherry-pick" => Ok(acyclic_fs::JoinHistory::CherryPick),
        _ => Err(RpcError::invalid("unknown workspace join history")),
    }
}

fn parse_mount_publication(value: &str) -> Result<acyclic_fs::MountPublication, RpcError> {
    match value {
        "close-and-sync" => Ok(acyclic_fs::MountPublication::CloseAndSync),
        "per-mutation" => Ok(acyclic_fs::MountPublication::PerMutation),
        "manual" => Ok(acyclic_fs::MountPublication::Manual),
        _ => Err(RpcError::invalid("unknown mount publication policy")),
    }
}

fn parse_source_mode(value: &str) -> Result<acyclic_fs::SourceMode, RpcError> {
    match value {
        "pinned" => Ok(acyclic_fs::SourceMode::Pinned),
        "tracking" => Ok(acyclic_fs::SourceMode::Tracking),
        _ => Err(RpcError::invalid("unknown source mode")),
    }
}

fn source_state_value(state: acyclic_fs::SourceState) -> Value {
    match state {
        acyclic_fs::SourceState::Clean => json!({ "status": "clean" }),
        acyclic_fs::SourceState::PendingCapture => json!({ "status": "pending-capture" }),
        acyclic_fs::SourceState::NeedsRescan(reason) => json!({
            "status": "needs-rescan",
            "reason": watch_reason(reason),
        }),
        acyclic_fs::SourceState::Conflict => json!({ "status": "conflict" }),
        acyclic_fs::SourceState::Sealed => json!({ "status": "sealed" }),
    }
}

fn source_reconcile_value(
    outcome: acyclic_fs::ReconcileOutcome<LocalAuthorityBackend, LocalObjectBackend>,
) -> Value {
    match outcome {
        acyclic_fs::ReconcileOutcome::Clean(generation) => json!({
            "status": "clean",
            "generationId": hex::encode(generation.id().digest().into_bytes()),
        }),
        acyclic_fs::ReconcileOutcome::NeedsRescan(reason) => json!({
            "status": "needs-rescan",
            "reason": watch_reason(reason),
        }),
        acyclic_fs::ReconcileOutcome::Conflict => json!({ "status": "conflict" }),
    }
}

fn workspace_join_value(
    outcome: acyclic_fs::JoinOutcome<LocalAuthorityBackend, LocalObjectBackend>,
) -> Value {
    let generation = |status, generation: acyclic_fs::Generation<_, _>| {
        json!({
            "status": status,
            "generationId": hex::encode(generation.id().digest().into_bytes()),
            "conflicts": [],
            "truncated": false,
        })
    };
    match outcome {
        acyclic_fs::JoinOutcome::Applied(value) => generation("applied", value),
        acyclic_fs::JoinOutcome::AlreadyApplied(value) => generation("already-applied", value),
        acyclic_fs::JoinOutcome::NoChanges(value) => generation("no-changes", value),
        acyclic_fs::JoinOutcome::StaleTarget(value) => generation("stale-target", value),
        acyclic_fs::JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } => json!({
            "status": "conflicted",
            "conflicts": conflicts.into_iter().map(merge_conflict_value).collect::<Vec<_>>(),
            "truncated": truncated,
        }),
        acyclic_fs::JoinOutcome::Fenced => json!({
            "status": "fenced",
            "conflicts": [],
            "truncated": false,
        }),
        acyclic_fs::JoinOutcome::IdempotencyConflict => json!({
            "status": "idempotency-conflict",
            "conflicts": [],
            "truncated": false,
        }),
    }
}

fn s3_head_value(head: &acyclic_fs::S3ObjectHead) -> Value {
    json!({
        "objectKey": head.key,
        "contentLength": head.content_length,
        "etag": head.etag,
    })
}

fn workspace_file_change_value(record: &FileRecord) -> Value {
    let (payload_kind, logical_bytes, payload_object, inline_bytes, device_major, device_minor) =
        match &record.payload {
            FilePayload::InlineRegular(bytes) => (
                "inline-regular",
                Some(u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)),
                None,
                Some(base64::engine::general_purpose::STANDARD.encode(bytes.as_bytes())),
                None,
                None,
            ),
            FilePayload::Regular {
                logical_bytes,
                extents,
            } => (
                "regular",
                Some(*logical_bytes),
                Some(object_id_value(*extents)),
                None,
                None,
                None,
            ),
            FilePayload::Directory { entries } => (
                "directory",
                None,
                Some(object_id_value(*entries)),
                None,
                None,
                None,
            ),
            FilePayload::SymbolicLink {
                target_bytes,
                target,
            } => (
                "symbolic-link",
                Some(*target_bytes),
                Some(object_id_value(*target)),
                None,
                None,
                None,
            ),
            FilePayload::Empty => ("empty", None, None, None, None, None),
            FilePayload::Device { major, minor } => {
                ("device", None, None, None, Some(*major), Some(*minor))
            }
            FilePayload::ReparsePoint {
                payload_bytes,
                payload,
            } => (
                "reparse-point",
                Some(*payload_bytes),
                Some(object_id_value(*payload)),
                None,
                None,
                None,
            ),
        };
    json!({
        "fileId": hex::encode(record.file_id.into_bytes()),
        "fileKind": file_kind(record.kind),
        "linkCount": record.link_count,
        "metadataObject": object_id_value(record.metadata),
        "payloadKind": payload_kind,
        "logicalBytes": logical_bytes,
        "payloadObject": payload_object,
        "inlineBytesBase64": inline_bytes,
        "deviceMajor": device_major,
        "deviceMinor": device_minor,
    })
}

fn workspace_binding_value(entry: &TreeEntry) -> Value {
    json!({
        "name": name_value(&entry.name),
        "fileId": hex::encode(entry.file_id.into_bytes()),
        "fileKind": file_kind(entry.kind),
    })
}

fn object_id_value(value: acyclic_fs::ObjectId) -> String {
    let mut bytes = [0_u8; 33];
    bytes[0] = value.kind.canonical_tag();
    bytes[1..].copy_from_slice(value.digest.as_bytes());
    hex::encode(bytes)
}

async fn workspace_value(
    workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
) -> Result<Value, RpcError> {
    let generation = workspace.head().await.map_err(RpcError::engine)?;
    Ok(json!({
        "name": workspace.name().as_str(),
        "workspaceId": hex::encode(workspace.id().into_bytes()),
        "generationId": hex::encode(generation.id().digest().into_bytes()),
    }))
}

fn workspace_commit_value(
    outcome: TransactionCommit<LocalAuthorityBackend, LocalObjectBackend>,
) -> Value {
    match outcome {
        TransactionCommit::Committed(generation) => json!({
            "status": "committed",
            "generationId": hex::encode(generation.id().digest().into_bytes()),
        }),
        TransactionCommit::AlreadyCommitted(generation) => json!({
            "status": "already-committed",
            "generationId": hex::encode(generation.id().digest().into_bytes()),
        }),
        TransactionCommit::Conflict { actual } => json!({
            "status": "conflict",
            "generationId": hex::encode(actual.id().digest().into_bytes()),
        }),
        TransactionCommit::Fenced => json!({ "status": "fenced" }),
        TransactionCommit::IdempotencyConflict => {
            json!({ "status": "idempotency-conflict" })
        }
    }
}

fn workspace_rebase_value(
    outcome: WorkspaceRebase<LocalAuthorityBackend, LocalObjectBackend>,
) -> Value {
    let generation = |status, generation: acyclic_fs::Generation<_, _>| {
        json!({
            "status": status,
            "generationId": hex::encode(generation.id().digest().into_bytes()),
            "conflicts": [],
            "truncated": false,
        })
    };
    match outcome {
        WorkspaceRebase::Rebased(value) => generation("rebased", value),
        WorkspaceRebase::AlreadyRebased(value) => generation("already-rebased", value),
        WorkspaceRebase::Current(value) => generation("current", value),
        WorkspaceRebase::Stale(value) => generation("stale", value),
        WorkspaceRebase::Conflicted {
            conflicts,
            truncated,
        } => json!({
            "status": "conflicted",
            "conflicts": conflicts.into_iter().map(merge_conflict_value).collect::<Vec<_>>(),
            "truncated": truncated,
        }),
        WorkspaceRebase::Fenced => json!({
            "status": "fenced",
            "conflicts": [],
            "truncated": false,
        }),
        WorkspaceRebase::IdempotencyConflict => json!({
            "status": "idempotency-conflict",
            "conflicts": [],
            "truncated": false,
        }),
    }
}

fn transaction_rebase_value(
    outcome: acyclic_fs::TransactionRebase<LocalAuthorityBackend, LocalObjectBackend>,
) -> Value {
    match outcome {
        acyclic_fs::TransactionRebase::Rebased(generation) => json!({
            "status": "rebased",
            "generationId": hex::encode(generation.id().digest().into_bytes()),
            "conflicts": [],
            "truncated": false,
        }),
        acyclic_fs::TransactionRebase::Conflicted {
            conflicts,
            truncated,
        } => json!({
            "status": "conflicted",
            "generationId": Value::Null,
            "conflicts": conflicts.into_iter().map(transaction_conflict_value).collect::<Vec<_>>(),
            "truncated": truncated,
        }),
    }
}

#[allow(clippy::too_many_lines)]
fn transaction_conflict_value(conflict: acyclic_fs::TransactionConflict) -> Value {
    use acyclic_fs::{TransactionConflictRegion, TransactionDependencyUse, TransactionSparseSeek};
    let (region, file_id, directory_id, offset, length, sparse_target, name, maximum_entries) =
        match conflict.region {
            TransactionConflictRegion::FileRecord(file_id) => (
                "file-record",
                Some(file_id),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            TransactionConflictRegion::Metadata(file_id) => (
                "metadata",
                Some(file_id),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            TransactionConflictRegion::FileLength(file_id) => (
                "file-length",
                Some(file_id),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            TransactionConflictRegion::ContentRange {
                file_id,
                offset,
                length,
            } => (
                "content-range",
                Some(file_id),
                None,
                Some(offset),
                Some(length),
                None,
                None,
                None,
            ),
            TransactionConflictRegion::SparseSeek {
                file_id,
                offset,
                target,
            } => (
                "sparse-seek",
                Some(file_id),
                None,
                Some(offset),
                None,
                Some(match target {
                    TransactionSparseSeek::Data => "data",
                    TransactionSparseSeek::Hole => "hole",
                }),
                None,
                None,
            ),
            TransactionConflictRegion::DirectoryName { directory_id, name } => (
                "directory-name",
                None,
                Some(directory_id),
                None,
                None,
                None,
                Some(name),
                None,
            ),
            TransactionConflictRegion::DirectoryRange {
                directory_id,
                after,
                maximum_entries,
            } => (
                "directory-range",
                None,
                Some(directory_id),
                None,
                None,
                None,
                after,
                Some(maximum_entries),
            ),
        };
    json!({
        "region": region,
        "fileId": file_id.map(|value| hex::encode(value.into_bytes())),
        "directoryId": directory_id.map(|value| hex::encode(value.into_bytes())),
        "offset": offset,
        "length": length,
        "sparseTarget": sparse_target,
        "name": name.as_ref().map(name_value),
        "maximumEntries": maximum_entries,
        "usage": match conflict.usage {
            TransactionDependencyUse::Observation => "observation",
            TransactionDependencyUse::Mutation => "mutation",
            TransactionDependencyUse::ObservationAndMutation => "observation-and-mutation",
        },
        "expected": conflict.expected.map(|value| hex::encode(value.into_bytes())),
        "actual": conflict.actual.map(|value| hex::encode(value.into_bytes())),
    })
}

fn parse_idempotency_key(value: Option<&str>) -> Result<IdempotencyKey, RpcError> {
    value.map_or_else(
        || Ok(IdempotencyKey::new()),
        |value| parse_16(value, "idempotency key").map(IdempotencyKey::from_bytes),
    )
}

fn watch_reason(reason: WatchInvalidationReason) -> &'static str {
    match reason {
        WatchInvalidationReason::InitialSnapshotRequired => "initial-snapshot-required",
        WatchInvalidationReason::QueueOverflow => "queue-overflow",
        WatchInvalidationReason::NativeRescanRequired => "native-rescan-required",
        WatchInvalidationReason::BackendError => "backend-error",
        WatchInvalidationReason::UnrepresentablePath => "unrepresentable-path",
        WatchInvalidationReason::AmbiguousRename => "ambiguous-rename",
        WatchInvalidationReason::RootChanged => "root-changed",
    }
}

fn file_kind(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Regular => "regular",
        FileKind::Directory => "directory",
        FileKind::SymbolicLink => "symbolic_link",
        FileKind::Fifo => "fifo",
        FileKind::Socket => "socket",
        FileKind::CharacterDevice => "character_device",
        FileKind::BlockDevice => "block_device",
        FileKind::ReparsePoint => "reparse_point",
        FileKind::MountBoundary => "mount_boundary",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn unknown_methods_have_one_typed_terminal() -> Result<(), Box<dyn std::error::Error>> {
        let request: Request = serde_json::from_str(r#"{"id":"one","method":"other"}"#)?;
        assert!(matches!(request.command, Command::Unknown));
        Ok(())
    }

    #[test]
    fn shutdown_is_an_explicit_transport_lifecycle_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let request: Request = serde_json::from_str(r#"{"id":"stop","method":"shutdown"}"#)?;
        assert!(request.requests_shutdown());
        Ok(())
    }

    #[test]
    fn identity_and_binary_boundaries_fail_closed() {
        assert!(parse_16("00", "volume").is_err());
        assert!(decode_bytes("not base64!").is_err());
    }

    #[test]
    fn malformed_protocol_boundaries_fail_closed_before_dispatch() {
        for value in [
            "",
            "00",
            &"00".repeat(31),
            &"00".repeat(33),
            &format!("{}gg", "00".repeat(31)),
        ] {
            assert!(
                parse_generation_id(value).is_err(),
                "accepted generation {value:?}"
            );
        }
        for value in [
            "",
            "00",
            &"00".repeat(7),
            &"00".repeat(9),
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        ] {
            assert!(
                parse_16(value, "operation").is_err(),
                "accepted operation {value:?}"
            );
        }
        assert!(
            parse_logical_name(&WireLogicalName {
                encoding: "unknown".to_owned(),
                bytes_base64: String::new(),
            })
            .is_err()
        );
        assert!(
            parse_logical_name(&WireLogicalName {
                encoding: "utf8".to_owned(),
                bytes_base64: "not base64!".to_owned(),
            })
            .is_err()
        );
        assert!(parse_join_history("unknown").is_err());
        assert!(parse_mount_publication("unknown").is_err());
        assert!(parse_source_mode("unknown").is_err());

        for value in [
            json!({"id": "read", "method": "workspace_read", "workspace": "w", "path": "/"}),
            json!({"id": "read", "method": "workspace_read", "workspace": "w", "path": "/", "maximum_bytes": []}),
            json!({"id": 7, "method": "health"}),
            json!({"id": "transaction", "method": "workspace_transaction", "workspace": "w", "operations": "not-an-array"}),
            json!({"id": "operation", "method": "workspace_transaction", "workspace": "w", "operations": [{"kind": "unknown", "path": "/"}]}),
            json!({"id": "operation", "method": "workspace_write", "workspace": "w", "path": "/x", "bytes_base64": 7}),
        ] {
            assert!(
                serde_json::from_value::<Request>(value).is_err(),
                "malformed request was accepted"
            );
        }
    }

    #[test]
    fn customer_workspace_protocol_is_idempotent_and_rebases_forks()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        std::thread::Builder::new()
            .name("fsd-customer-workspace-protocol".to_owned())
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(customer_workspace_protocol_inner())
            })?
            .join()
            .map_err(|_| std::io::Error::other("workspace protocol test thread panicked"))?
    }

    #[allow(clippy::too_many_lines)]
    async fn customer_workspace_protocol_inner()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "acyclic-fsd-customer-{}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
            hex::encode(MountId::new().into_bytes())
        ));
        let _ = std::fs::remove_dir_all(&root);
        let state = DaemonState::new(Arc::new(LocalFs::local(acyclic_fs::LocalOptions::new(
            &root,
        ))?));

        let create = state
            .handle(serde_json::from_value(json!({
                "id": "create-workspace",
                "method": "create_workspace",
                "name": "source"
            }))?)
            .await;
        assert!(create.ok, "create failed: {:?}", create.error);
        let created = create.result.as_ref().ok_or("missing create result")?;
        let workspace_id = created["workspaceId"]
            .as_str()
            .ok_or("missing workspace identity")?;
        let initial_generation = created["generationId"]
            .as_str()
            .ok_or("missing initial generation")?;

        let open = state
            .handle(serde_json::from_value(json!({
                "id": "open-workspace",
                "method": "open_workspace",
                "name": "source"
            }))?)
            .await;
        assert!(open.ok, "open failed: {:?}", open.error);
        let opened = open.result.as_ref().ok_or("missing open result")?;
        assert_eq!(opened["workspaceId"].as_str(), Some(workspace_id));
        assert_eq!(opened["generationId"].as_str(), Some(initial_generation));

        let idempotency_key = "00000000000000000000000000000001";
        let write = json!({
            "id": "write-source",
            "method": "workspace_write",
            "workspace": "source",
            "path": "/source.txt",
            "bytes_base64": "c291cmNl",
            "idempotency_key": idempotency_key
        });
        let written = state.handle(serde_json::from_value(write.clone())?).await;
        assert!(written.ok, "write failed: {:?}", written.error);
        assert_eq!(
            written
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("committed")
        );
        let written_generation = written
            .result
            .as_ref()
            .and_then(|value| value["generationId"].as_str())
            .ok_or("missing written generation")?;
        let repeated = state.handle(serde_json::from_value(write)?).await;
        assert!(repeated.ok, "retry failed: {:?}", repeated.error);
        assert_eq!(
            repeated
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("already-committed")
        );
        assert_eq!(
            repeated
                .result
                .as_ref()
                .and_then(|value| value["generationId"].as_str()),
            Some(written_generation)
        );

        let read = state
            .handle(serde_json::from_value(json!({
                "id": "read-source",
                "method": "workspace_read",
                "workspace": "source",
                "path": "/source.txt",
                "maximum_bytes": 6
            }))?)
            .await;
        assert!(read.ok, "read failed: {:?}", read.error);
        assert_eq!(
            read.result
                .as_ref()
                .and_then(|value| value["bytesBase64"].as_str()),
            Some("c291cmNl")
        );
        let bounded = state
            .handle(serde_json::from_value(json!({
                "id": "bounded-read",
                "method": "workspace_read",
                "workspace": "source",
                "path": "/source.txt",
                "maximum_bytes": 5
            }))?)
            .await;
        assert!(!bounded.ok);

        let atomic = state
            .handle(serde_json::from_value(json!({
                "id": "atomic-transaction",
                "method": "workspace_transaction",
                "workspace": "source",
                "idempotency_key": "aaaabbbbccccddddeeeeffff00001111",
                "operations": [
                    { "kind": "create-dir-all", "path": "/tree/nested" },
                    { "kind": "write", "path": "/tree/nested/file", "bytes_base64": "YWJjZGVm" },
                    { "kind": "hard-link", "source": "/tree/nested/file", "destination": "/tree/link" },
                    { "kind": "write-range", "path": "/tree/nested/file", "offset": 2, "bytes_base64": "Wlo=" },
                    { "kind": "copy", "source": "/tree/nested/file", "destination": "/tree/copy" },
                    { "kind": "resize", "path": "/tree/copy", "logical_bytes": 10 },
                    { "kind": "zero-range", "path": "/tree/copy", "offset": 6, "length": 4, "allocated": false, "extend": true },
                    { "kind": "clone-range", "source": "/tree/nested/file", "source_offset": 0, "destination": "/tree/copy", "destination_offset": 0, "length": 6 },
                    { "kind": "rename", "source": "/tree/link", "destination": "/tree/renamed" },
                    { "kind": "create-symbolic-link", "path": "/tree/symlink", "target_base64": "bmVzdGVkL2ZpbGU=" }
                ]
            }))?)
            .await;
        assert!(atomic.ok, "transaction failed: {:?}", atomic.error);
        assert_eq!(
            atomic
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("committed")
        );
        let retained = state
            .handle(serde_json::from_value(json!({
                "id": "begin-retained-transaction",
                "method": "workspace_begin_transaction",
                "workspace": "source",
                "idempotency_key": "2122232425262728292a2b2c2d2e2f30"
            }))?)
            .await;
        assert!(retained.ok, "begin failed: {:?}", retained.error);
        let transaction_id = retained
            .result
            .as_ref()
            .and_then(|value| value["transactionId"].as_str())
            .ok_or("missing retained transaction identity")?;
        let staged = state
            .handle(serde_json::from_value(json!({
                "id": "stage-retained-transaction",
                "method": "workspace_stage_transaction",
                "transaction_id": transaction_id,
                "operation": {
                    "kind": "write",
                    "path": "/retained.txt",
                    "bytes_base64": "cmV0YWluZWQ="
                }
            }))?)
            .await;
        assert!(staged.ok, "stage failed: {:?}", staged.error);
        let retained_commit = state
            .handle(serde_json::from_value(json!({
                "id": "commit-retained-transaction",
                "method": "workspace_commit_transaction",
                "transaction_id": transaction_id
            }))?)
            .await;
        assert!(
            retained_commit.ok,
            "retained commit failed: {:?}",
            retained_commit.error
        );
        assert_eq!(
            retained_commit
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("committed")
        );
        let retained_generation = retained_commit
            .result
            .as_ref()
            .and_then(|value| value["generationId"].as_str())
            .ok_or("missing retained generation")?;
        let retained_retry = state
            .handle(serde_json::from_value(json!({
                "id": "retry-retained-transaction",
                "method": "workspace_commit_transaction",
                "transaction_id": transaction_id
            }))?)
            .await;
        assert!(
            retained_retry.ok,
            "retained retry failed: {:?}",
            retained_retry.error
        );
        assert_eq!(
            retained_retry
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("already-committed")
        );
        assert_eq!(
            retained_retry
                .result
                .as_ref()
                .and_then(|value| value["generationId"].as_str()),
            retained_commit
                .result
                .as_ref()
                .and_then(|value| value["generationId"].as_str())
        );
        let closed = state
            .handle(serde_json::from_value(json!({
                "id": "close-retained-transaction",
                "method": "workspace_close_transaction",
                "transaction_id": transaction_id
            }))?)
            .await;
        assert!(closed.ok, "close failed: {:?}", closed.error);
        assert_eq!(
            closed
                .result
                .as_ref()
                .and_then(|value| value["closed"].as_bool()),
            Some(true)
        );
        let closed_again = state
            .handle(serde_json::from_value(json!({
                "id": "close-retained-transaction-again",
                "method": "workspace_close_transaction",
                "transaction_id": transaction_id
            }))?)
            .await;
        assert!(closed_again.ok);
        assert_eq!(
            closed_again
                .result
                .as_ref()
                .and_then(|value| value["closed"].as_bool()),
            Some(false)
        );
        let retained_read = state
            .handle(serde_json::from_value(json!({
                "id": "read-retained",
                "method": "workspace_read",
                "workspace": "source",
                "path": "/retained.txt",
                "maximum_bytes": 8
            }))?)
            .await;
        assert!(retained_read.ok, "read failed: {:?}", retained_read.error);
        assert_eq!(
            retained_read
                .result
                .as_ref()
                .and_then(|value| value["bytesBase64"].as_str()),
            Some("cmV0YWluZWQ=")
        );
        let atomic_generation = atomic
            .result
            .as_ref()
            .and_then(|value| value["generationId"].as_str())
            .ok_or("missing atomic generation")?;
        for (id, path, expected) in [
            ("read-atomic", "/tree/nested/file", "YWJaWmVm"),
            ("read-hard-link", "/tree/renamed", "YWJaWmVm"),
        ] {
            let response = state
                .handle(serde_json::from_value(json!({
                    "id": id,
                    "method": "workspace_read",
                    "workspace": "source",
                    "path": path,
                    "maximum_bytes": 16
                }))?)
                .await;
            assert!(response.ok, "{id} failed: {:?}", response.error);
            assert_eq!(
                response
                    .result
                    .as_ref()
                    .and_then(|value| value["bytesBase64"].as_str()),
                Some(expected)
            );
        }
        let empty = state
            .handle(serde_json::from_value(json!({
                "id": "empty-transaction",
                "method": "workspace_transaction",
                "workspace": "source",
                "operations": []
            }))?)
            .await;
        assert!(!empty.ok);

        let range = state
            .handle(serde_json::from_value(json!({
                "id": "read-range",
                "method": "workspace_read_range",
                "workspace": "source",
                "path": "/tree/nested/file",
                "offset": 1,
                "length": 3
            }))?)
            .await;
        assert!(range.ok, "range read failed: {:?}", range.error);
        assert_eq!(
            range
                .result
                .as_ref()
                .and_then(|value| value["bytesBase64"].as_str()),
            Some("Ylpa")
        );
        let stat = state
            .handle(serde_json::from_value(json!({
                "id": "stat",
                "method": "workspace_stat",
                "workspace": "source",
                "path": "/tree/renamed"
            }))?)
            .await;
        assert!(stat.ok, "stat failed: {:?}", stat.error);
        assert_eq!(
            stat.result
                .as_ref()
                .and_then(|value| value["linkCount"].as_u64()),
            Some(2)
        );
        let list = state
            .handle(serde_json::from_value(json!({
                "id": "list",
                "method": "workspace_list_directory",
                "workspace": "source",
                "path": "/tree",
                "maximum_entries": 2
            }))?)
            .await;
        assert!(list.ok, "list failed: {:?}", list.error);
        assert_eq!(
            list.result
                .as_ref()
                .and_then(|value| value["entries"].as_array())
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(
            list.result
                .as_ref()
                .and_then(|value| value["hasMore"].as_bool()),
            Some(true)
        );
        let link = state
            .handle(serde_json::from_value(json!({
                "id": "read-link",
                "method": "workspace_read_symbolic_link",
                "workspace": "source",
                "path": "/tree/symlink"
            }))?)
            .await;
        assert!(link.ok, "link read failed: {:?}", link.error);
        assert_eq!(
            link.result
                .as_ref()
                .and_then(|value| value["targetBase64"].as_str()),
            Some("bmVzdGVkL2ZpbGU=")
        );
        let extents = state
            .handle(serde_json::from_value(json!({
                "id": "extents",
                "method": "workspace_plan_extents",
                "workspace": "source",
                "path": "/tree/copy",
                "offset": 0,
                "length": 10,
                "maximum_spans": 8
            }))?)
            .await;
        assert!(extents.ok, "extent plan failed: {:?}", extents.error);
        assert_eq!(
            extents
                .result
                .as_ref()
                .and_then(|value| value["spans"].as_array())
                .map(Vec::len),
            Some(2)
        );
        let old_generation = state
            .handle(serde_json::from_value(json!({
                "id": "old-generation",
                "method": "workspace_read",
                "workspace": "source",
                "generation_id": written_generation,
                "path": "/source.txt",
                "maximum_bytes": 6
            }))?)
            .await;
        assert!(
            old_generation.ok,
            "exact generation read failed: {:?}",
            old_generation.error
        );
        let checkpoint = state
            .handle(serde_json::from_value(json!({
                "id": "checkpoint",
                "method": "workspace_checkpoint",
                "workspace": "source",
                "label": "atomic"
            }))?)
            .await;
        assert!(checkpoint.ok, "checkpoint failed: {:?}", checkpoint.error);
        assert_eq!(
            checkpoint
                .result
                .as_ref()
                .and_then(|value| value["generationId"].as_str()),
            Some(retained_generation)
        );
        let pin = state
            .handle(serde_json::from_value(json!({
                "id": "pin",
                "method": "workspace_pin",
                "workspace": "source",
                "generation_id": written_generation,
                "identity": "pre-atomic"
            }))?)
            .await;
        assert!(pin.ok, "pin failed: {:?}", pin.error);
        assert_eq!(
            pin.result
                .as_ref()
                .and_then(|value| value["generationId"].as_str()),
            Some(written_generation)
        );
        let diff = state
            .handle(serde_json::from_value(json!({
                "id": "diff",
                "method": "workspace_diff",
                "workspace": "source",
                "from_generation_id": written_generation,
                "to_generation_id": atomic_generation,
                "maximum_changes": 64
            }))?)
            .await;
        assert!(diff.ok, "diff failed: {:?}", diff.error);
        assert!(
            diff.result
                .as_ref()
                .and_then(|value| value["files"].as_array())
                .is_some_and(|files| !files.is_empty())
        );

        let delete_create = state
            .handle(serde_json::from_value(json!({
                "id": "delete-create",
                "method": "create_workspace",
                "name": "delete-me"
            }))?)
            .await;
        assert!(delete_create.ok);
        let delete_request = json!({
            "id": "delete",
            "method": "workspace_delete",
            "workspace": "delete-me",
            "idempotency_key": "00000000000000000000000000000002"
        });
        let deleted = state
            .handle(serde_json::from_value(delete_request.clone())?)
            .await;
        assert!(deleted.ok, "delete failed: {:?}", deleted.error);
        assert_eq!(
            deleted
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("deleted")
        );
        let deleted_again = state.handle(serde_json::from_value(delete_request)?).await;
        assert!(
            deleted_again.ok,
            "delete retry failed: {:?}",
            deleted_again.error
        );
        assert_eq!(
            deleted_again
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("already-deleted")
        );

        let fork = state
            .handle(serde_json::from_value(json!({
                "id": "fork",
                "method": "workspace_fork",
                "workspace": "source",
                "destination": "fork"
            }))?)
            .await;
        assert!(fork.ok, "fork failed: {:?}", fork.error);
        assert_ne!(
            fork.result
                .as_ref()
                .and_then(|value| value["workspaceId"].as_str()),
            Some(workspace_id)
        );

        for (id, workspace, path, payload, key) in [
            (
                "write-upstream",
                "source",
                "/upstream.txt",
                "dXBzdHJlYW0=",
                "11112222333344445555666677778888",
            ),
            (
                "write-local",
                "fork",
                "/local.txt",
                "bG9jYWw=",
                "9999aaaabbbbccccddddeeeeffff0000",
            ),
        ] {
            let response = state
                .handle(serde_json::from_value(json!({
                    "id": id,
                    "method": "workspace_write",
                    "workspace": workspace,
                    "path": path,
                    "bytes_base64": payload,
                    "idempotency_key": key
                }))?)
                .await;
            assert!(response.ok, "{id} failed: {:?}", response.error);
        }

        let rebase = state
            .handle(serde_json::from_value(json!({
                "id": "rebase",
                "method": "workspace_live_rebase",
                "workspace": "fork",
                "idempotency_key": "00000000000000000000000000000003",
                "maximum_generations": 32,
                "maximum_changes": 32,
                "maximum_conflicts": 8
            }))?)
            .await;
        assert!(rebase.ok, "rebase failed: {:?}", rebase.error);
        assert_eq!(
            rebase
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("rebased")
        );
        for (id, path, expected) in [
            ("read-upstream", "/upstream.txt", "dXBzdHJlYW0="),
            ("read-local", "/local.txt", "bG9jYWw="),
        ] {
            let response = state
                .handle(serde_json::from_value(json!({
                    "id": id,
                    "method": "workspace_read",
                    "workspace": "fork",
                    "path": path,
                    "maximum_bytes": 16
                }))?)
                .await;
            assert!(response.ok, "{id} failed: {:?}", response.error);
            assert_eq!(
                response
                    .result
                    .as_ref()
                    .and_then(|value| value["bytesBase64"].as_str()),
                Some(expected)
            );
        }

        let join_plan = state
            .handle(serde_json::from_value(json!({
                "id": "join-plan",
                "method": "workspace_plan_join",
                "source": "fork",
                "target": "source",
                "history": "merge",
                "maximum_generations": 64,
                "maximum_changes": 64,
                "maximum_conflicts": 8
            }))?)
            .await;
        assert!(join_plan.ok, "join planning failed: {:?}", join_plan.error);
        let join_plan_value = join_plan.result.as_ref().ok_or("missing join plan")?;
        let plan_id = join_plan_value["planId"]
            .as_str()
            .ok_or("missing join plan identity")?;
        let target_generation = join_plan_value["targetGenerationId"]
            .as_str()
            .ok_or("missing join target")?;
        let apply = json!({
            "id": "join-apply",
            "method": "workspace_apply_join",
            "plan_id": plan_id,
            "if_target_generation_id": target_generation,
            "idempotency_key": "0f0e0d0c0b0a09080706050403020100"
        });
        let joined = state.handle(serde_json::from_value(apply.clone())?).await;
        assert!(joined.ok, "join failed: {:?}", joined.error);
        assert_eq!(
            joined
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("applied")
        );
        let joined_again = state.handle(serde_json::from_value(apply)?).await;
        assert!(
            joined_again.ok,
            "join retry failed: {:?}",
            joined_again.error
        );
        assert_eq!(
            joined_again
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("already-applied")
        );
        let joined_read = state
            .handle(serde_json::from_value(json!({
                "id": "join-read",
                "method": "workspace_read",
                "workspace": "source",
                "path": "/local.txt",
                "maximum_bytes": 16
            }))?)
            .await;
        assert!(
            joined_read.ok,
            "joined read failed: {:?}",
            joined_read.error
        );
        let closed_plan = state
            .handle(serde_json::from_value(json!({
                "id": "join-close",
                "method": "workspace_close_join_plan",
                "plan_id": plan_id
            }))?)
            .await;
        assert!(closed_plan.ok);
        assert_eq!(
            closed_plan
                .result
                .as_ref()
                .and_then(|value| value["closed"].as_bool()),
            Some(true)
        );

        let s3_put_request = json!({
            "id": "s3-put",
            "method": "s3_put_object",
            "workspace": "source",
            "object_key": "artifacts/value.bin",
            "bytes_base64": "YWJjZGVm",
            "idempotency_key": "cafebabecafebabecafebabecafebabe"
        });
        let s3_put = state
            .handle(serde_json::from_value(s3_put_request.clone())?)
            .await;
        assert!(s3_put.ok, "S3 put failed: {:?}", s3_put.error);
        let s3_put_retry = state.handle(serde_json::from_value(s3_put_request)?).await;
        assert!(
            s3_put_retry.ok,
            "S3 put retry failed: {:?}",
            s3_put_retry.error
        );
        assert_eq!(
            s3_put_retry
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("already-committed")
        );
        let s3_head = state
            .handle(serde_json::from_value(json!({
                "id": "s3-head",
                "method": "s3_head_object",
                "workspace": "source",
                "object_key": "artifacts/value.bin"
            }))?)
            .await;
        assert!(s3_head.ok, "S3 head failed: {:?}", s3_head.error);
        assert_eq!(
            s3_head
                .result
                .as_ref()
                .and_then(|value| value["contentLength"].as_u64()),
            Some(6)
        );
        let s3_range = state
            .handle(serde_json::from_value(json!({
                "id": "s3-range",
                "method": "s3_get_object_range",
                "workspace": "source",
                "object_key": "artifacts/value.bin",
                "offset": 2,
                "length": 3
            }))?)
            .await;
        assert!(s3_range.ok, "S3 range failed: {:?}", s3_range.error);
        assert_eq!(
            s3_range
                .result
                .as_ref()
                .and_then(|value| value["bytesBase64"].as_str()),
            Some("Y2Rl")
        );
        let s3_copy = state
            .handle(serde_json::from_value(json!({
                "id": "s3-copy",
                "method": "s3_copy_object",
                "workspace": "source",
                "source_key": "artifacts/value.bin",
                "destination_key": "published/value.bin",
                "idempotency_key": "00000000000000000000000000000004"
            }))?)
            .await;
        assert!(s3_copy.ok, "S3 copy failed: {:?}", s3_copy.error);
        let s3_list = state
            .handle(serde_json::from_value(json!({
                "id": "s3-list",
                "method": "s3_list_objects",
                "workspace": "source",
                "prefix": "",
                "delimiter": "/",
                "maximum_keys": 16,
                "maximum_entries_examined": 128
            }))?)
            .await;
        assert!(s3_list.ok, "S3 list failed: {:?}", s3_list.error);
        assert!(
            s3_list
                .result
                .as_ref()
                .and_then(|value| value["commonPrefixes"].as_array())
                .is_some_and(|prefixes| prefixes.len() >= 2)
        );
        let s3_delete = state
            .handle(serde_json::from_value(json!({
                "id": "s3-delete",
                "method": "s3_delete_objects",
                "workspace": "source",
                "object_keys": ["artifacts/value.bin", "published/value.bin"],
                "idempotency_key": "abcdefabcdefabcdefabcdefabcdefab"
            }))?)
            .await;
        assert!(s3_delete.ok, "S3 delete failed: {:?}", s3_delete.error);
        let multipart = state
            .handle(serde_json::from_value(json!({
                "id": "multipart-create",
                "method": "s3_create_multipart_upload",
                "workspace": "source",
                "object_key": "large/value.bin",
                "maximum_parts": 8,
                "maximum_part_bytes": 16,
                "idempotency_key": "10101010101010101010101010101010"
            }))?)
            .await;
        assert!(
            multipart.ok,
            "multipart create failed: {:?}",
            multipart.error
        );
        let upload_id = multipart
            .result
            .as_ref()
            .and_then(|value| value["uploadId"].as_str())
            .ok_or("missing multipart upload identity")?;
        for (part_number, bytes_base64) in [(2, "c2Vjb25k"), (1, "Zmlyc3Qt")] {
            let part = state
                .handle(serde_json::from_value(json!({
                    "id": format!("multipart-part-{part_number}"),
                    "method": "s3_upload_part",
                    "upload_id": upload_id,
                    "part_number": part_number,
                    "bytes_base64": bytes_base64
                }))?)
                .await;
            assert!(part.ok, "multipart part failed: {:?}", part.error);
        }
        let complete_request = json!({
            "id": "multipart-complete",
            "method": "s3_complete_multipart_upload",
            "upload_id": upload_id,
            "ordered_parts": [1, 2]
        });
        let complete = state
            .handle(serde_json::from_value(complete_request.clone())?)
            .await;
        assert!(
            complete.ok,
            "multipart completion failed: {:?}",
            complete.error
        );
        let complete_retry = state
            .handle(serde_json::from_value(complete_request)?)
            .await;
        assert!(
            complete_retry.ok,
            "multipart completion retry failed: {:?}",
            complete_retry.error
        );
        assert_eq!(
            complete_retry
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("already-committed")
        );
        let abort = state
            .handle(serde_json::from_value(json!({
                "id": "multipart-abort",
                "method": "s3_abort_multipart_upload",
                "upload_id": upload_id
            }))?)
            .await;
        assert!(abort.ok);
        assert_eq!(
            abort
                .result
                .as_ref()
                .and_then(|value| value["aborted"].as_bool()),
            Some(true)
        );

        let source_root = root.with_extension("customer-source");
        let _ = std::fs::remove_dir_all(&source_root);
        std::fs::create_dir(&source_root)?;
        std::fs::write(source_root.join("source.txt"), b"one")?;
        let attached = state
            .handle(serde_json::from_value(json!({
                "id": "source-attach",
                "method": "attach_directory",
                "name": "native-source",
                "path": source_root.to_string_lossy(),
                "mode": "tracking",
                "maximum_paths": 32,
                "maximum_extent_spans": 32,
                "maximum_queued_changes": 32
            }))?)
            .await;
        assert!(attached.ok, "source attach failed: {:?}", attached.error);
        let source_id = attached
            .result
            .as_ref()
            .and_then(|value| value["sourceId"].as_str())
            .ok_or("missing source handle")?;
        let source_state = state
            .handle(serde_json::from_value(json!({
                "id": "source-state",
                "method": "source_state",
                "source_id": source_id
            }))?)
            .await;
        assert!(source_state.ok);
        assert_eq!(
            source_state
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("clean")
        );
        std::fs::write(source_root.join("source.txt"), b"two")?;
        let rescanned = state
            .handle(serde_json::from_value(json!({
                "id": "source-rescan",
                "method": "source_rescan",
                "source_id": source_id
            }))?)
            .await;
        assert!(rescanned.ok, "source rescan failed: {:?}", rescanned.error);
        assert_eq!(
            rescanned
                .result
                .as_ref()
                .and_then(|value| value["status"].as_str()),
            Some("clean")
        );
        let source_read = state
            .handle(serde_json::from_value(json!({
                "id": "source-read",
                "method": "workspace_read",
                "workspace": "native-source",
                "path": "/source.txt",
                "maximum_bytes": 3
            }))?)
            .await;
        assert!(
            source_read.ok,
            "source read failed: {:?}",
            source_read.error
        );
        assert_eq!(
            source_read
                .result
                .as_ref()
                .and_then(|value| value["bytesBase64"].as_str()),
            Some("dHdv")
        );
        let sealed = state
            .handle(serde_json::from_value(json!({
                "id": "source-seal",
                "method": "source_seal",
                "source_id": source_id
            }))?)
            .await;
        assert!(sealed.ok, "source seal failed: {:?}", sealed.error);
        let source_close = state
            .handle(serde_json::from_value(json!({
                "id": "source-close",
                "method": "source_close",
                "source_id": source_id
            }))?)
            .await;
        assert!(source_close.ok);

        let malformed = state
            .handle(serde_json::from_value(json!({
                "id": "malformed-key",
                "method": "workspace_remove",
                "workspace": "fork",
                "path": "/local.txt",
                "idempotency_key": "00"
            }))?)
            .await;
        assert!(!malformed.ok);

        drop(state);
        std::fs::remove_dir_all(root)?;
        std::fs::remove_dir_all(source_root)?;
        Ok(())
    }
}
