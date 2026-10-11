//! Selected controls execute inside the ORIGINAL command owner, never a source worker.
use crate::{AdapterState, PiTaskBinding, PluginTaskBinding, ServiceResources,
    pi_native_host::{PiNativeCommandHost, PiNativeFilesystem, PiNativeRuntime},
    pi_owner::PiOwnerConfig, pi_root_provider::PiRootHandle,
    pi_task::{PiTaskExecution, PiTaskInput}, pi_root_inputs::PiRootRequest,
    pi_fork::{PiForkCoordinator, original_operation, derived}, pi_inspection::*};
use acyclic_fs::{CancellationToken, LazySnapshotRef, WorkCounters};
use acyclic_harness::{Error, Result, TaskId, OperationId, durable_host::TaskJournalOwner,
    filesystem::{TaskCommandHost, TaskCommandProgress, TaskSourceInspection},
    runtime::{TaskContext, NativeTaskContext}, scheduler::LeaseFence, workflow::WorkflowCommand,
    conversation::VolumeRef, resources::GenerationRef};
use acyclic_stream::{LocalStream, StreamClient, BoxProviderFuture};
use serde::{Serialize, Deserialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
use base64::Engine as _;

/// Public metadata from the original Root observe-source command ACK. This
/// deliberately cannot carry a Root context, owner, grant, signer or lease.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootSourceCut {
    pub(crate) snapshots: BTreeMap<String, LazySnapshotRef>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiSelectedCommand {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) source: TaskId,
    pub(crate) root: Option<String>,
    #[serde(default)] pub(crate) directory: String,
    pub(crate) cursor: Option<String>,
    pub(crate) maximum_entries: Option<u32>,
    pub(crate) destination: Option<TaskId>,
    #[serde(default)] pub(crate) paths: Vec<String>,
    #[serde(default)] pub(crate) confirm: bool,
    pub(crate) approval_digest: Option<String>,
    pub(crate) import_operation: Option<OperationId>,
}

pub(crate) struct PiSelectedHost<'a> {
    runtime: Arc<PiNativeRuntime>,
    filesystem: Arc<PiNativeFilesystem>,
    stream: StreamClient<LocalStream>,
    resources: ServiceResources,
    protected: PiOwnerConfig,
    retained: &'a AdapterState,
    roots: &'a BTreeMap<String, PiRootHandle>,
    native: Arc<PiNativeCommandHost>,
    root_source_cut: Option<&'a PiRootSourceCut>,
    cancellation: Option<CancellationToken>,
    work: Option<acyclic_fs::NativeMaterializationWork>,
}

impl<'a> PiSelectedHost<'a> {
    #[allow(clippy::too_many_arguments, reason = "original providers, catalog and creator handles are independent")]
    pub(crate) fn new(runtime: Arc<PiNativeRuntime>, filesystem: Arc<PiNativeFilesystem>,
        stream: StreamClient<LocalStream>, resources: ServiceResources, protected: PiOwnerConfig,
        retained: &'a AdapterState, roots: &'a BTreeMap<String, PiRootHandle>,
        native: Arc<PiNativeCommandHost>) -> Result<Self> {
        runtime.verify_owner(&protected)?;
        Ok(Self { runtime, filesystem, stream, resources, protected, retained, roots, native,
            root_source_cut: None, cancellation: None, work: None })
    }
    pub(crate) fn with_root_source_cut(mut self, cut: Option<&'a PiRootSourceCut>) -> Self {
        self.root_source_cut = cut; self
    }
    pub(crate) fn with_invocation_cancellation(mut self, token: &CancellationToken) -> Self {
        self.cancellation = Some(token.clone()); self
    }
    pub(crate) fn with_work(mut self, work: acyclic_fs::NativeMaterializationWork) -> Self {
        self.cancellation = Some(work.cancellation().clone());
        self.work = Some(work);
        self
    }
    fn coordinator(&self) -> Result<PiForkCoordinator> {
        PiForkCoordinator::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            self.resources.clone(), self.protected.clone())
    }
    fn root_binding(&self) -> Result<&PiTaskBinding> {
        match &self.retained.task_binding {
            PluginTaskBinding::Pi(binding) if binding.task_id == self.protected.policy().root_task => Ok(binding),
            _ => Err(denied("selected Root binding differs from the original protected allocation")),
        }
    }
    fn project_key(&self, binding: &PiTaskBinding, requested: Option<&str>) -> Result<String> {
        match requested {
            Some(key) if binding.project_volumes.contains_key(key) => Ok(key.into()),
            Some(_) => Err(denied("selected project is not in the original source binding")),
            None if binding.project_volumes.len() == 1 => Ok(binding.project_volumes.keys().next().expect("one project").clone()),
            None => Err(invalid("select an explicit root when the source does not have exactly one project")),
        }
    }
    async fn context(&self, context: TaskContext, fence: LeaseFence, cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork) -> Result<(TaskContext, Arc<TaskJournalOwner<LocalStream>>, PiTaskInput, acyclic_fs::NativeAllocation)> { let task = context.durable_task_id().ok_or_else(|| denied("selected control has no original caller"))?;
    let owner = Arc::new(self.runtime.runtime.task_host().journal_owner(task, fence).await?
        .with_invocation_cancellation(cancellation));
    owner.native_view_owner().verify_with_native_work(work).await?;
    let decoded = self.admission_input(task, work).await?;
    let input = &decoded.0;
    if input.binding.task_id != task { return Err(denied("selected admission changed the original caller")); }
    let context = if task == self.protected.policy().root_task {
        let PiTaskExecution::Host { root, .. } = &input.execution else { return Err(denied("selected Root became a child")); };
        let request = PiRootRequest { binding: input.binding.clone(), root: root.clone() };
        consume_root(work, crate::pi_root_provider::bind_root_output_context(context, &owner, self.protected.clone(),
            self.resources.clone(), self.filesystem.clone(), self.stream.clone(), &request, work.remaining().map_err(native_error)?, cancellation).await)?
    } else { context.with_output_volume(&input.binding.scratch.volume)? };
    Ok((context, owner, decoded.0, decoded.1)) }
    async fn native_caller(&self, context: &TaskContext, fence: LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(Arc<TaskJournalOwner<LocalStream>>, PiTaskInput, acyclic_fs::NativeAllocation)> {
        let task = context.durable_task_id().ok_or_else(|| denied("selected native control lacks its original caller"))?;
        let owner = self.runtime.runtime.task_host().journal_owner_with_native_work(task, fence, work).await?
            .with_invocation_cancellation(work.cancellation());
        owner.verify_with_native_work(false, work).await?;
        let owner = owner.into_native_shared(work)?;
        let native_owner = owner.native_view_owner();
        let decoded = self.admission_input(task, work).await?;
        let input = &decoded.0;
        if input.binding.task_id != task {
            return Err(denied("selected native admission changed its original caller"));
        }
        if task == self.protected.policy().root_task && !matches!(&input.execution, PiTaskExecution::Host { .. }) {
            return Err(denied("selected native Root became a child"));
        }
        let scratch = &input.binding.scratch.volume;
        if context.output_volume() != Some(scratch) {
            return Err(denied("selected native output is not its original sealed private scratch"));
        }
        if !context.scope().grants().contains(&scratch.capability(acyclic_harness::conversation::VolumeOperation::Write)?) {
            return Err(denied("selected native context lacks its original private scratch WRITE grant"));
        }
        native_owner.require_volume_grant_with_native_work(scratch,
            acyclic_harness::conversation::VolumeOperation::Write, work)?;
        // The native dispatcher supplies the canonical, already bound Root or
        // sealed child scratch context. Never clone or rebind that context here.
        Ok((owner, decoded.0, decoded.1))
    }
    async fn admission_input(&self, task: TaskId, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(PiTaskInput, acyclic_fs::NativeAllocation)> {
        let admission = self.runtime.runtime.task_host().observe_admission_with_native_work(task, work).await?;
        if admission.0.task_id() != task { return Err(denied("selected metadata substituted its actual admission task")); }
        let maximum = self.protected.policy().maximum_payload_bytes.min(crate::MAXIMUM_ADAPTER_STATE_BYTES) as u64;
        let encoded = crate::pi_root_provider::guarded_pi_bytes(&admission.0.input, maximum, work)?;
        drop(admission);
        let result = crate::pi_root_provider::guarded_pi_decode(&encoded.0, maximum,
            maximum.checked_mul(8).ok_or_else(|| invalid("admission ownership bound overflow"))?, work);
        drop(encoded);
        result
    }
    pub(crate) async fn selected(&self, context: &TaskContext, owner: &Arc<TaskJournalOwner<LocalStream>>,
        task: TaskId, requested: Option<&str>, pinned: Option<LazySnapshotRef>, operation: OperationId, cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork) -> Result<(String, VolumeRef, Arc<crate::LocalLazyWorkspace>, LazySnapshotRef,
        TaskSourceInspection<LocalStream, crate::LocalAuthorityBackend, crate::LocalObjectBackend>)> { authorize_selected_source(&self.runtime.runtime, self.protected.policy().root_task,
        context, owner, task, self.protected.policy().session_limits.depth, work).await?;
    let decoded = self.admission_input(task, work).await?;
    let input = &decoded.0;
    let key = self.project_key(&input.binding, requested)?;
    let volume = input.binding.project_volumes[&key].clone();
    let workspace = if task == self.protected.policy().root_task {
        Arc::new(self.roots.get(&key).ok_or_else(|| denied("selected Root lost its actual lazy allocation"))?.workspace.clone())
    } else { self.native.live_workspace(task).ok_or_else(|| denied("selected child lost its actual creator workspace"))? };
    let frozen = if let Some(pinned) = pinned { pinned } else if task == self.protected.policy().root_task {
        let actual = self.roots.get(&key).ok_or_else(|| denied("selected Root allocation unavailable"))?;
        if owner.task_id() == task {
            consume_root(work, crate::pi_lazy_capture::freeze_root_source(task, context, owner, &actual.physical,
                &workspace, work.remaining().map_err(native_error)?, cancellation).await)?
        } else { *self.root_source_cut.and_then(|cut| cut.snapshots.get(&key))
            .ok_or_else(|| denied("foreign Root source needs its original observe-source ACK"))? }
    } else {
        let (snapshot, custody) = self.native.selection_snapshot(task, work).await?;
        drop(custody);
        snapshot
    };
    let handle = self.coordinator()?.with_work(work.clone()).authorize_selected_project(context, owner, task, workspace.clone(), frozen,
        operation, work.remaining().map_err(native_error)?.items_examined, self.protected.policy().limits.file_bytes,
        work.remaining().map_err(native_error)?, cancellation.clone()).await?;
    let (native_snapshot, snapshot_custody) = crate::pi_root_provider::guarded_pi_value(
        &frozen, acyclic_harness::conversation::MAX_CONTROL_BYTES as u64, work)?;
    let selection = acyclic_harness::source_authorization::ProjectSourceSnapshot {
        project: volume.clone(), generation: GenerationRef::new(volume.provider().clone(),
            frozen.authored_generation.digest().into_bytes(), None)?,
        native_snapshot: Some(native_snapshot),
    };
    let inspection = self.runtime.runtime.inspect_project_source_with_native_work(context, owner.clone(), handle,
        operation, task, &selection, work).await?;
    drop(selection);
    drop(snapshot_custody);
    Ok((key, volume, workspace, frozen, inspection)) }
    async fn run(&self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value, reconcile: bool) -> Result<TaskCommandProgress> {
        let current = context.durable_task_id().ok_or_else(|| denied("selected control lacks its actual caller context"))?;
        let cancellation = if current == self.protected.policy().root_task {
            self.cancellation.clone()
        } else { self.native.invocation_cancellation(current)? }
            .ok_or_else(|| denied("selected control lacks its original invocation cancellation"))?;
        let cancellation = &cancellation;
        let work = if current == self.protected.policy().root_task {
            self.work.as_ref().cloned().ok_or_else(|| denied("Root selected control lacks its ORIGINAL command work ledger"))?
        } else { self.native.invocation_work(current).await? };
        let work = &work;
        let caller_custody = self.context(context, fence, cancellation, work).await?;
        let (context, owner, caller, _) = &caller_custody;
        self.run_bound(context, owner, caller, command, payload, reconcile, cancellation, work).await
    }
    async fn run_native(&self, context: Arc<NativeTaskContext>, fence: LeaseFence,
        command: WorkflowCommand, payload: Value, reconcile: bool,
        work: &acyclic_fs::NativeMaterializationWork) -> Result<TaskCommandProgress> {
        let caller_custody = self.native_caller(context.context(), fence, work).await?;
        let (owner, caller, _) = &caller_custody;
        self.run_bound(context.context(), owner, caller, command, payload, reconcile,
            work.cancellation(), work).await
    }
    async fn run_bound(&self, context: &TaskContext, owner: &Arc<TaskJournalOwner<LocalStream>>,
        caller: &PiTaskInput, command: WorkflowCommand, payload: Value, reconcile: bool,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<TaskCommandProgress> {
        let encoded = crate::pi_root_provider::guarded_pi_bytes(&payload,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work)?;
        drop(payload);
        let decoded = crate::pi_root_provider::guarded_pi_decode::<PiSelectedCommand>(&encoded.0,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)?;
        drop(encoded);
        let input = &decoded.0;
        if input.caller != owner.task_id() || input.sdk_operation.is_empty() {
            return Err(denied("selected control changed its actual caller or SDK operation"));
        }
        let operation = original_operation(self.protected.policy().root_task, input.caller, &input.sdk_operation);
        let role = match command.kind.as_str() {
            "acyclic.pi.inspect" => "selected-inspect", "acyclic.pi.diff" => "selected-diff",
            "acyclic.pi.import" => "selected-import",
            "acyclic.pi.writeback" if input.approval_digest.is_some() => "selected-writeback-publish",
            "acyclic.pi.writeback" => "selected-writeback",
            _ => return Err(Error::Unsupported("not a selected Pi command".into())),
        };
        if command.operation_id != derived(operation, role) { return Err(denied("selected command changed its original effect operation")); }
        let cursor_custody = input.cursor.as_deref().map(|cursor| decode_cursor::<CursorEnvelope>(cursor, cancellation, work)).transpose()?;
        let cursor = cursor_custody.as_ref().map(|(cursor, _)| cursor);
        if let Some(cursor) = &cursor {
            if cursor.caller != input.caller || cursor.source != input.source || cursor.kind != command.kind
                || cursor.directory != input.directory || input.root.as_deref().is_some_and(|root| root != cursor.root) {
                return Err(denied("selected cursor changed caller, source, directory, root or control kind"));
            }
        }
        let pinned = cursor.as_ref().and_then(|cursor| match &cursor.cursor {
            PageCursor::Inspect(cursor) => Some(cursor.snapshot), PageCursor::Diff(cursor) => Some(cursor.after_snapshot()),
        });
        let base_selection_operation = derived(operation, if command.kind == "acyclic.pi.writeback" {
            "selected-writeback" } else { "selected-import" });
        let import_operation = if command.kind == "acyclic.pi.writeback" {
            derived(base_selection_operation, "writeback-import") } else { base_selection_operation };
        let prior_import = if matches!(command.kind.as_str(), "acyclic.pi.import" | "acyclic.pi.writeback") {
            self.read_import_intent(&context, import_operation, cancellation, work).await?
        } else { None };
        let pinned = pinned.or_else(|| prior_import.as_ref().map(|(intent, _)| intent.source_snapshot));
        let (key, volume, workspace, frozen, inspection) = self.selected(&context, &owner, input.source,
            input.root.as_deref(), pinned, operation, cancellation, work).await?;
        if cursor.is_some_and(|cursor| cursor.root != key) {
            return Err(denied("selected cursor changed its actual resolved source root"));
        }
        let selected = PiNativeSelectedSource::select(&inspection, input.source, input.caller, &volume, &workspace, frozen, work)?;
        let maximum = input.maximum_entries.unwrap_or(128);
        match command.kind.as_str() {
            "acyclic.pi.inspect" => {
                let continuation = match cursor.as_ref().map(|cursor| &cursor.cursor) {
                    Some(PageCursor::Inspect(cursor)) => Some(cursor), None => None,
                    _ => return Err(denied("diff continuation used for inspection")),
                };
                let page = consume_root(work, selected.inspect(&context, &owner, &input.directory, continuation, maximum, cancellation, work).await)?;
                let continuation = page.cursor.map(|cursor| self.cursor(input, &key, &command.kind, PageCursor::Inspect(cursor), cancellation, work)).transpose()?;
                ready(&PiPageResponse { source: input.source, root: &key, volume: Some(&volume),
                    before_root: None, before_snapshot: None, snapshot: frozen, directory: &page.directory,
                    entries: &page.entries, cursor: continuation.as_ref().map(|(cursor, _)| cursor.as_str()) }, work)
            }
            "acyclic.pi.diff" => {
                let baseline = self.native.frozen_source(input.source).ok_or_else(|| denied("diff requires the child's actual ORIGINAL frozen source"))?;
                let source_task = self.native.selected_source_task(input.source)
                    .ok_or_else(|| denied("diff lost the original authorized selected source task"))?;
                let (before_key, before_volume, before_workspace, before_frozen, before_inspection) = self.selected(&context, &owner,
                    source_task, Some(&key), Some(baseline), operation, cancellation, work).await?;
                let before = PiNativeSelectedSource::select(&before_inspection, source_task, input.caller,
                    &before_volume, &before_workspace, before_frozen, work)?;
                let continuation = match cursor.as_ref().map(|cursor| &cursor.cursor) {
                    Some(PageCursor::Diff(cursor)) => Some(cursor), None => None,
                    _ => return Err(denied("inspection continuation used for diff")),
                };
                let page = consume_root(work, diff_native(&before, &selected, &context, &owner, &input.directory, continuation, maximum, cancellation, work).await)?;
                let continuation = page.cursor.map(|cursor| self.cursor(input, &key, &command.kind, PageCursor::Diff(cursor), cancellation, work)).transpose()?;
                ready(&PiPageResponse { source: input.source, root: &key, volume: None,
                    before_root: Some(&before_key), before_snapshot: Some(before_frozen), snapshot: frozen,
                    directory: &page.directory, entries: &page.entries, cursor: continuation.as_ref().map(|(cursor, _)| cursor.as_str()) }, work)
            }
            "acyclic.pi.import" | "acyclic.pi.writeback" => {
                if command.kind == "acyclic.pi.writeback" && input.destination.is_some_and(|destination| destination != input.caller) {
                    return Err(denied("writeback destination must be the ORIGINAL Root caller"));
                }
                self.publish_selection(&context, &owner, &caller.binding, &input, &key, &selected,
                    derived(operation, if command.kind == "acyclic.pi.writeback" { "selected-writeback" } else { "selected-import" }),
                    command.kind == "acyclic.pi.writeback", reconcile, cancellation, work).await
            }
            _ => unreachable!("kind checked above"),
        }
    }
    fn cursor(&self, input: &PiSelectedCommand, root: &str, kind: &str, cursor: PageCursor,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork) -> Result<(String, acyclic_fs::NativeAllocation)> {
        encode_cursor(&CursorEnvelopeRef { caller: input.caller, source: input.source, root,
            kind, directory: &input.directory, cursor: &cursor }, cancellation, work)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorEnvelope {
    caller: TaskId, source: TaskId, root: String, kind: String, directory: String, cursor: PageCursor,
}
#[derive(Serialize)]
struct CursorEnvelopeRef<'a> {
    caller: TaskId, source: TaskId, root: &'a str, kind: &'a str, directory: &'a str, cursor: &'a PageCursor,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
enum PageCursor { Inspect(PiNativeInspectionCursor), Diff(PiNativeDiffCursor) }

fn encode_cursor<T: Serialize>(value: &T, cancellation: &CancellationToken,
    work: &acyclic_fs::NativeMaterializationWork) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let maximum = (crate::MAXIMUM_CONTROL_MESSAGE_BYTES / 2) as u64;
    let (encoded, encoded_custody) = crate::pi_root_provider::guarded_pi_bytes(value, maximum, work)?;
    let result = (|| {
        let hex_length = (encoded.len() as u64).checked_mul(2).ok_or_else(|| invalid("cursor hex length overflow"))?;
        let hex_custody = work.claim(hex_length).map_err(native_error)?;
        work.charge(WorkCounters { bytes_copied: hex_length, allocation_operations: u64::from(hex_length != 0),
            ..WorkCounters::default() }).map_err(native_error)?;
        cancellation.check().map_err(storage)?;
        Ok((hex::encode(&encoded), hex_custody))
    })();
    drop(encoded);
    drop(encoded_custody);
    result
}
fn decode_cursor<T: serde::de::DeserializeOwned>(value: &str, cancellation: &CancellationToken,
    work: &acyclic_fs::NativeMaterializationWork) -> Result<(T, acyclic_fs::NativeAllocation)> {
    if value.len() > crate::MAXIMUM_CONTROL_MESSAGE_BYTES || value.len() % 2 != 0 {
        return Err(invalid("cursor exceeds bounded transport"));
    }
    cancellation.check().map_err(storage)?;
    let length = value.len() as u64 / 2;
    let bytes_custody = work.claim(length).map_err(native_error)?;
    work.charge(WorkCounters { bytes_copied: length, allocation_operations: u64::from(length != 0),
        ..WorkCounters::default() }).map_err(native_error)?;
    let bytes = hex::decode(value).map_err(storage)?;
    let result = crate::pi_root_provider::guarded_pi_decode(&bytes,
        (crate::MAXIMUM_CONTROL_MESSAGE_BYTES / 2) as u64,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work);
    drop(bytes);
    drop(bytes_custody);
    result
}
fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason }
}
fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized, reason }
}
fn storage(error: impl std::fmt::Display) -> Error { Error::Storage(error.to_string()) }
fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}
fn native_workspace_error((source, custody): (acyclic_fs::WorkspaceError, Option<acyclic_fs::NativeAllocation>)) -> Error {
    Error::NativeWorkspacePublication {
        source: acyclic_fs::NativeWorkspacePublicationError::Workspace(source), custody,
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedImportIntent {
    caller: TaskId, source: TaskId, root: String, destination: VolumeRef,
    source_snapshot: LazySnapshotRef, destination_snapshot: LazySnapshotRef,
    paths: Vec<String>, operation: OperationId, writeback: bool,
}

#[derive(Clone, Copy, Serialize)]
struct SelectedImportIntentRef<'a> {
    caller: TaskId, source: TaskId, root: &'a str, destination: &'a VolumeRef,
    source_snapshot: LazySnapshotRef, destination_snapshot: LazySnapshotRef,
    paths: &'a [String], operation: OperationId, writeback: bool,
}

impl<'a> From<&'a SelectedImportIntent> for SelectedImportIntentRef<'a> {
    fn from(value: &'a SelectedImportIntent) -> Self {
        Self { caller: value.caller, source: value.source, root: &value.root,
            destination: &value.destination, source_snapshot: value.source_snapshot,
            destination_snapshot: value.destination_snapshot, paths: &value.paths,
            operation: value.operation, writeback: value.writeback }
    }
}

/// Metadata from a completed canonical SDK selected import, retained under the
/// original immutable Pi input operation; never a replacement publication grant.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiSelectedImportReceipt {
    pub(crate) caller: TaskId,
    pub(crate) source: TaskId,
    pub(crate) root: String,
    pub(crate) destination: VolumeRef,
    pub(crate) selected_snapshot: LazySnapshotRef,
    pub(crate) destination_snapshot: LazySnapshotRef,
    pub(crate) imported: LazySnapshotRef,
    pub(crate) paths: Vec<String>,
    pub(crate) operation: OperationId,
}

#[derive(Clone, Copy, Serialize)]
pub(crate) struct PiSelectedImportReceiptRef<'a> {
    pub(crate) caller: TaskId,
    pub(crate) source: TaskId,
    pub(crate) root: &'a str,
    pub(crate) destination: &'a VolumeRef,
    pub(crate) selected_snapshot: LazySnapshotRef,
    pub(crate) destination_snapshot: LazySnapshotRef,
    pub(crate) imported: LazySnapshotRef,
    pub(crate) paths: &'a [String],
    pub(crate) operation: OperationId,
}

impl<'a> From<&'a PiSelectedImportReceipt> for PiSelectedImportReceiptRef<'a> {
    fn from(value: &'a PiSelectedImportReceipt) -> Self {
        Self { caller: value.caller, source: value.source, root: &value.root,
            destination: &value.destination, selected_snapshot: value.selected_snapshot,
            destination_snapshot: value.destination_snapshot, imported: value.imported,
            paths: &value.paths, operation: value.operation }
    }
}

impl PiSelectedHost<'_> {
    async fn publish_selection(&self, context: &TaskContext, owner: &Arc<TaskJournalOwner<LocalStream>>,
        binding: &PiTaskBinding, input: &PiSelectedCommand, key: &str,
        selected: &PiNativeSelectedSource<'_, crate::LocalAuthorityBackend, crate::LocalObjectBackend,
            crate::LocalLazySource, crate::LocalCoreStateStore>,
        operation: OperationId, writeback: bool, reconcile: bool, cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork) -> Result<TaskCommandProgress> { if writeback && (input.caller != self.protected.policy().root_task || input.source == input.caller) {
        return Err(denied("writeback requires the ORIGINAL Root caller and a separately selected child"));
    }
    if input.import_operation.is_some() {
        return Err(invalid("writeback imports under the same original SDK operation; external import keys are not accepted"));
    }
    let destination = input.destination.unwrap_or(input.caller);
    let foreign_binding = if destination != input.caller && destination != self.protected.policy().root_task {
        Some(self.native.binding_with_work(destination, work)?
            .ok_or_else(|| denied("explicit destination has no actual creator binding"))?)
    } else { None };
    let destination_binding = if destination == input.caller { binding }
        else if destination == self.protected.policy().root_task { self.root_binding()? }
        else { &foreign_binding.as_ref().ok_or_else(|| denied("explicit destination binding unavailable"))?.0 };
    let volume = destination_binding.project_volumes.get(key).ok_or_else(|| denied("destination lacks this actual project"))?;
    let write_capability = volume.capability_with_native_work(acyclic_harness::conversation::VolumeOperation::Write, work)?;
    if !context.scope().grants().contains(&write_capability.0) {
        return Err(denied("current caller has no actual WRITE grant for the explicit destination"));
    }
    let authorization = self.protected.conversation_authority_with_native_work(
        input.caller, binding.agent_id, context.scope().grants(), work)?;
    let verifier = authorization.0.1.verifier_with_native_work(work)?;
    let scope = &authorization.0.2;
    let destination_grant = acyclic_harness::conversation::ContentGrant::verify_with_native_work(
        &verifier.0, scope, volume, acyclic_harness::conversation::VolumeOperation::Write, work)?;
    if destination != input.caller && destination == self.protected.policy().root_task {
        return Err(denied("delegated Root mutation requires its original mediated Root workflow command"));
    }
    let destination_live = if destination != input.caller {
        Some(self.native.live_source(destination, work).await?.ok_or_else(|| denied("explicit destination has no ORIGINAL live owner"))?)
    } else { None };
    let destination_context = destination_live.as_ref().map_or(context, |live| &live.context);
    let destination_owner = destination_live.as_ref().map_or(owner, |live| &live.owner);
    let child_workspace = if destination == self.protected.policy().root_task { None } else {
        Some(self.native.live_workspace(destination).ok_or_else(|| denied("destination child lost actual creator workspace"))?)
    };
    let workspace = if let Some(workspace) = &child_workspace { workspace.as_ref() } else {
        &self.roots.get(key).ok_or_else(|| denied("destination Root lost actual lazy allocation"))?.workspace
    };
    let mount = if destination == self.protected.policy().root_task { None } else {
        Some(self.native.project_mount(destination, volume).ok_or_else(|| denied("destination child has no actual live project mount"))?)
    };
    let publication = if let Some(view) = self.native.live_view(destination) {
        view.project_publication(volume).ok_or_else(|| denied("destination lacks its ORIGINAL project publication permit"))?
    } else if destination == self.protected.policy().root_task {
        // The ordinary Root volume factory is logical, not a native mount;
        // its original permit is Unrestricted. The SDK per-seal owner guard
        // below is mandatory and retains the actual original WRITE grant.
        acyclic_fs::PublicationPermit::Unrestricted
    } else { return Err(denied("destination live owner unavailable")); };
    let import_operation = if writeback { derived(operation, "writeback-import") } else { operation };
    let prior = self.read_import_intent(context, import_operation, cancellation, work).await?;
    let resuming_import = prior.is_some();
    let intent = if let Some((intent, _custody)) = &prior {
        if intent.caller != input.caller || intent.source != input.source || intent.root != key
            || &intent.destination != volume || intent.paths != input.paths || intent.operation != import_operation
            || intent.writeback != writeback {
            return Err(denied("original selected import retry changed its sealed request"));
        }
        SelectedImportIntentRef::from(intent)
    } else {
        if reconcile { return Ok(TaskCommandProgress::Pending); }
        let destination_snapshot = if destination == self.protected.policy().root_task {
            let root = self.roots.get(key).ok_or_else(|| denied("actual original Root unavailable"))?;
            consume_root(work, crate::pi_lazy_capture::freeze_root_source(input.caller, context, owner, &root.physical,
                &workspace, work.remaining().map_err(native_error)?, cancellation).await)?
        } else {
            let view = self.native.live_view(destination).ok_or_else(|| denied("destination original native view unavailable"))?;
            let (snapshot, custody) = crate::pi_lazy_capture::freeze_child_source(
                destination_context, destination_owner, &view, &workspace, work).await?;
            drop(custody);
            snapshot
        };
        let intent = SelectedImportIntentRef { caller: input.caller, source: input.source, root: key,
            destination: volume, source_snapshot: selected.snapshot(), destination_snapshot,
            paths: &input.paths, operation: import_operation, writeback };
        let staged = crate::pi_root_provider::stage_pi_input_with_native_work(&self.protected,
            &self.filesystem, import_operation, "selected-import-intent", &intent, work).await?;
        drop(staged);
        intent
    };
    if selected.snapshot() != intent.source_snapshot {
        return Err(denied("selected import changed its original pinned source snapshot"));
    }
    let fs_operation = acyclic_fs::OperationId::from_bytes(import_operation.into_bytes());
    owner.native_view_owner().verify_with_native_work(work).await?;
    destination_owner.native_view_owner().verify_with_native_work(work).await?;
    let completed = if let Some(mount) = &mount {
        let result = mount.selected_import_snapshot_with_work(fs_operation, &work.native_mount_work(), cancellation).await;
        if let Err(acyclic_fs::NativeMountError::Source(error)) = &result {
            if matches!(error.semantic_error(), acyclic_fs::MountSourceError::OriginalSnapshotUnavailable
                | acyclic_fs::MountSourceError::Indeterminate) {
                return Ok(TaskCommandProgress::Pending);
            }
        }
        result.map_err(Error::from)?
    } else {
        let result = workspace.selected_import_snapshot_with_native_work(fs_operation, work, cancellation).await;
        if let Err(source) = &result {
            if matches!(source.semantic_error(), acyclic_fs::LazyWorkspaceError::OriginalSnapshotUnavailable)
                || matches!(source.semantic_error(), acyclic_fs::LazyWorkspaceError::SelectedImportIndeterminate { operation } if *operation == fs_operation) {
                return Ok(TaskCommandProgress::Pending);
            }
        }
        result.map_err(|source| Error::NativeSource { source, custody: None })?
    };
    let imported = if let Some(imported) = completed { imported } else {
        if reconcile || resuming_import { return Ok(TaskCommandProgress::Pending); }
        let result = consume_root(work, import_native_project(selected, context, owner, destination_context, destination_owner,
            &workspace, mount.as_deref(), volume,
            intent.destination_snapshot, &verifier.0, scope, intent.paths, import_operation, publication,
            acyclic_harness::conversation::MAX_PRIVATE_DIRECTORY_PAGE, self.protected.policy().limits.file_bytes,
            cancellation, work).await);
        if let Err(Error::NativeSource { source, .. }) = &result {
            if matches!(source.semantic_error(), acyclic_fs::LazyWorkspaceError::SelectedImportIndeterminate { operation } if *operation == fs_operation) {
                return Ok(TaskCommandProgress::Pending);
            }
        }
        result?
    };
    let import_receipt = PiSelectedImportReceiptRef {
        caller: intent.caller, source: intent.source, root: intent.root,
        destination: intent.destination, selected_snapshot: intent.source_snapshot,
        destination_snapshot: intent.destination_snapshot, imported, paths: intent.paths,
        operation: import_operation,
    };
    owner.native_view_owner().verify_with_native_work(work).await?;
    destination_owner.native_view_owner().verify_with_native_work(work).await?;
    let staged = crate::pi_root_provider::stage_pi_input_with_native_work(&self.protected,
        &self.filesystem, import_operation, "selected-import-receipt", &import_receipt, work).await?;
    drop(staged);
    owner.native_view_owner().verify_with_native_work(work).await?;
    if !writeback {
        return ready(&PiImportResponse { status: "imported", source: input.source, destination,
            root: key, operation: &input.sdk_operation, original_operation: import_operation, snapshot: imported }, work);
    }
    self.writeback(context, owner, input, key, volume, selected.volume(), imported, operation, publication,
        &verifier.0, scope, import_receipt, reconcile, cancellation, work).await }
    async fn read_import_intent(&self, context: &TaskContext, operation: OperationId, _cancellation: &CancellationToken,
        work: &acyclic_fs::NativeMaterializationWork) -> Result<Option<(SelectedImportIntent, acyclic_fs::NativeAllocation)>> {
        crate::pi_root_provider::read_pi_input_with_native_work(&self.protected, &self.filesystem,
            operation, "selected-import-intent", crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            context, work).await
    }
    #[allow(clippy::too_many_arguments, reason = "original owner, frozen evidence and prepared approval are independent")]
    async fn writeback(&self, context: &TaskContext, owner: &Arc<TaskJournalOwner<LocalStream>>,
        input: &PiSelectedCommand, key: &str, volume: &VolumeRef, source_volume: &VolumeRef, imported: LazySnapshotRef,
        operation: OperationId, publication: acyclic_fs::PublicationPermit,
        verifier: &acyclic_harness::core::AuthorityVerifier, scope: &acyclic_harness::core::Scope,
        import_receipt: PiSelectedImportReceiptRef<'_>, reconcile: bool, cancellation: &CancellationToken,
    work: &acyclic_fs::NativeMaterializationWork) -> Result<TaskCommandProgress> { let root_task = self.protected.policy().root_task;
    if input.caller != root_task || owner.task_id() != root_task
        || self.root_binding()?.project_volumes.get(key) != Some(volume)
        || self.native.selected_source_task(input.source) != Some(root_task) {
        return Err(denied("writeback is not the child's authentic ORIGINAL Root source"));
    }
    let actual = self.roots.get(key).ok_or_else(|| denied("writeback original Root lazy handle unavailable"))?;
    let retained = self.retained.roots.get(key).ok_or_else(|| denied("writeback original Root identity unavailable"))?;
    let creation = retained.created().map_err(Error::Unauthorized)?;
    let identity = actual.physical.source.inner().root_identity();
    if creation.native_root_identity != identity.to_bytes()
        || creation.source_identity != actual.physical.source.reference().identity
        || imported.workspace_id != actual.workspace.workspace().id()
        || creation.repository_workspace_id != actual.workspace.workspace().id().into_bytes() {
        return Err(denied("writeback changed the original protected allocation or native source identity"));
    }
    let original = self.native.frozen_source(input.source)
        .ok_or_else(|| denied("writeback lost the original creator's frozen Root facts"))?;
    if original.workspace_id != actual.workspace.workspace().id() {
        return Err(denied("writeback baseline is not the actual ORIGINAL Root snapshot"));
    }
    let fs_operation = acyclic_fs::OperationId::from_bytes(operation.into_bytes());
    let operation_directory = crate::root_materialization_directory(&retained.path, fs_operation).map_err(Error::Invalid)?;
    let root = PiOriginalWritebackRoot { task: root_task, volume: volume.clone(), workspace: original.workspace_id,
        identity, path: retained.path.clone(), operation_directory };
    let options = acyclic_fs::MaterializeOptions::native(&root.operation_directory);
    let candidate = actual.workspace.workspace().generation_with_native_work(imported.authored_generation,
        work, cancellation).await.map_err(native_workspace_error)?;
    let generations = consume_root(work, build_writeback_generations(&root, context, owner, verifier, scope, &self.protected,
        &self.filesystem, actual, original, &candidate, &input.paths, operation, publication,
        acyclic_harness::conversation::MAX_PRIVATE_DIRECTORY_PAGE, cancellation, work).await)?;
    if reconcile {
        let store = self.resources.store.materialization_with_work(work.clone());
        let journal = acyclic_fs::MaterializationJournalStore::load(&store, fs_operation).await
            .map_err(|cause| match cause {
                acyclic_fs::LocalCoreStateStoreError::MaterializationWork(cause) => Error::NativeWork { cause, custody: None },
                source => Error::NativeWorkspacePublication {
                    source: acyclic_fs::NativeWorkspacePublicationError::State(source), custody: None,
                },
            })?;
        let Some(journal) = journal.as_ref() else { return Ok(TaskCommandProgress::Pending); };
        if !journal.is_valid_for(fs_operation) { return Err(denied("original native writeback journal is invalid")); }
        if journal.phase == acyclic_fs::MaterializationPhase::Applied {
            if input.approval_digest.is_none() { return Err(denied("applied writeback has no original exact user approval")); }
            validate_applied_writeback(&root, &generations, &journal, input.approval_digest.as_deref().expect("checked"), work)?;
            let approved_custody = crate::pi_operator_host::read_prepared_approval(&self.runtime,
                &self.filesystem, &self.protected, &input.sdk_operation, work).await?
                .ok_or_else(|| denied("applied writeback lost its original prepared approval ticket"))?;
            let approved = &approved_custody.0;
            if approved.operation != operation || approved.command.source != input.source
                || approved.command.root.as_deref() != Some(key) || approved.command.paths != input.paths
                || approved.approval_digest != input.approval_digest.as_deref().expect("checked")
                || approved.imported != imported.authored_generation {
                return Err(denied("applied writeback differs from its original ordinary approval"));
            }
            crate::pi_operator_host::require_resolved_approval(&self.filesystem, &self.stream, &self.protected, &approved).await?;
            owner.native_view_owner().verify_with_native_work(work).await?;
            return ready(&PiWritebackResponse { status: "published", source: input.source,
                root: key, paths: &input.paths, approval_digest: &approved.approval_digest,
                operation: &input.sdk_operation, original_operation: operation, source_snapshot: None,
                selected_snapshot: None, imported: None, receipt: Some(&journal) }, work);
        }
        return Ok(TaskCommandProgress::Pending);
    }
    let prepared = consume_root(work, prepare_writeback(&root, context, owner, verifier, scope, &generations, &self.resources.store,
        acyclic_harness::conversation::MAX_PRIVATE_DIRECTORY_PAGE, &options, &[".git"], cancellation, work).await)?;
    let root_binding = self.root_binding()?;
    let root_display = work.display(&format_args!("{}", root.path.display())).map_err(native_error)?;
    let source_display = self.native.with_selected_project_path(input.source, source_volume,
        |path| work.display(&format_args!("{}", path.display())).map_err(native_error)).transpose()?;
    let descriptor = crate::pi_operator_host::PiPreparedApprovalRef {
        version: 1, session: &root_binding.native_session_id,
        command: crate::pi_operator_host::PiPreparedCommandRef::normalized(input, key),
        operation, approval_digest: &prepared.approval_digest,
        private_volume: &root_binding.scratch.volume,
        source_snapshot: prepared.original_snapshot(), selected_snapshot: import_receipt.selected_snapshot,
        imported: prepared.imported(), import_receipt,
        journal: &prepared.journal, canonical_digest_input: &prepared.canonical_digest_input,
        root_path: &root_display.0, source_path: source_display.as_ref().map_or("", |(value, _)| value),
    };
    crate::pi_operator_host::open_prepared_approval(&self.runtime, &self.filesystem, &self.protected,
        context, owner, root_binding, &descriptor, work).await?;
    let Some(approval) = input.approval_digest.as_deref() else {
        return ready(&PiWritebackResponse { status: "prepared", source: input.source,
            root: key, operation: &input.sdk_operation, original_operation: operation,
            approval_digest: &prepared.approval_digest, paths: &input.paths,
            source_snapshot: Some(descriptor.source_snapshot), selected_snapshot: Some(descriptor.selected_snapshot),
            imported: Some(descriptor.imported), receipt: None }, work);
    };
    if approval != descriptor.approval_digest {
        return Err(denied("user approval does not match this exact prepared original-root plan"));
    }
    crate::pi_operator_host::require_resolved_approval(&self.filesystem, &self.stream, &self.protected, descriptor).await?;
    let published = consume_root(work, publish_writeback(&prepared, &root, context, owner, verifier, scope, &generations,
        &self.resources.store, approval, true, &options, &[".git"], cancellation, work).await)?;
    if published.phase != acyclic_fs::MaterializationPhase::Applied { return Ok(TaskCommandProgress::Pending); }
    owner.native_view_owner().verify_with_native_work(work).await?;
    ready(&PiWritebackResponse { status: "published", source: input.source, root: key,
        paths: &input.paths, approval_digest: approval, operation: &input.sdk_operation,
        original_operation: operation, source_snapshot: None, selected_snapshot: None, imported: None,
        receipt: Some(&published) }, work) }
}

/// Passive operator evidence reads the genuine immutable Root candidate only.
/// The descriptor is an original protected prepared input, not caller JSON.
pub(crate) async fn read_imported_selected_files(
    protected: &PiOwnerConfig, retained: &AdapterState,
    roots: &BTreeMap<String, PiRootHandle>, descriptor: &crate::pi_operator::PiPreparedApproval,
    evidence: Option<&crate::pi_operator::PiOperatorFileEvidence>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    use acyclic_harness::conversation::{ContentGrant, VolumeOperation};
    let PluginTaskBinding::Pi(binding) = &retained.task_binding else {
        return Err(denied("operator evidence has no original Pi Root binding"));
    };
    let key = descriptor.command.root.as_deref().ok_or_else(|| denied("prepared evidence lost its explicit original Root"))?;
    let volume = binding.project_volumes.get(key).ok_or_else(|| denied("prepared evidence lost its Root content volume"))?;
    let root = roots.get(key).ok_or_else(|| denied("prepared evidence lost its actual Root lazy workspace"))?;
    let created = retained.roots.get(key).ok_or_else(|| denied("prepared evidence lost its protected creator allocation"))?
        .created().map_err(Error::Unauthorized)?;
    let receipt = &descriptor.import_receipt;
    let storage_name = volume.storage_name_with_native_work(work)?;
    if binding.task_id != protected.policy().root_task || descriptor.command.caller != binding.task_id
        || descriptor.session != binding.native_session_id || receipt.caller != binding.task_id
        || receipt.source != descriptor.command.source || receipt.root != key || &receipt.destination != volume
        || receipt.paths != descriptor.command.paths || receipt.selected_snapshot != descriptor.selected_snapshot
        || receipt.imported.authored_generation != descriptor.imported
        || receipt.imported.workspace_id != root.workspace.workspace().id()
        || receipt.operation != derived(descriptor.operation, "writeback-import")
        || created.repository_workspace_id != root.workspace.workspace().id().into_bytes()
        || created.native_root_identity != root.physical.source.inner().root_identity().to_bytes()
        || storage_name.0 != root.workspace.workspace().name().as_str() {
        return Err(denied("operator evidence does not bind the actual original selected import receipt"));
    }
    let capability = volume.capability_with_native_work(VolumeOperation::Read, work)?;
    let grants = acyclic_harness::core::Capabilities::new_with_native_work([capability.0.as_str()], work)?;
    let authorization = protected.conversation_authority_with_native_work(
        binding.task_id, binding.agent_id, &grants.0, work)?;
    let verifier = authorization.0.1.verifier_with_native_work(work)?;
    let scope = &authorization.0.2;
    let _read_grant = ContentGrant::verify_with_native_work(&verifier.0, scope, volume, VolumeOperation::Read, work)?;
    let cancellation = work.cancellation();
    let generation = root.workspace.workspace().generation_with_native_work(
        descriptor.imported, work, cancellation).await.map_err(native_workspace_error)?;
    let mut paths = std::collections::BTreeSet::new();
    for path in &descriptor.command.paths { paths.insert(path.as_str()); }
    for edit in &descriptor.journal.plan.edits {
        match edit {
            acyclic_fs::MaterializationEdit::Install { path, .. }
            | acyclic_fs::MaterializationEdit::Remove { path }
            | acyclic_fs::MaterializationEdit::SetMetadata { path, .. } => { paths.insert(path.as_str()); }
            acyclic_fs::MaterializationEdit::Rename { from, to } => {
                paths.insert(from.as_str()); paths.insert(to.as_str());
            }
            _ => return Err(denied("prepared evidence contains an unsupported selected edit")),
        }
    }
    let maximum = evidence.map_or(262_144, |request| request.maximum_bytes);
    if maximum == 0 || maximum > 262_144 { return Err(invalid("file evidence exceeds its bounded range limit")); }
    if let Some(request) = evidence {
        if !paths.contains(request.path.as_str()) { return Err(denied("file evidence names an unprepared path")); }
        if let Some(cursor) = &request.cursor {
            if cursor.operation != descriptor.command.sdk_operation || cursor.root != key
                || cursor.generation != descriptor.imported || cursor.path != request.path {
                return Err(denied("file evidence cursor crosses its original operation, Root, generation or path"));
            }
        }
    }
    let mut payload = (BTreeMap::<String, Option<String>>::new(), None, Vec::new());
    let (files, cursor, allocations) = &mut payload;
    let mut remaining = u64::from(maximum);
    let start = evidence.map(|request| request.path.as_str());
    for path in paths {
        if start.is_some_and(|start| path < start) { continue; }
        acyclic_harness::conversation::validate_content_path(path)?;
        let selected = descriptor.command.paths.iter().any(|prefix| path == prefix
            || path.strip_prefix(prefix).is_some_and(|suffix| suffix.starts_with('/')));
        if !selected { return Err(denied("candidate evidence edit escaped the explicitly selected paths")); }
        cancellation.check().map_err(storage)?;
        let _current_read_grant = ContentGrant::verify_with_native_work(
            &verifier.0, scope, volume, VolumeOperation::Read, work)?;
        let absolute = work.display(&format_args!("/{path}")).map_err(native_error)?;
        let stat = match generation.stat_with_native_work(&absolute.0, work, cancellation).await {
            Ok(stat) => stat,
            Err((acyclic_fs::WorkspaceError::NotFound, custody)) => {
                drop(custody);
                allocations.push(work.claim(path.len() as u64).map_err(native_error)?);
                work.charge(WorkCounters { bytes_copied: path.len() as u64,
                    allocation_operations: u64::from(!path.is_empty()), ..WorkCounters::default() }).map_err(native_error)?;
                files.insert(path.into(), None); continue;
            }
            Err(failure) => return Err(native_workspace_error(failure)),
        };
        if stat.kind != acyclic_fs::kernel::FileKind::Regular { continue; }
        let offset = evidence.filter(|request| request.path == path)
            .and_then(|request| request.cursor.as_ref()).map_or(0, |cursor| cursor.offset);
        let length = stat.logical_bytes.ok_or_else(|| denied("regular candidate evidence lacks an authenticated length"))?;
        if offset > length { return Err(invalid("file evidence cursor exceeds its immutable file")); }
        if remaining == 0 && offset < length {
            *cursor = Some(crate::pi_operator::PiOperatorFileCursor { operation: descriptor.command.sdk_operation.clone(),
                root: key.into(), generation: descriptor.imported, path: path.into(), offset });
            break;
        }
        let count = (length - offset).min(remaining);
        let bytes = generation.read_range_with_native_work(&absolute.0, offset, count, work, cancellation)
            .await.map_err(native_workspace_error)?;
        if bytes.len() as u64 != count { return Err(denied("candidate evidence range differs from its authenticated immutable length")); }
        let encoded_length = count.checked_add(2).and_then(|size| size.checked_div(3))
            .and_then(|size| size.checked_mul(4)).ok_or_else(|| invalid("file evidence encoded size overflow"))?;
        allocations.push(work.claim(encoded_length + path.len() as u64).map_err(native_error)?);
        work.charge(WorkCounters { bytes_copied: encoded_length + path.len() as u64,
            allocation_operations: u64::from(encoded_length != 0) + u64::from(!path.is_empty()),
            ..WorkCounters::default() }).map_err(native_error)?;
        files.insert(path.into(), Some(base64::engine::general_purpose::STANDARD.encode(&bytes)));
        drop(bytes);
        remaining -= count;
        if offset + count < length {
            *cursor = Some(crate::pi_operator::PiOperatorFileCursor { operation: descriptor.command.sdk_operation.clone(),
                root: key.into(), generation: descriptor.imported, path: path.into(), offset: offset + count });
            break;
        }
    }
    let guarded = crate::pi_root_provider::guarded_pi_value(&PiFileEvidenceProjection {
        files: &files, complete: cursor.is_none(), cursor: cursor.as_ref(),
    }, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)?;
    drop(payload);
    Ok(guarded)
}

impl TaskCommandHost for PiSelectedHost<'_> {
    fn execute<'a>(&'a self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(self.run(context, fence, command, payload, false))
    }
    fn reconcile<'a>(&'a self, context: TaskContext, fence: LeaseFence, command: WorkflowCommand,
        payload: Value) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(self.run(context, fence, command, payload, true))
    }
    fn execute_with_native_work<'a>(&'a self, context: Arc<NativeTaskContext>,
        fence: LeaseFence, command: WorkflowCommand, payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            self.run_native(context, fence, command, payload, false, work).await
        })
    }
    fn reconcile_with_native_work<'a>(&'a self, context: Arc<NativeTaskContext>,
        fence: LeaseFence, command: WorkflowCommand, payload: Value,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<acyclic_harness::NativeFutureBundle<'a, TaskCommandProgress>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            self.run_native(context, fence, command, payload, true, work).await
        })
    }
}


fn consume_root<T>(work: &acyclic_fs::NativeMaterializationWork,
    result: crate::pi_root_provider::PiMeasuredResult<acyclic_fs::OperationReceipt<T>>) -> Result<T> {
    match result {
        Ok(receipt) => { work.spent(receipt.work).map_err(native_error)?; Ok(receipt.value) }
        Err((cause, prefix)) => { work.spent(prefix).map_err(native_error)?; Err(cause) }
    }
}

#[derive(Serialize)]
struct PiPageResponse<'a, E: Serialize> {
    source: TaskId, root: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume: Option<&'a VolumeRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    before_root: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    before_snapshot: Option<LazySnapshotRef>,
    snapshot: LazySnapshotRef, directory: &'a str, entries: &'a [E], cursor: Option<&'a str>,
}
#[derive(Serialize)]
struct PiImportResponse<'a> {
    status: &'static str, source: TaskId, destination: TaskId, root: &'a str,
    operation: &'a str, original_operation: OperationId, snapshot: LazySnapshotRef,
}
#[derive(Serialize)]
struct PiWritebackResponse<'a> {
    status: &'static str, source: TaskId, root: &'a str, paths: &'a [String],
    approval_digest: &'a str, operation: &'a str, original_operation: OperationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_snapshot: Option<LazySnapshotRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    selected_snapshot: Option<LazySnapshotRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    imported: Option<acyclic_fs::GenerationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt: Option<&'a acyclic_fs::MaterializationJournal>,
}
#[derive(Serialize)]
struct PiFileEvidenceProjection<'a> {
    files: &'a BTreeMap<String, Option<String>>,
    cursor: Option<&'a crate::pi_operator::PiOperatorFileCursor>,
    complete: bool,
}
fn ready<T: Serialize>(value: &T, work: &acyclic_fs::NativeMaterializationWork) -> Result<TaskCommandProgress> {
    let (value, custody) = crate::pi_root_provider::guarded_pi_value(value,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8, work)?;
    Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
}
