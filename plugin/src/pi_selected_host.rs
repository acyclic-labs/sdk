//! Selected controls execute inside the ORIGINAL command owner, never a source worker.
use crate::{
    AdapterState, PiTaskBinding, PluginTaskBinding, ServiceResources,
    pi_fork::{derived, original_operation},
    pi_inspection::*,
    pi_native_host::{PiNativeCommandHost, PiNativeFilesystem, PiNativeRuntime},
    pi_owner::PiOwnerConfig,
    pi_root_provider::PiRootHandle,
    pi_task::{PiTaskExecution, PiTaskInput},
};
use acyclic_fs::{CancellationToken, LazySnapshotRef, WorkCounters};
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::store::native_work_io;
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::VolumeRef,
    durable_host::TaskJournalOwner,
    filesystem::{TaskCommandProgress, TaskSourceInspection},
    runtime::TaskContext,
    scheduler::LeaseFence,
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

const SELECTED_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
        + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "PiTaskInput",
            "execution",
            "PiTaskExecution",
            "host",
            "child",
            "preparation",
            "launch",
            "WorkflowCommand",
            "operation_id",
            "payload",
            "PiSelectedCommand",
            "caller",
            "sdk_operation",
            "source",
            "root",
            "directory",
            "cursor",
            "maximum_entries",
            "destination",
            "paths",
            "confirm",
            "approval_digest",
            "import_operation",
            "CursorEnvelope",
            "PageCursor",
            "Inspect",
            "Diff",
            "value",
            "PiNativeInspectionCursor",
            "volume",
            "snapshot",
            "continuation",
            "PiNativeDiffCursor",
            "before",
            "after",
            "before_pass",
            "PiNativeSourceKey",
            "LazySnapshotRef",
            "id",
            "workspace_id",
            "authored_generation",
            "SourceReference",
            "identity",
            "epoch",
            "overlay",
            "shadows",
            "an array of length 16",
            "an array of length 32",
            "VolumeClass",
            "VolumeOwner",
            "ProjectSourceSnapshot",
            "project",
            "generation",
            "native_snapshot",
            "SelectedImportIntent",
            "source_snapshot",
            "destination_snapshot",
            "operation",
            "writeback",
        ]);
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "S: Deserialize<'de> + Default, P: Deserialize<'de> + Default")
)]
pub(crate) struct PiSelectedCommand<S = String, P = Vec<String>> {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: S,
    pub(crate) source: TaskId,
    pub(crate) root: Option<S>,
    #[serde(default)]
    pub(crate) directory: S,
    pub(crate) cursor: Option<S>,
    pub(crate) maximum_entries: Option<u32>,
    pub(crate) destination: Option<TaskId>,
    #[serde(default)]
    pub(crate) paths: P,
    #[serde(default)]
    pub(crate) confirm: bool,
    pub(crate) approval_digest: Option<S>,
    pub(crate) import_operation: Option<OperationId>,
}

pub(crate) struct PiControlHost<'a> {
    pub(crate) directory: &'a std::path::Path,
    pub(crate) runtime: Arc<PiNativeRuntime>,
    pub(crate) filesystem: Arc<PiNativeFilesystem>,
    pub(crate) stream: std::sync::Arc<LocalStream>,
    pub(crate) resources: &'a ServiceResources,
    pub(crate) protected: Arc<PiOwnerConfig>,
    pub(crate) retained: &'a AdapterState,
    pub(crate) roots: &'a BTreeMap<String, PiRootHandle>,
    pub(crate) native: Option<Arc<PiNativeCommandHost>>,
    pub(crate) root_provider:
        tokio::sync::Mutex<Option<Arc<crate::pi_root_provider::PiRootProvider>>>,
    pub(crate) root_source: Option<&'a crate::pi_fork_host::PiRootSource>,
    pub(crate) work: acyclic_fs::NativeMaterializationWork,
    pub(crate) pending:
        std::sync::Mutex<Option<(Vec<crate::PendingSpawn>, acyclic_fs::NativeAllocation)>>,
    pub(crate) prepared: std::sync::Mutex<
        Option<(
            Vec<crate::pi_fork::PreparedPiFork>,
            acyclic_fs::NativeAllocation,
        )>,
    >,
}

impl<'a> PiControlHost<'a> {
    #[allow(
        clippy::too_many_arguments,
        reason = "original providers and invocation work are independent"
    )]
    pub(crate) fn new(
        directory: &'a std::path::Path,
        runtime: Arc<PiNativeRuntime>,
        filesystem: Arc<PiNativeFilesystem>,
        stream: std::sync::Arc<LocalStream>,
        resources: &'a ServiceResources,
        protected: Arc<PiOwnerConfig>,
        retained: &'a AdapterState,
        roots: &'a BTreeMap<String, PiRootHandle>,
        native: Option<Arc<PiNativeCommandHost>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        runtime.verify_owner_with_native_work(&protected, work)?;
        Ok(Self {
            directory,
            runtime,
            filesystem,
            stream,
            resources,
            protected,
            retained,
            roots,
            native,
            root_provider: tokio::sync::Mutex::new(None),
            root_source: None,
            work: work.clone(),
            pending: std::sync::Mutex::new(None),
            prepared: std::sync::Mutex::new(None),
        })
    }
    pub(crate) fn native(&self) -> Result<&Arc<PiNativeCommandHost>> {
        self.native
            .as_ref()
            .ok_or_else(|| denied("Pi control has no original native host"))
    }
    fn root_binding(&self) -> Result<&PiTaskBinding> {
        match &self.retained.task_binding {
            PluginTaskBinding::Pi(binding)
                if binding.task_id == self.protected.policy().root_task =>
            {
                Ok(binding)
            }
            _ => Err(denied(
                "selected Root binding differs from the original protected allocation",
            )),
        }
    }
    fn project_key<'b>(
        &self,
        binding: &'b PiTaskBinding,
        requested: Option<&str>,
    ) -> Result<&'b str> {
        match requested {
            Some(key) if binding.project_volumes.contains_key(key) => Ok(binding
                .project_volumes
                .get_key_value(key)
                .expect("known project")
                .0
                .as_str()),
            Some(_) => Err(denied(
                "selected project is not in the original source binding",
            )),
            None if binding.project_volumes.len() == 1 => Ok(binding
                .project_volumes
                .keys()
                .next()
                .expect("one project")
                .as_str()),
            None => Err(invalid(
                "select an explicit root when the source does not have exactly one project",
            )),
        }
    }
    pub(super) async fn caller(
        &self,
        context: &TaskContext,
        fence: &LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(
        Arc<TaskJournalOwner<LocalStream>>,
        PiTaskInput,
        acyclic_fs::NativeAllocation,
    )> {
        self.runtime
            .verify_owner_with_native_work(&self.protected, work)?;
        let task = context
            .durable_task_id()
            .ok_or_else(|| denied("Pi control has no original task"))?;
        let mut admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(task, work)
            .await?;
        if admission.0.task_id() != task
            || admission.0.task != *self.runtime.definition.identity()
            || context.scope().grants() != &admission.0.grants
            || context.scope().limits() != admission.0.limits
            || context.scope().run_limits() != admission.0.run_limits
            || context.owned_task_fence()? != *fence
        {
            return Err(denied(
                "Pi control differs from its original admission or fence",
            ));
        }
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(admission.0.limits.file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value(&admission.0.input, maximum, work)?;
        let mut input = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
            &bytes.0,
            maximum,
            self.protected.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        if input.0.binding.task_id != task {
            return Err(denied("Pi control substituted its admitted task binding"));
        }
        let owner = self
            .runtime
            .runtime
            .task_host()
            .journal_owner_with_native_work(task, fence, work)
            .await?
            .with_invocation_cancellation(work.cancellation())
            .into_native_shared(work)?;
        owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        if task == self.protected.policy().root_task
            && !matches!(&input.0.execution, PiTaskExecution::Host { .. })
        {
            return Err(denied("selected native Root became a child"));
        }
        admission.1.absorb(&mut input.1)?;
        Ok((owner, input.0, admission.1))
    }
    pub(crate) async fn admission_input(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(PiTaskInput, acyclic_fs::NativeAllocation)> {
        let admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(task, work)
            .await?;
        if admission.0.task_id() != task {
            return Err(denied(
                "selected metadata substituted its actual admission task",
            ));
        }
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(crate::MAXIMUM_ADAPTER_STATE_BYTES) as u64;
        let encoded = crate::pi_root_provider::guarded_pi_bytes(&admission.0.input, maximum, work)?;
        drop(admission);
        let result = crate::pi_root_provider::guarded_pi_decode(
            &encoded.0,
            maximum,
            maximum
                .checked_mul(8)
                .ok_or_else(|| invalid("admission ownership bound overflow"))?,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            SELECTED_DIAGNOSTIC_LABEL_BYTES,
            work,
        );
        drop(encoded);
        result
    }
    pub(crate) async fn selected(
        &self,
        context: &TaskContext,
        native_context: &Arc<TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        task: TaskId,
        requested: Option<&str>,
        pinned: Option<LazySnapshotRef>,
        operation: OperationId,
        cancellation: &CancellationToken,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(
        String,
        VolumeRef,
        Arc<crate::LocalLazyWorkspace>,
        LazySnapshotRef,
        TaskSourceInspection<LocalStream, crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
        (
            acyclic_fs::NativeAllocation,
            Option<acyclic_fs::NativeAllocation>,
        ),
    )> {
        authorize_selected_source(
            &self.runtime.runtime,
            self.protected.policy().root_task,
            context,
            owner,
            task,
            self.protected.policy().session_limits.depth,
            work,
        )
        .await?;
        let decoded = self.admission_input(task, work).await?;
        let input = &decoded.0;
        let mut key = work.display(&format_args!(
            "{}",
            self.project_key(&input.binding, requested)?
        ))?;
        let project = &input.binding.project_volumes[&key.0];
        let workspace = if task == self.protected.policy().root_task {
            Arc::clone(
                &self
                    .roots
                    .get(&key.0)
                    .ok_or_else(|| denied("selected Root lost its actual lazy allocation"))?
                    .workspace,
            )
        } else {
            self.native()?
                .live_workspace(task)
                .ok_or_else(|| denied("selected child lost its actual creator workspace"))?
        };
        let frozen_custody = if let Some(pinned) = pinned {
            (pinned, None)
        } else if task == self.protected.policy().root_task {
            let actual = self
                .roots
                .get(&key.0)
                .ok_or_else(|| denied("selected Root allocation unavailable"))?;
            if owner.task_id() == task {
                let shared = native_context;
                let (snapshot, custody) = crate::pi_fork_host::freeze_created_root_source(
                    Arc::clone(&self.protected),
                    self.resources,
                    Arc::clone(&self.filesystem),
                    self.stream.clone(),
                    shared,
                    owner,
                    input,
                    &key.0,
                    actual,
                    work,
                )
                .await?;
                (snapshot, Some(custody))
            } else {
                (
                    self.root_source
                        .filter(|source| source.root_key == key.0)
                        .map(|source| source.snapshot)
                        .ok_or_else(|| {
                            denied("foreign Root source needs its original observe-source ACK")
                        })?,
                    None,
                )
            }
        } else {
            let (snapshot, custody) = self.native()?.selection_snapshot(task, work).await?;
            (snapshot, Some(custody))
        };
        let frozen = frozen_custody.0;
        let handle = self
            .coordinator(work)?
            .authorize_selected_project_with_native_work(
                context,
                owner,
                task,
                Arc::clone(&workspace),
                frozen,
                operation,
                work.remaining()?.items_examined,
                self.protected.policy().limits.file_bytes,
                work,
            )
            .await?;
        let maximum = self.protected.policy().maximum_payload_bytes;
        let mut selection = crate::pi_fork::project_snapshot(project, frozen, maximum, work)?;
        let inspection = self
            .runtime
            .runtime
            .inspect_project_source_with_native_work(
                context,
                Arc::clone(owner),
                handle,
                operation,
                task,
                &selection.0,
                work,
            )
            .await?;
        if let Some(view) = self.native()?.live_view(task) {
            let mut observed = view
                .inspect_project_snapshot_with_native_work(&inspection, frozen, work)?
                .await?;
            if observed.1 != frozen {
                return Err(denied(
                    "immutable mixed-view inspection changed its authorized frozen source",
                ));
            }
            selection.1.absorb(&mut observed.2)?;
        } else if task != self.protected.policy().root_task {
            let mut qualified = self
                .native()?
                .validate_published_selection_snapshot(task, &inspection, project, frozen, work)
                .await?;
            selection.1.absorb(&mut qualified)?;
        }
        let volume = selection.0.project;
        drop(selection.0.generation);
        drop(selection.0.native_snapshot);
        selection.1.absorb(&mut key.1)?;
        Ok((
            key.0,
            volume,
            workspace,
            frozen,
            inspection,
            (selection.1, frozen_custody.1),
        ))
    }
    pub(crate) async fn run_selected(
        &self,
        context: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let caller_custody = self.caller(context.as_ref(), &fence, work).await?;
        let (owner, caller, _) = &caller_custody;
        let native_context = &context;
        let context = context.as_ref();
        let scratch = &caller.binding.scratch.volume;
        if context.output_volume() != Some(scratch) {
            return Err(denied(
                "selected native output is not its original sealed private scratch",
            ));
        }
        if !context
            .scope()
            .grants()
            .contains(&scratch.capability(acyclic_harness::conversation::VolumeOperation::Write)?)
        {
            return Err(denied(
                "selected native context lacks its original private scratch WRITE grant",
            ));
        }
        owner.require_volume_grant(
            scratch,
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut native_work_io(work)?),
        )?;
        let cancellation = work.cancellation();
        let encoded = crate::pi_root_provider::guarded_pi_bytes(
            &payload,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            work,
        )?;
        drop(payload);
        let decoded = crate::pi_root_provider::guarded_pi_decode::<PiSelectedCommand>(
            &encoded.0,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            SELECTED_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        drop(encoded);
        let input = &decoded.0;
        if input.caller != owner.task_id() || input.sdk_operation.is_empty() {
            return Err(denied(
                "selected control changed its actual caller or SDK operation",
            ));
        }
        let operation = original_operation(
            self.protected.policy().root_task,
            input.caller,
            &input.sdk_operation,
            &work,
        )?;
        let role = match command.kind.as_str() {
            "acyclic.pi.inspect" => "selected-inspect",
            "acyclic.pi.diff" => "selected-diff",
            "acyclic.pi.import" => "selected-import",
            "acyclic.pi.writeback" if input.approval_digest.is_some() => {
                "selected-writeback-publish"
            }
            "acyclic.pi.writeback" => "selected-writeback",
            _ => return Err(Error::Unsupported("not a selected Pi command".into())),
        };
        if command.operation_id != derived(operation, role, &work)? {
            return Err(denied(
                "selected command changed its original effect operation",
            ));
        }
        let cursor_custody = input
            .cursor
            .as_deref()
            .map(|cursor| decode_cursor::<CursorEnvelope>(cursor, cancellation, work))
            .transpose()?;
        let cursor = cursor_custody.as_ref().map(|(cursor, _)| cursor);
        if let Some(cursor) = &cursor {
            if cursor.caller != input.caller
                || cursor.source != input.source
                || cursor.kind != command.kind
                || cursor.directory != input.directory
                || input
                    .root
                    .as_deref()
                    .is_some_and(|root| root != cursor.root)
            {
                return Err(denied(
                    "selected cursor changed caller, source, directory, root or control kind",
                ));
            }
        }
        let pinned = cursor.as_ref().and_then(|cursor| match &cursor.cursor {
            PageCursor::Inspect(cursor) => Some(cursor.snapshot),
            PageCursor::Diff(cursor) => Some(cursor.after.snapshot),
        });
        let base_selection_operation = derived(
            operation,
            if command.kind == "acyclic.pi.writeback" {
                "selected-writeback"
            } else {
                "selected-import"
            },
            &work,
        )?;
        let import_operation = if command.kind == "acyclic.pi.writeback" {
            derived(base_selection_operation, "writeback-import", &work)?
        } else {
            base_selection_operation
        };
        let prior_import = if matches!(
            command.kind.as_str(),
            "acyclic.pi.import" | "acyclic.pi.writeback"
        ) {
            self.read_import_intent(&context, import_operation, work)
                .await?
        } else {
            None
        };
        let pinned = pinned.or_else(|| {
            prior_import
                .as_ref()
                .map(|(intent, _)| intent.source_snapshot)
        });
        let selected_custody = self
            .selected(
                &context,
                native_context,
                &owner,
                input.source,
                input.root.as_deref(),
                pinned,
                operation,
                cancellation,
                work,
            )
            .await?;
        let (key, volume, workspace, frozen, inspection, _) = &selected_custody;
        if cursor.is_some_and(|cursor| &cursor.root != key) {
            return Err(denied(
                "selected cursor changed its actual resolved source root",
            ));
        }
        let selected = PiNativeSelectedSource::select(
            inspection,
            input.source,
            input.caller,
            volume,
            workspace,
            *frozen,
            work,
        )?;
        let maximum = input.maximum_entries.unwrap_or(128);
        match command.kind.as_str() {
            "acyclic.pi.inspect" => {
                let continuation = match cursor.as_ref().map(|cursor| &cursor.cursor) {
                    Some(PageCursor::Inspect(cursor)) => Some(cursor),
                    None => None,
                    _ => return Err(denied("diff continuation used for inspection")),
                };
                let page = selected
                    .inspect(
                        &context,
                        &owner,
                        &input.directory,
                        continuation,
                        maximum,
                        cancellation,
                        work,
                    )
                    .await?;
                let continuation = page
                    .cursor
                    .as_ref()
                    .map(|cursor| {
                        self.cursor(
                            input,
                            &key,
                            &command.kind,
                            PageCursor::Inspect(cursor),
                            cancellation,
                            work,
                        )
                    })
                    .transpose()?;
                ready(
                    &PiPageResponse {
                        source: input.source,
                        root: &key,
                        volume: Some(&volume),
                        before_root: None,
                        before_snapshot: None,
                        snapshot: *frozen,
                        directory: &page.directory,
                        entries: &page.entries,
                        cursor: continuation.as_ref().map(|(cursor, _)| cursor.as_str()),
                    },
                    work,
                )
            }
            "acyclic.pi.diff" => {
                let baseline = self.native()?.frozen_source(input.source).ok_or_else(|| {
                    denied("diff requires the child's actual ORIGINAL frozen source")
                })?;
                let source_task = self
                    .native()?
                    .selected_source_task(input.source)
                    .ok_or_else(|| {
                        denied("diff lost the original authorized selected source task")
                    })?;
                let before_custody = self
                    .selected(
                        &context,
                        native_context,
                        &owner,
                        source_task,
                        Some(&key),
                        Some(baseline),
                        operation,
                        cancellation,
                        work,
                    )
                    .await?;
                let (
                    before_key,
                    before_volume,
                    before_workspace,
                    before_frozen,
                    before_inspection,
                    _,
                ) = &before_custody;
                let before = PiNativeSelectedSource::select(
                    before_inspection,
                    source_task,
                    input.caller,
                    before_volume,
                    before_workspace,
                    *before_frozen,
                    work,
                )?;
                let continuation = match cursor.as_ref().map(|cursor| &cursor.cursor) {
                    Some(PageCursor::Diff(cursor)) => Some(cursor),
                    None => None,
                    _ => return Err(denied("inspection continuation used for diff")),
                };
                let page = diff_native(
                    &before,
                    &selected,
                    &context,
                    &owner,
                    &input.directory,
                    continuation,
                    maximum,
                    cancellation,
                    work,
                )
                .await?;
                let continuation = page
                    .cursor
                    .as_ref()
                    .map(|cursor| {
                        self.cursor(
                            input,
                            &key,
                            &command.kind,
                            PageCursor::Diff(cursor),
                            cancellation,
                            work,
                        )
                    })
                    .transpose()?;
                ready(
                    &PiPageResponse {
                        source: input.source,
                        root: &key,
                        volume: None,
                        before_root: Some(before_key),
                        before_snapshot: Some(*before_frozen),
                        snapshot: *frozen,
                        directory: &page.directory,
                        entries: &page.entries,
                        cursor: continuation.as_ref().map(|(cursor, _)| cursor.as_str()),
                    },
                    work,
                )
            }
            "acyclic.pi.import" | "acyclic.pi.writeback" => {
                if command.kind == "acyclic.pi.writeback"
                    && input
                        .destination
                        .is_some_and(|destination| destination != input.caller)
                {
                    return Err(denied(
                        "writeback destination must be the ORIGINAL Root caller",
                    ));
                }
                self.publish_selection(
                    &context,
                    native_context,
                    &owner,
                    &caller.binding,
                    &input,
                    &key,
                    &selected,
                    derived(
                        operation,
                        if command.kind == "acyclic.pi.writeback" {
                            "selected-writeback"
                        } else {
                            "selected-import"
                        },
                        &work,
                    )?,
                    command.kind == "acyclic.pi.writeback",
                    reconcile,
                    cancellation,
                    work,
                )
                .await
            }
            _ => unreachable!("kind checked above"),
        }
    }
    pub(crate) async fn run(
        &self,
        context: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        if matches!(
            command.kind.as_str(),
            crate::pi_builtin::PI_BUILTIN_COMMAND_KIND
                | crate::pi_operator_host::PI_OPERATOR_COMMAND_KIND
        ) && !self.work.shares_ledger(work)
        {
            return Err(denied("Pi control substituted its original native work"));
        }
        match command.kind.as_str() {
            crate::pi_fork_host::PI_FORK_COMMAND_KIND => {
                self.fork(context, fence, command, payload, reconcile, work)
                    .await
            }
            crate::pi_fork_host::PI_OBSERVE_SOURCE_COMMAND_KIND => {
                self.observe_source(context, fence, command, payload, work)
                    .await
            }
            crate::pi_operator_host::PI_OPERATOR_COMMAND_KIND => {
                self.run_operator(context, fence, command, payload, reconcile, work)
                    .await
            }
            crate::pi_builtin::PI_BUILTIN_COMMAND_KIND => {
                self.run_builtin(context, fence, command, payload).await
            }
            _ => {
                self.run_selected(context, fence, command, payload, reconcile, work)
                    .await
            }
        }
    }

    fn cursor(
        &self,
        input: &PiSelectedCommand,
        root: &str,
        kind: &str,
        cursor: PageCursor<&PiNativeInspectionCursor<&VolumeRef>, &PiNativeDiffCursor<&VolumeRef>>,
        cancellation: &CancellationToken,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(String, acyclic_fs::NativeAllocation)> {
        encode_cursor(
            &CursorEnvelope {
                caller: input.caller,
                source: input.source,
                root,
                kind,
                directory: input.directory.as_str(),
                cursor: &cursor,
            },
            cancellation,
            work,
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorEnvelope<S = String, C = PageCursor> {
    caller: TaskId,
    source: TaskId,
    root: S,
    kind: S,
    directory: S,
    cursor: C,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
enum PageCursor<I = PiNativeInspectionCursor, D = PiNativeDiffCursor> {
    Inspect(I),
    Diff(D),
}

fn encode_cursor<T: Serialize>(
    value: &T,
    cancellation: &CancellationToken,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let maximum = (crate::MAXIMUM_CONTROL_MESSAGE_BYTES / 2) as u64;
    let (encoded, encoded_custody) =
        crate::pi_root_provider::guarded_pi_bytes(value, maximum, work)?;
    let result = (|| {
        let hex_length = (encoded.len() as u64)
            .checked_mul(2)
            .ok_or_else(|| invalid("cursor hex length overflow"))?;
        let hex_custody = work.claim(
            hex_length,
            u64::from((hex_length) != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        work.charge(WorkCounters {
            bytes_copied: hex_length,
            allocation_operations: u64::from(hex_length != 0),
            ..WorkCounters::default()
        })?;
        cancellation.check().map_err(|_| {
            acyclic_harness::Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled)
        })?;
        Ok((hex::encode(&encoded), hex_custody))
    })();
    drop(encoded);
    drop(encoded_custody);
    result
}
fn decode_cursor<T: serde::de::DeserializeOwned>(
    value: &str,
    cancellation: &CancellationToken,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(T, acyclic_fs::NativeAllocation)> {
    if value.len() > crate::MAXIMUM_CONTROL_MESSAGE_BYTES || value.len() % 2 != 0 {
        return Err(invalid("cursor exceeds bounded transport"));
    }
    cancellation.check().map_err(|_| {
        acyclic_harness::Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled)
    })?;
    let length = value.len() as u64 / 2;
    let bytes_custody = work.claim(
        length,
        u64::from((length) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    work.charge(WorkCounters {
        bytes_copied: length,
        allocation_operations: u64::from(length != 0),
        ..WorkCounters::default()
    })?;
    let bytes =
        hex::decode(value).map_err(|_| invalid("selected cursor is not canonical hexadecimal"))?;
    let result = crate::pi_root_provider::guarded_pi_decode(
        &bytes,
        (crate::MAXIMUM_CONTROL_MESSAGE_BYTES / 2) as u64,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        acyclic_harness::wire::ErrorCode::Invalid,
        acyclic_harness::JsonValueSchema::None,
        SELECTED_DIAGNOSTIC_LABEL_BYTES,
        work,
    );
    drop(bytes);
    drop(bytes_custody);
    result
}
fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}
fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason,
    }
}
fn native_workspace_error(
    (source, custody): (
        acyclic_fs::WorkspaceError,
        Option<acyclic_fs::NativeAllocation>,
    ),
) -> Error {
    Error::NativeWorkspacePublication {
        source: acyclic_fs::NativeWorkspacePublicationError::Workspace(source),
        custody,
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedImportIntent<S = String, V = VolumeRef, P = Vec<String>> {
    caller: TaskId,
    source: TaskId,
    root: S,
    destination: V,
    source_snapshot: LazySnapshotRef,
    destination_snapshot: LazySnapshotRef,
    paths: P,
    operation: OperationId,
    writeback: bool,
}

impl<'a> From<&'a SelectedImportIntent>
    for SelectedImportIntent<&'a str, &'a VolumeRef, &'a [String]>
{
    fn from(value: &'a SelectedImportIntent) -> Self {
        Self {
            caller: value.caller,
            source: value.source,
            root: &value.root,
            destination: &value.destination,
            source_snapshot: value.source_snapshot,
            destination_snapshot: value.destination_snapshot,
            paths: &value.paths,
            operation: value.operation,
            writeback: value.writeback,
        }
    }
}

/// Metadata from a completed canonical SDK selected import, retained under the original immutable Pi input operation; never a replacement publication grant.
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiSelectedImportReceipt<S = String, V = VolumeRef, P = Vec<String>> {
    pub(crate) caller: TaskId,
    pub(crate) source: TaskId,
    pub(crate) root: S,
    pub(crate) destination: V,
    pub(crate) selected_snapshot: LazySnapshotRef,
    pub(crate) destination_snapshot: LazySnapshotRef,
    pub(crate) imported: LazySnapshotRef,
    pub(crate) paths: P,
    pub(crate) operation: OperationId,
}

impl<'a> From<&'a PiSelectedImportReceipt>
    for PiSelectedImportReceipt<&'a str, &'a VolumeRef, &'a [String]>
{
    fn from(value: &'a PiSelectedImportReceipt) -> Self {
        Self {
            caller: value.caller,
            source: value.source,
            root: &value.root,
            destination: &value.destination,
            selected_snapshot: value.selected_snapshot,
            destination_snapshot: value.destination_snapshot,
            imported: value.imported,
            paths: &value.paths,
            operation: value.operation,
        }
    }
}

impl PiControlHost<'_> {
    async fn publish_selection(
        &self,
        context: &TaskContext,
        native_context: &Arc<TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        binding: &PiTaskBinding,
        input: &PiSelectedCommand,
        key: &str,
        selected: &PiNativeSelectedSource<'_>,
        operation: OperationId,
        writeback: bool,
        reconcile: bool,
        cancellation: &CancellationToken,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        if writeback
            && (input.caller != self.protected.policy().root_task || input.source == input.caller)
        {
            return Err(denied(
                "writeback requires the ORIGINAL Root caller and a separately selected child",
            ));
        }
        let caller_native = native_context;
        if input.import_operation.is_some() {
            return Err(invalid(
                "writeback imports under the same original SDK operation; external import keys are not accepted",
            ));
        }
        let destination = input.destination.unwrap_or(input.caller);
        let foreign_binding =
            if destination != input.caller && destination != self.protected.policy().root_task {
                Some(
                    self.native()?.binding(destination, work)?.ok_or_else(|| {
                        denied("explicit destination has no actual creator binding")
                    })?,
                )
            } else {
                None
            };
        let destination_binding = if destination == input.caller {
            binding
        } else if destination == self.protected.policy().root_task {
            self.root_binding()?
        } else {
            &foreign_binding
                .as_ref()
                .ok_or_else(|| denied("explicit destination binding unavailable"))?
                .0
        };
        let volume = destination_binding
            .project_volumes
            .get(key)
            .ok_or_else(|| denied("destination lacks this actual project"))?;
        let write_capability = {
            let mut io = native_work_io(work)?;
            let value = volume.capability(
                acyclic_harness::conversation::VolumeOperation::Write,
                &mut Some(&mut io),
            )?;
            (value, io.into_custody())
        };
        if !context.scope().grants().contains(&write_capability.0) {
            return Err(denied(
                "current caller has no actual WRITE grant for the explicit destination",
            ));
        }
        let authorization = self.protected.conversation_authority_with_native_work(
            input.caller,
            binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let verifier = {
            let mut io = native_work_io(work)?;
            let value = authorization.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        let scope = &authorization.0.2;
        let mut destination_io = native_work_io(work)?;
        let _destination_grant = acyclic_harness::conversation::ContentGrant::verify(
            &verifier.0,
            scope,
            volume,
            acyclic_harness::conversation::VolumeOperation::Write,
            context.scope().limits().file_bytes,
            &mut Some(&mut destination_io),
        )?;
        if destination != input.caller && destination == self.protected.policy().root_task {
            return Err(denied(
                "delegated Root mutation requires its original mediated Root workflow command",
            ));
        }
        let destination_live = if destination != self.protected.policy().root_task {
            Some(
                self.native()?
                    .live_source(destination, work)
                    .await?
                    .ok_or_else(|| denied("explicit destination has no ORIGINAL live owner"))?,
            )
        } else {
            None
        };
        let destination_native = if destination == input.caller {
            caller_native
        } else {
            &destination_live
                .as_ref()
                .ok_or_else(|| denied("explicit destination lost its original native context"))?
                .context
        };
        let destination_owner = if destination == input.caller {
            owner
        } else {
            &destination_live
                .as_ref()
                .ok_or_else(|| denied("explicit destination lost its original native owner"))?
                .owner
        };
        let child_workspace = if destination == self.protected.policy().root_task {
            None
        } else {
            Some(
                self.native()?
                    .live_workspace(destination)
                    .ok_or_else(|| denied("destination child lost actual creator workspace"))?,
            )
        };
        let workspace = if let Some(workspace) = &child_workspace {
            workspace.as_ref()
        } else {
            &self
                .roots
                .get(key)
                .ok_or_else(|| denied("destination Root lost actual lazy allocation"))?
                .workspace
        };
        let (mount, publication_work) = if destination == self.protected.policy().root_task {
            (None, work.clone())
        } else {
            let (mount, original_work) = self
                .native()?
                .project_mount(destination, volume)
                .ok_or_else(|| denied("destination child has no actual live project mount"))?;
            (Some(mount), original_work)
        };
        if !publication_work.shares_ledger(work) {
            return Err(denied(
                "selected publication crosses its original caller work ledger",
            ));
        }
        let source_write_guard = if mount.is_some() {
            let live = destination_live
                .as_ref()
                .ok_or_else(|| denied("mounted destination lost its original issued context"))?;
            let guard = live
                .context
                .volume_callback_guard_with_native_work(&live.owner, &live.original_work)
                .await?;
            Some(
                guard
                    .for_volume(&live.owner, volume, true, &publication_work)?
                    .0,
            )
        } else {
            None
        };
        let publication = if let Some(view) = self.native()?.live_view(destination) {
            view.project_publication(volume).ok_or_else(|| {
                denied("destination lacks its ORIGINAL project publication permit")
            })?
        } else if destination == self.protected.policy().root_task {
            acyclic_fs::PublicationPermit::Unrestricted
        } else {
            return Err(denied("destination live owner unavailable"));
        };
        let import_operation = if writeback {
            derived(operation, "writeback-import", &work)?
        } else {
            operation
        };
        let prior = self
            .read_import_intent(context, import_operation, work)
            .await?;
        let resuming_import = prior.is_some();
        let intent_custody = if let Some((intent, _custody)) = &prior {
            if intent.caller != input.caller
                || intent.source != input.source
                || intent.root != key
                || &intent.destination != volume
                || intent.paths != input.paths
                || intent.operation != import_operation
                || intent.writeback != writeback
            {
                return Err(denied(
                    "original selected import retry changed its sealed request",
                ));
            }
            (SelectedImportIntent::from(intent), None)
        } else {
            if reconcile {
                return Ok(TaskCommandProgress::Pending);
            }
            let destination_cut = if destination == self.protected.policy().root_task {
                let root = self
                    .roots
                    .get(key)
                    .ok_or_else(|| denied("actual original Root unavailable"))?;
                let shared = native_context;
                let root_input = self.admission_input(input.caller, work).await?;
                let (snapshot, custody) = crate::pi_fork_host::freeze_created_root_source(
                    Arc::clone(&self.protected),
                    self.resources,
                    Arc::clone(&self.filesystem),
                    self.stream.clone(),
                    shared,
                    owner,
                    &root_input.0,
                    key,
                    root,
                    work,
                )
                .await?;
                (snapshot, custody)
            } else {
                let live = destination_live
                    .as_ref()
                    .ok_or_else(|| denied("destination original native view unavailable"))?;
                let (snapshot, custody) = crate::pi_fork_host::freeze_child_source(
                    live.context.as_ref(),
                    &live.owner,
                    &live.view,
                    &workspace,
                    &live.original_work,
                )
                .await?;
                owner
                    .verify(false, &mut Some(&mut native_work_io(work)?))
                    .await?;
                (snapshot, custody)
            };
            let intent = SelectedImportIntent {
                caller: input.caller,
                source: input.source,
                root: key,
                destination: volume,
                source_snapshot: selected.snapshot(),
                destination_snapshot: destination_cut.0,
                paths: input.paths.as_slice(),
                operation: import_operation,
                writeback,
            };
            let staged = crate::pi_root_provider::stage_pi_input(
                &self.protected,
                &self.filesystem,
                import_operation,
                "selected-import-intent",
                &intent,
                work,
            )
            .await?;
            drop(staged);
            (intent, Some(destination_cut))
        };
        let intent = intent_custody.0;
        if selected.snapshot() != intent.source_snapshot {
            return Err(denied(
                "selected import changed its original pinned source snapshot",
            ));
        }
        let fs_operation = acyclic_fs::OperationId::from_bytes(import_operation.into_bytes());
        owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        destination_owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        let completed = if let Some(mount) = &mount {
            let result = mount
                .selected_import_snapshot_with_work(fs_operation, &publication_work, cancellation)
                .await;
            if let Err(acyclic_fs::NativeMountError::Source(error)) = &result {
                if matches!(
                    error.semantic_error(),
                    acyclic_fs::MountSourceError::OriginalSnapshotUnavailable
                        | acyclic_fs::MountSourceError::Indeterminate
                ) {
                    return Ok(TaskCommandProgress::Pending);
                }
            }
            result.map_err(Error::from)?
        } else {
            let result = async {
                let accounting = acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                    work,
                    cancellation,
                )?;
                workspace
                    .selected_import_snapshot(fs_operation, &accounting)
                    .await
            }
            .await;
            if let Err(source) = &result {
                if matches!(
                    source.semantic_error(),
                    acyclic_fs::LazyWorkspaceError::OriginalSnapshotUnavailable
                ) || matches!(source.semantic_error(), acyclic_fs::LazyWorkspaceError::SelectedImportIndeterminate { operation } if *operation == fs_operation)
                {
                    return Ok(TaskCommandProgress::Pending);
                }
            }
            result.map_err(crate::pi_fork_host::native_lazy_error)?
        };
        let imported = if let Some(imported) = completed {
            imported
        } else {
            if reconcile || resuming_import {
                return Ok(TaskCommandProgress::Pending);
            }
            let result = import_native_project(
                selected,
                caller_native,
                owner,
                destination_native,
                destination_owner,
                &workspace,
                mount.as_deref(),
                source_write_guard,
                volume,
                intent.destination_snapshot,
                &verifier.0,
                scope,
                intent.paths,
                import_operation,
                publication,
                acyclic_harness::conversation::MAX_PRIVATE_DIRECTORY_PAGE,
                self.protected.policy().limits.file_bytes,
                cancellation,
                &publication_work,
            )
            .await;
            if let Err(Error::NativeSource { source, .. }) = &result {
                if matches!(source.semantic_error(), acyclic_fs::LazyWorkspaceError::SelectedImportIndeterminate { operation } if *operation == fs_operation)
                {
                    return Ok(TaskCommandProgress::Pending);
                }
            }
            result?
        };
        let import_receipt = PiSelectedImportReceipt {
            caller: intent.caller,
            source: intent.source,
            root: intent.root,
            destination: intent.destination,
            selected_snapshot: intent.source_snapshot,
            destination_snapshot: intent.destination_snapshot,
            imported,
            paths: intent.paths,
            operation: import_operation,
        };
        owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        destination_owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        let staged = crate::pi_root_provider::stage_pi_input(
            &self.protected,
            &self.filesystem,
            import_operation,
            "selected-import-receipt",
            &import_receipt,
            work,
        )
        .await?;
        drop(staged);
        owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        if !writeback {
            return ready(
                &PiImportResponse {
                    status: "imported",
                    source: input.source,
                    destination,
                    root: key,
                    operation: &input.sdk_operation,
                    original_operation: import_operation,
                    snapshot: imported,
                },
                work,
            );
        }
        self.writeback(
            context,
            native_context,
            owner,
            input,
            key,
            volume,
            selected.volume(),
            imported,
            operation,
            publication,
            &verifier.0,
            scope,
            import_receipt,
            reconcile,
            cancellation,
            work,
        )
        .await
    }
    async fn read_import_intent(
        &self,
        context: &TaskContext,
        operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<(SelectedImportIntent, acyclic_fs::NativeAllocation)>> {
        crate::pi_root_provider::read_pi_input_with_native_work(
            &self.protected,
            &self.filesystem,
            operation,
            "selected-import-intent",
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            acyclic_harness::JsonValueSchema::None,
            SELECTED_DIAGNOSTIC_LABEL_BYTES,
            context,
            work,
        )
        .await
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "original owner, frozen evidence and prepared approval are independent"
    )]
    async fn writeback(
        &self,
        context: &TaskContext,
        native_context: &Arc<TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        input: &PiSelectedCommand,
        key: &str,
        volume: &VolumeRef,
        source_volume: &VolumeRef,
        imported: LazySnapshotRef,
        operation: OperationId,
        publication: acyclic_fs::PublicationPermit,
        verifier: &acyclic_harness::core::AuthorityVerifier,
        scope: &acyclic_harness::core::Scope,
        import_receipt: PiSelectedImportReceipt<&str, &VolumeRef, &[String]>,
        reconcile: bool,
        cancellation: &CancellationToken,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let root_task = self.protected.policy().root_task;
        if input.caller != root_task
            || owner.task_id() != root_task
            || self.root_binding()?.project_volumes.get(key) != Some(volume)
            || self.native()?.selected_source_task(input.source) != Some(root_task)
        {
            return Err(denied(
                "writeback is not the child's authentic ORIGINAL Root source",
            ));
        }
        let actual = self
            .roots
            .get(key)
            .ok_or_else(|| denied("writeback original Root lazy handle unavailable"))?;
        let retained = self
            .retained
            .roots
            .get(key)
            .ok_or_else(|| denied("writeback original Root identity unavailable"))?;
        let creation = retained.created().map_err(Error::Unauthorized)?;
        let identity = actual.physical.source.inner().root_identity();
        if creation.native_root_identity != identity.to_bytes()
            || creation.source_identity != actual.physical.source.reference().identity
            || imported.workspace_id != actual.workspace.workspace().id()
            || creation.repository_workspace_id != actual.workspace.workspace().id().into_bytes()
        {
            return Err(denied(
                "writeback changed the original protected allocation or native source identity",
            ));
        }
        let original = self
            .native()?
            .frozen_source(input.source)
            .ok_or_else(|| denied("writeback lost the original creator's frozen Root facts"))?;
        if original.workspace_id != actual.workspace.workspace().id() {
            return Err(denied(
                "writeback baseline is not the actual ORIGINAL Root snapshot",
            ));
        }
        let fs_operation = acyclic_fs::OperationId::from_bytes(operation.into_bytes());
        let operation_directory = crate::util::root_materialization_directory_with_native_work(
            &retained.path,
            fs_operation,
            work,
        )?;
        let mut root_volume_io = native_work_io(work)?;
        let root_volume = volume.copy(&mut Some(&mut root_volume_io))?;
        let root_path = work.copy_path(&retained.path)?;
        let root = PiOriginalWritebackRoot {
            task: root_task,
            volume: root_volume,
            workspace: original.workspace_id,
            identity,
            path: root_path.0,
            operation_directory: operation_directory.0,
            _custody: [
                root_volume_io.into_custody(),
                root_path.1,
                operation_directory.1,
            ],
        };
        let options_path = work.copy_path(&root.operation_directory)?;
        let options_custody = (
            acyclic_fs::MaterializeOptions::native(options_path.0),
            options_path.1,
        );
        let options = &options_custody.0;
        let candidate = actual
            .workspace
            .workspace()
            .generation(imported.authored_generation, work, cancellation)
            .await
            .map_err(|error| native_workspace_error((error, None)))?;
        let root_input = self.admission_input(root_task, work).await?;
        let PiTaskExecution::Host {
            root: root_intent, ..
        } = root_input.0.execution
        else {
            return Err(denied(
                "writeback replaced its original Root admission with a child",
            ));
        };
        let original_request = crate::pi_root_inputs::PiRootRequest {
            binding: root_input.0.binding,
            root: root_intent,
        };
        let publication_guard = crate::pi_inspection::PiWritebackPublicationGuard::new(
            Arc::clone(&self.protected),
            self.resources,
            Arc::clone(&self.filesystem),
            self.stream.clone(),
            Arc::clone(native_context),
            Arc::clone(owner),
            (original_request, root_input.1),
            key,
            root.identity,
            work,
        )
        .await?;
        let generations = build_writeback_generations(
            &root,
            native_context,
            owner,
            verifier,
            scope,
            &self.protected,
            &self.filesystem,
            actual,
            original,
            &candidate,
            &input.paths,
            operation,
            publication,
            acyclic_harness::conversation::MAX_PRIVATE_DIRECTORY_PAGE,
            cancellation,
            work,
            publication_guard,
        )
        .await?;
        if reconcile {
            let store = self.resources.store.materialization_with_work(work.clone());
            let journal = acyclic_fs::MaterializationJournalStore::load(&store, fs_operation)
                .await
                .map_err(|cause| match cause {
                    acyclic_fs::LocalCoreStateStoreError::MaterializationWork(cause) => {
                        Error::NativeWork {
                            cause,
                            custody: None,
                        }
                    }
                    source => Error::NativeWorkspacePublication {
                        source: acyclic_fs::NativeWorkspacePublicationError::State(source),
                        custody: None,
                    },
                })?;
            let Some(journal) = journal.as_ref() else {
                return Ok(TaskCommandProgress::Pending);
            };
            if !journal.is_valid_for(fs_operation) {
                return Err(denied("original native writeback journal is invalid"));
            }
            if journal.phase == acyclic_fs::MaterializationPhase::Applied {
                if input.approval_digest.is_none() {
                    return Err(denied(
                        "applied writeback has no original exact user approval",
                    ));
                }
                validate_applied_writeback(
                    &root,
                    &generations,
                    &journal,
                    input.approval_digest.as_deref().expect("checked"),
                    work,
                )?;
                let approved_custody = crate::pi_operator_host::read_prepared_approval(
                    &self.runtime,
                    &self.filesystem,
                    &self.protected,
                    &input.sdk_operation,
                    work,
                )
                .await?
                .ok_or_else(|| {
                    denied("applied writeback lost its original prepared approval ticket")
                })?;
                let approved = &approved_custody.0;
                if approved.operation != operation
                    || approved.command.source != input.source
                    || approved.command.root.as_deref() != Some(key)
                    || approved.command.paths != input.paths
                    || approved.approval_digest
                        != input.approval_digest.as_deref().expect("checked")
                    || approved.imported != imported.authored_generation
                {
                    return Err(denied(
                        "applied writeback differs from its original ordinary approval",
                    ));
                }
                crate::pi_operator_host::require_resolved_approval(
                    &self.filesystem,
                    &self.stream,
                    &self.protected,
                    &approved,
                    work,
                )
                .await?;
                owner
                    .verify(false, &mut Some(&mut native_work_io(work)?))
                    .await?;
                return ready(
                    &PiWritebackResponse {
                        status: "published",
                        source: input.source,
                        root: key,
                        paths: &input.paths,
                        approval_digest: &approved.approval_digest,
                        operation: &input.sdk_operation,
                        original_operation: operation,
                        source_snapshot: None,
                        selected_snapshot: None,
                        imported: None,
                        receipt: Some(&journal),
                    },
                    work,
                );
            }
            return Ok(TaskCommandProgress::Pending);
        }
        let prepared = prepare_writeback(
            &root,
            context,
            owner,
            verifier,
            scope,
            &generations,
            &self.resources.store,
            acyclic_harness::conversation::MAX_PRIVATE_DIRECTORY_PAGE,
            &options,
            &[".git"],
            cancellation,
            work,
        )
        .await?;
        let root_binding = self.root_binding()?;
        let root_display = work.display(&format_args!("{}", root.path.display()))?;
        let source_display = self
            .native()?
            .with_selected_project_path(input.source, source_volume, |path| {
                work.display(&format_args!("{}", path.display()))
                    .map_err(acyclic_harness::Error::from)
            })
            .transpose()?;
        let descriptor = crate::pi_operator::PiPreparedApproval {
            version: 1,
            session: &root_binding.native_session_id,
            command: PiSelectedCommand::normalized(input, key),
            operation,
            approval_digest: &prepared.approval_digest,
            private_volume: &root_binding.scratch.volume,
            source_snapshot: prepared.generations.original_snapshot,
            selected_snapshot: import_receipt.selected_snapshot,
            imported: prepared.generations.imported,
            import_receipt,
            journal: &prepared.journal,
            canonical_digest_input: &prepared.canonical_digest_input,
            root_path: &root_display.0,
            source_path: source_display.as_ref().map_or("", |(value, _)| value),
        };
        crate::pi_operator_host::open_prepared_approval(
            &self.runtime,
            &self.filesystem,
            &self.protected,
            native_context,
            owner,
            root_binding,
            &descriptor,
            work,
        )
        .await?;
        let Some(approval) = input.approval_digest.as_deref() else {
            return ready(
                &PiWritebackResponse {
                    status: "prepared",
                    source: input.source,
                    root: key,
                    operation: &input.sdk_operation,
                    original_operation: operation,
                    approval_digest: &prepared.approval_digest,
                    paths: &input.paths,
                    source_snapshot: Some(descriptor.source_snapshot),
                    selected_snapshot: Some(descriptor.selected_snapshot),
                    imported: Some(descriptor.imported),
                    receipt: None,
                },
                work,
            );
        };
        if approval != descriptor.approval_digest {
            return Err(denied(
                "user approval does not match this exact prepared original-root plan",
            ));
        }
        crate::pi_operator_host::require_resolved_approval(
            &self.filesystem,
            &self.stream,
            &self.protected,
            descriptor,
            work,
        )
        .await?;
        let published = publish_writeback(
            &prepared,
            context,
            owner,
            verifier,
            scope,
            &self.resources.store,
            approval,
            true,
            cancellation,
            work,
        )
        .await?;
        if published.phase != acyclic_fs::MaterializationPhase::Applied {
            return Ok(TaskCommandProgress::Pending);
        }
        owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        ready(
            &PiWritebackResponse {
                status: "published",
                source: input.source,
                root: key,
                paths: &input.paths,
                approval_digest: approval,
                operation: &input.sdk_operation,
                original_operation: operation,
                source_snapshot: None,
                selected_snapshot: None,
                imported: None,
                receipt: Some(&published),
            },
            work,
        )
    }
}

/// Passive operator evidence reads the genuine immutable Root candidate only.
pub(crate) async fn read_imported_selected_files(
    protected: &PiOwnerConfig,
    retained: &AdapterState,
    roots: &BTreeMap<String, PiRootHandle>,
    descriptor: &crate::pi_operator::PiPreparedApproval,
    evidence: Option<&crate::pi_operator::PiOperatorFileEvidence>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    use acyclic_harness::conversation::{ContentGrant, VolumeOperation};
    let PluginTaskBinding::Pi(binding) = &retained.task_binding else {
        return Err(denied("operator evidence has no original Pi Root binding"));
    };
    let key = descriptor
        .command
        .root
        .as_deref()
        .ok_or_else(|| denied("prepared evidence lost its explicit original Root"))?;
    let volume = binding
        .project_volumes
        .get(key)
        .ok_or_else(|| denied("prepared evidence lost its Root content volume"))?;
    let root = roots
        .get(key)
        .ok_or_else(|| denied("prepared evidence lost its actual Root lazy workspace"))?;
    let created = retained
        .roots
        .get(key)
        .ok_or_else(|| denied("prepared evidence lost its protected creator allocation"))?
        .created()
        .map_err(Error::Unauthorized)?;
    let receipt = &descriptor.import_receipt;
    let storage_name = {
        let mut io = native_work_io(work)?;
        let value = volume.storage_name(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    if binding.task_id != protected.policy().root_task
        || descriptor.command.caller != binding.task_id
        || descriptor.session != binding.native_session_id
        || receipt.caller != binding.task_id
        || receipt.source != descriptor.command.source
        || receipt.root != key
        || &receipt.destination != volume
        || receipt.paths != descriptor.command.paths
        || receipt.selected_snapshot != descriptor.selected_snapshot
        || receipt.imported.authored_generation != descriptor.imported
        || receipt.imported.workspace_id != root.workspace.workspace().id()
        || receipt.operation != derived(descriptor.operation, "writeback-import", &work)?
        || created.repository_workspace_id != root.workspace.workspace().id().into_bytes()
        || created.native_root_identity != root.physical.source.inner().root_identity().to_bytes()
        || storage_name.0 != root.workspace.workspace().name().as_str()
    {
        return Err(denied(
            "operator evidence does not bind the actual original selected import receipt",
        ));
    }
    let capability = {
        let mut io = native_work_io(work)?;
        let value = volume.capability(VolumeOperation::Read, &mut Some(&mut io))?;
        (value, io.into_custody())
    };
    let grants =
        acyclic_harness::Capabilities::new_with_native_work([capability.0.as_str()], work)?;
    let authorization = protected.conversation_authority_with_native_work(
        binding.task_id,
        binding.agent_id,
        &grants.0,
        work,
    )?;
    let verifier = {
        let mut io = native_work_io(work)?;
        let value = authorization.0.1.verifier(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    let scope = &authorization.0.2;
    let mut read_io = native_work_io(work)?;
    let _read_grant = ContentGrant::verify(
        &verifier.0,
        scope,
        volume,
        VolumeOperation::Read,
        protected.policy().limits.file_bytes,
        &mut Some(&mut read_io),
    )?;
    let cancellation = work.cancellation();
    let generation = root
        .workspace
        .workspace()
        .generation(descriptor.imported, work, cancellation)
        .await
        .map_err(|error| native_workspace_error((error, None)))?;
    let path_capacity = descriptor
        .command
        .paths
        .len()
        .checked_add(
            descriptor
                .journal
                .plan
                .edits
                .len()
                .checked_mul(2)
                .ok_or_else(|| invalid("selected evidence path count overflow"))?,
        )
        .ok_or_else(|| invalid("selected evidence path count overflow"))?;
    let path_slots = path_capacity
        .checked_mul(std::mem::size_of::<&str>())
        .ok_or_else(|| invalid("selected evidence path allocation overflow"))?;
    let path_custody = work.claim(
        path_slots as u64,
        u64::from((path_slots as u64) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    work.charge(WorkCounters {
        allocation_operations: u64::from(path_capacity != 0),
        items_examined: (descriptor.command.paths.len() + descriptor.journal.plan.edits.len())
            as u64,
        ..WorkCounters::default()
    })?;
    let mut paths = (Vec::<&str>::with_capacity(path_capacity), path_custody);
    paths
        .0
        .extend(descriptor.command.paths.iter().map(String::as_str));
    for edit in &descriptor.journal.plan.edits {
        match edit {
            acyclic_fs::MaterializationEdit::Install { path, .. }
            | acyclic_fs::MaterializationEdit::Remove { path }
            | acyclic_fs::MaterializationEdit::SetMetadata { path, .. } => paths.0.push(path),
            acyclic_fs::MaterializationEdit::Rename { from, to } => {
                paths.0.push(from);
                paths.0.push(to);
            }
            _ => {
                return Err(denied(
                    "prepared evidence contains an unsupported selected edit",
                ));
            }
        }
    }
    paths.0.sort_unstable();
    paths.0.dedup();
    let maximum = evidence.map_or(262_144, |request| request.maximum_bytes);
    if maximum == 0 || maximum > 262_144 {
        return Err(invalid("file evidence exceeds its bounded range limit"));
    }
    if let Some(request) = evidence {
        if paths.0.binary_search(&request.path.as_str()).is_err() {
            return Err(denied("file evidence names an unprepared path"));
        }
        if let Some(cursor) = &request.cursor {
            if cursor.operation != descriptor.command.sdk_operation
                || cursor.root != key
                || cursor.generation != descriptor.imported
                || cursor.path != request.path
            {
                return Err(denied(
                    "file evidence cursor crosses its original operation, Root, generation or path",
                ));
            }
        }
    }
    let capacity = paths.0.len();
    let slots = capacity
        .checked_mul(std::mem::size_of::<(&str, Option<String>)>())
        .ok_or_else(|| invalid("selected evidence payload allocation overflow"))?;
    let payload_custody = work.claim(
        slots as u64,
        u64::from((slots as u64) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    work.charge(WorkCounters {
        allocation_operations: u64::from(capacity != 0),
        ..WorkCounters::default()
    })?;
    let mut payload = (
        Vec::<(&str, Option<String>)>::with_capacity(capacity),
        None,
        payload_custody,
    );
    let (files, cursor, custody) = &mut payload;
    let mut remaining = u64::from(maximum);
    let start = evidence.map(|request| request.path.as_str());
    for &path in &paths.0 {
        if start.is_some_and(|start| path < start) {
            continue;
        }
        acyclic_harness::conversation::validate_content_path(path)?;
        let selected = descriptor.command.paths.iter().any(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        });
        if !selected {
            return Err(denied(
                "candidate evidence edit escaped the explicitly selected paths",
            ));
        }
        cancellation.check().map_err(|_| {
            acyclic_harness::Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled)
        })?;
        let mut current_read_io = native_work_io(work)?;
        let _current_read_grant = ContentGrant::verify(
            &verifier.0,
            scope,
            volume,
            VolumeOperation::Read,
            protected.policy().limits.file_bytes,
            &mut Some(&mut current_read_io),
        )?;
        let absolute = work.display(&format_args!("/{path}"))?;
        let stat = match generation
            .stat(&absolute.0, work, cancellation)
            .await
            .map_err(|error| (error, None))
        {
            Ok(stat) => stat,
            Err((acyclic_fs::WorkspaceError::NotFound, custody)) => {
                drop(custody);
                files.push((path, None));
                continue;
            }
            Err(failure) => return Err(native_workspace_error(failure)),
        };
        if stat.kind != acyclic_fs::kernel::FileKind::Regular {
            continue;
        }
        let offset = evidence
            .filter(|request| request.path == path)
            .and_then(|request| request.cursor.as_ref())
            .map_or(0, |cursor| cursor.offset);
        let length = stat
            .logical_bytes
            .ok_or_else(|| denied("regular candidate evidence lacks an authenticated length"))?;
        if offset > length {
            return Err(invalid("file evidence cursor exceeds its immutable file"));
        }
        if remaining == 0 && offset < length {
            *cursor = Some(crate::pi_operator::PiOperatorFileCursor {
                operation: descriptor.command.sdk_operation.as_str(),
                root: key,
                generation: descriptor.imported,
                path,
                offset,
            });
            break;
        }
        let count = (length - offset).min(remaining);
        let bytes = generation
            .read_range(&absolute.0, offset, count, work, cancellation)
            .await
            .map(|chunk| chunk.bytes)
            .map_err(|error| native_workspace_error((error, None)))?;
        if bytes.len() as u64 != count {
            return Err(denied(
                "candidate evidence range differs from its authenticated immutable length",
            ));
        }
        let encoded_length = count
            .checked_add(2)
            .and_then(|size| size.checked_div(3))
            .and_then(|size| size.checked_mul(4))
            .ok_or_else(|| invalid("file evidence encoded size overflow"))?;
        custody.grow(encoded_length)?;
        work.charge(WorkCounters {
            bytes_encoded: encoded_length,
            allocation_operations: u64::from(encoded_length != 0),
            ..WorkCounters::default()
        })?;
        files.push((
            path,
            Some(base64::engine::general_purpose::STANDARD.encode(&bytes)),
        ));
        drop(bytes);
        remaining -= count;
        if offset + count < length {
            *cursor = Some(crate::pi_operator::PiOperatorFileCursor {
                operation: descriptor.command.sdk_operation.as_str(),
                root: key,
                generation: descriptor.imported,
                path,
                offset: offset + count,
            });
            break;
        }
    }
    let guarded = crate::pi_root_provider::guarded_pi_value(
        &PiFileEvidenceProjection {
            files: &files,
            complete: cursor.is_none(),
            cursor: cursor.as_ref(),
        },
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        work,
    )?;
    drop(payload);
    Ok(guarded)
}

#[derive(Serialize)]
struct PiPageResponse<'a, E: Serialize> {
    source: TaskId,
    root: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    volume: Option<&'a VolumeRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    before_root: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    before_snapshot: Option<LazySnapshotRef>,
    snapshot: LazySnapshotRef,
    directory: &'a str,
    entries: &'a [E],
    cursor: Option<&'a str>,
}
#[derive(Serialize)]
struct PiImportResponse<'a> {
    status: &'static str,
    source: TaskId,
    destination: TaskId,
    root: &'a str,
    operation: &'a str,
    original_operation: OperationId,
    snapshot: LazySnapshotRef,
}
#[derive(Serialize)]
struct PiWritebackResponse<'a> {
    status: &'static str,
    source: TaskId,
    root: &'a str,
    paths: &'a [String],
    approval_digest: &'a str,
    operation: &'a str,
    original_operation: OperationId,
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
    #[serde(serialize_with = "serialize_evidence_files")]
    files: &'a [(&'a str, Option<String>)],
    cursor: Option<&'a crate::pi_operator::PiOperatorFileCursor<&'a str>>,
    complete: bool,
}
fn serialize_evidence_files<S: serde::Serializer>(
    files: &&[(&str, Option<String>)],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap as _;
    let mut entries = serializer.serialize_map(Some(files.len()))?;
    for (path, value) in *files {
        entries.serialize_entry(path, value)?;
    }
    entries.end()
}
fn ready<T: Serialize>(
    value: &T,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<TaskCommandProgress> {
    let (value, custody) = crate::pi_root_provider::guarded_pi_value(
        value,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        work,
    )?;
    Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
}
