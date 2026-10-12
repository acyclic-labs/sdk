//! Native Pi fork controls over the one retained catalog and task journal.
use crate::{
    LocalAuthorityBackend, LocalLazyWorkspace, LocalObjectBackend, PendingSpawn,
    PendingSpawnLifecycle, PluginTaskBinding, ServiceResources, SharedPhysicalRoot,
    pi_fork::{
        PiForkCoordinator, PiForkOperations, PiForkPlan, PreparedPiFork, RetainedSpawnCommit,
        derived, original_operation,
    },
    pi_native_host::PiNativeFilesystem,
    pi_owner::PiOwnerConfig,
    pi_root_provider::PiRootHandle,
    pi_task::{PiTaskExecution, PiTaskInput},
};
use acyclic_fs::{LazySnapshotRef, NativeMaterializationWork, WorkBudget, WorkCounters};
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    core::SchemaRegistry,
    durable_host::TaskJournalOwner,
    filesystem::{ForkPreparationLimits, TaskCommandProgress},
    runtime::TaskContext,
    scheduler::LeaseFence,
    store::StreamAggregate,
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, sync::Arc};

pub(crate) const PI_FORK_COMMAND_KIND: &str = "acyclic.pi.fork";
pub(crate) const PI_OBSERVE_SOURCE_COMMAND_KIND: &str = "acyclic.pi.observe-source";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiForkCommand {
    pub(crate) commit: RetainedSpawnCommit,
    pub(crate) root_source: Option<LazySnapshotRef>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiObserveSourceCommand {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) root_key: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObservedSource {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) root_key: String,
    pub(crate) snapshot: LazySnapshotRef,
}

pub(crate) const OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
        + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "ObservedSource",
            "caller",
            "sdk_operation",
            "root_key",
            "snapshot",
            "TaskId",
            "LazySnapshotRef",
            "id",
            "workspace_id",
            "authored_generation",
            "source",
            "overlay",
            "shadows",
            "SourceReference",
            "identity",
            "epoch",
            "an array of length 16",
        ]);

#[derive(Serialize)]
struct PiPreparedReply<'a> {
    task: TaskId,
    binding: &'a crate::PiTaskBinding,
    state: &'static str,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootSource<S = String> {
    pub(crate) snapshot: LazySnapshotRef,
    pub(crate) root_key: S,
}

const FORK_COMMAND_DIAGNOSTIC_LABEL_BYTES: u64 = crate::pi_fork::FORK_PLAN_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiForkCommand",
        "commit",
        "root_source",
    ]);
const OBSERVE_COMMAND_DIAGNOSTIC_LABEL_BYTES: u64 = OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES
    + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&["PiObserveSourceCommand"]);
const PENDING_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
        + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "PendingSpawn",
            "task_binding",
            "agent_id",
            "parent_agent_id",
            "tool_use_id",
            "active_root_id",
            "expires_at_millis",
            "workspace_name",
            "fork_key",
            "roots",
            "mount_path",
            "lifecycle",
            "PluginTaskBinding",
            "kind",
            "binding",
            "Pi",
            "pi",
            "HostManaged",
            "host_managed",
            "PiTaskBinding",
            "native_session_id",
            "task_id",
            "agent_id",
            "project_volumes",
            "scratch",
            "results",
            "PendingSpawnLifecycle",
            "preparing",
            "mounting",
            "prepared",
            "discarding",
        ]);

impl<'a> crate::pi_host::PiControlHost<'a> {
    /// The actor durably saves these existing Preparing records before another pass.
    pub(crate) fn pending_to_save(
        &self,
    ) -> Result<Option<(Vec<PendingSpawn>, acyclic_fs::NativeAllocation)>> {
        Ok(self
            .pending
            .lock()
            .map_err(|_| invalid("Pi pending control lock poisoned"))?
            .take())
    }

    /// The actor saves the Created route before handing this to the native host.
    pub(crate) fn take_prepared(&self, task: TaskId) -> Result<Option<PreparedPiFork>> {
        let mut prepared = self
            .prepared
            .lock()
            .map_err(|_| invalid("Pi prepared control lock poisoned"))?;
        let Some((entries, _)) = prepared.as_mut() else {
            return Ok(None);
        };
        let Some(index) = entries
            .iter()
            .position(|entry| entry.binding.task_id == task)
        else {
            return Ok(None);
        };
        Ok(Some(entries.swap_remove(index)))
    }

    fn retain_prepared(
        &self,
        value: PreparedPiFork,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let mut retained = self
            .prepared
            .lock()
            .map_err(|_| invalid("Pi prepared control lock poisoned"))?;
        if let Some((entries, _)) = retained.as_mut() {
            if let Some(existing) = entries
                .iter_mut()
                .find(|entry| entry.binding.task_id == value.binding.task_id)
            {
                *existing = value;
                return Ok(());
            }
        }
        let length = retained.as_ref().map_or(0, |(entries, _)| entries.len());
        let capacity = length.checked_add(1).ok_or(Error::NativeWork {
            cause: acyclic_fs::WorkError::Overflow.into(),
            custody: None,
        })?;
        let copied = length
            .checked_mul(std::mem::size_of::<PreparedPiFork>())
            .ok_or(Error::NativeWork {
                cause: acyclic_fs::WorkError::Overflow.into(),
                custody: None,
            })? as u64;
        work.admit(WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        })?;
        let mut replacement = crate::pi_native::pi_native_vec(capacity, work)?;
        if let Some((entries, old_custody)) = retained.take() {
            replacement.0.extend(entries);
            drop(old_custody);
        }
        work.spent(WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        })?;
        replacement.0.push(value);
        *retained = Some(replacement);
        Ok(())
    }

    pub(crate) fn coordinator(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<PiForkCoordinator<'_>> {
        PiForkCoordinator::new(
            self.runtime.clone(),
            self.filesystem.clone(),
            self.stream.clone(),
            self.resources,
            self.protected.clone(),
            work,
        )
    }

    async fn context(
        &self,
        shared: Arc<TaskContext>,
        fence: &LeaseFence,
        work: &NativeMaterializationWork,
    ) -> Result<(
        (
            Arc<TaskContext>,
            Arc<TaskJournalOwner<LocalStream>>,
            PiTaskInput,
        ),
        acyclic_fs::NativeAllocation,
    )> {
        let caller = self.caller(shared.as_ref(), fence, work).await?;
        let (owner, input, _) = &caller;
        let task = owner.task_id();
        let shared = if task == self.protected.policy().root_task {
            let PiTaskExecution::Host { root, .. } = &input.execution else {
                return Err(unauthorized("Pi original root input substituted a child"));
            };
            crate::pi_root_provider::bind_root_output_context_with_native_work(
                shared,
                owner,
                self.protected.clone(),
                self.resources,
                self.filesystem.clone(),
                self.stream.clone(),
                &input.binding,
                root,
                work,
            )
            .await?
        } else {
            shared
                .with_output_volume_with_native_work(&input.binding.scratch.volume, work)
                .await?
        };
        Ok(((shared, caller.0, caller.1), caller.2))
    }

    fn decode<T: serde::de::DeserializeOwned, S: Serialize>(
        &self,
        value: &S,
        label_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let maximum = self.protected.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(value, maximum, work)?;
        let (value, custody) = crate::pi_root_provider::guarded_pi_decode(
            &bytes.0,
            maximum,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            label_bytes,
            work,
        )?;
        Ok((value, custody))
    }

    async fn source(
        &self,
        shared: &Arc<TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        caller: &PiTaskInput,
        command: &PiForkCommand,
        plan: Option<&PiForkPlan>,
        work: &NativeMaterializationWork,
    ) -> Result<(
        Arc<crate::LocalLazyWorkspace>,
        LazySnapshotRef,
        Option<acyclic_fs::NativeAllocation>,
    )> {
        let root_key = command
            .commit
            .child
            .project_volumes
            .keys()
            .next()
            .ok_or_else(|| invalid("Pi fork has no selected project"))?;
        if command.commit.child.project_volumes.len() != 1 {
            return Err(invalid(
                "Pi fork requires exactly its actually selected project",
            ));
        }
        crate::pi_inspection::authorize_selected_source(
            &self.runtime.runtime,
            self.protected.policy().root_task,
            shared.as_ref(),
            owner,
            command.commit.source_task,
            self.protected.policy().session_limits.depth,
            work,
        )
        .await?;
        let source = if command.commit.source_task == self.protected.policy().root_task {
            let actual = self
                .roots
                .get(root_key)
                .ok_or_else(|| invalid("Pi original selected root has no actual lazy handle"))?;
            Arc::clone(&actual.workspace)
        } else {
            self.native()?
                .live_workspace(command.commit.source_task)
                .ok_or_else(|| {
                    invalid("Pi selected child has no actual reopened creator workspace")
                })?
        };
        if let Some(plan) = plan {
            source
                .validate_snapshot_with_native_work(plan.frozen, work)
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            return Ok((source, plan.frozen, None));
        }
        let mut source_custody = None;
        let frozen = if command.commit.source_task == self.protected.policy().root_task {
            if let Some(frozen) = command.root_source {
                source
                    .validate_snapshot_with_native_work(frozen, work)
                    .await
                    .map_err(crate::pi_fork_host::native_lazy_error)?;
                frozen
            } else if owner.task_id() == self.protected.policy().root_task {
                let actual = self
                    .roots
                    .get(root_key)
                    .ok_or_else(|| invalid("Pi selected root handle missing"))?;
                let (snapshot, custody) = freeze_created_root_source(
                    self.protected.clone(),
                    self.resources,
                    self.filesystem.clone(),
                    self.stream.clone(),
                    shared,
                    owner,
                    caller,
                    root_key,
                    actual,
                    work,
                )
                .await?;
                source_custody = Some(custody);
                snapshot
            } else {
                return Err(invalid(
                    "Pi selected Root requires its real original Root observe control receipt",
                ));
            }
        } else {
            let source_owner = self
                .runtime
                .runtime
                .task_host()
                .readonly_journal_owner_with_native_work(command.commit.source_task, work)
                .await?;
            let outcome = source_owner.outcome_with_native_work(work).await?;
            if outcome
                .0
                .as_ref()
                .is_some_and(|outcome| outcome.is_terminal())
            {
                let (snapshot, custody) = self
                    .native()?
                    .selection_snapshot(command.commit.source_task, work)
                    .await?;
                source_custody = Some(custody);
                snapshot
            } else if let Some(live) = self
                .native()?
                .live_source(command.commit.source_task, work)
                .await?
            {
                if live.prepared.binding.task_id != command.commit.source_task
                    || live.prepared.workspace.workspace().id() != source.workspace().id()
                {
                    return Err(unauthorized(
                        "Pi live source substituted its actual creator workspace",
                    ));
                }
                let (snapshot, custody) = crate::pi_fork_host::freeze_child_source(
                    live.context.as_ref(),
                    &live.owner,
                    &live.view,
                    &source,
                    &live.original_work,
                )
                .await?;
                source_custody = Some(custody);
                owner
                    .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                    .await?;
                snapshot
            } else {
                let frozen = self
                    .native()?
                    .source_frozen(command.commit.source_task)
                    .ok_or_else(|| {
                        invalid("Pi undispatched source lost its actual first native companion")
                    })?;
                source
                    .validate_snapshot_with_native_work(frozen, work)
                    .await
                    .map_err(crate::pi_fork_host::native_lazy_error)?;
                frozen
            }
        };
        Ok((source, frozen, source_custody))
    }

    fn parent_alias(&self, task: TaskId) -> Result<&str> {
        if task == self.protected.policy().root_task {
            return Ok(&self.retained.root_session_id);
        }
        self.retained
            .routes
            .values()
            .find_map(|route| match &route.task_binding {
                PluginTaskBinding::Pi(binding) if binding.task_id == task => {
                    Some(route.agent_id.as_str())
                }
                _ => None,
            })
            .ok_or_else(|| unauthorized("Pi original caller has no actual persisted route alias"))
    }

    pub(crate) fn reservation_with_native_work(
        &self,
        plan: &PiForkPlan,
        work: &NativeMaterializationWork,
    ) -> Result<(PendingSpawn, acyclic_fs::NativeAllocation)> {
        let root_key = plan
            .commit
            .child
            .project_volumes
            .keys()
            .next()
            .ok_or_else(|| invalid("Pi project missing"))?;
        let mut root = [0; 16];
        hex::decode_to_slice(root_key, &mut root)
            .map_err(|_| invalid("Pi pending root identity is not canonical"))?;
        let paths = crate::pi_native::planned_native_paths(
            self.resources,
            &self.retained.root_session_id,
            plan.commit.child.task_id,
            root_key,
            work,
        )?;
        if paths.0.cwd != plan.commit.launch.invocation.cwd
            || paths.0.session_file != plan.commit.launch.invocation.session_file
        {
            return Err(unauthorized(
                "Pi SDK retained native paths differ from its original planned namespace",
            ));
        }
        let project = plan
            .commit
            .child
            .project_volumes
            .values()
            .next()
            .ok_or_else(|| invalid("Pi project missing"))?;
        let workspace = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = project.storage_name(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        #[derive(Serialize)]
        struct Pending<'b> {
            task_binding: PluginTaskBinding<&'b crate::PiTaskBinding>,
            agent_id: Option<&'b str>,
            parent_agent_id: &'b str,
            tool_use_id: &'b str,
            active_root_id: Option<[u8; 16]>,
            expires_at_millis: u64,
            workspace_name: &'b str,
            fork_key: [u8; 16],
            roots: BTreeMap<String, crate::RouteRoot>,
            mount_path: &'b std::path::Path,
            lifecycle: PendingSpawnLifecycle,
        }
        let pending = Pending {
            task_binding: PluginTaskBinding::Pi(&plan.commit.child),
            agent_id: Some(&plan.commit.alias),
            parent_agent_id: self.parent_alias(plan.commit.caller)?,
            tool_use_id: &plan.commit.sdk_operation,
            active_root_id: Some(root),
            expires_at_millis: crate::now_millis().saturating_add(2 * 60 * 1_000),
            workspace_name: &workspace.0,
            fork_key: PiForkOperations::new(plan.original, work)?
                .fork
                .into_bytes(),
            roots: BTreeMap::new(),
            mount_path: &paths.0.view_root,
            lifecycle: PendingSpawnLifecycle::Preparing,
        };
        let (value, custody) = self.decode(&pending, PENDING_DIAGNOSTIC_LABEL_BYTES, work)?;
        Ok((value, custody))
    }

    fn push_pending(
        &self,
        mut record: (PendingSpawn, acyclic_fs::NativeAllocation),
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| invalid("Pi pending control lock poisoned"))?;
        if let Some((records, custody)) = pending.as_mut() {
            if records.len() < records.capacity() {
                custody.absorb(&mut record.1)?;
                records.push(record.0);
                return Ok(());
            }
        }
        let length = pending.as_ref().map_or(0, |(records, _)| records.len());
        let capacity = length
            .checked_add(1)
            .ok_or_else(|| invalid("Pi pending capacity overflow"))?;
        let copied = length
            .checked_mul(std::mem::size_of::<PendingSpawn>())
            .ok_or_else(|| invalid("Pi pending copy overflows"))? as u64;
        work.admit(WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        })?;
        let mut replacement = crate::pi_native::pi_native_vec(capacity, work)?;
        if let Some((records, custody)) = pending.as_mut() {
            custody.absorb(&mut record.1)?;
            custody.absorb(&mut replacement.1)?;
            let old_bytes = records
                .capacity()
                .checked_mul(std::mem::size_of::<PendingSpawn>())
                .ok_or_else(|| invalid("Pi pending previous capacity overflows"))?
                as u64;
            let mut previous = pending
                .take()
                .expect("the original pending component remains installed");
            replacement.0.extend(previous.0);
            if let Err(cause) = previous.1.release(old_bytes) {
                *pending = Some((replacement.0, previous.1));
                return Err(acyclic_harness::Error::from(cause));
            }
            replacement.1 = previous.1;
        } else {
            replacement.1.absorb(&mut record.1)?;
        }
        if let Err(cause) = work.spent(WorkCounters {
            bytes_copied: copied,
            ..WorkCounters::UNCHARGED
        }) {
            *pending = Some(replacement);
            return Err(acyclic_harness::Error::from(cause));
        }
        replacement.0.push(record.0);
        *pending = Some(replacement);
        Ok(())
    }

    pub(crate) async fn fork(
        &self,
        shared: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
        work: &NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let mut supplied: (PiForkCommand, acyclic_fs::NativeAllocation) =
            self.decode(&payload, FORK_COMMAND_DIAGNOSTIC_LABEL_BYTES, work)?;
        supplied
            .0
            .commit
            .launch
            .invocation
            .environment
            .retain(|key, _| !crate::pi_caller::is_environment_key(key));
        let input = supplied.0;
        let cancel = work.cancellation().clone();
        let actual = self.context(shared, &fence, work).await?;
        let (shared, owner, caller) = &actual.0;
        let context = shared.as_ref();
        let original = original_operation(
            self.protected.policy().root_task,
            owner.task_id(),
            &input.commit.sdk_operation,
            &work,
        )?;
        let operations = PiForkOperations::new(original, &work)?;
        if command.operation_id != original
            || input.commit.caller != owner.task_id()
            || input.commit.launch.source.parent_session_id != caller.binding.native_session_id
        {
            return Err(unauthorized(
                "Pi fork control changed its original caller/SDK operation",
            ));
        }
        if self.retained.pending.iter().any(|pending| {
            pending.agent_id.as_deref() == Some(input.commit.alias.as_str())
                && pending.fork_key != operations.fork.into_bytes()
        }) {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi requested alias is already reserved by another original fork",
            });
        }
        if self.retained.routes.values().any(|route| route.agent_id == input.commit.alias
            && !matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if same_identity(binding, &input.commit.child))) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi requested alias already belongs to another admitted child" });
        }
        let coordinator = self.coordinator(&work)?;
        let existing = coordinator.load_plan(&context, original).await?;
        if reconcile && existing.is_none() {
            return Ok(TaskCommandProgress::Pending);
        }
        let (source, frozen, _source_custody) = self
            .source(shared, owner, caller, &input, existing.as_ref(), work)
            .await?;
        let conversation = self.protected.conversation_authority_with_native_work(
            owner.task_id(),
            caller.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let mut authority = (conversation.0, conversation.1);
        let mut verifier = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let parent = StreamAggregate::open_with_native_work(
            &self.stream,
            authority.0.0,
            verifier.0,
            SchemaRegistry::new(),
            work,
        )
        .await?;
        let mut parent_custody = parent.1;
        drop(authority.0.1);
        drop(authority.0.2);
        parent_custody.absorb(&mut authority.1)?;
        parent_custody.absorb(&mut verifier.1)?;
        let mut parent = parent.0;
        parent.set_limits(context.scope().limits())?;
        let plan = coordinator
            .begin(
                shared,
                owner,
                &fence,
                parent,
                source.clone(),
                frozen,
                input.commit,
                ForkPreparationLimits {
                    resources: 128,
                    work: 1_000_000,
                    bytes: self.protected.policy().maximum_payload_bytes,
                },
                remaining(&work)?,
                cancel.clone(),
            )
            .await?;
        let saved = self
            .retained
            .pending
            .iter()
            .find(|pending| pending.fork_key == operations.fork.into_bytes());
        if let Some(saved) = saved {
            let PluginTaskBinding::Pi(binding) = &saved.task_binding else {
                return Err(unauthorized(
                    "Pi pending substituted host-managed admission",
                ));
            };
            let root_key = plan
                .commit
                .child
                .project_volumes
                .keys()
                .next()
                .ok_or_else(|| invalid("Pi child project missing"))?;
            let mut root = [0; 16];
            hex::decode_to_slice(root_key, &mut root)
                .map_err(|_| invalid("Pi pending root identity is not canonical"))?;
            let paths = crate::pi_native::planned_native_paths(
                self.resources,
                &self.retained.root_session_id,
                plan.commit.child.task_id,
                root_key,
                work,
            )?;
            let workspace = plan
                .commit
                .child
                .project_volumes
                .values()
                .next()
                .ok_or_else(|| invalid("Pi child project missing"))?
                .storage_name_with_native_work(work)?;
            if !same_identity(binding, &plan.commit.child)
                || saved.tool_use_id != plan.commit.sdk_operation
                || saved.agent_id.as_deref() != Some(plan.commit.alias.as_str())
                || saved.parent_agent_id != self.parent_alias(plan.commit.caller)?
                || saved.workspace_name != workspace.0
                || saved.active_root_id != Some(root)
                || saved.mount_path != paths.0.view_root
                || !saved.roots.is_empty()
                || !matches!(&saved.lifecycle, PendingSpawnLifecycle::Preparing)
                || saved.expires_at_millis <= crate::now_millis()
            {
                return Err(unauthorized(
                    "Pi pending changed or expired its original preparation identity",
                ));
            }
        } else if let Some(route) = self
            .retained
            .routes
            .values()
            .find(|route| route.agent_id == plan.commit.alias)
        {
            let PluginTaskBinding::Pi(binding) = &route.task_binding else {
                return Err(unauthorized(
                    "Pi existing route is not its original admitted child",
                ));
            };
            if !same_identity(binding, &plan.commit.child)
                || route.turn_id != plan.commit.sdk_operation
                || route.parent_agent_id != self.parent_alias(plan.commit.caller)?
            {
                return Err(unauthorized(
                    "Pi existing route changed its original preparation identity",
                ));
            }
        } else {
            let reservation = self.reservation_with_native_work(&plan, work)?;
            coordinator
                .admit_preparation(&context, &owner, &fence, &plan)
                .await?;
            self.push_pending(reservation, work)?;
            return Ok(TaskCommandProgress::Pending);
        }
        let Some(prepared) = coordinator
            .advance(
                shared,
                &owner,
                &fence,
                &plan,
                source,
                remaining(&work)?,
                cancel,
            )
            .await?
        else {
            return Ok(TaskCommandProgress::Pending);
        };
        let task = prepared.binding.task_id;
        let result = ready(
            &PiPreparedReply {
                task,
                binding: &prepared.binding,
                state: "prepared",
            },
            &work,
        )?;
        self.retain_prepared(prepared, work)?;
        Ok(result)
    }

    pub(crate) async fn observe_source(
        &self,
        shared: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let supplied: (PiObserveSourceCommand, acyclic_fs::NativeAllocation) =
            self.decode(&payload, OBSERVE_COMMAND_DIAGNOSTIC_LABEL_BYTES, work)?;
        let input = supplied.0;
        let cancel = work.cancellation().clone();
        let actual_context = self.context(shared, &fence, work).await?;
        let (shared, owner, caller) = &actual_context.0;
        let context = shared.as_ref();
        let original = original_operation(
            self.protected.policy().root_task,
            input.caller,
            &input.sdk_operation,
            &work,
        )?;
        if owner.task_id() != self.protected.policy().root_task
            || input.sdk_operation.is_empty()
            || command.operation_id != derived(original, "observe-root-source", &work)?
        {
            return Err(unauthorized(
                "Pi Root observer changed its actual owner or original SDK operation",
            ));
        }
        crate::pi_inspection::authorize_selected_source(
            &self.runtime.runtime,
            self.protected.policy().root_task,
            context,
            owner,
            input.caller,
            self.protected.policy().session_limits.depth,
            work,
        )
        .await?;
        let actual = self
            .roots
            .get(&input.root_key)
            .ok_or_else(|| invalid("Pi observed root has no actual lazy producer"))?;
        if let Some(reference) = crate::pi_root_provider::observe_pi_input(
            &self.protected,
            &self.filesystem,
            command.operation_id,
            "root-source-observation",
            &work,
        )
        .await?
        {
            let bytes = self
                .coordinator(&work)?
                .read_original_input(&reference.0)
                .await?;
            let decoded: (ObservedSource, _) = crate::pi_root_provider::guarded_pi_decode(
                bytes.as_ref(),
                self.protected.policy().maximum_payload_bytes,
                self.protected.policy().maximum_payload_bytes,
                acyclic_harness::wire::ErrorCode::Invalid,
                acyclic_harness::JsonValueSchema::None,
                OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES,
                &work,
            )?;
            let observed = &decoded.0;
            if observed.caller != input.caller
                || observed.sdk_operation != input.sdk_operation
                || observed.root_key != input.root_key
            {
                return Err(unauthorized(
                    "Pi original Root observation retry changed its source request",
                ));
            }
            actual
                .workspace
                .validate_snapshot_with_native_work(observed.snapshot, work)
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            return ready(
                &PiRootSource {
                    snapshot: observed.snapshot,
                    root_key: &observed.root_key,
                },
                &work,
            );
        }
        let frozen = freeze_created_root_source(
            self.protected.clone(),
            self.resources,
            self.filesystem.clone(),
            self.stream.clone(),
            shared,
            owner,
            caller,
            &input.root_key,
            actual,
            work,
        )
        .await?;
        let snapshot = frozen.0;
        owner
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?)))
            .await?;
        let observed = ObservedSource {
            caller: input.caller,
            sdk_operation: input.sdk_operation,
            root_key: input.root_key,
            snapshot,
        };
        let _published = crate::pi_root_provider::stage_pi_input(
            &self.protected,
            &self.filesystem,
            command.operation_id,
            "root-source-observation",
            &observed,
            &work,
        )
        .await?;
        owner
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?)))
            .await?;
        ready(
            &PiRootSource {
                snapshot: observed.snapshot,
                root_key: &observed.root_key,
            },
            &work,
        )
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the original creator, current owner and native source remain explicit"
)]
pub(crate) async fn freeze_created_root_source(
    protected: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<PiNativeFilesystem>,
    stream: std::sync::Arc<LocalStream>,
    shared: &Arc<TaskContext>,
    owner: &Arc<TaskJournalOwner<LocalStream>>,
    input: &PiTaskInput,
    key: &str,
    actual: &PiRootHandle,
    work: &NativeMaterializationWork,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
    let PiTaskExecution::Host { root, .. } = &input.execution else {
        return Err(unauthorized(
            "Root source substituted its original admitted bootstrap input",
        ));
    };
    let preparation = crate::pi_root_provider::load_original_preparation_with_native_work(
        &protected,
        &filesystem,
        &input.binding,
        root,
        shared.clone(),
        work,
    )
    .await?;
    let provider = crate::pi_root_provider::root_provider(
        protected.clone(),
        resources,
        filesystem.clone(),
        stream.clone(),
        preparation,
        None,
        work.clone(),
    )?;
    let attempt = acyclic_harness::EffectAttemptId::from_bytes(
        provider.effect_id_with_native_work(work)?.into_bytes(),
    );
    let original = provider
        .observe_allocation_with_native_work(attempt, work)
        .await?
        .ok_or_else(|| unauthorized("Root freeze has no actual original created allocation"))?;
    crate::pi_fork_host::freeze_root_source_with_native_work(
        protected.policy().root_task,
        shared.as_ref(),
        owner,
        &actual.physical,
        &actual.workspace,
        work,
        || async {
            owner
                .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                .await?;
            provider
                .verify_created_source(owner, key, &original.0.1, work)
                .await?;
            owner
                .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                .await
        },
    )
    .await
}
fn same_identity(left: &crate::PiTaskBinding, right: &crate::PiTaskBinding) -> bool {
    left.task_id == right.task_id
        && left.agent_id == right.agent_id
        && left.native_session_id == right.native_session_id
        && left.project_volumes == right.project_volumes
        && left.scratch.volume == right.scratch.volume
        && left.results.as_ref().map(|value| &value.volume)
            == right.results.as_ref().map(|value| &value.volume)
}
fn invalid(message: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason: message,
    }
}
fn unauthorized(message: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason: message,
    }
}

fn ready<T: Serialize>(
    response: &T,
    work: &NativeMaterializationWork,
) -> Result<TaskCommandProgress> {
    let (value, custody) = crate::pi_root_provider::guarded_pi_value(
        response,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        work,
    )?;
    Ok(TaskCommandProgress::ReadyWithNativeCustody { value, custody })
}

fn remaining(work: &NativeMaterializationWork) -> Result<WorkBudget> {
    work.remaining().map_err(acyclic_harness::Error::from)
}

/// The Root's actual created-source guard spans the watcher, rebind and freeze on the command's existing ledger.
pub(crate) async fn freeze_root_source_with_native_work<P, F, G>(
    original_root: acyclic_harness::TaskId,
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<P>,
    physical: &SharedPhysicalRoot,
    source: &LocalLazyWorkspace,
    work: &acyclic_fs::NativeMaterializationWork,
    mut guard: G,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)>
where
    P: acyclic_stream::StreamProvider,
    G: FnMut() -> F + Send,
    F: Future<Output = Result<()>> + Send,
{
    if owner.task_id() != original_root || context.durable_task_id() != Some(original_root) {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Root snapshot requires its actual original Root context and owner",
        });
    }
    owner
        .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
        .await?;
    let observed = physical.observe_with_native_work(work, &mut guard).await?;
    guard().await?;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work,
        cancellation: work.cancellation(),
    };
    let rebound = source
        .rebind_source(&mut accounting)
        .await
        .map_err(native_lazy_error)?;
    guard().await?;
    if rebound.source != observed.0.source {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source rebind changed its guarded watcher observation",
        });
    }
    let frozen = source
        .snapshot(&mut accounting)
        .await
        .map_err(native_lazy_error)?;
    guard().await?;
    if frozen.source != observed.0.source {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source freeze changed its guarded watcher observation",
        });
    }
    owner
        .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
        .await?;
    drop(rebound);
    drop(observed.0);
    Ok((frozen, observed.1))
}

pub(crate) fn native_lazy_error(source: acyclic_fs::LazyWorkspaceError) -> Error {
    match source {
        acyclic_fs::LazyWorkspaceError::NativeWork(cause) => cause.into(),
        source => source.into(),
    }
}

/// Only an active source's ORIGINAL native view owner may checkpoint its dirty mounts.
pub(crate) async fn freeze_child_source<P: acyclic_stream::StreamProvider>(
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<P>,
    view: &acyclic_harness::filesystem::NativeVolumeView<LocalAuthorityBackend, LocalObjectBackend>,
    source: &LocalLazyWorkspace,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
    if context.durable_task_id() != Some(owner.task_id()) {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi source checkpoint requires its actual source task context",
        });
    }
    owner
        .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
        .await?;
    let checkpoint = view
        .checkpoint_projects_with_native_work(owner, work)?
        .await?;
    owner
        .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
        .await?;
    let mut selected = checkpoint
        .0
        .iter()
        .filter(|(_, snapshot)| snapshot.workspace_id == source.workspace().id());
    let snapshot = selected
        .next()
        .ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi checkpoint has no actual selected source workspace",
        })?
        .1;
    if selected.next().is_some() {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi checkpoint repeats a selected source workspace",
        });
    }
    drop(checkpoint.0);
    Ok((snapshot, checkpoint.1))
}
