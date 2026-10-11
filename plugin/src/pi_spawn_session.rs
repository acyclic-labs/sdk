//! SpawnCommit consumes the original SDK cut and ordinary fork preparation.
//! The catalog barriers precede creator execution and native background dispatch.
//! SDK reply custody spans source decoding, catalog saves and native registration.

use super::*;
use acyclic_fs::{LazySnapshotRef, NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{ContentResidencyVerifier, FilesystemContentVerifier, TaskCommandHost, TaskCommandProgress},
    runtime::NativeTaskContext,
    workflow::WorkflowCommand,
};
use serde::de::DeserializeOwned;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedRootSource {
    caller: TaskId,
    sdk_operation: String,
    root_key: String,
    snapshot: LazySnapshotRef,
}

// Only these closed DTOs are retained here; neither contains a serde_json::Value.
trait SpawnRetainedInput: DeserializeOwned {
    const DIAGNOSTIC_LABEL_BYTES: u64;
}
impl SpawnRetainedInput for RetainedRootSource {
    const DIAGNOSTIC_LABEL_BYTES: u64 = pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "RetainedRootSource", "caller", "sdk_operation", "root_key", "snapshot",
        "TaskId", "LazySnapshotRef", "id", "workspace_id", "authored_generation",
        "source", "overlay", "shadows", "LazySnapshotId", "WorkspaceId",
        "GenerationId", "Digest", "SourceReference", "identity", "epoch",
        "LazyOverlayId", "LazyShadowId",
        "a string", "a UUID string", "u64", "u8",
        "an array of length 16", "an array of length 32",
    ]);
}
impl SpawnRetainedInput for pi_native::PiNativeLaunchIntent {
    const DIAGNOSTIC_LABEL_BYTES: u64 = pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES;
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnRootSourceAcknowledgement {
    root_key: String,
    snapshot: LazySnapshotRef,
}

#[derive(Serialize)]
struct SpawnObservedChild<'a> {
    id: &'a str,
    task: TaskId,
    native_session_id: &'a str,
    cwd: &'a Path,
    session_file: &'a Path,
    session_directory: Option<&'a Path>,
}

#[derive(Serialize)]
struct SpawnObservedPreparation<'a> {
    task: TaskId,
    operation: acyclic_harness::OperationId,
    outcome: &'a Option<Outcome<Value>>,
}

#[derive(Serialize)]
struct SpawnObservation<'a, E: Serialize> {
    operation: &'a str,
    state: &'a str,
    uncertain: bool,
    child: SpawnObservedChild<'a>,
    preparation: SpawnObservedPreparation<'a>,
    fork_operation: acyclic_harness::OperationId,
    native_operation: acyclic_harness::OperationId,
    native_effect: E,
    outcome: &'a Option<Outcome<Value>>,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "binding", rename_all = "snake_case")]
enum SpawnBorrowedBinding<'a> { Pi(&'a PiTaskBinding) }

struct SpawnBorrowedRoot<'a> { key: &'a str, root: RouteRoot }
impl Serialize for SpawnBorrowedRoot<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.key, &self.root)?;
        map.end()
    }
}

#[derive(Serialize)]
struct SpawnBorrowedRoute<'a> {
    task_binding: SpawnBorrowedBinding<'a>, agent_id: &'a str, turn_id: &'a str,
    context_id: [u8; 16], root_id: [u8; 16], parent_agent_id: &'a str,
    roots: SpawnBorrowedRoot<'a>, mount_path: &'a Path, lifecycle: RouteLifecycle,
}

const SPAWN_ROUTE_DIAGNOSTIC_LABEL_BYTES: u64 =
    pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "Route", "task_binding", "agent_id", "turn_id", "context_id", "root_id",
        "parent_agent_id", "roots", "mount_path", "lifecycle", "RouteRoot",
        "repository_workspace_id", "published_generation", "RouteLifecycle",
        "mounting", "active", "stop_requested", "frozen", "PluginTaskBinding",
        "kind", "binding", "pi", "host_managed", "a string", "path string",
        "an array of length 16", "an array of length 32",
    ]);

impl PiSession {
    pub(super) async fn spawn_commit(
        &mut self, caller: TaskId, arguments: &Value, owner: &pi_owner::PiOwnerConfig,
        original_native_work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, NativeAllocation)> {
        self.runtime.verify_owner_with_native_work(owner, original_native_work)?;
        let native = self.native_host_with_native_work(original_native_work)?;
        let work = if caller == owner.policy().root_task {
            original_native_work.clone()
        } else {
            native.invocation_work(caller).await?
        };
        let (commit, commit_custody) = self.spawn_native_commit(caller, arguments, owner, &work)?;
        let original = pi_fork::original_operation_with_native_work(owner.policy().root_task, caller, &commit.sdk_operation, &work)?;
        let operations = pi_fork::PiForkOperations::new_with_native_work(original, &work)?;
        let task = commit.child.task_id;
        let cancellation = work.cancellation();
        let mut retained_custody = Some(commit_custody);
        let coordinator = pi_fork::PiForkCoordinator::new_with_native_work(self.runtime.clone(), self.filesystem.clone(),
            self.stream.clone(), &self.resources, native.original_owner(), &work)?;
        let plan = coordinator.load_original_plan(original, work.remaining().map_err(native_error)?, &cancellation)
            .await?;
        if let Some(plan) = &plan {
            if plan.original != original || !same_spawn_commit(&plan.commit, &commit)
            {
                return Err(acyclic_harness::Error::Invalid("Pi retry changed its original retained native spawn intent".to_owned()));
            }
        }
        if let Some(route) = self.state.routes.get(&commit.alias) {
            self.spawn_validate_route(route, &commit, operations)?;
            let plan = plan.as_ref().ok_or_else(|| acyclic_harness::Error::Invalid("Pi created route lost its original immutable fork plan".to_owned()))?;
            self.spawn_validate_admitted(plan, owner.policy().maximum_payload_bytes, &work).await?;
            let (launch, launch_custody): (pi_native::PiNativeLaunchIntent, NativeAllocation) = self.spawn_input(owner,
                pi_fork::derived_with_native_work(original, "native-input", &work)?, "native-input", &work).await?.ok_or_else(|| acyclic_harness::Error::Invalid("Pi created child lost its original immutable native intent".to_owned()))?;
            if launch != commit.launch {
                return Err(acyclic_harness::Error::Invalid("Pi duplicate changed its original sealed native intent".to_owned()));
            }
            drop(launch);
            spawn_retain_custody(&mut retained_custody, launch_custody)?;
            // A duplicate only observes. In particular it neither seeds another
            // SDK session nor acquires a worker reservation or relaunches Pi.
            return self.spawn_observation(&commit, operations, true, false, &work, owner.policy().maximum_payload_bytes, retained_custody).await;
        }
        let pending = self.state.pending.iter().find(|pending| pending.fork_key == operations.fork.into_bytes());
        if let Some(pending) = pending {
            self.spawn_validate_pending(pending, &commit, operations, &work)?;
            if plan.is_none() {
                return Err(acyclic_harness::Error::Invalid("Pi admitted preparation lost its original immutable fork plan".to_owned()));
            }
        }
        if self.state.routes.len() >= MAXIMUM_ADAPTER_ROUTES
            || (pending.is_none() && self.state.pending.len() >= MAXIMUM_ADAPTER_PENDING)
        {
            return Err(acyclic_harness::Error::Invalid("Pi original child catalog bound is exhausted".to_owned()));
        }
        if self.native_jobs.contains_key(&task) {
            return Err(acyclic_harness::Error::Invalid("Pi uncatalogued task already owns an original native worker".to_owned()));
        }
        let live = if caller == owner.policy().root_task {
            None
        } else {
            let live = native.live_source(caller, &work).await?.ok_or_else(|| acyclic_harness::Error::Invalid("Pi recursive spawn requires its actual running caller owner".to_owned()))?;
            if live.binding != *self.binding(caller)? || live.context.context().durable_task_id() != Some(caller) {
                return Err(acyclic_harness::Error::Invalid("Pi recursive spawn changed its original caller binding".to_owned()));
            }
            let cwd = Path::new(&commit.launch.source.parent_cwd);
            if !live.view.manifest().volumes.iter().any(|mapping| cwd.starts_with(&mapping.path)) {
                return Err(acyclic_harness::Error::Invalid("Pi recursive native source cut is outside its actual caller view".to_owned()));
            }
            live.owner.verify_dispatch().await?;
            Some(live)
        };
        let root_key = commit.child.project_volumes.keys().next().ok_or_else(|| acyclic_harness::Error::Invalid("Pi child has no selected project".to_owned()))?.clone();
        let root_source = if commit.source_task == owner.policy().root_task {
            if let Some(plan) = &plan {
                Some(plan.frozen)
            } else if let Some((observed, custody)) = self.spawn_input::<RetainedRootSource>(
                owner, pi_fork::derived_with_native_work(original, "observe-root-source", &work)?, "root-source-observation",
                &work,
            ).await? {
                let observed = observed;
                if observed.caller != caller || observed.sdk_operation != commit.sdk_operation
                    || observed.root_key != root_key
                {
                    return Err(acyclic_harness::Error::Invalid("Pi retry changed its original Root source observation".to_owned()));
                }
                spawn_retain_custody(&mut retained_custody, custody)?;
                Some(observed.snapshot)
            } else {
                let input = pi_fork_host::PiObserveSourceCommand {
                    caller, sdk_operation: commit.sdk_operation.clone(), root_key: root_key.clone(),
                };
                let operation = pi_fork::derived_with_native_work(original, "observe-root-source", &work)?;
                let (command, command_custody) = self.spawn_command(owner, operation, pi_fork_host::PI_OBSERVE_SOURCE_COMMAND_KIND,
                    &input, &work).await?;
                let (acknowledgement, pending, _) = self.spawn_root_step(owner, native.clone(), command, false, None, &work).await?;
                if pending.is_some() {
                    return Err(acyclic_harness::Error::Invalid("Pi Root source observer unexpectedly admitted a child".to_owned()));
                }
                spawn_retain_custody(&mut retained_custody, command_custody)?;
                let Some((acknowledgement, custody)) = acknowledgement else {
                    return self.spawn_observation(&commit, operations, false, true, &work, owner.policy().maximum_payload_bytes, retained_custody).await;
                };
                let acknowledgement: SpawnRootSourceAcknowledgement =
                    serde_json::from_value(acknowledgement).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
                if acknowledgement.root_key != root_key {
                    return Err(acyclic_harness::Error::Invalid("Pi Root source acknowledgement changed its selected resource".to_owned()));
                }
                spawn_retain_custody(&mut retained_custody, custody)?;
                Some(acknowledgement.snapshot)
            }
        } else {
            None
        };
        let payload = pi_fork_host::PiForkCommand { commit, root_source };
        let commit = &payload.commit;
        let (command, command_custody) = self.spawn_command(owner, original, pi_fork_host::PI_FORK_COMMAND_KIND,
            &payload, &work).await?;

        // A process loss can erase a host's transient pending notification, but
        // not its ordinary preparation admission. Rebuild catalog metadata only
        // after observing that exact original admission, never from path/ACK.
        if !self.state.pending.iter().any(|pending| pending.fork_key == operations.fork.into_bytes()) {
            if let Some(plan) = &plan {
                let root_waiting = if caller == owner.policy().root_task {
                    let (suspension, suspension_custody) = self.runtime.runtime.task_host()
                        .workflow_suspension_with_native_work(caller, &work).await?;
                    let suspension = suspension;
                    let waiting = suspension.as_ref()
                        .is_some_and(|suspension| suspension.waiting_command == Some(original));
                    drop(suspension);
                    spawn_retain_custody(&mut retained_custody, suspension_custody)?;
                    waiting
                } else { true };
                if root_waiting {
                    let (admission, admission_custody) = self.runtime.runtime.task_host()
                        .observe_admission_with_native_work(plan.preparation.task_id(), &work).await?;
                    let admission = admission;
                    if admission != plan.preparation {
                        drop(admission);
                        drop(admission_custody);
                        return Err(acyclic_harness::Error::Invalid("Pi original preparation admission was substituted".to_owned()));
                    }
                    let reservation = pi_fork_host::PiForkHost::new(self.runtime.clone(), self.filesystem.clone(),
                        self.stream.clone(), &self.resources, native.original_owner(), &self.state, &self.roots, native.clone())
                        .reservation_with_native_work(plan, &work)?;
                    let reservation = spawn_pending_batch(reservation, &work)?;
                    self.spawn_save_pending(Some(reservation), &commit, operations, &work)?;
                    drop(admission);
                    drop(admission_custody);
                }
            }
        }
        let saved_pending = self.state.pending.iter().any(|pending| pending.fork_key == operations.fork.into_bytes());
        let (acknowledgement, pending, prepared) = if let Some(live) = &live {
            self.spawn_child_step(owner, native.clone(), &live.context, command.clone(), plan.is_some(), task, &work, &payload).await?
        } else {
            self.spawn_root_step(owner, native.clone(), command.clone(), false, Some(task), &work).await?
        };
        let newly_saved = pending.is_some();
        self.spawn_save_pending(pending, &commit, operations, &work)?;
        let (acknowledgement, prepared) = if prepared.is_some() {
            (acknowledgement, prepared)
        } else if newly_saved || (saved_pending && acknowledgement.is_none()) {
            // A fresh host sees the PowerLoss-saved Preparing record. Only this
            // pass may wake the Root's exact original waiting command/creator.
            if let Some((value, custody)) = acknowledgement {
                drop(value);
                spawn_retain_custody(&mut retained_custody, custody)?;
            }
            let (acknowledgement, pending, prepared) = if let Some(live) = &live {
                live.owner.verify_with_native_work(false, &work).await?;
                self.spawn_child_step(owner, native.clone(), &live.context, command, true, task, &work, &payload).await?
            } else {
                self.spawn_root_step(owner, native.clone(), command, true, Some(task), &work).await?
            };
            self.spawn_save_pending(pending, &commit, operations, &work)?;
            (acknowledgement, prepared)
        } else {
            (acknowledgement, prepared)
        };
        let prepared = match prepared {
            Some(prepared) => Some(prepared),
            None if acknowledgement.is_some() => {
                let plan = coordinator.load_original_plan(original, work.remaining().map_err(native_error)?, &cancellation)
                    .await?.ok_or_else(|| acyclic_harness::Error::Invalid("Pi prepared acknowledgement lost its original fork plan".to_owned()))?;
                let source = if plan.commit.source_task == owner.policy().root_task {
                    Arc::new(self.roots.get(&root_key).ok_or_else(|| acyclic_harness::Error::Invalid("Pi original Root source handle is unavailable".to_owned()))?.workspace.clone())
                } else {
                    native.live_workspace(plan.commit.source_task).ok_or_else(|| acyclic_harness::Error::Invalid("Pi original selected child source handle is unavailable".to_owned()))?
                };
                coordinator.observe_prepared(&plan, source, work.remaining().map_err(native_error)?, cancellation.clone())
                    .await?
            }
            None => None,
        };
        spawn_retain_custody(&mut retained_custody, command_custody)?;
        let Some(prepared) = prepared else {
            if let Some((value, custody)) = acknowledgement {
                drop(value);
                spawn_retain_custody(&mut retained_custody, custody)?;
            }
            return self.spawn_observation(&commit, operations, false, true, &work, owner.policy().maximum_payload_bytes, retained_custody).await;
        };
        if let Some((acknowledgement, custody)) = acknowledgement {
            let acknowledged_task = TaskId::deserialize(acknowledgement.get("task").ok_or_else(|| acyclic_harness::Error::Invalid("Pi fork acknowledgement has no admitted child".to_owned()))?).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
            if acknowledged_task != task {
                return Err(acyclic_harness::Error::Invalid("Pi fork acknowledgement changed its original admitted child".to_owned()));
            }
            drop(acknowledgement);
            spawn_retain_custody(&mut retained_custody, custody)?;
        }
        if let Some(live) = &live { live.owner.verify_with_native_work(false, &work).await?; }
        self.spawn_save_created(&commit, operations, &prepared, &work)?;
        native.register_prepared(prepared).await?;
        // The stock task/effect future may block on native SessionStart, tools
        // and mail RPC. It must never hold the session actor during that wait.
        self.native_jobs.insert(task, tokio::spawn(async move { native.run_child(task).await }));
        self.spawn_observation(&commit, operations, true, false, &work, owner.policy().maximum_payload_bytes, retained_custody).await
    }

    fn spawn_native_commit(
        &self, caller: TaskId, arguments: &Value, owner: &pi_owner::PiOwnerConfig,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<(pi_fork::RetainedSpawnCommit, NativeAllocation)> {
        let sdk_operation = string(arguments, "operation").map_err(acyclic_harness::Error::Invalid)?;
        let alias = string(arguments, "id").map_err(acyclic_harness::Error::Invalid)?;
        if sdk_operation.is_empty() || sdk_operation.len() > 512 || alias.is_empty() || alias.len() > 128
            || alias == "root" || alias == self.state.root_agent_id || alias == self.state.root_session_id
            || !alias.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(acyclic_harness::Error::Invalid("Pi spawn requires bounded original tool and child alias identities".to_owned()));
        }
        let caller_binding = self.binding(caller)?;
        let source_task = match arguments.get("source") {
            None | Some(Value::Null) => caller,
            Some(Value::String(alias)) => self.alias_task(alias)?,
            _ => return Err(acyclic_harness::Error::Invalid("Pi workspace source must be a registered agent identity".to_owned())),
        };
        if source_task != caller && !owner.policy().grants.contains("pi:source:select") {
            return Err(acyclic_harness::Error::Invalid("Pi original root policy does not authorize selected-source reads".to_owned()));
        }
        let source = self.binding(source_task)?;
        let root_key = match arguments.get("root") {
            Some(Value::String(key)) => source.project_volumes.get_key_value(key)
                .map(|(key, _)| key.clone()).ok_or_else(|| acyclic_harness::Error::Invalid("Pi selected root is not an original source project".to_owned()))?,
            None | Some(Value::Null) if source.project_volumes.len() == 1 => source.project_volumes.keys().next().ok_or_else(|| acyclic_harness::Error::Invalid("Pi selected source has no original project".to_owned()))?.clone(),
            None | Some(Value::Null) => return Err(acyclic_harness::Error::Invalid("Pi multi-root source requires its explicit retained root identity".to_owned())),
            _ => return Err(acyclic_harness::Error::Invalid("Pi root selector must be its retained identity".to_owned())),
        };
        let root: [u8; 16] = decode_fixed(&root_key).map_err(acyclic_harness::Error::Invalid)?;
        if root_key != super::super::root_key(WorkspaceRootId::from_bytes(root)) || !self.state.roots.contains_key(&root_key) {
            return Err(acyclic_harness::Error::Invalid("Pi selected project root is not in the original Root allocation".to_owned()));
        }
        let prompt = string(arguments, "prompt").map_err(acyclic_harness::Error::Invalid)?;
        let provider = string(arguments, "provider").map_err(acyclic_harness::Error::Invalid)?;
        let model = string(arguments, "model").map_err(acyclic_harness::Error::Invalid)?;
        let maximum_bytes = owner.policy().limits.file_bytes.min(owner.policy().maximum_payload_bytes)
            .min(MAXIMUM_ADAPTER_STATE_BYTES).min(512 * 1024);
        if maximum_bytes == 0 || prompt.is_empty() || prompt.len() as u64 > maximum_bytes.min(262_144)
            || provider.is_empty() || provider.len() > 256 || model.is_empty() || model.len() > 256
            || provider.contains('\0') || model.contains('\0')
        {
            return Err(acyclic_harness::Error::Invalid("Pi native literal instruction or model exceeds its original bounds".to_owned()));
        }
        let child_session_id = string(arguments, "native_session_id").map_err(acyclic_harness::Error::Invalid)?;
        let uuid = uuid::Uuid::parse_str(&child_session_id).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
        if uuid.get_version() != Some(uuid::Version::Random) || uuid.get_variant() != uuid::Variant::RFC4122
            || uuid.to_string() != child_session_id || child_session_id == caller_binding.native_session_id
        {
            return Err(acyclic_harness::Error::Invalid("Pi child session is not its actual independent SDK UUID".to_owned()));
        }
        let original = pi_fork::original_operation_with_native_work(owner.policy().root_task, caller, &sdk_operation, work)?;
        let operations = pi_fork::PiForkOperations::new_with_native_work(original, work)?;
        let task = acyclic_harness::runtime::task_id_for_admission(operations.child);
        for binding in std::iter::once(&self.state.task_binding)
            .chain(self.state.routes.values().map(|route| &route.task_binding))
            .chain(self.state.pending.iter().map(|pending| &pending.task_binding))
        {
            if let PluginTaskBinding::Pi(binding) = binding
                && binding.native_session_id == child_session_id && binding.task_id != task
            {
                return Err(acyclic_harness::Error::Invalid("Pi SDK child session identity is already owned by another task".to_owned()));
            }
        }
        if self.state.pending.iter().any(|pending| pending.agent_id.as_deref() == Some(alias.as_str())
            && pending.fork_key != operations.fork.into_bytes())
        {
            return Err(acyclic_harness::Error::Invalid("Pi child alias is reserved by another original spawn".to_owned()));
        }
        let selected_project = source.project_volumes.get(&root_key).ok_or_else(|| acyclic_harness::Error::Invalid("Pi selected project binding disappeared".to_owned()))?;
        if selected_project.provider() != caller_binding.scratch.volume.provider()
            || selected_project.provider() != owner.policy().journal.provider()
        {
            return Err(acyclic_harness::Error::Invalid("Pi selected project changed its original filesystem provider".to_owned()));
        }
        let volume_provider = selected_project.provider().clone();
        let project = VolumeRef::new(volume_provider.clone(), format!("project-{task}"), VolumeClass::Project,
            VolumeOwner::Project(root_key.clone()))?;
        let scratch = VolumeRef::new(volume_provider.clone(), "scratch", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(operations.agent))?;
        let results = VolumeRef::new(volume_provider, "native-results", VolumeClass::AgentPrivate,
            VolumeOwner::Agent(operations.agent))?;
        let paths = pi_native::planned_native_paths(&self.resources, &self.state.root_session_id, task, Some(&root_key))?;
        let cut = pi_native::PiNativeSourceCut::deserialize(arguments.get("cut").ok_or_else(|| acyclic_harness::Error::Invalid("Pi spawn has no actual original SDK native source cut".to_owned()))?).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
        let context = match arguments.get("context") {
            None | Some(Value::Null) => "scratch",
            Some(Value::String(context)) => context.as_str(),
            _ => return Err(acyclic_harness::Error::Invalid("Pi context must be scratch or native_cut".to_owned())),
        };
        let mode = match context {
            "scratch" => pi_native::PiNativeSourceMode::Scratch,
            "native_cut" => pi_native::PiNativeSourceMode::CallerTool,
            _ => return Err(acyclic_harness::Error::Invalid("Pi context must be scratch or native_cut".to_owned())),
        };
        let cwd = string(arguments, "cwd").map_err(acyclic_harness::Error::Invalid)?;
        if cut.mode != mode || cut.parent_session_id != caller_binding.native_session_id || cut.parent_cwd != cwd
            || !Path::new(&cwd).is_absolute()
            || (caller == owner.policy().root_task && !self.state.roots.values().any(|root| root.path == Path::new(&cwd)))
        {
            return Err(acyclic_harness::Error::Invalid("Pi native context differs from the original caller; workspace selection never selects a transcript".to_owned()));
        }
        let session_bytes = string(arguments, "session_bytes").map_err(acyclic_harness::Error::Invalid)?;
        if session_bytes.len() as u64 > maximum_bytes || cut.entries_json.len() as u64 > maximum_bytes {
            return Err(acyclic_harness::Error::Invalid("Pi original SDK native session exceeds its context allowance".to_owned()));
        }
        let capture = pi_native::capture_limits(owner.policy().limits, owner.policy().maximum_payload_bytes)?;
        let (invocation, invocation_custody) = owner.native_runtime()?.intent_invocation_with_work(
            &paths.session_file, &paths.cwd, &provider, &model, &prompt, capture, work)?;
        let launch = pi_native::PiNativeLaunchIntent { agent_alias: alias.clone(), child_session_id: child_session_id.clone(),
            source: cut, invocation, session_path: "session.jsonl".to_owned(), session_bytes,
            maximum_entries: 4096, maximum_bytes };
        launch.validate()?;
        let child = PiTaskBinding { native_session_id: child_session_id, task_id: task, agent_id: operations.agent,
            project_volumes: BTreeMap::from([(root_key, project)]),
            scratch: PiScratchBinding { volume: scratch, allocation: PiScratchAllocation::Preparing },
            results: Some(PiScratchBinding { volume: results, allocation: PiScratchAllocation::Preparing }) };
        child.validate(None, child.project_volumes.keys().map(String::as_str), true)?;
        Ok((pi_fork::RetainedSpawnCommit { sdk_operation, alias, prompt, caller, source_task, child, launch }, invocation_custody))
    }

    async fn spawn_input<T: SpawnRetainedInput>(
        &self, owner: &pi_owner::PiOwnerConfig, operation: acyclic_harness::OperationId, label: &str,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<Option<(T, NativeAllocation)>> {
        let reference = pi_root_provider::observe_pi_input_with_native_work(
            owner, &self.filesystem, operation, label, work).await?;
        let Some((reference, reference_custody)) = reference else { return Ok(None); };
        // Owned data is declared after its guard so unwinding destroys data first.
        let reference = reference;
        let authority = owner.authority()?;
        let maximum = owner.policy().maximum_payload_bytes.min(MAXIMUM_ADAPTER_STATE_BYTES);
        let reader = FilesystemContentVerifier::new(self.filesystem.clone(), authority.verifier,
            authority.signed, maximum)?;
        let (bytes, bytes_custody) = reader.read_with_native_work(&reference, maximum, work).await?;
        let bytes = bytes;
        let owned = std::mem::size_of::<T>() as u64;
        let (value, custody) = pi_root_provider::guarded_pi_decode(
            &bytes, maximum, owned, acyclic_harness::JsonValueSchema::None,
            T::DIAGNOSTIC_LABEL_BYTES, work)?;
        drop(bytes);
        drop(bytes_custody);
        drop(reference);
        drop(reference_custody);
        Ok(Some((value, custody)))
    }

    async fn spawn_command<T: Serialize>(
        &self, owner: &pi_owner::PiOwnerConfig, operation: acyclic_harness::OperationId, kind: &str, payload: &T,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<(WorkflowCommand, NativeAllocation)> {
        let (payload, custody) = pi_root_provider::stage_pi_input_with_native_work(
            owner, &self.filesystem, operation, "command", payload, work).await?;
        Ok((WorkflowCommand { operation_id: operation, kind: kind.to_owned(), payload }, custody))
    }

    async fn spawn_root_step(
        &self, owner: &pi_owner::PiOwnerConfig, native: Arc<pi_native_host::PiNativeCommandHost>,
        command: WorkflowCommand, wake_pending: bool, task: Option<TaskId>, work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Option<(Value, NativeAllocation)>, Option<(Vec<PendingSpawn>, NativeAllocation)>, Option<pi_fork::PreparedPiFork>)> {
        let fork = pi_fork_host::PiForkHost::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, native.original_owner(), &self.state, &self.roots, native.clone());
        let selected = pi_selected_host::PiSelectedHost::new_with_native_work(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, native.original_owner(), &self.state, &self.roots, native, work)?;
        let workflow = pi_workflow::PiWorkflowHost::new(&self.relative_directory, self.runtime.clone(), self.filesystem.clone(),
            self.stream.clone(), &self.resources, &self.state).with_controls(&fork, &selected).with_work(work.clone());
        let reply = workflow.control_step_with_native_work(command, wake_pending).await?;
        let pending = fork.pending_to_save()?;
        let prepared = match task { Some(task) => fork.take_prepared(task)?, None => None };
        Ok((reply, pending, prepared))
    }

    async fn spawn_child_step(
        &self, owner: &pi_owner::PiOwnerConfig, native: Arc<pi_native_host::PiNativeCommandHost>,
        context: &Arc<NativeTaskContext>, command: WorkflowCommand, reconcile: bool, task: TaskId,
        work: &NativeMaterializationWork, input: &pi_fork_host::PiForkCommand,
    ) -> acyclic_harness::Result<(Option<(Value, NativeAllocation)>, Option<(Vec<PendingSpawn>, NativeAllocation)>, Option<pi_fork::PreparedPiFork>)> {
        let fork = pi_fork_host::PiForkHost::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, native.original_owner(), &self.state, &self.roots, native);
        let fence = context.context().owned_task_fence()?;
        let (payload, mut input_custody) = pi_root_provider::guarded_pi_value(
            input, owner.policy().maximum_payload_bytes, work)?;
        let progress = if reconcile {
            fork.reconcile_with_native_work(context.clone(), fence, command, payload, work)?.await
        } else {
            fork.execute_with_native_work(context.clone(), fence, command, payload, work)?.await
        }?;
        let reply = match progress {
            TaskCommandProgress::ReadyWithNativeCustody { value, mut custody } => {
                if let Err(cause) = custody.absorb(&mut input_custody) {
                    drop(value);
                    return Err(native_error(cause));
                }
                Some((value, custody))
            }
            TaskCommandProgress::Ready(_) => return Err(acyclic_harness::Error::Invalid("Pi native fork acknowledgement lost its original SDK custody".to_owned())),
            TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => None,
        };
        Ok((reply, fork.pending_to_save()?, fork.take_prepared(task)?))
    }

    fn spawn_parent_alias(&self, caller: TaskId) -> acyclic_harness::Result<&str> {
        if let PluginTaskBinding::Pi(root) = &self.state.task_binding
            && root.task_id == caller
        {
            return Ok(&self.state.root_session_id);
        }
        self.state.routes.values().find_map(|route| match &route.task_binding {
            PluginTaskBinding::Pi(binding) if binding.task_id == caller => Some(route.agent_id.as_str()),
            _ => None,
        }).ok_or_else(|| acyclic_harness::Error::Invalid("Pi original caller has no retained route alias".to_owned()))
    }


    fn spawn_validate_pending(&self, pending: &PendingSpawn, commit: &pi_fork::RetainedSpawnCommit,
        operations: pi_fork::PiForkOperations, work: &NativeMaterializationWork) -> acyclic_harness::Result<()> {
        let (key, project) = commit.child.project_volumes.iter().next().ok_or_else(|| acyclic_harness::Error::Invalid("Pi original pending lost its selected project".to_owned()))?;
        let (paths, paths_custody) = pi_native::planned_native_paths_with_native_work(
            &self.resources, &self.state.root_session_id, commit.child.task_id, Some(key), work)?;
        let paths = paths;
        let (workspace, workspace_custody) = project.storage_name_with_native_work(work)?;
        let workspace = workspace;
        if !matches!(&pending.task_binding, PluginTaskBinding::Pi(binding) if binding.as_ref() == &commit.child)
            || pending.agent_id.as_deref() != Some(commit.alias.as_str())
            || pending.parent_agent_id != self.spawn_parent_alias(commit.caller)?
            || pending.tool_use_id != commit.sdk_operation || pending.active_root_id != Some(decode_fixed(key).map_err(acyclic_harness::Error::Invalid)?)
            || pending.workspace_name != workspace || pending.fork_key != operations.fork.into_bytes()
            || pending.mount_path != paths.view_root || !pending.roots.is_empty()
            || pending.lifecycle != PendingSpawnLifecycle::Preparing || pending.expires_at_millis <= now_millis()
        {
            return Err(acyclic_harness::Error::Invalid("Pi retry changed or expired its original Preparing child catalog record".to_owned()));
        }
        drop(workspace);
        drop(workspace_custody);
        drop(paths);
        drop(paths_custody);
        Ok(())
    }

    fn spawn_save_pending(&mut self, pending: Option<(Vec<PendingSpawn>, NativeAllocation)>,
        commit: &pi_fork::RetainedSpawnCommit, operations: pi_fork::PiForkOperations,
        work: &NativeMaterializationWork) -> acyclic_harness::Result<()> {
        let Some((pending, custody)) = pending else { return Ok(()); };
        let pending = pending;
        if pending.len() != 1 || self.state.pending.iter().any(|saved| saved.fork_key == operations.fork.into_bytes()) {
            return Err(acyclic_harness::Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi preparation returned a duplicate or unrelated original reservation" });
        }
        self.spawn_validate_pending(&pending[0], commit, operations, work)?;
        self.prepare_native_catalog_slot(work)?;
        let original_length = self.state.pending.len();
        if self.state.pending.capacity() == original_length {
            let capacity = original_length.checked_add(1)
                .ok_or_else(|| native_error(acyclic_fs::WorkError::Overflow.into()))?;
            let bytes = capacity.checked_mul(std::mem::size_of::<PendingSpawn>())
                .ok_or_else(|| native_error(acyclic_fs::WorkError::Overflow.into()))? as u64;
            let backing = work.claim(bytes).map_err(native_error)?;
            work.charge(acyclic_fs::WorkCounters { bytes_copied:
                (original_length * std::mem::size_of::<PendingSpawn>()) as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED }).map_err(native_error)?;
            let mut replacement = std::collections::VecDeque::with_capacity(capacity);
            replacement.append(&mut self.state.pending);
            let old = std::mem::replace(&mut self.state.pending, replacement);
            let old_custody = self.replace_native_pending_backing_custody(backing);
            drop(old);
            drop(old_custody);
        }
        self.state.pending.extend(pending);
        let old_custody = self.install_native_catalog_custody(commit.child.task_id,
            crate::service_session::NativeCatalogComponent::Pending, custody);
        if let Err(error) = self.slots.save_with_native_work(&self.resources.private_state_root,
            &self.relative_directory, &self.state, Survives::PowerLoss, work) {
            self.state.pending.truncate(original_length);
            let removed = self.take_native_catalog_custody(commit.child.task_id,
                crate::service_session::NativeCatalogComponent::Pending);
            drop(removed);
            if let Some(custody) = old_custody {
                self.install_native_catalog_custody(commit.child.task_id,
                    crate::service_session::NativeCatalogComponent::Pending, custody);
            }
            return Err(error);
        }
        drop(old_custody);
        Ok(())
    }

    fn spawn_validate_route(&self, route: &Route, commit: &pi_fork::RetainedSpawnCommit,
        operations: pi_fork::PiForkOperations) -> acyclic_harness::Result<()> {
        let PluginTaskBinding::Pi(binding) = &route.task_binding else { return Err(acyclic_harness::Error::Invalid("Pi original route is host-managed".to_owned())); };
        let key = commit.child.project_volumes.keys().next().ok_or_else(|| acyclic_harness::Error::Invalid("Pi original route lost its selected project".to_owned()))?;
        let paths = pi_native::planned_native_paths(&self.resources, &self.state.root_session_id,
            commit.child.task_id, Some(key))?;
        if binding.task_id != commit.child.task_id || binding.agent_id != commit.child.agent_id
            || binding.native_session_id != commit.child.native_session_id || binding.project_volumes != commit.child.project_volumes
            || binding.scratch.volume != commit.child.scratch.volume
            || binding.results.as_ref().map(|result| &result.volume) != commit.child.results.as_ref().map(|result| &result.volume)
            || route.agent_id != commit.alias || route.turn_id != commit.sdk_operation
            || route.parent_agent_id != self.spawn_parent_alias(commit.caller)? || route.context_id != operations.fork.into_bytes()
            || route.root_id != decode_fixed::<16>(key).map_err(acyclic_harness::Error::Invalid)? || route.mount_path != paths.view_root
        {
            return Err(acyclic_harness::Error::Invalid("Pi duplicate alias does not observe the original admitted child".to_owned()));
        }
        binding.validate(None, route.roots.keys().map(String::as_str), false)
    }

    async fn spawn_validate_admitted(&self, plan: &pi_fork::PiForkPlan, maximum_owned_bytes: u64,
        work: &NativeMaterializationWork) -> acyclic_harness::Result<()> {
        let host = self.runtime.runtime.task_host();
        let (preparation, preparation_custody) = host.observe_admission_with_native_work(plan.preparation.task_id(), work).await?;
        let preparation = preparation;
        let (child, child_custody) = host.observe_admission_with_native_work(plan.request.child.task_id(), work).await?;
        let child = child;
        if preparation != plan.preparation || child != plan.request.child {
            drop(child);
            drop(child_custody);
            drop(preparation);
            drop(preparation_custody);
            return Err(acyclic_harness::Error::Invalid("Pi retained child differs from its original preparation or sealed admission".to_owned()));
        }
        drop(child);
        drop(child_custody);
        drop(preparation);
        drop(preparation_custody);
        let (outcome, outcome_custody) = host.outcome_with_native_work(plan.preparation.task_id(), work).await?;
        let outcome = outcome;
        let Some(Outcome::Succeeded(value)) = outcome else {
            return Err(acyclic_harness::Error::Invalid("Pi retained child has no successful original creator outcome".to_owned()));
        };
        let report: acyclic_harness::fork::ForkReport = serde_json::from_value(value).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
        report.validate()?;
        let (request, request_custody) = pi_root_provider::guarded_pi_value(&report.request, maximum_owned_bytes, work)?;
        let request = request;
        if request != plan.request.payload {
            return Err(acyclic_harness::Error::Invalid("Pi original creator outcome changed its retained resource preparation".to_owned()));
        }
        drop(request);
        drop(request_custody);
        drop(report);
        drop(outcome_custody);
        Ok(())
    }

    fn spawn_save_created(&mut self, commit: &pi_fork::RetainedSpawnCommit, operations: pi_fork::PiForkOperations,
        prepared: &pi_fork::PreparedPiFork, work: &NativeMaterializationWork) -> acyclic_harness::Result<()> {
        let index = self.state.pending.iter().position(|pending| pending.fork_key == operations.fork.into_bytes()).ok_or_else(|| acyclic_harness::Error::Invalid("Pi actual creator has no durably saved original Preparing record".to_owned()))?;
        self.prepare_native_catalog_slot(work)?;
        let pending = &self.state.pending[index];
        self.spawn_validate_pending(pending, commit, operations, work)?;
        let key = commit.child.project_volumes.keys().next().ok_or_else(|| acyclic_harness::Error::Invalid("Pi child has no selected root".to_owned()))?;
        let binding = &prepared.binding;
        if binding.task_id != commit.child.task_id || binding.agent_id != commit.child.agent_id
            || binding.native_session_id != commit.child.native_session_id || binding.project_volumes != commit.child.project_volumes
            || binding.scratch.volume != commit.child.scratch.volume
            || binding.results.as_ref().map(|result| &result.volume) != commit.child.results.as_ref().map(|result| &result.volume)
            || prepared.admission.task_id() != binding.task_id || prepared.admission.parent != Some(commit.caller)
            || prepared.report.request.operation_id != operations.fork
            || prepared.report.request.child_agent != operations.agent
            || prepared.native_lazy.workspace_id != prepared.workspace.workspace().id()
        {
            return Err(acyclic_harness::Error::Invalid("Pi creator substituted its original child binding or workspace".to_owned()));
        }
        prepared.report.validate()?;
        binding.validate(None, [key.as_str()], false)?;
        let route = SpawnBorrowedRoute { task_binding: SpawnBorrowedBinding::Pi(binding), agent_id: &commit.alias,
            turn_id: &commit.sdk_operation, context_id: operations.fork.into_bytes(),
            root_id: pending.active_root_id.ok_or_else(|| acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi original reservation lost its selected root" })?,
            parent_agent_id: &pending.parent_agent_id,
            roots: SpawnBorrowedRoot { key, root: RouteRoot {
                root_id: decode_fixed(key).map_err(acyclic_harness::Error::Invalid)?,
                repository_workspace_id: prepared.workspace.workspace().id().into_bytes(),
                published_generation: prepared.native_lazy.authored_generation.digest().into_bytes() } },
            mount_path: &pending.mount_path, lifecycle: RouteLifecycle::Mounting };
        if self.state.routes.contains_key(&commit.alias) { return Err(acyclic_harness::Error::Invalid("Pi created alias was already registered".to_owned())); }
        let (bytes, bytes_custody) = pi_root_provider::canonical_pi_value_with_native_work(
            &(commit.alias.as_str(), &route), MAXIMUM_ADAPTER_STATE_BYTES, work)?;
        let bytes = bytes;
        let ((route_key, route), mut route_custody): ((String, Route), NativeAllocation) =
            pi_root_provider::guarded_pi_decode(&bytes, MAXIMUM_ADAPTER_STATE_BYTES,
                std::mem::size_of::<(String, Route)>() as u64,
                acyclic_harness::JsonValueSchema::None, SPAWN_ROUTE_DIAGNOSTIC_LABEL_BYTES, work)?;
        let (route_key, route) = (route_key, route);
        drop(bytes);
        drop(bytes_custody);
        let mut slot_custody = work.claim(std::mem::size_of::<(String, Route)>() as u64).map_err(native_error)?;
        if let Err(cause) = route_custody.absorb(&mut slot_custody) {
            drop(route);
            drop(route_key);
            return Err(acyclic_harness::Error::NativeWork { cause, custody: Some(slot_custody) });
        }
        let consumed_custody = self.take_native_catalog_custody(commit.child.task_id,
            crate::service_session::NativeCatalogComponent::Pending);
        let consumed = self.state.pending.remove(index).ok_or_else(|| acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid, reason: "Pi original pending disappeared" })?;
        self.state.routes.insert(route_key, route);
        let old_route_custody = self.install_native_catalog_custody(commit.child.task_id,
            crate::service_session::NativeCatalogComponent::Route, route_custody);
        if let Err(error) = self.slots.save_with_native_work(&self.resources.private_state_root,
            &self.relative_directory, &self.state, Survives::PowerLoss, work) {
            let removed = self.state.routes.remove_entry(&commit.alias);
            drop(removed);
            let removed_custody = self.take_native_catalog_custody(commit.child.task_id,
                crate::service_session::NativeCatalogComponent::Route);
            drop(removed_custody);
            self.state.pending.insert(index, consumed);
            if let Some(custody) = consumed_custody {
                self.install_native_catalog_custody(commit.child.task_id,
                    crate::service_session::NativeCatalogComponent::Pending, custody);
            }
            if let Some(custody) = old_route_custody {
                self.install_native_catalog_custody(commit.child.task_id,
                    crate::service_session::NativeCatalogComponent::Route, custody);
            }
            return Err(error);
        }
        drop(consumed);
        drop(consumed_custody);
        drop(old_route_custody);
        Ok(())
    }

    async fn spawn_observation(&self, commit: &pi_fork::RetainedSpawnCommit, operations: pi_fork::PiForkOperations,
        created: bool, uncertain: bool, work: &NativeMaterializationWork, maximum_owned_bytes: u64,
        original_custody: Option<NativeAllocation>) -> acyclic_harness::Result<(Value, NativeAllocation)> {
        let host = self.runtime.runtime.task_host();
        let preparation = acyclic_harness::runtime::task_id_for_admission(operations.preparation);
        let (preparation_outcome, preparation_custody) = match host.outcome_with_native_work(preparation, work).await {
            Ok((value, custody)) => (value, Some(custody)),
            Err(acyclic_harness::Error::NotFound(_)) if !created => (None, None),
            Err(error) => return Err(error),
        };
        let preparation_outcome = preparation_outcome;
        let (outcome, outcome_custody) = if created {
            let (outcome, custody) = host.outcome_with_native_work(commit.child.task_id, work).await?;
            (outcome, Some(custody))
        } else { (None, None) };
        let outcome = outcome;
        let state = if outcome.as_ref().is_some_and(Outcome::is_terminal) { "completed" }
            else if created { "pending" } else { "preparing" };
        let native_effect = acyclic_harness::effect_host::task_effect_id(commit.child.task_id, operations.native)?;
        let body = SpawnObservation {
            operation: &commit.sdk_operation, state, uncertain,
            child: SpawnObservedChild { id: &commit.alias, task: commit.child.task_id,
                native_session_id: &commit.child.native_session_id, cwd: &commit.launch.invocation.cwd,
                session_file: &commit.launch.invocation.session_file,
                session_directory: commit.launch.invocation.session_file.parent() },
            preparation: SpawnObservedPreparation { task: preparation, operation: operations.preparation,
                outcome: &preparation_outcome },
            fork_operation: operations.fork, native_operation: operations.native, native_effect, outcome: &outcome,
        };
        let (value, mut custody) = pi_root_provider::guarded_pi_value(
            &body, maximum_owned_bytes, work)?;
        drop(body);
        drop(preparation_outcome);
        drop(outcome);
        if let Some(mut preparation_custody) = preparation_custody {
            if let Err(cause) = custody.absorb(&mut preparation_custody) {
                drop(value);
                return Err(native_error(cause));
            }
        }
        if let Some(mut outcome_custody) = outcome_custody {
            if let Err(cause) = custody.absorb(&mut outcome_custody) {
                drop(value);
                return Err(native_error(cause));
            }
        }
        if let Some(mut original_custody) = original_custody {
            if let Err(cause) = custody.absorb(&mut original_custody) {
                drop(value);
                return Err(native_error(cause));
            }
        }
        Ok((value, custody))
    }
}

fn spawn_pending_batch((pending, mut custody): (PendingSpawn, NativeAllocation),
    work: &NativeMaterializationWork) -> acyclic_harness::Result<(Vec<PendingSpawn>, NativeAllocation)> {
    let pending = pending;
    let mut backing = work.claim(std::mem::size_of::<PendingSpawn>() as u64).map_err(native_error)?;
    let mut records = Vec::with_capacity(1);
    records.push(pending);
    if let Err(cause) = custody.absorb(&mut backing) {
        drop(records);
        return Err(acyclic_harness::Error::NativeWork { cause, custody: Some(backing) });
    }
    Ok((records, custody))
}

fn spawn_retain_custody(retained: &mut Option<NativeAllocation>, mut custody: NativeAllocation) -> acyclic_harness::Result<()> {
    if let Some(retained) = retained {
        if let Err(cause) = retained.absorb(&mut custody) {
            return Err(acyclic_harness::Error::NativeWork { cause, custody: Some(custody) });
        }
    }
    else { *retained = Some(custody); }
    Ok(())
}


fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeWork { cause, custody: None }
}

fn same_spawn_commit(left: &pi_fork::RetainedSpawnCommit, right: &pi_fork::RetainedSpawnCommit) -> bool {
    left.sdk_operation == right.sdk_operation && left.alias == right.alias && left.prompt == right.prompt
        && left.caller == right.caller && left.source_task == right.source_task
        && left.child == right.child && left.launch == right.launch
}

#[cfg(test)]
#[path = "pi_spawn_session_tests.rs"]
mod tests;
