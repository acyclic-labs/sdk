//! Real Filesystem creators, original owner leases, and production fork paths.
use super::*;
use crate::{PiScratchBinding, pi_owner::PiOwnerPolicy,
    pi_root::PiRootIntent, pi_root_inputs::{PiRootRequest, PiRootPreparation, ROOT_INPUT_DIRECTORY, ROOT_ALLOCATION_PROVIDER},
    pi_native::{PiNativeInvocation, PiNativeSourceCut, PiNativeSourceMode, PI_NATIVE_VERSION},
    pi_task::PiControlInput};
use acyclic_harness::{IdempotencyKey,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
    runtime::TaskRunLimits, scheduler::SessionLimits};
use acyclic_stream::{LocalStreamLimits, StreamClient};
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;

pub(crate) struct Fixture {
    _directory: tempfile::TempDir,
    pub(crate) owner: PiOwnerConfig,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) filesystem: Arc<Filesystem>,
    pub(crate) stream: StreamClient<LocalStream>,
    pub(crate) resources: ServiceResources,
    pub(crate) source: Arc<LocalLazyWorkspace>,
    physical: Arc<crate::SharedPhysicalRoot>,
    parent: StreamAggregate<LocalStream>,
    pub(crate) context: acyclic_harness::runtime::TaskContext,
    pub(crate) journal: Arc<TaskJournalOwner<LocalStream>>,
    pub(crate) fence: LeaseFence,
    native: PathBuf,
    pub(crate) commit: RetainedSpawnCommit,
    pub(crate) root_request: PiRootRequest,
    pub(crate) root_allocation: crate::pi_root_allocation::PiRootAllocation,
    pub(crate) root_control: WorkflowCommand,
    _root_allocation_custody: acyclic_fs::NativeAllocation,
}

impl Fixture {
    pub(crate) async fn new() -> Self {
        Self::new_with_native_runtime(None).await
    }

    pub(crate) async fn new_with_native_runtime(native_runtime: Option<crate::pi_native::PiNativeHostRuntime>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let native = directory.path().join("native");
        std::fs::create_dir(&native).unwrap();
        std::fs::write(native.join("observed.txt"), b"original").unwrap();
        let native = native.canonicalize().unwrap();
        let (resources, _) = ServiceResources::open(directory.path().join("service")).await.unwrap();
        let session = resources.session_directory("fork-root");
        std::fs::create_dir_all(&session).unwrap();
        let agent = AgentId::from_bytes([161; 16]);
        let task = TaskId::from_bytes([162; 16]);
        let root = [163; 16];
        let provider = acyclic_harness::resources::ProviderRef::new("pi-fork-tests", "filesystem", "1").unwrap();
        let journal_volume = VolumeRef::new(provider.clone(), "journal", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).unwrap();
        let scratch = VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).unwrap();
        let project = VolumeRef::new(provider.clone(), "project", VolumeClass::Project, VolumeOwner::Project(hex::encode(root))).unwrap();
        let grants = Capabilities::new([
            "operation:declare".to_owned(), "operation:observe".to_owned(), "operation:cancel".to_owned(),
            "operation:wake".to_owned(), "task:spawn:acyclic.pi@1".to_owned(),
            "interaction:route".to_owned(), "interaction:open".to_owned(), "interaction:observe".to_owned(),
            "task:spawn:acyclic.fork_preparation@1".to_owned(), "fork:publish".to_owned(), "conversation:bind".to_owned(),
            "effect:plan".to_owned(), "effect:run".to_owned(), format!("effect:provider:{ROOT_ALLOCATION_PROVIDER}"),
            format!("effect:provider:{}", crate::pi_native::PI_NATIVE_PROVIDER),
            journal_volume.directory_read_capability(ROOT_INPUT_DIRECTORY).unwrap(),
            scratch.capability(VolumeOperation::Read).unwrap(), scratch.capability(VolumeOperation::Write).unwrap(),
            project.capability(VolumeOperation::Read).unwrap(), project.capability(VolumeOperation::Write).unwrap(),
        ]);
        let owner = PiOwnerConfig::new_with_native_runtime(PiOwnerPolicy { root_task: task, agent, journal: journal_volume.clone(), grants,
            limits: Limits { file_bytes: 1_048_576, path_bytes: 4096, render_bytes: 1_048_576, ..Limits::default() },
            run_limits: TaskRunLimits { concurrency: Some(8), max_steps: Some(256), deadline_epoch_ms: None },
            session_limits: SessionLimits { active_tasks: 8, total_tasks: 32, depth: 8, model_steps: 256 },
            concurrency: 8, maximum_payload_bytes: 1_048_576 }, native_runtime).unwrap();
        owner.save_new(&session).unwrap();
        let filesystem = Arc::new(Filesystem::new(resources.fs.clone(), provider.clone()).unwrap());
        filesystem.create_volume(&journal_volume).await.unwrap();
        let root_binding = PiTaskBinding { native_session_id: "fork-root".into(), task_id: task, agent_id: agent,
            project_volumes: BTreeMap::from([(hex::encode(root), project)]),
            scratch: PiScratchBinding { volume: scratch, allocation: PiScratchAllocation::Preparing }, results: None };
        let request = PiRootRequest { binding: root_binding.clone(), root: PiRootIntent::Project {
            context_id: [164; 16], root_id: root,
            paths: BTreeMap::from([(hex::encode(root), native.to_str().unwrap().to_owned())]) } };
        let startup_work = acyclic_fs::NativeMaterializationWork::new(budget(), CancellationToken::new()).unwrap();
        let command = crate::pi_root_inputs::stage_root_command(&owner, &filesystem, &request, &startup_work).await.unwrap();
        let stream = StreamClient::new(Arc::new(LocalStream::open(session.join("stream"), LocalStreamLimits::default()).await.unwrap()));
        let runtime = Arc::new(Runtime::open(stream.clone(), filesystem.clone(), journal_volume, owner.authority().unwrap()).await.unwrap());
        runtime.admit_root(root_binding, request.root.clone(), command.0).await.unwrap();
        let worker = Worker { id: "pi-fork-fixture".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new() };
        let first = runtime.runtime.run_operation_with_native_work(&worker, task, &runtime.runtime.commands(), 1, &startup_work).await.unwrap();
        let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = first.0 else { panic!("root allocation lease"); };
        let dispatch_fence = LeaseFence::from(&lease.reservation);
        let dispatch_context = runtime.runtime.task_context_with_native_work(task, &dispatch_fence, &startup_work).await.unwrap();
        let (preparation, journal) = PiRootPreparation::load_for_dispatch(&session, &runtime, &filesystem,
            &request, &dispatch_fence, dispatch_context.clone(), &startup_work).await.unwrap();
        let journal = journal.into_native_shared(&startup_work).unwrap();
        let content = dispatch_context.content_residency_verifier_with_native_work(journal.clone(), &startup_work).unwrap();
        let effect_authorization = owner.effect_authority_with_native_work(&startup_work).unwrap();
        let root_provider = crate::pi_root_provider::root_provider(owner.clone(), resources.clone(), filesystem.clone(),
            stream.clone(), preparation, journal, dispatch_context, startup_work.clone()).unwrap();
        let effects = crate::pi_owner::root_effects_with_native_work(stream.clone(), effect_authorization.0,
            effect_authorization.1, content, root_provider.clone(), &startup_work).unwrap();
        let outcome = runtime.runtime.resume_task_with_native_work(lease, first.1,
            &runtime.runtime.commands().with_effects(effects), 8, &startup_work).await.unwrap();
        assert!(matches!(outcome.0, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { .. })));
        let original = PiRootPreparation::load(&session, &runtime, &filesystem, &request, &startup_work).await.unwrap();
        let observed = crate::pi_root_provider::observe_root_allocation(owner.clone(), resources.clone(),
            filesystem.clone(), stream.clone(), original, &startup_work).await.unwrap().unwrap();
        let root_allocation = observed.0.1;
        let mut handles = root_provider.take_created_handles().await;
        let handle = handles.remove(&hex::encode(root)).unwrap();
        let physical = handle.physical;
        let source = Arc::new(handle.workspace);
        source.read("/observed.txt", 64).await.unwrap();
        let control_operation = OperationId::from_bytes([165; 16]);
        let payload = crate::pi_root_provider::stage_pi_input(&owner, &filesystem, control_operation,
            "timer", &serde_json::json!({"deadline_unix_ms":0}), budget(), &CancellationToken::new()).await.unwrap().value;
        let control = PiControlInput { command: WorkflowCommand { operation_id: control_operation,
            kind: "timer.v1".into(), payload } };
        let root_control = control.command.clone();
        let control = crate::pi_root_provider::stage_pi_input(&owner, &filesystem, control_operation,
            "control", &control, budget(), &CancellationToken::new()).await.unwrap().value;
        let suspension = runtime.runtime.task_host().workflow_suspension(task).await.unwrap().unwrap();
        runtime.runtime.task_host().resume_workflow(task, suspension.revision, control,
            IdempotencyKey::new("pi-fork-test/original-parent-owner").unwrap()).await.unwrap();
        let first = runtime.runtime.run_operation(&worker, task, &runtime.runtime.commands(), 1).await.unwrap().unwrap();
        let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = first else { panic!("original parent control lease"); };
        let fence = LeaseFence::from(&lease.reservation);
        let journal = Arc::new(runtime.runtime.task_host().journal_owner(task, fence.clone()).await.unwrap());
        let context = runtime.runtime.task_context(task, fence.clone()).await.unwrap();
        let authority = owner.authority().unwrap();
        let reader = Arc::new(acyclic_harness::filesystem::FilesystemContentVerifier::new(
            filesystem.clone(), authority.verifier.clone(), authority.signed.clone(), owner.policy().maximum_payload_bytes,
        ).unwrap());
        let writer = Arc::new(acyclic_harness::filesystem::FilesystemContentPublisher::new(filesystem.clone(),
            request.binding.scratch.volume.clone(), &authority.verifier, &authority.signed,
            owner.policy().maximum_payload_bytes).unwrap());
        let context = context.with_content_bindings(acyclic_harness::runtime::ContentBindings {
            reader, writer: Some(writer),
        }).unwrap();
        let original = PiRootPreparation::load(&session, &runtime, &filesystem, &request, &startup_work).await.unwrap();
        let proof = crate::pi_root_provider::prove_root_scratch_allocation_with_native_work(owner.clone(), resources.clone(),
            filesystem.clone(), stream.clone(), original, &startup_work).await.unwrap().unwrap();
        let (authority, issuer, scope) = owner.conversation_authority(task, agent, context.scope().grants().clone()).unwrap();
        crate::pi_conversation::bind_task_conversation_measured(&stream, &authority, &issuer, scope,
            &journal, OperationId::from_bytes(proof.operation().into_bytes()), context.scope().limits(),
            budget(), &CancellationToken::new()).await.unwrap();
        let parent = StreamAggregate::open_measured(&stream, authority, issuer.verifier(), SchemaRegistry::new(),
            budget(), &CancellationToken::new()).await.unwrap().value.with_limits(context.scope().limits()).unwrap();
        let original = original_operation(task, task, "actual-sdk-tool-call-1");
        let operations = PiForkOperations::new(original);
        let child_cwd = directory.path().join("child").join("project");
        let session_file = directory.path().join("child").join("scratch").join("native-session.jsonl");
        let captured_at = "2026-10-10T12:00:00.000Z";
        let child_session = "sdk-new-native-session";
        let source_cut = PiNativeSourceCut { mode: PiNativeSourceMode::Scratch,
            parent_session_id: "fork-root".into(), parent_session_file: None,
            parent_cwd: native.to_str().unwrap().into(), source_leaf_id: None, owner_entry_id: None,
            parent_leaf_id: None, captured_at: captured_at.into(), entries_json: "[]".into(),
            sha256: hex::encode(Sha256::digest(b"[]")) };
        let session_bytes = format!("{}\n", serde_json::json!({"type":"session","version":3,
            "id":child_session,"timestamp":captured_at,"cwd":child_cwd}));
        let commit = RetainedSpawnCommit { sdk_operation: "actual-sdk-tool-call-1".into(), alias: "new-child".into(),
            prompt: "continue the exact original task".into(), caller: task, source_task: task,
            child: PiTaskBinding { native_session_id: child_session.into(), task_id: task_id_for_admission(operations.child),
                agent_id: operations.agent, project_volumes: BTreeMap::from([(hex::encode(root),
                    VolumeRef::new(provider.clone(), "child-project", VolumeClass::Project, VolumeOwner::Project("child-project".into())).unwrap())]),
                scratch: PiScratchBinding { volume: VolumeRef::new(provider.clone(), "child-scratch", VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(operations.agent)).unwrap(), allocation: PiScratchAllocation::Preparing },
                results: Some(PiScratchBinding { volume: VolumeRef::new(provider, "native-results", VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(operations.agent)).unwrap(), allocation: PiScratchAllocation::Preparing }) },
            launch: PiNativeLaunchIntent { child_session_id: child_session.into(), source: source_cut,
                agent_alias: "new-child".into(),
                invocation: PiNativeInvocation { version: PI_NATIVE_VERSION.into(), executable: std::env::current_exe().unwrap(),
                    executable_prefix: vec![], extension: directory.path().join("extension.ts"), provider_extension: None,
                    session_file, cwd: child_cwd, provider: "actual-provider".into(), model: "actual-model".into(),
                    prompt: "continue the exact original task".into(), builtin_tools: vec!["read".into(), "write".into(), "edit".into()],
                    environment: BTreeMap::from([("PATH".into(), std::env::var("PATH").unwrap()),
                        ("ACYCLIC_PI_CALLER".into(), "never-publish-this-proof".into())]),
                    timeout_ms: 1000, control_timeout_ms: 1000, cancellation_poll_ms: 10,
                    maximum_output_bytes: 65536, maximum_result_bytes: 65536 },
                session_path: "native-session.jsonl".into(), session_bytes,
                maximum_entries: 128, maximum_bytes: 1_048_576 } };
        Self { _directory: directory, owner, runtime, filesystem, stream, resources, source,
            parent, context, journal, fence, native, commit, physical, root_request: request, root_allocation, root_control,
            _root_allocation_custody: observed.1 }
    }

    pub(crate) fn coordinator(&self) -> PiForkCoordinator {
        PiForkCoordinator::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            self.resources.clone(), self.owner.clone()).unwrap()
    }
    pub(crate) async fn plan(&self) -> PiForkPlan {
        let (_, issuer, _) = self.owner.conversation_authority(self.journal.task_id(),
            self.owner.policy().agent, self.context.scope().grants().clone()).unwrap();
        let parent = StreamAggregate::open_measured(&self.stream, self.parent.reducer().authority().clone(),
            issuer.verifier(), SchemaRegistry::new(), budget(), &CancellationToken::new()).await.unwrap().value
            .with_limits(self.context.scope().limits()).unwrap();
        let frozen = self.source.snapshot_measured(budget(), &CancellationToken::new()).await.unwrap().value;
        self.coordinator().begin(&self.context, &self.journal, self.fence.clone(), parent, self.source.clone(), frozen,
            self.commit.clone(), ForkPreparationLimits { resources: 128, work: 1_000_000, bytes: 1_048_576 },
            budget(), CancellationToken::new()).await.unwrap()
    }
    fn next_commit(&self, caller: &PiTaskBinding, source: TaskId, sdk_operation: &str) -> RetainedSpawnCommit {
        let mut commit = self.commit.clone();
        let operations = PiForkOperations::new(original_operation(self.owner.policy().root_task,
            caller.task_id, sdk_operation));
        let provider = commit.child.scratch.volume.provider().clone();
        let directory = self._directory.path().join(sdk_operation);
        commit.sdk_operation = sdk_operation.into();
        commit.alias = sdk_operation.into();
        commit.caller = caller.task_id;
        commit.source_task = source;
        commit.child.task_id = task_id_for_admission(operations.child);
        commit.child.agent_id = operations.agent;
        commit.child.native_session_id = format!("sdk-{sdk_operation}");
        commit.launch.agent_alias = commit.alias.clone();
        commit.child.project_volumes = caller.project_volumes.keys().map(|root| (root.clone(),
            VolumeRef::new(provider.clone(), format!("{sdk_operation}-project"), VolumeClass::Project,
                VolumeOwner::Project(sdk_operation.into())).unwrap())).collect();
        commit.child.scratch = PiScratchBinding { volume: VolumeRef::new(provider.clone(),
            format!("{sdk_operation}-scratch"), VolumeClass::AgentPrivate, VolumeOwner::Agent(operations.agent)).unwrap(),
            allocation: PiScratchAllocation::Preparing };
        commit.child.results = Some(PiScratchBinding { volume: VolumeRef::new(provider,
            format!("{sdk_operation}-results"), VolumeClass::AgentPrivate, VolumeOwner::Agent(operations.agent)).unwrap(),
            allocation: PiScratchAllocation::Preparing });
        commit.launch.child_session_id = commit.child.native_session_id.clone();
        commit.launch.source.parent_session_id = caller.native_session_id.clone();
        commit.launch.invocation.cwd = directory.join("project");
        commit.launch.invocation.session_file = directory.join("scratch").join("native-session.jsonl");
        commit.launch.session_bytes = format!("{}\n", serde_json::json!({"type":"session","version":3,
            "id":commit.child.native_session_id,"timestamp":commit.launch.source.captured_at,
            "cwd":commit.launch.invocation.cwd}));
        commit
    }

    async fn selected_plan(&self, context: &acyclic_harness::runtime::TaskContext,
        journal: &Arc<TaskJournalOwner<LocalStream>>, fence: LeaseFence, caller: &PiTaskBinding,
        source: Arc<LocalLazyWorkspace>, commit: RetainedSpawnCommit) -> Result<PiForkPlan> {
        let (authority, issuer, _) = self.owner.conversation_authority(caller.task_id,
            caller.agent_id, context.scope().grants().clone())?;
        let parent = StreamAggregate::open_measured(&self.stream, authority, issuer.verifier(),
            SchemaRegistry::new(), budget(), &CancellationToken::new()).await
            .map_err(|failure| Error::Invalid(failure.error.to_string()))?.value
            .with_limits(context.scope().limits())?;
        let frozen = source.snapshot_measured(budget(), &CancellationToken::new()).await
            .map_err(|failure| Error::Invalid(failure.error.to_string()))?.value;
        self.coordinator().begin(context, journal, fence, parent, source, frozen, commit,
            ForkPreparationLimits { resources: 128, work: 1_000_000, bytes: 1_048_576 },
            budget(), CancellationToken::new()).await
    }
}

fn budget() -> WorkBudget { crate::pi_root_provider::allocation_budget() }

#[test]
fn original_sdk_operations_are_distinct_stable_and_caller_bound() {
    let root = TaskId::from_bytes([1; 16]);
    let caller = TaskId::from_bytes([2; 16]);
    let operation = original_operation(root, caller, "sdk-original-tool-call");
    assert_eq!(operation, original_operation(root, caller, "sdk-original-tool-call"));
    assert_ne!(operation, original_operation(root, root, "sdk-original-tool-call"));
    let operations = PiForkOperations::new(operation);
    let ids = [operation, operations.child, operations.preparation, operations.fork, operations.native];
    for (index, id) in ids.iter().enumerate() { assert!(!ids[..index].contains(id)); }
}

#[tokio::test]
async fn actual_fork_creation_admission_publication_and_lost_reply_reopen_preserve_one_original_cut() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    assert!(!plan.commit.launch.invocation.environment.keys().any(|key| reserved_native_key(key)));
    let coordinator = fixture.coordinator();
    let created = coordinator.advance(&fixture.context, &fixture.journal, fixture.fence.clone(), &plan,
        fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(created.workspace.workspace().name().as_str(), plan.commit.child.project_volumes.values().next().unwrap().storage_name().unwrap());
    assert_eq!(created.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    assert!(matches!(created.binding.scratch.allocation, PiScratchAllocation::Created(_)));
    assert!(!created.admission.grants.contains(&plan.source.project.capability(VolumeOperation::Write).unwrap()));
    assert!(!created.admission.grants.contains(&plan.source.private_volume.capability(VolumeOperation::Read).unwrap()));
    let child_workspace = created.workspace.workspace().id();
    let child_generation = created.native_lazy;
    drop(created); // All original successful replies may be lost by the caller.
    drop(coordinator);
    let reopened = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(), &plan,
        fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(reopened.workspace.workspace().id(), child_workspace);
    assert_eq!(reopened.native_lazy, child_generation);
    assert_eq!(std::fs::read(fixture.native.join("observed.txt")).unwrap(), b"original");
}

#[tokio::test]
async fn original_preparation_task_partial_checkpoint_reopens_without_early_child_admission() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    let host = fixture.runtime.runtime.task_host();
    host.admit_owned(plan.preparation.clone(), fixture.fence.clone()).await.unwrap();
    let partial = fixture.runtime.runtime.run_operation(&Worker { id: "fork-partial-original".into(),
        available: ResourceSnapshot::default(), labels: BTreeMap::new() }, plan.preparation.task_id(),
        &fixture.runtime.runtime.commands(), 1).await.unwrap().unwrap();
    assert!(matches!(partial, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })));
    assert!(host.observe_admission(plan.request.child.task_id()).await.is_err());
    assert!(fixture.resources.fs.open_workspace(plan.commit.child.scratch.volume.storage_name().unwrap()).await.is_err());
    let restored = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(), &plan,
        fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(restored.admission, plan.request.child);
}

#[tokio::test]
async fn cancellation_and_owner_replacement_never_admit_a_child() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(), &plan,
        fixture.source.clone(), budget(), cancel).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(plan.request.child.task_id()).await.is_err());
    let replacement = PiOwnerConfig::new(fixture.owner.policy().clone()).unwrap();
    assert!(PiForkCoordinator::new(fixture.runtime.clone(), fixture.filesystem.clone(), fixture.stream.clone(),
        fixture.resources.clone(), replacement).is_err());
    assert!(fixture.resources.fs.open_workspace(plan.commit.child.scratch.volume.storage_name().unwrap()).await.is_err());
}

#[tokio::test]
async fn advancing_actual_parent_authored_head_cannot_replace_retained_fork_input() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    fixture.source.workspace().write("/authored-later.txt", bytes::Bytes::from_static(b"later parent generation")).await.unwrap();
    let current = fixture.source.snapshot_measured(budget(), &CancellationToken::new()).await.unwrap().value;
    assert_ne!(current.authored_generation, plan.frozen.authored_generation);
    let created = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(), &plan,
        fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert!(created.workspace.read("/authored-later.txt", 64).await.is_err());
    assert_eq!(created.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    assert_eq!(created.report.request, serde_json::from_value::<ForkRequest>(plan.request.payload).unwrap());
}

#[tokio::test]
async fn changed_native_sdk_cut_session_bytes_and_child_identity_never_enter_preparation() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    for field in 0..3 {
        let mut changed = plan.clone();
        match field {
            0 => changed.commit.launch.source.entries_json = "[{\"id\":\"foreign\"}]".into(),
            1 => changed.commit.launch.session_bytes.push_str("{}\n"),
            _ => changed.commit.child.native_session_id = "another-native-sdk-session".into(),
        }
        assert!(fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
            &changed, fixture.source.clone(), budget(), CancellationToken::new()).await.is_err());
        assert!(fixture.runtime.runtime.task_host().observe_admission(plan.preparation.task_id()).await.is_err());
        assert!(fixture.resources.fs.open_workspace(plan.commit.child.scratch.volume.storage_name().unwrap()).await.is_err());
    }
}

#[tokio::test]
async fn lost_prepare_reply_reloads_original_input_instead_of_recapturing_caller_heads() {
    let fixture = Fixture::new().await;
    let original = fixture.plan().await;
    fixture.source.workspace().write("/after-prepare.txt", bytes::Bytes::from_static(b"new mutable source")).await.unwrap();
    let retried = fixture.plan().await;
    assert_eq!(retried.frozen, original.frozen);
    assert_eq!(retried.project_creation, original.project_creation);
    assert_eq!(retried.source, original.source);
    assert_eq!(retried.request, original.request);
    assert_eq!(retried.preparation, original.preparation);
    assert!(fixture.runtime.runtime.task_host().observe_admission(original.preparation.task_id()).await.is_err());
}

#[tokio::test]
async fn explicit_root_sibling_and_descendant_sources_use_operation_read_proofs_not_caller_grants() {
    let fixture = Fixture::new().await;
    let root_grants = fixture.context.scope().grants().clone();
    let first_plan = fixture.plan().await;
    let first = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &first_plan, fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    let root: PiTaskInput = serde_json::from_value(fixture.runtime.runtime.task_host()
        .observe_admission(fixture.journal.task_id()).await.unwrap().input).unwrap();
    let sibling_commit = fixture.next_commit(&root.binding, root.binding.task_id, "sibling-call");
    let sibling_plan = fixture.selected_plan(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &root.binding, fixture.source.clone(), sibling_commit).await.unwrap();
    let sibling = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &sibling_plan, fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();

    let worker = Worker { id: "actual-child-caller".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let attempt = fixture.runtime.runtime.run_operation(&worker, first.binding.task_id,
        &fixture.runtime.runtime.commands(), 1).await.unwrap().unwrap();
    let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = attempt else { panic!("original child native command owner"); };
    let fence = LeaseFence::from(&lease.reservation);
    let journal = Arc::new(fixture.runtime.runtime.task_host().journal_owner(first.binding.task_id, fence.clone()).await.unwrap());
    let context = fixture.runtime.runtime.task_context(first.binding.task_id, fence.clone()).await.unwrap();
    let original_grants = context.scope().grants().clone();
    let source_read = root.binding.project_volumes.values().next().unwrap().capability(VolumeOperation::Read).unwrap();
    assert!(!original_grants.contains(&source_read));

    let mut commit = fixture.next_commit(&first.binding, root.binding.task_id, "selected-root-call");
    commit.launch.source.parent_cwd = first_plan.commit.launch.invocation.cwd.to_str().unwrap().into();
    let root_plan = fixture.selected_plan(&context, &journal, fence.clone(), &first.binding,
        fixture.source.clone(), commit).await.unwrap();
    let fork: ForkRequest = serde_json::from_value(root_plan.request.payload.clone()).unwrap();
    assert_eq!(fork.authorized_source.as_ref().unwrap().source_task, root.binding.task_id);
    assert_eq!(fork.preparation.inherited_through_sequence, 0);
    assert_eq!(root_plan.source.private_volume, first.binding.scratch.volume);
    assert_eq!(context.scope().grants(), &original_grants);
    let descendant = fixture.coordinator().advance(&context, &journal, fence.clone(), &root_plan,
        fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert!(!descendant.admission.grants.contains(&source_read));
    assert!(matches!(descendant.binding.results.as_ref().unwrap().allocation, PiScratchAllocation::Created(_)));
    assert_eq!(descendant.report.child_result_volume, descendant.binding.results.as_ref().map(|binding| binding.volume.clone()));

    let mut commit = fixture.next_commit(&first.binding, sibling.binding.task_id, "selected-sibling-call");
    commit.launch.source.parent_cwd = first_plan.commit.launch.invocation.cwd.to_str().unwrap().into();
    let sibling_source = Arc::new(sibling.workspace);
    let plan = fixture.selected_plan(&context, &journal, fence.clone(), &first.binding,
        sibling_source.clone(), commit).await.unwrap();
    let fork: ForkRequest = serde_json::from_value(plan.request.payload.clone()).unwrap();
    assert_eq!(fork.authorized_source.as_ref().unwrap().source_task, sibling.binding.task_id);
    assert_eq!(context.scope().grants(), &original_grants);
    let actual = fixture.coordinator().advance(&context, &journal, fence, &plan, sibling_source,
        budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(actual.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    assert!(!actual.admission.grants.contains(&sibling.binding.project_volumes.values().next().unwrap()
        .capability(VolumeOperation::Read).unwrap()));

    let source = Arc::new(descendant.workspace);
    let commit = fixture.next_commit(&root.binding, descendant.binding.task_id, "selected-descendant-call");
    let plan = fixture.selected_plan(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &root.binding, source.clone(), commit).await.unwrap();
    let actual = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &plan, source, budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(actual.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    assert_eq!(fixture.context.scope().grants(), &root_grants);
}

#[tokio::test]
async fn actual_stream_and_task_registry_restart_reconciles_original_creator_proofs_and_publication() {
    let fixture = Fixture::new().await;
    let observation_work = acyclic_fs::NativeMaterializationWork::new(budget(), CancellationToken::new()).unwrap();
    let plan = fixture.plan().await;
    let first = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &plan, fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    let stream = StreamClient::new(Arc::new(LocalStream::open(fixture.resources.session_directory("fork-root").join("stream"),
        LocalStreamLimits::default()).await.unwrap()));
    let runtime = Arc::new(Runtime::open(stream.clone(), fixture.filesystem.clone(),
        fixture.owner.policy().journal.clone(), fixture.owner.authority().unwrap()).await.unwrap());
    let journal = Arc::new(runtime.runtime.task_host().journal_owner(fixture.journal.task_id(), fixture.fence.clone()).await.unwrap());
    let context = runtime.runtime.task_context(journal.task_id(), fixture.fence.clone()).await.unwrap();
    let root: PiTaskInput = serde_json::from_value(runtime.runtime.task_host().observe_admission(journal.task_id()).await.unwrap().input).unwrap();
    let authority = fixture.owner.authority().unwrap();
    let reader = Arc::new(acyclic_harness::filesystem::FilesystemContentVerifier::new(fixture.filesystem.clone(),
        authority.verifier.clone(), authority.signed.clone(), fixture.owner.policy().maximum_payload_bytes).unwrap());
    let writer = Arc::new(acyclic_harness::filesystem::FilesystemContentPublisher::new(fixture.filesystem.clone(),
        root.binding.scratch.volume, &authority.verifier, &authority.signed, fixture.owner.policy().maximum_payload_bytes).unwrap());
    let context = context.with_content_bindings(acyclic_harness::runtime::ContentBindings { reader, writer: Some(writer) }).unwrap();
    let preparation = PiRootPreparation::load(&fixture.resources.session_directory("fork-root"), &runtime,
        &fixture.filesystem, &fixture.root_request, &observation_work).await.unwrap();
    let proof = crate::pi_root_provider::prove_root_scratch_allocation_with_native_work(fixture.owner.clone(), fixture.resources.clone(),
        fixture.filesystem.clone(), stream.clone(), preparation, &observation_work).await.unwrap().unwrap();
    let (authority, issuer, scope) = fixture.owner.conversation_authority(journal.task_id(), root.binding.agent_id,
        context.scope().grants().clone()).unwrap();
    crate::pi_conversation::bind_task_conversation_measured(&stream, &authority, &issuer, scope,
        &journal, OperationId::from_bytes(proof.operation().into_bytes()), context.scope().limits(),
        budget(), &CancellationToken::new()).await.unwrap();
    let coordinator = PiForkCoordinator::new(runtime, fixture.filesystem.clone(), stream, fixture.resources.clone(),
        fixture.owner.clone()).unwrap();
    let original = coordinator.load_plan(&context, plan.original, budget(), &CancellationToken::new()).await.unwrap().unwrap();
    let recovered = coordinator.advance(&context, &journal, fixture.fence.clone(), &original, fixture.source.clone(),
        budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(first.report, recovered.report);
    assert_eq!(first.admission, recovered.admission);
    assert_eq!(first.binding, recovered.binding);
    assert_eq!(first.native_lazy, recovered.native_lazy);
    assert_eq!(first.workspace.workspace().id(), recovered.workspace.workspace().id());
}

#[tokio::test]
async fn cold_pre_first_dispatch_restores_only_under_actual_child_owner_and_existing_publication() {
    let fixture = Fixture::new().await;
    let plan = fixture.plan().await;
    let first = fixture.coordinator().advance(&fixture.context, &fixture.journal, fixture.fence.clone(), &plan,
        fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap().unwrap();
    let worker = Worker { id: "cold-native-child-owner".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let attempt = fixture.runtime.runtime.run_operation(&worker, first.binding.task_id,
        &fixture.runtime.runtime.commands(), 1).await.unwrap().unwrap();
    let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = attempt else { panic!("actual child owner"); };
    let fence = LeaseFence::from(&lease.reservation);
    let journal = Arc::new(fixture.runtime.runtime.task_host().journal_owner(first.binding.task_id, fence.clone()).await.unwrap());
    let context = fixture.runtime.runtime.task_context(first.binding.task_id, fence.clone()).await.unwrap();
    let original = fixture.coordinator().load_plan(&context, plan.original, budget(), &CancellationToken::new()).await.unwrap().unwrap();
    let restored = fixture.coordinator().restore_admitted(&context, &journal, fence.clone(), &original,
        fixture.source.clone(), budget(), CancellationToken::new()).await.unwrap();
    assert_eq!(restored.binding, first.binding);
    assert_eq!(restored.report, first.report);
    assert_eq!(restored.admission, first.admission);
    assert_eq!(restored.native_lazy, first.native_lazy);
    let mut changed = original;
    changed.commit.source_task = first.binding.task_id;
    assert!(fixture.coordinator().restore_admitted(&context, &journal, fence, &changed, fixture.source.clone(),
        budget(), CancellationToken::new()).await.is_err());
}

#[tokio::test]
async fn root_freeze_uses_actual_original_callback_context_and_watcher_epoch() {
    let fixture = Fixture::new().await;
    let frozen = crate::pi_lazy_capture::freeze_root_source(fixture.journal.task_id(), &fixture.context,
        &fixture.journal, &fixture.physical, &fixture.source, budget(), &CancellationToken::new()).await.unwrap();
    assert_eq!(frozen.value.workspace_id, fixture.source.workspace().id());
    fixture.source.validate_snapshot_measured(frozen.value, budget(), &CancellationToken::new()).await.unwrap();
    assert!(crate::pi_lazy_capture::freeze_root_source(TaskId::from_bytes([9; 16]), &fixture.context,
        &fixture.journal, &fixture.physical, &fixture.source, budget(), &CancellationToken::new()).await.is_err());
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(crate::pi_lazy_capture::freeze_root_source(fixture.journal.task_id(), &fixture.context,
        &fixture.journal, &fixture.physical, &fixture.source, budget(), &cancelled).await.is_err());
}

#[tokio::test]
async fn explicit_source_without_original_session_task_proof_cannot_prepare_or_allocate() {
    let fixture = Fixture::new().await;
    let root: PiTaskInput = serde_json::from_value(fixture.runtime.runtime.task_host()
        .observe_admission(fixture.journal.task_id()).await.unwrap().input).unwrap();
    let commit = fixture.next_commit(&root.binding, TaskId::from_bytes([230; 16]), "foreign-root-escape");
    let operations = PiForkOperations::new(original_operation(root.binding.task_id, root.binding.task_id, &commit.sdk_operation));
    let future_project = commit.child.project_volumes.values().next().unwrap().clone();
    assert!(fixture.selected_plan(&fixture.context, &fixture.journal, fixture.fence.clone(),
        &root.binding, fixture.source.clone(), commit).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(task_id_for_admission(operations.preparation)).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(task_id_for_admission(operations.child)).await.is_err());
    assert!(fixture.resources.fs.open_workspace(future_project.storage_name().unwrap()).await.is_err());
}
