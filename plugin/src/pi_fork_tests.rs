use super::*;
use crate::{PiScratchBinding, pi_owner::PiOwnerPolicy,
    pi_root::PiRootIntent, pi_root_inputs::{PiRootRequest, PiRootPreparation, ROOT_INPUT_DIRECTORY, ROOT_ALLOCATION_PROVIDER},
    pi_native::{PiNativeInvocation, PiNativeSourceCut, PiNativeSourceMode, PI_NATIVE_VERSION},
    pi_task::PiControlInput};
use acyclic_harness::{IdempotencyKey,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
    runtime::TaskRunLimits, scheduler::SessionLimits,
    workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine}};
use acyclic_stream::{LocalStreamLimits};
use sha2::{Digest as _, Sha256};
use std::path::PathBuf;

pub(crate) struct Passive { pub(crate) identity: MachineIdentity, pub(crate) schema: serde_json::Value }
impl ResumableMachine for Passive {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &serde_json::Value { &self.schema }
    fn initialize(&self, input: &serde_json::Value) -> Result<serde_json::Value> { Ok(input.clone()) }
    fn transition(&self, state: &serde_json::Value, _: &serde_json::Value) -> Result<MachineTransition> {
        Ok(MachineTransition { state: state.clone(), commands: vec![], status: MachineStatus::Suspended })
    }
}

pub(crate) struct Fixture {
    pub(crate) owner: Arc<PiOwnerConfig>,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) filesystem: Arc<Filesystem>,
    pub(crate) stream: std::sync::Arc<LocalStream>,
    pub(crate) resources: ServiceResources,
    pub(crate) source: Arc<LocalLazyWorkspace>,
    root_handle: crate::pi_root_provider::PiRootHandle,
    pub(crate) context: acyclic_harness::runtime::TaskContext,
    pub(crate) journal: Arc<TaskJournalOwner<LocalStream>>,
    pub(crate) fence: LeaseFence,
    native: PathBuf,
    pub(crate) commit: RetainedSpawnCommit,
    pub(crate) root_request: PiRootRequest,
    pub(crate) root_allocation: crate::pi_root_allocation::PiRootAllocation,
    pub(crate) private_root: acyclic_fs::NativeHostPrivateStateRoot,
    pub(crate) relative_session: PathBuf,
    pub(crate) root_control: WorkflowCommand,
    _root_allocation_custody: acyclic_fs::NativeAllocation,
    _service_lock: crate::ServiceLock,
    _directory: tempfile::TempDir,
}

impl Fixture {
    pub(crate) async fn new(native_runtime: Option<crate::pi_native::PiNativeHostRuntime>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let native = directory.path().join("native");
        std::fs::create_dir(&native).unwrap();
        std::fs::write(native.join("observed.txt"), b"original").unwrap();
        let native = native.canonicalize().unwrap();
        let data = directory.path().join("service");
        let (service_lock, configured, private_root, startup_work) = crate::tests::configured_test_service_bootstrap(&data).await.unwrap();
        let (resources, sessions) = ServiceResources::open(data, configured, private_root.clone(), &startup_work).await.unwrap();
        assert!(sessions.is_empty());
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
        let owner = PiOwnerConfig::new(&PiOwnerPolicy { root_task: task, agent, journal: journal_volume.clone(), grants,
            limits: Limits { file_bytes: 1_048_576, path_bytes: 4096, render_bytes: 1_048_576, ..Limits::default() },
            run_limits: TaskRunLimits { concurrency: Some(8), max_steps: Some(256), deadline_epoch_ms: None },
            session_limits: SessionLimits { active_tasks: 8, total_tasks: 32, depth: 8, model_steps: 256 },
            concurrency: 8, maximum_payload_bytes: 1_048_576 }, native_runtime.as_ref(), &startup_work).unwrap();
        owner.save_new(&session).unwrap();
        let filesystem = Arc::new(Filesystem::new(resources.fs.clone(), provider.clone()).unwrap());
        filesystem.create_volume(&journal_volume).await.unwrap();
        let root_binding = PiTaskBinding { native_session_id: "fork-root".into(), task_id: task, agent_id: agent,
            project_volumes: BTreeMap::from([(hex::encode(root), project)]),
            scratch: PiScratchBinding { volume: scratch, allocation: PiScratchAllocation::Preparing }, results: None };
        let request = PiRootRequest { binding: root_binding.clone(), root: PiRootIntent::Project {
            context_id: [164; 16], root_id: root,
            paths: BTreeMap::from([(hex::encode(root), native.to_str().unwrap().to_owned())]) } };
        let relative_session = session.strip_prefix(&resources.data).unwrap().to_path_buf();
        drop(owner);
        let owner = PiOwnerConfig::load_with_native_work(&private_root, &relative_session, &startup_work).unwrap()
            .into_native_shared(&startup_work).unwrap();
        let command = crate::pi_root_inputs::stage_root_command(&owner, &filesystem, &request, &startup_work).await.unwrap();
        let stream = Arc::new(LocalStream::open(session.join("stream"), LocalStreamLimits::default()).await.unwrap());
        let runtime = Arc::new(Runtime::open_with_native_work(stream.clone(), filesystem.clone(), &journal_volume,
            owner.authority_with_native_work(&startup_work).unwrap(), &startup_work).await.unwrap());
        let _root_admission = runtime.admit_root_with_native_work(root_binding, request.root.clone(), command.0, &startup_work).await.unwrap();
        let worker = Worker { id: "pi-fork-fixture".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new() };
        let first = runtime.runtime.run_operation_with_native_work(&worker, task, &runtime.runtime.commands(), 1, &startup_work).await.unwrap();
        let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = first.0 else { panic!("root allocation lease"); };
        let dispatch_fence = LeaseFence::from(&lease.reservation);
        let dispatch_context = runtime.runtime.task_context_with_native_work(task, &dispatch_fence, &startup_work).await.unwrap();
        let (preparation, journal) = PiRootPreparation::load_for_dispatch(&private_root, &relative_session, &runtime, &filesystem,
            &request, &dispatch_fence, dispatch_context.clone(), &startup_work).await.unwrap();
        let journal = journal.into_native_shared(&startup_work).unwrap();
        let content = dispatch_context.content_residency_verifier_with_native_work(journal.clone(), &startup_work).unwrap();
        let effect_authorization = owner.effect_authority_with_native_work(&startup_work).unwrap();
        let root_provider = crate::pi_root_provider::root_provider(owner.clone(), &resources, filesystem.clone(),
            stream.clone(), preparation, Some(journal), dispatch_context, startup_work.clone()).unwrap();
        let effects = crate::pi_owner::root_effects_with_native_work(stream.clone(), effect_authorization.0,
            effect_authorization.1, content, root_provider.clone(), &startup_work).unwrap();
        let outcome = runtime.runtime.resume_task_with_native_work(lease, first.1,
            &runtime.runtime.commands().with_effects(effects), 8, &startup_work).await.unwrap();
        assert!(matches!(outcome.0, TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { .. })));
        let original = PiRootPreparation::load(&private_root, &relative_session, &runtime, &filesystem, &request, &startup_work).await.unwrap();
        let mut observed = crate::pi_root_provider::observe_root_allocation(owner.clone(), &resources,
            filesystem.clone(), stream.clone(), original, &startup_work).await.unwrap().unwrap();
        let root_allocation = observed.0.1;
        let mut handles = root_provider.take_created_handles().await;
        let handle = handles.remove(&hex::encode(root)).unwrap();
        let source = Arc::clone(&handle.workspace);
        source.read("/observed.txt", 64).await.unwrap();
        let control_operation = OperationId::from_bytes([165; 16]);
        let mut payload = crate::pi_root_provider::stage_pi_input(&owner, &filesystem, control_operation,
            "timer", &serde_json::json!({"deadline_unix_ms":0}), &startup_work).await.unwrap();
        let control = PiControlInput { command: WorkflowCommand { operation_id: control_operation,
            kind: "timer.v1".into(), payload: payload.0 } };
        observed.1.absorb(&mut payload.1).unwrap();
        let root_control = control.command.clone();
        let mut control = crate::pi_root_provider::stage_pi_input(&owner, &filesystem, control_operation,
            "control", &control, &startup_work).await.unwrap();
        observed.1.absorb(&mut control.1).unwrap();
        let suspension = runtime.runtime.task_host().workflow_suspension(task).await.unwrap().unwrap();
        runtime.runtime.task_host().resume_workflow(task, suspension.revision, control.0,
            IdempotencyKey::new("pi-fork-test/original-parent-owner").unwrap()).await.unwrap();
        let mut first = runtime.runtime.run_operation_with_native_work(&worker, task, &runtime.runtime.commands(), 1, &startup_work).await.unwrap();
        observed.1.absorb(&mut first.1).unwrap();
        let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = first.0 else { panic!("original parent control lease"); };
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
        let original = PiRootPreparation::load(&private_root, &relative_session, &runtime, &filesystem, &request, &startup_work).await.unwrap();
        let proof = crate::pi_root_provider::prove_root_scratch_allocation_with_native_work(owner.clone(), &resources,
            filesystem.clone(), stream.clone(), original, &startup_work).await.unwrap().unwrap();
        let native_authority = owner.conversation_authority_with_native_work(task, agent, context.scope().grants(), &startup_work).unwrap();
        crate::pi_conversation::bind_task_conversation(&stream, native_authority.0,
            native_authority.1, &journal, &request.binding.scratch.volume, proof.operation(), context.scope().limits(),
            &startup_work).await.unwrap();
        let original = original_operation(task, task, "actual-sdk-tool-call-1", &startup_work).unwrap();
        let operations = PiForkOperations::new(original, &startup_work).unwrap();
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
            context, journal, fence, native, commit, root_handle: handle, root_request: request, root_allocation, root_control,
            private_root, relative_session,
            _root_allocation_custody: observed.1, _service_lock: service_lock }
    }

    pub(crate) fn coordinator(&self) -> PiForkCoordinator {
        let work = acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
        PiForkCoordinator::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, self.owner.clone(), &work).unwrap()
    }
    pub(crate) async fn plan(&self, work: &acyclic_fs::NativeMaterializationWork) -> PiForkPlan {
        let coordinator = PiForkCoordinator::new(self.runtime.clone(), self.filesystem.clone(),
            self.stream.clone(), &self.resources, self.owner.clone(), work).unwrap();
        let context = self.runtime.runtime.task_context_with_native_work(
            self.journal.task_id(), &self.fence, work).await.unwrap();
        let context = crate::pi_root_provider::bind_root_output_context_with_native_work(
            context, &self.journal, self.owner.clone(), &self.resources, self.filesystem.clone(),
            self.stream.clone(), &self.root_request, work).await.unwrap();
        let original = original_operation(self.owner.policy().root_task,
            self.journal.task_id(), &self.commit.sdk_operation, work).unwrap();
        if let Some(plan) = coordinator.load_plan(context.context(), original, work.remaining().unwrap(),
            work.cancellation()).await.unwrap() {
            return plan;
        }
        let admission = self.runtime.runtime.task_host().observe_admission_with_native_work(self.journal.task_id(), work).await.unwrap();
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(&admission.0.input, maximum, work).unwrap();
        let input = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(&bytes.0, maximum, maximum,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES, work).unwrap();
        let key = input.0.binding.project_volumes.keys().next().unwrap();
        let frozen = crate::pi_fork_host::freeze_created_root_source(self.owner.clone(), &self.resources,
            self.filesystem.clone(), self.stream.clone(), &context, &self.journal,
            &input.0, key, &self.root_handle, work).await.unwrap();
        let authority = self.owner.conversation_authority_with_native_work(self.journal.task_id(),
            self.owner.policy().agent, context.context().scope().grants(), work).unwrap();
        let verifier = authority.0.1.verifier_with_native_work(work).unwrap();
        let parent = StreamAggregate::open_with_native_work(&self.stream, authority.0.0, verifier.0,
            SchemaRegistry::new(), work).await.unwrap();
        let parent_value = parent.0.with_limits(context.context().scope().limits()).unwrap();
        let bytes = crate::pi_root_provider::canonical_pi_value(&self.commit, maximum, work).unwrap();
        let mut commit = crate::pi_root_provider::guarded_pi_decode::<RetainedSpawnCommit>(&bytes.0, maximum, maximum,
            acyclic_harness::JsonValueSchema::None, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES, work).unwrap();
        let mut plan = coordinator.begin(&context, &self.journal, &self.fence, parent_value, self.source.clone(), frozen.0,
            commit.0, ForkPreparationLimits { resources: 128, work: 1_000_000, bytes: maximum },
            work.remaining().unwrap(), work.cancellation().clone()).await.unwrap();
        Arc::get_mut(plan._input_allocations.as_mut().unwrap()).unwrap().absorb(&mut commit.1).unwrap();
        plan
    }
    fn next_commit(&self, caller: &PiTaskBinding, source: TaskId, sdk_operation: &str) -> RetainedSpawnCommit {
        let mut commit = self.commit.clone();
        let operations = PiForkOperations::new(original_operation(self.owner.policy().root_task,
            caller.task_id, sdk_operation, &crate::pi_fork::tests::work()).unwrap(), &crate::pi_fork::tests::work()).unwrap();
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
        journal: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, caller: &PiTaskBinding,
        source: Arc<LocalLazyWorkspace>, commit: RetainedSpawnCommit) -> Result<PiForkPlan> {
        let work = acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let coordinator = PiForkCoordinator::new(self.runtime.clone(), self.filesystem.clone(),
            self.stream.clone(), &self.resources, self.owner.clone(), &work)?;
        coordinator.authenticate(context, journal, fence).await?;
        let shared = self.runtime.runtime.task_context_with_native_work(caller.task_id, fence, &work).await?;
        let shared = if caller.task_id == self.owner.policy().root_task {
            crate::pi_root_provider::bind_root_output_context_with_native_work(shared, journal,
                self.owner.clone(), &self.resources, self.filesystem.clone(), self.stream.clone(),
                &self.root_request, &work).await?
        } else { shared.with_output_volume_with_native_work(&caller.scratch.volume, &work)? };
        let authority = self.owner.conversation_authority_with_native_work(caller.task_id,
            caller.agent_id, shared.context().scope().grants(), &work)?;
        let verifier = authority.0.1.verifier_with_native_work(&work)?;
        let parent = StreamAggregate::open_with_native_work(&self.stream, authority.0.0, verifier.0,
            SchemaRegistry::new(), &work).await?;
        let parent_value = parent.0.with_limits(shared.context().scope().limits())?;
        let frozen = source.snapshot_with_native_work(&work).await.map_err(crate::pi_lazy_capture::native_lazy_error)?;
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(&commit, maximum, &work)?;
        let mut commit = crate::pi_root_provider::guarded_pi_decode::<RetainedSpawnCommit>(&bytes.0, maximum, maximum,
            acyclic_harness::JsonValueSchema::None, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES, &work)?;
        let mut plan = coordinator.begin(&shared, journal, fence, parent_value, source, frozen.0, commit.0,
            ForkPreparationLimits { resources: 128, work: 1_000_000, bytes: maximum },
            work.remaining().map_err(|cause| Error::NativeWork { cause, custody: None })?,
            work.cancellation().clone()).await?;
        Arc::get_mut(plan._input_allocations.as_mut().unwrap()).unwrap().absorb(&mut commit.1)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(plan)
    }
    async fn current_context_with(&self, coordinator: &PiForkCoordinator,
        supplied: &acyclic_harness::runtime::TaskContext, journal: &Arc<TaskJournalOwner<LocalStream>>,
        fence: &LeaseFence) -> Result<Arc<acyclic_harness::NativeTaskContext>> {
        let work = &coordinator.work;
        coordinator.authenticate(supplied, journal, fence).await?;
        let shared = coordinator.runtime.runtime.task_context_with_native_work(journal.task_id(), fence, work).await?;
        let original = coordinator.runtime.runtime.task_host().observe_admission_with_native_work(journal.task_id(), work).await?;
        let input: (PiTaskInput, acyclic_fs::NativeAllocation) = coordinator.decode_input_value(&original.0.input,
            acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES)?;
        if journal.task_id() == coordinator.owner.policy().root_task {
            crate::pi_root_provider::bind_root_output_context_with_native_work(shared, journal,
                coordinator.owner.clone(), &coordinator.resources, coordinator.filesystem.clone(), coordinator.stream.clone(),
                &self.root_request, work).await
        } else { shared.with_output_volume_with_native_work(&input.0.binding.scratch.volume, work) }
    }
    #[allow(clippy::too_many_arguments, reason = "regression preserves the supplied original owner and immutable operation")]
    pub(crate) async fn advance_with(&self, coordinator: &PiForkCoordinator, context: &acyclic_harness::runtime::TaskContext,
        journal: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken) -> Result<Option<PreparedPiFork>> {
        let shared = self.current_context_with(coordinator, context, journal, fence).await?;
        coordinator.advance(&shared, journal, fence, plan, source, budget, cancel).await
    }
    #[allow(clippy::too_many_arguments, reason = "preparation must settle without admitting its future child")]
    pub(crate) async fn prepare_with(&self, coordinator: &PiForkCoordinator, context: &acyclic_harness::runtime::TaskContext,
        journal: &Arc<TaskJournalOwner<LocalStream>>, fence: &LeaseFence, plan: &PiForkPlan,
        source: Arc<LocalLazyWorkspace>, budget: WorkBudget, cancel: CancellationToken)
        -> Result<Option<(acyclic_harness::fork::ForkReport, acyclic_fs::NativeAllocation)>> {
        let shared = self.current_context_with(coordinator, context, journal, fence).await?;
        coordinator.prepare(&shared, journal, fence, plan, source, budget, cancel).await
    }
}

pub(crate) fn policy() -> PiOwnerPolicy {
    let agent = AgentId::from_bytes([81; 16]);
    let provider = acyclic_harness::resources::ProviderRef::new("pi-owner-test", "filesystem", "1").unwrap();
    let scratch = VolumeRef::new(
        provider.clone(),
        "scratch",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )
    .unwrap();
    PiOwnerPolicy {
        root_task: TaskId::from_bytes([82; 16]),
        agent,
        journal: VolumeRef::new(
            provider,
            "journal",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )
        .unwrap(),
        grants: Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:cancel".to_owned(),
            "operation:wake".to_owned(),
            "task:spawn:acyclic.pi@1".to_owned(),
            scratch.capability(VolumeOperation::Read).unwrap(),
            scratch.capability(VolumeOperation::Write).unwrap(),
        ]),
        limits: Limits {
            file_bytes: 65_536,
            path_bytes: 1024,
            attachments: 16,
            render_bytes: 65_536,
            model_steps: 16,
            model_events_per_step: 128,
            tool_calls_per_step: 16,
            context_messages: 128,
        },
        run_limits: TaskRunLimits {
            concurrency: Some(4),
            max_steps: Some(16),
            deadline_epoch_ms: None,
        },
        session_limits: SessionLimits {
            active_tasks: 4,
            total_tasks: 8,
            depth: 3,
            model_steps: 16,
        },
        concurrency: 4,
        maximum_payload_bytes: 65_536,
    }
}

pub(crate) fn work() -> acyclic_fs::NativeMaterializationWork {
    acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap()
}

#[test]
fn original_sdk_operations_are_distinct_stable_and_caller_bound() {
    let root = TaskId::from_bytes([1; 16]);
    let caller = TaskId::from_bytes([2; 16]);
    let operation = original_operation(root, caller, "sdk-original-tool-call", &crate::pi_fork::tests::work()).unwrap();
    assert_eq!(operation, original_operation(root, caller, "sdk-original-tool-call", &crate::pi_fork::tests::work()).unwrap());
    assert_ne!(operation, original_operation(root, root, "sdk-original-tool-call", &crate::pi_fork::tests::work()).unwrap());
    let operations = PiForkOperations::new(operation, &crate::pi_fork::tests::work()).unwrap();
    let ids = [operation, operations.child, operations.preparation, operations.fork, operations.native];
    for (index, id) in ids.iter().enumerate() { assert!(!ids[..index].contains(id)); }
}

#[tokio::test]
async fn actual_fork_creation_admission_publication_and_lost_reply_reopen_preserve_one_original_cut() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    fixture.source.workspace().write("/authored-later.txt", bytes::Bytes::from_static(b"later parent generation")).await.unwrap();
    let query = work();
    let current = fixture.source.snapshot_with_native_work(&query).await.unwrap();
    assert_ne!(current.0.authored_generation, plan.frozen.authored_generation);
    let retried = fixture.plan(&query).await;
    assert_eq!(retried.frozen, plan.frozen);
    assert_eq!(retried.project_creation, plan.project_creation);
    assert_eq!(retried.source, plan.source);
    assert_eq!(retried.request, plan.request);
    assert_eq!(retried.preparation, plan.preparation);
    assert!(fixture.runtime.runtime.task_host().observe_admission(plan.preparation.task_id()).await.is_err());
    assert!(!plan.commit.launch.invocation.environment.keys().any(|key| reserved_native_key(key)));
    let coordinator = fixture.coordinator();
    let created = fixture.advance_with(&coordinator, &fixture.context, &fixture.journal, &fixture.fence, &plan,
        fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(created.workspace.workspace().name().as_str(), plan.commit.child.project_volumes.values().next().unwrap().storage_name().unwrap());
    assert_eq!(created.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    assert!(created.workspace.read("/authored-later.txt", 64).await.is_err());
    assert_eq!(created.report.request, serde_json::from_value::<ForkRequest>(plan.request.payload.clone()).unwrap());
    assert!(matches!(created.binding.scratch.allocation, PiScratchAllocation::Created(_)));
    assert!(!created.admission.grants.contains(&plan.source.project.capability(VolumeOperation::Write).unwrap()));
    assert!(!created.admission.grants.contains(&plan.source.private_volume.capability(VolumeOperation::Read).unwrap()));
    let child_workspace = created.workspace.workspace().id();
    let child_generation = created.native_lazy;
    drop(created); // All original successful replies may be lost by the caller.
    drop(coordinator);
    let reopened = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &plan,
        fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(reopened.workspace.workspace().id(), child_workspace);
    assert_eq!(reopened.native_lazy, child_generation);
    assert_eq!(std::fs::read(fixture.native.join("observed.txt")).unwrap(), b"original");
}

#[tokio::test]
async fn original_preparation_task_partial_checkpoint_reopens_without_early_child_admission() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let coordinator = fixture.coordinator();
    let work = &coordinator.work;
    coordinator.admit_preparation(&fixture.context, &fixture.journal, &fixture.fence, &plan).await.unwrap();
    let host = fixture.runtime.runtime.task_host();
    let worker_name = work.display("fork-partial-original").unwrap();
    let worker = Worker { id: worker_name.0, available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let partial = fixture.runtime.runtime.run_operation_with_native_work(&worker, plan.preparation.task_id(),
        &fixture.runtime.runtime.commands(), 1, work).await.unwrap();
    assert!(matches!(partial.0, Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. }))));
    assert!(host.observe_admission(plan.request.child.task_id()).await.is_err());
    assert!(fixture.resources.fs.open_workspace_with_native_work(&plan.commit.child.scratch.volume.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.is_err());
    let restored = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &plan,
        fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(restored.admission, plan.request.child);
}

#[tokio::test]
async fn cancellation_and_owner_replacement_never_admit_a_child() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &plan,
        fixture.source.clone(), crate::pi_root_provider::allocation_budget(), cancel).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(plan.request.child.task_id()).await.is_err());
    let replacement = PiOwnerConfig::new(fixture.owner.policy(), None, &crate::pi_fork::tests::work()).unwrap();
    assert!(PiForkCoordinator::new(fixture.runtime.clone(), fixture.filesystem.clone(), fixture.stream.clone(),
        &fixture.resources, Arc::new(replacement), &acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap()).is_err());
    assert!(fixture.resources.fs.open_workspace_with_native_work(&plan.commit.child.scratch.volume.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.is_err());
}

#[tokio::test]
async fn changed_native_sdk_cut_session_bytes_and_child_identity_never_enter_preparation() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    assert!(plan.request.child.resource_proofs.is_empty());
    for field in 0..3 {
        let mut changed = plan.clone();
        match field {
            0 => changed.commit.launch.source.entries_json = "[{\"id\":\"foreign\"}]".into(),
            1 => changed.commit.launch.session_bytes.push_str("{}\n"),
            _ => changed.commit.child.native_session_id = "another-native-sdk-session".into(),
        }
        assert!(fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence,
            &changed, fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.is_err());
        assert!(fixture.runtime.runtime.task_host().observe_admission(plan.preparation.task_id()).await.is_err());
        assert!(fixture.resources.fs.open_workspace_with_native_work(&plan.commit.child.scratch.volume.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.is_err());
    }
}

#[tokio::test]
async fn internally_consistent_child_authority_substitution_cannot_replace_protected_original_plan() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let mut changed = plan.clone();
    let foreign_write = plan.source.project.capability(VolumeOperation::Write).unwrap();
    changed.request.child.grants = Capabilities::new(changed.request.child.grants.iter()
        .chain(std::iter::once(foreign_write.as_str())));
    changed.preparation.input = serde_json::to_value(&changed.request).unwrap();
    assert!(fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence,
        &changed, fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(plan.preparation.task_id()).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(plan.request.child.task_id()).await.is_err());
    assert!(fixture.resources.fs.open_workspace_with_native_work(&plan.commit.child.scratch.volume.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.is_err());
}

#[tokio::test]
async fn explicit_root_sibling_and_descendant_sources_use_operation_read_proofs_not_caller_grants() {
    let fixture = Fixture::new(None).await;
    let root_grants = fixture.context.scope().grants().clone();
    let first_plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let first = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &first_plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    let root: PiTaskInput = serde_json::from_value(fixture.runtime.runtime.task_host()
        .observe_admission(fixture.journal.task_id()).await.unwrap().input).unwrap();
    let sibling_commit = fixture.next_commit(&root.binding, root.binding.task_id, "sibling-call");
    let sibling_plan = fixture.selected_plan(&fixture.context, &fixture.journal, &fixture.fence, &root.binding, fixture.source.clone(), sibling_commit).await.unwrap();
    let sibling = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &sibling_plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();

    let worker = Worker { id: "actual-child-caller".into(), available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let original = work();
    let attempt = fixture.runtime.runtime.run_operation_with_native_work(&worker, first.binding.task_id,
        &fixture.runtime.runtime.commands(), 1, &original).await.unwrap();
    let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = attempt.0 else { panic!("original child native command owner"); };
    let fence = LeaseFence::from(&lease.reservation);
    let journal = Arc::new(fixture.runtime.runtime.task_host().journal_owner(first.binding.task_id, fence.clone()).await.unwrap());
    let context = fixture.runtime.runtime.task_context(first.binding.task_id, fence.clone()).await.unwrap();
    let original_grants = context.scope().grants().clone();
    let source_read = root.binding.project_volumes.values().next().unwrap().capability(VolumeOperation::Read).unwrap();
    assert!(!original_grants.contains(&source_read));

    let mut commit = fixture.next_commit(&first.binding, root.binding.task_id, "selected-root-call");
    commit.launch.source.parent_cwd = first_plan.commit.launch.invocation.cwd.to_str().unwrap().into();
    let root_plan = fixture.selected_plan(&context, &journal, &fence, &first.binding,
        fixture.source.clone(), commit).await.unwrap();
    let fork: ForkRequest = serde_json::from_value(root_plan.request.payload.clone()).unwrap();
    assert_eq!(fork.authorized_source.as_ref().unwrap().source_task, root.binding.task_id);
    assert_eq!(fork.preparation.inherited_through_sequence, 0);
    assert_eq!(root_plan.source.private_volume, first.binding.scratch.volume);
    assert_eq!(context.scope().grants(), &original_grants);
    let descendant = fixture.advance_with(&fixture.coordinator(), &context, &journal, &fence, &root_plan,
        fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert!(!descendant.admission.grants.contains(&source_read));
    assert!(matches!(descendant.binding.results.as_ref().unwrap().allocation, PiScratchAllocation::Created(_)));
    assert_eq!(descendant.report.child_result_volume, descendant.binding.results.as_ref().map(|binding| binding.volume.clone()));

    let mut commit = fixture.next_commit(&first.binding, sibling.binding.task_id, "selected-sibling-call");
    commit.launch.source.parent_cwd = first_plan.commit.launch.invocation.cwd.to_str().unwrap().into();
    let sibling_source = sibling.workspace.clone();
    let plan = fixture.selected_plan(&context, &journal, &fence, &first.binding,
        sibling_source.clone(), commit).await.unwrap();
    let fork: ForkRequest = serde_json::from_value(plan.request.payload.clone()).unwrap();
    assert_eq!(fork.authorized_source.as_ref().unwrap().source_task, sibling.binding.task_id);
    assert_eq!(context.scope().grants(), &original_grants);
    let actual = fixture.advance_with(&fixture.coordinator(), &context, &journal, &fence, &plan, sibling_source,
        crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(actual.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    assert!(!actual.admission.grants.contains(&sibling.binding.project_volumes.values().next().unwrap()
        .capability(VolumeOperation::Read).unwrap()));

    let source = descendant.workspace.clone();
    let commit = fixture.next_commit(&root.binding, descendant.binding.task_id, "selected-descendant-call");
    let plan = fixture.selected_plan(&fixture.context, &fixture.journal, &fixture.fence, &root.binding, source.clone(), commit).await.unwrap();
    let actual = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &plan, source, crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    assert_eq!(actual.workspace.read("/observed.txt", 64).await.unwrap().as_ref(), b"original");
    assert_eq!(fixture.context.scope().grants(), &root_grants);
}

#[tokio::test]
async fn actual_stream_and_task_registry_restart_reconciles_original_creator_proofs_and_publication() {
    let fixture = Fixture::new(None).await;
    let observation_work = acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let first = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &plan, fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    let stream = Arc::new(LocalStream::open(fixture.resources.session_directory("fork-root").join("stream"),
        LocalStreamLimits::default()).await.unwrap());
    let runtime = Arc::new(Runtime::open_with_native_work(stream.clone(), fixture.filesystem.clone(),
        &fixture.owner.policy().journal, fixture.owner.authority_with_native_work(&observation_work).unwrap(),
        &observation_work).await.unwrap());
    let journal = runtime.runtime.task_host().journal_owner_with_native_work(
        fixture.journal.task_id(), &fixture.fence, &observation_work).await.unwrap()
        .into_native_shared(&observation_work).unwrap();
    let context = runtime.runtime.task_context_with_native_work(journal.task_id(), &fixture.fence,
        &observation_work).await.unwrap();
    let context = crate::pi_root_provider::bind_root_output_context_with_native_work(context, &journal,
        fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), stream.clone(),
        &fixture.root_request, &observation_work).await.unwrap();
    let coordinator = PiForkCoordinator::new(runtime, fixture.filesystem.clone(), stream,
        &fixture.resources, fixture.owner.clone(), &observation_work).unwrap();
    let original = coordinator.load_plan(context.context(), plan.original, observation_work.remaining().unwrap(),
        observation_work.cancellation()).await.unwrap().unwrap();
    let recovered = coordinator.advance(&context, &journal, &fixture.fence, &original, fixture.source.clone(),
        observation_work.remaining().unwrap(), observation_work.cancellation().clone()).await.unwrap().unwrap();
    assert_eq!(first.report, recovered.report);
    assert_eq!(first.admission, recovered.admission);
    assert_eq!(first.binding, recovered.binding);
    assert_eq!(first.native_lazy, recovered.native_lazy);
    assert_eq!(first.workspace.workspace().id(), recovered.workspace.workspace().id());
}

#[tokio::test]
async fn cold_pre_first_dispatch_restores_only_under_actual_child_owner_and_existing_publication() {
    let fixture = Fixture::new(None).await;
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let first = fixture.advance_with(&fixture.coordinator(), &fixture.context, &fixture.journal, &fixture.fence, &plan,
        fixture.source.clone(), crate::pi_root_provider::allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    let work = acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    let coordinator = PiForkCoordinator::new(fixture.runtime.clone(), fixture.filesystem.clone(),
        fixture.stream.clone(), &fixture.resources, fixture.owner.clone(), &work).unwrap();
    let worker_name = work.display("cold-native-child-owner").unwrap();
    let worker = Worker { id: worker_name.0, available: ResourceSnapshot::default(), labels: BTreeMap::new() };
    let attempt = fixture.runtime.runtime.run_operation_with_native_work(&worker, first.binding.task_id,
        &fixture.runtime.runtime.commands(), 1, &work).await.unwrap();
    let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = attempt.0 else { panic!("actual child owner"); };
    let (reservation_id, mut custody) = work.display(&lease.reservation.id).unwrap();
    let (placement, mut placement_custody) = work.display(&lease.reservation.placement).unwrap();
    custody.absorb(&mut placement_custody).unwrap();
    let fence = (LeaseFence { reservation_id, placement }, custody);
    let journal = fixture.runtime.runtime.task_host().journal_owner_with_native_work(
        first.binding.task_id, &fence.0, &work).await.unwrap().into_native_shared(&work).unwrap();
    let context = fixture.runtime.runtime.task_context_with_native_work(first.binding.task_id, &fence.0, &work).await.unwrap()
        .with_output_volume_with_native_work(&first.binding.scratch.volume, &work).unwrap();
    let original = coordinator.load_plan(context.context(), plan.original, work.remaining().unwrap(),
        work.cancellation()).await.unwrap().unwrap();
    let restored = coordinator.restore_admitted(&context, &journal, &fence.0, &original,
        fixture.source.clone(), work.remaining().unwrap(), work.cancellation().clone()).await.unwrap();
    assert_eq!(restored.binding, first.binding);
    assert_eq!(restored.report, first.report);
    assert_eq!(restored.admission, first.admission);
    assert_eq!(restored.native_lazy, first.native_lazy);
    let mut changed = original;
    changed.commit.source_task = first.binding.task_id;
    assert!(coordinator.restore_admitted(&context, &journal, &fence.0, &changed, fixture.source.clone(),
        work.remaining().unwrap(), work.cancellation().clone()).await.is_err());
}

#[tokio::test]
async fn root_freeze_uses_actual_original_callback_context_and_watcher_epoch() {
    let fixture = Fixture::new(None).await;
    let work = acyclic_fs::NativeMaterializationWork::new(crate::pi_root_provider::allocation_budget(), &CancellationToken::new()).unwrap();
    let coordinator = PiForkCoordinator::new(fixture.runtime.clone(), fixture.filesystem.clone(),
        fixture.stream.clone(), &fixture.resources, fixture.owner.clone(), &work).unwrap();
    let context = fixture.current_context_with(&coordinator, &fixture.context, &fixture.journal, &fixture.fence).await.unwrap();
    let original = coordinator.runtime.runtime.task_host().observe_admission_with_native_work(fixture.journal.task_id(), &work).await.unwrap();
    let input: (PiTaskInput, acyclic_fs::NativeAllocation) = coordinator.decode_input_value(&original.0.input,
        acyclic_harness::JsonValueSchema::None, crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES).unwrap();
    let key = input.0.binding.project_volumes.keys().next().unwrap();
    let frozen = crate::pi_fork_host::freeze_created_root_source(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &context, &fixture.journal,
        &input.0, key, &fixture.root_handle, &work).await.unwrap();
    assert_eq!(frozen.0.workspace_id, fixture.source.workspace().id());
    fixture.source.validate_snapshot_with_native_work(frozen.0, &work).await.unwrap();
    assert!(crate::pi_lazy_capture::freeze_root_source_with_native_work(TaskId::from_bytes([9; 16]),
        context.context(), &fixture.journal, &fixture.root_handle.physical, &fixture.source, &work,
        || async { panic!("foreign root must be rejected before the original creator callback") }).await.is_err());
    work.cancellation().cancel();
    assert!(crate::pi_fork_host::freeze_created_root_source(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &context, &fixture.journal,
        &input.0, key, &fixture.root_handle, &work).await.is_err());
}

#[tokio::test]
async fn explicit_source_without_original_session_task_proof_cannot_prepare_or_allocate() {
    let fixture = Fixture::new(None).await;
    let root: PiTaskInput = serde_json::from_value(fixture.runtime.runtime.task_host()
        .observe_admission(fixture.journal.task_id()).await.unwrap().input).unwrap();
    let commit = fixture.next_commit(&root.binding, TaskId::from_bytes([230; 16]), "foreign-root-escape");
    let operations = PiForkOperations::new(original_operation(root.binding.task_id, root.binding.task_id, &commit.sdk_operation, &crate::pi_fork::tests::work()).unwrap(), &crate::pi_fork::tests::work()).unwrap();
    let future_project = commit.child.project_volumes.values().next().unwrap().clone();
    assert!(fixture.selected_plan(&fixture.context, &fixture.journal, &fixture.fence,
        &root.binding, fixture.source.clone(), commit).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(task_id_for_admission(operations.preparation)).await.is_err());
    assert!(fixture.runtime.runtime.task_host().observe_admission(task_id_for_admission(operations.child)).await.is_err());
    assert!(fixture.resources.fs.open_workspace_with_native_work(&future_project.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.is_err());
}
