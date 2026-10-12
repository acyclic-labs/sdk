//! Credential-free, explicitly trusted local qualification input producer.
#![allow(missing_docs, reason = "private installed-binary scenario boundary")]

use super::native::{AuthorityBinding, Config, ForkSourceConfig};
use super::native_tools::{NativeToolCaller, NativeToolFork, NativeToolMerge};
use super::native_writeback::NativePublicationConfig;
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness::{
    Admission, AgentId, Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{ContentGrant, ConversationMessage, FileRef, Limits, MessageKind,
        VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, SchemaRegistry},
    distributed::{WorkLease, WorkPull, Worker},
    executor::TurnOutput,
    filesystem::{FilesystemContentVerifier, FilesystemForkVerifier, FilesystemHost,
        FilesystemProjectMergeVerifier, FilesystemResourceCreationProvider, FilesystemTaskRuntime,
        ForkPreparationLimits, ForkPreparationMachine, ForkToolArguments, RecursiveForkConfig,
        RecursiveSourceSnapshot, StockTurnMachine, StreamHistoryForkVerifier,
        fork_preparation_request, recursive_fork_source, workspace_ref,
        FORK_PREPARE_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND, ModelTaskCommand},
    fork::{CompositeForkVerifier, ForkReport},
    model::ModelContent,
    resource_preparation::ResourcePreparationRequest,
    resources::ProviderRef,
    runtime::{DurableTaskHost as _, RuntimeScope, RuntimeTask, TaskAdmissionRecord, TaskDefinition,
        TaskRegistry, TaskRunLimits},
    scheduler::{LeaseFence, SessionLimits},
    store::{HistoryReadLimits, StreamAggregate},
    workflow::{MachineRegistry, WorkflowCommand},
};
use acyclic_stream::{LocalStream, StreamClient, SystemUnixMillisClock};
use prost::Message as _;
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::{Path, PathBuf}, sync::Arc};

const ISSUER: &str = "graphcoder-explicit-trusted-local-qualification";
// Public test authority, deliberately not production authentication or a secret.
const TEST_KEY: [u8; 32] = [91; 32];
const PAYLOAD_BYTES: u64 = super::terminal::MAX_FRAME_BYTES as u64;

type Files = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
type Runtime = FilesystemTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

struct Caller {
    task: TaskId,
    binding: AuthorityBinding,
    project: VolumeRef,
    private: VolumeRef,
    result: Option<VolumeRef>,
    aggregate: StreamAggregate<LocalStream>,
    lease: WorkLease,
}

struct Producer {
    root: PathBuf,
    files: Arc<Files>,
    stream: StreamClient<LocalStream>,
    runtime: Runtime,
    config: Config,
    template: TaskAdmissionRecord,
    worker: Worker,
    preparations: Vec<Value>,
    references: BTreeMap<String, FileRef>,
    planned_commands: BTreeMap<String, WorkflowCommand>,
    planned_inputs: BTreeMap<String, FileRef>,
    leases: BTreeMap<String, PathBuf>,
    cancellation: acyclic_fs::CancellationToken,
}

/// Produces actual local provider resources, authenticated source checkpoints, retained ordinary preparation requests/reports and original owner leases.
pub async fn prepare(root: &Path, cancellation: &acyclic_fs::CancellationToken) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(Error::Invalid("scenario root must be an absolute path".into()));
    }
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(format!("native IO service startup: {error}")))?;
    acyclic_fs::initialize_native_mount_services()
        .map_err(|error| Error::Storage(format!("native mount service startup: {error}")))?;
    let negative = prepare_case(&root.join("one-below-actual"), 11, None, cancellation).await?;
    prepare_case(root, 12, Some(negative), cancellation).await
}

async fn prepare_case(root: &Path, total_tasks: u64, negative_control: Option<PathBuf>,
    cancellation: &acyclic_fs::CancellationToken) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(Error::Invalid("scenario root must be an absolute path".into()));
    }
    tokio::fs::create_dir_all(root).await.map_err(|error| Error::Storage(error.to_string()))?;
    let root = tokio::fs::canonicalize(root).await.map_err(|error| Error::Storage(error.to_string()))?;
    // Never mix qualification identities with a previous or caller-owned store.
    match tokio::fs::create_dir(root.join("inputs")).await {
        Ok(()) => {},
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(Error::Conflict("scenario inputs already exist; use a fresh directory".into()));
        },
        Err(error) => return Err(Error::Storage(error.to_string())),
    }
    let deployment = format!("{ISSUER}.{}", uuid::Uuid::new_v4());
    let filesystem_provider = ProviderRef::new(&deployment, "filesystem", "2")?;
    let stream_provider = ProviderRef::new(&deployment, "stream", "2")?;
    let seed_agent = AgentId::new();
    let private = VolumeRef::new(filesystem_provider.clone(), "bootstrap-private",
        VolumeClass::AgentPrivate, VolumeOwner::Agent(seed_agent))?;
    let project = VolumeRef::new(filesystem_provider.clone(), "bootstrap-project",
        VolumeClass::Project, VolumeOwner::Project("qualification-project".into()))?;
    let authority = Authority { kind: AggregateKind::Conversation, id: format!("qualification-bootstrap-{deployment}") };
    let issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, authority.clone());
    let grants = Capabilities::new([
        "conversation:bind".to_owned(), "conversation:append".to_owned(),
        "fork:publish".to_owned(), "project:merge".to_owned(),
        "operation:declare".to_owned(), "operation:observe".to_owned(),
        "operation:cancel".to_owned(), "operation:wake".to_owned(),
        "model:generate".to_owned(), "task:spawn:acyclic.stock_turn@1".to_owned(),
        "task:spawn:acyclic.fork_preparation@1".to_owned(),
        "task:spawn:acyclic.mailbox_model@1".to_owned(),
        "mail:send".to_owned(), "mail:read".to_owned(),
        "interaction:open".to_owned(), "interaction:resolve".to_owned(),
        "tool:call:acyclic.fork".to_owned(), "tool:call:acyclic.merge".to_owned(),
        "tool:call:acyclic.read_file".to_owned(), "tool:call:acyclic.write_file".to_owned(),
        "tool:call:acyclic.edit_file".to_owned(),
        private.capability(VolumeOperation::Read)?, private.capability(VolumeOperation::Write)?,
        project.capability(VolumeOperation::Read)?, project.capability(VolumeOperation::Write)?,
    ]);
    let scope = issuer.root_for_agent(seed_agent, "explicit-trusted-local-bootstrap", grants);
    let mut owner = AuthorityBinding { authority, issuer: ISSUER.into(), key: TEST_KEY, scope };
    let limits = Limits { file_bytes: PAYLOAD_BYTES, path_bytes: 1024, attachments: 16,
        render_bytes: 16 * 1024, model_steps: 1, model_events_per_step: 64,
        tool_calls_per_step: 16, context_messages: 64 };
    let run_limits = TaskRunLimits { concurrency: Some(6), model_steps: Some(1), deadline_epoch_ms: None };
    let mut config = Config {
        filesystem_root: root.join("filesystem"), stream_root: root.join("stream"),
        filesystem_provider: filesystem_provider.clone(), stream_provider: stream_provider.clone(),
        private_volume: private.clone(), owner: owner.clone(), observed_authorities: Vec::new(),
        generation: format!("qualification-{}", uuid::Uuid::new_v4()), limits, run_limits,
        model_invocation_ledger: root.join("model-invocations.jsonl"),
        session_limits: SessionLimits { active_tasks: 6, total_tasks, depth: 3, model_steps: 7 },
        concurrency: 6, deterministic_delay_ms: 2000, fork_sources: Vec::new(),
        tools: Default::default(), native_publications: Vec::new(),
        project_source_root: None,
    };
    // Open the actual diagnostic provider sink before any model invocation.
    drop(super::provider::Deterministic::new(
        config.deterministic_delay_ms, &config.model_invocation_ledger,
    )?);
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&config.filesystem_root)).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let native_root = root.join("native-project");
    tokio::fs::create_dir(&native_root).await.map_err(|error| Error::Storage(error.to_string()))?;
    tokio::fs::write(native_root.join("sentinel.txt"), b"host original imported sentinel\n").await.map_err(|error| Error::Storage(error.to_string()))?;
    tokio::fs::write(native_root.join("concurrent-user.txt"), b"host original concurrent file\n").await.map_err(|error| Error::Storage(error.to_string()))?;
    let imported = filesystem.attach_directory(project.storage_name()?, &native_root, acyclic_fs::SourceOptions {
        mode: acyclic_fs::SourceMode::Pinned, maximum_paths: 64, maximum_extent_spans: 16,
        maximum_queued_changes: 64, excluded_paths: Vec::new(),
    }).await.map_err(|error| Error::Unsupported(format!("genuine bounded native source import failed: {error}")))?;
    let imported_generation = imported.head().await.map_err(|error| Error::Storage(error.to_string()))?;
    imported_generation.pin("graphcoder-native-original-baseline").await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let native_operation_directory = root.join("native-writeback-operation");
    tokio::fs::create_dir(&native_operation_directory).await.map_err(|error| Error::Storage(error.to_string()))?;
    let files = Arc::new(FilesystemHost::new(filesystem.clone(), filesystem_provider)?);
    files.create_volume(&private).await?;
    files.create_volume(&project).await?;
    // Known qualification prompts are real provider-owned command artifacts.
    let project_writer = ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &owner.scope, &project, VolumeOperation::Write)?;
    let mut planned_commands = BTreeMap::new();
    let mut planned_inputs = BTreeMap::new();
    let mut artifact_grants = owner.scope.capabilities().iter().map(str::to_owned).collect::<Vec<_>>();
    for label in ["bootstrap", "root", "left", "right", "grandchild", "cross_source"] {
        let input = ModelTaskCommand { input: ModelContent::Text(format!("real isolated {label} qualification task")),
            selected_context: None, max_steps: 1, max_output_tokens: Some(256), mailbox_operation: None };
        let bytes = serde_json::to_vec(&input).map_err(|error| Error::Invalid(error.to_string()))?;
        let file = files.put_content(&project, &project_writer, &format!(".qualification-inputs/{label}.json"),
            &bytes, "application/json", "model-input.json", PAYLOAD_BYTES,
            &IdempotencyKey::new(format!("planned-input-{label}"))?).await?;
        artifact_grants.push(file.read_capability()?);
        let command = WorkflowCommand { operation_id: OperationId::new(), kind: MODEL_TASK_COMMAND_KIND.into(),
            payload: file.clone() };
        planned_commands.insert(label.to_owned(), command);
        planned_inputs.insert(label.to_owned(), file);
    }
    owner.scope = issuer.root_for_agent(seed_agent, "explicit-trusted-local-bootstrap", Capabilities::new(artifact_grants));
    config.owner = owner.clone();
    let stream = StreamClient::new(Arc::new(LocalStream::open(&config.stream_root, Default::default())
        .await.map_err(|error| Error::Storage(error.to_string()))?));
    let stock = Arc::new(StockTurnMachine::new());
    let preparation = Arc::new(ForkPreparationMachine::new());
    let mut tasks = TaskRegistry::default();
    tasks.register(stock.definition()?)?;
    tasks.register(preparation.definition()?)?;
    let mut machines = MachineRegistry::default();
    machines.register(stock)?;
    machines.register(preparation)?;
    let creator = Arc::new(FilesystemResourceCreationProvider::new(files.clone(), issuer.clone())?);
    let runtime = Runtime::open_with_creation_providers(
        stream.clone(), files.clone(), private.clone(), issuer.verifier(&mut None).expect("verifier copy"), owner.scope.clone(),
        RuntimeScope::new(owner.scope.capabilities().clone(), limits)?.with_run_limits(run_limits)?,
        tasks, machines, Default::default(), config.session_limits, config.concurrency,
        PAYLOAD_BYTES, Arc::new(SystemUnixMillisClock), None, vec![creator], Default::default(),
    ).await?;
    let definition: Arc<TaskDefinition<WorkflowCommand, TurnOutput>> = runtime.harness().task("acyclic.stock_turn@1")?;
    let operation = OperationId::new();
    let command = planned_commands["bootstrap"].clone();
    let seed_task = admitted_task(runtime.harness().admit(operation, &definition, command, None).await?)?;
    config.project_source_root = Some(seed_task);
    let template = runtime.task_host().observe_admission(seed_task).await?;
    let worker = Worker { id: format!("qualification-bootstrap-{}", uuid::Uuid::new_v4()),
        available: Default::default(), labels: Default::default() };
    let seed_lease = claim(&runtime, seed_task, &worker).await?;
    let seed_lease_path = root.join("inputs").join("bootstrap_seed-original-lease.json");
    persist(&seed_lease_path, &seed_lease).await?;
    let mut aggregate = aggregate(&stream, files.clone(), &owner, &config).await?;
    aggregate.execute(Command { operation_id: OperationId::new(), idempotency_key: IdempotencyKey::new("bind-bootstrap")?,
        expected_revision: 0, scope: owner.scope.clone(), causal_parent: None,
        action: Action::BindConversation { agent: seed_agent } }).await?;
    let mut seed = Caller { task: seed_task, binding: owner, project, private, result: None,
        aggregate, lease: seed_lease };
    let mut producer = Producer { root: root.clone(), files, stream, runtime, config, template,
        worker, preparations: Vec::new(), references: BTreeMap::new(),
        leases: BTreeMap::from([("bootstrap_seed".to_owned(), seed_lease_path)]),
        planned_commands, planned_inputs, cancellation: cancellation.clone() };
    let imported_ref = acyclic_harness::resources::GenerationRef::new(producer.config.filesystem_provider.clone(),
        imported_generation.id().digest().into_bytes(), None)?;
    let imported_bytes = producer.files.read(&workspace_ref(producer.config.filesystem_provider.clone(),
        &seed.project.storage_name()?)?, Some(&imported_ref), "/sentinel.txt", PAYLOAD_BYTES).await?;
    let imported_file = FileRef::new(seed.project.clone(), "sentinel.txt",
        hex::encode(imported_generation.id().digest().into_bytes()),
        acyclic_harness::conversation::FileDescriptor::from_bytes(&imported_bytes, "text/plain")?, "sentinel.txt")?;
    producer.references.insert("native_import_baseline".into(), imported_file);
    for (label, file) in &producer.planned_inputs {
        producer.references.insert(format!("{label}_input"), file.clone());
    }
    producer.seed(&mut seed, "bootstrap", 1).await?;
    let mut logical_root = producer.fork(&mut seed, "root", None, true).await?
        .ok_or_else(|| Error::Conflict("bootstrap root was not published".into()))?;
    producer.seed(&mut logical_root, "root", 17).await?;
    let mut left = producer.fork(&mut logical_root, "left", None, true).await?
        .ok_or_else(|| Error::Conflict("left fork was not published".into()))?;
    producer.seed(&mut left, "left", 2).await?;
    let mut right = producer.fork(&mut logical_root, "right", None, true).await?
        .ok_or_else(|| Error::Conflict("right fork was not published".into()))?;
    producer.seed(&mut right, "right", 1).await?;
    let mut grandchild = producer.fork(&mut left, "grandchild", None, true).await?
        .ok_or_else(|| Error::Conflict("grandchild fork was not published".into()))?;
    producer.seed(&mut grandchild, "grandchild", 1).await?;
    // Authenticated foreign project selection only.
    producer.fork(&mut left, "cross_source", Some(logical_root.task), false).await?;
    let left_issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, left.binding.authority.clone());
    let notice_writer = ContentGrant::verify(&left_issuer.verifier(&mut None).expect("verifier copy"), &left.binding.scope, &left.private, VolumeOperation::Write)?;
    let notice_file = producer.files.put_content(&left.private, &notice_writer, "merge/selected-grandchild.txt",
        b"Explicit caller notice: import the selected immutable grandchild project; do not import private scratch.\n",
        "text/plain", "merge-notice.txt", PAYLOAD_BYTES, &IdempotencyKey::new("left-grandchild-merge-notice")?).await?;
    producer.references.insert("merge_notice".into(), notice_file.clone());
    let notice_sequence = left.aggregate.reducer().conversation()
        .ok_or_else(|| Error::Invalid("merge caller conversation is missing".into()))?
        .logical_revision().checked_add(1).ok_or_else(|| Error::Invalid("merge notice sequence overflow".into()))?;
    producer.config.tools.merges.push(NativeToolMerge { caller: left.task, source: grandchild.task,
        notice: ConversationMessage { id: uuid::Uuid::new_v4(), sequence: notice_sequence, kind: MessageKind::Merge,
            content: notice_file, attachments: Default::default(), reply_to: None,
            tool_call_id: None, extensions: Default::default() } });
    let callers = [(&seed, "bootstrap_seed"), (&logical_root, "root"), (&left, "left"),
        (&right, "right"), (&grandchild, "grandchild")];
    let mut identities = BTreeMap::new();
    let mut caller_records = BTreeMap::new();
    for (caller, label) in callers {
        if label != "bootstrap_seed" {
            producer.config.tools.callers.push(NativeToolCaller { task: caller.task, owner: caller.binding.clone(),
                project: caller.project.clone(), private_volume: caller.private.clone(),
                native_snapshot: None, result_volume: caller.result.clone() });
        }
        identities.insert(label, caller.task);
        caller_records.insert(label, json!({ "task": caller.task, "agent": caller.binding.scope.agent(),
            "authority": caller.binding.authority, "project": caller.project, "private_volume": caller.private,
            "result_volume": caller.result,
            "admission": producer.runtime.task_host().observe_admission(caller.task).await?,
            "volume_snapshots": producer.runtime.task_host().observe_volume_snapshots(caller.task).await? }));
    }
    let mut file_refs = BTreeMap::new();
    for (label, file) in &producer.references {
        let path = root.join("inputs").join(format!("ref-{label}.json"));
        persist(&path, file).await?;
        file_refs.insert(label, path);
    }
    let cursor = acyclic_harness::wire::ReplayCursor {
        authority: Some(acyclic_harness::wire_codec::encode_authority(&producer.config.owner.authority)),
        generation: producer.config.generation.clone(), revision: 0,
    };
    let cursor_path = root.join("inputs").join("owner-cursor.pb");
    let cursor_frame = acyclic_harness::wire::ClientFrame {
        frame: Some(acyclic_harness::wire::client_frame::Frame::Resume(acyclic_harness::wire::ResumeRequest {
            protocol: Some(acyclic_harness::wire_api::current_protocol()), cursors: vec![cursor],
        })),
    };
    tokio::fs::write(&cursor_path, cursor_frame.encode_to_vec()).await.map_err(|error| Error::Storage(error.to_string()))?;
    let config_path = root.join("native-config.json");
    let target_workspace = filesystem.open_workspace(logical_root.project.storage_name()?).await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let target_generation = target_workspace.head().await.map_err(|error| Error::Storage(error.to_string()))?;
    target_generation.pin("graphcoder-native-exact-approved-target").await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let publication_operation = OperationId::new();
    let source_root_identity = acyclic_fs::capture_root_identity(&native_root)
        .map_err(|error| Error::Unsupported(format!("actual imported root identity is unavailable: {error}")))?.to_bytes();
    let native_binding = NativePublicationConfig {
        operation_id: publication_operation, caller: logical_root.task, caller_lease: logical_root.lease.clone(),
        owner: logical_root.binding.clone(), private_volume: logical_root.private.clone(),
        root: native_root.clone(), operation_directory: native_operation_directory.clone(),
        state_root: root.join("native-publication-state"), source_root_identity,
        from_project: seed.project.clone(), to_project: logical_root.project.clone(),
        from: imported_generation.id(), to: target_generation.id(),
        excluded_names: vec![".system".into(), ".qualification-inputs".into()],
        budget: acyclic_fs::WorkBudget {
            authority_records_read: 8192, authority_records_appended: 2048,
            authority_bytes_read: 16 * 1024 * 1024, authority_bytes_written: 16 * 1024 * 1024,
            object_probes: 8192, backend_read_operations: 8192, backend_write_operations: 8192,
            durability_operations: 4096, page_reads: 8192, page_writes: 8192,
            object_bytes_read: 16 * 1024 * 1024, object_bytes_written: 16 * 1024 * 1024,
            bytes_hashed: 32 * 1024 * 1024, bytes_copied: 32 * 1024 * 1024, bytes_encoded: 16 * 1024 * 1024,
            source_bytes_read: 1024 * 1024, source_path_components: 4096, source_entries_visited: 4096,
            output_bytes: 1024 * 1024, items_examined: 16384, items_returned: 4096,
            allocation_operations: 65536, peak_allocation_bytes: 8 * 1024 * 1024, materializations: 64,
        },
        maximum_directory_entries: 64, maximum_extent_spans: 16, transfer_bytes: PAYLOAD_BYTES,
    };
    native_binding.validate()?;
    producer.config.tools.content_budget = native_binding.budget;
    producer.config.native_publications.push(native_binding);
    persist(&config_path, &producer.config).await?;
    let publication_path = root.join("inputs").join("native-publication.json");
    let publication = json!({
        "caller": logical_root.task, "caller_lease": logical_root.lease,
        "root": native_root, "operation_directory": native_operation_directory,
        "state_root": root.join("native-publication-state"),
        "operation_id": publication_operation, "source_root_identity": source_root_identity,
        "from_workspace": seed.project.storage_name()?, "from": imported_generation.id(),
        "to_workspace": logical_root.project.storage_name()?, "to": target_generation.id(),
        "excluded_names": [".system", ".qualification-inputs"],
        "concurrent_edit_path": native_root.join("concurrent-user.txt"),
        "conflicting_edit_path": native_root.join("sentinel.txt"),
        "target_file": producer.references["root_project"],
        "baseline_file": producer.references["native_import_baseline"]
    });
    persist(&publication_path, &json!({ "operation_id": publication_operation,
        "caller": logical_root.task, "caller_lease": logical_root.lease })).await?;
    let manifest = root.join("native-scenario.json");
    persist(&manifest, &json!({
        "format_version": 1,
        "bootstrap": "genuine ordinary fork-machine/provider precreation; no model dispatch; not a PTY claim",
        "authentication": "explicit credential-free trusted local AuthorityIssuer; NOT production authentication",
        "config": config_path, "tasks": identities, "callers": caller_records,
        "model_invocation_ledger": producer.config.model_invocation_ledger,
        "preparations": producer.preparations, "files": producer.references, "file_refs": file_refs,
        "leases": producer.leases, "cursor_protobuf": cursor_path,
        "native_publication": publication, "native_publication_request": publication_path,
        "limits": producer.config.limits, "session_limits": producer.config.session_limits,
        "history_limits": HistoryReadLimits { maximum_events: 256, maximum_bytes: 4 * 1024 * 1024 },
        "task_accounting": {
            "bootstrap_seed_tasks": 1, "logical_agent_tasks": 4, "executed_preparation_tasks": 4,
            "actual_retained_tasks": caller_records.len() + producer.preparations.iter()
                .filter(|preparation| preparation["executed"] == true).count(),
            "pending_preparation_tasks": 1, "pending_agent_tasks": 1,
            "ordinary_mailbox_helper_tasks": 1, "complete_graph_tasks": 12,
            "maximum_owned_reservations": 6, "maximum_parent_edges": 3,
            "preparation_tasks_are_not_agent_ancestry": true
        },
        "negative_control": negative_control,
        "negative_control_assertion": "in the separate eleven-task store, admit one ordinary mailbox helper before cross-source preparation; preparation is task eleven, and prepared child task twelve must be rejected by the original immutable session limit",
        "qualification_ready": true,
        "missing_prerequisites": [],
        "effect_assertions": {
            "creator_provenance": "root/left/right/grandchild admissions must reauthenticate real created-resource proofs",
            "isolated_forks": "project and agent-private volume identities differ across all logical callers",
            "cross_source": "pending left-caller fork selects root project only; inherits left transcript and empty left-private lineage",
            "bounded_history": "root contains 17 genuine local messages; require page16 and continuation",
            "original_lease": "resume only the exact retained original WorkLease after process death; never reclaim a new reservation"
        }
    })).await?;
    println!("native-scenario bootstrap=real-provider-precreation manifest={} inputs-ready=true; qualification remains unexecuted", manifest.display());
    Ok(manifest)
}

impl Producer {
    async fn seed(&mut self, caller: &mut Caller, label: &str, messages: usize) -> Result<()> {
        let issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, caller.binding.authority.clone());
        let writer = ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &caller.binding.scope, &caller.private, VolumeOperation::Write)?;
        let scratch = self.files.put_content(&caller.private, &writer, "scratch/caller-marker.txt",
            format!("{label} private scratch; must never be copied as child scratch\n").as_bytes(),
            "text/plain", "caller-marker.txt", PAYLOAD_BYTES,
            &IdempotencyKey::new(format!("{label}-scratch"))?).await?;
        self.references.insert(format!("{label}_scratch"), scratch);
        for index in 0..messages {
            let file = self.files.put_content(&caller.private, &writer, &format!("messages/{label}-{index}.txt"),
                format!("{label} ORIGINAL CALLER transcript marker {index}\n").as_bytes(),
                "text/plain", "transcript.txt", PAYLOAD_BYTES,
                &IdempotencyKey::new(format!("{label}-message-{index}"))?).await?;
            if index == 0 { self.references.insert(format!("{label}_transcript"), file.clone()); }
            let sequence = caller.aggregate.reducer().conversation()
                .ok_or_else(|| Error::Invalid("caller conversation was not initialized".into()))?
                .logical_revision().checked_add(1)
                .ok_or_else(|| Error::Invalid("caller conversation sequence overflow".into()))?;
            caller.aggregate.execute(Command { operation_id: OperationId::new(),
                idempotency_key: IdempotencyKey::new(format!("{label}-append-{index}"))?,
                expected_revision: caller.aggregate.reducer().revision(), scope: caller.binding.scope.clone(),
                causal_parent: None, action: Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage { id: uuid::Uuid::new_v4(), sequence,
                        kind: MessageKind::User, content: file, attachments: Default::default(),
                        reply_to: None, tool_call_id: None, extensions: Default::default() }) } }).await?;
        }
        let project_writer = ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &caller.binding.scope,
            &caller.project, VolumeOperation::Write)?;
        let before = self.files.put_content(&caller.project, &project_writer, "sentinel.txt",
            format!("{label} project baseline\n").as_bytes(), "text/plain", "sentinel.txt", PAYLOAD_BYTES,
            &IdempotencyKey::new(format!("{label}-project-baseline"))?).await?;
        let after = self.files.put_content(&caller.project, &project_writer, "sentinel.txt",
            format!("{label} project selected generation\n").as_bytes(), "text/plain", "sentinel.txt", PAYLOAD_BYTES,
            &IdempotencyKey::new(format!("{label}-project-selected"))?).await?;
        self.references.insert(format!("{label}_project_before"), before);
        self.references.insert(format!("{label}_project_after"), after);
        self.references.insert(format!("{label}_project"), self.references[&format!("{label}_project_after")].clone());
        Ok(())
    }

    async fn fork(&mut self, parent: &mut Caller, label: &str, source: Option<TaskId>, execute: bool)
        -> Result<Option<Caller>>
    {
        let agent = AgentId::new();
        let child_authority = Authority { kind: AggregateKind::Conversation, id: format!("qualification-{label}-{agent}") };
        let project = VolumeRef::new(self.config.filesystem_provider.clone(), format!("{label}-project"),
            VolumeClass::Project, VolumeOwner::Project("qualification-project".into()))?;
        let private = VolumeRef::new(self.config.filesystem_provider.clone(), format!("{label}-private"),
            VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        let result = VolumeRef::new(self.config.filesystem_provider.clone(), format!("{label}-result"),
            VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        let operation = OperationId::new();
        let preparation_operation = OperationId::new();
        let preparation_task = acyclic_harness::runtime::task_id_for_admission(preparation_operation);
        let child_operation = OperationId::new();
        let initial_prompt = format!("real isolated {label} qualification task");
        let verifier = AuthorityIssuer::new(ISSUER, TEST_KEY, parent.binding.authority.clone()).verifier(&mut None).expect("verifier copy");
        let admission = self.runtime.task_host().observe_admission(parent.task).await?;
        let context = self.runtime.task_context(parent.task, LeaseFence::from(&parent.lease.reservation)).await?;
        let source_read = if let Some(source_task) = source.filter(|task| *task != parent.task) {
            let inspection = self.runtime.inspect_source(parent.task, source_task).await?;
            let selected = inspection.project()?;
            let graph_root = self.config.project_source_root.ok_or_else(|| Error::Unsupported(
                "actual coordinator graph root source policy is not configured".into()))?;
            let policy = acyclic_harness::filesystem::FsRootProjectSourceProvider::new(
                self.files.clone(), self.config.owner.authority.clone(), graph_root,
            )?;
            let service_issuer = AuthorityIssuer::new(
                &self.config.owner.issuer, self.config.owner.key, self.config.owner.authority.clone(),
            );
            let owner = Arc::new(self.runtime.task_host().journal_owner(
                parent.task, LeaseFence::from(&parent.lease.reservation),
            ).await?.with_invocation_cancellation(&self.cancellation));
            Some(owner.authorize_project_source(&context, &service_issuer, &policy,
                source_task, operation, acyclic_harness::source_authorization::ProjectSourceSnapshot {
                    project: selected.volume.clone(), generation: selected.generation.clone(),
                    native_snapshot: None,
                }, 16 * 1024 * 1024, 16 * 1024 * 1024).await?)
        } else { None };
        let binding = recursive_fork_source(&self.runtime, self.files.clone(), &self.stream,
            &parent.aggregate, verifier, parent.binding.scope.clone(), &context,
            RecursiveSourceSnapshot {
                task: parent.task, project: parent.project.clone(),
                project_generation: self.files.resolve(&workspace_ref(self.config.filesystem_provider.clone(),
                    &parent.project.storage_name()?)?).await?.generation,
                private_volume: parent.private.clone(),
                private_generation: self.files.resolve(&workspace_ref(self.config.filesystem_provider.clone(),
                    &parent.private.storage_name()?)?).await?.generation,
            }, self.config.stream_provider.clone(), operation,
            &ForkToolArguments { id: agent, msg: initial_prompt.clone(), source },
            RecursiveForkConfig { child: child_authority.clone(), child_project: project.clone(),
                child_private: private.clone(), child_result_volume: Some(result.clone()),
                limits: self.config.limits, inherited_messages: 64,
                inherited_bytes: PAYLOAD_BYTES, inherited_references: 64,
                history_limits: HistoryReadLimits { maximum_events: 256, maximum_bytes: 4 * 1024 * 1024 } },
            source_read, &self.cancellation,
        ).await?;
        let projected_reference_grants = binding.preparer.required_reference_grants(&binding.request, None).await?;
        let checkpoint = ForkSourceConfig { caller: parent.task, owner: parent.binding.clone(),
            project: parent.project.clone(), snapshot: parent.aggregate.reducer().snapshot(usize::MAX, &mut None)?.0,
            request: binding.request.clone() };
        let checkpoint_path = self.root.join("inputs").join(format!("{label}-source.json"));
        persist(&checkpoint_path, &checkpoint).await?;
        self.config.fork_sources.push(checkpoint);
        let mut child = self.template.clone();
        child.operation_id = child_operation;
        child.parent = Some(parent.task);
        child.input = serde_json::to_value(&self.planned_commands[label]).map_err(|error| Error::Invalid(error.to_string()))?;
        // Parent-owned scratch rights never become generic child rights.
        let mut grants = vec!["operation:declare".to_owned(), "operation:observe".to_owned(),
            "operation:cancel".to_owned(), "operation:wake".to_owned(), "model:generate".to_owned(),
            "fork:publish".to_owned(), "project:merge".to_owned(),
            "task:spawn:acyclic.stock_turn@1".to_owned(), "task:spawn:acyclic.fork_preparation@1".to_owned(),
            "task:spawn:acyclic.mailbox_model@1".to_owned(), "mail:send".to_owned(), "mail:read".to_owned(),
            "interaction:open".to_owned(), "interaction:resolve".to_owned()];
        grants.extend(["tool:call:acyclic.fork".to_owned(), "tool:call:acyclic.merge".to_owned(),
            "tool:call:acyclic.read_file".to_owned(), "tool:call:acyclic.write_file".to_owned(),
            "tool:call:acyclic.edit_file".to_owned()]);
        let parent_project_read = parent.project.capability(VolumeOperation::Read)?;
        if admission.grants.contains(&parent_project_read) { grants.push(parent_project_read); }
        for file in self.planned_inputs.values() {
            let capability = file.read_capability()?;
            if admission.grants.contains(&capability) { grants.push(capability); }
        }
        for grant in &projected_reference_grants {
            if grant.reader != agent {
                return Err(Error::Unauthorized("canonical projection unexpectedly grants another reader".into()));
            }
            grants.push(grant.capability()?);
        }
        child.grants = Capabilities::new(grants);
        let fork_bytes = serde_json::to_vec(&binding.request).map_err(|error| Error::Invalid(error.to_string()))?;
        if fork_bytes.len() as u64 > PAYLOAD_BYTES {
            return Err(Error::Invalid("retained fork command exceeds fixture artifact bound".into()));
        }
        let command_file = context.stage_file_once(OperationId::new(),
            &format!(".qualification-forks/{operation}.json"), &fork_bytes,
            "application/json", "fork-request.json").await?;
        let command = WorkflowCommand { operation_id: operation, kind: FORK_PREPARE_TASK_COMMAND_KIND.into(),
            payload: command_file.clone() };
        let request = fork_preparation_request(&binding.preparer, binding.request, preparation_task,
            child, command, ForkPreparationLimits { resources: 3, work: 16 * 1024 * 1024,
                bytes: 16 * 1024 * 1024 }, None,
            vec![command_file, self.planned_commands[label].payload.clone()])?;
        let request_path = self.root.join("inputs").join(format!("{label}-prepare.json"));
        persist(&request_path, &json!({ "operation": preparation_operation, "parent": parent.task,
            "caller_lease": parent.lease, "request": request })).await?;
        let report_path = self.root.join("inputs").join(format!("{label}-report.json"));
        let preparation_lease_path = self.root.join("inputs").join(format!("{label}-preparation-lease.json"));
        self.preparations.push(json!({ "label": label, "parent": parent.task, "operation": preparation_operation,
            "task": preparation_task, "child_task_operation": child_operation, "child_agent": agent,
            "request": request_path, "report": if execute { Some(report_path.clone()) } else { None },
            "projected_reference_grants": projected_reference_grants,
            "source_snapshot": checkpoint_path, "executed": execute,
            "original_lease": if execute { Some(preparation_lease_path.clone()) } else { None } }));
        if !execute {
            let child_issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, child_authority.clone());
            let mut child_grants = request.child.grants.iter().map(str::to_owned).collect::<Vec<_>>();
            child_grants.extend(["conversation:bind".into(), "conversation:append".into()]);
            let child_owner = AuthorityBinding {
                authority: child_authority, issuer: ISSUER.into(), key: TEST_KEY,
                scope: child_issuer.root_for_agent(agent, "explicit-trusted-pending-child", Capabilities::new(child_grants)),
            };
            self.config.observed_authorities.push(child_owner.clone());
            self.config.tools.forks.push(NativeToolFork { caller: parent.task, child: child_owner,
                project, private_volume: private, admission: request.child.clone(),
                limits: ForkPreparationLimits { resources: 3, work: 16 * 1024 * 1024, bytes: 16 * 1024 * 1024 },
                result_volume: Some(result) });
            return Ok(None);
        }
        let definition: Arc<TaskDefinition<ResourcePreparationRequest, ForkReport>> =
            self.runtime.harness().task("acyclic.fork_preparation@1")?;
        let admitted = admitted_task(context.admit(preparation_operation, &definition, request.clone()).await?)?;
        if admitted != preparation_task { return Err(Error::Conflict("ordinary preparation task identity differs".into())); }
        let preparation_lease = claim(&self.runtime, preparation_task, &self.worker).await?;
        persist(&preparation_lease_path, &preparation_lease).await?;
        let commands = self.runtime.commands().with_fork_preparer(binding.preparer);
        let attempt = self.runtime.resume_task(preparation_lease.clone(), &commands, 16).await;
        if let acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { error, .. } = attempt {
            return Err(error);
        }
        let report: ForkReport = match self.runtime.task_host().outcome(preparation_task).await? {
            Some(Outcome::Succeeded(value)) => serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?,
            Some(Outcome::Failed { message }) => return Err(Error::Conflict(format!("real fork {label} failed: {message}"))),
            _ => return Err(Error::Unsupported(format!("real fork {label} did not authoritatively complete; no synthetic report is allowed"))),
        };
        let child_issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, child_authority.clone());
        let mut signed_grants = request.child.grants.iter().map(str::to_owned).collect::<Vec<_>>();
        signed_grants.push("conversation:bind".into());
        signed_grants.push("conversation:append".into());
        // Capture cannot widen the already admitted exact child rights.
        for grant in &report.reference_grants {
            if !request.child.grants.contains(&grant.capability()?) {
                return Err(Error::Unauthorized("capture requires an unadmitted original reference grant".into()));
            }
        }
        let owner = AuthorityBinding { authority: child_authority, issuer: ISSUER.into(), key: TEST_KEY,
            scope: child_issuer.root_for_agent(agent, "explicit-trusted-created-child", Capabilities::new(signed_grants)) };
        let mut child_aggregate = aggregate(&self.stream, self.files.clone(), &owner, &self.config).await?;
        let seed = child_aggregate.spawn_from_report(&mut parent.aggregate, report.clone(),
            parent.binding.scope.clone(), owner.scope.clone()).await?;
        let child_task = match self.runtime.task_host().admit_owned(request.child.clone(),
            LeaseFence::from(&parent.lease.reservation)).await? {
            Admission::Accepted(task) => task,
            Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => return Err(Error::Indeterminate(operation_id)),
        };
        let child_lease = claim(&self.runtime, child_task, &self.worker).await?;
        let child_lease_path = self.root.join("inputs").join(format!("{label}-original-lease.json"));
        persist(&child_lease_path, &child_lease).await?;
        self.leases.insert(label.to_owned(), child_lease_path);
        persist(&report_path, &json!({ "report": report, "seed": seed,
            "child": self.runtime.task_host().observe_admission(child_task).await?,
            "parent_lease": parent.lease, "preparation_lease": preparation_lease })).await?;
        self.config.observed_authorities.push(owner.clone());
        println!("native-scenario real-fork label={label} parent={} preparation={preparation_task} child={child_task}", parent.task);
        Ok(Some(Caller { task: child_task, binding: owner, project, private, result: Some(result),
            aggregate: child_aggregate, lease: child_lease }))
    }
}

async fn aggregate(stream: &StreamClient<LocalStream>, files: Arc<Files>,
    owner: &AuthorityBinding, config: &Config) -> Result<StreamAggregate<LocalStream>>
{
    let verifier = AuthorityIssuer::new(&owner.issuer, owner.key, owner.authority.clone()).verifier(&mut None).expect("verifier copy");
    let content = Arc::new(FilesystemContentVerifier::new(files.clone(), verifier.clone(), owner.scope.clone(), PAYLOAD_BYTES)?);
    let forks = Arc::new(CompositeForkVerifier::new(vec![
        Arc::new(FilesystemForkVerifier::new(files.clone(), PAYLOAD_BYTES)?),
        Arc::new(StreamHistoryForkVerifier::new(config.stream_provider.clone())?),
    ])?);
    StreamAggregate::open(stream, owner.authority.clone(), verifier, SchemaRegistry::new()).await?
        .with_content_verifier(content).with_fork_verifier(forks)
        .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(files)))
        .with_limits(config.limits)
}

async fn claim(runtime: &Runtime, task: TaskId, worker: &Worker) -> Result<WorkLease> {
    let admission = runtime.task_host().observe_admission(task).await?;
    let lease = match runtime.task_host().pull_operation(worker, admission.operation_id).await? {
        WorkPull::Claimed(lease) => lease,
        WorkPull::Idle => return Err(Error::Conflict(format!("qualification task {task} has no owner lease"))),
        WorkPull::Unresolved { error, .. } => return Err(error),
    };
    runtime.task_host().start_task(&lease).await?;
    Ok(lease)
}

fn admitted_task<T: Send + 'static>(admission: Admission<RuntimeTask<T>>) -> Result<TaskId> {
    match admission {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => Ok(task_id),
        Admission::Accepted(RuntimeTask::Live(_)) => Err(Error::Unsupported("qualification requires genuine durable admission".into())),
        Admission::Rejected { reason } => Err(Error::Conflict(reason)),
        Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
    }
}

async fn persist(path: &Path, value: &impl Serialize) -> Result<()> {
    tokio::fs::write(path, serde_json::to_vec_pretty(value).map_err(|error| Error::Invalid(error.to_string()))?).await.map_err(|error| Error::Storage(error.to_string()))
}
