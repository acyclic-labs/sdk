
use super::*;
use crate::pi_fork::tests::Passive;
use acyclic_harness::{
    AgentId, Capabilities, TaskId,
    conversation::{ContentPublisher, ContentResidencyVerifier, Limits, VolumeClass, VolumeOwner},
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, EffectStatus, SchemaRegistry},
    distributed::{WorkPull, Worker},
    executor::ExecutionJournal,
    effect_host::ConversationEffectHost,
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal, FilesystemTaskRuntime,
        NativePublicationReservation, NativeProcessAuthority, NativeProcessProvider, workspace_ref},
    interaction::{Interaction, InteractionResponse},
    resources::ProviderRef,
    runtime::{RuntimeScope, TaskAdmissionRecord, TaskDefinition, TaskRegistry},
    scheduler::{LeaseFence, SessionLimits},
    store::{HistoryReadLimits, StreamAggregate},
    tool::ToolRegistry,
    workflow::{MachineIdentity, MachineRegistry},
};
use acyclic_stream::{LocalStream, LocalStreamLimits};

type Files = FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
type Runtime = FilesystemTaskRuntime<LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
type View = NativeVolumeView<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
type Journal = FilesystemExecutionJournal<LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

type Reservation = NativePublicationReservation<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
struct FixtureRoot {
    directory: Option<tempfile::TempDir>,
    publication_pending: bool,
}
impl FixtureRoot {
    fn path(&self) -> &Path { self.directory.as_ref().expect("fixture owns its physical root").path() }
}
impl Drop for FixtureRoot {
    fn drop(&mut self) {
        if self.publication_pending {
            if let Some(directory) = self.directory.take() {
                directory.keep();
            }
        }
    }
}

struct Fixture {
    _directory: FixtureRoot,
    files: Arc<Files>,
    stream: std::sync::Arc<LocalStream>,
    issuer: AuthorityIssuer,
    scope: Scope,
    grants: Capabilities,
    results: Arc<FilesystemContentPublisher<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    content: Arc<FilesystemContentVerifier<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    approvals: Arc<Journal>,
    runtime: Runtime,
    owner: Arc<TaskJournalOwner<LocalStream>>,
    context: Arc<NativeTaskContext>,
    fence: LeaseFence,
    binding: PiTaskBinding,
    invocation: PiNativeInvocation,
    view: Arc<View>,
    work: NativeMaterializationWork,
    command: OperationId,
    approval: InteractionId,
    plan: (TaskEffectPlan, acyclic_fs::NativeAllocation),
    results_volume: VolumeRef,
    reservation: Option<Reservation>,
}

async fn open_runtime(stream: std::sync::Arc<LocalStream>, files: Arc<Files>, volume: VolumeRef,
    issuer: &AuthorityIssuer, scope: &Scope, grants: Capabilities)
    -> Result<(Runtime, acyclic_harness::registry::ComponentIdentity, MachineIdentity)> {
    let machine = Arc::new(Passive { identity: MachineIdentity { name: "pi-native-provider-qualification".into(), version: "1".into(), digest: [20; 32] }, schema: json!({"type":"integer"}) });
    let definition = TaskDefinition::<Value, Value>::resumable(machine.clone(), json!({"type":"integer"}), json!({"type":"object"}))?;
    let identity = definition.identity().clone();
    let machine_identity = machine.identity.clone();
    let mut tasks = TaskRegistry::default(); tasks.register(definition)?;
    let mut machines = MachineRegistry::default(); machines.register(machine)?;
    let runtime = FilesystemTaskRuntime::open(stream, files, volume, issuer.verifier(), scope.clone(), RuntimeScope::new(grants, Limits::default())?,
        tasks, machines, ToolRegistry::default(), SessionLimits { active_tasks: 1, total_tasks: 1, depth: 0, model_steps: 1 }, 1, 1_048_576).await?;
    Ok((runtime, identity, machine_identity))
}

fn required_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(std::env::var_os(name).ok_or_else(|| invalid(&format!("required pinned Pi lane environment is missing: {name}")))?);
    if !path.is_absolute() { return Err(invalid(&format!("{name} must select an absolute pinned artifact"))); }
    Ok(path)
}

impl Fixture {
    async fn new(delay_ms: u32) -> Result<Self> {
        // Actual shared services precede the finite original operation span.
        acyclic_native_runtime::initialize_native_io_services()
            .map_err(|cause| native_error(acyclic_fs::NativeTreeMaterializationError::Io(cause)))?;
        acyclic_fs::initialize_native_mount_services()
            .map_err(|cause| native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?;
        let mut directory = FixtureRoot { directory: Some(tempfile::tempdir().map_err(io_error)?), publication_pending: false };
        let node = required_path("ACYCLIC_PI_TEST_NODE")?;
        let cli = required_path("ACYCLIC_PI_TEST_CLI")?;
        let package = cli.parent().and_then(Path::parent).and_then(Path::parent)
            .ok_or_else(|| invalid("Pi CLI must be the pinned package dist/bundle/cli.js"))?.join("package.json");
        let package: Value = serde_json::from_slice(&std::fs::read(package).map_err(io_error)?).map_err(decode_error)?;
        if package.get("name").and_then(Value::as_str) != Some("@earendil-works/pi-coding-agent")
            || package.get("version").and_then(Value::as_str) != Some(PI_NATIVE_VERSION) {
            return Err(invalid("native provider qualification did not select Pi 1.1.0"));
        }
        let provider = ProviderRef::new("pi-native-provider-test", "filesystem", "1")?;
        let agent = AgentId::from_bytes([21; 16]);
        let task = TaskId::from_bytes([22; 16]);
        let scratch = VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        let results_volume = VolumeRef::new(provider.clone(), "results", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent))?;
        let command = OperationId::from_bytes([23; 16]);
        let approval = InteractionId::from_bytes([24; 16]);
        let grants = Capabilities::new([
            "operation:declare".into(), "operation:observe".into(), "operation:cancel".into(),
            "task:spawn:pi-native-provider-qualification@1".into(), "effect:plan".into(), "effect:run".into(),
            format!("effect:provider:{PI_NATIVE_PROVIDER}"), "interaction:open".into(), "interaction:resolve".into(),
            format!("interaction:respond:{approval}"), "interaction:route".into(), "conversation:bind".into(),
            scratch.capability(VolumeOperation::Read)?, scratch.capability(VolumeOperation::Write)?,
            results_volume.capability(VolumeOperation::Read)?, results_volume.capability(VolumeOperation::Write)?,
        ]);
        let issuer = AuthorityIssuer::new("pi-native-provider-test", [25; 32], Authority { kind: AggregateKind::Conversation, id: "pi-native-provider-test".into() });
        let scope = issuer.issue("owner", &grants, Some(agent), None, &mut None).expect("scope proof");
        let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.path().join("filesystem"))).await.map_err(|error| invalid(&error.to_string()))?;
        let files = Arc::new(FilesystemHost::new(filesystem.clone(), provider)?);
        files.create_volume(&scratch).await?; files.create_volume(&results_volume).await?;
        let stream = Arc::new(LocalStream::open(directory.path().join("stream"), LocalStreamLimits::default()).await.map_err(|error| invalid(&error.to_string()))?);
        let (runtime, identity, machine) = open_runtime(stream.clone(), files.clone(), results_volume.clone(), &issuer, &scope, grants.clone()).await?;
        runtime.task_host().admit(TaskAdmissionRecord { operation_id: OperationId::from_bytes(task.into_bytes()), task: identity, machine,
            input: json!(0), input_schema: json!({"type":"integer"}), output_schema: json!({"type":"object"}), parent: None,
            grants: grants.clone(), limits: Limits::default(), run_limits: acyclic_harness::runtime::TaskRunLimits::default(), policy: None, extensions: None, execution: None }).await?;
        let WorkPull::Claimed(lease) = runtime.task_host().pull_work(&Worker { id: "pi-native-worker".into(), available: Default::default(), labels: BTreeMap::new() }).await? else {
            return Err(invalid("actual native task lease was not admitted"));
        };
        runtime.task_host().start_task(&lease).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let mut aggregate = StreamAggregate::open(&stream, issuer.verifier().audience().clone(), issuer.verifier(), SchemaRegistry::new()).await?;
        aggregate.execute(Command { operation_id: OperationId::from_bytes([26; 16]), idempotency_key: IdempotencyKey::new("pi-native-bind")?,
            expected_revision: 0, scope: scope.clone(), causal_parent: None, action: Action::BindConversation { agent } }).await?;
        let grant = ContentGrant::verify(&issuer.verifier(), &scope, &scratch, VolumeOperation::Write)?;
        let cancellation = CancellationToken::new();
        let budget = super::super::native_root_source::budget();
        let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let initial = files.resolve(&workspace_ref(scratch.provider().clone(), &scratch.storage_name()?)?).await?;
        let initial_id = filesystem.open_workspace_with_native_work(&scratch.storage_name()?, &work, &cancellation).await
            .map_err(|(error, _)| invalid(&error.to_string()))?.id().into_bytes();
        let native = directory.path().join("native");
        let operation_directory = directory.path().join("preparation");
        for path in [&native, &operation_directory, &operation_directory.join("target"), &operation_directory.join("backup")] {
            std::fs::create_dir(path).map_err(io_error)?;
        }
        let owner = runtime.task_host().journal_owner_with_native_work(task, &fence, &work).await?
            .into_native_shared(&work)?;
        let context = runtime.task_context_with_native_work(task, &fence, &work).await?;
        let allocation = NativePreparationRoot::witness_with_native_work(&native, &operation_directory, &work)?.await?;
        let cwd = native.join("scratch");
        let source_entries = json!([{"type":"message","id":"original","parentId":null,"timestamp":"2026-10-09T00:00:00.000Z",
            "message":{"role":"user","content":"original native instruction","timestamp":1}}]);
        let source_json = source_entries.to_string();
        let source = PiNativeSourceCut { mode: PiNativeSourceMode::CallerTool, parent_session_id: "parent".into(), parent_session_file: None, parent_cwd: native.to_str().unwrap().into(),
            source_leaf_id: Some("spawn".into()), owner_entry_id: Some("spawn".into()), parent_leaf_id: Some("original".into()), captured_at: "2026-10-09T00:00:01.000Z".into(),
            sha256: hex::encode(Sha256::digest(source_json.as_bytes())), entries_json: source_json };
        let bytes = format!("{}\n{}\n", json!({"type":"session","version":3,"id":"child","timestamp":source.captured_at,"cwd":cwd}), source_entries[0]);
        source.validate_session("child", &cwd, bytes.as_bytes(), 16, 65_536, &work)?;
        files.put_content_with_native_work(&scratch, &grant, "child.jsonl", bytes.as_bytes(),
            "application/x-ndjson", "Pi child session", 65_536,
            &IdempotencyKey::new("pi-native-child-session")?, &work).await?;
        let selected = files.resolve(&workspace_ref(scratch.provider().clone(), &scratch.storage_name()?)?).await?;
        let workspace = filesystem.open_workspace_with_native_work(&scratch.storage_name()?, &work, &cancellation).await
            .map_err(|(error, _)| invalid(&error.to_string()))?;
        let generation = acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(selected.generation.as_resource().key().try_into()
            .map_err(|_| invalid("actual selected filesystem generation is not a digest"))?));
        directory.publication_pending = true;
        let reservation = owner.native_view_owner().reserve_publication(&workspace, &scratch, generation,
            acyclic_fs::OperationId::from_bytes(command.into_bytes()), &work, &cancellation, &CancellationToken::new()).await?;
        let namespace = NativeVolumeNamespace::MaterializedDirectory;
        let scratch_verifier = issuer.verifier_with_native_work(&work)?;
        let callback_guard = context.volume_callback_guard_with_native_work(&owner, &work).await?;
        let callback_guards = [callback_guard];
        let view = NativeVolumeView::prepare_with_native_work(owner.as_ref(), &callback_guards, &allocation.0, &NativeViewOptions { root: native.clone(), maximum_volumes: 1, work_per_volume: budget },
            &[BorrowedNativeVolumeBinding { host: files.as_ref(), volume: &scratch, generation: &selected.generation, path: "scratch", writable: true,
                verifier: &scratch_verifier.0, scope: &scope, publication: reservation.permit(), namespace: &namespace }], &work)?.await?
            .into_arc_with_native_work(&work)?;
        let result_creation = files.resolve(&workspace_ref(results_volume.provider().clone(), &results_volume.storage_name()?)?).await?;
        let result_workspace = filesystem.open_workspace_with_native_work(&results_volume.storage_name()?, &work, &cancellation).await
            .map_err(|(error, _)| invalid(&error.to_string()))?.id().into_bytes();
        let binding = PiTaskBinding { native_session_id: "child".into(), task_id: task, agent_id: agent, project_volumes: BTreeMap::new(),
            scratch: super::super::PiScratchBinding { volume: scratch, allocation: PiScratchAllocation::Created(super::super::PiScratchCreation {
                workspace_id: initial_id, generation: initial.generation,
            }) },
            results: Some(super::super::PiScratchBinding { volume: results_volume.clone(),
                allocation: PiScratchAllocation::Created(super::super::PiScratchCreation {
                    workspace_id: result_workspace, generation: result_creation.generation,
                }) }),
        };
        let provider_directory = required_path("ACYCLIC_PI_TEST_PROVIDER_DIR")?;
        let provider_module = provider_directory.join(format!("acyclic-native-{}.mjs", uuid::Uuid::new_v4()));
        let package_probe = format!("import {{ VERSION }} from '@earendil-works/pi-coding-agent';\nimport {{ fauxProvider, fauxAssistantMessage }} from '@earendil-works/pi-ai';\nimport {{ appendFileSync }} from 'node:fs';\nexport default function(pi) {{ if (VERSION !== '1.1.0') throw new Error('unpinned Pi'); const provider = fauxProvider({{provider:'pi-native-test',models:[{{id:'probe'}}]}}); provider.setResponses([context => fauxAssistantMessage(JSON.stringify({{messages:context.messages}}))]); pi.registerProvider(provider.provider); pi.on('before_agent_start', async () => {{ appendFileSync({}, 'started\\n'); await new Promise(resolve => setTimeout(resolve,{delay_ms})); }}); }}\n", serde_json::to_string(&cwd.join("launches.txt")).unwrap());
        std::fs::write(&provider_module, package_probe).map_err(io_error)?;
        let mut environment = BTreeMap::from([("HOME".into(), directory.path().to_str().unwrap().into()), ("USERPROFILE".into(), directory.path().to_str().unwrap().into()),
            ("PI_CODING_AGENT_DIR".into(), directory.path().join("agent").to_str().unwrap().into())]);
        for name in ["SystemRoot", "TEMP", "TMP"] { if let Ok(value) = std::env::var(name) { environment.insert(name.into(), value); } }
        let capture = capture_limits(Limits::default(), 1_048_576)?;
        let invocation = PiNativeInvocation { version: PI_NATIVE_VERSION.into(), executable: node, executable_prefix: vec![cli.to_str().unwrap().into()],
            extension: provider_module, provider_extension: None, session_file: cwd.join("child.jsonl"), cwd, provider: "pi-native-test".into(), model: "probe".into(),
            prompt: "actual native child instruction".into(), builtin_tools: Vec::new(), environment, timeout_ms: capture.timeout_ms,
            control_timeout_ms: capture.control_timeout_ms, cancellation_poll_ms: capture.cancellation_poll_ms,
            maximum_output_bytes: capture.maximum_output_bytes, maximum_result_bytes: capture.maximum_result_bytes };
        let request = invocation.request_with_work(&binding, view.manifest(), &work)?;
        let results = Arc::new(FilesystemContentPublisher::new(files.clone(), results_volume.clone(), &issuer.verifier(), &scope, 1_048_576)?);
        let content = Arc::new(FilesystemContentVerifier::new(files.clone(), issuer.verifier(), scope.clone(), 1_048_576)?);
        let bytes = crate::pi_root_provider::guarded_pi_bytes(&request.0, 1_048_576, &work)?;
        let mut request_file = results.stage_once_with_native_work(
            native_artifact_operation(command, "request", &work)?,
            &format!("pi-native/{command}/request.json"), &bytes.0, "application/json", "request", &work)?.await?;
        let bytes = crate::pi_root_provider::guarded_pi_bytes(&result_schema()?, 1_048_576, &work)?;
        let mut schema = results.stage_once_with_native_work(
            native_artifact_operation(command, "schema", &work)?,
            &format!("pi-native/{command}/schema.json"), &bytes.0, "application/schema+json", "schema", &work)?.await?;
        request_file.1.absorb(&mut schema.1).map_err(native_error)?;
        let plan = (TaskEffectPlan { provider: PI_NATIVE_PROVIDER.into(), effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
            guarantee: EffectGuarantee::AtMostOnce, request: request_file.0, result_schema: schema.0 }, request_file.1);
        let approvals = Arc::new(FilesystemExecutionJournal::new(stream.clone(), files.clone(), results_volume.clone(), issuer.verifier(), scope.clone(), 1_048_576)?);
        approvals.open_interaction(approval, Interaction::approval("Run pinned native Pi", command, request.0.approval_digest_with_native_work(task, command, &work)?)?).await?;
        approvals.resolve_interaction(approval, InteractionResponse::Approval { approved: true, reason: None }, &scope).await?;
        Ok(Self { _directory: directory, files, stream, issuer, scope, grants, results, content, approvals, runtime, owner, context, fence, binding, invocation, view, work, command, approval, plan, results_volume,
            reservation: Some(reservation) })
    }

    fn authority(&self, work: &NativeMaterializationWork) -> Result<NativeProcessAuthority> {
        let verifier = self.issuer.verifier_with_native_work(work)?;
        Ok(NativeProcessAuthority { verifier: verifier.0, history_limits: HistoryReadLimits { maximum_events: 512, maximum_bytes: 16 * 1024 * 1024 },
            content: self.content.clone(), result_content: self.content.clone(), results: self.results.clone(), approvals: self.approvals.clone(),
            authority_custody: Some(verifier.1) })
    }
    async fn provider(&self) -> Result<Arc<NativeProcessProvider<LocalStream>>> {
        NativeProcessProvider::new(self.owner.clone(), self.command, &self.plan.0, self.view.clone(),
            self.approval, self.authority(&self.work)?, &self.work)?.await?.into_arc_with_native_work(&self.work)
    }
    fn effects(&self, provider: &Arc<NativeProcessProvider<LocalStream>>, work: &NativeMaterializationWork) -> Result<ConversationEffectHost<LocalStream>> {
        let mut registry = acyclic_harness::effects::EffectRegistry::new_with_native_work(work)?
            .with_result_resolver_with_native_work(self.content.clone(), work)?;
        registry.register_with_native_work(provider.clone(), work)?;
        let audience_id = work.display("pi-native-provider-test").map_err(native_error)?;
        let audience = (Authority { kind: AggregateKind::Conversation, id: audience_id.0 }, audience_id.1);
        let issuer_id = work.display("pi-native-provider-test").map_err(native_error)?;
        let mut issuer_audience_id = work.display("pi-native-provider-test").map_err(native_error)?;
        let mut issuer = (AuthorityIssuer::new(issuer_id.0, [25; 32],
            Authority { kind: AggregateKind::Conversation, id: issuer_audience_id.0 }), issuer_id.1);
        issuer.1.absorb(&mut issuer_audience_id.1).map_err(native_error)?;
        let mut scope = issuer.0.root_for_agent_with_native_work(self.binding.agent_id, "owner", &self.grants, work)?;
        let mut authorization = ((audience.0, issuer.0, scope.0), audience.1);
        authorization.1.absorb(&mut issuer.1).map_err(native_error)?;
        authorization.1.absorb(&mut scope.1).map_err(native_error)?;
        let ((audience, issuer, scope), custody) = authorization;
        ConversationEffectHost::new_with_native_work(self.stream.clone(), audience, issuer, scope,
            SchemaRegistry::new(), self.content.clone(), registry, custody, work)
    }
    async fn wait_started(&self) -> Result<()> {
        for _ in 0..6000 {
            if self.invocation.cwd.join("launches.txt").try_exists().map_err(io_error)? { return Ok(()); }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Err(invalid("real pinned Pi process did not reach its before-agent boundary"))
    }

    async fn release_publication(&mut self) -> Result<()> {
        let cancellation = CancellationToken::new();
        let meter = self.work.clone();
        self.owner.verify_with_native_work(true, &meter).await?;
        if let Some(reservation) = self.reservation.take() {
            reservation.into_cleanup().release(&self.owner.native_view_owner(), &meter, &cancellation).await?;
            self._directory.publication_pending = false;
        }
        if self._directory.publication_pending {
            return Err(Error::Indeterminate(self.command));
        }
        Ok(())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if !self._directory.publication_pending {
            let _ = std::fs::remove_file(&self.invocation.extension);
        }
    }
}
fn io_error(error: std::io::Error) -> Error { Error::Storage(error.to_string()) }

#[tokio::test]
#[ignore = "requires pinned Pi CLI and native qualification lane"]
async fn required_pinned_pi_native_effect_lost_ack_and_disk_runtime_reopen_do_not_relaunch() -> Result<()> {
    let mut fixture = Fixture::new(250).await?;
    let provider = fixture.provider().await?;
    let effects = Arc::new(fixture.effects(&provider, &fixture.work)?);
    let launched_effects = effects.clone();
    let plan = fixture.plan.0.clone();
    let owner = fixture.owner.clone();
    let command = fixture.command;
    let work = fixture.work.clone();
    let launched = tokio::spawn(async move { launched_effects.run_task_effect_with_native_work(&owner, command, &plan, &work).await });
    fixture.wait_started().await?;
    launched.abort(); // Caller ACK loss; the actual producer supervisor survives.
    let mut status = effects.reconcile_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan.0, &fixture.work).await?;
    for _ in 0..6000 {
        if matches!(status.0, EffectStatus::Succeeded { .. } | EffectStatus::Failed { .. }) { break; }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        status = effects.reconcile_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan.0, &fixture.work).await?;
    }
    assert!(matches!(status.0, EffectStatus::Succeeded { .. }), "real native Pi failed: {:?}", status.0);
    let receipt = provider.receipt(provider.original_attempt(), &fixture.work, None).await?.ok_or_else(|| invalid("native receipt absent"))?;
    assert!(receipt.observed);
    drop(effects); drop(provider);
    let (runtime, _, _) = open_runtime(fixture.stream.clone(), fixture.files.clone(), fixture.results_volume.clone(), &fixture.issuer, &fixture.scope, fixture.grants.clone()).await?;
    let recovery_cancellation = CancellationToken::new();
    let recovery_work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &recovery_cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let owner = runtime.task_host().journal_owner_with_native_work(
        fixture.binding.task_id, &fixture.fence, &recovery_work).await?.into_native_shared(&recovery_work)?;
    let recovered = NativeProcessProvider::recover(owner.clone(), fixture.command, &fixture.plan.0, fixture.approval, fixture.authority(&recovery_work)?, &recovery_work)
        .await?.into_arc_with_native_work(&recovery_work)?;
    let effects = fixture.effects(&recovered, &recovery_work)?;
    let reconciled = effects.reconcile_task_effect_with_native_work(&owner, fixture.command, &fixture.plan.0, &recovery_work).await?;
    assert_eq!(reconciled.0, status.0);
    let settled = effects.run_task_effect_with_native_work(&owner, fixture.command, &fixture.plan.0, &recovery_work).await?;
    assert_eq!(settled.0, status.0, "recovery must settle original effect rather than dispatch");
    let recovered_receipt = recovered.receipt(recovered.original_attempt(), &recovery_work, None).await?
        .ok_or_else(|| invalid("recovered native receipt absent"))?;
    assert_eq!(recovered_receipt.effect, receipt.effect);
    assert_eq!(recovered_receipt.work, receipt.work);
    assert_eq!(recovered_receipt.task, receipt.task);
    assert_eq!(recovered_receipt.command, receipt.command);
    assert_eq!(std::fs::read_to_string(fixture.invocation.cwd.join("launches.txt")).map_err(io_error)?, "started\n");
    let EffectStatus::Succeeded { result: result_ref } = &recovered_receipt.effect.status else {
        return Err(invalid("native output absent from authentic successful receipt"));
    };
    let bytes = fixture.context.context().read_file_with_native_work(result_ref, &recovery_work).await?;
    let result = acyclic_harness::filesystem::NativeProcessResult::decode_with_native_work(bytes.as_ref(), &recovery_work)?;
    assert!(result.0.success); assert!(result.0.stop.is_none());
    let stdout = std::str::from_utf8(&result.0.stdout).map_err(|error| invalid(&error.to_string()))?;
    assert!(stdout.contains("original native instruction")); assert!(stdout.contains("actual native child instruction"));
    let drain_proof = recovered.drain_with_native_work(recovered.original_attempt(),
        &fixture.view, &recovery_work, &recovery_cancellation)?.await?;
    fixture.release_publication().await?;
    drop(drain_proof);
    Ok(())
}

#[tokio::test]
#[ignore = "requires pinned Pi CLI and native qualification lane"]
async fn required_pinned_pi_native_cancellation_records_actual_cleanup_without_publication_or_relaunch() -> Result<()> {
    let mut fixture = Fixture::new(60_000).await?;
    let provider = fixture.provider().await?;
    let effects = Arc::new(fixture.effects(&provider, &fixture.work)?);
    let launched_effects = effects.clone();
    let plan = fixture.plan.0.clone();
    let owner = fixture.owner.clone();
    let command = fixture.command;
    let work = fixture.work.clone();
    let launched = tokio::spawn(async move { launched_effects.run_task_effect_with_native_work(&owner, command, &plan, &work).await });
    fixture.wait_started().await?;
    fixture.runtime.task_host().cancel_with_native_work(fixture.binding.task_id, &fixture.work).await?;
    let _ = effects.reconcile_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan.0, &fixture.work).await?;
    let _ = launched.await.map_err(|error| invalid(&error.to_string()))??;
    let receipt = provider.receipt(provider.original_attempt(), &fixture.work, None).await?.ok_or_else(|| invalid("cancelled native receipt absent"))?;
    assert!(receipt.observed); assert!(matches!(receipt.effect.status, EffectStatus::Indeterminate));
    let output = receipt.diagnostic.as_ref().ok_or_else(|| invalid("native cleanup diagnostic absent from authentic receipt"))?;
    let bytes = fixture.context.context().read_file_with_native_work(output, &fixture.work).await?;
    let result = acyclic_harness::filesystem::NativeProcessResult::decode_with_native_work(bytes.as_ref(), &fixture.work)?;
    let stop = result.0.stop.as_ref().ok_or_else(|| invalid("native cancellation lost stop observation"))?;
    assert_eq!(stop.kind, acyclic_harness::filesystem::NativeProcessStopKind::ControlStopped);
    assert!(stop.cleanup_completed, "native cleanup must be observed, not inferred from the session path");
    let cleanup_cancellation = CancellationToken::new();
    let cleanup_work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &cleanup_cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let drain_proof = provider.drain_with_native_work(provider.original_attempt(),
        &fixture.view, &cleanup_work, &cleanup_cancellation)?.await?;
    assert!(fixture.owner.child_scratch_creation_proof().is_err(), "an admitted root's named volume grants cannot fabricate a created-child scratch disposal proof");
    let recovery_cancellation = CancellationToken::new();
    let recovery_work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &recovery_cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let recovered = NativeProcessProvider::recover(fixture.owner.clone(), fixture.command, &fixture.plan.0, fixture.approval, fixture.authority(&recovery_work)?, &recovery_work)
        .await?.into_arc_with_native_work(&recovery_work)?;
    let effects = fixture.effects(&recovered, &recovery_work)?;
    let reconciled = effects.reconcile_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan.0, &recovery_work).await?;
    assert!(matches!(reconciled.0, EffectStatus::Indeterminate));
    assert_eq!(std::fs::read_to_string(fixture.invocation.cwd.join("launches.txt")).map_err(io_error)?, "started\n");
    fixture.release_publication().await?;
    drop(drain_proof);
    Ok(())
}

#[tokio::test]
#[ignore = "requires pinned Pi CLI and native qualification lane"]
async fn required_pinned_pi_native_warm_supervisor_stop_drains_without_cancelling_task_or_relaunching() -> Result<()> {
    let mut fixture = Fixture::new(60_000).await?;
    let provider = fixture.provider().await?;
    let effects = Arc::new(fixture.effects(&provider, &fixture.work)?);
    let launched_effects = effects.clone();
    let plan = fixture.plan.0.clone();
    let owner = fixture.owner.clone();
    let command = fixture.command;
    let work = fixture.work.clone();
    let launched = tokio::spawn(async move { launched_effects.run_task_effect_with_native_work(&owner, command, &plan, &work).await });
    fixture.wait_started().await?;
    let cleanup_cancellation = CancellationToken::new();
    let work = fixture.work.clone();
    provider.signal_stop(provider.original_attempt(), &work, &cleanup_cancellation).await?;
    let observed = provider.request_stop(provider.original_attempt(), &work, &cleanup_cancellation).await?;
    let _ = launched.await.map_err(|error| invalid(&error.to_string()))??;
    assert!(observed.observed);
    assert_eq!(observed.task, fixture.binding.task_id);
    assert_eq!(observed.command, fixture.command);
    assert!(matches!(observed.effect.status, EffectStatus::Indeterminate));
    let original_work = observed.work;
    fixture.owner.verify_with_native_work(false, &work).await?;
    assert!(!<View as NativeProcessView<LocalStream>>::operation_cancellation(fixture.view.as_ref()).is_cancelled(),
        "stopping a warm native process must not retire the original mounted view");
    let diagnostic = observed.diagnostic.as_ref().ok_or_else(|| invalid("actual stop lacks its cleanup diagnostic"))?;
    let bytes = fixture.context.context().read_file_with_native_work(diagnostic, &work).await?;
    let result = acyclic_harness::filesystem::NativeProcessResult::decode_with_native_work(bytes.as_ref(), &work)?;
    let stop = result.0.stop.as_ref().ok_or_else(|| invalid("actual warm stop lacks its process-tree observation"))?;
    assert_eq!(stop.kind, acyclic_harness::filesystem::NativeProcessStopKind::ControlStopped);
    assert!(stop.cleanup_completed, "signal acknowledgement alone is not process-tree and stdio drainage");
    let recovered = NativeProcessProvider::recover(fixture.owner.clone(), fixture.command, &fixture.plan.0, fixture.approval, fixture.authority(&work)?, &work)
        .await?.into_arc_with_native_work(&work)?;
    assert!(matches!(recovered.signal_stop(recovered.original_attempt(), &work, &cleanup_cancellation).await, Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unsupported, .. })),
        "cold recovery has no original ephemeral supervisor to control");
    let observation = NativeMaterializationWork::new(super::super::native_root_source::budget(), &cleanup_cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let receipt = recovered.receipt(recovered.original_attempt(), &observation, None).await?
        .ok_or_else(|| invalid("bounded recovery lost the original native receipt"))?;
    assert_eq!(receipt.work, original_work, "readonly recovery must not recharge or replace the original capture/publication work");
    assert_eq!(observation.receipt().backend_write_operations, 0, "receipt observation must not append or dispatch");
    assert_eq!(std::fs::read_to_string(fixture.invocation.cwd.join("launches.txt")).map_err(io_error)?, "started\n");
    let drain_work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &cleanup_cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let proof = provider.drain_with_native_work(provider.original_attempt(), &fixture.view, &drain_work, &cleanup_cancellation)?.await?;
    let recovered_proof = recovered.drain_with_native_work(recovered.original_attempt(),
        &fixture.view, &observation, &cleanup_cancellation)?.await?;
    let mut foreign = Fixture::new(250).await?;
    assert!(provider.drain_with_native_work(provider.original_attempt(), &foreign.view, &drain_work, &cleanup_cancellation).is_err(),
        "an authentic prepared view for another task cannot substitute for the dispatched physical view");
    foreign.release_publication().await?;
    let cancelled = CancellationToken::new();
    let cancelled_work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &cancelled).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    cancelled.cancel();
    let denied = match provider.drain_with_native_work(provider.original_attempt(), &fixture.view, &cancelled_work, &cancelled) {
        Ok(future) => future.await,
        Err(error) => Err(error),
    };
    assert!(denied.is_err(), "cancelled cleanup cannot return a substitute drain proof");
    fixture.release_publication().await?;
    drop(recovered_proof);
    drop(proof);
    Ok(())
}

#[tokio::test]
#[ignore = "requires pinned Pi CLI and native qualification lane"]
async fn required_pinned_pi_native_denies_changed_publication_and_unrelated_ledger() -> Result<()> {
    let mut fixture = Fixture::new(0).await?;
    let mut changed = fixture.invocation.clone();
    changed.prompt = "forged original command".into();
    let changed = changed.request_with_work(&fixture.binding, fixture.view.manifest(), &fixture.work)?;
    let bytes = crate::pi_root_provider::guarded_pi_bytes(&changed.0, 1_048_576, &fixture.work)?;
    let failed = fixture.results.stage_once_with_native_work(
        native_artifact_operation(fixture.command, "request", &fixture.work)?,
        fixture.plan.0.request.path(), &bytes.0, "application/json", "request", &fixture.work)?.await;
    assert!(failed.is_err(), "changed retry bytes reused the original actual upload operation");
    let original = fixture.provider().await?;
    let sealed_guard = fixture.context.volume_callback_guard_with_native_work(&fixture.owner, &fixture.work).await?;
    let callback_guard = sealed_guard.for_volume(&fixture.owner, &fixture.binding.scratch.volume, false, &fixture.work)?;
    let original_header = fixture.work.clone();
    acyclic_fs::native_mount::NativeMountCallbackGuard::check(callback_guard.as_ref(), &original_header)
        .map_err(|cause| native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?.await
        .map_err(|cause| native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?;
    let unrelated_cancellation = CancellationToken::new();
    let unrelated_work = NativeMaterializationWork::new(super::super::native_root_source::budget(), &unrelated_cancellation).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let unrelated_effects = fixture.effects(&original, &unrelated_work)?;
    let before = fixture.work.receipt();
    assert!(unrelated_effects.run_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan.0, &fixture.work).await.is_err(), "another ledger supplied native metadata authority");
    assert_eq!(fixture.work.receipt().backend_read_operations, before.backend_read_operations);
    assert_eq!(fixture.work.receipt().backend_write_operations, before.backend_write_operations);
    let foreign_header = unrelated_work.clone();
    let callback = acyclic_fs::native_mount::NativeMountCallbackGuard::check(callback_guard.as_ref(), &foreign_header)
        .map_err(|cause| native_view_error(acyclic_fs::native_mount::MountLifecycleError::Native(cause)))?;
    assert!(callback.await.is_err(), "an unrelated accounting header admitted the original task's source callback");
    assert_eq!(unrelated_work.receipt().backend_read_operations, 0);
    assert_eq!(unrelated_work.receipt().backend_write_operations, 0);
    assert!(original.receipt(original.original_attempt(), &fixture.work, None).await?.is_none(), "a denied request created native launch evidence");
    assert!(fixture.effects(&original, &fixture.work)?.reconcile_task_effect_with_native_work(&fixture.owner, fixture.command, &fixture.plan.0, &fixture.work).await.is_err(), "a denied request created a committed effect");
    fixture.release_publication().await?;
    Ok(())
}
