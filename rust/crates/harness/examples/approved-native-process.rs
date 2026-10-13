//! Public consumer: explicitly approved native execution and receipt-only recovery.
use acyclic_harness::runtime::TaskSpawner as _;

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs, PublicationPermit, WorkBudget};
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, InteractionId, OperationId, Result, TaskId,
    conversation::{
        ContentGrant, ContentPublisher, ContentResidencyVerifier, Limits, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{
        Action, AggregateKind, Authority, AuthorityIssuer, Command, EffectGuarantee, EffectStatus,
        SchemaRegistry, Scope,
    },
    distributed::{WorkPull, Worker},
    durable_host::TaskJournalOwner,
    effect_host::{ConversationEffectHost, TaskEffectPlan},
    effects::EffectRegistry,
    executor::ExecutionJournal,
    filesystem::{
        BorrowedNativeVolumeBinding, FilesystemContentPublisher, FilesystemContentVerifier,
        FilesystemExecutionJournal, FilesystemHost, FilesystemTaskRuntime,
        NATIVE_PROCESS_EFFECT_KIND, NativePreparationRoot, NativeProcessAuthority,
        NativeProcessProvider, NativeProcessRequest, NativeProcessResult, NativeProcessStopKind,
        NativeViewManifest, NativeViewOptions, NativeVolumeNamespace, NativeVolumeView,
        workspace_ref,
    },
    interaction::{Interaction, InteractionResponse},
    mcp::{PROTOCOL_VERSION, stdio::McpStdioRequest},
    resources::ProviderRef,
    runtime::{RuntimeScope, TaskAdmissionRecord, TaskDefinition, TaskRegistry},
    scheduler::{LeaseFence, SessionLimits},
    store::StreamAggregate,
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{MemoryStream, StreamProvider};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

struct Machine {
    identity: MachineIdentity,
    schema: Value,
}
impl ResumableMachine for Machine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, _: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: vec![],
            status: MachineStatus::Suspended,
        })
    }
}

type Files<A = acyclic_fs::MemoryAuthorityBackend, O = acyclic_fs::MemoryObjectBackend> =
    FilesystemHost<A, O>;
type Runtime<P, A = acyclic_fs::MemoryAuthorityBackend, O = acyclic_fs::MemoryObjectBackend> =
    FilesystemTaskRuntime<P, A, O>;

async fn runtime<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
) -> Result<Runtime<P, A, O>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let machine = Arc::new(Machine {
        identity: MachineIdentity {
            name: "example.native".into(),
            version: "1".into(),
            digest: [7; 32],
        },
        schema: json!({"type":"integer"}),
    });
    let definition = TaskDefinition::<Value, Value>::resumable(
        machine.clone(),
        json!({"type":"integer"}),
        json!({"type":"object"}),
    )?;
    let identity = definition.identity().clone();
    let machine_identity = machine.identity.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    let runtime_scope = RuntimeScope::new(scope.capabilities().clone(), Limits::default())?;
    let runtime = Runtime::open(
        stream,
        files,
        &volume,
        &issuer.verifier(&mut None)?,
        scope,
        &runtime_scope,
        &tasks,
        &machines,
        &Default::default(),
        SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        },
        1,
        1_000_000,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        None,
        &[],
        &Default::default(),
        None,
    )
    .await?;
    runtime
        .task_host()
        .admit(TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: OperationId::from_bytes([3; 16]),
            task: identity,
            machine: machine_identity,
            input: json!(0),
            input_schema: json!({"type":"integer"}),
            output_schema: json!({"type":"object"}),
            parent: None,
            grants: runtime_scope.grants().clone(),
            limits: runtime_scope.limits(),
            run_limits: runtime_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        })
        .await?;
    Ok(runtime)
}

async fn run(mcp: Option<&'static str>) -> std::result::Result<(), Box<dyn std::error::Error>> {
    run_on(mcp, Arc::new(MemoryStream::default()), None, false).await
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Restart {
    task: TaskId,
    fence: LeaseFence,
    scope: Scope,
    command: OperationId,
    approval: InteractionId,
    plan: TaskEffectPlan,
    results_volume: VolumeRef,
    destination: VolumeRef,
    request: NativeProcessRequest,
    first: Option<EffectStatus>,
    applied: bool,
}

struct Evidence<A, O> {
    restart: Restart,
    directory: tempfile::TempDir,
    _preparation_directory: tempfile::TempDir,
    files: std::sync::Weak<Files<A, O>>,
}

struct Recovery<P> {
    owner: Arc<TaskJournalOwner<P>>,
    effects: Arc<ConversationEffectHost<P>>,
    content: Arc<dyn ContentResidencyVerifier>,
}

fn issuer() -> AuthorityIssuer {
    AuthorityIssuer::new(
        "example",
        [6; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "approved-native-example".into(),
        },
    )
}

async fn run_on<P: StreamProvider>(
    mcp: Option<&'static str>,
    stream: std::sync::Arc<P>,
    receipt_fault: Option<(&str, bool)>,
    lost_response: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let filesystem = Fs::memory();
    let evidence = prepare_on(
        mcp,
        stream.clone(),
        filesystem.clone(),
        receipt_fault,
        lost_response,
        None,
    )
    .await?;
    if evidence.files.upgrade().is_some() {
        return Err("initial filesystem host remained live during recovery".into());
    }
    if evidence.directory.path() != evidence.restart.request.cwd {
        return Err("restart metadata changed the owned native directory".into());
    }
    recover_on(
        stream,
        filesystem,
        &evidence.restart,
        receipt_fault,
        lost_response,
    )
    .await
}

async fn prepare_on<P, A, O>(
    mcp: Option<&'static str>,
    stream: std::sync::Arc<P>,
    filesystem: Fs<A, O>,
    receipt_fault: Option<(&str, bool)>,
    lost_response: bool,
    restart_path: Option<&std::path::Path>,
) -> std::result::Result<Evidence<A, O>, Box<dyn std::error::Error>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let provider = ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?;
    let files = FilesystemHost::new(filesystem, &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let volume = |name| {
        VolumeRef::new(
            provider.clone(),
            name,
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )
    };
    let source = volume("source".into())?;
    let destination = volume("destination".into())?;
    let results_volume = volume("results".into())?;
    let task_id = TaskId::from_bytes([3; 16]);
    let command = OperationId::from_bytes([4; 16]);
    let approval = InteractionId::from_bytes([5; 16]);
    let issuer = issuer();
    let mut grants = vec![
        "operation:declare".into(),
        "operation:observe".into(),
        "operation:cancel".into(),
        "task:spawn:example.native@1".into(),
        source.capability(VolumeOperation::Read, &mut None)?,
        destination.capability(VolumeOperation::Read, &mut None)?,
        destination.capability(VolumeOperation::Write, &mut None)?,
        results_volume.capability(VolumeOperation::Read, &mut None)?,
        results_volume.capability(VolumeOperation::Write, &mut None)?,
        "effect:plan".into(),
        "effect:run".into(),
        "effect:provider:example.native".into(),
        "interaction:open".into(),
        "interaction:resolve".into(),
        format!("interaction:respond:{approval}"),
        "conversation:bind".into(),
    ];
    if mcp == Some("tools/call") {
        grants.extend([
            "mcp:call:example".into(),
            "tool:call:mcp.example.echo".into(),
        ]);
    }
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new(grants),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    for volume in [&source, &destination, &results_volume] {
        files.create_volume(volume).await?;
    }
    let setup_scope = issuer
        .issue(
            "setup",
            &Capabilities::new([source.capability(VolumeOperation::Write, &mut None)?]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let setup = ContentGrant::verify(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &setup_scope,
        &source,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    files
        .put_content(
            &source,
            &setup,
            "input.txt",
            b"public native consumer",
            "text/plain",
            "input.txt",
            128,
            &IdempotencyKey::new("example-source")?,
            None,
            None,
        )
        .await?;
    let runtime = runtime(
        stream.clone(),
        files.clone(),
        results_volume.clone(),
        &issuer,
        &scope,
    )
    .await?;
    let WorkPull::Claimed(lease) = runtime
        .task_host()
        .pull_work(
            &Worker {
                id: "example-worker".into(),
                available: Default::default(),
                labels: BTreeMap::new(),
            },
            None,
            &mut None,
        )
        .await?
    else {
        return Err("missing task lease".into());
    };
    runtime.task_host().start_task(&lease, &mut None).await?;
    let mut conversation = StreamAggregate::open(
        &stream.clone(),
        issuer
            .verifier(&mut None)
            .expect("verifier copy")
            .audience()
            .clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        acyclic_harness::store::default_projection_read_limits(),
        acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    conversation
        .execute(
            Command {
                operation_id: OperationId::from_bytes([8; 16]),
                idempotency_key: IdempotencyKey::new("example-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let directory = if let Some(path) = restart_path {
        tempfile::tempdir_in(path.parent().ok_or("restart storage has no parent")?)?
    } else {
        tempfile::tempdir()?
    };
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    if let Some(fields) = allowances.as_object_mut() {
        for value in fields.values_mut() {
            *value = json!(1_000_000);
        }
    }
    let work_per_volume: WorkBudget = serde_json::from_value(allowances)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(work_per_volume, &cancellation)
        .map_err(|(cause, work)| acyclic_harness::Error::NativeConstruction { cause, work })?;
    let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work)?;
    let fence = LeaseFence::from(&lease.reservation);
    let owner = runtime
        .task_host()
        .journal_owner(task_id, &fence, Some(&io))
        .await?
        .into_native_shared(&work)?;
    let context = runtime
        .task_context_with_native_work(task_id, &fence, &work)
        .await?;
    let preparation_directory = tempfile::tempdir_in(
        directory
            .path()
            .parent()
            .ok_or("native root has no parent")?,
    )?;
    std::fs::create_dir(preparation_directory.path().join("target"))?;
    std::fs::create_dir(preparation_directory.path().join("backup"))?;
    let (allocation, _custody) =
        NativePreparationRoot::witness(directory.path(), preparation_directory.path(), &work)?
            .await?;
    let mut selections = Vec::new();
    for (volume, path, writable) in [
        (&source, "source", false),
        (&destination, "destination", true),
    ] {
        let generation = files
            .resolve(&workspace_ref(
                volume.provider().clone(),
                &volume.storage_name(&mut None)?,
            )?)
            .await?
            .generation;
        selections.push((volume, generation, path, writable));
    }
    let verifier = issuer.verifier(&mut None).expect("verifier copy");
    let bindings = selections
        .iter()
        .map(
            |(volume, generation, path, writable)| BorrowedNativeVolumeBinding {
                host: &files,
                volume,
                generation,
                path,
                writable: *writable,
                namespace: &NativeVolumeNamespace::MaterializedDirectory,
                verifier: &verifier,
                scope: &scope,
                publication: PublicationPermit::Unrestricted,
            },
        )
        .collect::<Vec<_>>();
    let view = Arc::new(
        NativeVolumeView::prepare(
            &owner,
            &[context.clone(), context],
            &allocation,
            &NativeViewOptions {
                root: directory.path().to_path_buf(),
                maximum_volumes: 2,
                work_per_volume,
            },
            &bindings,
            &work,
        )?
        .await?,
    );
    let mut request = native_request(mcp, lost_response, view.manifest().clone())?;
    if restart_path.is_some() {
        request.timeout_ms = 30_000;
    }
    let content = FilesystemContentVerifier::new(
        files.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        1_000_000,
        None,
    )?;
    let results = FilesystemContentPublisher::new(
        files.clone(),
        &results_volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        1_000_000,
        None,
    )?;
    let request_ref = results
        .stage(
            command,
            "native/request.json",
            &serde_json::to_vec(&request)?,
            "application/json",
            "request.json",
            None,
        )?
        .await?
        .0;
    let result_schema = results
        .stage(
            command,
            "native/schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
            None,
        )?
        .await?
        .0;
    let plan = TaskEffectPlan {
        provider: "example.native".into(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
        request: request_ref,
        result_schema,
    };
    let approvals = execution_journal(
        stream.clone(),
        files.clone(),
        &results_volume,
        &issuer,
        &scope,
    )?;
    approvals
        .open_interaction(
            approval,
            &Interaction::approval(
                "Run this exact example process".into(),
                command,
                request.approval_digest(task_id, command, None)?,
            )?,
            None,
        )?
        .await?;
    approvals
        .resolve_interaction(
            approval,
            InteractionResponse::Approval {
                approved: true,
                reason: None,
            },
            &scope,
        )
        .await?;
    let authority = NativeProcessAuthority {
        verifier: issuer.verifier(&mut None)?,
        schemas: SchemaRegistry::default(),
        content: content.clone(),
        results: results.clone(),
        approvals: approvals.clone(),
    };
    let process = Arc::new(
        NativeProcessProvider::new(
            owner.clone(),
            command,
            plan.clone(),
            Some(view.clone()),
            approval,
            authority,
        )
        .await?,
    );
    let mut registry = EffectRegistry::new(None, Some(content.clone()))?;
    registry.register(process.clone(), None)?;
    let effects = Arc::new(ConversationEffectHost::new(
        stream.clone(),
        issuer
            .verifier(&mut None)
            .expect("verifier copy")
            .audience()
            .clone(),
        issuer.clone(),
        scope.clone(),
        SchemaRegistry::new(),
        content.clone(),
        registry,
        None,
        None,
    )?);
    let mut restart = Restart {
        task: task_id,
        fence: LeaseFence::from(&lease.reservation),
        scope: scope.clone(),
        command,
        approval,
        plan: plan.clone(),
        results_volume,
        destination: destination.clone(),
        request: request.clone(),
        first: None,
        applied: false,
    };
    if let Some(path) = restart_path {
        use std::io::Write as _;
        let mut file = std::fs::File::create(path)?;
        file.write_all(&serde_json::to_vec(&restart)?)?;
        file.sync_all()?;
    }
    if mcp == Some("tools/call") && restart_path.is_none() {
        model_tool::run(
            model_tool::Approved {
                recovery: Recovery {
                    owner: owner.clone(),
                    effects: effects.clone(),
                    content: content.clone(),
                },
                task: task_id,
                command,
                plan: plan.clone(),
                request: request.clone(),
            },
            runtime
                .harness()
                .durable_context(task_id, OperationId::from_bytes(task_id.into_bytes()), None)
                .await?,
            approvals.clone(),
            false,
            !lost_response && receipt_fault.is_none(),
            receipt_fault.is_some(),
        )
        .await?;
    }
    let status = if mcp == Some("tools/call") && receipt_fault.is_some() {
        None
    } else {
        let first = effects
            .run_task_effect(&owner, command, &plan, None)
            .await
            .map(|result| result.0);
        verify_first_result(
            first,
            content.as_ref(),
            &request,
            receipt_fault,
            lost_response,
        )
        .await?
    };
    let applied = receipt_fault.is_none_or(|(kind, _)| kind == "observed");
    verify_publication(&files, &destination, applied && !lost_response).await?;
    drop(effects);
    drop(process);
    drop(view);
    remove_physical_output(directory.path(), applied)?;
    restart.first = status;
    restart.applied = applied;
    Ok(Evidence {
        restart,
        directory,
        _preparation_directory: preparation_directory,
        files: Arc::downgrade(&files),
    })
}

async fn recover_on<P, A, O>(
    stream: std::sync::Arc<P>,
    filesystem: Fs<A, O>,
    seed: &Restart,
    receipt_fault: Option<(&str, bool)>,
    lost_response: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let issuer = issuer();
    let files = FilesystemHost::new(filesystem, seed.destination.provider(), None)?;
    let runtime = runtime(
        stream.clone(),
        files.clone(),
        seed.results_volume.clone(),
        &issuer,
        &seed.scope,
    )
    .await?;
    let Recovery {
        owner,
        effects,
        content,
    } = recovered_effects(stream.clone(), files.clone(), &runtime, seed, &issuer).await?;
    let recovered_status = effects
        .reconcile_task_effect(&owner, seed.command, &seed.plan, None)
        .await
        .map(|result| result.0)?;
    verify_recovered_status(&recovered_status, seed.first.as_ref(), receipt_fault)?;
    let mcp = seed.request.mcp_stdio.is_some();
    if seed
        .request
        .mcp_stdio
        .as_ref()
        .is_some_and(|request| request.method == "tools/call")
        && (receipt_fault.is_some() || seed.first.is_some())
    {
        let journal = execution_journal(
            stream.clone(),
            files.clone(),
            &seed.results_volume,
            &issuer,
            &seed.scope,
        )?;
        model_tool::run(
            model_tool::Approved {
                recovery: Recovery {
                    owner: owner.clone(),
                    effects: effects.clone(),
                    content: content.clone(),
                },
                task: seed.task,
                command: seed.command,
                plan: seed.plan.clone(),
                request: seed.request.clone(),
            },
            runtime
                .harness()
                .durable_context(
                    seed.task,
                    OperationId::from_bytes(seed.task.into_bytes()),
                    None,
                )
                .await?,
            journal,
            true,
            matches!(recovered_status, EffectStatus::Succeeded { .. }) && !lost_response,
            receipt_fault.is_some(),
        )
        .await?;
    }
    if matches!(recovered_status, EffectStatus::Succeeded { .. }) {
        verify_stored_result(
            content.as_ref(),
            &seed.request,
            &recovered_status,
            lost_response,
        )
        .await?;
    }
    if effects
        .run_task_effect(&owner, seed.command, &seed.plan, None)
        .await
        .map(|result| result.0)?
        != recovered_status
        || seed.request.cwd.join("destination/output.txt").exists()
    {
        return Err("receipt recovery repeated native execution".into());
    }
    if mcp {
        verify_mcp_calls(&seed.request, seed.applied)?;
    }
    verify_publication(&files, &seed.destination, seed.applied && !lost_response).await?;
    if receipt_fault.is_none() && !lost_response {
        println!(
            "approved {} execution, SDK publication and receipt-only recovery passed",
            if mcp { "MCP stdio" } else { "native" }
        );
    }
    Ok(())
}

async fn recovered_effects<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    runtime: &Runtime<P, A, O>,
    seed: &Restart,
    issuer: &AuthorityIssuer,
) -> Result<Recovery<P>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let owner = Arc::new(
        runtime
            .task_host()
            .journal_owner(seed.task, &seed.fence, None)
            .await?,
    );
    let authority = recovered_authority(
        stream.clone(),
        files,
        &seed.results_volume,
        issuer,
        &seed.scope,
    )?;
    let content = authority.content.clone();
    let recovered = Arc::new(
        NativeProcessProvider::<P, A, O>::new(
            owner.clone(),
            seed.command,
            seed.plan.clone(),
            None,
            seed.approval,
            authority,
        )
        .await?,
    );
    let mut registry = EffectRegistry::new(None, Some(content.clone()))?;
    registry.register(recovered, None)?;
    let effects = Arc::new(ConversationEffectHost::new(
        stream,
        issuer
            .verifier(&mut None)
            .expect("verifier copy")
            .audience()
            .clone(),
        issuer.clone(),
        seed.scope.clone(),
        SchemaRegistry::new(),
        content.clone(),
        registry,
        None,
        None,
    )?);
    Ok(Recovery {
        owner,
        effects,
        content,
    })
}

fn execution_journal<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    volume: &VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
) -> Result<Arc<FilesystemExecutionJournal<P, A, O>>>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    Ok(Arc::new(FilesystemExecutionJournal::new(
        stream,
        files,
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &acyclic_harness::core::SchemaRegistry::new(),
        &scope,
        1_000_000,
        None,
    )?))
}

fn recovered_authority<P, A, O>(
    stream: std::sync::Arc<P>,
    files: Arc<Files<A, O>>,
    volume: &VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
) -> Result<NativeProcessAuthority>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let content: Arc<dyn ContentResidencyVerifier> = FilesystemContentVerifier::new(
        files.clone(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        1_000_000,
        None,
    )?;
    Ok(NativeProcessAuthority {
        verifier: issuer.verifier(&mut None)?,
        schemas: SchemaRegistry::default(),
        content,
        results: FilesystemContentPublisher::new(
            files.clone(),
            volume,
            &issuer.verifier(&mut None)?,
            scope,
            1_000_000,
            None,
        )?,
        approvals: execution_journal(stream, files, volume, issuer, scope)?,
    })
}

async fn verify_publication<A, O>(
    files: &Files<A, O>,
    destination: &VolumeRef,
    expected: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let reference = workspace_ref(
        destination.provider().clone(),
        &destination.storage_name(&mut None)?,
    )?;
    let published = files.read(&reference, None, "/output.txt", 128).await;
    if expected {
        if published?.as_ref() != b"public native consumer" {
            return Err("destination was not published".into());
        }
    } else {
        match published {
            Err(acyclic_harness::Error::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err("destination was published without a complete capture".into()),
        }
    }
    Ok(())
}

fn native_request(
    mcp: Option<&'static str>,
    lost_response: bool,
    view: NativeViewManifest,
) -> std::result::Result<NativeProcessRequest, Box<dyn std::error::Error>> {
    Ok(NativeProcessRequest {
        executable: std::env::current_exe()?,
        argv: vec![
            if lost_response {
                "--mcp-native-lost-response"
            } else if mcp.is_some() {
                "--mcp-native-child"
            } else {
                "--native-child"
            }
            .into(),
        ],
        cwd: view.options.root.clone(),
        environment: BTreeMap::from([
            ("APPROVED_TOKEN".into(), "exact-token".into()),
            ("MCP_MAX_BYTES".into(), "4096".into()),
        ]),
        timeout_ms: if lost_response { 1_000 } else { 10_000 },
        control_timeout_ms: 250,
        cancellation_poll_ms: 10,
        maximum_output_bytes: 8192,
        maximum_result_bytes: 65_536,
        standard_input: None,
        mcp_stdio: if let Some(method) = mcp {
            Some(McpStdioRequest {
                initialization: OperationId::from_bytes([31; 16]),
                operation: OperationId::from_bytes([32; 16]),
                method: method.into(),
                params: match method {
                    "tools/call" => {
                        json!({"name":"echo","arguments":{"text":"héllo","sequence":u64::MAX}})
                    }
                    "tools/list" => json!({"cursor":"next"}),
                    "resources/list" | "resources/templates/list" | "prompts/list" => json!({}),
                    "resources/read" => json!({"uri":"example://content"}),
                    "prompts/get" => json!({"name":"example","arguments":{}}),
                    _ => return Err("unsupported MCP method".into()),
                },
                maximum_bytes: 4096,
            })
        } else {
            None
        },
        view,
    })
}

fn remove_physical_output(
    root: &std::path::Path,
    applied: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let output = root.join("destination/output.txt");
    if output.exists() != applied {
        return Err("physical output disagrees with the receipt fault boundary".into());
    }
    if applied {
        std::fs::remove_file(&output)?;
    }
    Ok(())
}

async fn verify_first_result(
    first: Result<EffectStatus>,
    content: &dyn ContentResidencyVerifier,
    request: &NativeProcessRequest,
    receipt_fault: Option<(&str, bool)>,
    lost_response: bool,
) -> std::result::Result<Option<EffectStatus>, Box<dyn std::error::Error>> {
    if receipt_fault.is_some() {
        if first.is_ok() {
            return Err("receipt fault did not interrupt the effect".into());
        }
        return Ok(None);
    }
    let status = first?;
    verify_stored_result(content, request, &status, lost_response).await?;
    Ok(Some(status))
}

async fn verify_stored_result(
    content: &dyn ContentResidencyVerifier,
    request: &NativeProcessRequest,
    status: &EffectStatus,
    lost_response: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let EffectStatus::Succeeded { result } = status else {
        return Err(format!("unobserved native result: {status:?}").into());
    };
    let output: NativeProcessResult =
        serde_json::from_slice(&content.read(result, 1_000_000, None)?.await?)?;
    if request.mcp_stdio.is_some() {
        mcp_peer::verify_result(request, &output, lost_response)?;
    } else if !output.success
        || !output
            .stdout
            .windows(b"exact-token".len())
            .any(|bytes| bytes == b"exact-token")
    {
        return Err("incorrect native result".into());
    }
    Ok(())
}

fn verify_recovered_status(
    recovered: &EffectStatus,
    first: Option<&EffectStatus>,
    receipt_fault: Option<(&str, bool)>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if let Some((kind, lost_reply)) = receipt_fault {
        if kind == "observed" && lost_reply {
            if !matches!(recovered, EffectStatus::Succeeded { .. }) {
                return Err("committed observation was not recovered".into());
            }
        } else if *recovered != EffectStatus::Indeterminate {
            return Err("unknown MCP outcome became authoritative".into());
        }
    } else if first.is_none() {
        if *recovered != EffectStatus::Indeterminate {
            return Err("launch without an observation became authoritative".into());
        }
    } else if Some(recovered) != first {
        return Err("receipt recovery changed the observed result".into());
    }
    Ok(())
}

fn verify_mcp_calls(
    request: &NativeProcessRequest,
    applied: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let calls = std::fs::read(request.cwd.join("destination/calls.txt"));
    if applied {
        let expected: &[u8] = match request
            .mcp_stdio
            .as_ref()
            .ok_or("missing MCP descriptor")?
            .method
            .as_str()
        {
            "tools/call" => b"call\n",
            "tools/list" => b"list\n",
            "resources/list" => b"resources\n",
            "resources/templates/list" => b"templates\n",
            "resources/read" => b"read\n",
            "prompts/list" => b"prompts\n",
            "prompts/get" => b"prompt\n",
            _ => return Err("unsupported MCP method".into()),
        };
        if calls? != expected {
            return Err("MCP request was not applied exactly once".into());
        }
    } else {
        match calls {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => return Err("unknown launch caused an MCP call".into()),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "approved-native-process/faults.rs"]
mod faults;

#[path = "approved-native-process/mcp_peer.rs"]
mod mcp_peer;
#[path = "approved-native-process/model_tool.rs"]
mod model_tool;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    if cfg!(test) && std::env::args().any(|arg| arg == "--list") {
        if !std::env::args().any(|arg| arg == "--ignored") {
            println!("approved_native_process: test");
        }
        return Ok(());
    }
    #[cfg(all(test, feature = "filesystem-local"))]
    if std::env::args().any(|arg| arg == "--mcp-host-death-child") {
        return faults::host_child().await;
    }
    if std::env::args()
        .any(|arg| arg == "--mcp-native-child" || arg == "--mcp-native-lost-response")
    {
        return mcp_peer::child(std::env::args().any(|arg| arg == "--mcp-native-lost-response"));
    }
    if std::env::args().any(|arg| arg == "--native-child") {
        std::fs::write("destination/output.txt", std::fs::read("source/input.txt")?)?;
        println!("{}", std::env::var("APPROVED_TOKEN")?);
        return Ok(());
    }
    if cfg!(test) {
        run(None).await?;
        run(Some("tools/call")).await?;
        run(Some("tools/list")).await?;
        for method in [
            "resources/list",
            "resources/templates/list",
            "resources/read",
            "prompts/list",
            "prompts/get",
        ] {
            run(Some(method)).await?;
        }
        #[cfg(test)]
        faults::run().await?;
        Ok(())
    } else {
        let method = if std::env::args().any(|arg| arg == "--mcp-discovery") {
            Some("tools/list")
        } else if std::env::args().any(|arg| arg == "--mcp-stdio") {
            Some("tools/call")
        } else {
            None
        };
        run(method).await
    }
}
