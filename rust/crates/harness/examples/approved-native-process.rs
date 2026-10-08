//! Public consumer: explicitly approved native execution and receipt-only recovery.

use acyclic_fs::{Fs, PublicationPermit, WorkBudget};
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
    effect_host::{ConversationEffectHost, TaskEffectPlan},
    effects::EffectRegistry,
    executor::ExecutionJournal,
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
        FilesystemHost, FilesystemTaskRuntime, NATIVE_PROCESS_EFFECT_KIND, NativeProcessAuthority,
        NativeProcessProvider, NativeProcessRequest, NativeProcessResult, NativeViewOptions,
        NativeVolumeBinding, NativeVolumeView, workspace_ref,
    },
    interaction::{Interaction, InteractionResponse},
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskDefinition, TaskRegistry},
    scheduler::{LeaseFence, SessionLimits},
    store::StreamAggregate,
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{MemoryStream, StreamClient};
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

type Files = FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
type Runtime = FilesystemTaskRuntime<
    MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

async fn runtime(
    stream: StreamClient<MemoryStream>,
    files: Arc<Files>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
) -> Result<Runtime> {
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
    machines.register(machine)?;
    let runtime_scope = RuntimeScope::new(scope.capabilities().clone(), Limits::default())?;
    let runtime = Runtime::open(
        stream,
        files,
        volume,
        issuer.verifier(),
        scope.clone(),
        runtime_scope.clone(),
        tasks,
        machines,
        Default::default(),
        SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 0,
            model_steps: 1,
        },
        1,
        1_000_000,
    )
    .await?;
    runtime
        .task_host()
        .admit(TaskAdmissionRecord {
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

#[allow(
    clippy::too_many_lines,
    reason = "self-contained public composition example with explicit capabilities and no private SDK helpers"
)]
async fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let provider = ProviderRef::new("example", "filesystem", "1")?;
    let files = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::from_bytes([1; 16]);
    let volume = |name| {
        VolumeRef::new(
            provider.clone(),
            name,
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )
    };
    let source = volume("source")?;
    let destination = volume("destination")?;
    let results_volume = volume("results")?;
    let task_id = TaskId::from_bytes([3; 16]);
    let command = OperationId::from_bytes([4; 16]);
    let approval = InteractionId::from_bytes([5; 16]);
    let issuer = AuthorityIssuer::new(
        "example",
        [6; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "approved-native-example".into(),
        },
    );
    let scope = issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "task:spawn:example.native@1".into(),
            source.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Read)?,
            destination.capability(VolumeOperation::Write)?,
            results_volume.capability(VolumeOperation::Read)?,
            results_volume.capability(VolumeOperation::Write)?,
            "effect:plan".into(),
            "effect:run".into(),
            "effect:provider:example.native".into(),
            "interaction:open".into(),
            "interaction:resolve".into(),
            format!("interaction:respond:{approval}"),
            "conversation:bind".into(),
        ]),
    );
    for volume in [&source, &destination, &results_volume] {
        files.create_volume(volume).await?;
    }
    let setup_scope = issuer.root_for_agent(
        agent,
        "setup",
        Capabilities::new([source.capability(VolumeOperation::Write)?]),
    );
    let setup = ContentGrant::verify(
        &issuer.verifier(),
        &setup_scope,
        &source,
        VolumeOperation::Write,
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
        )
        .await?;
    let stream = StreamClient::new(Arc::new(MemoryStream::default()));
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
        .pull_work(&Worker {
            id: "example-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        })
        .await?
    else {
        return Err("missing task lease".into());
    };
    runtime.task_host().start_task(&lease).await?;
    let owner = Arc::new(
        runtime
            .task_host()
            .journal_owner(task_id, LeaseFence::from(&lease.reservation))
            .await?,
    );
    let mut conversation = StreamAggregate::open(
        &stream.clone(),
        issuer.verifier().audience().clone(),
        issuer.verifier(),
        SchemaRegistry::new(),
    )
    .await?;
    conversation
        .execute(Command {
            operation_id: OperationId::from_bytes([8; 16]),
            idempotency_key: IdempotencyKey::new("example-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation { agent },
        })
        .await?;
    let directory = tempfile::tempdir()?;
    let mut allowances = serde_json::to_value(WorkBudget::UNBOUNDED)?;
    if let Some(fields) = allowances.as_object_mut() {
        for value in fields.values_mut() {
            *value = json!(1_000_000);
        }
    }
    let mut bindings = Vec::new();
    for (volume, path, writable) in [
        (&source, "source", false),
        (&destination, "destination", true),
    ] {
        let generation = files
            .resolve(&workspace_ref(
                volume.provider().clone(),
                &volume.storage_name()?,
            )?)
            .await?
            .generation;
        bindings.push(NativeVolumeBinding {
            host: files.clone(),
            volume: volume.clone(),
            generation,
            path: path.into(),
            writable,
            verifier: issuer.verifier(),
            scope: scope.clone(),
            publication: PublicationPermit::Unrestricted,
        });
    }
    let view = Arc::new(
        NativeVolumeView::prepare(
            &owner,
            NativeViewOptions {
                root: directory.path().to_path_buf(),
                maximum_volumes: 2,
                work_per_volume: serde_json::from_value(allowances)?,
            },
            bindings,
        )
        .await?,
    );
    let request = NativeProcessRequest {
        executable: std::env::current_exe()?,
        argv: vec!["--native-child".into()],
        cwd: view.manifest().options.root.clone(),
        environment: BTreeMap::from([("APPROVED_TOKEN".into(), "exact-token".into())]),
        timeout_ms: 10_000,
        control_timeout_ms: 250,
        cancellation_poll_ms: 10,
        maximum_output_bytes: 8192,
        maximum_result_bytes: 65_536,
        view: view.manifest().clone(),
    };
    let content = Arc::new(FilesystemContentVerifier::new(
        files.clone(),
        issuer.verifier(),
        scope.clone(),
        1_000_000,
    )?);
    let results = Arc::new(FilesystemContentPublisher::new(
        files.clone(),
        results_volume.clone(),
        &issuer.verifier(),
        &scope,
        1_000_000,
    )?);
    let request_ref = results
        .stage(
            command,
            "native/request.json",
            &serde_json::to_vec(&request)?,
            "application/json",
            "request.json",
        )
        .await?;
    let result_schema = results
        .stage(
            command,
            "native/schema.json",
            b"{\"type\":\"object\"}",
            "application/schema+json",
            "schema.json",
        )
        .await?;
    let plan = TaskEffectPlan {
        provider: "example.native".into(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
        request: request_ref,
        result_schema,
    };
    let approvals = Arc::new(FilesystemExecutionJournal::new(
        stream.clone(),
        files.clone(),
        results_volume,
        issuer.verifier(),
        scope.clone(),
        1_000_000,
    )?);
    approvals
        .open_interaction(
            approval,
            Interaction::approval(
                "Run this exact example process",
                command,
                request.approval_digest(task_id, command)?,
            )?,
        )
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
    let authority = || NativeProcessAuthority {
        verifier: issuer.verifier(),
        schemas: SchemaRegistry::new(),
        content: content.clone(),
        results: results.clone(),
        approvals: approvals.clone(),
    };
    let process = Arc::new(
        NativeProcessProvider::new(
            owner.clone(),
            command,
            plan.clone(),
            view.clone(),
            approval,
            authority(),
        )
        .await?,
    );
    let mut registry = EffectRegistry::default().with_result_resolver(content.clone());
    registry.register(process.clone())?;
    let effects = ConversationEffectHost::new(
        stream.clone(),
        issuer.verifier().audience().clone(),
        issuer.clone(),
        scope.clone(),
        SchemaRegistry::new(),
        content.clone(),
        registry,
    )?;
    let status = effects
        .run_task_effect(&owner, command, plan.clone())
        .await?;
    let EffectStatus::Succeeded { result } = &status else {
        return Err(format!("unobserved native result: {status:?}").into());
    };
    let output: NativeProcessResult = serde_json::from_slice(&content.read(result).await?)?;
    if !output.success
        || !output
            .stdout
            .windows(b"exact-token".len())
            .any(|bytes| bytes == b"exact-token")
    {
        return Err("incorrect native result".into());
    }
    let destination_ref =
        workspace_ref(destination.provider().clone(), &destination.storage_name()?)?;
    if files
        .read(&destination_ref, None, "/output.txt", 128)
        .await?
        .as_ref()
        != b"public native consumer"
    {
        return Err("destination was not published".into());
    }
    drop(effects);
    drop(process);
    drop(view);
    std::fs::remove_file(directory.path().join("destination/output.txt"))?;
    let recovered = Arc::new(
        NativeProcessProvider::<
            _,
            acyclic_fs::MemoryAuthorityBackend,
            acyclic_fs::MemoryObjectBackend,
        >::recover(owner.clone(), command, plan.clone(), approval, authority())
        .await?,
    );
    let mut registry = EffectRegistry::default().with_result_resolver(content.clone());
    registry.register(recovered)?;
    let effects = ConversationEffectHost::new(
        stream.clone(),
        issuer.verifier().audience().clone(),
        issuer,
        scope,
        SchemaRegistry::new(),
        content,
        registry,
    )?;
    if effects.run_task_effect(&owner, command, plan).await? != status
        || directory.path().join("destination/output.txt").exists()
    {
        return Err("receipt recovery repeated native execution".into());
    }
    println!("approved native execution, SDK publication and receipt-only recovery passed");
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--native-child") {
        std::fs::write("destination/output.txt", std::fs::read("source/input.txt")?)?;
        println!("{}", std::env::var("APPROVED_TOKEN")?);
        return Ok(());
    }
    run().await
}
