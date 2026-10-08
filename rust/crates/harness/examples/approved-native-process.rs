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
        NativeProcessProvider, NativeProcessRequest, NativeProcessResult, NativeProcessStopKind,
        NativeViewManifest, NativeViewOptions, NativeVolumeBinding, NativeVolumeView,
        workspace_ref,
    },
    interaction::{Interaction, InteractionResponse},
    mcp::{
        PROTOCOL_VERSION,
        stdio::{McpStdioMethod, McpStdioRequest},
    },
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskDefinition, TaskRegistry},
    scheduler::{LeaseFence, SessionLimits},
    store::StreamAggregate,
    workflow::{
        MachineIdentity, MachineRegistry, MachineStatus, MachineTransition, ResumableMachine,
    },
};
use acyclic_stream::{MemoryStream, StreamClient, StreamProvider};
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
type Runtime<P> =
    FilesystemTaskRuntime<P, acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;

async fn runtime<P: StreamProvider>(
    stream: StreamClient<P>,
    files: Arc<Files>,
    volume: VolumeRef,
    issuer: &AuthorityIssuer,
    scope: &Scope,
) -> Result<Runtime<P>> {
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

async fn run(mcp: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
    run_on(
        mcp,
        StreamClient::new(Arc::new(MemoryStream::default())),
        None,
        false,
    )
    .await
}

#[allow(
    clippy::too_many_lines,
    reason = "self-contained public composition example with explicit capabilities and no private SDK helpers"
)]
async fn run_on<P: StreamProvider>(
    mcp: bool,
    stream: StreamClient<P>,
    receipt_fault: Option<(&str, bool)>,
    lost_response: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
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
    let request = native_request(mcp, lost_response, view.manifest().clone())?;
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
    let first = effects.run_task_effect(&owner, command, plan.clone()).await;
    let status = verify_first_result(
        first,
        content.as_ref(),
        &request,
        receipt_fault,
        lost_response,
    )
    .await?;
    let applied = receipt_fault.is_none_or(|(kind, _)| kind == "observed");
    verify_publication(&files, &destination, applied && !lost_response).await?;
    drop(effects);
    drop(process);
    drop(view);
    remove_physical_output(directory.path(), applied)?;
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
        content.clone(),
        registry,
    )?;
    let recovered_status = effects
        .reconcile_task_effect(&owner, command, &plan)
        .await?;
    verify_recovered_status(&recovered_status, status.as_ref(), receipt_fault)?;
    if let EffectStatus::Succeeded { result } = &recovered_status
        && mcp
    {
        let output: NativeProcessResult = serde_json::from_slice(&content.read(result).await?)?;
        verify_mcp_result(&request, &output, lost_response)?;
    }
    if effects.run_task_effect(&owner, command, plan).await? != recovered_status
        || directory.path().join("destination/output.txt").exists()
    {
        return Err("receipt recovery repeated native execution".into());
    }
    if mcp {
        verify_mcp_calls(directory.path(), applied)?;
    }
    if receipt_fault.is_none() && !lost_response {
        println!(
            "approved {} execution, SDK publication and receipt-only recovery passed",
            if mcp { "MCP stdio" } else { "native" }
        );
    }
    Ok(())
}

async fn verify_publication(
    files: &Files,
    destination: &VolumeRef,
    expected: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let reference = workspace_ref(destination.provider().clone(), &destination.storage_name()?)?;
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
    mcp: bool,
    lost_response: bool,
    view: NativeViewManifest,
) -> std::result::Result<NativeProcessRequest, Box<dyn std::error::Error>> {
    Ok(NativeProcessRequest {
        executable: std::env::current_exe()?,
        argv: vec![
            if lost_response {
                "--mcp-native-lost-response"
            } else if mcp {
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
        mcp_stdio: mcp.then(|| McpStdioRequest {
            initialization: OperationId::from_bytes([31; 16]),
            operation: OperationId::from_bytes([32; 16]),
            method: McpStdioMethod::CallTool,
            params: json!({"name":"echo","arguments":{"text":"héllo","sequence":u64::MAX}}),
            maximum_bytes: 4096,
        }),
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
    let EffectStatus::Succeeded { result } = &status else {
        return Err(format!("unobserved native result: {status:?}").into());
    };
    let output: NativeProcessResult = serde_json::from_slice(&content.read(result).await?)?;
    if request.mcp_stdio.is_some() {
        verify_mcp_result(request, &output, lost_response)?;
    } else if !output.success
        || !output
            .stdout
            .windows(b"exact-token".len())
            .any(|bytes| bytes == b"exact-token")
    {
        return Err("incorrect native result".into());
    }
    Ok(Some(status))
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
    } else if Some(recovered) != first {
        return Err("receipt recovery changed the observed result".into());
    }
    Ok(())
}

fn verify_mcp_calls(
    root: &std::path::Path,
    applied: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let calls = std::fs::read(root.join("destination/calls.txt"));
    if applied {
        if calls? != b"call\n" {
            return Err("MCP call was not applied exactly once".into());
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

fn verify_mcp_result(
    request: &NativeProcessRequest,
    output: &NativeProcessResult,
    lost_response: bool,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if lost_response {
        if !output.stop.as_ref().is_some_and(|stop| {
            stop.kind == NativeProcessStopKind::Timeout && stop.cleanup_completed
        }) || !matches!(request.mcp_response(output), Err(acyclic_harness::Error::Indeterminate(operation)) if operation == OperationId::from_bytes([32; 16]))
        {
            return Err(
                "lost MCP response became authoritative or cleanup was not confirmed".into(),
            );
        }
    } else if request.mcp_response(output)?
        != json!({
            "content":[{"type":"text","text":"héllo"}],
            "structuredContent":{"text":"héllo","sequence":u64::MAX}
        })
    {
        return Err("incorrect MCP process result".into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "approved-native-process/faults.rs"]
mod faults;

fn mcp_native_child(lost_response: bool) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use bytes::BytesMut;
    use rmcp::transport::async_rw::JsonRpcMessageCodec;
    use std::io::{BufRead as _, Read as _, Write as _};
    use tokio_util::codec::{Decoder as _, Encoder as _};
    let maximum: u32 = std::env::var("MCP_MAX_BYTES")?.parse()?;
    if maximum == 0 || std::env::var("APPROVED_TOKEN")? != "exact-token" {
        return Err("invalid explicit server input".into());
    }
    let mut input = std::io::stdin().lock();
    let mut codec = JsonRpcMessageCodec::<Value>::new_with_max_length(maximum as usize);
    let mut read = || -> std::result::Result<Value, Box<dyn std::error::Error>> {
        let mut line = Vec::new();
        input
            .by_ref()
            .take(u64::from(maximum) + 1)
            .read_until(b'\n', &mut line)?;
        if line.len() > maximum as usize || !line.ends_with(b"\n") {
            return Err("invalid bounded client frame".into());
        }
        codec
            .decode(&mut BytesMut::from(line.as_slice()))?
            .ok_or_else(|| "missing client message".into())
    };
    let write = |value: Value| -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut bytes = BytesMut::new();
        JsonRpcMessageCodec::new().encode(value, &mut bytes)?;
        std::io::stdout().write_all(&bytes)?;
        std::io::stdout().flush()?;
        Ok(())
    };
    let initialization = read()?;
    if initialization.get("method").and_then(Value::as_str) != Some("initialize")
        || initialization
            .pointer("/params/protocolVersion")
            .and_then(Value::as_str)
            != Some(PROTOCOL_VERSION)
    {
        return Err("incorrect client initialization".into());
    }
    let initialization_id = initialization
        .get("id")
        .ok_or("missing initialization id")?;
    write(json!({"jsonrpc":"2.0","id":initialization_id,"result":{
        "protocolVersion":PROTOCOL_VERSION,"capabilities":{"tools":{}},
        "serverInfo":{"name":"example.echo","version":"1"}}}))?;
    if read()?.get("method").and_then(Value::as_str) != Some("notifications/initialized") {
        return Err("tool called before initialized notification".into());
    }
    let call = read()?;
    if call.get("method").and_then(Value::as_str) != Some("tools/call")
        || call.pointer("/params/name").and_then(Value::as_str) != Some("echo")
        || call.pointer("/params/arguments") != Some(&json!({"text":"héllo","sequence":u64::MAX}))
    {
        return Err("incorrect pinned tool request".into());
    }
    std::fs::write("destination/output.txt", std::fs::read("source/input.txt")?)?;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("destination/calls.txt")?
        .write_all(b"call\n")?;
    let operation = call.get("id").ok_or("missing tool operation")?;
    let arguments = call
        .pointer("/params/arguments")
        .ok_or("missing tool arguments")?;
    if !lost_response {
        write(json!({"jsonrpc":"2.0","id":operation,"result":{
            "content":[{"type":"text","text":"héllo"}],
            "structuredContent":arguments}}))?;
    }
    // The ordinary owner closes stdin and terminates containment on completion.
    let mut byte = [0];
    let _ = input.read(&mut byte)?;
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    if std::env::args()
        .any(|arg| arg == "--mcp-native-child" || arg == "--mcp-native-lost-response")
    {
        return mcp_native_child(std::env::args().any(|arg| arg == "--mcp-native-lost-response"));
    }
    if std::env::args().any(|arg| arg == "--native-child") {
        std::fs::write("destination/output.txt", std::fs::read("source/input.txt")?)?;
        println!("{}", std::env::var("APPROVED_TOKEN")?);
        return Ok(());
    }
    if cfg!(test) {
        run(false).await?;
        run(true).await?;
        #[cfg(test)]
        faults::run().await?;
        Ok(())
    } else {
        run(std::env::args().any(|arg| arg == "--mcp-stdio")).await
    }
}
