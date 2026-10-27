//! Actual tool calls over the caller-selected provider; no response fixtures.
use acyclic_harness::{
    AgentId, Capabilities, Error, OperationId, Result,
    conversation::{Limits, VolumeOperation},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::MemoryHarnessStorage,
    machines::{MachineShell, MachinesHost},
    resources::{ProviderRef, SandboxRef},
    runtime::{Bindings, RuntimeScope, TaskRunLimits, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_machines::{
    CreateMachine, ExecCommand, ExecOutput, IdempotencyKey, Image, MachineId, MachinesProvider,
    MutationOutcome,
};
use futures::StreamExt;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn require(condition: bool, step: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "machine shell {step} failed; recovery: rerun the provider conformance example after fixing this vector"
        )))
    }
}

fn context(
    storage: &MemoryHarnessStorage,
    grants: &Capabilities,
    bytes: u64,
    deadline: Option<u64>,
) -> Result<ToolContext> {
    let mut bindings = Bindings::local();
    bindings.scope = RuntimeScope::new(
        grants.clone(),
        Limits {
            file_bytes: 65_536,
            render_bytes: bytes,
            ..Limits::default()
        },
    )?
    .with_run_limits(
        TaskRunLimits {
            deadline_epoch_ms: deadline,
            ..TaskRunLimits::default()
        },
        &mut None,
    )?;
    let harness = bindings.build(None)?;
    let operation = OperationId::new();
    let task = harness.agent_context(
        operation,
        &Capabilities::default(),
        storage.content_bindings(),
    )?;
    ToolContext::new(Arc::new(task), operation, "machine-shell-conformance", None)
}

fn invocation(context: &ToolContext, source: &str, timeout: Option<u32>) -> ToolInvocation {
    let mut arguments = serde_json::json!({"command": source});
    if let Some(timeout) = timeout {
        arguments["timeout"] = timeout.into();
    }
    ToolInvocation {
        operation_id: context.operation_id(),
        call_id: context.call_id().into(),
        name: "shell".into(),
        arguments,
    }
}

async fn guest(provider: &dyn MachinesProvider, machine: MachineId, source: &str) -> Result<()> {
    let command = ExecCommand::new(vec!["sh".into(), "-c".into(), source.into()], 5_000)
        .map_err(Error::Machines)?;
    let mut output = provider
        .exec(machine, command)
        .await
        .map_err(Error::Machines)?;
    while let Some(frame) = output.next().await {
        match frame.map_err(Error::Machines)? {
            ExecOutput::ExitCode(code) => return require(code == 0, "guest barrier"),
            ExecOutput::Stopped(_) => return require(false, "guest barrier capture"),
            _ => {}
        }
    }
    require(false, "guest barrier terminal result")
}

async fn completed(
    shell: &MachineShell,
    context: ToolContext,
    source: &str,
    timeout: Option<u32>,
) -> Result<ToolResult> {
    tokio::time::timeout(Duration::from_secs(7), shell.execute(invocation(&context, source, timeout), Some(context)))
        .await.map_err(|_| Error::Invalid("machine shell completion exceeded its bound; recovery: inspect provider process cleanup and rerun conformance".into()))?
}

/// Exercises streamed publication, exact exit, typed stops and future-drop cleanup.
/// The enclosing runner owns cleanup even if this function fails.
pub async fn verify(
    provider: Arc<dyn MachinesProvider>,
    workspace: &str,
) -> std::result::Result<(), String> {
    verify_inner(provider, workspace)
        .await
        .map_err(|error| format!("machine shell tool vector: {error}"))
}

async fn verify_inner(provider: Arc<dyn MachinesProvider>, workspace: &str) -> Result<()> {
    let request = CreateMachine::new(
        IdempotencyKey::new(),
        Image::custom([7; 32]).map_err(Error::Machines)?,
        [8; 32],
    );
    let MutationOutcome::Created(machine) =
        provider.create(request).await.map_err(Error::Machines)?
    else {
        return require(false, "sandbox creation");
    };
    let identity = ProviderRef::new(
        "conformance".into(),
        "machines".into(),
        "1".into(),
        &mut None,
    )?;
    let sandbox = SandboxRef::new(identity.clone(), machine.id.as_bytes().into(), None)?;
    let host = MachinesHost::new(provider.clone(), identity)?;
    let agent = AgentId::new();
    let storage = MemoryHarnessStorage::new(agent, 65_536).await?;
    let grants = Capabilities::new([
        "tool:call:shell".to_owned(),
        "machines:workspace:conformance".to_owned(),
        storage
            .volume()
            .capability(VolumeOperation::Read, &mut None)?,
        storage
            .volume()
            .capability(VolumeOperation::Write, &mut None)?,
    ]);
    let root = AuthorityRoot::new(
        "machine-shell-conformance",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: OperationId::new().to_string(),
        },
    );
    let signed = root.bootstrap_scope("shell", &grants, Some(agent), &mut None)?;
    let shell = MachineShell::new(
        host,
        &sandbox,
        workspace,
        "machines:workspace:conformance",
        signed,
        Arc::new(root.verifier(&mut None)?),
    )?;

    let other = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
    let foreign = context(&other, &grants, 65_536, None)?;
    require(
        matches!(
            shell.authorize(
                None,
                Some(&foreign),
                &invocation(&foreign, "true", Some(5)),
                None
            ),
            Err(Error::Unauthorized(_))
        ),
        "foreign agent rejection",
    )?;

    // The native default and longer explicit deadlines remain valid ShellInput,
    // but exceed the canonical Machines admission. Refuse before any guest IO.
    let refused = format!("{workspace}/shell-refused-{}", OperationId::new());
    let mutation = format!("printf unexpected > '{refused}'");
    for (source, timeout) in [
        (mutation.clone(), None),
        (mutation.clone(), Some(120)),
        (format!("{mutation}\n#{}", "x".repeat(32_768)), Some(5)),
    ] {
        require(
            matches!(
                completed(
                    &shell,
                    context(&storage, &grants, 65_536, None)?,
                    &source,
                    timeout
                )
                .await,
                Err(Error::Machines(acyclic_machines::ProviderError::Invalid(_)))
            ),
            "canonical command admission before dispatch",
        )?;
    }
    guest(
        provider.as_ref(),
        machine.id,
        &format!("test ! -e '{refused}'"),
    )
    .await?;

    // Split a multibyte source across the protocol's 4096-byte argument bound
    // without quoting changes, using a supported explicit deadline.
    let padding = "\u{e9}'\"$()".repeat(700);
    let source =
        format!("# .... {padding}\nprintf '%s\\n' 'portable source'; test \"$#\" -eq 0\n\n");
    let ordinary = completed(
        &shell,
        context(&storage, &grants, 65_536, None)?,
        &source,
        Some(5),
    )
    .await?;
    require(
        ordinary.value["stdout"] == "portable source\n"
            && ordinary.value["exit_code"] == 0
            && ordinary.value["stop"].is_null(),
        "long UTF-8 source",
    )?;

    // The tool future must publish both streams before the guest can exit.
    let call = context(&storage, &grants, 65_536, None)?;
    let prefix = format!("tools/{}/{}/", call.turn_id(), call.operation_id());
    let barrier = format!("{workspace}/shell-barrier-{}", OperationId::new());
    let source = format!(
        "printf '\\000\\377'; printf 'err\\000' >&2; while test ! -f '{barrier}'; do sleep 0.01; done; exit 7"
    );
    let mut pending = shell.execute(invocation(&call, &source, Some(5)), Some(call.clone()));
    let published = async {
        loop {
            let stdout = call
                .read_private_path(
                    storage.volume(),
                    "",
                    &format!("{prefix}stdout/0000000000000000"),
                    None,
                )
                .await;
            let stderr = call
                .read_private_path(
                    storage.volume(),
                    "",
                    &format!("{prefix}stderr/0000000000000000"),
                    None,
                )
                .await;
            if let (Ok((_, stdout)), Ok((_, stderr))) = (stdout, stderr) {
                require(
                    stdout.as_ref() == [0, 255] && stderr.as_ref() == b"err\0",
                    "binary stream files",
                )?;
                return Ok::<(), Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        result = &mut pending => { result?; return require(false, "publication before exit"); }
        result = tokio::time::timeout(Duration::from_secs(3), published) => { result.map_err(|_| Error::Invalid("machine shell streamed files were not published; recovery: inspect output publication and rerun conformance".into()))??; }
    }
    guest(provider.as_ref(), machine.id, &format!("touch '{barrier}'")).await?;
    let result = pending.await?;
    require(
        result.value["exit_code"] == 7
            && result.value["success"] == false
            && result.value["stop"].is_null(),
        "nonzero exit",
    )?;
    let (_, metadata) = call
        .read_private_path(storage.volume(), "", &format!("{prefix}result.json"), None)
        .await?;
    let metadata: serde_json::Value =
        serde_json::from_slice(&metadata).map_err(|error| Error::Invalid(error.to_string()))?;
    require(
        metadata.get("stdout").is_none()
            && metadata.get("stderr").is_none()
            && metadata["exit_code"] == 7,
        "terminal metadata",
    )?;

    let limited = completed(
        &shell,
        context(&storage, &grants, 3, None)?,
        "printf abcdef; sleep 5",
        Some(5),
    )
    .await?;
    require(
        limited.value["stdout"] == "abc"
            && limited.value["stop"]["kind"] == "output_limit"
            && limited.value["stop"]["cleanup_completed"] == true,
        "output limit",
    )?;
    let timed = completed(
        &shell,
        context(&storage, &grants, 65_536, None)?,
        "printf partial; sleep 5",
        Some(1),
    )
    .await?;
    require(
        timed.value["stdout"] == "partial"
            && timed.value["stop"]["kind"] == "timeout"
            && timed.value["stop"]["cleanup_completed"] == true,
        "timeout",
    )?;
    let deadline = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Invalid(error.to_string()))?
        .as_millis() as u64
        + 500;
    let expired = completed(
        &shell,
        context(&storage, &grants, 65_536, Some(deadline))?,
        "printf partial; sleep 5",
        Some(5),
    )
    .await?;
    require(
        expired.value["stdout"] == "partial"
            && expired.value["stop"]["kind"] == "control_stopped"
            && expired.value["stop"]["cleanup_completed"] == true,
        "absolute expiry",
    )?;

    let call = context(&storage, &grants, 65_536, None)?;
    let path = format!(
        "tools/{}/{}/stdout/0000000000000000",
        call.turn_id(),
        call.operation_id()
    );
    let marker = format!("{workspace}/shell-cancelled-{}", OperationId::new());
    let source = format!("(sleep 1; touch '{marker}') & printf started; sleep 5");
    let mut pending = shell.execute(invocation(&call, &source, Some(5)), Some(call.clone()));
    let published = async {
        loop {
            if call
                .read_private_path(storage.volume(), "", &path, None)
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        result = &mut pending => { result?; return require(false, "drop barrier"); }
        result = tokio::time::timeout(Duration::from_secs(3), published) => { result.map_err(|_| Error::Invalid("machine shell drop barrier was not published; recovery: inspect output publication and rerun conformance".into()))?; }
    }
    drop(pending);
    guest(
        provider.as_ref(),
        machine.id,
        &format!("sleep 1.1; test ! -f '{marker}'"),
    )
    .await?;
    provider
        .destroy_machine(machine.id, IdempotencyKey::new())
        .await
        .map_err(Error::Machines)?;
    Ok(())
}
