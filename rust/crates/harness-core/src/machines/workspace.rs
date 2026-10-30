//! Task shell binding over the public Machines execution primitive.
use super::*;
use crate::{
    conversation::{VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    runtime::{RuntimeScope, ToolContext},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
    tools::ShellInput,
};
use acyclic_machines::{ExecCommand, capture_exec};
use acyclic_stream::BoxProviderFuture;

/// Shell executor for one explicitly admitted machine and volume.
/// File tools retain the task's existing authenticated content bindings.
pub struct MachinesWorkspace {
    host: MachinesHost,
    sandbox: SandboxRef,
    volume: VolumeRef,
    signed: Scope,
    verifier: AuthorityVerifier,
    read: String,
    write: String,
    executable: String,
    shell: &'static acyclic_host_platform::shell::ShellSpec,
}

impl MachinesWorkspace {
    /// Verifies the same parent authority used by ordinary harness admission.
    /// Credentials stay in the client; only command bytes enter the guest.
    pub fn new(
        host: MachinesHost,
        sandbox: SandboxRef,
        admitted: MachineObservation,
        volume: &VolumeRef,
        signed: Scope,
        verifier: AuthorityVerifier,
    ) -> Result<Self> {
        if host.machine_id(&sandbox)? != admitted.id
            || admitted.contract.selected_volume_ref.as_ref() != Some(&volume.into())
            || !matches!(
                admitted.state,
                acyclic_machines::MachineState::Running | acyclic_machines::MachineState::Suspended
            )
        {
            return Err(Error::Unauthorized("workspace differs from its admitted Machine and selected volume; recovery: retain the original Create/Attach or native enrollment observation".into()));
        }
        verifier.verify(&signed, &mut None)?;
        let read = volume.capability(VolumeOperation::Read, &mut None)?;
        let write = volume.capability(VolumeOperation::Write, &mut None)?;
        if !signed.capabilities().contains(&read)
            || !signed.capabilities().contains(&write)
            || !signed
                .capabilities()
                .contains(&crate::contract::capability::tool_call("shell"))
        {
            return Err(Error::Unauthorized("machine shell admission lacks volume read/write; recovery: derive the workspace from the verified parent volume grant".into()));
        }
        Ok(Self {
            host,
            sandbox,
            volume: volume.clone(),
            signed,
            verifier,
            read,
            write,
            executable: "bash".into(),
            shell: &acyclic_host_platform::shell::BASH,
        })
    }

    /// The exact source-admitted selection retained by this workspace binding.
    pub fn selected_volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Selects the actual Machine's shell from its trusted hosting preset.
    /// Native hosts pass their already-resolved executable and PLATFORM spec;
    /// hosted Linux defaults use BASH. The task executor remains the same.
    pub fn with_shell(
        mut self,
        executable: String,
        shell: &'static acyclic_host_platform::shell::ShellSpec,
    ) -> Result<Self> {
        if executable.is_empty() || executable.contains('\0') {
            return Err(Error::Invalid("Machine shell selection failed; recovery: bind the host's original resolved executable".into()));
        }
        self.executable = executable;
        self.shell = shell;
        Ok(self)
    }

    /// Streams one shell invocation, preserving channel bytes and terminal status.
    pub async fn shell(
        &self,
        input: ShellInput,
        maximum_output: u64,
    ) -> Result<acyclic_machines::ExecStream> {
        self.shell_until(input, maximum_output, None).await
    }

    async fn shell_until(
        &self,
        input: ShellInput,
        maximum_output: u64,
        task_deadline: Option<u64>,
    ) -> Result<acyclic_machines::ExecStream> {
        input.validate()?;
        let expiry = self.verifier.expiry(&self.signed)?.checked_mul(1000)
            .ok_or_else(|| Error::Invalid("machine authority expiry overflow; recovery: issue an exactly representable expiry".into()))?;
        let expiry = task_deadline.map_or(expiry, |deadline| deadline.min(expiry));
        let timeout = u64::from(input.timeout_ms()?);
        let (source, stdin) = match self.shell.stdin_bootstrap {
            Some(bootstrap) => (bootstrap.to_owned(), input.command.into_bytes()),
            None => (input.command, Vec::new()),
        };
        let argv = std::iter::once(self.executable.clone())
            .chain(self.shell.arguments.iter().map(|value| (*value).into()))
            .chain(std::iter::once(source))
            .collect();
        let mut command = ExecCommand::new(argv, timeout)
            .map_err(|error| map_error(error, None))?
            .with_expiry(expiry)
            .with_stdin(stdin)
            .with_output_limit(maximum_output);
        if let Some(cwd) = input.cwd.filter(|value| !value.is_empty()) {
            command = command
                .with_cwd(cwd)
                .map_err(|error| map_error(error, None))?;
        }
        self.host.exec(&self.sandbox, command).await
    }
}

impl ToolExecutor for MachinesWorkspace {
    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "machine commands require ordinary task execution",
            });
        }
        let _ = work;
        let context = context.ok_or_else(|| {
            Error::Unauthorized(
                "machine shell has no task; recovery: bind its original tool context".into(),
            )
        })?;
        let grants = context.effective_scope().grants();
        if invocation.name != "shell"
            || context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || !grants.contains(&crate::contract::capability::tool_call("shell"))
            || !grants.contains(&self.read)
            || !grants.contains(&self.write)
        {
            return Err(Error::Unauthorized("machine shell differs from its task authority; recovery: use the original admitted tool call and volume grants".into()));
        }
        self.verifier.verify(&self.signed, &mut None)
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context = context.ok_or_else(|| {
                Error::Unauthorized(
                    "machine shell task is absent; recovery: bind the original tool context".into(),
                )
            })?;
            let input: ShellInput = serde_json::from_value(invocation.arguments)
                .map_err(|error| Error::Invalid(format!("shell input decode failed: {error}; recovery: use the shared ShellInput schema")))?;
            let maximum = context.effective_scope().limits().render_bytes;
            let stream = self
                .shell_until(
                    input,
                    maximum,
                    context.effective_scope().run_limits().deadline_epoch_ms,
                )
                .await
                .map_err(|error| match error {
                    Error::Machines(cause) => map_error(cause, Some(context.operation_id())),
                    cause => cause,
                })?;
            let output = capture_exec(stream, maximum)
                .await
                // Dispatch already happened. Without a terminal receipt,
                // dropping capture cannot attest physical process retirement.
                .map_err(|_| Error::Indeterminate(context.operation_id()))?;
            if output
                .stop
                .as_ref()
                .is_some_and(|stop| !stop.cleanup_completed)
            {
                return Err(Error::Indeterminate(context.operation_id()));
            }
            let stop = output.stop.map(|stop| {
                use acyclic_machines::ExecStopKind;
                let kind = match stop.kind {
                    ExecStopKind::Timeout => "timeout",
                    ExecStopKind::OutputLimit => "output_limit",
                    ExecStopKind::Expired | ExecStopKind::ControlStopped => "control_stopped",
                    ExecStopKind::CaptureFailed => "capture_failed",
                };
                serde_json::json!({
                    "kind": kind, "message": stop.message,
                    "cleanup_completed": stop.cleanup_completed,
                })
            });
            Ok(ToolResult {
                value: serde_json::json!({
                    "success": output.exit_code == Some(0) && stop.is_none(),
                    "exit_code": output.exit_code,
                    "stdout": String::from_utf8_lossy(&output.stdout),
                    "stderr": String::from_utf8_lossy(&output.stderr), "stop": stop,
                }),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            // No command identity is fabricated from a lifecycle operation.
            Err(Error::Indeterminate(invocation.operation_id))
        })
    }
}
