//! One shell tool binding for every Machines provider.
use super::*;
use crate::{
    core::{AuthorityVerifier, Scope},
    runtime::{RuntimeScope, ToolContext},
    tool::{Tool, ToolExecutor, ToolInvocation, ToolResult},
    tools::{ShellInput, ToolOutputWriter},
};
use acyclic_machines::{ExecCommand, ExecOutput, ExecStopKind};
use acyclic_native_runtime::ProcessOutput;
use futures::StreamExt;

/// A task workspace executor bound to one sandbox and authenticated authority.
/// The application chooses the machine-specific grant; callers need that grant
/// and the stock `tool:call:shell` grant. The signed scope supplies the expiry.
#[derive(Clone)]
pub struct MachineShell {
    host: MachinesHost,
    machine: MachineId,
    workspace: String,
    grant: String,
    signed: Scope,
    verifier: Arc<AuthorityVerifier>,
}

impl MachineShell {
    /// Binds a POSIX workspace in a selected machine. Does not start a process.
    pub fn new(
        host: MachinesHost,
        sandbox: &SandboxRef,
        workspace: &str,
        required_grant: &str,
        signed: Scope,
        verifier: Arc<AuthorityVerifier>,
    ) -> Result<Self> {
        verifier.expiry(&signed)?;
        if workspace.len() > 4096
            || !workspace.starts_with('/')
            || workspace.contains('\0')
            || workspace.contains('\\')
            || workspace
                .split('/')
                .skip(1)
                .any(|part| matches!(part, "" | "." | ".."))
        {
            return Err(Error::Invalid("machine shell workspace admission failed; recovery: configure a normalized absolute POSIX workspace below /".into()));
        }
        if required_grant.is_empty()
            || !signed.capabilities().contains(required_grant)
            || !signed.capabilities().contains("tool:call:shell")
        {
            return Err(Error::Unauthorized("machine shell binding failed; recovery: authenticate the workspace grant and tool:call:shell before binding".into()));
        }
        let machine = host.machine_id(sandbox)?;
        Ok(Self {
            host,
            machine,
            workspace: workspace.into(),
            grant: required_grant.into(),
            signed,
            verifier,
        })
    }

    /// Uses the stock shell schema and projection, with the selected guest's
    /// language description. Register this when the host OS differs from Linux.
    pub fn tool(self: Arc<Self>) -> Result<Tool> {
        let mut tool = crate::tools::catalog()
            .find(|spec| spec.name == "shell")
            .ok_or_else(|| {
                Error::Invalid(
                    "shell catalog entry missing; recovery: restore the stock shell catalog".into(),
                )
            })?
            .bind()?;
        tool.definition.description = "Run a bash command in the calling task's isolated machine workspace. Use bash syntax and POSIX paths; cwd is relative to the configured workspace.".into();
        tool.executor = self;
        Ok(tool)
    }
}

impl ToolExecutor for MachineShell {
    fn cancellable_on_drop(&self) -> bool {
        true
    }

    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "machine shell requires ordinary task execution",
            });
        }
        self.verifier.expiry(&self.signed)?;
        let context = context
            .ok_or_else(|| Error::Unauthorized("machine shell requires its task context".into()))?;
        let writer = context
            .task()
            .content_binding(None)
            .and_then(|binding| binding.writer.as_ref())
            .ok_or_else(|| {
                Error::Unauthorized(
                    "machine shell requires an authenticated private output volume".into(),
                )
            })?;
        if self.signed.agent().is_none_or(|agent| {
            writer.volume().owner() != &crate::conversation::VolumeOwner::Agent(agent)
        }) {
            return Err(Error::Unauthorized(
                "machine shell output owner differs from its authenticated agent".into(),
            ));
        }
        let grants = context.effective_scope().grants();
        if invocation.name != "shell"
            || context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || !grants.contains("tool:call:shell")
            || !grants.contains(&self.grant)
            || !grants.is_subset_of(self.signed.capabilities())
        {
            return Err(Error::Unauthorized(
                "machine shell invocation differs from its authenticated task authority".into(),
            ));
        }
        Ok(())
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context = context
                .ok_or_else(|| Error::Unauthorized("machine shell task context missing".into()))?;
            let request = serde_json::to_vec(&invocation)
                .map_err(|error| Error::Storage(error.to_string()))?;
            let input: ShellInput = serde_json::from_value(invocation.arguments)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            input.validate()?;
            let cwd = input.cwd.as_deref().unwrap_or("");
            if cwd.starts_with('/') || cwd.contains('\\') || cwd.split('/').any(|part| part == "..")
            {
                return Err(Error::Invalid("machine shell cwd admission failed; recovery: use a relative POSIX directory without parent traversal".into()));
            }
            let cwd = format!("{}/{}", self.workspace, cwd);
            if cwd.len() > 4096 {
                return Err(Error::Invalid("machine shell cwd exceeds the portable path bound; recovery: use a workspace-relative path whose full guest path fits 4096 bytes".into()));
            }
            let argv = shell_argv(&self.workspace, &cwd, &input.command);
            let expiry = self.verifier.expiry(&self.signed)?.saturating_mul(1000);
            let expiry = context
                .effective_scope()
                .run_limits()
                .deadline_epoch_ms
                .map_or(expiry, |deadline| expiry.min(deadline));
            let command = ExecCommand::new(argv, input.timeout_ms()?)
                .map_err(|error| map_error(error, Some(invocation.operation_id)))?
                .with_expiry(expiry)
                .with_output_limit(context.effective_scope().limits().render_bytes);
            let mut files = ToolOutputWriter::open(&context, request).await?;
            let mut output = self
                .host
                .provider
                .exec(self.machine, command)
                .await
                .map_err(|error| map_error(error, Some(invocation.operation_id)))?;
            let maximum = usize::try_from(context.effective_scope().limits().render_bytes)
                .unwrap_or(usize::MAX);
            let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
            while let Some(frame) = output.next().await {
                match frame.map_err(|error| map_error(error, Some(invocation.operation_id)))? {
                    ExecOutput::Stdout(data) => {
                        files.observe(ProcessOutput::Stdout(&data)).await?;
                        files.flush(false).await?;
                        let count = data
                            .len()
                            .min(maximum.saturating_sub(stdout.len() + stderr.len()));
                        stdout.extend_from_slice(data.get(..count).unwrap_or_default());
                    }
                    ExecOutput::Stderr(data) => {
                        files.observe(ProcessOutput::Stderr(&data)).await?;
                        files.flush(true).await?;
                        let count = data
                            .len()
                            .min(maximum.saturating_sub(stdout.len() + stderr.len()));
                        stderr.extend_from_slice(data.get(..count).unwrap_or_default());
                    }
                    ExecOutput::ExitCode(code) => {
                        let result = ToolResult {
                            value: serde_json::json!({"success": code == 0, "exit_code": code, "stdout": String::from_utf8_lossy(&stdout), "stderr": String::from_utf8_lossy(&stderr), "stop": null}),
                        };
                        files.finish(&result).await?;
                        return Ok(result);
                    }
                    ExecOutput::Stopped(stop) => {
                        let kind = match stop.kind {
                            ExecStopKind::Timeout => {
                                crate::filesystem::NativeProcessStopKind::Timeout
                            }
                            ExecStopKind::Expired | ExecStopKind::ControlStopped => {
                                crate::filesystem::NativeProcessStopKind::ControlStopped
                            }
                            ExecStopKind::OutputLimit => {
                                crate::filesystem::NativeProcessStopKind::OutputLimit
                            }
                            ExecStopKind::CaptureFailed => {
                                crate::filesystem::NativeProcessStopKind::CaptureFailed
                            }
                        };
                        let result = ToolResult {
                            value: serde_json::json!({"success": false, "exit_code": null, "stdout": String::from_utf8_lossy(&stdout), "stderr": String::from_utf8_lossy(&stderr), "stop": crate::filesystem::NativeProcessStop { kind, message: stop.message, cleanup_completed: stop.cleanup_completed }}),
                        };
                        files.finish(&result).await?;
                        return Ok(result);
                    }
                }
            }
            Err(Error::Indeterminate(invocation.operation_id))
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            // An interrupted shell is never replayed. Its streamed files remain
            // on the private volume, and the command owner's drop guard stops it.
            Err(Error::Indeterminate(invocation.operation_id))
        })
    }
}

// Pass source as bounded positional arguments, preserving arbitrary UTF-8,
// quotes and trailing newlines. The inner bash receives the original script
// with no extra positional parameters or wrapper variables in its environment.
fn shell_argv(workspace: &str, cwd: &str, source: &str) -> Vec<String> {
    const BOOTSTRAP: &str = r#"acyclic_root=$(cd -P -- "$1" && pwd -P) || { printf '%s\n' 'shell workspace selection failed; recovery: restore the configured workspace' >&2; exit 125; }
cd -P -- "$2" || { printf '%s\n' 'shell cwd selection failed; recovery: choose an existing workspace directory' >&2; exit 125; }
case "$PWD/" in "$acyclic_root/"*) ;; *) printf '%s\n' 'shell cwd escapes the workspace; recovery: choose a directory within the configured workspace' >&2; exit 125;; esac
shift 2
printf -v acyclic_source %s "$@"
exec bash --noprofile --norc -c "$acyclic_source"
"#;
    let mut argv = vec![
        "bash".into(),
        "--noprofile".into(),
        "--norc".into(),
        "-c".into(),
        BOOTSTRAP.into(),
        "bash".into(),
        workspace.into(),
        cwd.into(),
    ];
    let mut remaining = source;
    while !remaining.is_empty() {
        let mut end = remaining.len().min(4096);
        while !remaining.is_char_boundary(end) {
            end -= 1;
        }
        argv.push(remaining[..end].into());
        remaining = &remaining[end..];
    }
    argv
}
