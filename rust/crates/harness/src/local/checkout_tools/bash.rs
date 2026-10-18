//! Task shell execution through the shared native process capture engine.
use super::*;
use crate::filesystem::native_process::{self, Invocation};

impl CheckoutTools {
    pub(super) async fn bash(
        &self,
        input: crate::tools::BashInput,
        maximum_output: usize,
        state: OwnedMutexGuard<State>,
        files: output::Output,
        claims: workspace::volumes::Claims,
    ) -> Result<(
        Result<ToolResult>,
        OwnedMutexGuard<State>,
        output::Output,
        workspace::volumes::Claims,
    )> {
        let workspace = state
            .workspace
            .as_ref()
            .map(|(_, workspace)| workspace.clone())
            .unwrap_or_else(|| Arc::clone(&self.workspace));
        let invocation = Invocation {
            executable: which::which("bash").map_err(|cause| Error::Invalid(cause.to_string()))?,
            argv: Vec::new(),
            cwd: workspace.path.clone(),
            environment: Default::default(),
            inherit_environment: false,
            timeout_ms: 300_000,
            control_timeout_ms: output::CONTROL_TIMEOUT_MS,
            cancellation_poll_ms: 25,
            maximum_output_bytes: maximum_output,
            standard_input: None,
            mcp_stdio: None,
        };
        let invocation = native_process::bash::prepare(
            input,
            &state.mounts,
            invocation,
            self.clock.now_unix_millis() / 1000,
        )?;
        let check_owner = Arc::clone(&files.owner);
        let runtime = tokio::runtime::Handle::current();
        let (output, (state, files, claims)) = native_process::capture::capture_with_custody(
            invocation,
            |(_, files, _): &mut (
                OwnedMutexGuard<State>,
                output::Output,
                workspace::volumes::Claims,
            ),
             chunk| files.observe(chunk),
            move || {
                if acyclic_fs::capture_root_identity(&workspace.path)
                    .map_err(std::io::Error::other)?
                    != workspace.identity
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "checkout root was replaced",
                    ));
                }
                runtime.block_on(async {
                    tokio::time::timeout(
                        std::time::Duration::from_millis(u64::from(output::CONTROL_TIMEOUT_MS)),
                        check_owner.verify(false, &mut None),
                    )
                    .await
                    .map_err(native_process::capture::control_error)?
                    .map_err(native_process::capture::control_error)
                })
            },
            (state, files, claims),
        )
        .await?;
        let result = output
            .ok_or_else(|| Error::Conflict("process capture was interrupted".into()))
            .map(|output| ToolResult {
                value: serde_json::json!({
                    "success": output.success, "exit_code": output.exit_code,
                    "stdout": String::from_utf8_lossy(&output.stdout),
                    "stderr": String::from_utf8_lossy(&output.stderr),
                    "stop": output.stop,
                }),
            });
        Ok((result, state, files, claims))
    }
}
