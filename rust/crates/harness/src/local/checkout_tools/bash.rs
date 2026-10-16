//! Bounded shell capture in the selected ordinary checkout.
use super::*;
use std::{process::Stdio, time::Duration};

const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

impl CheckoutTools {
    pub(super) async fn bash(
        &self,
        command: String,
        maximum_output: usize,
        state: OwnedMutexGuard<State>,
    ) -> Result<(Result<ToolResult>, OwnedMutexGuard<State>)> {
        let workspace = state
            .workspace
            .as_ref()
            .map(|(_, workspace)| workspace.clone())
            .unwrap_or_else(|| self.workspace.clone());
        acyclic_native_runtime::run_blocking_io(
            move || guarded(state, |_| run(&workspace, &command, maximum_output)),
            None,
        )
        .map_err(Error::process_io)?
        .await
        .map_err(Error::process_io)
    }
}

fn run(workspace: &LocalWorkspace, command: &str, maximum_output: usize) -> Result<ToolResult> {
    if acyclic_fs::capture_root_identity(&workspace.path).map_err(storage)? != workspace.identity {
        return Err(Error::Unauthorized("checkout root was replaced".into()));
    }
    let mut invocation = acyclic_native_runtime::shell_command(command, None);
    invocation
        .current_dir(&workspace.path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut process =
        acyclic_native_runtime::ProcessTree::spawn(&mut invocation).map_err(Error::process_io)?;
    let value = match process.wait_with_output(
        COMMAND_TIMEOUT,
        maximum_output,
        None,
        || Ok(()),
        |_| Ok(Default::default()),
        None,
        None,
    ) {
        Ok(output) => serde_json::json!({
            "success": output.status.success(), "exit_code": output.status.code(),
            "stdout": String::from_utf8_lossy(&output.stdout),
            "stderr": String::from_utf8_lossy(&output.stderr)
        }),
        Err(failure) => serde_json::json!({
            "success": false, "exit_code": failure.status.and_then(|status| status.code()),
            "stdout": String::from_utf8_lossy(&failure.stdout),
            "stderr": String::from_utf8_lossy(&failure.stderr),
            "error": failure.error.to_string(), "cleanup_completed": failure.cleanup_completed
        }),
    };
    Ok(ToolResult { value })
}
