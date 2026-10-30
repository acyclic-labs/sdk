//! Task shell execution through the shared native process capture engine.
use super::*;
use crate::filesystem::native_process;

impl CheckoutTools {
    pub(super) async fn shell(
        &self,
        input: crate::tools::ShellInput,
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
        let workspace = state.workspace.as_ref().map_or_else(
            || Arc::clone(&self.workspace),
            |(_, workspace)| workspace.clone(),
        );
        let invocation = native_process::shell::prepare(
            input,
            &state.mounts,
            acyclic_native_runtime::NativeHost {
                cwd: workspace.path.clone(),
                environment: self.environment.clone(),
                inherit_environment: false,
                custody: workspace.processes.clone(),
            },
            executable()?,
            maximum_output,
            self.clock.now_unix_millis() / 1000,
        )?;
        let control =
            acyclic_native_runtime::ProcessControl::open(self.expiry_unix_ms).map_err(storage)?;
        // A dropped turn stops its command; the worker still reaps the tree.
        let _stop = StopOnDrop(control.clone());
        let wake = control.wake();
        files
            .health
            .wake_on_failure(wake.clone())
            .map_err(storage)?;
        let output_health = files.health.clone();
        let (output, (state, files, claims)) = native_process::capture::capture_with_custody(
            invocation,
            |(_, files, _): &mut (
                OwnedMutexGuard<State>,
                output::Output,
                workspace::volumes::Claims,
            ),
             chunk| files.observe(chunk),
            move || {
                output_health.check()?;
                if acyclic_fs::capture_root_identity(&workspace.path)
                    .map_err(std::io::Error::other)?
                    != workspace.identity
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "checkout root was replaced",
                    ));
                }
                control.check()
            },
            (state, files, claims),
            Some(wake),
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

pub(super) struct StopOnDrop(pub(super) acyclic_native_runtime::ProcessControl);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.stop();
    }
}

pub(in crate::local) fn executable() -> Result<&'static std::path::Path> {
    acyclic_native_runtime::platform_shell().map_err(Error::process_io)
}
