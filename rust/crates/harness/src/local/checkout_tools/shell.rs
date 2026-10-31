//! Task shell execution through the shared native process capture engine.
use super::*;
use crate::filesystem::native_process::{self, Invocation};
mod tail;

impl CheckoutTools {
    pub(super) async fn shell(
        &self,
        (input, operation): (crate::tools::ShellInput, crate::OperationId),
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
        let invocation = Invocation {
            executable: executable()?.to_path_buf(),
            argv: Vec::new(),
            cwd: workspace.path.clone(),
            environment: self.environment.clone(),
            inherit_environment: false,
            timeout_ms: 300_000,
            control_timeout_ms: output::CONTROL_TIMEOUT_MS,
            // Presentation is bounded below; streaming output is not a reason to stop a command.
            maximum_output_bytes: usize::MAX,
            standard_input: None,
            process_custody: workspace.processes.clone(),
        };
        let invocation = native_process::shell::prepare(
            input,
            &state.mounts,
            invocation,
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
        let spill_name = format!("shell-{operation}.log");
        let (scratch, _) = state
            .mounts
            .resolve(std::path::Path::new(".volumes/scratch"))?;
        scratch.check(
            acyclic_fs::access::Action::Write,
            &spill_name,
            self.clock.now_unix_millis() / 1000,
        )?;
        let spill_root = scratch
            .native_root
            .clone()
            .unwrap_or_else(|| state.mounts.root.join(".volumes/scratch"));
        let retained = tail::Tail::new(spill_root.join(&spill_name), maximum_output.min(30_000));
        let (output, (state, files, claims, retained)) =
            native_process::capture::capture_with_custody(
                invocation,
                |(_, files, _, retained): &mut (
                    OwnedMutexGuard<State>,
                    output::Output,
                    workspace::volumes::Claims,
                    tail::Tail,
                ),
                 chunk| {
                    files.observe(chunk)?;
                    let (acyclic_native_runtime::ProcessOutput::Stdout(bytes)
                    | acyclic_native_runtime::ProcessOutput::Stderr(bytes)) = chunk;
                    retained.observe(bytes)
                },
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
                (state, files, claims, retained),
                Some(wake),
                native_process::capture::OutputRetention::Observe,
            )
            .await?;
        let (text, omitted, spilled) = retained.finish();
        let result = output
            .ok_or_else(|| Error::Conflict("process capture was interrupted".into()))
            .map(|output| ToolResult {
                value: serde_json::json!({
                    "success": output.success, "exit_code": output.exit_code,
                    "stdout": text,
                    "stderr": "",
                    "omitted_bytes": omitted,
                    "spill": spilled.then(|| format!(".volumes/scratch/{spill_name}")),
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
