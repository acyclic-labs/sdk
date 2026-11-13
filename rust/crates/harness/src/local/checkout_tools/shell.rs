//! Task shell execution through the shared native process capture engine.
use super::*;
use crate::filesystem::native_process::{self, Invocation};
mod job;
mod tail;

impl CheckoutTools {
    pub(super) async fn shell(
        &self,
        (input, operation, execution_grant, started, admitted, control): (
            crate::tools::ShellInput,
            crate::OperationId,
            Option<acyclic_native_runtime::execution::ExecutionGrant>,
            Option<tokio::sync::oneshot::Sender<ToolResult>>,
            std::time::Instant,
            acyclic_native_runtime::ProcessControl,
        ),
        maximum_output: usize,
        state: OwnedMutexGuard<State>,
        files: output::Output,
        claims: workspace::volumes::Claims,
        context: &ToolContext,
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
        let background = input.background;
        let invocation = Invocation {
            executable: executable()?.to_path_buf(),
            argv: Vec::new(),
            cwd: workspace.path.clone(),
            environment: self.environment.clone(),
            inherit_environment: false,
            execution_grant,
            schedule: acyclic_native_runtime::CaptureSchedule::Unbounded,
            // Presentation is bounded below; streaming output is not a reason to stop a command.
            maximum_output_bytes: None,
            standard_input: None,
            process_custody: workspace.processes.clone(),
        };
        let invocation = native_process::shell::prepare(
            input,
            &workspace.mounts,
            invocation,
            self.clock.now_unix_millis() / 1000,
            admitted,
        )?;
        control.check().map_err(Error::process_io)?;
        // A dropped turn stops its command; the worker still reaps the tree.
        let _stop = StopOnDrop(control.clone());
        let wake = control.wake();
        files
            .health
            .wake_on_failure(wake.clone())
            .map_err(storage)?;
        let output_health = files.health.clone();
        let capture_control = control.clone();
        let shutdown = workspace.shutdown.clone();
        let spill_name = format!("shell-{operation}.log");
        let spill_root = workspace
            .mounts
            .scratch_root
            .clone()
            .ok_or_else(|| Error::Invalid("shell requires host scratch".into()))?;
        let native_spill = spill_root
            .join(&spill_name)
            .to_str()
            .ok_or_else(|| Error::Invalid("shell scratch path is not UTF-8".into()))?
            .to_owned();
        let spill_file = format!("tools/{}/{operation}/spill.log", context.turn_id());
        let spill_path = format!("{}{spill_file}", crate::layers::OUTPUT_PATH);
        let retained = tail::Tail::new(spill_root.join(&spill_name), maximum_output);
        let cancellation = background
            .then(|| job::Cancellation::open(&spill_root, operation))
            .transpose()?;
        // Background ownership retains the selected workspace and volume claims,
        // while the command guard is available to later turns and file controls.
        let state = if background {
            drop(state);
            None
        } else {
            Some(state)
        };
        let completion = native_process::capture::start_capture_with_custody(
            invocation,
            |(_, files, _, retained): &mut (
                Option<OwnedMutexGuard<State>>,
                output::Output,
                workspace::volumes::Claims,
                tail::Tail,
            ),
             chunk| {
                files.observe(chunk)?;
                match chunk {
                    acyclic_native_runtime::ProcessOutput::Stdout(bytes) => {
                        retained.observe(bytes, false)
                    }
                    acyclic_native_runtime::ProcessOutput::Stderr(bytes) => {
                        retained.observe(bytes, true)
                    }
                }
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
                capture_control.check()
            },
            (state, files, claims, retained),
            Some(wake),
            native_process::capture::OutputRetention::Observe,
        )
        .await?;
        if let Some(started) = started {
            if let Err(error) = context
                .record_native_progress(
                    crate::runtime::NativeCaptureEvidence::Pending {
                        operation: super::capture::capture_operation(operation, 1),
                    },
                    None,
                )
                .await
            {
                control.stop();
                let _ = completion.await;
                return Err(error);
            }
            let directory = format!("tools/{}/{}", context.turn_id(), operation);
            let cancel = native_spill.trim_end_matches(".log").to_owned() + ".cancel";
            let mut result = ToolResult {
                value: serde_json::json!({
                    "background": true, "output_directory": directory, "cancel": cancel,
                    "text": format!("Background shell started. Read complete stdout, stderr and result.json under {}{directory}. Write {cancel} to cancel this job.", crate::layers::OUTPUT_PATH)
                }),
            };
            let bytes = serde_json::to_vec(&result.value).map_err(storage)?;
            let file =
                match output::writer::stage(context, "started.json", &bytes, "application/json")
                    .await
                {
                    Ok(file) => file,
                    Err(error) => {
                        control.stop();
                        let _ = completion.await;
                        return Err(error);
                    }
                };
            result.value["output_volume"] = serde_json::to_value(file.volume()).map_err(storage)?;
            let _ = started.send(result);
        }
        tokio::pin!(completion);
        let closed = async {
            match context.task().agent_children() {
                Some(owner) => owner.closed().await,
                None => std::future::pending().await,
            }
        };
        let mut control_error = None;
        let (output, (state, files, claims, retained)) = if let Some(cancellation) = cancellation {
            tokio::select! {
                result = &mut completion => result?,
                stopped = async { tokio::select! { result = cancellation.wait() => result, result = closed => result, _ = shutdown.cancelled() => Ok(()) } } => {
                    control.stop();
                    let completed = completion.await?;
                    control_error = stopped.err();
                    completed
                }
            }
        } else {
            completion.await?
        };
        // The native capture future owns and reaps the complete process tree.
        // Publish that fact before checkout capture or result presentation.
        files.completed().await?;
        let state = match state {
            Some(state) => state,
            None => lock_owned(StateArc::clone(&self.state)).await,
        };
        let result = output
            .and_then(|output| {
                output.ok_or_else(|| Error::Conflict("process capture was interrupted".into()))
            })
            .and_then(|output| {
                retained.settle(
                    ToolResult {
                        value: serde_json::json!({
                            "success": output.success, "exit_code": output.exit_code,
                            "spill": spill_path,
                            "stop": output.stop,
                        }),
                    },
                    maximum_output as u64,
                )
            });
        let result = match result {
            Ok((result, Some(file))) if result.value["spill"].is_string() => {
                let published = async {
                    let mut source = futures::io::AllowStdIo::new(file);
                    context
                        .stage_output_stream_once(
                            "spill.log",
                            &mut source,
                            ("application/octet-stream", "spill.log"),
                        )
                        .await?;
                    std::fs::remove_file(&native_spill).map_err(storage)?;
                    Ok(result)
                }
                .await;
                published
            }
            result => result.map(|(result, _)| result),
        };
        let result = match control_error {
            Some(error) => Error::after_cleanup(Err(error), result.map(|_| ())),
            None => result,
        };
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
