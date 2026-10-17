//! A native program is a separate durable child; its hook slot stays free.
use super::*;
use crate::{
    external_executor::launch::{self, Launch, PROCESS_COMMAND},
    filesystem::{TaskCommandHost, TaskCommandProgress},
    runtime::{TaskContext, TaskStateProvider},
    scheduler::LeaseFence,
    task_volume::ToolFile,
    workflow::WorkflowCommand,
};
use acyclic_stream::BoxProviderFuture;
use serde_json::json;
use std::io::{Read as _, Write as _};

mod completion;
mod output;
use output::Output;

struct Commands<'a> {
    harness: &'a LocalHarness,
    volumes: Arc<Volumes>,
    cancellation: &'a acyclic_fs::CancellationToken,
}

impl LocalHarness {
    pub(super) async fn advance_process(
        &self,
        task: TaskId,
        volumes: Arc<Volumes>,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<()> {
        self.advance(
            task,
            &Commands {
                harness: self,
                volumes,
                cancellation,
            },
        )
        .await
    }
}

impl Commands<'_> {
    fn run<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let task = context
                .durable_task_id()
                .ok_or_else(|| Error::Unauthorized("native process has no task".into()))?;
            if command.kind != PROCESS_COMMAND
                || command.operation_id.into_bytes() != task.into_bytes()
            {
                return Err(Error::Unauthorized(
                    "native process command differs from its task".into(),
                ));
            }
            if !context.original_task_binding_matches(task, &fence)
                || !Arc::ptr_eq(&context.harness, self.harness.runtime.harness())
            {
                return Err(Error::Unauthorized(
                    "native command uses another execution owner".into(),
                ));
            }
            let owner = context.ordinary_journal_owner::<LocalStream>()?;
            if !self.volumes.results.shares_provider(&owner.stream()) {
                return Err(Error::Unauthorized(
                    "native results use another task provider".into(),
                ));
            }
            owner.verify(reconcile, &mut None).await?;
            if !context.scope().grants().contains("tool:call:bash") {
                return Err(Error::Unauthorized(
                    "native task lacks process execution authority".into(),
                ));
            }
            let parent = owner.admission().parent.ok_or_else(|| {
                Error::Unauthorized("native process has no session parent".into())
            })?;
            let attached = self
                .harness
                .runtime
                .task_host()
                .observe_admission(parent)
                .await?;
            if command.operation_id != launch::process_operation(parent)
                || attached.input.get("executor")
                    != Some(&serde_json::to_value(&command.payload).map_err(storage)?)
            {
                return Err(Error::Unauthorized(
                    "native process is not the session's retained executor".into(),
                ));
            }
            let launch: Launch = serde::Deserialize::deserialize(payload)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let spawn = launch::Spawn::from_request(&launch.request)?;
            if attached.parent != Some(launch.parent)
                || spawn.attachment(&launch.request, "")?.task_id() != parent
            {
                return Err(Error::Unauthorized(
                    "native process selected a foreign session".into(),
                ));
            }
            drop(launch);
            let output = Output {
                owner,
                volumes: self.volumes.clone(),
                call: command.operation_id,
                sequence: (0, 0),
                runtime: tokio::runtime::Handle::current(),
                failed: None,
            };
            let cached = output
                .volumes
                .results
                .snapshot(&Query::File(output.key(ToolFile::Result)?), 1)
                .await
                .map_err(storage)?;
            if let Some(result) = cached.files.first() {
                return Ok(TaskCommandProgress::Ready(
                    crate::external_executor::volume::decode(&result.bytes).map_err(storage)?,
                ));
            }
            if reconcile {
                // Dispatch was already recorded by the worker. Lost custody or
                // output never authorizes launching the native program again.
                return Ok(TaskCommandProgress::Indeterminate);
            }
            output
                .publish(
                    ToolFile::Request,
                    crate::external_executor::volume::encode(&command).map_err(storage)?,
                    false,
                )
                .await?;
            let workspace = self.harness.workspace.as_ref().ok_or_else(|| {
                Error::Unsupported("native process needs a local workspace".into())
            })?;
            let root = crate::local::private_directory(&launch::process_directory(
                &workspace.mount_directory,
                parent,
            ))?;
            let binding = crate::external_executor::HookBinding::Local {
                workspace: workspace.path.clone(),
                task: Some(parent),
            };
            let binding_file = root.join("binding.json");
            cache(
                &binding_file,
                &crate::external_executor::volume::encode(&binding).map_err(storage)?,
            )?;
            let session = root.join("session.jsonl");
            let seed = match (
                spawn.executor.context.as_ref(),
                attached.input.get("native_context"),
            ) {
                (Some(_), Some(file)) => {
                    let file: crate::conversation::FileRef =
                        serde::Deserialize::deserialize(file).map_err(storage)?;
                    let bytes = context.read_file(&file, None).await?;
                    crate::external_executor::native_context::rebase(
                        &bytes,
                        &spawn.executor.session.session_id,
                        &root,
                    )?
                }
                (None, None) => spawn.seed(&root)?,
                _ => {
                    return Err(Error::Unauthorized(
                        "native context differs from its preparation".into(),
                    ));
                }
            };
            cache(&session, &seed)?;
            let identity = acyclic_fs::capture_root_identity(&root).map_err(storage)?;
            let argv = spawn.arguments(&session)?;
            let invocation = crate::filesystem::native_process::Invocation {
                executable: spawn.executor.executable,
                argv,
                cwd: root.clone(),
                environment: std::collections::BTreeMap::from([(
                    "ACYCLIC_HARNESS_BINDING".into(),
                    binding_file
                        .to_str()
                        .ok_or_else(|| Error::Invalid("native binding path is not UTF-8".into()))?
                        .into(),
                )]),
                inherit_environment: true,
                timeout_ms: spawn.executor.timeout_ms,
                control_timeout_ms: 1000,
                cancellation_poll_ms: 25,
                maximum_output_bytes: usize::try_from(context.scope().limits().render_bytes)
                    .map_err(storage)?,
                standard_input: None,
                mcp_stdio: None,
            };
            let owner = output.owner.clone();
            let cancelled = self.cancellation.clone();
            let runtime = tokio::runtime::Handle::current();
            let (result, mut output) =
                crate::filesystem::native_process::capture::capture_with_custody(
                    invocation,
                    |output: &mut Output, chunk| output.observe(chunk),
                    move || {
                        cancelled
                            .check()
                            .map_err(crate::filesystem::native_process::capture::control_error)?;
                        if acyclic_fs::capture_root_identity(&root)
                            .map_err(std::io::Error::other)?
                            != identity
                        {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::PermissionDenied,
                                "native process view was replaced",
                            ));
                        }
                        runtime.block_on(async {
                            tokio::time::timeout(
                                std::time::Duration::from_millis(1000),
                                owner.verify(false, &mut None),
                            )
                            .await
                            .map_err(crate::filesystem::native_process::capture::control_error)?
                            .map_err(crate::filesystem::native_process::capture::control_error)
                        })
                    },
                    output,
                )
                .await?;
            if let Some(error) = output.failed.take() {
                return Err(error);
            }
            let Some(result) = result else {
                return Ok(TaskCommandProgress::Indeterminate);
            };
            if result
                .stop
                .as_ref()
                .is_some_and(|stop| !stop.cleanup_completed)
            {
                return Ok(TaskCommandProgress::Indeterminate);
            }
            output.owner.verify(true, &mut None).await?;
            let mut bytes = Vec::new();
            let bound = context.scope().limits().file_bytes.saturating_add(1);
            std::fs::File::open(&session)
                .map_err(storage)?
                .take(bound)
                .read_to_end(&mut bytes)
                .map_err(storage)?;
            let transcript = context
                .stage_private_file_once(
                    artifact(task, b"session"),
                    "native/session.jsonl",
                    &bytes,
                    None,
                )
                .await?;
            let value = json!({
                "success":result.success,"exit_code":result.exit_code,"stop":result.stop,
                "session":transcript,
                "stdout":{"volume":output.volumes.results.path().as_str(),"task":task,"call":command.operation_id,"chunks":output.sequence.0},
                "stderr":{"volume":output.volumes.results.path().as_str(),"task":task,"call":command.operation_id,"chunks":output.sequence.1},
            });
            output
                .publish(
                    ToolFile::Result,
                    crate::external_executor::volume::encode(&value).map_err(storage)?,
                    true,
                )
                .await?;
            Ok(TaskCommandProgress::Ready(value))
        })
    }
}

impl TaskCommandHost for Commands<'_> {
    fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        Some(self.cancellation)
    }
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        self.run(context, fence, command, payload, false)
    }
    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        self.run(context, fence, command, payload, true)
    }
}

fn cache(path: &Path, bytes: &[u8]) -> Result<()> {
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => file.write_all(bytes).map_err(storage),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let file = std::fs::File::open(path).map_err(storage)?;
            let mut stored = Vec::new();
            file.take(bytes.len() as u64 + 1)
                .read_to_end(&mut stored)
                .map_err(storage)?;
            if stored == bytes {
                Ok(())
            } else {
                Err(Error::Conflict("native input cache changed".into()))
            }
        }
        Err(error) => Err(storage(error)),
    }
}

fn artifact(task: TaskId, name: &[u8]) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.native-artifact/v1\0");
    hash.update(&task.into_bytes());
    hash.update(name);
    let mut id = [0; 16];
    id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(id)
}
