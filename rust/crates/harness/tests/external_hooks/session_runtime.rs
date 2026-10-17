use super::*;
use acyclic_fs::Fs;
use acyclic_harness::task_volume::{Component, Query, TaskVolume};
use acyclic_harness::{
    AgentId, Capabilities, OperationId, TaskId,
    conversation::{
        ContentResidencyVerifier, Limits, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    distributed::{SchedulerPayloadStore, Worker},
    durable_tool::ResumableToolRegistry,
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost,
        FilesystemTaskRuntime, TOOL_TASK_COMMAND_KIND, TaskCommandHost, TaskWorkerAttempt,
        TaskWorkerOutcome, ToolTaskCommand,
    },
    resources::ProviderRef,
    runtime::{RuntimeScope, TaskRegistry, TaskStateProvider},
    scheduler::{ResourceRequest, SessionLimits},
    workflow::{MachineRegistry, WorkflowCommand},
};
use acyclic_stream::{MemoryStream, StreamPath, SystemUnixMillisClock};
use proptest::prelude::*;
use std::{collections::BTreeMap, sync::Arc};

#[path = "session_runtime/fixture.rs"]
mod fixture;
pub(crate) use fixture::Fixture;

#[tokio::test]
async fn session_executes_the_registered_write_tool_through_the_real_worker() -> Result<()> {
    Box::pin(run_write_session(VolumeBinding::Separate)).await
}

#[tokio::test]
async fn request_volumes_cannot_bypass_the_task_fence_with_another_provider() -> Result<()> {
    Box::pin(run_write_session(VolumeBinding::Foreign)).await
}

#[tokio::test]
async fn request_permissions_cannot_include_owner_result_files() -> Result<()> {
    Box::pin(run_write_session(VolumeBinding::Shared)).await
}

#[tokio::test]
async fn prepared_command_cannot_change_the_native_tool_arguments() -> Result<()> {
    Box::pin(run_write_session(VolumeBinding::ChangedArguments)).await
}

#[tokio::test]
async fn a_lifecycle_command_cannot_report_a_tool_as_successful() -> Result<()> {
    Box::pin(run_write_session(VolumeBinding::ForgedLifecycle)).await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Every retained tool revision remains executable after selecting a newer
    // catalog revision; the exact original arguments and result are preserved.
    #[test]
    fn retained_command_ignores_later_catalog_selection(revision in "r[a-z]{1,8}") {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()
            .map_err(|error| Error::Storage(error.to_string()))?;
        runtime.block_on(Box::pin(run_write_session_at(VolumeBinding::RetainedRevision, &revision)))?;
    }
}
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum VolumeBinding {
    Separate,
    Foreign,
    Shared,
    ChangedArguments,
    ForgedLifecycle,
    RetainedRevision,
    Recursive,
}

async fn run_write_session(binding: VolumeBinding) -> Result<()> {
    Box::pin(run_write_session_at(binding, "2")).await
}

async fn run_write_session_at(binding: VolumeBinding, selected_revision: &str) -> Result<()> {
    let mut fixture = Fixture::at_revision(binding, selected_revision).await?;
    let operation = OperationId::from_bytes([63; 16]);
    let task = TaskId::from_bytes(operation.into_bytes());
    let task_folder = Component::new(task.to_string()).map_err(storage)?;
    // Establish the actual native attachment first. Bad provider bindings below
    // still exercise dispatch, independently of this authenticated bootstrap.
    let actual_results = if matches!(binding, VolumeBinding::Shared | VolumeBinding::Foreign) {
        let bootstrap = TaskVolume::open(
            fixture.stream.clone(),
            StreamPath::new("bootstrap-results").map_err(storage)?,
        )
        .await
        .map_err(storage)?;
        Some(std::mem::replace(&mut fixture.results, bootstrap))
    } else {
        None
    };
    let attach = request(
        ExternalHost::Pi,
        "session_start",
        json!({"sessionId":"session","cwd":"/project"}),
    )?;
    super::intake::deliver(&fixture, &task_folder, &attach).await?;
    if let Some(results) = actual_results {
        fixture.results = results;
    }
    let Fixture {
        stream,
        runtime,
        files,
        volume,
        verifier,
        signed,
        tasks: _,
        revision,
        publisher,
        requests,
        results,
    } = fixture;
    let arguments = json!({
        "path":"session.txt","text":"written by the task worker",
        "media_type":"text/plain","display_name":"session.txt"
    });
    let original = request(
        ExternalHost::Pi,
        "tool_call",
        json!({
            "sessionId":"session", "toolCallId":"call", "toolName":"write", "input":arguments
        }),
    )?;
    let tool_operation = original.operation_id;
    let file = original.to_file(task_folder.clone()).map_err(storage)?;
    let snapshot = requests
        .snapshot(&Query::File(file.key.clone()), 1)
        .await
        .map_err(storage)?;
    requests
        .append(snapshot.generation, &file)
        .await
        .map_err(storage)?;
    let mut arguments = arguments;
    if binding == VolumeBinding::ChangedArguments {
        arguments["text"] = json!("changed after immutable request publication");
    }
    let payload = if binding == VolumeBinding::ForgedLifecycle {
        serde_json::to_vec(&original)
    } else {
        serde_json::to_vec(&ToolTaskCommand {
            name: "write".into(),
            revision,
            arguments,
        })
    }
    .map_err(|error| invalid(&error))?;
    let payload =
        SchedulerPayloadStore::stage(publisher.as_ref(), tool_operation, "tool", &payload, None)?
            .await?;
    let command = WorkflowCommand {
        operation_id: tool_operation,
        kind: if binding == VolumeBinding::ForgedLifecycle {
            "acyclic.external.lifecycle.v1"
        } else {
            TOOL_TASK_COMMAND_KIND
        }
        .into(),
        payload,
    };
    let input = serde_json::to_vec(&json!({"command":command,"terminal":true})).map_err(storage)?;
    let input = SchedulerPayloadStore::stage_input(
        publisher.as_ref(),
        tool_operation,
        "native-session-command",
        &input,
        None,
    )?
    .await?;
    let reader = FilesystemContentVerifier::new(files.clone(), &verifier, &signed, 65_536, None)?;
    let projection = acyclic_harness::distributed::DistributedCoordinator::open(
        &stream,
        reader,
        Some(publisher.clone()),
        &mut None,
    )
    .await?;
    let slot = projection
        .scheduler()
        .operation(operation)
        .and_then(|state| state.workflow.as_ref())
        .ok_or_else(|| Error::Conflict("attached session has no input slot".into()))?;
    runtime
        .task_host()
        .resume_workflow(
            task,
            slot.revision,
            input,
            acyclic_harness::IdempotencyKey::new("native-session-command")?,
            None,
            &mut None,
        )
        .await?;
    let worker = Worker {
        id: "session-worker".into(),
        available: ResourceRequest::default(),
        labels: BTreeMap::new(),
    };
    let commands = RequestCommands::new(&runtime, &requests, &results, publisher.as_ref());
    let guarded = SizeGuard(&commands);
    let previous_results = results
        .snapshot(&Query::Requests(task_folder.clone()), 64)
        .await
        .map_err(storage)?
        .files;
    let attempt = Box::pin(runtime.run_operation(&worker, task, &guarded, 8, None)).await?;
    if !matches!(
        binding,
        VolumeBinding::Separate | VolumeBinding::RetainedRevision
    ) {
        if binding == VolumeBinding::ForgedLifecycle {
            assert!(matches!(
                attempt,
                Some(TaskWorkerAttempt::Progress(
                    TaskWorkerOutcome::Suspended { .. }
                ))
            ));
            assert_eq!(
                runtime.task_host().outcome(task).await?,
                None,
                "a rejected terminal command cannot close the attached session"
            );
            let result = results
                .snapshot(
                    &Query::File(original.result_key(task_folder.clone()).map_err(storage)?),
                    1,
                )
                .await
                .map_err(storage)?;
            assert_eq!(result.files.len(), 1);
            let outcome: acyclic_harness::Outcome<Value> =
                serde_json::from_slice(&result.files[0].bytes).map_err(|error| invalid(&error))?;
            assert!(matches!(outcome, acyclic_harness::Outcome::Failed { .. }));
        } else {
            assert!(matches!(
                attempt,
                Some(TaskWorkerAttempt::Progress(
                    TaskWorkerOutcome::Reconciling { .. }
                ))
            ));
        }
        let actor_files = results
            .snapshot(&Query::Requests(task_folder), 64)
            .await
            .map_err(storage)?
            .files;
        if binding != VolumeBinding::ForgedLifecycle {
            assert_eq!(
                actor_files, previous_results,
                "uncertain dispatch cannot change the authentic bootstrap's results"
            );
        }
        let reader = FilesystemContentVerifier::new(files, &verifier, &signed, 65_536, None)?;
        assert!(
            reader
                .read_private_path(&volume, "", "session.txt", None, 65_536, None)?
                .await
                .is_err()
        );
        return Ok(());
    }
    match attempt {
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task: completed })) => {
            assert_eq!(completed, task)
        }
        Some(TaskWorkerAttempt::Unresolved { error, .. }) => return Err(error),
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { revision, .. })) => {
            return Err(Error::Storage(format!(
                "command session suspended at {revision}"
            )));
        }
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { .. })) => {
            return Err(Error::Storage(
                "command session requires reconciliation".into(),
            ));
        }
        Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { .. })) => {
            return Err(Error::Storage(
                "command session exhausted its transition allowance".into(),
            ));
        }
        None => return Err(Error::Storage("command session was not runnable".into())),
    }
    assert_eq!(
        runtime.task_host().outcome(task).await?,
        Some(acyclic_harness::Outcome::Succeeded(Value::Null))
    );
    let reader = FilesystemContentVerifier::new(files, &verifier, &signed, 65_536, None)?;
    let (file, content, _) = reader
        .read_private_path(&volume, "", "session.txt", None, 65_536, None)?
        .await?;
    assert_eq!(file.path(), "session.txt");
    assert_eq!(&*content, b"written by the task worker");
    let result = results
        .snapshot(
            &Query::File(original.result_key(task_folder).map_err(storage)?),
            1,
        )
        .await
        .map_err(storage)?;
    assert_eq!(result.files.len(), 1);
    let outcome: acyclic_harness::Outcome<acyclic_harness::tool::files::FileResult> =
        serde_json::from_slice(&result.files[0].bytes).map_err(|error| invalid(&error))?;
    let acyclic_harness::Outcome::Succeeded(written) = outcome else {
        return Err(Error::Storage("write command did not succeed".into()));
    };
    assert_eq!(written.file.volume(), &volume);
    assert_eq!(written.file.path(), "session.txt");
    let answer = original
        .send(
            &requests,
            &results,
            Component::new(task.to_string()).map_err(storage)?,
        )
        .await
        .map_err(storage)?;
    assert_eq!(
        answer,
        serde_json::from_slice::<Value>(&result.files[0].bytes).map_err(|error| invalid(&error))?
    );
    // Completed work cannot re-execute its publication on a client retry.
    assert!(
        runtime
            .run_operation(&worker, task, &commands, 8, None)
            .await?
            .is_none()
    );
    Ok(())
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

// Measure the actual dispatch future supplied with the worker's owned context.
struct SizeGuard<'a>(&'a dyn TaskCommandHost);

impl TaskCommandHost for SizeGuard<'_> {
    fn execute<'a>(
        &'a self,
        context: acyclic_harness::runtime::TaskContext,
        fence: acyclic_harness::scheduler::LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        Result<acyclic_harness::filesystem::TaskCommandProgress>,
    > {
        let future = self.0.execute(context, fence, command, payload);
        let bytes = std::mem::size_of_val(&future);
        eprintln!("external request command future: {bytes} bytes");
        assert!(
            bytes < 16 * 1024,
            "external request command future: {bytes} bytes"
        );
        future
    }
}
