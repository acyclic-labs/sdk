use super::{
    ExternalAction, ExternalRequest,
    intake::LIFECYCLE,
    publication::{publish, storage},
    volume,
};
use crate::{
    Error, Result,
    distributed::JournalWrite,
    filesystem::{
        FilesystemTaskRuntime, TOOL_TASK_COMMAND_KIND, TaskCommandHost, TaskCommandProgress,
        ToolTaskCommand,
    },
    runtime::TaskContext,
    scheduler::LeaseFence,
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskVolume},
    workflow::WorkflowCommand,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde_json::Value;

mod children;
mod context;
mod launch;
pub(crate) use children::ChildAdmission;

/// Executes prepared request commands through the runtime's existing executors.
/// Started/result files use the same provider transaction as the task fence.
/// The actor prepares the pinned command; this adapter owns no task state.
pub struct RequestCommands<'a, P, A, O> {
    runtime: &'a FilesystemTaskRuntime<P, A, O>,
    requests: &'a TaskVolume<P>,
    results: &'a TaskVolume<P>,
    payloads: &'a dyn crate::distributed::SchedulerPayloadStore,
    children: Option<&'a dyn ChildAdmission>,
}

impl<'a, P, A, O> RequestCommands<'a, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds caller-writable requests and owner-writable results to one runtime.
    pub fn new(
        runtime: &'a FilesystemTaskRuntime<P, A, O>,
        requests: &'a TaskVolume<P>,
        results: &'a TaskVolume<P>,
        payloads: &'a dyn crate::distributed::SchedulerPayloadStore,
    ) -> Self {
        Self {
            runtime,
            requests,
            results,
            payloads,
            children: None,
        }
    }

    /// Shares the host's resource preparation for native command-session children.
    pub(crate) fn with_child_admission(mut self, children: &'a dyn ChildAdmission) -> Self {
        self.children = Some(children);
        self
    }

    fn dispatch(
        &self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
    ) -> BoxProviderFuture<'_, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let task = context
                .durable_task_id()
                .ok_or_else(|| Error::Unauthorized("request has no admitted task".into()))?;
            if !std::sync::Arc::ptr_eq(&context.harness, self.runtime.harness()) {
                return Err(Error::Unauthorized(
                    "request uses another runtime context".into(),
                ));
            }
            let owner = self
                .runtime
                .task_host()
                .journal_owner(task, &fence, None)
                .await?;
            let provider = owner.stream();
            if !self.requests.shares_provider(&provider) || !self.results.shares_provider(&provider)
            {
                return Err(Error::Unauthorized(
                    "request volumes use another task provider".into(),
                ));
            }
            owner.verify(reconcile, &mut None).await?;
            let task = Component::new(task.to_string()).map_err(storage)?;
            let operation = Component::new(command.operation_id.to_string()).map_err(storage)?;
            let key = FileKey::Task {
                task: task.clone(),
                file: TaskFile::Request(operation.clone()),
            };
            let mut snapshot = self
                .requests
                .snapshot(&Query::File(key), 1)
                .await
                .map_err(storage)?;
            let original = snapshot.files.pop();
            let request_volume = snapshot.generation.volume;
            drop(snapshot);
            let request = match original {
                Some(original) => ExternalRequest::from_file(&task, &original).map_err(storage)?,
                None => {
                    self.initial_child_request(&owner, &command, &payload, reconcile)
                        .await?
                }
            };
            if request.operation_id != command.operation_id {
                return Err(Error::Conflict(
                    "request differs from the retained command".into(),
                ));
            }
            let mut failure = validate_command(
                &request,
                &command.kind,
                &payload,
                self.runtime.tool_registry(),
            )?;
            let result_key = request.result_key(task.clone()).map_err(storage)?;
            let started = FileValue::new(
                FileKey::Task {
                    task,
                    file: TaskFile::RequestStarted(operation),
                },
                volume::encode(&command).map_err(storage)?,
            );
            let existing = self
                .results
                .snapshot(&Query::File(started.key.clone()), 1)
                .await
                .map_err(storage)?;
            if existing.generation.volume == request_volume {
                return Err(Error::Unauthorized(
                    "request and result volumes must be distinct".into(),
                ));
            }
            if reconcile && existing.files.first() != Some(&started) {
                return Err(Error::Conflict(
                    "request reconciliation has no original dispatch".into(),
                ));
            }
            drop(existing);
            let write = if reconcile {
                JournalWrite::Settlement
            } else {
                JournalWrite::Fresh
            };
            publish(
                self.results,
                &started,
                owner.condition(write, &mut None).await?,
                owner.admission().run_limits.deadline_epoch_ms,
            )
            .await?;
            let cached = self
                .results
                .snapshot(&Query::File(result_key.clone()), 1)
                .await
                .map_err(storage)?;
            if let Some(file) = cached.files.first() {
                return Ok(TaskCommandProgress::Ready(
                    volume::decode(&file.bytes).map_err(storage)?,
                ));
            }
            drop(cached);
            let attachment = super::RequestConsumer::authenticate(
                self.runtime,
                owner.task_id(),
                owner.admission(),
                &request,
            )
            .await?;
            let rejected = match &attachment {
                Err(rejected) => Some(*rejected),
                Ok(_) if matches!(request.action, ExternalAction::Join { .. }) => {
                    super::RequestConsumer::checked_child_completion(
                        self.runtime,
                        self.results,
                        owner.task_id(),
                        &request,
                    )
                    .await?
                    .err()
                }
                Ok(_) => None,
            };
            let mut cancelled = None;
            let progress = if let Some(rejected) = rejected {
                TaskCommandProgress::Ready(
                    serde_json::to_value(rejected.outcome())
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                )
            } else if super::launch::requested(&request) {
                let Ok(ExternalRequest {
                    action: ExternalAction::Attach { workspace },
                    ..
                }) = attachment
                else {
                    return Err(Error::Conflict(
                        "authenticated attachment is missing".into(),
                    ));
                };
                self.native_spawn(&context, &fence, &owner, &request, &workspace, reconcile)
                    .await?
            } else if command.kind == LIFECYCLE
                && matches!(request.action, ExternalAction::Attach { .. })
            {
                self.native_process(&context, &fence, &owner, &request, reconcile)
                    .await?
            } else if command.kind == LIFECYCLE
                && matches!(request.action, ExternalAction::Fork { .. })
            {
                let Ok(ExternalRequest {
                    action: ExternalAction::Attach { workspace },
                    ..
                }) = attachment
                else {
                    return Err(Error::Conflict(
                        "authenticated attachment is missing".into(),
                    ));
                };
                self.fork(
                    &context, &fence, &owner, &request, &workspace, None, reconcile,
                )
                .await?
            } else if command.kind == LIFECYCLE {
                if let ExternalAction::ToolFinished { call_id, .. } = &request.action {
                    let result_key = request
                        .original_tool_result_key(
                            Component::new(owner.task_id().to_string()).map_err(storage)?,
                            call_id,
                        )
                        .map_err(storage)?;
                    if self
                        .results
                        .snapshot(&Query::File(result_key), 1)
                        .await
                        .map_err(storage)?
                        .files
                        .is_empty()
                    {
                        failure = Some("native completion has no original settled tool result");
                    }
                }
                let value = match failure {
                    Some(message) => serde_json::to_value(crate::Outcome::<Value>::Failed {
                        message: message.into(),
                    })
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                    None => {
                        serde_json::json!({"task":owner.task_id(),"operation_id":request.operation_id})
                    }
                };
                TaskCommandProgress::Ready(value)
            } else if !matches!(request.action, ExternalAction::RunTool { .. }) {
                let commands = self.runtime.commands();
                if reconcile {
                    commands.reconcile(context, fence, command, payload).await?
                } else {
                    commands.execute(context, fence, command, payload).await?
                }
            } else {
                let context = if command.kind == TOOL_TASK_COMMAND_KIND
                    && payload.get("name").and_then(Value::as_str) == Some("write")
                {
                    context.with_created_project_output()?
                } else {
                    context
                };
                // Establish the cancellation cut before polling any execution.
                let follow = super::cancellation::prepare(
                    self.requests,
                    &request,
                    Component::new(owner.task_id().to_string()).map_err(storage)?,
                )
                .await?;
                drop(request);
                let token = acyclic_fs::CancellationToken::new();
                let commands = self.runtime.commands().with_execution_cancellation(&token);
                let execution = async {
                    if token.is_cancelled() && !reconcile {
                        Ok(TaskCommandProgress::Ready(
                            serde_json::to_value(crate::Outcome::<Value>::Failed {
                                message: "native tool cancelled before execution".into(),
                            })
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                        ))
                    } else if reconcile {
                        commands.reconcile(context, fence, command, payload).await
                    } else {
                        commands.execute(context, fence, command, payload).await
                    }
                };
                let (progress, cancel) =
                    super::cancellation::settle(execution, follow, || token.cancel()).await?;
                cancelled = cancel;
                progress
            };
            if let TaskCommandProgress::Ready(value) = &progress {
                let result = FileValue::new(result_key, volume::encode(value).map_err(storage)?);
                publish(
                    self.results,
                    &result,
                    owner.condition(JournalWrite::Settlement, &mut None).await?,
                    None,
                )
                .await?;
            }
            if let Some(cancel) = cancelled {
                if matches!(progress, TaskCommandProgress::Ready(_)) {
                    let task = Component::new(owner.task_id().to_string()).map_err(storage)?;
                    let acknowledgement = cancel
                        .result_file(
                            task,
                            &serde_json::json!({
                                "task":owner.task_id(),"operation_id":cancel.operation_id,
                            }),
                        )
                        .map_err(storage)?;
                    publish(
                        self.results,
                        &acknowledgement,
                        owner.condition(JournalWrite::Settlement, &mut None).await?,
                        None,
                    )
                    .await?;
                }
            }
            Ok(progress)
        })
    }
}

pub(super) fn validate_command(
    request: &ExternalRequest,
    kind: &str,
    payload: &Value,
    tools: &crate::tool::ToolRegistry,
) -> Result<Option<&'static str>> {
    if kind != LIFECYCLE {
        if let Some((expected_kind, expected)) = request.task_command()? {
            if kind != expected_kind || *payload != expected {
                return Err(Error::Conflict(
                    "task command differs from the original native request".into(),
                ));
            }
            return Ok(None);
        }
    }
    if kind == TOOL_TASK_COMMAND_KIND {
        let actual: ToolTaskCommand = serde::Deserialize::deserialize(payload)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if actual != request.tool_call_at(tools, Some(&actual.revision))? {
            return Err(Error::Conflict(
                "tool command differs from the original native request".into(),
            ));
        }
        return Ok(None);
    }
    if kind != LIFECYCLE {
        return Err(Error::Unsupported(
            "external command kind is not bound".into(),
        ));
    }
    let original: ExternalRequest = serde::Deserialize::deserialize(payload)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if original != *request {
        return Err(Error::Conflict(
            "lifecycle command differs from its native request".into(),
        ));
    }
    Ok(match &request.action {
        ExternalAction::RunTool { .. } => {
            Some("native tool request has no supported prepared tool command")
        }
        ExternalAction::Join { .. } => Some("native join has no prepared child wait"),
        _ => None,
    })
}

impl<P, A, O> TaskCommandHost for RequestCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        self.dispatch(context, fence, command, payload, false)
    }

    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        self.dispatch(context, fence, command, payload, true)
    }
}
