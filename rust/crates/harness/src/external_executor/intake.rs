use super::rejection::{Checked, RequestRejection};
use super::{ExternalAction, ExternalRequest, publication::storage, volume};
use crate::{
    Admission, Error, IdempotencyKey, OperationId, Result, TaskId,
    distributed::SchedulerPayloadStore,
    filesystem::{FilesystemTaskRuntime, TOOL_TASK_COMMAND_KIND},
    runtime::{RuntimeTask, TaskStateProvider},
    task_volume::{Component, FileValue, Query, TaskVolume},
    workflow::WorkflowCommand,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::StreamProvider;

pub(crate) const LIFECYCLE: &str = "acyclic.external.lifecycle.v1";

/// One request-file delivery into the existing session workflow. This consumer
/// retains no queue or task map; admission and the workflow slot own retries.
pub struct RequestConsumer<'a, P, A, O> {
    runtime: &'a FilesystemTaskRuntime<P, A, O>,
    requests: &'a TaskVolume<P>,
    results: &'a TaskVolume<P>,
    payloads: &'a dyn SchedulerPayloadStore,
    workspace: &'a str,
}

impl<'a, P, A, O> RequestConsumer<'a, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds the same provider-backed task runtime and its immutable input store.
    pub fn new(
        runtime: &'a FilesystemTaskRuntime<P, A, O>,
        requests: &'a TaskVolume<P>,
        results: &'a TaskVolume<P>,
        payloads: &'a dyn SchedulerPayloadStore,
        workspace: &'a str,
    ) -> Self {
        Self {
            runtime,
            requests,
            results,
            payloads,
            workspace,
        }
    }

    /// Authenticates a native stop against both immutable attachments before
    /// delivering the child's terminal request and then the parent's join.
    pub async fn child_completion(
        runtime: &FilesystemTaskRuntime<P, A, O>,
        results: &TaskVolume<P>,
        parent_task: TaskId,
        request: &ExternalRequest,
    ) -> Result<ExternalRequest> {
        let parent = runtime.task_host().observe_admission(parent_task).await?;
        Self::authenticate(runtime, parent_task, &parent, request)
            .await?
            .map_err(RequestRejection::error)?;
        Self::checked_child_completion(runtime, results, parent_task, request)
            .await?
            .map_err(RequestRejection::error)
    }

    pub(super) async fn checked_child_completion(
        runtime: &FilesystemTaskRuntime<P, A, O>,
        results: &TaskVolume<P>,
        parent_task: TaskId,
        request: &ExternalRequest,
    ) -> Result<Checked<ExternalRequest>> {
        let completion = request.child_completion()?;
        let child = match runtime
            .task_host()
            .observe_admission(completion.task_id())
            .await
        {
            Ok(child) => child,
            Err(Error::NotFound(_)) => return Ok(Err(RequestRejection::ChildNotAdmitted)),
            Err(error) => return Err(error),
        };
        if child.parent != Some(parent_task) {
            return Ok(Err(RequestRejection::ForeignChild));
        }
        if !results.shares_provider(&runtime.task_host().workflow_stream()) {
            return Err(Error::Unauthorized(
                "native child bindings use another task provider".into(),
            ));
        }
        if let Err(rejected) = super::child_binding::require(results, parent_task, request).await? {
            return Ok(Err(rejected));
        }
        Ok(
            Self::authenticate(runtime, completion.task_id(), &child, &completion)
                .await?
                .map(|_| completion),
        )
    }

    pub(super) async fn authenticate(
        runtime: &FilesystemTaskRuntime<P, A, O>,
        task: TaskId,
        admission: &crate::runtime::TaskAdmissionRecord,
        request: &ExternalRequest,
    ) -> Result<Checked<ExternalRequest>> {
        let Some(command) = admission.input.get("command").cloned() else {
            return Ok(Err(RequestRejection::NotAttached));
        };
        let initial: WorkflowCommand =
            serde_json::from_value(command).map_err(|error| Error::Invalid(error.to_string()))?;
        if initial.kind != LIFECYCLE {
            return Ok(Err(RequestRejection::NotAttached));
        }
        let context = runtime
            .harness()
            .durable_context(task, admission.operation_id, None)
            .await?;
        let original: ExternalRequest =
            volume::decode(&context.read_file(&initial.payload, None).await?).map_err(storage)?;
        if !matches!(original.action, ExternalAction::Attach { .. })
            || (original.host, &original.session_id, &original.caller)
                != (request.host, &request.session_id, &request.caller)
        {
            return Ok(Err(RequestRejection::NativeSession));
        }
        Ok(Ok(original))
    }

    /// Admits Attach or fills the current suspended session slot exactly once.
    /// The actor schedules its existing worker after this delivery is accepted.
    pub fn accept<'b>(
        &'b self,
        task: &'b Component,
        file: &'b FileValue,
    ) -> impl Future<Output = Result<()>> + 'b {
        Box::pin(async move {
            let request = ExternalRequest::from_file(task, file).map_err(storage)?;
            let task_id = TaskId::from_bytes(
                uuid::Uuid::parse_str(task.as_str())
                    .map_err(|_| Error::Invalid("external task folder is not a task ID".into()))?
                    .into_bytes(),
            );
            if task.as_str() != task_id.to_string() {
                return Err(Error::Invalid(
                    "external task folder is not canonical".into(),
                ));
            }
            let provider = self.runtime.task_host().workflow_stream();
            if !self.requests.shares_provider(&provider) || !self.results.shares_provider(&provider)
            {
                return Err(Error::Unauthorized(
                    "request volumes use another task provider".into(),
                ));
            }
            let source = self
                .requests
                .snapshot(&Query::File(file.key.clone()), 1)
                .await
                .map_err(storage)?;
            if source.files.first() != Some(file) {
                return Err(Error::Conflict(
                    "delivery differs from the immutable request file".into(),
                ));
            }
            let result = self
                .results
                .snapshot(
                    &Query::File(request.result_key(task.clone()).map_err(storage)?),
                    1,
                )
                .await
                .map_err(storage)?;
            if source.generation.volume == result.generation.volume {
                return Err(Error::Unauthorized(
                    "request and result volumes must be distinct".into(),
                ));
            }
            if let ExternalAction::Attach { workspace } = &request.action {
                if workspace != self.workspace {
                    return Err(Error::Unauthorized(
                        "native workspace differs from the configured project".into(),
                    ));
                }
                let command = self.command(&request).await?;
                let mut input = serde_json::json!({"command":command,"terminal":false});
                let definition = self
                    .runtime
                    .harness()
                    .task::<serde_json::Value, serde_json::Value>("acyclic.command_session@1")?;
                if let Some((admitted, _)) = self
                    .runtime
                    .harness()
                    .reconcile_admission(OperationId::from_bytes(task_id.into_bytes()))
                    .await?
                {
                    let retained = self.runtime.task_host().observe_admission(admitted).await?;
                    if retained.parent.is_some() {
                        // Only the fenced parent can admit this input reference;
                        // native SessionStart retries retain that exact declaration.
                        for name in ["executor", "native_context"] {
                            let Some(file) = retained.input.get(name) else {
                                continue;
                            };
                            let _: crate::conversation::FileRef =
                                serde::Deserialize::deserialize(file)
                                    .map_err(|error| Error::Invalid(error.to_string()))?;
                            input[name] = file.clone();
                        }
                    }
                    if admitted != task_id
                        || retained.task.name != "acyclic.command_session"
                        || retained.input != input
                    {
                        return Err(Error::Conflict(
                            "native attachment differs from its retained admission".into(),
                        ));
                    }
                    return Ok(());
                }
                return match self
                    .runtime
                    .harness()
                    .admit(
                        OperationId::from_bytes(task_id.into_bytes()),
                        &definition,
                        input,
                        None,
                    )
                    .await?
                {
                    Admission::Accepted(RuntimeTask::Durable {
                        task_id: admitted, ..
                    }) if admitted == task_id => Ok(()),
                    Admission::Rejected { reason } => Err(Error::Conflict(reason)),
                    _ => Err(Error::Indeterminate(request.operation_id)),
                };
            }
            let admission = self.runtime.task_host().observe_admission(task_id).await?;
            // The sole fenced command handler authenticates before execution;
            // definite input rejection is an outcome, not a stopped consumer.
            if !result.files.is_empty() {
                return Ok(());
            }
            let command = self.command(&request).await?;
            // Detach closes a native connection, not its resumable task.
            let terminal = admission.parent.is_some()
                && admission.input.get("executor").is_none()
                && matches!(request.action, ExternalAction::Detach);
            let input = serde_json::json!({"command":command,"terminal":terminal});
            let (phase, slot) = self
                .runtime
                .task_host()
                .workflow_slot(task_id, None, &mut None)
                .await?
                .ok_or_else(|| Error::Conflict("external session is not suspended".into()))?;
            if slot.waiting_command.is_some() {
                return Err(Error::Conflict(
                    "external session still owns a pending command".into(),
                ));
            }
            let bytes = volume::encode(&input).map_err(storage)?;
            let key = format!("external-resume:{}", request.operation_id);
            let payload = self
                .payloads
                .stage_input(request.operation_id, &key, &bytes, None)?
                .await?;
            if slot.input.as_ref() == Some(&payload) {
                return Ok(());
            }
            if phase != crate::scheduler::OperationPhase::Suspended {
                return Err(Error::Conflict(
                    "external session already has an admitted input".into(),
                ));
            }
            self.runtime
                .task_host()
                .resume_workflow(
                    task_id,
                    slot.revision,
                    payload,
                    IdempotencyKey::new(key)?,
                    None,
                    &mut None,
                )
                .await
        })
    }

    async fn command(&self, request: &ExternalRequest) -> Result<WorkflowCommand> {
        let (kind, bytes) = match &request.action {
            ExternalAction::RunTool { .. } | ExternalAction::Join { .. } => {
                match request.task_command() {
                    Ok(Some((kind, payload))) => (kind, volume::encode(&payload).map_err(storage)?),
                    Err(_) => (LIFECYCLE, volume::encode(request).map_err(storage)?),
                    Ok(None) => match request.tool_call(self.runtime.tool_registry()) {
                        Ok(input) => (
                            TOOL_TASK_COMMAND_KIND,
                            volume::encode(&input).map_err(storage)?,
                        ),
                        Err(_) => (LIFECYCLE, volume::encode(request).map_err(storage)?),
                    },
                }
            }
            ExternalAction::Attach { .. }
            | ExternalAction::Detach
            | ExternalAction::CancelTool { .. }
            | ExternalAction::HostTool { .. }
            | ExternalAction::ToolFinished { .. }
            | ExternalAction::Fork { .. } => (LIFECYCLE, volume::encode(request).map_err(storage)?),
        };
        let key = format!("external-command:{}", request.operation_id);
        let payload = self
            .payloads
            .stage_input(request.operation_id, &key, &bytes, None)?
            .await?;
        Ok(WorkflowCommand {
            operation_id: request.operation_id,
            kind: kind.into(),
            payload,
        })
    }
}
