//! Admission of native sessions and their separate, owned process command.
use super::*;
use crate::{conversation::FileRef, durable_host::TaskJournalOwner};

impl<P, A, O> RequestCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn native_spawn<'a>(
        &'a self,
        context: &'a TaskContext,
        fence: &'a LeaseFence,
        owner: &'a TaskJournalOwner<P>,
        request: &'a ExternalRequest,
        workspace: &'a str,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let spawn = match super::super::launch::Spawn::from_request(request) {
                Ok(spawn) => spawn,
                Err(error) => return failed(error.to_string()),
            };
            if !cfg!(all(feature = "native-execution", feature = "hooks"))
                || self.children.is_none()
                || !context.scope().grants().contains("tool:call:bash")
            {
                return failed("native process execution is not bound to this task".into());
            }
            let selected = match self
                .native_context(context, &owner, request, &spawn, workspace, reconcile)
                .await
            {
                Ok(selected) => selected,
                Err(Error::Invalid(message)) => return failed(message),
                Err(error) => return Err(error),
            };
            let child = spawn.fork(request);
            drop(spawn);
            let bytes = match super::super::launch::encode(&super::super::launch::Launch {
                parent: owner.task_id(),
                request,
            }) {
                Ok(bytes) if bytes.len() as u64 <= context.scope().limits().file_bytes => bytes,
                _ => {
                    return failed("native executor declaration exceeds its file allowance".into());
                }
            };
            let executor = self
                .payloads
                .stage_input(request.operation_id, "native-launch", &bytes, None)?
                .await?;
            self.fork(
                context,
                fence,
                owner,
                &child,
                workspace,
                Some(super::super::launch::Prepared {
                    executor,
                    context: selected,
                }),
                reconcile,
            )
            .await
        })
    }

    pub(super) fn native_process<'a>(
        &'a self,
        context: &'a TaskContext,
        fence: &'a LeaseFence,
        owner: &'a TaskJournalOwner<P>,
        request: &'a ExternalRequest,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let Some(executor) = owner.admission().input.get("executor") else {
                return acknowledgement(owner, request);
            };
            let executor: FileRef = serde::Deserialize::deserialize(executor)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let (launch, spawn) =
                super::super::launch::Launch::decode(&context.read_file(&executor, None).await?)?;
            if owner.admission().parent != Some(launch.parent)
                || spawn.fork(&launch.request).child_attachment("")?.task_id() != owner.task_id()
            {
                return Err(Error::Unauthorized(
                    "native executor declaration belongs to another session".into(),
                ));
            }
            drop((launch, spawn));
            if !cfg!(all(feature = "native-execution", feature = "hooks"))
                || self.children.is_none()
                || !context.scope().grants().contains("tool:call:bash")
            {
                return native_failed("native process execution is not bound to this task".into());
            }
            let operation = super::super::launch::process_operation(owner.task_id());
            let command = WorkflowCommand {
                operation_id: operation,
                kind: super::super::launch::PROCESS_COMMAND.into(),
                payload: executor.clone(),
            };
            let admission = self
                .runtime
                .harness()
                .registered_admission(
                    operation,
                    "acyclic.command_session",
                    "1",
                    serde_json::json!({"command":command,"terminal":true}),
                    Some((owner.task_id(), context.scope())),
                    None,
                    None,
                )?
                .0;
            match self
                .admit_child(context, fence, owner, admission, vec![executor], reconcile)
                .await?
            {
                TaskCommandProgress::Ready(value) => {
                    match serde::Deserialize::deserialize(&value)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                    {
                        crate::Outcome::<crate::TaskId>::Succeeded(task)
                            if task.into_bytes() == operation.into_bytes() =>
                        {
                            acknowledgement(owner, request)
                        }
                        crate::Outcome::Failed { message } => native_failed(message),
                        _ => Err(Error::Conflict("native process admission changed".into())),
                    }
                }
                other => Ok(other),
            }
        })
    }
}

fn acknowledgement<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    request: &ExternalRequest,
) -> Result<TaskCommandProgress> {
    Ok(TaskCommandProgress::Ready(
        serde_json::json!({"task":owner.task_id(),"operation_id":request.operation_id}),
    ))
}

fn failed(message: String) -> Result<TaskCommandProgress> {
    serde_json::to_value(crate::Outcome::<Value>::Failed { message })
        .map(TaskCommandProgress::Ready)
        .map_err(|error| Error::Invalid(error.to_string()))
}

fn native_failed(message: String) -> Result<TaskCommandProgress> {
    Ok(TaskCommandProgress::Ready(
        serde_json::json!({"native_exit":crate::Outcome::<Value>::Failed {message}}),
    ))
}
