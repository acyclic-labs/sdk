//! A native host's child attaches through the existing fenced admission path.
use super::*;
use crate::{Admission, TaskId, durable_host::TaskJournalOwner, runtime::TaskSpawner};

/// Binds a native child to the same resource preparation as other child tasks.
pub(crate) trait ChildAdmission: Send + Sync {
    fn native_workspace(&self, task: TaskId) -> Result<std::path::PathBuf>;
    /// Retains the supplied identity and inputs under the parent's current fence.
    fn admit<'a>(
        &'a self,
        context: &'a TaskContext,
        fence: &'a LeaseFence,
        reference: &'a Component,
        child: crate::runtime::TaskAdmissionRecord,
        inputs: Vec<crate::conversation::FileRef>,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskId>>;
}

impl<P, A, O> RequestCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn fork<'a>(
        &'a self,
        context: &'a TaskContext,
        fence: &'a LeaseFence,
        owner: &'a TaskJournalOwner<P>,
        request: &'a ExternalRequest,
        workspace: &'a str,
        executor: Option<super::super::launch::Prepared>,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let child = request.child_attachment(workspace)?;
            let task = child.task_id();
            if task == owner.task_id() {
                return ready(crate::Outcome::<TaskId>::Failed {
                    message: "native child cannot replace its parent".into(),
                });
            }
            if let Err(rejected) = super::super::child_binding::bind(
                self.results,
                owner.task_id(),
                request,
                owner
                    .condition(
                        if reconcile {
                            JournalWrite::Settlement
                        } else {
                            JournalWrite::Fresh
                        },
                        &mut None,
                    )
                    .await?,
                owner.admission().run_limits.deadline_epoch_ms,
            )
            .await?
            {
                return ready(rejected.outcome());
            }
            let key = format!("external-command:{}", child.operation_id);
            let payload = self
                .payloads
                .stage_input(
                    child.operation_id,
                    &key,
                    &volume::encode(&child).map_err(storage)?,
                    None,
                )?
                .await?;
            let command = WorkflowCommand {
                operation_id: child.operation_id,
                kind: LIFECYCLE.into(),
                payload: payload.clone(),
            };
            let mut input = serde_json::json!({"command":command,"terminal":false});
            let mut inputs = vec![payload];
            if let Some(executor) = executor {
                input["executor"] = serde_json::to_value(&executor.executor)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                inputs.push(executor.executor);
                if let Some(selected) = executor.context {
                    input["native_context"] = serde_json::to_value(&selected)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    inputs.push(selected);
                }
            }
            let admission = self
                .runtime
                .harness()
                .registered_admission(
                    child.operation_id,
                    "acyclic.command_session",
                    "1",
                    input,
                    Some((owner.task_id(), context.scope())),
                    None,
                    None,
                )?
                .0;
            self.admit_child(context, fence, owner, admission, inputs, reconcile)
                .await
        })
    }

    pub(super) fn admit_child<'a>(
        &'a self,
        context: &'a TaskContext,
        fence: &'a LeaseFence,
        owner: &'a TaskJournalOwner<P>,
        mut admission: crate::runtime::TaskAdmissionRecord,
        inputs: Vec<crate::conversation::FileRef>,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            if admission.parent != Some(owner.task_id())
                || context.durable_task_id() != Some(owner.task_id())
            {
                return Err(Error::Unauthorized(
                    "child declaration differs from its execution owner".into(),
                ));
            }
            let task = admission.task_id();
            let operation = admission.operation_id;
            if let Some(children) = self.children {
                admission.resource_proofs.clear();
                let reference = Component::new(task.to_string()).map_err(storage)?;
                let admitted = match children
                    .admit(context, fence, &reference, admission, inputs, reconcile)
                    .await
                {
                    Ok(task) => task,
                    Err(Error::Indeterminate(_)) => return Ok(TaskCommandProgress::Indeterminate),
                    Err(Error::AdmissionRejected(message)) => {
                        return ready(crate::Outcome::<TaskId>::Failed { message });
                    }
                    Err(error) => return Err(error),
                };
                if admitted != task {
                    return Err(Error::Conflict("native child identity changed".into()));
                }
                return ready(crate::Outcome::Succeeded(task));
            }
            if reconcile {
                match self
                    .runtime
                    .task_host()
                    .reconcile_admission(operation)
                    .await?
                {
                    Some((retained_task, retained))
                        if retained_task == task && retained == admission => {}
                    Some(_) => {
                        return Err(Error::Conflict("native child admission changed".into()));
                    }
                    None => return Ok(TaskCommandProgress::Indeterminate),
                }
            } else {
                match self
                    .runtime
                    .task_host()
                    .admit_owned(admission, fence.clone())
                    .await?
                {
                    Admission::Accepted(admitted) if admitted == task => {}
                    Admission::Rejected { reason } => {
                        return ready(crate::Outcome::<TaskId>::Failed { message: reason });
                    }
                    _ => return Ok(TaskCommandProgress::Indeterminate),
                }
            }
            ready(crate::Outcome::Succeeded(task))
        })
    }

    pub(super) async fn initial_child_request(
        &self,
        owner: &TaskJournalOwner<P>,
        command: &WorkflowCommand,
        payload: &Value,
        reconcile: bool,
    ) -> Result<ExternalRequest> {
        // The parent atomically admitted this initial command under its fence.
        // A worker may claim it before the native child's SessionStart arrives.
        // Only this pinned initial attachment may supply its missing input file.
        if owner.admission().parent.is_none()
            || command.kind != LIFECYCLE
            || owner.admission().input.get("command")
                != Some(
                    &serde_json::to_value(command)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                )
        {
            return Err(Error::NotFound("task request file is missing".into()));
        }
        let request: ExternalRequest = serde::Deserialize::deserialize(payload)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if !matches!(request.action, ExternalAction::Attach { .. })
            || !request.valid()
            || request.task_id() != owner.task_id()
            || request.operation_id != command.operation_id
        {
            return Err(Error::Unauthorized(
                "initial child attachment differs from its admission".into(),
            ));
        }
        let file = request
            .to_file(Component::new(owner.task_id().to_string()).map_err(storage)?)
            .map_err(storage)?;
        publish(
            self.requests,
            &file,
            owner
                .condition(
                    if reconcile {
                        JournalWrite::Settlement
                    } else {
                        JournalWrite::Fresh
                    },
                    &mut None,
                )
                .await?,
            owner.admission().run_limits.deadline_epoch_ms,
        )
        .await?;
        Ok(request)
    }
}

fn ready(value: impl serde::Serialize) -> Result<TaskCommandProgress> {
    serde_json::to_value(value)
        .map(TaskCommandProgress::Ready)
        .map_err(|error| Error::Invalid(error.to_string()))
}
