//! The native result follow completes its session after all hook effects settle.
use super::*;
use crate::{distributed::SchedulerPayloadStore, runtime::TaskAdmissionRecord};
use futures::StreamExt as _;

impl LocalHarness {
    pub(in crate::local::external) async fn native_parent(
        &self,
        process: &TaskAdmissionRecord,
    ) -> Result<TaskId> {
        let parent = process
            .parent
            .ok_or_else(|| Error::Unauthorized("native process has no session parent".into()))?;
        let attached = self.runtime.task_host().observe_admission(parent).await?;
        if process.task.name != "acyclic.command_session"
            || process.task.version != "1"
            || process
                .input
                .pointer("/command/kind")
                .and_then(Value::as_str)
                != Some(PROCESS_COMMAND)
            || process.input.pointer("/command/operation_id") != Some(&json!(process.operation_id))
            || process.operation_id != launch::process_operation(parent)
            || attached.input.get("executor") != process.input.pointer("/command/payload")
            || attached.input.get("executor").is_none()
        {
            return Err(Error::Unauthorized(
                "native process is not the retained session executor".into(),
            ));
        }
        Ok(parent)
    }

    pub(in crate::local::external) async fn finish_native_process(
        &self,
        process: TaskId,
        parent: TaskId,
    ) -> Result<()> {
        if self.runtime.task_host().outcome(parent).await?.is_some() {
            return Ok(());
        }
        let outcome = match self.runtime.task_host().outcome(process).await? {
            Some(crate::Outcome::Succeeded(value))
                if value.get("success").and_then(Value::as_bool) == Some(true) =>
            {
                crate::Outcome::Succeeded(value)
            }
            Some(crate::Outcome::Succeeded(value)) => crate::Outcome::Failed {
                message: format!(
                    "native executor stopped: {}",
                    value
                        .get("stop")
                        .filter(|stop| !stop.is_null())
                        .unwrap_or(&value["exit_code"])
                ),
            },
            Some(crate::Outcome::Failed { message }) => crate::Outcome::Failed { message },
            Some(crate::Outcome::Cancelled) => crate::Outcome::Failed {
                message: "native executor cancelled".into(),
            },
            Some(_) => {
                return Err(Error::Indeterminate(OperationId::from_bytes(
                    process.into_bytes(),
                )));
            }
            None => return Ok(()),
        };
        let bytes = crate::external_executor::volume::encode(&json!({"native_exit":outcome}))
            .map_err(storage)?;
        let publisher = self.input_publisher()?;
        let payload = publisher
            .stage_input(
                OperationId::from_bytes(process.into_bytes()),
                "native-finished",
                &bytes,
                None,
            )?
            .await?;
        crate::external_executor::progress::follow(
            (),
            |()| async {
                // The process task's guard has retired before taking this guard.
                // Native hooks hold only their own task guard during execution.
                let (_binding, volumes) = self.binding(parent).await?;
                let commands = RequestCommands::new(
                    &self.runtime,
                    &volumes.requests,
                    &volumes.results,
                    publisher.as_ref(),
                )
                .with_child_admission(self);
                use crate::external_executor::settlement::{self, Slot};
                let settled = settlement::step(
                    || Box::pin(self.advance(parent, &commands)),
                    || async {
                        if self.runtime.task_host().outcome(parent).await?.is_some() {
                            return Ok(Slot::Settled);
                        }
                        Ok(
                            match self
                                .runtime
                                .task_host()
                                .idle_workflow(parent)
                                .await?
                            {
                                Some(idle) => Slot::Idle(idle.workflow_revision),
                                None => Slot::Busy,
                            },
                        )
                    },
                    |revision| {
                        let payload = &payload;
                        async move {
                            self.runtime
                                .task_host()
                                .resume_workflow(
                                    parent,
                                    revision,
                                    payload.clone(),
                                    crate::IdempotencyKey::new(format!(
                                        "native-finished:{process}"
                                    ))?,
                                    None,
                                    &mut None,
                                )
                                .await
                        }
                    },
                )
                .await?;
                Ok(((), settled.then_some(())))
            },
            async {
                Ok(self
                    .runtime
                    .task_host()
                    .workflow_changes()
                    .await?
                    .map(|record| record.map(|_| ()).map_err(storage)))
            },
            std::future::pending(),
        )
        .await?;
        Ok(())
    }
}
