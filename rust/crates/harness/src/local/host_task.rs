//! Passive task admissions for native hosts. No model, tool or loop commands.
use super::*;
use crate::{
    distributed::{WorkPull, Worker},
    runtime::{TaskContext, TaskDefinition, TaskStateProvider},
    scheduler::LeaseFence,
    workflow::{MachineIdentity, MachineTransition, ResumableMachine},
};
use serde_json::{Value, json};

pub(super) const TASK: &str = "acyclic.native_host@1";

pub(super) struct NativeHostTask {
    identity: MachineIdentity,
    schema: Value,
}

impl NativeHostTask {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            identity: MachineIdentity {
                name: "acyclic.native_host".into(),
                version: "1".into(),
                digest: *blake3::hash(include_bytes!("host_task.rs")).as_bytes(),
            },
            schema: json!({"type":"object","required":["host","session"],"properties":{"host":{"enum":["codex","claude-code","pi"]},"session":{"type":"string","minLength":1,"maxLength":256}},"additionalProperties":false}),
        })
    }

    pub(super) fn definition(self: &Arc<Self>) -> Result<TaskDefinition<Value, Value>> {
        TaskDefinition::resumable(self.clone(), self.schema.clone(), json!({"type":"null"}))
    }
}

impl ResumableMachine for NativeHostTask {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, _: &Value, _: &Value) -> Result<MachineTransition> {
        Err(Error::Unsupported(
            "native hosts own their agent loop and tool execution".into(),
        ))
    }
}

impl LocalHarness {
    /// Admits a native host's passive task in the ordinary shared task tree.
    /// The caller is the trusted harness owner, never a host tool executor.
    pub async fn admit_host_task(
        &self,
        operation: OperationId,
        host: &str,
        session: &str,
    ) -> Result<TaskId> {
        let definition = self.runtime.harness().task::<Value, Value>(TASK)?;
        let input = json!({"host":host,"session":session});
        let admission = self
            .runtime
            .harness()
            .admit(operation, &definition, input, None)
            .await?;
        match admission {
            Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => {
                self.refresh_command_task(task_id).await?;
                Ok(task_id)
            }
            Admission::Accepted(_) => Err(Error::Conflict(
                "native task admission is not durable".into(),
            )),
            Admission::Rejected { reason } => Err(Error::Unauthorized(reason)),
            Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
        }
    }

    /// Claims only an existing passive task; never advances its machine.
    pub async fn bind_host_task(&self, task: TaskId) -> Result<TaskContext> {
        let host = self.runtime.task_host();
        let record = host.observe_admission(task).await?;
        if record.task.name != "acyclic.native_host" || record.task.version != "1" {
            return Err(Error::Unauthorized(
                "task is not a native host admission".into(),
            ));
        }
        let pulled = host.recover_work(task, &mut None).await?;
        let lease = match pulled {
            WorkPull::Claimed(lease) => lease,
            WorkPull::Idle => {
                let WorkPull::Claimed(lease) = host
                    .pull_work(
                        &Worker {
                            id: "native-host".into(),
                            available: Default::default(),
                            labels: Default::default(),
                        },
                        Some(record.operation_id),
                        &mut None,
                    )
                    .await?
                else {
                    return Err(Error::Conflict("native task is not claimable".into()));
                };
                lease
            }
            WorkPull::Unresolved { error, .. } => return Err(error),
        };
        host.start_task(&lease, &mut None).await?;
        self.runtime
            .task_context(task, LeaseFence::from(&lease.reservation))
            .await
    }

    /// Allocates the same isolated project/private volumes and child request
    /// files as `GraphCoder`, while admitting a passive native-host task.
    pub async fn admit_host_child(&self, parent: &TaskContext, reference: &str) -> Result<TaskId> {
        use crate::task_volume::{ChildFile, ChildRequest, Component, FileKey, FileValue, Query};
        let (task, _) = parent.owned_task_binding(None)?;
        let owner = parent
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("native parent owner absent".into()))?;
        owner.verify_owner(None)?.await?;
        if owner.admission().0.task.name != "acyclic.native_host"
            || owner.admission().0.task.version != "1"
            || !parent
                .scope()
                .grants()
                .contains("task:spawn:acyclic.native_host@1")
        {
            return Err(Error::Unauthorized(
                "native child requires its passive parent admission".into(),
            ));
        }
        let reference = Component::new(reference).map_err(storage)?;
        let child_files = self
            .task_files
            .for_created_task(parent, parent.scope())
            .await?;
        let files = child_files.as_ref().unwrap_or(&self.task_files);
        let request = ChildRequest {
            prompt: "Native host subagent".into(),
            model: None,
            tools: None,
            grants: Some(parent.scope().grants().iter().map(str::to_owned).collect()),
        };
        let mut file = FileValue::new(
            FileKey::Child {
                reference,
                file: ChildFile::Request,
            },
            request.encode(),
        );
        file.requester = Some(task);
        let snapshot = files
            .snapshot(&Query::File(file.key.clone()), 1)
            .await
            .map_err(storage)?;
        crate::task_volume::publish(&files.inner, snapshot.generation, &file, || async {
            let (condition, deadline) = owner.file_publication_condition().await?;
            Ok::<_, Error>((vec![condition], deadline))
        })
        .await?;
        let child = self.admit_child_task(files, &file).await?;
        self.refresh_command_task(child).await?;
        Ok(child)
    }

    /// Settles a quiesced native host's existing task without dispatching work.
    /// The host must stop its writes and capture its view before this call.
    pub async fn finish_host_task(
        &self,
        context: &TaskContext,
        outcome: crate::Outcome<Value>,
    ) -> Result<()> {
        let (task, fence) = context.owned_task_binding(None)?;
        let owner = context
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("native task owner absent".into()))?;
        owner.verify_owner(None)?.await?;
        if owner.admission().0.task.name != "acyclic.native_host"
            || owner.admission().0.task.version != "1"
        {
            return Err(Error::Unauthorized(
                "task is not a native host admission".into(),
            ));
        }
        self.runtime
            .task_host()
            .settle_task(task, fence, outcome, &mut None)
            .await?;
        self.refresh_command_task(task).await
    }
}
