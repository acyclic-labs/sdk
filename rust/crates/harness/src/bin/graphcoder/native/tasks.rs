use super::*;
use acyclic_harness::{conversation::ContentPublisher as _, runtime::TaskSpawner as _};

impl NativeSession {
    pub async fn queue(
        &self,
        operation: OperationId,
        command: WorkflowCommand,
        parent: Option<TaskId>,
    ) -> Result<Admission<RuntimeTask<TurnOutput>>> {
        let definition = self.runtime.harness().task("acyclic.stock_turn@1")?;
        match parent {
            Some(parent) => {
                let admission = self.runtime.task_host().observe_admission(parent).await?;
                self.runtime
                    .harness()
                    .durable_context(parent, admission.operation_id)
                    .await?
                    .admit(operation, &definition, command)
                    .await
            }
            None => {
                self.runtime
                    .harness()
                    .admit(operation, &definition, command, None)
                    .await
            }
        }
    }
    pub async fn queue_preparation(
        &self,
        operation: OperationId,
        parent: TaskId,
        lease: &WorkLease,
        request: ResourcePreparationRequest,
    ) -> Result<Admission<RuntimeTask<ForkReport>>> {
        request.validate(&mut None)?;
        if request.child.parent != Some(parent) {
            return Err(Error::Unauthorized(
                "preparation child belongs to a different caller".into(),
            ));
        }
        let definition = self.runtime.harness().task("acyclic.fork_preparation@1")?;
        self.caller_context(parent, lease)
            .await?
            .admit(operation, &definition, request)
            .await
    }
    pub async fn prompt_command(
        &self,
        text: &str,
        operation: OperationId,
    ) -> Result<WorkflowCommand> {
        let bytes = serde_json::to_vec(&acyclic_harness::filesystem::ModelTaskCommand {
            input: ModelContent::Text(text.into()),
            selected_context: None,
            max_steps: 1,
            max_output_tokens: Some(256),
            mailbox_operation: None,
            selected_output_volume: None,
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(Error::Invalid(
                "published prompt command exceeds terminal bounds".into(),
            ));
        }
        let publisher = FilesystemContentPublisher::new(
            self.files.clone(),
            &self.config.private_volume,
            &self.config.owner.verifier()?,
            &self.config.owner.scope,
            MAX_FRAME_BYTES as u64,
            None,
        )?;
        let payload = publisher
            .stage_once(
                operation,
                &format!("graphcoder-inputs/{operation}.json"),
                &bytes,
                "application/json",
                "GraphCoder prompt command",
            )
            .await?;
        Ok(WorkflowCommand {
            operation_id: operation,
            kind: acyclic_harness::filesystem::MODEL_TASK_COMMAND_KIND.into(),
            payload,
        })
    }
    pub async fn claim(&self, task: TaskId, worker: &Worker) -> Result<WorkPull> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        self.runtime.poll_task_wake(task, None).await?;
        self.runtime
            .task_host()
            .pull_operation(worker, admission.operation_id, &mut None)
            .await
    }
    pub async fn cancel_task(&self, task: TaskId) -> Result<String> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        self.runtime.task_host().cancel(task, None).await?;
        Ok(format!(
            "cancellation requested task={task} admission-operation={}; completion requires authoritative observation\n",
            admission.operation_id
        ))
    }
    pub async fn resume(&self, lease: WorkLease) -> Result<String> {
        let context = self
            .context
            .read()
            .map_err(|_| Error::Storage("context lock poisoned".into()))?
            .pipeline()?;
        let commands = self.runtime.commands().with_model(
            crate::provider::model(self.config.deterministic_delay_ms),
            self.model_provider.clone(),
            context,
            Default::default(),
        );
        render_attempt(self.runtime.resume_task(lease, &commands, 16, None).await)
    }
    pub async fn caller_context(&self, caller: TaskId, lease: &WorkLease) -> Result<TaskContext> {
        let (retained, _) = self
            .runtime
            .harness()
            .reconcile_admission(lease.operation.operation_id)
            .await?
            .ok_or_else(|| Error::NotFound("original caller admission".into()))?;
        if retained != caller {
            return Err(Error::Unauthorized(
                "original lease belongs to a different caller task".into(),
            ));
        }
        self.runtime
            .task_context(caller, LeaseFence::from(&lease.reservation))
            .await
    }
    pub async fn caller_binding(&self, caller: TaskId) -> Result<&AuthorityBinding> {
        let agent = self.runtime.inspect_source(caller, caller).await?.agent()?;
        let binding = self
            .config
            .fork_sources
            .iter()
            .rev()
            .find(|source| source.caller == caller)
            .map(|source| &source.owner)
            .or_else(|| {
                self.config
                    .tools
                    .callers
                    .iter()
                    .find(|source| source.task == caller)
                    .map(|source| &source.owner)
            })
            .or_else(|| {
                self.config
                    .tools
                    .forks
                    .iter()
                    .find(|source| source.admission.task_id() == caller)
                    .map(|source| &source.child)
            })
            .ok_or_else(|| {
                Error::Unsupported("caller conversation provider binding unavailable".into())
            })?;
        binding.verifier()?;
        if binding.scope.agent() != Some(agent) {
            return Err(Error::Unauthorized(
                "conversation binding differs from actual created caller".into(),
            ));
        }
        Ok(binding)
    }
    pub async fn queue_mailbox(
        &self,
        operation: OperationId,
        caller: TaskId,
        lease: &WorkLease,
        input: serde_json::Value,
    ) -> Result<Admission<RuntimeTask<serde_json::Value>>> {
        let definition = self.runtime.harness().task("acyclic.mailbox_model@1")?;
        self.caller_context(caller, lease)
            .await?
            .admit(operation, &definition, input)
            .await
    }
    pub async fn reconcile(&self, operation: OperationId) -> Result<String> {
        let found = self
            .runtime
            .harness()
            .reconcile_admission(operation)
            .await?;
        Ok(crate::terminal::text(&format!(
            "admission operation={operation} retained={found:?}\n"
        )))
    }
    pub async fn children(
        &self,
        task: TaskId,
        revision: Option<u64>,
        after: Option<String>,
    ) -> Result<String> {
        let page = self
            .runtime
            .harness()
            .children(task, revision, after, MAX_PAGE_EVENTS)
            .await?;
        let encoded =
            serde_json::to_string(&page).map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(format!(
            "{}children-json={encoded}\n",
            crate::terminal::text(&format!("children parent-task={task} {page:?}\n"))
        ))
    }
    pub async fn outcome(&self, task: TaskId) -> Result<String> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        let outcome = self.runtime.task_host().outcome(task).await?;
        let encoded = serde_json::to_string(&serde_json::json!({
            "task": task, "admission_operation": admission.operation_id, "outcome": outcome,
        }))
        .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(format!(
            "{}task-json={encoded}\n",
            crate::terminal::text(&format!(
                "task={task} admission-operation={} outcome={outcome:?}\n",
                admission.operation_id
            ))
        ))
    }
}
pub fn lease_hex(lease: &WorkLease) -> Result<String> {
    Ok(hex::encode(
        serde_json::to_vec(lease).map_err(|error| Error::Invalid(error.to_string()))?,
    ))
}
pub fn render_attempt(attempt: TaskWorkerAttempt) -> Result<String> {
    match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task }) => Ok(format!(
            "worker completed task={task}; observe /outcome for the authoritative result\n"
        )),
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { task, revision }) => {
            Ok(format!(
                "worker suspended task={task} checkpoint={revision}; no active future retained\n"
            ))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => Ok(format!(
            "worker yielded lease-json-hex={}\n",
            lease_hex(&lease)?
        )),
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => Ok(format!(
            "worker reconciling lease-json-hex={}; no new work pulled\n",
            lease_hex(&lease)?
        )),
        TaskWorkerAttempt::Unresolved { lease, error } => Ok(format!(
            "worker unresolved lease-json-hex={} error={}\n",
            lease_hex(&lease)?,
            crate::terminal::text(&error.to_string())
        )),
    }
}
