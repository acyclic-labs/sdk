use super::*;
use acyclic_harness::conversation::ContentPublisher as _;

impl NativeSession {
    pub async fn queue(
        &self,
        operation: OperationId,
        command: WorkflowCommand,
        parent: Option<TaskId>,
    ) -> Result<Admission<RuntimeTask<TurnOutput>>> {
        let (receive, model) =
            stock_turn_commands(&self.files, &self.config.owner, command).await?;
        let input = serde_json::json!({"receive": receive, "model": model});
        let definition = self.runtime.harness().task("acyclic.stock_turn@1")?;
        match parent {
            Some(parent) => {
                let admission = self.runtime.task_host().observe_admission(parent).await?;
                self.runtime
                    .harness()
                    .durable_context(parent, admission.operation_id, None)
                    .await?
                    .admit(operation, &definition, input)
                    .await
            }
            None => {
                self.runtime
                    .harness()
                    .admit(operation, &definition, input, None)
                    .await
            }
        }
    }
    pub async fn prompt_command(
        &self,
        text: &str,
        operation: OperationId,
    ) -> Result<WorkflowCommand> {
        let bytes = serde_json::to_vec(&acyclic_harness::filesystem::ModelTaskCommand {
            summary_source: None,
            context_manifest: self.context_manifest.clone(),
            input: ModelContent::Text(text.into()),
            selected_context: None,
            max_steps: 1,
            max_output_tokens: Some(256),
            mailbox_operation: None,
            selected_output_volume: None,
        })
        .map_err(invalid)?;
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
                None,
            )?
            .await?
            .0;
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
            .pull_work(worker, Some(admission.operation_id), &mut None)
            .await
    }
    pub async fn resume(&self, lease: WorkLease) -> Result<String> {
        let commands = self.runtime.commands().with_model(
            &crate::provider::model(self.config.deterministic_delay_ms),
            self.model_provider.clone(),
            &Default::default(),
            None,
        )?;
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
            .tools
            .callers
            .iter()
            .find(|source| source.task == caller)
            .map(|source| &source.owner)
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
}

pub(crate) async fn stock_turn_commands(
    files: &Arc<Files>,
    owner: &AuthorityBinding,
    model: WorkflowCommand,
) -> Result<(WorkflowCommand, WorkflowCommand)> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.graphcoder.mail-receive/v1\0");
    hash.update(&model.operation_id.into_bytes());
    let operation = OperationId::from_bytes(hash.finalize().as_bytes()[..16].try_into().unwrap());
    let publisher = FilesystemContentPublisher::new(
        files.clone(),
        model.payload.volume(),
        &owner.verifier()?,
        &owner.scope,
        MAX_FRAME_BYTES as u64,
        None,
    )?;
    let payload = publisher
        .stage_once(
            operation,
            &format!("{}.receive.json", model.payload.path()),
            b"{\"after\":0}",
            "application/json",
            "GraphCoder mailbox receive",
            None,
        )?
        .await?
        .0;
    Ok((
        WorkflowCommand {
            operation_id: operation,
            kind: acyclic_harness::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND.into(),
            payload,
        },
        model,
    ))
}
pub fn lease_hex(lease: &WorkLease) -> Result<String> {
    Ok(hex::encode(serde_json::to_vec(lease).map_err(invalid)?))
}
pub fn render_attempt(attempt: TaskWorkerAttempt) -> Result<String> {
    match attempt {
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Completed { task }) => {
            Ok(format!("worker completed task={task}\n"))
        }
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Suspended { task, revision }) => Ok(
            format!("worker suspended task={task} checkpoint={revision}\n"),
        ),
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) => Ok(format!(
            "worker yielded lease-json-hex={}\n",
            lease_hex(&lease)?
        )),
        TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }) => Ok(format!(
            "worker reconciling lease-json-hex={}\n",
            lease_hex(&lease)?
        )),
        TaskWorkerAttempt::Unresolved { lease, error } => Ok(format!(
            "worker unresolved lease-json-hex={} error={}\n",
            lease_hex(&lease)?,
            crate::terminal::text(&error.to_string())
        )),
    }
}
