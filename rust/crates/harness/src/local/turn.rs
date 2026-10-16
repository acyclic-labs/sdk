use super::*;
use crate::{executor::TurnOutput, runtime::TaskStateProvider};

const TRANSITIONS_PER_SLICE: u32 = 32;

impl LocalHarness {
    /// Stock command bindings for a service-owned fenced worker.
    pub(crate) fn commands(
        &self,
    ) -> Result<
        crate::filesystem::FilesystemTaskCommands<
            '_,
            LocalStream,
            LocalAuthorityBackend,
            LocalObjectBackend,
        >,
    > {
        self.runtime.commands().with_model(
            &self.model,
            self.provider.clone(),
            &self.compaction,
            None,
        )
    }
    /// Stages and admits a resumable stock turn under a stable operation identity.
    pub fn submit<'a>(
        &'a self,
        operation: OperationId,
        prompt: &'a str,
    ) -> impl Future<Output = Result<Admission<RuntimeTask<TurnOutput>>>> + 'a {
        Box::pin(async move {
            if prompt.is_empty() || prompt.len() as u64 > self.limits.file_bytes {
                return Err(Error::Invalid(
                    "local prompt is empty or exceeds the file limit".into(),
                ));
            }
            self.submit_turn(operation, prompt).await
        })
    }

    pub(super) async fn stage_turn(
        &self,
        operation: OperationId,
        prompt: &str,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        let publisher = self.input_publisher()?;
        let model = crate::filesystem::ModelTaskCommand {
            context_manifest: self.context_manifest.clone(),
            input: ModelContent::Text(prompt.into()),
            selected_context: None,
            max_steps: u32::try_from(self.limits.model_steps)
                .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?,
            max_output_tokens: None,
            mailbox_operation: None,
            selected_output_volume: None,
        };
        let bytes = serde_json::to_vec(&model).map_err(storage)?;
        let payload = publisher
            .stage_once(
                operation,
                &format!("inputs/{operation}.json"),
                &bytes,
                "application/json",
                "model input",
                None,
            )?
            .await?
            .0;
        self.finish_turn_inputs(operation, payload).await
    }

    fn input_publisher(
        &self,
    ) -> Result<Arc<FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentPublisher::new(
            self.files.clone(),
            &self.journal,
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )
    }

    pub(super) async fn recover_turn_inputs(
        &self,
        operation: OperationId,
    ) -> Result<Option<(WorkflowCommand, WorkflowCommand)>> {
        let publisher = self.input_publisher()?;
        let (payload, _) = publisher
            .observe_staged_once(operation, self.limits.file_bytes, None)?
            .await?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        if payload.path() != format!("inputs/{operation}.json") {
            return Err(Error::Conflict(
                "retained model input has another path".into(),
            ));
        }
        self.finish_turn_inputs(operation, payload).await.map(Some)
    }

    async fn finish_turn_inputs(
        &self,
        operation: OperationId,
        payload: crate::conversation::FileRef,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        let publisher = self.input_publisher()?;
        let model = WorkflowCommand {
            operation_id: operation,
            kind: crate::filesystem::MODEL_TASK_COMMAND_KIND.into(),
            payload,
        };
        let receive_operation = receive_operation(operation);
        let payload = publisher
            .stage_once(
                receive_operation,
                &format!("inputs/{operation}.receive.json"),
                b"{\"after\":0}",
                "application/json",
                "mailbox receive",
                None,
            )?
            .await?
            .0;
        let receive = WorkflowCommand {
            operation_id: receive_operation,
            kind: crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND.into(),
            payload,
        };
        Ok((receive, model))
    }

    /// Runs a prompt through durable admission, the fenced worker and stock executor.
    pub async fn run(&self, prompt: &str) -> Result<TurnOutput> {
        let operation = OperationId::new();
        match self.submit(operation, prompt).await? {
            Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => self.resume(task_id).await,
            Admission::Accepted(RuntimeTask::Live(_)) => Err(Error::Invalid(
                "local stock turn was not durably admitted".into(),
            )),
            Admission::Indeterminate { .. } => Err(Error::Indeterminate(operation)),
            Admission::Rejected { reason } => Err(Error::Conflict(reason)),
        }
    }

    pub(super) async fn advance(
        &self,
        task: TaskId,
        commands: &dyn crate::filesystem::TaskCommandHost,
    ) -> Result<()> {
        let worker = crate::distributed::Worker {
            id: format!("local-{}", uuid::Uuid::new_v4()),
            available: Default::default(),
            labels: Default::default(),
        };
        let mut attempt =
            match Box::pin(self.runtime.task_host().recover_work(task, &mut None)).await? {
                crate::distributed::WorkPull::Claimed(lease) => Some(
                    self.runtime
                        .resume_task(lease, commands, TRANSITIONS_PER_SLICE, None)
                        .await,
                ),
                crate::distributed::WorkPull::Unresolved { error, .. } => {
                    if self.runtime.task_host().outcome(task).await?.is_none() {
                        return Err(error);
                    }
                    None
                }
                crate::distributed::WorkPull::Idle => {
                    self.runtime
                        .run_operation(&worker, task, commands, TRANSITIONS_PER_SLICE, None)
                        .await?
                }
            };
        loop {
            match attempt {
                Some(crate::filesystem::TaskWorkerAttempt::Unresolved { error, .. }) => {
                    return Err(error);
                }
                Some(crate::filesystem::TaskWorkerAttempt::Progress(
                    crate::filesystem::TaskWorkerOutcome::Yielded { lease },
                )) => {
                    attempt = Some(
                        self.runtime
                            .resume_task(lease, commands, TRANSITIONS_PER_SLICE, None)
                            .await,
                    );
                }
                Some(crate::filesystem::TaskWorkerAttempt::Progress(
                    crate::filesystem::TaskWorkerOutcome::Suspended { .. },
                )) => {
                    // Only an authenticated wake makes a passive command claimable.
                    // Without one, run_operation returns None and yields control.
                    attempt = self
                        .runtime
                        .run_operation(&worker, task, commands, TRANSITIONS_PER_SLICE, None)
                        .await?;
                }
                _ => break,
            }
        }
        Ok(())
    }

    /// Resumes a pinned stock task, recovering its original reservation after reopen.
    pub fn resume(&self, task: TaskId) -> impl Future<Output = Result<TurnOutput>> {
        Box::pin(async move {
            let root = self.loop_root(task).await?;
            let mut current = task;
            loop {
                let output = self.resume_turn(current).await;
                if !matches!(&output, Ok(_) | Err(Error::TaskFailed { .. })) {
                    return output;
                }
                match self.continue_goal(current, root).await? {
                    Some(next) => current = next,
                    None => return output,
                }
            }
        })
    }

    pub(super) async fn resume_turn(&self, task: TaskId) -> Result<TurnOutput> {
        let definition = self
            .runtime
            .harness()
            .task::<serde_json::Value, TurnOutput>("acyclic.stock_turn@1")?;
        let handle = match self.runtime.harness().attach(task, &definition).await {
            Ok(handle) => handle,
            Err(Error::NotFound(_)) => self.recover_unadmitted_turn(task).await?,
            Err(error) => return Err(error),
        };
        let terminal = matches!(
            self.runtime.task_host().outcome(task).await?,
            Some(
                crate::Outcome::Succeeded(_)
                    | crate::Outcome::Failed { .. }
                    | crate::Outcome::Cancelled
            )
        );
        if !terminal {
            let commands = self.commands()?;
            Box::pin(self.advance(task, &commands)).await?;
            if self.runtime.task_host().outcome(task).await?.is_none() {
                let admission = self.runtime.task_host().observe_admission(task).await?;
                return Err(Error::Indeterminate(admission.operation_id));
            }
        }
        match handle.result().await? {
            crate::Outcome::Succeeded(output) => Ok(output),
            crate::Outcome::Failed { message } => Err(Error::TaskFailed { task, message }),
            crate::Outcome::Cancelled => Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled,
            )),
            crate::Outcome::Indeterminate { operation_id } => {
                Err(Error::Indeterminate(operation_id))
            }
        }
    }
}

fn receive_operation(operation: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.mail-receive/v1\0");
    hash.update(&operation.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

// One content-addressed default file per effective catalog, shared by every
// prompt opened with that catalog. Admission retains its exact immutable ref.
pub(super) async fn default_manifest(
    publisher: &FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>,
    tools: &ToolRegistry,
    limits: Limits,
    mounts: Option<&str>,
) -> Result<crate::conversation::FileRef> {
    let mut manifest = crate::context::ContextManifest::new(Default::default(), Vec::new(), tools)?;
    if let Some(mounts) = mounts {
        manifest.conventions.push(mounts.to_owned());
    }
    let bytes = manifest.encode(limits)?;
    let digest = blake3::hash(&bytes);
    let operation = OperationId::from_bytes(digest.as_bytes()[..16].try_into().unwrap());
    Ok(publisher
        .stage_once(
            operation,
            &format!("inputs/context/{digest}.json"),
            &bytes,
            "application/json",
            "context-manifest.json",
            None,
        )?
        .await?
        .0)
}
