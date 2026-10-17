use super::*;
use crate::{executor::TurnOutput, runtime::TaskStateProvider};
use acyclic_stream::BoxProviderFuture;

const TRANSITIONS_PER_SLICE: u32 = 32;

impl LocalHarness {
    /// Stock command bindings using retained task inheritance.
    pub(super) async fn stock_commands(
        &self,
        task: Option<TaskId>,
    ) -> Result<
        crate::filesystem::FilesystemTaskCommands<
            '_,
            LocalStream,
            LocalAuthorityBackend,
            LocalObjectBackend,
        >,
    > {
        let model = self.command_model(task).await?;
        let (_, provider) = self.model_binding()?;
        self.runtime
            .commands()
            .with_model(&model, provider.clone(), &self.compaction, None)
    }

    pub(super) fn model_binding(&self) -> Result<&(Model, Arc<dyn ModelProvider>)> {
        self.model.as_ref().ok_or_else(|| {
            Error::Unsupported("this local harness uses an external model executor".into())
        })
    }

    pub(super) async fn command_model(&self, task: Option<TaskId>) -> Result<Model> {
        let mut model = self.model_binding()?.0.clone();
        let settings = self
            .task_files
            .inner
            .snapshot(
                &crate::task_volume::Query::File(
                    crate::task_volume::FileKey::parse(".ui/config/settings.json")
                        .map_err(storage)?,
                ),
                1,
            )
            .await
            .map_err(storage)?;
        if let Some(file) = settings.files.first() {
            if file.bytes.len() as u64 > self.limits.file_bytes {
                return Err(Error::Invalid("settings exceed file bound".into()));
            }
            let settings: serde_json::Value =
                serde_json::from_slice(&file.bytes).map_err(storage)?;
            if !settings.is_object() {
                return Err(Error::Invalid("settings must be an object".into()));
            }
            if let Some(name) = settings.get("model") {
                model.name = name
                    .as_str()
                    .ok_or_else(|| Error::Invalid("model must be text".into()))?
                    .into();
            }
        }
        if let Some(task) = task {
            if let Some(name) = self.resolved_value(task, "model").await? {
                model.name = name;
            }
        }
        model.validate()?;
        Ok(model)
    }
    /// Owner-gated stock commands for this same runtime.
    pub fn commands(&self) -> Result<impl crate::filesystem::TaskCommandHost + '_> {
        Ok(commands::CommandOwner(self))
    }
    pub(super) async fn submit_to(
        &self,
        operation: OperationId,
        parent: Option<TaskId>,
        prompt: &str,
    ) -> Result<Admission<RuntimeTask<TurnOutput>>> {
        let Some(parent) = parent else {
            return self.submit(operation, prompt).await;
        };
        if prompt.is_empty() || prompt.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid(
                "local prompt is empty or exceeds the file limit".into(),
            ));
        }
        let task = crate::runtime::task_id_for_admission(operation);
        let continuation = self.continuation_selection(task).await?;
        // Ordinary active children need no terminal-parent probe. Their existing
        // admission/parent check is already performed by prepare_inheritance.
        if continuation.is_some()
            || compact::checkpoints::capture_selection(
                &self.task_files.inner,
                parent,
                self.limits.file_bytes,
            )
            .await?
            .1
            .is_some()
        {
            let existing = match self.runtime.task_host().observe_admission(task).await {
                Ok(record) => Some(record.parent),
                Err(Error::NotFound(_)) => None,
                Err(error) => return Err(error),
            };
            if continuation.is_none() && existing.is_some_and(|retained| retained != Some(parent)) {
                return Err(Error::Conflict(
                    "task admission belongs to another conversation".into(),
                ));
            }
            // Completed executions retain their lifetime boundary. A conversation
            // continuation is a new root bound to the selected immutable checkpoint.
            if continuation.is_some()
                || existing.is_none()
                    && matches!(
                        self.runtime.task_host().outcome(parent).await?,
                        Some(
                            crate::Outcome::Succeeded(_)
                                | crate::Outcome::Failed { .. }
                                | crate::Outcome::Cancelled
                        )
                    )
            {
                return super::admission::serialized(&self.admission, async || {
                    let (selected, through) = match self.continuation_selection(task).await? {
                        Some(selected) => {
                            let through = selected
                                .continuation
                                .as_ref()
                                .ok_or_else(|| {
                                    Error::Invalid("continuation lost parent history".into())
                                })?
                                .through
                                .clone();
                            (selected, through)
                        }
                        None => {
                            let (cut, selected, _) = compact::checkpoints::capture_selection(
                                &self.task_files.inner,
                                parent,
                                self.limits.file_bytes,
                            )
                            .await?;
                            let selected = selected.ok_or_else(|| {
                                Error::Conflict("continuation lost its checkpoint".into())
                            })?;
                            let through = selected.through.clone().unwrap_or(cut.cursor);
                            (selected, through)
                        }
                    };
                    let (retained, checkpoint) = self
                        .retain_continuation(task, parent, selected, through, prompt)
                        .await?;
                    let prompt = &retained
                        .continuation
                        .as_ref()
                        .ok_or_else(|| Error::Invalid("continuation lost its prompt".into()))?
                        .prompt;
                    self.prepare_inheritance(task, None).await?;
                    let (receive, model) = match self.recover_turn_inputs(operation).await? {
                        Some(inputs) => inputs,
                        None => {
                            let prompt = self.prompt_context(Some(task), prompt).await?;
                            self.stage_bound_turn(
                                operation,
                                &prompt,
                                checkpoint.boundary.manifest,
                                self.input_publisher()?.as_ref(),
                            )
                            .await?
                        }
                    };
                    self.admit_turn(operation, None, receive, model).await
                })
                .await;
            }
        }
        inheritance::admit_prepared(
            &self.admission,
            Box::pin(self.prepare_inheritance(task, Some(parent))),
            Box::pin(async {
                let prompt = self.prompt_context(Some(task), prompt).await?;
                let (receive, model) = self.stage_turn(operation, &prompt).await?;
                self.admit_turn(operation, Some(parent), receive, model)
                    .await
            }),
        )
        .await
    }
    // The caller holds the existing admission gate and prepares inheritance.
    pub(super) async fn admit_turn(
        &self,
        operation: OperationId,
        parent: Option<TaskId>,
        receive: WorkflowCommand,
        model: WorkflowCommand,
    ) -> Result<Admission<RuntimeTask<TurnOutput>>> {
        let definition = self
            .runtime
            .harness()
            .task::<serde_json::Value, TurnOutput>("acyclic.stock_turn@1")?;
        let input = serde_json::json!({"receive":receive,"model":model});
        if let Some(parent) = parent {
            let record = self.runtime.task_host().observe_admission(parent).await?;
            self.runtime
                .harness()
                .durable_context(parent, record.operation_id, None)
                .await?
                .admit(operation, &definition, input)
                .await
        } else {
            self.runtime
                .harness()
                .admit(operation, &definition, input, None)
                .await
        }
    }
    /// Stages and admits a resumable stock turn under a stable operation identity.
    pub fn submit<'a>(
        &'a self,
        operation: OperationId,
        prompt: &'a str,
    ) -> BoxProviderFuture<'a, Result<Admission<RuntimeTask<TurnOutput>>>> {
        Box::pin(async move {
            if self.model.is_none() {
                return Err(Error::Unsupported(
                    "this local harness uses an external model executor".into(),
                ));
            }
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
        self.stage_turn_with(operation, prompt, publisher.as_ref())
            .await
    }

    pub(super) async fn stage_turn_with(
        &self,
        operation: OperationId,
        prompt: &str,
        publisher: &dyn ContentPublisher,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        let (receive, model, _) = self
            .stage_turn_with_output_using(operation, prompt, self.limits, None, publisher)
            .await?;
        Ok((receive, model))
    }

    pub(super) async fn stage_turn_with_limits(
        &self,
        operation: OperationId,
        prompt: &str,
        limits: Limits,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        let (receive, model, _) = self
            .stage_turn_with_output(operation, prompt, limits, None)
            .await?;
        Ok((receive, model))
    }

    pub(super) async fn stage_turn_with_output(
        &self,
        operation: OperationId,
        prompt: &str,
        limits: Limits,
        output: Option<&VolumeRef>,
    ) -> Result<(
        WorkflowCommand,
        WorkflowCommand,
        crate::conversation::FileRef,
    )> {
        let publisher = self.input_publisher_with_limit(limits.file_bytes)?;
        self.stage_turn_with_output_using(operation, prompt, limits, output, publisher.as_ref())
            .await
    }

    async fn stage_turn_with_output_using(
        &self,
        operation: OperationId,
        prompt: &str,
        limits: Limits,
        output: Option<&VolumeRef>,
        publisher: &dyn ContentPublisher,
    ) -> Result<(
        WorkflowCommand,
        WorkflowCommand,
        crate::conversation::FileRef,
    )> {
        let manifest = match &self.context_manifest {
            Some(reference) => reference.clone(),
            None => {
                self.stage_context_manifest_with(operation, publisher)
                    .await?
            }
        };
        let (receive, model) = self
            .stage_bound_turn_using(
                operation,
                prompt,
                limits,
                output,
                manifest.clone(),
                publisher,
            )
            .await?;
        Ok((receive, model, manifest))
    }

    pub(super) async fn stage_bound_turn(
        &self,
        operation: OperationId,
        prompt: &str,
        manifest: crate::conversation::FileRef,
        publisher: &dyn ContentPublisher,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        self.stage_bound_turn_using(operation, prompt, self.limits, None, manifest, publisher)
            .await
    }

    async fn stage_bound_turn_using(
        &self,
        operation: OperationId,
        prompt: &str,
        limits: Limits,
        output: Option<&VolumeRef>,
        manifest: crate::conversation::FileRef,
        publisher: &dyn ContentPublisher,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        context::binding::retain(
            &self.task_files.inner,
            crate::runtime::task_id_for_admission(operation),
            &manifest,
            limits.file_bytes,
        )
        .await?;
        let model = crate::filesystem::ModelTaskCommand {
            summary_source: None,
            context_manifest: manifest,
            input: ModelContent::Text(prompt.into()),
            selected_context: None,
            max_steps: u32::try_from(limits.model_steps)
                .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?,
            max_output_tokens: None,
            mailbox_operation: None,
            selected_output_volume: output.cloned(),
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
        let (receive, model) = self
            .finish_turn_inputs(operation, payload, publisher)
            .await?;
        Ok((receive, model))
    }

    pub(super) fn input_publisher(
        &self,
    ) -> Result<Arc<FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>>> {
        self.input_publisher_with_limit(self.limits.file_bytes)
    }

    pub(super) fn input_publisher_with_limit(
        &self,
        maximum_bytes: u64,
    ) -> Result<Arc<FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentPublisher::new(
            self.files.clone(),
            &self.journal,
            &self.verifier,
            &self.signed,
            maximum_bytes,
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
        self.finish_turn_inputs(operation, payload, publisher.as_ref())
            .await
            .map(Some)
    }

    pub(super) async fn finish_turn_inputs(
        &self,
        operation: OperationId,
        payload: crate::conversation::FileRef,
        publisher: &dyn ContentPublisher,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        let model = WorkflowCommand {
            operation_id: operation,
            kind: crate::filesystem::MODEL_TASK_COMMAND_KIND.into(),
            payload,
        };
        let receive_operation = receive_operation(operation);
        // Older admissions retained one identical payload per turn. Keep their
        // exact reference on retry; new commands share one immutable journal file.
        let (retained, _) = publisher
            .observe_staged_once(receive_operation, self.limits.file_bytes, None)?
            .await?;
        let payload = match retained {
            Some(payload) => {
                let descriptor = crate::conversation::FileDescriptor::from_bytes(
                    b"{\"after\":0}",
                    "application/json",
                    &mut None,
                )?;
                if payload.path() != format!("inputs/{operation}.receive.json")
                    || payload.descriptor() != &descriptor
                    || payload.display_name() != "mailbox receive"
                {
                    return Err(Error::Conflict(
                        "retained receive input has another identity".into(),
                    ));
                }
                payload
            }
            None => {
                let mut hash = blake3::Hasher::new();
                hash.update(b"acyclic.local.mail-receive-input/v1\0");
                hash.update(publisher.volume().storage_name(&mut None)?.as_bytes());
                let identity = hash.finalize();
                let mut bytes = [0; 16];
                bytes.copy_from_slice(&identity.as_bytes()[..16]);
                publisher
                    .stage_once(
                        OperationId::from_bytes(bytes),
                        "inputs/mailbox-receive.json",
                        b"{\"after\":0}",
                        "application/json",
                        "mailbox receive",
                        None,
                    )?
                    .await?
                    .0
            }
        };
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
        self.advance_while(task, commands, || async { Ok(true) })
            .await
    }

    pub(super) async fn advance_while<F, Fut>(
        &self,
        task: TaskId,
        commands: &dyn crate::filesystem::TaskCommandHost,
        continue_work: F,
    ) -> Result<()>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: Future<Output = Result<bool>> + Send,
    {
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
                    if !continue_work().await? {
                        break;
                    }
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
        self.refresh_command_task(task).await?;
        Ok(())
    }

    /// Resumes a pinned stock task, recovering its original reservation after reopen.
    pub fn resume(&self, task: TaskId) -> BoxProviderFuture<'_, Result<TurnOutput>> {
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
            #[cfg(feature = "native-execution")]
            self.advance_with_children(task, &commands, self.task_files.clone(), None)
                .await?;
            #[cfg(not(feature = "native-execution"))]
            Box::pin(self.advance(task, &commands)).await?;
            if self.runtime.task_host().outcome(task).await?.is_none() {
                let admission = self.runtime.task_host().observe_admission(task).await?;
                return Err(Error::Indeterminate(admission.operation_id));
            }
        } else {
            #[cfg(feature = "native-execution")]
            self.advance_with_children(task, &self.commands()?, self.task_files.clone(), None)
                .await?;
            // Advance already refreshes projections; terminal recovery must repair them.
            self.refresh_command_task(task).await?;
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

pub(super) fn receive_operation(operation: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.mail-receive/v1\0");
    hash.update(&operation.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
