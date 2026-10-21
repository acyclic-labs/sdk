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
        self.runtime
            .commands()
            .with_model(&model, self.provider.clone(), &self.compaction, None)
    }

    pub(super) async fn command_model(&self, task: Option<TaskId>) -> Result<Model> {
        let mut model = self.model.clone();
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
        // Routing, history capture, retention and admission share one hold of
        // the admission gate, so a concurrent retry of this operation observes
        // a finished admission instead of racing it.
        super::routing::routed(
            &self.admission,
            async || {
                if self.continuation_selection(task).await?.is_some() {
                    return super::routing::route(parent, true, false, false, None);
                }
                let selected = compact::checkpoints::capture_selection(
                    &self.task_files.inner,
                    parent,
                    self.limits.file_bytes,
                )
                .await?
                .1
                .is_some();
                let settled = super::orphans::settled(
                    self.runtime.task_host().outcome(parent).await?.as_ref(),
                );
                // An ordinary child of a live, unselected parent routes without
                // its own admission: inheritance preparation checks that, and
                // reading it here would add a read to every parented turn.
                if !selected && !settled {
                    return Ok(super::routing::Route::Child);
                }
                let existing = match self.runtime.task_host().observe_admission(task).await {
                    Ok(record) => Some(record.parent),
                    Err(Error::NotFound(_)) => None,
                    Err(error) => return Err(error),
                };
                super::routing::route(parent, false, selected, settled, existing)
            },
            async |route| match route {
                super::routing::Route::Continue { history } => {
                    self.continue_conversation(operation, task, parent, history, prompt)
                        .await
                }
                super::routing::Route::Child => {
                    self.prepare_inheritance(task, Some(parent)).await?;
                    let (receive, model) = self.stage_question(operation, task, prompt).await?;
                    self.admit_turn(operation, Some(parent), receive, model)
                        .await
                }
            },
        )
        .await
    }
    /// Admits a new root bound to the parent's selected checkpoint, capturing
    /// the parent's whole history first when `history` asks for it. The caller
    /// holds the admission gate.
    async fn continue_conversation(
        &self,
        operation: OperationId,
        task: TaskId,
        parent: TaskId,
        history: bool,
        prompt: &str,
    ) -> Result<Admission<RuntimeTask<TurnOutput>>> {
        let (selected, through) = match self.continuation_selection(task).await? {
            Some(selected) => {
                let through = selected
                    .continuation
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("continuation lost parent history".into()))?
                    .through
                    .clone();
                (selected, through)
            }
            None => {
                let capture = async || {
                    compact::checkpoints::capture_selection(
                        &self.task_files.inner,
                        parent,
                        self.limits.file_bytes,
                    )
                    .await
                };
                let (mut cut, mut selected, _) = capture().await?;
                // Captured under the gate, so a concurrent retry of
                // this operation finds the checkpoint instead of
                // capturing another history position.
                if selected.is_none() && history {
                    self.checkpoint_history(operation, parent).await?;
                    (cut, selected, _) = capture().await?;
                }
                let selected = selected
                    .ok_or_else(|| Error::Conflict("continuation lost its checkpoint".into()))?;
                let through = selected.through.clone().unwrap_or(cut.cursor);
                (selected, through)
            }
        };
        let (retained, _) = self
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
            None => self.stage_question(operation, task, prompt).await?,
        };
        self.admit_turn(operation, None, receive, model).await
    }
    /// Stages a user question beneath a conversation. History retains its
    /// checkpoint; guidance is selected from the question alone, before any
    /// inherited or compacted history text is prepended, so earlier markers
    /// can never choose this admission's conventions.
    async fn stage_question(
        &self,
        operation: OperationId,
        task: TaskId,
        question: &str,
    ) -> Result<(WorkflowCommand, WorkflowCommand)> {
        let publisher = self.input_publisher()?;
        let manifest = self
            .turn_manifest(operation, question, publisher.as_ref())
            .await?;
        let prompt = self.prompt_context(Some(task), question).await?;
        self.stage_bound_turn(operation, &prompt, manifest, publisher.as_ref())
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
    ///
    /// Admission alone does not drive the head. Until a driver holds it (by
    /// `resume`, or a driver claim held across this call), a later prompt
    /// settles it as it settles a killed process's head: recorded effects are
    /// reconciled and nothing is dispatched again.
    pub fn submit<'a>(
        &'a self,
        operation: OperationId,
        prompt: &'a str,
    ) -> BoxProviderFuture<'a, Result<Admission<RuntimeTask<TurnOutput>>>> {
        Box::pin(async move { Ok(self.submit_claimed(operation, prompt).await?.0) })
    }

    /// Holds the head admitted under `operation` for a caller that admits and
    /// then drives it: claimed under the admission gate, and released by the
    /// drop that ends or cancels the caller.
    pub(crate) async fn driver_claim(&self, operation: OperationId) -> impl Send + '_ {
        super::admission::serialized(&self.admission, async || self.drivers.drive(operation)).await
    }

    async fn submit_claimed(
        &self,
        operation: OperationId,
        prompt: &str,
    ) -> Result<(
        Admission<RuntimeTask<TurnOutput>>,
        Option<super::orphans::Driving<'_>>,
    )> {
        if prompt.is_empty() || prompt.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid(
                "local prompt is empty or exceeds the file limit".into(),
            ));
        }
        self.retain_prompt_input(
            operation,
            crate::client::CommandResolution::Prompt {
                text: prompt.into(),
                source: None,
                mentions: Vec::new(),
            },
        )
        .await?;
        self.submit_turn(operation, prompt).await
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

    #[cfg(test)]
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
        let manifest = self.turn_manifest(operation, prompt, publisher).await?;
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

    async fn turn_manifest(
        &self,
        operation: OperationId,
        prompt: &str,
        publisher: &dyn ContentPublisher,
    ) -> Result<crate::conversation::FileRef> {
        Ok(match &self.context_manifest {
            Some(reference) => reference.clone(),
            None => {
                self.stage_context_manifest_with(operation, Some(prompt), publisher)
                    .await?
            }
        })
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
            input: self
                .turn_input_content(operation, prompt, publisher)
                .await?,
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
        self.run_as(OperationId::new(), prompt).await
    }

    pub(super) async fn run_as(&self, operation: OperationId, prompt: &str) -> Result<TurnOutput> {
        // The admission's claim is carried into driving, so cancelling this
        // future at any await leaves the head to the next prompt.
        let (admission, claim) = Box::pin(self.submit_claimed(operation, prompt)).await?;
        match admission {
            Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => {
                Box::pin(self.resume_claimed(task_id, claim)).await
            }
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
            id: format!("local-{}", crate::identity::fresh_uuid()),
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
            // Claimed before any read, under the gate that serializes
            // supersession, and released by any drop of this future.
            let operation = OperationId::from_bytes(task.into_bytes());
            let claim = super::admission::serialized(&self.admission, async || {
                self.drivers.drive(operation)
            })
            .await;
            self.resume_claimed(task, Some(claim)).await
        })
    }

    // Each claim, taken under the admission gate where its head was admitted,
    // is held until that head is driven: no await separates them.
    async fn resume_claimed(
        &self,
        task: TaskId,
        mut claim: Option<super::orphans::Driving<'_>>,
    ) -> Result<TurnOutput> {
        let root = self.loop_root(task).await?;
        let mut current = task;
        loop {
            let output = self.drive_turn(current, claim.take()).await;
            if !matches!(&output, Ok(_) | Err(Error::TaskFailed { .. })) {
                return output;
            }
            match self.continue_goal(current, root).await? {
                Some((next, next_claim)) => {
                    current = next;
                    claim = Some(next_claim);
                }
                None => return output,
            }
        }
    }

    /// Drives one turn, carrying the claim taken where it was admitted, or
    /// claiming it under the admission gate when there is none.
    pub(super) async fn drive_turn(
        &self,
        task: TaskId,
        claim: Option<super::orphans::Driving<'_>>,
    ) -> Result<TurnOutput> {
        let operation = OperationId::from_bytes(task.into_bytes());
        // Claim under the gate: a new prompt never supersedes a driven turn.
        let _driving = match claim.filter(|claim| claim.operation() == operation) {
            Some(claim) => claim,
            None => {
                super::admission::serialized(&self.admission, async || {
                    self.drivers.drive(operation)
                })
                .await
            }
        };
        let _executing = _driving.execute()?;
        let definition = self
            .runtime
            .harness()
            .task::<serde_json::Value, TurnOutput>("acyclic.stock_turn@1")?;
        let handle = match self.runtime.harness().attach(task, &definition).await {
            Ok(handle) => handle,
            Err(Error::NotFound(_)) => self.recover_unadmitted_turn(task).await?,
            Err(error) => return Err(error),
        };
        if !super::orphans::settled(self.runtime.task_host().outcome(task).await?.as_ref()) {
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
            crate::Outcome::Indeterminate {
                operation_id,
                cause,
            } => Err(cause.into_error(operation_id)),
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
