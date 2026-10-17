use super::*;
use crate::conversation::BoundedContentFuture;
use crate::executor::{ExecutionProgress, TurnOutput};
use crate::filesystem::task_runtime::observed::boundary;
use crate::filesystem::{FilesystemExecutionJournal, PreparationIo};
use crate::wire::ErrorCode;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn model_command(
        &self,
        CommandInvocation {
            context,
            admission,
            fence,
            command: workflow,
            reconciliation_only,
            io,
        }: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let task = admission.task_id();
        let context = if io.is_none() && context.owned_mailbox.is_none() {
            Arc::new(self.runtime.task_context(task, fence.clone()).await?)
        } else {
            Arc::clone(context)
        };
        let turn = workflow.operation_id;
        let stock = admission.task.name == "acyclic.stock_turn"
            || super::super::child_agent::is_child(admission);
        let mut command: ModelTaskCommand = mail::decode_command(payload, io)?;
        let binding = self.model.as_ref().ok_or_else(|| {
            boundary(
                io,
                ErrorCode::Unsupported,
                "model command provider is not bound",
            )
        })?;
        let compaction = if command.summary_source.is_some() {
            &crate::context::CompactionPolicy::Disabled
        } else {
            &binding.compaction
        };
        let steering = if stock || command.mailbox_operation.is_some() {
            BoundedContentFuture::new(io, || async {
                let journal = self
                    .command_workflow_journal(&context, task, fence, io)
                    .await?;
                let latest = journal.observe_latest_record(io).await?.ok_or_else(|| {
                    boundary(io, ErrorCode::Unauthorized, "model checkpoint is absent")
                })?;
                if !latest.transition.commands.iter().any(|command| {
                    command.operation_id == turn && command.kind == MODEL_TASK_COMMAND_KIND
                }) {
                    return Err(boundary(
                        io,
                        ErrorCode::Unauthorized,
                        "model is outside the committed outbox",
                    ));
                }
                let recorded = if stock
                    && latest
                        .transition
                        .state
                        .get("mail")
                        .is_some_and(|mail| !mail.is_null())
                {
                    Some(
                        OperationId::deserialize(
                            latest
                                .transition
                                .state
                                .get("receive")
                                .and_then(|receive| receive.get("operation_id"))
                                .unwrap_or(&Value::Null),
                        )
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    )
                } else {
                    None
                };
                Ok(command
                    .mailbox_operation
                    .or(recorded)
                    .map(|receive| (latest, receive)))
            })?
            .await?
        } else {
            None
        };
        let (journal, operation) = if io.is_some() || reconciliation_only {
            let (journal, operation) = BoundedContentFuture::new(io, || {
                self.command_journal(task, fence, turn, &context, io)
            })?
            .await?;
            (Some(journal), operation)
        } else {
            (
                None,
                crate::filesystem::task_runtime::execution_operation(task, turn, io)?,
            )
        };
        let require_journal = || {
            journal
                .as_ref()
                .ok_or_else(|| boundary(io, ErrorCode::Storage, "model journal is absent"))
        };
        if reconciliation_only
            && !was_dispatched(
                require_journal()?.as_ref(),
                operation,
                MODEL_TASK_COMMAND_KIND,
                io,
            )
            .await?
        {
            #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
            if io.is_none()
                && let Some(executor) = &context.harness.workspace_tools
            {
                // Hook dispatch precedes provider dispatch. Cancellation must
                // retain a claimed hook even when no model request was sent.
                let issued = ToolContext::new(
                    Arc::new(self.runtime.task_context(task, fence.clone()).await?),
                    operation,
                    "session-start",
                    None,
                )?;
                executor
                    .run_hooks(
                        (&crate::hooks::Hooks::default(), [0; 32]),
                        crate::hooks::HookEvent::SessionStart,
                        Value::Null,
                        &crate::tool::ToolRegistry::default(),
                        issued,
                        true,
                    )
                    .await?;
            }
            return mail::reconciled(io);
        }
        if let Some(steering) = steering {
            BoundedContentFuture::new(io, || {
                self.steer(
                    &context,
                    fence,
                    &mut command,
                    steering,
                    reconciliation_only,
                    io,
                )
            })?
            .await?;
        }
        if let Some(io) = io {
            io.retain_owned(
                std::mem::size_of::<TurnInput>()
                    .saturating_sub(std::mem::size_of::<ModelTaskCommand>()) as u64,
            )?;
        }
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        let manifest_digest = *command.context_manifest.descriptor().sha256();
        let (pipeline, tools) = BoundedContentFuture::new(io, || async {
            if command.summary_source.is_some() {
                Ok((
                    crate::context::ContextPipeline::default(),
                    crate::tool::ToolRegistry::default(),
                ))
            } else {
                self.model_context(command.context_manifest, &context, io)
                    .await
            }
        })?
        .await?;
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        let pipeline = if command.summary_source.is_none() {
            BoundedContentFuture::new(io, || {
                self.session_start(
                    pipeline,
                    &tools,
                    Arc::clone(&context),
                    (fence, operation, reconciliation_only, manifest_digest),
                    io,
                )
            })?
            .await?
        } else {
            pipeline
        };
        let input = TurnInput {
            operation_id: operation,
            input: command.input,
            selected_context: command.selected_context,
            max_steps: command.max_steps,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            return BoundedContentFuture::new(Some(io), || async {
                let work = io.original_native_work().ok_or_else(|| {
                    boundary(
                        Some(io),
                        ErrorCode::Unsupported,
                        "model requires original native work",
                    )
                })?;
                let context = match &command.selected_output_volume {
                    Some(volume) => context.with_output_volume(volume).await?,
                    None => context,
                };
                let journal = require_journal()?;
                let executor = self
                    .runtime
                    .stock_executor_with_native_work(
                        task,
                        fence,
                        operation,
                        &context,
                        &binding.model,
                        Arc::clone(&binding.provider),
                        &pipeline,
                        tools,
                        journal,
                        work,
                        compaction,
                        command.max_output_tokens,
                    )?
                    .with_summary_source(command.summary_source);
                let executor = if reconciliation_only {
                    executor.into_reconciliation()
                } else {
                    executor
                };
                let result = executor
                    .execute_progress_with_native_context(&context, &input, journal.as_ref(), work)?
                    .await;
                drop(executor);
                drop(context);
                if reconciliation_only {
                    return Self::reconcile_model(result.map(drop), journal, Some(io)).await;
                }
                let (progress, custody) = result?;
                io.absorb_native_custody(custody)?;
                Self::model_progress(progress, Some(io))
            })?
            .await;
        }
        BoundedContentFuture::new(io, || async {
            let execution = self
                .runtime
                .stock_execution_with_context(
                    Arc::clone(&context),
                    fence.clone(),
                    turn,
                    binding.model.clone(),
                    Arc::clone(&binding.provider),
                    pipeline,
                    tools,
                    command.selected_output_volume.as_ref(),
                )
                .await?
                .with_compaction_policy(compaction.clone())
                .with_summary_source(command.summary_source);
            let execution = match command.max_output_tokens {
                Some(maximum) => execution.with_max_output_tokens(maximum)?,
                None => execution,
            };
            if reconciliation_only {
                let result = execution.reconcile_progress(input).await;
                return Self::reconcile_model(result.map(drop), require_journal()?, io).await;
            }
            Self::model_progress(execution.execute_progress(input).await?, io)
        })?
        .await
    }

    fn model_progress(
        progress: ExecutionProgress<TurnOutput>,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        match progress {
            ExecutionProgress::Ready(output) => ready(Outcome::Succeeded(output), io),
            ExecutionProgress::Pending(_) => Ok(TaskCommandProgress::Pending),
            ExecutionProgress::Rejected(reason) => rejected(reason, io),
        }
    }

    async fn reconcile_model(
        result: Result<()>,
        journal: &FilesystemExecutionJournal<P, A, O>,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        match result {
            Ok(())
            | Err(Error::Indeterminate(_))
            | Err(Error::InteractionRejected(crate::InteractionRejection::Cancelled)) => {}
            Err(error) => return Err(error),
        }
        if journal.is_quiescent(io).await? {
            mail::reconciled(io)
        } else {
            Ok(TaskCommandProgress::Indeterminate)
        }
    }

    async fn steer(
        &self,
        context: &TaskContext,
        fence: &LeaseFence,
        command: &mut ModelTaskCommand,
        (latest, receive): (crate::workflow::WorkflowRecord, OperationId),
        reconciliation_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let task = context.durable_task_id().ok_or_else(|| {
            boundary(io, ErrorCode::Unauthorized, "steering has no admitted task")
        })?;
        let results = latest
            .input
            .get("commands")
            .and_then(Value::as_array)
            .ok_or_else(|| boundary(io, ErrorCode::Unauthorized, "steering result is absent"))?;
        let [result] = results.as_slice() else {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "steering requires one result",
            ));
        };
        let result: CommittedMailResult = match io {
            Some(io) => crate::filesystem::retain_decoded(
                io.value_decode_preparation(result, ErrorCode::Unauthorized)?,
            )?,
            None => serde_json::from_value(result.clone())
                .map_err(|error| Error::Unauthorized(error.to_string()))?,
        };
        let Outcome::Succeeded(item) = result.value else {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "steering receive did not succeed",
            ));
        };
        drop(latest);
        let reader = context;
        // The durable reader authenticates the committed receive, cursor and original parcel.
        let bytes = reader
            .read_received_mail(
                receive,
                &item,
                &mut io
                    .map(PreparationIo::history_io)
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::history::HistoryIo),
            )
            .await?;
        let operation = super::super::recursive::derive_operation(
            b"acyclic.harness.received-copy.v1\0",
            &[task.into_bytes(), receive.into_bytes()],
            io,
        )?;
        let path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("mailbox/{receive}/parcel"), work)
        })?;
        let _path = io
            .map(|io| io.guard_owned(path.capacity() as u64))
            .transpose()?;
        let (retained, _) = reader.observe_staged_file_once(operation, io).await?;
        let copy = match retained {
            Some(file) => file,
            None if reconciliation_only => {
                return Err(boundary(
                    io,
                    ErrorCode::Storage,
                    "started steered model has no retained receiver parcel",
                ));
            }
            None => {
                self.runtime.owner_observed(task, fence, false, io).await?;
                let (copy, _) = reader
                    .stage_file_once(
                        operation,
                        &path,
                        &bytes,
                        item.payload.descriptor().media_type(),
                        item.payload.display_name(),
                        io,
                    )
                    .await?;
                self.runtime.owner_observed(task, fence, false, io).await?;
                copy
            }
        };
        if copy.path() != path
            || copy.descriptor() != item.payload.descriptor()
            || copy.display_name() != item.payload.display_name()
        {
            return Err(boundary(
                io,
                ErrorCode::Conflict,
                "receiver parcel differs from original mail",
            ));
        }
        let mut parts = Vec::new();
        match std::mem::replace(&mut command.input, ModelContent::Parts(Vec::new())) {
            ModelContent::Parts(existing) => parts = existing,
            ModelContent::Text(text) => crate::executor::stock_vec_push(
                &mut parts,
                crate::model::ModelContentPart::Text { text },
                io,
            )?,
            ModelContent::Part(part) => crate::executor::stock_vec_push(&mut parts, part, io)?,
        }
        crate::executor::stock_vec_push(
            &mut parts,
            crate::model::ModelContentPart::File {
                file: copy,
                policy: crate::model::FileProjectionPolicy::BoundedFull,
            },
            io,
        )?;
        command.input = ModelContent::Parts(parts);
        Ok(())
    }
}
