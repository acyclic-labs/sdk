use super::*;
use crate::conversation::BoundedContentFuture;
use crate::executor::{ExecutionProgress, TurnOutput};
use crate::filesystem::task_runtime::observed::boundary;
use crate::filesystem::{FilesystemExecutionJournal, PreparationIo};
use crate::wire::ErrorCode;

type ModelJournal<P, A, O> = Option<Arc<FilesystemExecutionJournal<P, A, O>>>;

struct ModelExecution<P, A, O> {
    context: Arc<TaskContext>,
    command: ModelExecutionInput,
    pipeline: ContextPipeline,
    tools: crate::tool::ToolRegistry,
    journal: ModelJournal<P, A, O>,
    operation: OperationId,
    #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
    hooks: Option<Arc<crate::hooks::BoundHooks>>,
}

struct ModelExecutionInput {
    conversation_id: crate::ConversationId,
    role: Option<String>,
    input: ModelContent,
    selected_context: Option<SelectedModelContext>,
    max_steps: u32,
    summary_source: Option<Box<FileRef>>,
    selected_output_volume: Option<crate::conversation::VolumeRef>,
    max_output_tokens: Option<u32>,
}
impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn model_command(
        &self,
        invocation: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let task = invocation.admission.task_id();
        let turn = invocation.command.operation_id;
        let context = invocation.context;
        let fence = invocation.fence;
        let io = invocation.io;
        let reconciliation_only = invocation.reconciliation_only;
        let mut phase = crate::executor::DispatchPhase::new();
        // Keep execution out of the failure-settlement frame. Preparation and
        // recovery carry large storage futures, while settlement needs only the
        // dispatch capability. The bounded allocation also retains native IO
        // custody rather than adding an unaccounted executor allocation.
        let result = match BoundedContentFuture::new(io, || {
            self.run_model_command(invocation, payload, &mut phase)
        }) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        if result.is_err()
            && io.is_none()
            && let Some(files) = context.task_files()
        {
            let operation = crate::filesystem::task_runtime::execution_operation(task, turn, io)?;
            // Hook claims precede ModelStarted. Raw definitive errors are also
            // settled by the worker, so preserving the original error here
            // would still retire a task whose hook capture needs recovery.
            if !matches!(
                crate::hooks::effects::unsettled_prompt(&files.inner, task, operation).await,
                Ok(false)
            ) {
                return Err(Error::Indeterminate(operation));
            }
        }
        match result {
            Err(error) if !reconciliation_only && phase.can_fail(&error) => {
                let (journal, operation) =
                    self.command_journal(task, fence, turn, context, io).await?;
                let progress = phase
                    .finish::<TurnOutput>(Err(error), journal.as_ref(), operation, io)
                    .await?;
                Self::model_progress(progress, io)
            }
            result => result,
        }
    }

    async fn run_model_command(
        &self,
        invocation @ CommandInvocation {
            admission,
            fence,
            command: _,
            reconciliation_only,
            io,
            ..
        }: CommandInvocation<'_>,
        payload: Value,
        phase: &mut crate::executor::DispatchPhase,
    ) -> Result<TaskCommandProgress> {
        let context = self.model_task_context(invocation).await?;
        let stock = admission.task.name == "acyclic.stock_turn"
            || super::super::child_agent::is_child(admission);
        let mut command = self.decoded_model_command(payload, io)?;
        let steering = self
            .model_steering(invocation, &context, stock, &command)
            .await?;
        let (journal, operation) = self.prepared_model_journal(invocation, &context).await?;
        if self
            .reconcile_undispatched_model(
                invocation,
                #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
                &context,
                journal.as_deref(),
                operation,
            )
            .await?
        {
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
        reserve_model_input(io)?;
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        let manifest_digest = *command.context_manifest.descriptor().sha256();
        let (pipeline, tools) = BoundedContentFuture::new(io, || {
            self.prepared_model_context(
                &context,
                command.context_manifest,
                command.summary_source.is_some(),
                io,
            )
        })?
        .await?;
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        let (pipeline, hooks) = if command.summary_source.is_none() {
            match BoundedContentFuture::new(io, || {
                self.prompt_hooks(
                    pipeline,
                    &tools,
                    Arc::clone(&context),
                    (
                        fence,
                        operation,
                        reconciliation_only,
                        manifest_digest,
                        phase,
                    ),
                    &command.input,
                    io,
                )
            })?
            .await?
            {
                super::hooks::PromptHooks::Ready(pipeline, hooks) => (pipeline, hooks),
                super::hooks::PromptHooks::Blocked(message) => {
                    return ready(Outcome::<Value>::Failed { message }, io);
                }
            }
        } else {
            (pipeline, None)
        };
        self.execute_model_command(
            invocation,
            ModelExecution {
                context,
                command: ModelExecutionInput {
                    conversation_id: command.conversation_id.unwrap_or_else(|| {
                        crate::ConversationId::from_bytes(admission.task_id().into_bytes())
                    }),
                    role: command.role,
                    input: command.input,
                    selected_context: command.selected_context,
                    max_steps: command.max_steps,
                    summary_source: command.summary_source,
                    selected_output_volume: command.selected_output_volume,
                    max_output_tokens: command.max_output_tokens,
                },
                pipeline,
                tools,
                journal,
                operation,
                #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
                hooks,
            },
            phase,
        )
        .await
    }

    async fn model_task_context(
        &self,
        invocation: CommandInvocation<'_>,
    ) -> Result<Arc<TaskContext>> {
        if invocation.io.is_none() && invocation.context.owned_mailbox.is_none() {
            Ok(Arc::new(
                self.runtime
                    .task_context(invocation.admission.task_id(), invocation.fence.clone())
                    .await?,
            ))
        } else {
            Ok(Arc::clone(invocation.context))
        }
    }

    fn decoded_model_command(
        &self,
        payload: Value,
        io: Option<&PreparationIo>,
    ) -> Result<ModelTaskCommand> {
        let command: ModelTaskCommand = mail::decode_command(payload, io)?;
        if command
            .role
            .as_deref()
            .is_some_and(|role| role.trim().is_empty())
        {
            return Err(boundary(io, ErrorCode::Invalid, "agent role is empty"));
        }
        self.model.as_ref().ok_or_else(|| {
            boundary(
                io,
                ErrorCode::Unsupported,
                "model command provider is not bound",
            )
        })?;
        Ok(command)
    }

    async fn prepared_model_context(
        &self,
        context: &Arc<TaskContext>,
        manifest: FileRef,
        summary: bool,
        io: Option<&PreparationIo>,
    ) -> Result<(ContextPipeline, crate::tool::ToolRegistry)> {
        if summary {
            Ok((
                ContextPipeline::default(),
                crate::tool::ToolRegistry::default(),
            ))
        } else {
            self.model_context(manifest, context, io).await
        }
    }

    async fn prepared_model_journal(
        &self,
        invocation: CommandInvocation<'_>,
        context: &Arc<TaskContext>,
    ) -> Result<(ModelJournal<P, A, O>, OperationId)> {
        let CommandInvocation {
            admission,
            fence,
            command: workflow,
            reconciliation_only,
            io,
            ..
        } = invocation;
        let task = admission.task_id();
        let turn = workflow.operation_id;
        Ok(if io.is_some() || reconciliation_only {
            let (journal, operation) = BoundedContentFuture::new(io, || {
                self.command_journal(task, fence, turn, context, io)
            })?
            .await?;
            (Some(journal), operation)
        } else {
            (
                None,
                crate::filesystem::task_runtime::execution_operation(task, turn, io)?,
            )
        })
    }

    async fn execute_model_command(
        &self,
        invocation: CommandInvocation<'_>,
        prepared: ModelExecution<P, A, O>,
        phase: &mut crate::executor::DispatchPhase,
    ) -> Result<TaskCommandProgress> {
        #[cfg(not(target_arch = "wasm32"))]
        if invocation.io.is_some() {
            return BoundedContentFuture::new(invocation.io, || {
                self.execute_native_model(invocation, prepared, phase)
            })?
            .await;
        }
        let CommandInvocation {
            fence,
            command: workflow,
            reconciliation_only,
            io,
            ..
        } = invocation;
        let turn = workflow.operation_id;
        let ModelExecution {
            context,
            command,
            pipeline,
            tools,
            journal,
            operation,
            #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
            hooks,
        } = prepared;
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
        let require_journal = || {
            journal
                .as_ref()
                .ok_or_else(|| boundary(io, ErrorCode::Storage, "model journal is absent"))
        };
        let input = TurnInput {
            operation_id: operation,
            input: command.input,
            selected_context: command.selected_context,
            max_steps: command.max_steps,
        };
        BoundedContentFuture::new(io, || async {
            let execution = BoundedContentFuture::new(io, || {
                self.runtime.stock_execution_with_context(
                    Arc::clone(&context),
                    fence.clone(),
                    turn,
                    binding.model.clone(),
                    binding.provider_for(&context),
                    pipeline,
                    tools,
                    command.selected_output_volume.as_ref(),
                )
            })?
            .await?
            .with_compaction_policy(compaction.clone())
            .with_summary_source(command.summary_source)
            .with_conversation_id(command.conversation_id)
            .with_role(command.role);
            #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
            let execution = execution.with_hooks(hooks);
            let execution = match command.max_output_tokens {
                Some(maximum) => execution.with_max_output_tokens(maximum)?,
                None => execution,
            };
            if reconciliation_only {
                let result =
                    BoundedContentFuture::new(io, || execution.reconcile_progress(input))?.await;
                let journal = require_journal()?;
                return BoundedContentFuture::new(io, || {
                    Self::reconcile_model(result.map(drop), journal, operation, io)
                })?
                .await;
            }
            phase.delegate();
            Self::model_progress(
                BoundedContentFuture::new(io, || execution.execute_progress(input))?.await?,
                io,
            )
        })?
        .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn execute_native_model(
        &self,
        invocation: CommandInvocation<'_>,
        prepared: ModelExecution<P, A, O>,
        phase: &mut crate::executor::DispatchPhase,
    ) -> Result<TaskCommandProgress> {
        let CommandInvocation {
            admission,
            fence,
            reconciliation_only,
            io,
            ..
        } = invocation;
        let task = admission.task_id();
        let ModelExecution {
            context,
            command,
            pipeline,
            tools,
            journal,
            operation,
            #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
                hooks: _hooks,
        } = prepared;
        let binding = self.model.as_ref().ok_or_else(|| {
            boundary(
                io,
                ErrorCode::Unsupported,
                "model command provider is not bound",
            )
        })?;
        let io =
            io.ok_or_else(|| boundary(None, ErrorCode::Storage, "native model IO is absent"))?;
        let compaction = if command.summary_source.is_some() {
            &crate::context::CompactionPolicy::Disabled
        } else {
            &binding.compaction
        };
        let require_journal = || {
            journal
                .as_ref()
                .ok_or_else(|| boundary(Some(io), ErrorCode::Storage, "model journal is absent"))
        };
        let input = TurnInput {
            operation_id: operation,
            input: command.input,
            selected_context: command.selected_context,
            max_steps: command.max_steps,
        };
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
                binding.provider_for(&context),
                &pipeline,
                tools,
                journal,
                work,
                compaction,
                command.max_output_tokens,
            )?
            .with_summary_source(command.summary_source)
            .with_conversation_id(command.conversation_id)
            .with_role(command.role);
        let executor = if reconciliation_only {
            executor.into_reconciliation()
        } else {
            executor
        };
        phase.delegate();
        let result = executor
            .execute_progress_with_native_context(&context, &input, journal.as_ref(), work)?
            .await;
        drop(executor);
        drop(context);
        if reconciliation_only {
            return Self::reconcile_model(result.map(drop), journal, operation, Some(io)).await;
        }
        let (progress, custody) = result?;
        io.absorb_native_custody(custody)?;
        Self::model_progress(progress, Some(io))
    }

    async fn reconcile_undispatched_model(
        &self,
        invocation: CommandInvocation<'_>,
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))] context: &Arc<TaskContext>,
        journal: Option<&FilesystemExecutionJournal<P, A, O>>,
        operation: OperationId,
    ) -> Result<bool> {
        let CommandInvocation {
            reconciliation_only,
            io,
            ..
        } = invocation;
        if !reconciliation_only {
            return Ok(false);
        }
        let journal =
            journal.ok_or_else(|| boundary(io, ErrorCode::Storage, "model journal is absent"))?;
        if was_dispatched(journal, operation, MODEL_TASK_COMMAND_KIND, io).await? {
            return Ok(false);
        }
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        if io.is_none()
            && let Some(executor) = &context.harness.workspace_tools
        {
            let task = invocation.admission.task_id();
            let fence = invocation.fence;
            // Hook dispatch precedes provider dispatch. Cancellation must
            // retain a claimed hook even when no model request was sent.
            let owned = Arc::new(self.runtime.task_context(task, fence.clone()).await?);
            for event in crate::hooks::HookEvent::at(crate::hooks::HookSite::Prompt) {
                let issued = ToolContext::new(
                    Arc::clone(&owned),
                    operation,
                    super::hooks::prompt_hook_call(event),
                    None,
                )?;
                executor
                    .run_hooks(
                        (&crate::hooks::Hooks::default(), [0; 32]),
                        event,
                        (&Value::Null).into(),
                        &crate::tool::ToolRegistry::default(),
                        issued,
                        true,
                    )
                    .await?;
            }
        }
        Ok(true)
    }

    async fn model_steering(
        &self,
        invocation: CommandInvocation<'_>,
        context: &Arc<TaskContext>,
        stock: bool,
        command: &ModelTaskCommand,
    ) -> Result<Option<(crate::workflow::WorkflowRecord, OperationId)>> {
        let CommandInvocation {
            admission,
            fence,
            command: workflow,
            io,
            ..
        } = invocation;
        let task = admission.task_id();
        let turn = workflow.operation_id;
        Ok(if stock || command.mailbox_operation.is_some() {
            BoundedContentFuture::new(io, || async {
                let journal = self
                    .command_workflow_journal(context, task, fence, io)
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
        })
    }

    fn model_progress(
        progress: ExecutionProgress<TurnOutput>,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        match progress {
            ExecutionProgress::PreDispatchFailed(failure) => ready(
                Outcome::<TurnOutput>::Failed {
                    message: failure.into_message(),
                },
                io,
            ),
            ExecutionProgress::Ready(output) => ready(Outcome::Succeeded(output), io),
            ExecutionProgress::Pending(_) => Ok(TaskCommandProgress::Pending),
            ExecutionProgress::Rejected(reason) => rejected(reason, io),
            // A recorded tool failure settles the turn with its stable message.
            ExecutionProgress::Failed(reason) => ready(
                Outcome::<Value>::Failed {
                    message: crate::executor::stock_string_copy(reason.message(), io)?,
                },
                io,
            ),
        }
    }

    async fn reconcile_model(
        result: Result<()>,
        journal: &FilesystemExecutionJournal<P, A, O>,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        match result {
            Ok(()) | Err(Error::InteractionRejected(crate::InteractionRejection::Cancelled)) => {}
            Err(Error::Indeterminate(id)) if id == operation => {}
            Err(Error::Indeterminate(_)) => return Ok(TaskCommandProgress::Indeterminate),
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

fn reserve_model_input(io: Option<&PreparationIo>) -> Result<()> {
    if let Some(io) = io {
        io.retain_owned(
            std::mem::size_of::<TurnInput>().saturating_sub(std::mem::size_of::<ModelTaskCommand>())
                as u64,
        )?;
    }
    Ok(())
}
