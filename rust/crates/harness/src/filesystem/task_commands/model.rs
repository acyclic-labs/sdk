use super::*;
use crate::filesystem::PreparationIo;
use crate::filesystem::task_runtime::observed::boundary;
use crate::wire::ErrorCode;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn model_command(
        &self,
        task: TaskId,
        context: WorkerContext,
        fence: &LeaseFence,
        turn: OperationId,
        payload: Value,
        reconciliation_only: bool,
        io: Option<&PreparationIo>,
        stock: bool,
    ) -> Result<TaskCommandProgress> {
        let mut command: ModelTaskCommand = mail::decode_command(payload, io)?;
        let binding = self.model.as_ref().ok_or_else(|| {
            boundary(
                io,
                ErrorCode::Unsupported,
                "model command provider is not bound",
            )
        })?;
        let steering = if stock || command.mailbox_operation.is_some() {
            let journal = match &context {
                #[cfg(not(target_arch = "wasm32"))]
                WorkerContext::Native(_) => {
                    self.runtime
                        .workflow_journal_with_native_work(
                            task,
                            fence,
                            io.expect("native observer")
                                .original_native_work()
                                .expect("native work"),
                        )
                        .await?
                }
                WorkerContext::Ordinary(_) => {
                    self.runtime.workflow_journal(task, fence.clone()).await?
                }
            };
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
            let recorded = if stock && !latest.next.state["mail"].is_null() {
                Some(
                    OperationId::deserialize(&latest.next.state["receive"]["operation_id"])
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                )
            } else {
                None
            };
            command
                .mailbox_operation
                .or(recorded)
                .map(|receive| (latest, receive))
        } else {
            None
        };
        let (journal, operation) = match &context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(_) => {
                let (journal, operation) = self
                    .runtime
                    .execution_journal_with_native_work(
                        task,
                        fence,
                        turn,
                        io.expect("native observer")
                            .original_native_work()
                            .expect("native work"),
                    )
                    .await?;
                (Some(journal), operation)
            }
            WorkerContext::Ordinary(_) => (
                if reconciliation_only {
                    Some(
                        self.runtime
                            .execution_journal(task, fence.clone(), turn)
                            .await?,
                    )
                } else {
                    None
                },
                crate::filesystem::task_runtime::execution_operation(task, turn, io)?,
            ),
        };
        if reconciliation_only
            && !was_dispatched(
                journal.as_ref().expect("reconciliation journal").as_ref(),
                operation,
                MODEL_TASK_COMMAND_KIND,
                io,
            )
            .await?
        {
            return mail::reconciled(io);
        }
        if let Some((latest, receive)) = steering {
            self.steer(
                task,
                &context,
                fence,
                &mut command,
                latest,
                receive,
                reconciliation_only,
                io,
            )
            .await?;
        }
        if let Some(io) = io {
            io.retain_owned(
                std::mem::size_of::<TurnInput>()
                    .saturating_sub(std::mem::size_of::<ModelTaskCommand>()) as u64,
            )?;
        }
        let input = TurnInput {
            operation_id: operation,
            input: command.input,
            selected_context: command.selected_context,
            max_steps: command.max_steps,
        };
        let result: Result<(StockTurnProgress, crate::NativeCustody)> = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => {
                let io = io.expect("native observer");
                let work = io.original_native_work().expect("native work");
                let context = match &command.selected_output_volume {
                    Some(volume) => {
                        context
                            .with_output_volume_with_native_work(volume, work)
                            .await?
                    }
                    None => context,
                };
                let journal = journal.as_ref().expect("native journal");
                let executor = self
                    .runtime
                    .stock_executor_with_native_work(
                        task,
                        fence,
                        operation,
                        &context,
                        &binding.model,
                        Arc::clone(&binding.provider),
                        &binding.context,
                        journal,
                        work,
                    )?
                    .with_compaction_policy_with_native_work(&binding.compaction, work)?;
                let executor = match command.max_output_tokens {
                    Some(maximum) => executor.with_max_output_tokens(maximum)?,
                    None => executor,
                };
                let result = if reconciliation_only {
                    executor
                        .execute_reconcile_progress_with_native_context(
                            &context,
                            &input,
                            journal.as_ref(),
                            work,
                        )?
                        .await
                } else {
                    executor
                        .execute_progress_with_native_context(
                            &context,
                            &input,
                            journal.as_ref(),
                            work,
                        )?
                        .await
                };
                result.map(|(progress, custody)| (progress, Some(custody)))
            }
            WorkerContext::Ordinary(_) => {
                let execution = self
                    .runtime
                    .stock_execution(
                        task,
                        fence.clone(),
                        turn,
                        binding.model.clone(),
                        Arc::clone(&binding.provider),
                        binding.context.clone(),
                        command.selected_output_volume.as_ref(),
                    )
                    .await?
                    .with_compaction_policy(binding.compaction.clone());
                let execution = match command.max_output_tokens {
                    Some(maximum) => execution.with_max_output_tokens(maximum)?,
                    None => execution,
                };
                let result = if reconciliation_only {
                    execution.reconcile_progress(input).await
                } else {
                    execution.execute_progress(input).await
                };
                result.map(|progress| (progress, None))
            }
        };
        if reconciliation_only {
            match result {
                Ok((progress, custody)) => drop((progress, custody)),
                Err(Error::Indeterminate(_)) => {}
                Err(error) => return Err(error),
            }
            return if journal
                .as_ref()
                .expect("reconciliation journal")
                .is_quiescent(io)
                .await?
            {
                mail::reconciled(io)
            } else {
                Ok(TaskCommandProgress::Indeterminate)
            };
        }
        let (progress, custody) = result?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = custody {
            io.expect("native observer")
                .absorb_native_custody(custody)?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = custody;
        match progress {
            StockTurnProgress::Ready(output) => ready(Outcome::Succeeded(output), io),
            StockTurnProgress::Pending(_) => Ok(TaskCommandProgress::Pending),
            StockTurnProgress::Rejected(reason) => rejected(reason, io),
        }
    }

    async fn steer(
        &self,
        task: TaskId,
        context: &WorkerContext,
        fence: &LeaseFence,
        command: &mut ModelTaskCommand,
        latest: crate::workflow::WorkflowRecord,
        receive: OperationId,
        reconciliation_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let results = latest.input["commands"]
            .as_array()
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
            ),
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
        let ordinary;
        let reader = match context {
            WorkerContext::Ordinary(_) => {
                ordinary = self.runtime.task_context(task, fence.clone()).await?;
                &ordinary
            }
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => context.context(),
        };
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
        let operation = crate::durable_host::received_copy_operation(task, receive, io)?;
        let path =
            crate::contract::preparation_format(format_args!("mailbox/{receive}/parcel"), io)?;
        let _path = io
            .map(|io| io.guard_owned(path.capacity() as u64))
            .transpose()?;
        let retained = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => {
                let io = io.expect("native observer");
                let work = io.original_native_work().expect("native work");
                let (call_id, _call_id) = work.display(&operation)?;
                let publisher = ToolContext::new_with_native_work(
                    Arc::clone(context),
                    operation,
                    &call_id,
                    work,
                )?;
                let (file, custody) = publisher
                    .observe_staged_file_once_with_native_work(operation, work)
                    .await?;
                io.absorb_native_custody(custody)?;
                file
            }
            WorkerContext::Ordinary(_) => reader.observe_staged_file_once(operation, None).await?,
        };
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
                let copy = 'copy: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(io) = io {
                        let (file, custody) = reader
                            .stage_file_once_with_native_work(
                                operation,
                                &path,
                                &bytes,
                                item.payload.descriptor().media_type(),
                                item.payload.display_name(),
                                io.original_native_work().expect("native work"),
                            )
                            .await?;
                        io.absorb_native_custody(custody)?;
                        break 'copy file;
                    }
                    reader
                        .stage_file_once(
                            operation,
                            &path,
                            &bytes,
                            item.payload.descriptor().media_type(),
                            item.payload.display_name(),
                        )
                        .await?
                };
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
        let mut parts = match std::mem::replace(&mut command.input, ModelContent::Parts(Vec::new()))
        {
            ModelContent::Parts(parts) => parts,
            content => {
                let part = match content {
                    ModelContent::Text(text) => crate::model::ModelContentPart::Text { text },
                    ModelContent::Part(part) => part,
                    ModelContent::Parts(_) => unreachable!(),
                };
                let mut parts = Vec::new();
                crate::executor::stock_vec_push(&mut parts, part, io)?;
                parts
            }
        };
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
