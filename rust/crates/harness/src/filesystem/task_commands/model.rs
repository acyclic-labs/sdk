use super::*;

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
        io: Option<&crate::filesystem::PreparationIo>,
        stock: bool,
    ) -> Result<TaskCommandProgress> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        let mut input: ModelTaskCommand = mail::decode_command(payload, io)?;
        let binding = self.model.as_ref().ok_or_else(|| {
            boundary(
                io,
                ErrorCode::Unsupported,
                "model command provider is not bound",
            )
        })?;
        let steering = if stock || input.mailbox_operation.is_some() {
            #[cfg(not(target_arch = "wasm32"))]
            let journal = match io {
                Some(io) => {
                    self.runtime
                        .workflow_journal_with_native_work(
                            task,
                            fence,
                            io.original_native_work().ok_or_else(|| {
                                boundary(
                                    Some(io),
                                    ErrorCode::Unsupported,
                                    "original mail work is absent",
                                )
                            })?,
                        )
                        .await?
                }
                None => self.runtime.workflow_journal(task, fence.clone()).await?,
            };
            #[cfg(target_arch = "wasm32")]
            let journal = self.runtime.workflow_journal(task, fence.clone()).await?;
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
            input
                .mailbox_operation
                .or(recorded)
                .map(|receive| (journal, latest, receive))
        } else {
            None
        };
        let (journal, operation) = 'journal: {
            #[cfg(not(target_arch = "wasm32"))]
            if let WorkerContext::Native(_) = &context {
                let work = io
                    .and_then(crate::filesystem::PreparationIo::original_native_work)
                    .expect("native dispatch constructs its original observer");
                let (journal, operation) = self
                    .runtime
                    .execution_journal_with_native_work(task, fence, turn, work)
                    .await?;
                break 'journal (Some(journal), operation);
            }
            let journal = if reconciliation_only {
                Some(
                    self.runtime
                        .execution_journal(task, fence.clone(), turn)
                        .await?,
                )
            } else {
                None
            };
            break 'journal (
                journal,
                crate::filesystem::task_runtime::execution_operation(task, turn, io)?,
            );
        };
        if reconciliation_only
            && !was_dispatched(
                journal
                    .as_ref()
                    .expect("reconciliation opened its journal")
                    .as_ref(),
                operation,
                MODEL_TASK_COMMAND_KIND,
                io,
            )
            .await?
        {
            return mail::reconciled(io);
        }
        if let Some((journal, latest, receive)) = steering {
            let ordinary;
            let reader = match &context {
                WorkerContext::Ordinary(_) => {
                    ordinary = self.runtime.task_context(task, fence.clone()).await?;
                    &ordinary
                }
                #[cfg(not(target_arch = "wasm32"))]
                WorkerContext::Native(context) => context.context(),
            };
            let item = self
                .steering_mail(task, &journal, &latest, receive, reader, io)
                .await?;
            drop((journal, latest));
            let copy_operation = crate::durable_host::received_copy_operation(task, receive, io)?;
            let path_hold;
            let copy_path =
                crate::contract::preparation_format(format_args!("mailbox/{receive}/parcel"), io)?;
            path_hold = io
                .map(|io| io.guard_owned(copy_path.capacity() as u64))
                .transpose()?;
            let retained = 'retained: {
                #[cfg(not(target_arch = "wasm32"))]
                if let WorkerContext::Native(context) = &context {
                    let io = io.expect("native model observer");
                    let work = io.original_native_work().expect("native model work");
                    let (call_id, _call_id) = work.display(&copy_operation)?;
                    let publisher = ToolContext::new_with_native_work(
                        Arc::clone(context),
                        copy_operation,
                        &call_id,
                        work,
                    )?;
                    let (file, custody) = publisher
                        .observe_staged_file_once_with_native_work(copy_operation, work)
                        .await?;
                    io.absorb_native_custody(custody)?;
                    break 'retained file;
                }
                reader
                    .observe_staged_file_once(copy_operation, None)
                    .await?
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
                    let bytes = reader
                        .read_received_mail(
                            receive,
                            &item,
                            &mut io
                                .map(crate::filesystem::PreparationIo::history_io)
                                .as_mut()
                                .map(|work| work as &mut dyn crate::store::history::HistoryIo),
                        )
                        .await?;
                    self.runtime.owner_observed(task, fence, false, io).await?;
                    let copy = 'copy: {
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(io) = io {
                            let (file, custody) = reader
                                .stage_file_once_with_native_work(
                                    copy_operation,
                                    &copy_path,
                                    &bytes,
                                    item.payload.descriptor().media_type(),
                                    item.payload.display_name(),
                                    io.original_native_work().expect("native model work"),
                                )
                                .await?;
                            io.absorb_native_custody(custody)?;
                            break 'copy file;
                        }
                        reader
                            .stage_file_once(
                                copy_operation,
                                &copy_path,
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
            if copy.path() != copy_path
                || copy.descriptor() != item.payload.descriptor()
                || copy.display_name() != item.payload.display_name()
            {
                return Err(boundary(
                    io,
                    ErrorCode::Conflict,
                    "receiver parcel receipt differs from original mail",
                ));
            }
            drop(copy_path);
            drop(path_hold);
            let mut parts = match input.input {
                ModelContent::Text(text) => {
                    let mut parts = Vec::new();
                    crate::executor::stock_vec_push(
                        &mut parts,
                        crate::model::ModelContentPart::Text { text },
                        io,
                    )?;
                    parts
                }
                ModelContent::Part(part) => {
                    let mut parts = Vec::new();
                    crate::executor::stock_vec_push(&mut parts, part, io)?;
                    parts
                }
                ModelContent::Parts(parts) => parts,
            };
            crate::executor::stock_vec_push(
                &mut parts,
                crate::model::ModelContentPart::File {
                    file: copy,
                    policy: crate::model::FileProjectionPolicy::BoundedFull,
                },
                io,
            )?;
            input.input = ModelContent::Parts(parts);
        }
        let output_volume = input.selected_output_volume;
        let maximum = input.max_output_tokens;
        if let Some(io) = io {
            io.retain_owned(
                std::mem::size_of::<TurnInput>()
                    .saturating_sub(std::mem::size_of::<ModelTaskCommand>()) as u64,
            )?;
        }
        let input = TurnInput {
            operation_id: operation,
            input: input.input,
            selected_context: input.selected_context,
            max_steps: input.max_steps,
        };
        let progress = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => {
                let io = io.expect("native model has its original observer");
                let work = io
                    .original_native_work()
                    .expect("native model has its original work");
                let context = match &output_volume {
                    Some(volume) => {
                        context
                            .with_output_volume_with_native_work(volume, work)
                            .await?
                    }
                    None => context,
                };
                let journal = journal.as_ref().expect("native model opened its journal");
                let executor = self.runtime.stock_executor_with_native_work(
                    task,
                    fence,
                    operation,
                    &context,
                    &binding.model,
                    Arc::clone(&binding.provider),
                    &binding.context,
                    journal,
                    work,
                )?;
                let executor =
                    executor.with_compaction_policy_with_native_work(&binding.compaction, work)?;
                let executor = match maximum {
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
                if reconciliation_only {
                    match result {
                        Ok((progress, custody)) => {
                            drop(progress);
                            drop(custody);
                        }
                        Err(Error::Indeterminate(_)) => {}
                        Err(error) => return Err(error),
                    }
                    return if journal.is_quiescent(Some(io)).await? {
                        mail::reconciled(Some(io))
                    } else {
                        Ok(TaskCommandProgress::Indeterminate)
                    };
                }
                let (progress, custody) = result?;
                io.absorb_native_custody(custody)?;
                progress
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
                        output_volume.as_ref(),
                    )
                    .await?
                    .with_compaction_policy(binding.compaction.clone());
                let execution = match maximum {
                    Some(maximum) => execution.with_max_output_tokens(maximum)?,
                    None => execution,
                };
                if let Some(journal) = journal {
                    match execution.reconcile_progress(input).await {
                        Ok(_) | Err(Error::Indeterminate(_)) => {}
                        Err(error) => return Err(error),
                    }
                    return if journal.is_quiescent(io).await? {
                        mail::reconciled(io)
                    } else {
                        Ok(TaskCommandProgress::Indeterminate)
                    };
                }
                execution.execute_progress(input).await?
            }
        };
        match progress {
            StockTurnProgress::Ready(output) => ready(Outcome::Succeeded(output), io),
            StockTurnProgress::Pending(_) => Ok(TaskCommandProgress::Pending),
            StockTurnProgress::Rejected(reason) => rejected(reason, io),
        }
    }
    async fn steering_mail(
        &self,
        task: TaskId,
        journal: &super::super::FilesystemWorkflowJournal<P, A, O>,
        latest: &crate::workflow::WorkflowRecord,
        receive: OperationId,
        context: &TaskContext,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<crate::scheduler::InboxItem> {
        use crate::filesystem::fork_preparer_io::failure;
        use crate::wire::ErrorCode;
        let previous = journal
            .observe_transition(latest.prior.revision, io)
            .await?;
        let [selection] = previous.transition.commands.as_slice() else {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "steering requires one receive",
            ));
        };
        if selection.operation_id != receive
            || selection.kind != MAIL_RECEIVE_TASK_COMMAND_KIND
            || previous.next != latest.prior
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "steering changed its receive checkpoint",
            ));
        }
        let results = latest.input["commands"]
            .as_array()
            .ok_or_else(|| failure(io, ErrorCode::Unauthorized, "steering result is absent"))?;
        let [result] = results.as_slice() else {
            return Err(failure(
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
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "steering receive did not succeed",
            ));
        };
        if result.operation_id != receive || item.task_id != task || item.sequence == 0 {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "steering result identity differs",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let bytes = match io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            Some(work) => {
                context
                    .read_file_with_native_work(&selection.payload, work)
                    .await?
            }
            None => context.read_file(&selection.payload).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let bytes = context.read_file(&selection.payload).await?;
        let selection: MailReceiveTaskCommand = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData,
                &bytes,
                ErrorCode::Storage,
            )?),
            None => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(error.to_string()))?,
        };
        if selection.after.checked_add(1) != Some(item.sequence) {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "steering receive cursor differs",
            ));
        }
        let retained = self.inbox(task, item.sequence - 1, io).await?;
        if retained.as_slice() != std::slice::from_ref(&item) {
            return Err(failure(
                io,
                ErrorCode::Conflict,
                "steering differs from retained mail",
            ));
        }
        Ok(item)
    }
}
