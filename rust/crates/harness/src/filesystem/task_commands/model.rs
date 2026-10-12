use super::*;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn model_command(
        &self, task: TaskId, context: WorkerContext, fence: &LeaseFence,
        turn: OperationId, payload: Value, reconciliation_only: bool,
        io: Option<&crate::filesystem::PreparationIo>, receive: Option<OperationId>,
    ) -> Result<TaskCommandProgress> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        #[cfg(not(target_arch = "wasm32"))]
        if let WorkerContext::Native(_) = &context {
            let io = io.ok_or_else(|| boundary(io, ErrorCode::Unsupported, "native model requires original observer"))?;
            io.original_native_work().ok_or_else(|| boundary(Some(io), ErrorCode::Unsupported, "native model requires original work"))?;
        }
        let mut input: ModelTaskCommand = mail::decode_command(payload, io)?;
        input.mailbox_operation = input.mailbox_operation.or(receive);
        let binding = self.model.as_ref().ok_or_else(|| boundary(
            io, ErrorCode::Unsupported, "model command provider is not bound"))?;
        let (journal, operation) = 'journal: {
            #[cfg(not(target_arch = "wasm32"))]
            if let WorkerContext::Native(_) = &context {
                let work = io.and_then(crate::filesystem::PreparationIo::original_native_work).ok_or_else(|| boundary(
                    io, ErrorCode::Unsupported, "native model requires original work"))?;
                let (journal, operation) = self.runtime.execution_journal_with_native_work(task, fence, turn, work).await?;
                break 'journal (Some(journal), operation);
            }
            let journal = if reconciliation_only {
                Some(self.runtime.execution_journal(task, fence.clone(), turn).await?)
            } else { None };
            break 'journal (journal, crate::filesystem::task_runtime::execution_operation(task, turn, io)?);
        };
        if reconciliation_only && !was_dispatched(journal.as_ref().expect("reconciliation opened its journal").as_ref(),
            operation, MODEL_TASK_COMMAND_KIND, io).await? {
            return mail::reconciled(io);
        }
        if let Some(receive) = input.mailbox_operation {
            let copy = match &context {
                #[cfg(not(target_arch = "wasm32"))]
                WorkerContext::Native(context) => {
                    let io = io.expect("native model has its original observer");
                    let work = io.original_native_work().expect("native model has its original work");
                    let item = self.committed_mail_input(task, fence, turn, receive, context.context(), Some(io)).await?;
                    let copy_operation = crate::durable_host::received_copy_operation(task, receive, Some(io))?;
                    let copy_path = crate::contract::preparation_format(format_args!("mailbox/{receive}/parcel"), Some(io))?;
                    let _path = io.guard_owned(copy_path.capacity() as u64)?;
                    let (call_id, _call_id) = work.display(&copy_operation)?;
                    let publisher = ToolContext::new_with_native_work(Arc::clone(context), copy_operation, &call_id, work)?;
                    let retained = publisher.observe_staged_file_once_with_native_work(copy_operation, work).await?;
                    io.absorb_native_custody(retained.1)?;
                    let copy = match retained.0 {
                        Some(file) => file,
                        None if reconciliation_only => return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Storage,
                            reason: "started steered model has no retained receiver parcel",
                        }),
                        None => {
                            let bytes = context.context().read_received_mail(receive, &item, &mut Some(&mut io.history_io())).await?;
                            self.runtime.owner_observed(task, fence, false, Some(io)).await?;
                            let staged = context.context().stage_file_once_with_native_work(
                                copy_operation, &copy_path, &bytes,
                                item.payload.descriptor().media_type(), item.payload.display_name(), work,
                            ).await?;
                            io.absorb_native_custody(staged.1)?;
                            self.runtime.owner_observed(task, fence, false, Some(io)).await?;
                            staged.0
                        }
                    };
                    if copy.path() != copy_path
                        || copy.descriptor() != item.payload.descriptor()
                        || copy.display_name() != item.payload.display_name()
                    {
                        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                            reason: "receiver parcel receipt differs from original mail" });
                    }
                    copy
                }
                WorkerContext::Ordinary(_) => {
                    let context = self.runtime.task_context(task, fence.clone()).await?;
                    let item = self.committed_mail_input(task, fence, turn, receive, &context, None).await?;
                    let copy_operation = crate::durable_host::received_copy_operation(task, receive, None)?;
                    let copy_path = format!("mailbox/{receive}/parcel");
                    let copy = match context.observe_staged_file_once(copy_operation, None).await? {
                        Some(file) => file,
                        None if reconciliation_only => return Err(boundary(io, ErrorCode::Storage,
                            "started steered model has no retained receiver parcel")),
                        None => {
                            let bytes = context.read_received_mail(receive, &item, &mut None).await?;
                            self.runtime.task_host().verify_dispatch_owner(task, fence.clone()).await?;
                            let file = context.stage_file_once(copy_operation, &copy_path, &bytes,
                                item.payload.descriptor().media_type(), item.payload.display_name()).await?;
                            self.runtime.task_host().verify_dispatch_owner(task, fence.clone()).await?;
                            file
                        }
                    };
                    if copy.path() != copy_path || copy.descriptor() != item.payload.descriptor()
                        || copy.display_name() != item.payload.display_name() {
                        return Err(boundary(io, ErrorCode::Conflict,
                            "receiver parcel receipt differs from original mail"));
                    }
                    copy
                }
            };
            let mut parts = match input.input {
                ModelContent::Text(text) => {
                    let mut parts = Vec::new();
                    crate::executor::stock_vec_push(&mut parts, crate::model::ModelContentPart::Text { text }, io)?;
                    parts
                }
                ModelContent::Part(part) => {
                    let mut parts = Vec::new();
                    crate::executor::stock_vec_push(&mut parts, part, io)?;
                    parts
                }
                ModelContent::Parts(parts) => parts,
            };
            crate::executor::stock_vec_push(&mut parts, crate::model::ModelContentPart::File {
                file: copy, policy: crate::model::FileProjectionPolicy::BoundedFull,
            }, io)?;
            input.input = ModelContent::Parts(parts);
        }
        let output_volume = input.selected_output_volume;
        let maximum = input.max_output_tokens;
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<TurnInput>().saturating_sub(std::mem::size_of::<ModelTaskCommand>()) as u64)?;
        }
        let input = TurnInput { operation_id: operation, input: input.input,
            selected_context: input.selected_context, max_steps: input.max_steps };
        let progress = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => {
                let io = io.expect("native model has its original observer");
                let work = io.original_native_work().expect("native model has its original work");
                let context = match &output_volume {
                    Some(volume) => context.with_output_volume_with_native_work(volume, work).await?,
                    None => context,
                };
                let journal = journal.as_ref().expect("native model opened its journal");
                let executor = self.runtime.stock_executor_with_native_work(task, fence, operation,
                    &context, &binding.model, Arc::clone(&binding.provider), &binding.context, journal, work)?;
                let executor = executor.with_compaction_policy_with_native_work(&binding.compaction, work)?;
                let executor = match maximum { Some(maximum) => executor.with_max_output_tokens(maximum)?, None => executor };
                let result = if reconciliation_only {
                    executor.execute_reconcile_progress_with_native_context(&context, &input, journal.as_ref(), work)?.await
                } else { executor.execute_progress_with_native_context(&context, &input, journal.as_ref(), work)?.await };
                if reconciliation_only {
                    match result {
                        Ok((progress, custody)) => { drop(progress); drop(custody); }
                        Err(Error::Indeterminate(_)) => {}
                        Err(error) => return Err(error),
                    }
                    return if journal.is_quiescent(Some(io)).await? {
                        mail::reconciled(Some(io))
                    } else { Ok(TaskCommandProgress::Indeterminate) };
                }
                let (progress, custody) = result?;
                io.absorb_native_custody(custody)?;
                progress
            }
            WorkerContext::Ordinary(_) => {
                let execution = self.runtime.stock_execution(task, fence.clone(), turn,
                    binding.model.clone(), Arc::clone(&binding.provider), binding.context.clone(), output_volume.as_ref())
                    .await?.with_compaction_policy(binding.compaction.clone());
                let execution = match maximum { Some(maximum) => execution.with_max_output_tokens(maximum)?, None => execution };
                if let Some(journal) = journal {
                    match execution.reconcile_progress(input).await {
                        Ok(_) | Err(Error::Indeterminate(_)) => {}
                        Err(error) => return Err(error),
                    }
                    return if journal.is_quiescent(io).await? { mail::reconciled(io) }
                        else { Ok(TaskCommandProgress::Indeterminate) };
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
    pub(super) async fn stock_mail_operation(
        &self, task: TaskId, fence: &LeaseFence, model: OperationId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Option<OperationId>> {
        #[cfg(not(target_arch = "wasm32"))]
        let journal = match io {
            Some(io) => self.runtime.workflow_journal_with_native_work(
                task, fence, io.original_native_work().ok_or(Error::Unsupported("original mail work is absent".into()))?,
            ).await?,
            None => self.runtime.workflow_journal(task, fence.clone()).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let journal = self.runtime.workflow_journal(task, fence.clone()).await?;
        let record = journal.observe_latest_record(io).await?
            .ok_or_else(|| Error::Unauthorized("stock model has no selection checkpoint".into()))?;
        if !record.transition.commands.iter().any(|command| command.operation_id == model
            && command.kind == MODEL_TASK_COMMAND_KIND) {
            return Err(Error::Unauthorized("stock model is not in the selected outbox".into()));
        }
        if record.next.state["mail"].is_null() { return Ok(None); }
        OperationId::deserialize(&record.next.state["receive"]["operation_id"])
            .map(Some).map_err(|error| Error::Invalid(error.to_string()))
    }
    pub(super) async fn committed_mail_input(
        &self, task: TaskId, fence: &LeaseFence, model_operation: OperationId,
        receive_operation: OperationId, context: &TaskContext, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<crate::scheduler::InboxItem> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        #[cfg(not(target_arch = "wasm32"))]
        let journal = match io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            Some(work) => self.runtime.workflow_journal_with_native_work(task, fence, work).await?,
            None => self.runtime.workflow_journal(task, fence.clone()).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let journal = self.runtime.workflow_journal(task, fence.clone()).await?;
        let latest = journal.observe_latest_record(io).await?.ok_or_else(|| boundary(
            io, ErrorCode::Unauthorized, "model steering has no committed checkpoint"))?;
        if !latest.transition.commands.iter().any(|command|
            command.operation_id == model_operation && command.kind == MODEL_TASK_COMMAND_KIND)
        {
            return Err(boundary(io, ErrorCode::Unauthorized, "model is not in the committed steering outbox"));
        }
        let previous = journal.observe_transition(latest.prior.revision, io).await?;
        let [receive] = previous.transition.commands.as_slice() else {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering requires one committed receive"));
        };
        if receive.operation_id != receive_operation || receive.kind != MAIL_RECEIVE_TASK_COMMAND_KIND
            || previous.next != latest.prior
        {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering checkpoint does not consume the receive"));
        }
        let commands = latest.input["commands"].as_array().ok_or_else(|| boundary(
            io, ErrorCode::Unauthorized, "steering checkpoint has no command result"))?;
        let [result] = commands.as_slice() else {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering checkpoint consumes more than one item"));
        };
        let result: CommittedMailResult = match io {
            Some(io) => crate::filesystem::retain_decoded(io.value_decode_preparation(result, ErrorCode::Unauthorized)?),
            None => serde_json::from_value(result.clone())
                .map_err(|cause| Error::Unauthorized(format!("invalid steering receipt: {cause}")))?,
        };
        if result.operation_id != receive_operation {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering result changed its operation"));
        }
        let Outcome::Succeeded(item) = result.value else {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering receive did not succeed"));
        };
        if item.task_id != task || item.sequence == 0 {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering item belongs to another recipient"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let bytes = match io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            Some(work) => context.read_file_with_native_work(&receive.payload, work).await?,
            None => context.read_file(&receive.payload).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let bytes = context.read_file(&receive.payload).await?;
        let selection: MailReceiveTaskCommand = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<MailReceiveTaskCommand>, &bytes, ErrorCode::Storage)?),
            None => crate::contract::json_from_slice(&bytes).map_err(|error| Error::Storage(error.to_string()))?,
        };
        if selection.after.checked_add(1) != Some(item.sequence) {
            return Err(boundary(io, ErrorCode::Unauthorized, "steering receipt changed the selected cursor"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let retained = match io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            Some(work) => {
                let (items, custody) = self.runtime.task_host().inbox_with_native_work(task, item.sequence - 1, 1, work).await?;
                io.expect("native inbox observer").absorb_native_custody(custody)?;
                items
            },
            None => self.runtime.task_host().inbox(task, item.sequence - 1, 1).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let retained = self.runtime.task_host().inbox(task, item.sequence - 1, 1).await?;
        if retained.as_slice() != std::slice::from_ref(&item) {
            return Err(boundary(io, ErrorCode::Conflict, "steering item differs from the original mail"));
        }
        Ok(item)
    }

}
