use super::*;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn execute_observed(
        &self,
        context: WorkerContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconciliation_only: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        let task = context
            .context()
            .durable_task_id()
            .ok_or_else(|| boundary(io, ErrorCode::Unauthorized, "command has no admitted task"))?;
        let admission = self.runtime.admission_observed(task, io).await?;
        if context.context().scope().grants() != &admission.grants
            || context.context().scope().limits() != admission.limits
            || context.context().scope().run_limits() != admission.run_limits
        {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "command scope differs from retained task",
            ));
        }
        if admission.execution.is_some() {
            return Err(boundary(
                io,
                ErrorCode::Unsupported,
                "stock routed execution provider is not composed",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let WorkerContext::Native(shared) = &context {
            if !shared.as_ref().original_task_binding_matches(task, &fence) {
                return Err(boundary(
                    io,
                    ErrorCode::Unauthorized,
                    "command differs from original task binding",
                ));
            }
        }
        self.runtime
            .owner_observed(task, &fence, reconciliation_only, io)
            .await?;
        let progress = match command.kind.as_str() {
            TOOL_TASK_COMMAND_KIND => {
                self.tool_command(
                    task,
                    context,
                    &fence,
                    command.operation_id,
                    payload,
                    reconciliation_only,
                    io,
                )
                .await
            }
            MODEL_TASK_COMMAND_KIND => {
                self.model_command(
                    task,
                    context,
                    &fence,
                    command.operation_id,
                    payload,
                    reconciliation_only,
                    io,
                    admission.task.name == "acyclic.stock_turn",
                )
                .await
            }
            EFFECT_TASK_COMMAND_KIND => {
                self.effect_command(
                    task,
                    &fence,
                    command.operation_id,
                    payload,
                    reconciliation_only,
                    io,
                )
                .await
            }
            MAIL_SEND_TASK_COMMAND_KIND if reconciliation_only => {
                self.reconcile_mail(task, &fence, &command, payload, io)
                    .await
            }
            MAIL_SEND_TASK_COMMAND_KIND | MAIL_RECEIVE_TASK_COMMAND_KIND
                if !reconciliation_only =>
            {
                self.mail_command(
                    task,
                    &fence,
                    &command,
                    payload,
                    io,
                    admission.task.name == "acyclic.stock_turn",
                )
                .await
            }
            TASK_ADMIT_COMMAND_KIND | TASK_OBSERVE_COMMAND_KIND => {
                self.task_command(
                    task,
                    context.context(),
                    &fence,
                    &command,
                    payload,
                    reconciliation_only,
                    io,
                )
                .await
            }
            FORK_PREPARE_TASK_COMMAND_KIND if !reconciliation_only || io.is_some() => {
                self.prepare_fork_command(
                    task,
                    &context,
                    &fence,
                    &command,
                    payload,
                    admission.input,
                    reconciliation_only,
                    io,
                )
                .await
            }
            TASK_ADMIT_PREPARED_COMMAND_KIND => {
                let input: PreparedTaskAdmitCommand = mail::decode_command(payload, io)?;
                self.admit(
                    task,
                    &fence,
                    command.operation_id,
                    input.admission,
                    reconciliation_only,
                    io,
                )
                .await
            }
            TASK_CANCEL_COMMAND_KIND if !reconciliation_only => {
                self.cancel_child_command(task, &fence, &command, payload, io)
                    .await
            }

            TIMER_TASK_COMMAND_KIND if !reconciliation_only => {
                let input: TimerTaskCommand = mail::decode_command(payload, io)?;
                #[cfg(not(target_arch = "wasm32"))]
                let work = io
                    .map(|io| {
                        io.original_native_work().ok_or_else(|| {
                            boundary(
                                Some(io),
                                ErrorCode::Unsupported,
                                "native timer requires original work",
                            )
                        })
                    })
                    .transpose()?;
                #[cfg(target_arch = "wasm32")]
                let work = None;
                let elapsed = self
                    .runtime
                    .task_host()
                    .poll_timer(
                        task,
                        &fence,
                        command.operation_id,
                        input.deadline_unix_ms,
                        work,
                    )
                    .await?;
                if elapsed {
                    ready(Outcome::Succeeded(Value::Null), io)
                } else {
                    Ok(TaskCommandProgress::Pending)
                }
            }
            TIMER_TASK_COMMAND_KIND | MAIL_RECEIVE_TASK_COMMAND_KIND | TASK_CANCEL_COMMAND_KIND
                if reconciliation_only =>
            {
                mail::reconciled(io)
            }
            _ => Err(if io.is_some() {
                boundary(
                    io,
                    ErrorCode::Unsupported,
                    "stock command kind is not bound",
                )
            } else if reconciliation_only {
                Error::Unsupported(format!(
                    "stock command kind {} has no effect reconciliation",
                    command.kind
                ))
            } else {
                Error::Unsupported(format!("stock command kind {} is not bound", command.kind))
            }),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let progress = match progress {
            Err(error) if error.is_native_failure() => return Err(error),
            progress => progress,
        };
        self.runtime
            .owner_observed(task, &fence, reconciliation_only, io)
            .await?;
        progress
    }
    pub(in crate::filesystem) async fn wait_ready_observed(
        &self,
        context: &TaskContext,
        command: &WorkflowCommand,
        payload: Value,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        let task = context.durable_task_id().ok_or_else(|| {
            crate::filesystem::task_runtime::observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "wait has no admitted task",
            )
        })?;
        match command.kind.as_str() {
            TIMER_TASK_COMMAND_KIND => {
                if !context.scope().grants().contains("timer:wait") {
                    return Err(crate::filesystem::task_runtime::observed::boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "task scope lacks timer:wait",
                    ));
                }
                let input: TimerTaskCommand = mail::decode_command(payload, io)?;
                let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
                self.runtime
                    .task_host()
                    .timer_ready(
                        task,
                        command.operation_id,
                        input.deadline_unix_ms,
                        &mut observer
                            .as_mut()
                            .map(|work| work as &mut dyn crate::store::history::HistoryIo),
                    )
                    .await
            }
            MAIL_RECEIVE_TASK_COMMAND_KIND => {
                let input: MailReceiveTaskCommand = mail::decode_command(payload, io)?;
                self.wait_mail(task, input.after, io).await
            }
            TASK_OBSERVE_COMMAND_KIND => {
                let input: TaskObserveCommand = mail::decode_command(payload, io)?;
                let child = TaskId::from_bytes(
                    child_operation(task, input.admission_operation, io)?.into_bytes(),
                );
                if self.runtime.admission_observed(child, io).await?.parent != Some(task) {
                    return Err(crate::filesystem::task_runtime::observed::boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "wait is not for a direct child",
                    ));
                }
                Ok(matches!(self.runtime.outcome_observed(child, io).await?,
                    Some(outcome) if !matches!(outcome, Outcome::Indeterminate { .. })))
            }
            TOOL_TASK_COMMAND_KIND => {
                let input: ToolTaskCommand = mail::decode_command(payload, io)?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    io.original_native_work().ok_or_else(|| {
                        crate::filesystem::task_runtime::observed::boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "native tool wait requires original observer",
                        )
                    })?;
                }
                let definition = self
                    .runtime
                    .tool_definition(&input.name, &input.revision, io)?;
                let operation = crate::filesystem::task_runtime::execution_operation(
                    task,
                    command.operation_id,
                    io,
                )?;
                let invocation =
                    tool_invocation(context, operation, definition, input.arguments, io)?;
                let ready = self
                    .runtime
                    .tool_approval_ready(context, definition, &invocation, io);
                #[cfg(not(target_arch = "wasm32"))]
                if io.is_some() {
                    return ready.await;
                }
                Box::pin(ready).await
            }
            MODEL_TASK_COMMAND_KIND => {
                let input: ModelTaskCommand = mail::decode_command(payload, io)?;
                let ready =
                    self.runtime
                        .model_approval_ready(context, command.operation_id, &input, io);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    io.original_native_work().ok_or_else(|| {
                        crate::filesystem::task_runtime::observed::boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "native model wait requires original observer",
                        )
                    })?;
                    return ready.await;
                }
                Box::pin(ready).await
            }
            _ => Ok(false),
        }
    }
}
