use super::*;
use crate::conversation::BoundedContentFuture;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn execute_observed(
        &self,
        context: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconciliation_only: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        mail::require_native_observer(io)?;
        let task = context
            .durable_task_id()
            .ok_or_else(|| boundary(io, ErrorCode::Unauthorized, "command has no admitted task"))?;
        #[cfg(not(target_arch = "wasm32"))]
        let original = io
            .and_then(|io| io.original_native_work())
            .map(|work| context.admission(work))
            .transpose()?
            .flatten();
        #[cfg(target_arch = "wasm32")]
        let original: Option<&crate::runtime::TaskAdmissionRecord> = None;
        let observed;
        let admission = if let Some(original) = original {
            original
        } else {
            observed = self.runtime.admission_observed(task, io).await?;
            &observed
        };
        if context.as_ref().scope().grants() != &admission.grants
            || context.as_ref().scope().limits() != admission.limits
            || context.as_ref().scope().run_limits() != admission.run_limits
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
        if io.is_some() && !context.original_task_binding_matches(task, &fence) {
            return Err(boundary(
                io,
                ErrorCode::Unauthorized,
                "command differs from original task binding",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let owner = io
            .and_then(|io| io.original_native_work())
            .map(|work| context.journal_owner::<P>(&fence, work))
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let owner: Option<Arc<crate::durable_host::TaskJournalOwner<P>>> = None;
        let invocation = CommandInvocation {
            context: &context,
            admission,
            fence: &fence,
            command: &command,
            reconciliation_only,
            io,
        };
        self.verify_command_owner(owner.as_deref(), invocation)
            .await?;
        let progress = self.dispatch_command(invocation, payload)?.await;
        #[cfg(not(target_arch = "wasm32"))]
        let progress = match progress {
            Err(error) if error.is_native_failure() => return Err(error),
            progress => progress,
        };
        self.verify_command_owner(owner.as_deref(), invocation)
            .await?;
        progress
    }
    async fn verify_command_owner(
        &self,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        invocation: CommandInvocation<'_>,
    ) -> Result<()> {
        BoundedContentFuture::new(invocation.io, || async move {
            let CommandInvocation {
                admission,
                fence,
                reconciliation_only,
                io,
                ..
            } = invocation;
            match owner {
                Some(owner) => {
                    owner
                        .verify(
                            reconciliation_only,
                            &mut io
                                .map(|io| io.history_io())
                                .as_mut()
                                .map(|work| work as &mut dyn crate::store::HistoryIo),
                        )
                        .await
                }
                None => {
                    self.runtime
                        .owner_observed(admission.task_id(), fence, reconciliation_only, io)
                        .await
                }
            }
        })?
        .await
    }

    fn dispatch_command<'a>(
        &'a self,
        invocation: CommandInvocation<'a>,
        payload: Value,
    ) -> Result<BoundedContentFuture<'a, TaskCommandProgress>> {
        use crate::filesystem::task_runtime::observed::boundary;
        use crate::wire::ErrorCode;
        let CommandInvocation {
            admission,
            fence,
            command,
            reconciliation_only,
            io,
            ..
        } = invocation;
        let task = admission.task_id();
        match command.kind.as_str() {
            TOOL_TASK_COMMAND_KIND => {
                BoundedContentFuture::new(io, || self.tool_command(invocation, payload))
            }
            MODEL_TASK_COMMAND_KIND => {
                BoundedContentFuture::new(io, || self.model_command(invocation, payload))
            }
            EFFECT_TASK_COMMAND_KIND => {
                BoundedContentFuture::new(io, || self.effect_command(invocation, payload))
            }
            MAIL_SEND_TASK_COMMAND_KIND | MAIL_RECEIVE_TASK_COMMAND_KIND => {
                BoundedContentFuture::new(io, || self.mail_command(invocation, payload))
            }
            TASK_ADMIT_COMMAND_KIND
            | TASK_OBSERVE_COMMAND_KIND
            | TASK_INSPECT_COMMAND_KIND
            | TASK_JOIN_COMMAND_KIND => {
                BoundedContentFuture::new(io, || self.task_command(invocation, payload))
            }
            FORK_PREPARE_TASK_COMMAND_KIND if !reconciliation_only || io.is_some() => {
                BoundedContentFuture::new(io, || self.prepare_fork_command(invocation, payload))
            }
            TASK_ADMIT_PREPARED_COMMAND_KIND => BoundedContentFuture::new(io, || async move {
                let input: PreparedTaskAdmitCommand = mail::decode_command(payload, io)?;
                self.admit(
                    task,
                    fence,
                    command.operation_id,
                    input.admission,
                    reconciliation_only,
                    io,
                )
                .await
            }),
            TASK_CANCEL_COMMAND_KIND if !reconciliation_only => {
                BoundedContentFuture::new(io, || {
                    self.cancel_child_command(task, fence, command, payload, io)
                })
            }

            TIMER_TASK_COMMAND_KIND if !reconciliation_only => {
                BoundedContentFuture::new(io, || async move {
                    let input: TimerTaskCommand = mail::decode_command(payload, io)?;
                    let elapsed = self
                        .runtime
                        .task_host()
                        .poll_timer(
                            task,
                            fence,
                            command.operation_id,
                            input.deadline_unix_ms,
                            io,
                        )
                        .await?;
                    if elapsed {
                        ready(Outcome::Succeeded(Value::Null), io)
                    } else {
                        Ok(TaskCommandProgress::Pending)
                    }
                })
            }
            TIMER_TASK_COMMAND_KIND | TASK_CANCEL_COMMAND_KIND if reconciliation_only => {
                BoundedContentFuture::new(io, || async move { mail::reconciled(io) })
            }
            _ => Err(boundary(
                io,
                ErrorCode::Unsupported,
                "stock command kind is not bound",
            )),
        }
    }

    pub(in crate::filesystem) async fn wait_ready_observed(
        &self,
        context: &TaskContext,
        command: &WorkflowCommand,
        payload: Value,
        mail_after: Option<u64>,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        mail::require_native_observer(io)?;
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
                let mut history = io.map(crate::filesystem::PreparationIo::history_io);
                self.runtime
                    .task_host()
                    .timer_ready(
                        task,
                        command.operation_id,
                        input.deadline_unix_ms,
                        &mut history
                            .as_mut()
                            .map(|work| work as &mut dyn crate::store::HistoryIo),
                    )
                    .await
            }
            MAIL_RECEIVE_TASK_COMMAND_KIND => {
                let input: MailReceiveTaskCommand = mail::decode_command(payload, io)?;
                self.wait_mail(task, mail_after.unwrap_or(input.after), io)
                    .await
            }
            TASK_OBSERVE_COMMAND_KIND => {
                let input: TaskObserveCommand = mail::decode_command(payload, io)?;
                Ok(
                    matches!(self.observe_child(task, input.admission_operation, io).await?,
                    Some(outcome) if !matches!(outcome, Outcome::Indeterminate { .. })),
                )
            }
            TASK_JOIN_COMMAND_KIND => {
                let input: TaskJoinCommand = mail::decode_command(payload, io)?;
                Ok(
                    matches!(self.observe_direct_child(task, input.child, io).await?,
                    Some(outcome) if !matches!(outcome, Outcome::Indeterminate { .. })),
                )
            }
            TOOL_TASK_COMMAND_KIND => {
                let input: ToolTaskCommand = mail::decode_command(payload, io)?;
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
                ready.await
            }
            MODEL_TASK_COMMAND_KIND => {
                let input: ModelTaskCommand = mail::decode_command(payload, io)?;
                let ready =
                    self.runtime
                        .model_approval_ready(context, command.operation_id, &input, io);
                ready.await
            }
            _ => Ok(false),
        }
    }
}
