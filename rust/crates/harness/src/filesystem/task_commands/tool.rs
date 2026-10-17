use super::*;
use crate::filesystem::PreparationIo;
#[cfg(not(target_arch = "wasm32"))]
use crate::filesystem::task_runtime::observed::boundary;
#[cfg(not(target_arch = "wasm32"))]
use crate::wire::ErrorCode;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn command_journal(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        turn: OperationId,
        _native_context: &Arc<TaskContext>,
        io: Option<&PreparationIo>,
    ) -> Result<(
        Arc<super::super::FilesystemExecutionJournal<P, A, O>>,
        OperationId,
    )> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            return self
                .runtime
                .execution_journal_with_native_context(
                    task,
                    fence,
                    turn,
                    work,
                    Some(Arc::clone(_native_context)),
                )
                .await;
        }
        Ok((
            self.runtime
                .execution_journal(task, fence.clone(), turn)
                .await?,
            super::super::task_runtime::execution_operation(task, turn, io)?,
        ))
    }

    pub(super) async fn tool_command(
        &self,
        CommandInvocation {
            context,
            admission,
            fence,
            command,
            reconciliation_only,
            io,
        }: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let task = admission.task_id();
        let context = if io.is_none() {
            Arc::new(self.runtime.task_context(task, fence.clone()).await?)
        } else {
            Arc::clone(context)
        };
        let turn = command.operation_id;
        let input: ToolTaskCommand = mail::decode_command(payload, io)?;
        let definition = self
            .runtime
            .tool_definition(&input.name, &input.revision, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io
            .map(|io| {
                io.original_native_work()
                    .ok_or_else(|| io.invalid("dispatch requires original native work"))
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let work = None;
        let (journal, operation) = self
            .command_journal(task, fence, turn, &context, io)
            .await?;
        if reconciliation_only
            && !was_dispatched(journal.as_ref(), operation, TOOL_TASK_COMMAND_KIND, io).await?
        {
            return mail::reconciled(io);
        }
        let invocation =
            tool_invocation(context.as_ref(), operation, definition, input.arguments, io)?;
        let limits = context.as_ref().scope().limits();
        let context = ToolContext::new(context, operation, &invocation.call_id, io)?;
        let context = 'context: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = work {
                break 'context self.runtime.bind_native_tool_interactions(
                    context,
                    task,
                    fence,
                    turn,
                    Arc::clone(&journal),
                    work,
                )?;
            }
            let task_context = self.runtime.bind_context_interactions(
                context.task().clone(),
                task,
                Arc::clone(&journal),
            )?;
            break 'context ToolContext::new(
                Arc::new(task_context),
                operation,
                &invocation.call_id,
                None,
            )?;
        };
        if !reconciliation_only || io.is_none() {
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let records = journal.replay_page(operation, 0, 1, false, io).await?;
            let hold = io
                .map(|io| io.guard_owned(io.owned_since(before)?))
                .transpose()?;
            let empty = records.is_empty();
            drop((records, hold));
            if empty {
                let approved = 'approval: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = work {
                        break 'approval context
                            .authorize_tool(definition, &invocation, work)
                            .await;
                    }
                    break 'approval context.task().authorize_tool(definition, &invocation).await;
                };
                match approved {
                    Ok(Some(_)) => return Ok(TaskCommandProgress::Pending),
                    Ok(None) => {}
                    Err(Error::InteractionRejected(reason)) => return rejected(reason, io),
                    Err(error) => return Err(error),
                }
            }
        }
        let runner = DurableToolRunner::new(self.runtime.tool_registry(), journal, work)?
            .with_limits(limits)?;
        let (outcome, _native_custody) = runner
            .run(
                task,
                definition,
                &invocation,
                &context,
                work,
                reconciliation_only,
            )?
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            io.absorb_native_custody(_native_custody.ok_or_else(|| {
                boundary(
                    Some(io),
                    ErrorCode::Storage,
                    "native tool outcome has no custody",
                )
            })?)?;
        }
        match outcome {
            Outcome::Indeterminate { .. } => Ok(TaskCommandProgress::Indeterminate),
            outcome => ready(outcome, io),
        }
    }
}
pub(super) async fn was_dispatched(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    kind: &str,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<bool> {
    let dispatched = std::sync::atomic::AtomicBool::new(false);
    crate::executor::replay_execution(
        journal,
        operation,
        0,
        |event| {
            if matches!(
                (kind, event),
                (
                    MODEL_TASK_COMMAND_KIND,
                    crate::executor::ExecutionEvent::ModelStarted { .. }
                ) | (
                    TOOL_TASK_COMMAND_KIND,
                    crate::executor::ExecutionEvent::ToolStarted { .. }
                )
            ) {
                dispatched.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            false
        },
        io,
    )
    .await?;
    Ok(dispatched.load(std::sync::atomic::Ordering::Relaxed))
}
