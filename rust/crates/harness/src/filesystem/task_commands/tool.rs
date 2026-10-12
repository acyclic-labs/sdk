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
    pub(super) async fn tool_command(
        &self,
        task: TaskId,
        context: WorkerContext,
        fence: &LeaseFence,
        turn: OperationId,
        payload: Value,
        reconciliation_only: bool,
        io: Option<&PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        let input: ToolTaskCommand = mail::decode_command(payload, io)?;
        let definition = self
            .runtime
            .tool_definition(&input.name, &input.revision, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.map(|io| io.original_native_work().expect("native dispatch observer"));
        #[cfg(target_arch = "wasm32")]
        let work = None;
        let (journal, operation) = match &context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(_) => {
                self.runtime
                    .execution_journal_with_native_work(
                        task,
                        fence,
                        turn,
                        work.expect("native dispatch work"),
                    )
                    .await?
            }
            WorkerContext::Ordinary(_) => (
                self.runtime
                    .execution_journal(task, fence.clone(), turn)
                    .await?,
                crate::filesystem::task_runtime::execution_operation(task, turn, io)?,
            ),
        };
        if reconciliation_only
            && !was_dispatched(journal.as_ref(), operation, TOOL_TASK_COMMAND_KIND, io).await?
        {
            return mail::reconciled(io);
        }
        let invocation = tool_invocation(
            context.context(),
            operation,
            definition,
            input.arguments,
            io,
        )?;
        let limits = context.context().scope().limits();
        let context = match context {
            #[cfg(not(target_arch = "wasm32"))]
            WorkerContext::Native(context) => {
                let work = work.expect("native dispatch work");
                let context = ToolContext::new(context, operation, &invocation.call_id, io)?;
                self.runtime.bind_native_tool_interactions(
                    context,
                    task,
                    fence,
                    turn,
                    Arc::clone(&journal),
                    work,
                )?
            }
            WorkerContext::Ordinary(context) => ToolContext::new(
                Arc::new(self.runtime.bind_context_interactions(
                    context,
                    task,
                    Arc::clone(&journal),
                )?),
                operation,
                &invocation.call_id,
                None,
            )?,
        };
        if !reconciliation_only || io.is_none() {
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let records = journal.replay_page(operation, 0, 1, false, io).await?;
            let hold = io
                .map(|io| {
                    io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        boundary(
                            Some(io),
                            ErrorCode::Invalid,
                            "tool replay custody underflows",
                        )
                    })?)
                })
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
                    context.task().authorize_tool(definition, &invocation).await
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
        let (outcome, custody) = runner
            .run(task, definition, &invocation, &context, work, reconciliation_only)?
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            io.absorb_native_custody(custody.ok_or_else(|| {
                boundary(Some(io), ErrorCode::Storage, "native tool outcome has no custody")
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
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| {
            crate::filesystem::task_runtime::observed::boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "dispatch replay requires original work",
            )
        })?;
        if !journal.supports_native_execution(work)? {
            return Err(crate::filesystem::task_runtime::observed::boundary(
                Some(io),
                crate::wire::ErrorCode::Unsupported,
                "native execution journal is unavailable",
            ));
        }
    }
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
