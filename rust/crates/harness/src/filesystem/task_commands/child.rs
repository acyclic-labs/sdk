use super::*;
use crate::filesystem::task_runtime::observed::boundary;
use crate::wire::ErrorCode;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn child_mail_after(
        &self,
        context: &TaskContext,
        task: TaskId,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<u64> {
        let journal = self
            .command_workflow_journal(context, task, fence, io)
            .await?;
        let latest = journal.observe_latest_record(io).await?.ok_or_else(|| {
            boundary(
                io,
                ErrorCode::Unauthorized,
                "child receive checkpoint is absent",
            )
        })?;
        super::super::child_agent::mail_cursor(&latest.transition, command)
            .map_err(|reason| boundary(io, ErrorCode::Unauthorized, reason))
    }

    pub(in crate::filesystem) async fn command_workflow_journal(
        &self,
        context: &TaskContext,
        task: TaskId,
        fence: &LeaseFence,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Arc<crate::filesystem::FilesystemWorkflowJournal<P, A, O>>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(work) = io.and_then(crate::filesystem::PreparationIo::original_native_work)
            {
                return self
                    .runtime
                    .workflow_journal_with_native_work(task, fence, work)
                    .await;
            }
            if context.owned_mailbox.is_some() {
                if !context.original_task_binding_matches(task, fence) {
                    return Err(boundary(
                        io,
                        ErrorCode::Unauthorized,
                        "command journal differs from original task binding",
                    ));
                }
                return self.runtime.workflow_journal_with_context(context);
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = (context, io);
        self.runtime.workflow_journal(task, fence.clone()).await
    }
}
