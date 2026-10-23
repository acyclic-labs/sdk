//! Tool projection of the shared workspace git facade.
pub(super) use super::super::workspace::git::reconciliation_generation;
use super::*;

impl CheckoutTools {
    fn commands(&self) -> workspace::git::Commands<'_> {
        workspace::git::Commands {
            workspace: &self.workspace,
            clock: self.clock.as_ref(),
            capture_options: &self.capture_options,
            capture_policy: &self.capture_policy,
        }
    }

    pub(super) fn fs_arguments(
        &self,
        command: &str,
        scope: &RuntimeScope,
    ) -> Result<crate::tools::FsCommand> {
        self.commands().fs_arguments(command, scope)
    }

    pub(super) async fn fs(
        &self,
        parsed: crate::tools::FsCommand,
        context: &ToolContext,
        state: &mut State,
        workspace: &Arc<LocalWorkspace>,
    ) -> Result<ToolResult> {
        match self
            .commands()
            .fs(
                parsed,
                &workspace::git::CommandContext {
                    task: context.task(),
                    operation: context.operation_id(),
                    scope: context.effective_scope(),
                },
                state,
                workspace,
            )
            .await?
        {
            workspace::git::Outcome::Completed(command) => Ok(ToolResult {
                value: serde_json::json!({"result": command}),
            }),
            workspace::git::Outcome::Rejected(error) => Ok(crate::tools::rejected_input(*error)),
        }
    }
}
