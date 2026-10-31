//! Tool projection of the shared workspace git facade.
pub(super) use super::super::workspace::git::reconciliation_generation;
use super::*;

impl CheckoutTools {
    fn commands(&self) -> workspace::git::Commands<'_> {
        workspace::git::Commands {
            workspace: &self.workspace,
            signed: &self.signed,
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
        let parsed = self.commands().fs_arguments(command, scope)?;
        if !matches!(parsed.arguments.as_slice(), [operation, branch]
            if matches!(operation.as_str(), "diff" | "merge")
                && branch.strip_prefix("agents/").is_some_and(|name| !name.is_empty()))
        {
            return Err(Error::Invalid("fs supports child diff and merge only; recovery: use fs with diff agents/<name> or merge agents/<name>, or use shell for other git commands".into()));
        }
        Ok(parsed)
    }

    pub(super) async fn fs(
        &self,
        parsed: crate::tools::FsCommand,
        context: &ToolContext,
        state: &mut State,
        workspace: &Arc<LocalWorkspace>,
    ) -> Result<ToolResult> {
        // A parent names its spawned children as `agents/<name>` branches of
        // its own checkout.
        if let Some((agent, volumes)) = context.task().agent() {
            self.workspace
                .register_named_children(agent, volumes, &parsed.arguments)
                .await?;
        }
        match self
            .commands()
            .fs(
                parsed,
                &workspace::git::CommandContext {
                    owner: context.task(),
                    operation: context.operation_id(),
                    scope: context.effective_scope(),
                    request: None,
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
