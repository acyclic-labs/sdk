//! Tool projection of the shared workspace git facade.
pub(crate) use super::super::workspace::git::reconciliation_generation;
use super::*;

impl CheckoutTools {
    pub(crate) fn commands(&self) -> workspace::git::Commands<'_> {
        workspace::git::Commands {
            workspace: &self.workspace,
            signed: &self.signed,
            verifier: &self.verifier,
            clock: self.clock.as_ref(),
            capture_options: &self.capture_options,
            capture_policy: &self.capture_policy,
        }
    }

    pub(crate) fn fs_arguments(
        &self,
        command: &str,
        scope: &RuntimeScope,
    ) -> Result<crate::tools::FsCommand> {
        let parsed = self.commands().fs_arguments(command, scope)?;
        if !child_lifecycle(&parsed.arguments) {
            let flag = parsed
                .arguments
                .iter()
                .find(|arg| arg.starts_with('-'))
                .map_or(String::new(), |flag| {
                    format!("invalid option or operands near {flag}; ")
                });
            return Err(Error::Invalid(format!(
                "{flag}fs accepts: status; diff [--stat] CHILD (also diff CHILD --stat); show CHILD:PATH or HEAD:PATH [--offset N] (read-only 64 KiB pages, follow nextOffset); merge CHILD; discard CHILD; add PATH...; merge --continue; merge --abort. CHILD is agents/<name> or a direct child's AgentPath; show also accepts the spawn name. Use shell for other git commands"
            )));
        }
        Ok(parsed)
    }

    pub(crate) async fn fs(
        &self,
        mut parsed: crate::tools::FsCommand,
        context: &ToolContext,
        state: &mut State,
        workspace: &Arc<LocalWorkspace>,
    ) -> Result<ToolResult> {
        // A parent names its spawned children as `agents/<name>` branches of
        // its own checkout.
        if let Some((agent, volumes)) = context.task().agent()
            && let Some(resolver) = context.task().agent_children()
        {
            workspace
                .register_named_children(agent, resolver, volumes, &mut parsed.arguments)
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

/// The model-visible fs grammar: working-tree status and one child fork's lifecycle. A parent
/// inspects paths and reads, merges or discards `agents/<name>` or its canonical AgentPath, and a conflicted merge is
/// completed the way the join's own refusal instructs, by marking resolved
/// paths and continuing or aborting. Every other git command belongs to shell.
fn child_lifecycle(arguments: &[String]) -> bool {
    if crate::tools::child_branch_operand(arguments).is_some() {
        return true;
    }
    if arguments
        .first()
        .is_some_and(|operation| operation == "show")
    {
        return matches!(acyclic_fs::git_compat::parse_git_argv::<std::io::Error>(arguments, "", 0),
            Ok(acyclic_fs::git_compat::GitCommand::Show { object: Some(object), .. })
                if object.strip_prefix("HEAD:").is_some_and(|path| !path.is_empty()));
    }
    match arguments {
        [operation] if operation == "status" => true,
        [operation, argument] if matches!(operation.as_str(), "diff" | "merge" | "discard") => {
            (operation == "merge" && matches!(argument.as_str(), "--continue" | "--abort"))
                || argument
                    .strip_prefix("agents/")
                    .is_some_and(|name| !name.is_empty())
                || argument.starts_with("harness/agents/")
        }
        [operation, paths @ ..] if operation == "add" => {
            !paths.is_empty() && paths.iter().all(|path| !path.starts_with('-'))
        }
        _ => false,
    }
}
