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
        if !child_lifecycle(&parsed.arguments) {
            return Err(Error::Invalid("fs supports status and a child fork's lifecycle: diff, merge or discard agents/<name>, add <path> and merge --continue or --abort while resolving its conflicts; recovery: use shell for other git commands".into()));
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

/// The model-visible fs grammar: working-tree status and one child fork's
/// lifecycle. A parent inspects, merges or discards `agents/<name>`, and a conflicted merge is
/// completed the way the join's own refusal instructs, by marking resolved
/// paths and continuing or aborting. Every other git command belongs to shell.
fn child_lifecycle(arguments: &[String]) -> bool {
    match arguments {
        [operation] if operation == "status" => true,
        [operation, argument] if matches!(operation.as_str(), "diff" | "merge" | "discard") => {
            (operation == "merge" && matches!(argument.as_str(), "--continue" | "--abort"))
                || argument
                    .strip_prefix("agents/")
                    .is_some_and(|name| !name.is_empty())
        }
        [operation, paths @ ..] if operation == "add" => {
            !paths.is_empty() && paths.iter().all(|path| !path.starts_with('-'))
        }
        _ => false,
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn child_lifecycle_admission_preserves_selected_volume_authority() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let model =
            crate::model::Model::new("scripted".into(), "child-lifecycle".into(), "1".into())?;
        let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
        let harness = LocalHarness::open(
            crate::test_support::local_harness_config(directory.path(), (model, provider))?
                .state_directory(state.path()),
        )
        .await?;
        let tools = CheckoutTools::new(
            harness
                .workspace()
                .ok_or_else(|| Error::NotFound("checkout".into()))?
                .clone(),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            &harness.verifier,
            None,
        )
        .await?;
        let granted = RuntimeScope::new(harness.signed.capabilities().clone(), Default::default())?;
        let denied = RuntimeScope::new(Default::default(), Default::default())?;
        let mut refused = Vec::new();
        for (target, volume) in [("", None), ("--volume home ", Some("home"))] {
            let (_, mount) = tools.workspace.mounts.named(volume)?;
            let read_only = RuntimeScope::new(
                Capabilities::new([mount
                    .view
                    .volume
                    .capability(VolumeOperation::Read, &mut None)?]),
                Default::default(),
            )?;
            for command in [
                "status",
                "diff agents/child",
                "merge agents/child",
                "discard agents/child",
                "add resolved.txt",
                "merge --continue",
                "merge --abort",
            ] {
                let command = format!("{target}{command}");
                if let Err(error) = tools.fs_arguments(&command, &granted) {
                    refused.push((command.clone(), error));
                }
                assert!(
                    matches!(
                        tools.fs_arguments(&command, &denied),
                        Err(Error::Unauthorized(_))
                    ),
                    "child command bypassed volume authority: {command}"
                );
                if command.ends_with("diff agents/child") || command.ends_with("status") {
                    assert!(tools.fs_arguments(&command, &read_only).is_ok());
                } else {
                    assert!(
                        matches!(
                            tools.fs_arguments(&command, &read_only),
                            Err(Error::Unauthorized(_))
                        ),
                        "child mutation bypassed write authority: {command}"
                    );
                }
            }
            for command in [
                "merge main",
                "diff agents/",
                "add --all",
                "status --porcelain",
            ] {
                let command = format!("{target}{command}");
                assert!(
                    tools.fs_arguments(&command, &granted).is_err(),
                    "unrelated command entered the child lifecycle: {command}"
                );
            }
        }
        assert!(
            refused.is_empty(),
            "authorized child commands refused: {refused:?}"
        );
        Ok(())
    }
}
