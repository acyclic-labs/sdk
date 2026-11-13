//! Tool projection of the shared workspace git facade.
pub(super) use super::super::workspace::git::reconciliation_generation;
use super::*;

impl CheckoutTools {
    pub(super) fn commands(&self) -> workspace::git::Commands<'_> {
        workspace::git::Commands {
            workspace: &self.workspace,
            signed: &self.signed,
            verifier: &self.verifier,
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
            let flag = parsed
                .arguments
                .iter()
                .find(|arg| arg.starts_with('-'))
                .map_or(String::new(), |flag| {
                    format!("invalid option or operands near {flag}; ")
                });
            return Err(Error::Invalid(format!(
                "{flag}fs accepts: status; diff [--stat] CHILD (also diff CHILD --stat); show CHILD:PATH or HEAD:PATH [--offset N] (read-only 64 KiB pages, follow nextOffset); merge CHILD; discard CHILD; add PATH...; merge --continue; merge --abort. CHILD is agents/<name> or a direct child's AgentPath. Use shell for other git commands"
            )));
        }
        Ok(parsed)
    }

    pub(super) async fn fs(
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
            self.workspace
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
        let structured = RuntimeScope::new(
            Capabilities::new([acyclic_fs::access::Capability::whole_volume(
                None,
                &acyclic_fs::access::Action::ALL,
            )
            .to_scope_label()
            .map_err(storage)?]),
            Default::default(),
        )?;
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
                "diff agents/child --stat",
                "diff --stat agents/child",
                "show agents/child:findings/report.md",
                "show agents/child:findings/report.md --offset 65536",
                "show HEAD:findings/report.md",
                "show HEAD:findings/report.md --offset 65536",
                "merge agents/child",
                "discard agents/child",
                "add resolved.txt",
                "merge --continue",
                "merge --abort",
            ] {
                let command = format!("{target}{command}");
                for scope in [&granted, &structured] {
                    if let Err(error) = tools.fs_arguments(&command, scope) {
                        refused.push((command.clone(), error));
                    }
                }
                assert!(
                    matches!(
                        tools.fs_arguments(&command, &denied),
                        Err(Error::Unauthorized(_))
                    ),
                    "child command bypassed volume authority: {command}"
                );
                if command.contains("diff ")
                    || command.contains("show ")
                    || command.ends_with("status")
                {
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
                "diff agents/child --unknown",
                "show agents/child:",
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

    fn scope_for(harness: &LocalHarness) -> Result<RuntimeScope> {
        RuntimeScope::new(harness.signed.capabilities().clone(), Default::default())
    }

    #[tokio::test]
    async fn conflicted_fs_returns_paths_and_refuses_unrepaired_continuation() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(directory.path().join("shared.txt"), b"base\n").map_err(storage)?;
        let harness = LocalHarness::open(
            crate::LocalHarnessConfig::native_host(directory.path())
                .state_directory(state.path())
                .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                .without_default_tools(),
        )
        .await?;
        let workspace = harness
            .workspace()
            .ok_or_else(|| Error::NotFound("checkout".into()))?;
        let tools = CheckoutTools::new(
            workspace.clone(),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            &harness.verifier,
            None,
        )
        .await?;
        let scope = RuntimeScope::new(harness.signed.capabilities().clone(), Default::default())?;
        let task = Arc::new(crate::runtime::test_context::agent_context(
            scope_for(&harness)?,
            None,
        )?);
        let context = ToolContext::new(task, OperationId::new(), "conflict-branch")?;
        let mut checkout = lock_owned(StateArc::clone(&tools.state)).await;
        tools
            .fs(
                crate::tools::parse_fs_command("branch agents/repair")?,
                &context,
                &mut checkout,
                workspace,
            )
            .await?;
        let repository = acyclic_fs::GitCompatRepository::new(
            workspace.view().workspace().id(),
            workspace.store.clone(),
        );
        let child = acyclic_fs::WorkspaceResolver::resolve(
            &(
                workspace.filesystem.clone(),
                acyclic_fs::WorkspaceGraph::new(workspace.store.clone()),
            ),
            repository.load().await.map_err(storage)?.branches["agents/repair"].workspace_id,
        )
        .await
        .map_err(storage)?;
        child
            .write_text("/shared.txt", "child\n")
            .await
            .map_err(storage)?;
        std::fs::write(directory.path().join("shared.txt"), b"parent\n").map_err(storage)?;
        for command in ["merge agents/repair", "merge --continue"] {
            let task = Arc::new(crate::runtime::test_context::agent_context(
                scope_for(&harness)?,
                None,
            )?);
            let context = ToolContext::new(task, OperationId::new(), "conflicted-merge")?;
            let result = tools
                .fs(
                    tools.fs_arguments(command, &scope)?,
                    &context,
                    &mut checkout,
                    workspace,
                )
                .await?;
            assert_eq!(result.value["kind"], "merge conflict", "{result:?}");
            assert_eq!(result.value["paths"], serde_json::json!(["/shared.txt"]));
            let text =
                std::fs::read_to_string(directory.path().join("shared.txt")).map_err(storage)?;
            assert!(
                text.contains("<<<<<<<") && text.contains("parent") && text.contains("child"),
                "{text}"
            );
        }
        std::fs::write(directory.path().join("shared.txt"), b"repaired\n").map_err(storage)?;
        for command in ["add shared.txt", "merge --continue"] {
            let task = Arc::new(crate::runtime::test_context::agent_context(
                scope_for(&harness)?,
                None,
            )?);
            let context = ToolContext::new(task, OperationId::new(), "repaired-merge")?;
            let result = tools
                .fs(
                    tools.fs_arguments(command, &scope)?,
                    &context,
                    &mut checkout,
                    workspace,
                )
                .await?;
            assert!(result.value.get("error").is_none(), "{result:?}");
        }
        assert_eq!(
            std::fs::read(directory.path().join("shared.txt")).map_err(storage)?,
            b"repaired\n"
        );
        Ok(())
    }
}
