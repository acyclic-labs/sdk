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
                "{flag}fs accepts: status; diff [--stat] CHILD (also diff CHILD --stat); show CHILD:PATH or HEAD:PATH [--offset N] (read-only 64 KiB pages, follow nextOffset); merge CHILD; discard CHILD; add PATH...; merge --continue; merge --abort. CHILD is agents/<name> or a direct child's AgentPath; show also accepts the spawn name. Use shell for other git commands"
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

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use acyclic_fs::facade::FsWork as _;

    struct SelectedChild {
        parent: crate::agent::AgentPath,
        child: crate::agent::AgentPath,
    }

    impl crate::agent::TurnChildren for SelectedChild {
        fn agent(&self) -> &crate::agent::AgentPath {
            &self.parent
        }
        fn resolve<'a>(
            &'a self,
            parent: &'a crate::agent::AgentPath,
            name: &'a str,
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<crate::agent::SpawnResolution>> {
            Box::pin(async move {
                if parent != &self.parent || name != "review/worker" {
                    return Err(Error::NotFound(name.into()));
                }
                Ok(crate::agent::SpawnResolution::Live(0, self.child.clone()))
            })
        }
        fn select<'a>(
            &'a self,
            _: OperationId,
            _: &'a [String],
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<Vec<(String, crate::agent::AgentPath)>>>
        {
            unreachable!("filesystem reads do not select a wait")
        }
        fn settled<'a>(
            &'a self,
            _: &'a crate::agent::AgentPath,
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<crate::agent::Settlement>> {
            unreachable!("filesystem reads do not observe settlement")
        }
        fn answer<'a>(
            &'a self,
            _: &'a crate::agent::AgentPath,
            _: u64,
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<(String, crate::agent::TurnOutcome)>>
        {
            unreachable!("filesystem reads do not read conversation answers")
        }
    }

    #[tokio::test]
    async fn nested_parent_reads_child_from_selected_workspace_with_shared_tools() -> Result<()> {
        use crate::agent::{AgentId, AgentPath, WorkspaceEntry, WorkspaceRole};
        use crate::conversation::{VolumeClass, VolumeOwner, VolumeRef};
        let directory = tempfile::tempdir().map_err(storage)?;
        let state_directory = tempfile::tempdir().map_err(storage)?;
        let model =
            crate::model::Model::new("scripted".into(), "nested-inspection".into(), "1".into())?;
        let harness = LocalHarness::open(
            crate::test_support::local_harness_config(
                directory.path(),
                (
                    model,
                    Arc::new(crate::test_support::ScriptedModelProvider::new([])),
                ),
            )?
            .state_directory(state_directory.path())
            .workspace_mounting(crate::local::WorkspaceMounting::Physical),
        )
        .await?;
        let root = harness
            .workspace()
            .ok_or_else(|| Error::NotFound("root workspace".into()))?;
        let root_agent = AgentPath::root(&AgentId::from_bytes([11; 16]));
        let parent_agent = root_agent
            .child(&AgentId::from_bytes([12; 16]))
            .map_err(storage)?;
        let child_agent = crate::tools::spawned_child(&parent_agent, "review/worker")?;
        let project = VolumeRef::new(
            harness.project.provider().clone(),
            "nested-project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("nested-inspection".into()),
            &mut None,
        )?;
        let binding =
            WorkspaceEntry::initial_volume(WorkspaceRole::User, "project".into(), project.clone());
        let child_volume =
            crate::filesystem::spawned_binding(&child_agent, &binding).map_err(storage)?;
        let grants = Capabilities::new(
            harness
                .signed
                .capabilities()
                .iter()
                .map(str::to_owned)
                .chain(
                    [&project, &child_volume]
                        .into_iter()
                        .flat_map(|volume| {
                            [
                                VolumeOperation::Read,
                                VolumeOperation::Write,
                                VolumeOperation::Delete,
                            ]
                            .into_iter()
                            .map(move |operation| volume.capability(operation, &mut None))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
        );
        let signed = harness
            .root_authority
            .as_ref()
            .ok_or_else(|| Error::NotFound("signing custody".into()))?
            .bootstrap_scope("nested-inspection", &grants, None, &mut None)?;
        let authority = crate::filesystem::NativeMountAuthority::admitted(
            harness.verifier.clone(),
            signed.copy(&mut None)?,
        );
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut work = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        root.view()
            .fork(
                &project.storage_name(&mut None)?,
                acyclic_fs::IdempotencyKey::new(),
                acyclic_fs::PublicationPermit::Unrestricted,
                &mut work,
            )
            .await
            .map_err(storage)?;
        let fork = root.spawned_project_view(&project).await?;
        let parent = root
            .bound_task_view(
                fork,
                &project,
                &[],
                &workspace::owner::AgentOwner,
                &authority,
            )
            .await?;
        let (child, _) = parent
            .view()
            .fork(
                &child_volume.storage_name(&mut None)?,
                acyclic_fs::IdempotencyKey::new(),
                acyclic_fs::PublicationPermit::Unrestricted,
                &mut work,
            )
            .await
            .map_err(storage)?;
        child
            .workspace()
            .write_text("/review.md", "review complete\n")
            .await
            .map_err(storage)?;
        let tools = CheckoutTools::new(
            root.clone(),
            &harness.project,
            harness.clock.clone(),
            &signed,
            &harness.verifier,
            None,
        )
        .await?;
        let scope = RuntimeScope::new(grants, Default::default())?;
        let task = Arc::new(
            crate::runtime::test_context::agent_context(scope.clone(), None)?
                .for_agent(parent_agent.clone(), vec![binding])
                .with_agent_children(Some(Arc::new(SelectedChild {
                    parent: parent_agent,
                    child: child_agent.clone(),
                }))),
        );
        let mut state = parent.state.lock().await;
        for reference in [
            "review/worker".to_owned(),
            "agents/review/worker".to_owned(),
            child_agent.to_string(),
        ] {
            let context = ToolContext::new(task.clone(), OperationId::new(), "inspect-review")?;
            let result = tools
                .fs(
                    tools.fs_arguments(&format!("show {reference}:review.md"), &scope)?,
                    &context,
                    &mut state,
                    &parent,
                )
                .await?;
            let output: acyclic_fs::GitCommandOutput =
                serde_json::from_value(result.value["result"].clone()).map_err(storage)?;
            let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
                value,
                ..
            }) = output
            else {
                return Err(Error::Invalid("child file result missing".into()));
            };
            assert_eq!(value["text"], "review complete\n");
        }
        let context = ToolContext::new(task, OperationId::new(), "diff-review")?;
        let result = tools
            .fs(
                tools.fs_arguments("diff agents/review/worker --stat", &scope)?,
                &context,
                &mut state,
                &parent,
            )
            .await?;
        let output: acyclic_fs::GitCommandOutput =
            serde_json::from_value(result.value["result"].clone()).map_err(storage)?;
        let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
            value,
            ..
        }) = output
        else {
            return Err(Error::Invalid("child diff result missing".into()));
        };
        assert_eq!(value["fileChanges"], 1);
        let repository = acyclic_fs::GitCompatRepository::new(
            parent.view().workspace().id(),
            parent.store.clone(),
        );
        assert_eq!(
            repository.load().await.map_err(storage)?.branches[child_agent.as_str()].workspace_id,
            child.workspace().id()
        );
        let root_repository =
            acyclic_fs::GitCompatRepository::new(root.view().workspace().id(), root.store.clone());
        assert!(
            !root_repository
                .load()
                .await
                .map_err(storage)?
                .branches
                .contains_key(child_agent.as_str())
        );
        Ok(())
    }

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
                "show child:findings/report.md",
                "show child:findings/report.md --offset 65536",
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
