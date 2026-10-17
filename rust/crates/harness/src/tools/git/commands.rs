use super::*;

/// Whether a command needs an exact working tree.
pub(crate) fn git_requires_exact_workspace(argv: &[String]) -> bool {
    matches!(
        argv.first().map(String::as_str),
        Some(
            "commit" | "status" | "diff" | "blame" | "grep" | "clean" | "archive" | "check-ignore"
        )
    )
}

/// Whether a command resolves an outstanding transition.
pub(crate) fn git_transition_command(argv: &[String]) -> bool {
    matches!(
        argv,
        [command, option] if command == "merge" && matches!(option.as_str(), "--continue" | "--abort")
    ) || argv.first().is_some_and(|command| command == "add")
}

/// Recovers a pending transition or executes one facade command.
impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn run_git_command<E: GitFilesystemExecutor<Error = Error>>(
        &self,
        argv: &[String],
        workspace_tree: GitTreeRef,
        patch: Option<&[u8]>,
        author: &str,
        authored_at_seconds: i64,
        executor: &E,
    ) -> Result<Option<GitCommandOutput>, Error> {
        let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
        if !git_transition_command(argv)
            && repository
                .resume(executor)
                .await
                .map_err(super::errors::run_error)?
                .is_some()
        {
            return Ok(None);
        }
        let command = if let Some(patch) = patch {
            GitCommand::Apply {
                patch: patch.to_vec(),
            }
        } else {
            acyclic_fs::git_compat::parse_git_argv(argv, author, authored_at_seconds)
                .map_err(super::errors::compat_error)?
        };
        let output = match command {
            GitCommand::Restore {
                source: Some(source),
                paths,
            } if source.starts_with("agents/") => {
                let (tree, _) = self.agent_tree(&source, workspace_tree).await?;
                repository
                    .restore_paths_from(tree, paths, workspace_tree, executor)
                    .await
            }
            command => repository.run(command, workspace_tree, executor).await,
        };
        output.map(Some).map_err(super::errors::run_error)
    }
}

/// Reads a patch file from the managed workspace.
pub(crate) async fn git_apply_patch(
    workspace: View<'_>,
    root: &Path,
    argv: &[String],
) -> Result<Option<acyclic_stream::OwnedBytes>, Error> {
    if argv.first().is_none_or(|command| command != "apply") {
        return Ok(None);
    }
    let [_, patch_path] = argv else {
        return Err(Error::Invalid(
            "acyclic git apply requires exactly one patch file".to_owned(),
        ));
    };
    if patch_path == "-" {
        return Err(Error::Invalid(
            "acyclic git apply does not accept stdin through the service; pass a patch file"
                .to_owned(),
        ));
    }
    let patch_path = workspace_path_argument(root, patch_path)?;
    workspace
        .read(&patch_path, 16 * 1024 * 1024)
        .await
        .map(Some)
}

/// Captures the lazy or exact tree required by the command.
pub(crate) async fn git_workspace_tree(
    workspace: View<'_>,
    argv: &[String],
    permit: Option<PublicationPermit>,
) -> Result<GitTreeRef, Error> {
    workspace
        .tree(
            git_requires_exact_workspace(argv),
            permit.unwrap_or(PublicationPermit::Unrestricted),
        )
        .await
}

/// Loads ignore and merge policies from the workspace.
pub(crate) async fn git_policy(
    workspace: View<'_>,
) -> Result<(GitIgnorePolicy, Arc<MergeDriverRegistry>), Error> {
    Ok((
        crate::filesystem::workspace_policy::ignore_for(workspace).await?,
        crate::filesystem::workspace_policy::merge_drivers_for(workspace).await?,
    ))
}
