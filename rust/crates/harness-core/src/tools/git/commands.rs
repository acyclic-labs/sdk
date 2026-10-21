use super::*;

/// Whether a command needs an exact working tree.
///
/// A join is one: an unresolved host file reads as absent, so a branch that
/// changed it would replace whatever the host holds now, unseen. Exact, the
/// target binds the host file under the same source-derived identity the
/// branch did, and the join compares the two against their pinned base.
pub(crate) fn git_requires_exact_workspace(argv: &[String]) -> bool {
    match argv {
        [command, branch] if command == "merge" || command == "rebase" => !branch.starts_with('-'),
        _ => matches!(
            argv.first().map(String::as_str),
            Some(
                "commit"
                    | "status"
                    | "diff"
                    | "blame"
                    | "grep"
                    | "clean"
                    | "archive"
                    | "check-ignore"
            )
        ),
    }
}

/// Whether a command resolves an outstanding transition.
pub(crate) fn git_transition_command(argv: &[String]) -> bool {
    matches!(
        argv,
        [command, option] if command == "merge" && matches!(option.as_str(), "--continue" | "--abort")
    ) || argv.first().is_some_and(|command| command == "add")
}

/// Recovers a pending transition or executes one facade command.
pub(crate) async fn run_git_command<E: GitFilesystemExecutor<Error = Error>>(
    repository: &GitCompatRepository<LocalCoreStateStore>,
    argv: &[String],
    workspace_tree: GitTreeRef,
    patch: Option<&[u8]>,
    author: &str,
    authored_at_seconds: i64,
    executor: &E,
) -> Result<Option<GitCommandOutput>, Error> {
    if !git_transition_command(argv)
        && repository
            .resume(executor)
            .await
            .map_err(super::errors::run_error)?
            .is_some()
    {
        return Ok(None);
    }
    let output = if let Some(patch) = patch {
        repository
            .run(
                GitCommand::Apply {
                    patch: patch.to_vec(),
                },
                workspace_tree,
                executor,
            )
            .await
    } else {
        repository
            .run_argv(argv, workspace_tree, author, authored_at_seconds, executor)
            .await
    };
    output.map(Some).map_err(super::errors::run_error)
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
