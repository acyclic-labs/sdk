use super::*;

/// Whether a command needs an exact working tree.
pub fn git_requires_exact_workspace(argv: &[String]) -> bool {
    matches!(
        argv.first().map(String::as_str),
        Some("status" | "diff" | "blame" | "grep" | "clean" | "archive" | "check-ignore")
    )
}

/// Whether a command resolves an outstanding transition.
pub fn git_transition_command(argv: &[String]) -> bool {
    matches!(
        argv,
        [command, option] if command == "merge" && matches!(option.as_str(), "--continue" | "--abort")
    ) || argv.first().is_some_and(|command| command == "add")
}

/// Recovers a pending transition or executes one facade command.
pub async fn run_git_command<E: GitFilesystemExecutor>(
    repository: &GitCompatRepository<LocalCoreStateStore>,
    argv: &[String],
    workspace_tree: GitTreeRef,
    patch: Option<&[u8]>,
    author: &str,
    authored_at_seconds: i64,
    executor: &E,
) -> Result<Option<GitCommandOutput>, String> {
    if !git_transition_command(argv)
        && repository
            .resume(executor)
            .await
            .map_err(display)?
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
    output.map(Some).map_err(display)
}

/// Reads a patch file from the managed workspace.
pub async fn git_apply_patch(
    workspace: &LocalLazyWorkspace,
    root: &Path,
    argv: &[String],
) -> Result<Option<Vec<u8>>, String> {
    if argv.first().is_none_or(|command| command != "apply") {
        return Ok(None);
    }
    let [_, patch_path] = argv else {
        return Err("acyclic git apply requires exactly one patch file".to_owned());
    };
    if patch_path == "-" {
        return Err(
            "acyclic git apply does not accept stdin through the service; pass a patch file"
                .to_owned(),
        );
    }
    let patch_path = workspace_path_argument(root, patch_path)?;
    workspace
        .read(
            &patch_path,
            16 * 1024 * 1024,
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
        )
        .await
        .map(|chunk| Some(chunk.bytes.to_vec()))
        .map_err(display)
}

/// Captures the lazy or exact tree required by the command.
pub async fn git_workspace_tree(
    workspace: &LocalLazyWorkspace,
    argv: &[String],
    permit: Option<PublicationPermit>,
) -> Result<GitTreeRef, String> {
    if !git_requires_exact_workspace(argv) {
        return workspace
            .snapshot(&mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()))
            .await
            .map(GitTreeRef::Lazy)
            .map_err(display);
    }
    let exact = workspace
        .exactify(
            "/",
            permit.unwrap_or(PublicationPermit::Unrestricted),
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
        )
        .await
        .map_err(display)?;
    Ok(GitTreeRef::exact(workspace.workspace().id(), exact.id()))
}

/// Loads ignore and merge policies from the workspace.
pub async fn git_policy(
    workspace: &LocalLazyWorkspace,
) -> Result<(GitIgnorePolicy, Arc<MergeDriverRegistry>), String> {
    Ok((
        git_ignore_policy(workspace).await?,
        merge_drivers_for(workspace).await?,
    ))
}

/// Loads ignore rules, including reserved metadata directories.
pub async fn git_ignore_policy(workspace: &LocalLazyWorkspace) -> Result<GitIgnorePolicy, String> {
    let ignore_text = match workspace
        .read(
            "/.gitignore",
            1024 * 1024,
            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
        )
        .await
    {
        Ok(chunk) => String::from_utf8_lossy(&chunk.bytes).into_owned(),
        Err(acyclic_fs::LazyWorkspaceError::NotFound) => String::new(),
        Err(error) => return Err(display(error)),
    };
    Ok(GitIgnorePolicy::parse(&format!(
        "{ignore_text}\n{}",
        reserved_ignore_rules()
    )))
}
