use super::*;

/// The task-selected managed workspace and its publication authority.
pub struct Workspace<'a> {
    /// Local filesystem owning the view.
    pub filesystem: &'a LocalFs,
    /// Durable lineage and git state.
    pub store: &'a LocalCoreStateStore,
    /// Selected lazy workspace.
    pub view: View<'a>,
    /// Physical view root, used to resolve patch paths.
    pub root: &'a Path,
    /// Repository identity shared by branches of this lineage.
    pub repository: acyclic_fs::WorkspaceId,
    /// Admitted operation lease, when the caller owns an operation window.
    pub lease: Option<OperationWindowLease>,
    /// Pin for an ordinary checkout that must receive published changes.
    pub checkout: Option<acyclic_fs::NativeRootIdentity>,
    /// Root-relative paths the host owns inside the view, such as bound
    /// volume aliases and their mount controls. They are never Git content.
    pub reserved: &'a [String],
    /// Commit author.
    pub author: &'a str,
    /// Commit timestamp in seconds since the Unix epoch.
    pub authored_at_seconds: i64,
}

/// A facade result and any branch view selected by checkout.
pub struct Output {
    /// Absent when an earlier pending transition was recovered instead.
    pub command: Option<GitCommandOutput>,
    /// The newly selected view, if the command switched branches.
    pub switched: Option<OwnedView>,
}

/// Runs one git-facade command against the task-selected workspace.
pub async fn run(workspace: Workspace<'_>, args: &[String]) -> crate::Result<Output> {
    run_with_guard(workspace, args, || std::future::ready(Ok(()))).await
}

#[doc(hidden)]
pub async fn run_with_guard<F, Fut>(
    workspace: Workspace<'_>,
    args: &[String],
    before_effect: F,
) -> crate::Result<Output>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = crate::Result<()>> + Send,
{
    Box::pin(run_inner(workspace, args, None, before_effect)).await
}

/// Runs an inspection with disposable working-tree objects and no publication.
#[doc(hidden)]
pub async fn inspect_with_guard<'a, F, Fut>(
    workspace: Workspace<'a>,
    args: &[String],
    view: acyclic_fs::NativeGitView<'a>,
    before_effect: F,
) -> crate::Result<Output>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = crate::Result<()>> + Send,
{
    if !is_read_only(args) || workspace.lease.is_some() || workspace.checkout.is_some() {
        return Err(Error::Invalid(
            "native inspection requires a read-only command without publication authority".into(),
        ));
    }
    Box::pin(run_inner(workspace, args, Some(view), before_effect)).await
}

async fn run_inner<'a, F, Fut>(
    mut workspace: Workspace<'a>,
    args: &[String],
    inspection: Option<acyclic_fs::NativeGitView<'a>>,
    before_effect: F,
) -> crate::Result<Output>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = crate::Result<()>> + Send,
{
    before_effect().await?;
    let read_only = is_read_only(args);
    let permit = workspace.lease.as_ref().map_or(
        PublicationPermit::Unrestricted,
        OperationWindowLease::publication_permit,
    );
    let patch = Box::pin(git_apply_patch(workspace.view, workspace.root, args)).await?;
    let (ignore, merge_drivers) = Box::pin(git_policy(if read_only {
        View::Volume(workspace.view.workspace())
    } else {
        workspace.view
    }))
    .await?;
    let ignore = ignore.reserving(workspace.reserved);
    let repository =
        crate::filesystem::workspace_policy::git_view_policy("").reserving(workspace.reserved);
    let tree = if read_only {
        Box::pin(readonly::workspace_tree(workspace.view)).await?
    } else {
        Box::pin(git_workspace_tree(workspace.view, args, Some(permit))).await?
    };
    let store = if read_only {
        workspace.store.inspecting()
    } else {
        workspace.store.clone()
    };
    let executor = WorkspaceGitExecutor {
        before_effect: &before_effect,
        fs: workspace.filesystem,
        store: &store,
        current: workspace.view.workspace().clone(),
        lazy_current: workspace.view,
        inspection: inspection.map(|view| (tree, view)),
        read_only,
        repository_id: workspace.repository,
        ignore,
        repository,
        permit,
        lease: workspace.lease.take(),
        merge_drivers,
        merge_cache: AsyncMutex::new(MemoryMergeResolutionCache::default()),
        switched: Mutex::new(None),
    };
    if let Some(branch) = crate::tools::fs_command::agent_diff(args) {
        before_effect().await?;
        let output = Box::pin(executor.diff_agent(branch, tree)).await?;
        before_effect().await?;
        return Ok(output);
    }
    let command = if let Some(identity) = workspace.checkout {
        execute(
            &workspace,
            &store,
            args,
            tree,
            patch.as_deref(),
            &RootMaterializingGitExecutor {
                inner: &executor,
                root: workspace.root,
                source_root_identity: identity,
            },
        )
        .await?
    } else {
        execute(&workspace, &store, args, tree, patch.as_deref(), &executor).await?
    };
    let command = match command {
        Some(GitCommandOutput::Status(status)) => {
            before_effect().await?;
            // Keep native tree inspection's future independent of the complete
            // command dispatch future (which also contains publication paths).
            let status = Box::pin(executor.status_read_only(status)).await?;
            before_effect().await?;
            Some(GitCommandOutput::Status(status))
        }
        command => command,
    };
    Ok(Output {
        command: command.map(|output| without_host_paths(output, &executor.repository)),
        switched: executor.switched_workspace()?,
    })
}

/// Keeps host paths out of listings, including from history captured before
/// the repository policy reserved them: `ls-files`, `log` and `show` list
/// only what the repository view holds, without waiting for a new capture.
fn without_host_paths(output: GitCommandOutput, repository: &GitIgnorePolicy) -> GitCommandOutput {
    match output {
        GitCommandOutput::Paths(paths) => GitCommandOutput::Paths(
            paths
                .into_iter()
                .filter(|path| !repository.reserves(path))
                .collect(),
        ),
        GitCommandOutput::Commits(commits) => GitCommandOutput::Commits(
            commits
                .into_iter()
                .map(|mut commit| {
                    commit
                        .tracked_paths
                        .retain(|path| !repository.reserves(path));
                    commit
                })
                .collect(),
        ),
        output => output,
    }
}

async fn execute<E: GitFilesystemExecutor<Error = Error>>(
    workspace: &Workspace<'_>,
    store: &LocalCoreStateStore,
    args: &[String],
    tree: GitTreeRef,
    patch: Option<&[u8]>,
    executor: &E,
) -> crate::Result<Option<GitCommandOutput>> {
    Box::pin(run_git_command(
        &GitCompatRepository::new(workspace.repository, store.clone()),
        args,
        tree,
        patch,
        workspace.author,
        workspace.authored_at_seconds,
        executor,
    ))
    .await
}
