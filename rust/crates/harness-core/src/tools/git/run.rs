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
    mut workspace: Workspace<'_>,
    args: &[String],
    before_effect: F,
) -> crate::Result<Output>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = crate::Result<()>> + Send,
{
    before_effect().await?;
    let permit = workspace.lease.as_ref().map_or(
        PublicationPermit::Unrestricted,
        OperationWindowLease::publication_permit,
    );
    let patch = git_apply_patch(workspace.view, workspace.root, args).await?;
    let (ignore, merge_drivers) = git_policy(workspace.view).await?;
    let ignore = ignore.reserving(workspace.reserved);
    let repository =
        crate::filesystem::workspace_policy::git_view_policy("").reserving(workspace.reserved);
    let tree = git_workspace_tree(workspace.view, args, Some(permit)).await?;
    let executor = WorkspaceGitExecutor {
        before_effect: &before_effect,
        fs: workspace.filesystem,
        store: workspace.store,
        current: workspace.view.workspace().clone(),
        lazy_current: workspace.view,
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
        let output = executor.diff_agent(branch, tree).await?;
        before_effect().await?;
        return Ok(output);
    }
    let command = if let Some(identity) = workspace.checkout {
        execute(
            &workspace,
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
        execute(&workspace, args, tree, patch.as_deref(), &executor).await?
    };
    Ok(Output {
        command,
        switched: executor.switched_workspace()?,
    })
}

async fn execute<E: GitFilesystemExecutor<Error = Error>>(
    workspace: &Workspace<'_>,
    args: &[String],
    tree: GitTreeRef,
    patch: Option<&[u8]>,
    executor: &E,
) -> crate::Result<Option<GitCommandOutput>> {
    run_git_command(
        &GitCompatRepository::new(workspace.repository, workspace.store.clone()),
        args,
        tree,
        patch,
        workspace.author,
        workspace.authored_at_seconds,
        executor,
    )
    .await
}
