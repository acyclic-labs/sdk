use super::*;

/// The task-selected managed workspace and its publication authority.
pub struct Workspace<'a> {
    /// Local filesystem owning the view.
    pub filesystem: &'a LocalFs,
    /// Durable lineage and git state.
    pub store: &'a LocalCoreStateStore,
    /// Selected lazy workspace.
    pub view: &'a LocalLazyWorkspace,
    /// Physical view root, used to resolve patch paths.
    pub root: &'a Path,
    /// Repository identity shared by branches of this lineage.
    pub repository: acyclic_fs::WorkspaceId,
    /// Admitted operation lease, when the caller owns an operation window.
    pub lease: Option<OperationWindowLease>,
    /// Pin for an ordinary checkout that must receive published changes.
    pub checkout: Option<acyclic_fs::NativeRootIdentity>,
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
    pub switched: Option<LocalLazyWorkspace>,
}

/// Runs one git-facade command against the task-selected workspace.
pub async fn run(mut workspace: Workspace<'_>, args: &[String]) -> crate::Result<Output> {
    let permit = workspace.lease.as_ref().map_or(
        PublicationPermit::Unrestricted,
        OperationWindowLease::publication_permit,
    );
    let patch = git_apply_patch(workspace.view, workspace.root, args).await?;
    let (ignore, merge_drivers) = git_policy(workspace.view).await?;
    let tree = git_workspace_tree(workspace.view, args, Some(permit)).await?;
    let executor = WorkspaceGitExecutor {
        fs: workspace.filesystem,
        store: workspace.store,
        current: workspace.view.workspace().clone(),
        lazy_current: workspace.view,
        repository_id: workspace.repository,
        ignore,
        permit,
        lease: workspace.lease.take(),
        merge_drivers,
        merge_cache: AsyncMutex::new(MemoryMergeResolutionCache::default()),
        switched: Mutex::new(None),
    };
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
