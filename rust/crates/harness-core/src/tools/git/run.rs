use super::*;

/// The task-selected managed workspace and its publication authority.
pub struct Workspace<'a, O: 'static = DefaultObjects, S = DefaultStore> {
    /// Authenticated workload and invocation ceiling, checked for every resolved volume.
    pub access: &'a crate::filesystem::WorkspaceAccess,
    /// Local filesystem owning the view.
    pub filesystem: &'a acyclic_fs::Fs<acyclic_fs::StreamAuthorityStore, O>,
    /// Durable lineage and git state.
    pub store: &'a S,
    /// Selected lazy workspace.
    pub view: View<'a, O>,
    /// Physical view root, used to resolve patch paths.
    pub root: &'a Path,
    /// Repository identity shared by branches of this lineage.
    pub repository: acyclic_fs::WorkspaceId,
    /// Admitted operation lease, when the caller owns an operation window.
    pub lease: Option<OperationWindowLease>,
    /// Pin for an ordinary checkout that must receive published changes.
    #[cfg(not(target_arch = "wasm32"))]
    pub checkout: Option<acyclic_fs::NativeRootIdentity>,
    /// Root-relative paths the host owns inside the view, such as bound
    /// volume aliases and their mount controls. They are never Git content.
    pub reserved: &'a [String],
    /// Commit author.
    pub author: &'a str,
    /// Commit timestamp in seconds since the Unix epoch.
    pub authored_at_seconds: i64,
    /// The request this command runs as: a state-changing command saves its
    /// result with its effect, and a replay returns that result.
    pub request: Option<acyclic_fs::GitRequest>,
}

/// A facade result and any branch view selected by checkout.
pub struct Output<O: 'static = DefaultObjects> {
    /// The request's saved result, or absent when another request's pending
    /// transition was recovered instead: the caller selects the recovered
    /// view and retries.
    pub command: Option<GitCommandOutput>,
    /// The newly selected view, if the command switched branches.
    pub switched: Option<OwnedView<O>>,
}

/// Runs one git-facade command against the task-selected workspace.
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub fn run<'a>(
    workspace: Workspace<'a>,
    args: &'a [String],
) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Output>> {
    run_with_guard(workspace, args, || std::future::ready(Ok(())))
}

/// Runs one command with `before_effect` fencing every effect. The command
/// engine is erased here, so a consumer holds a pointer instead of every
/// filesystem preparation and action nested in the command.
#[doc(hidden)]
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub fn run_with_guard<'a, F, Fut>(
    workspace: Workspace<'a>,
    args: &'a [String],
    before_effect: F,
) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Output>>
where
    F: Fn() -> Fut + acyclic_stream::ProviderPlatform + 'a,
    Fut: Future<Output = crate::Result<()>> + acyclic_stream::ProviderTask + 'a,
{
    acyclic_stream::erase_provider_future(move || run_inner(workspace, args, before_effect))
}

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
async fn run_inner<F, Fut>(
    workspace: Workspace<'_>,
    args: &[String],
    before_effect: F,
) -> crate::Result<Output>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = crate::Result<()>> + Send,
{
    let guard: &EffectGuard<'_> = &|| Box::pin(before_effect());
    let (executor, tree, patch) = match prepare(&workspace, args, guard).await? {
        Prepared::Complete(output) => return Ok(output),
        Prepared::Ready {
            executor,
            tree,
            patch,
        } => (executor, tree, patch),
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
    finish(executor, command)
}

/// Runs the shared command engine directly against a provider-backed volume.
/// No physical checkout, process or machine is involved.
/// Descendant publications retain the repository's authenticated policy cut.
/// The command engine is erased here, as for [`run_with_guard`].
pub fn run_volume_with_guard<'a, F, Fut, O, S>(
    workspace: Workspace<'a, O, S>,
    root_cut: Option<acyclic_fs::PolicyCut>,
    args: &'a [String],
    before_effect: F,
) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Output<O>>>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore + 'a,
    F: Fn() -> Fut + acyclic_stream::ProviderPlatform + 'a,
    Fut: Future<Output = crate::Result<()>> + acyclic_stream::ProviderTask + 'a,
{
    acyclic_stream::erase_provider_future(move || async move {
        if workspace.view.workspace().id() != workspace.repository && root_cut.is_none() {
            return Err(Error::Unauthorized(
                "descendant command requires its repository policy cut".into(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if workspace.checkout.is_some() || !matches!(workspace.view, View::Volume(_)) {
            return Err(Error::Invalid(
                "volume commands require a volume view".into(),
            ));
        }
        let guard: &EffectGuard<'_> = &|| Box::pin(before_effect());
        let command = async {
            let (executor, tree, patch) = match prepare(&workspace, args, guard).await? {
                Prepared::Complete(output) => return Ok(output),
                Prepared::Ready {
                    executor,
                    tree,
                    patch,
                } => (executor, tree, patch),
            };
            let command = execute(&workspace, args, tree, patch.as_deref(), &executor).await?;
            finish(executor, command)
        };
        match root_cut {
            Some(cut) => cut.scope(command).await,
            None => command.await,
        }
    })
}

enum Prepared<'a, O: 'static, S> {
    Complete(Output<O>),
    Ready {
        executor: WorkspaceGitExecutor<'a, O, S>,
        tree: GitTreeRef,
        patch: Option<acyclic_stream::OwnedBytes>,
    },
}

async fn prepare<'a, O, S>(
    workspace: &Workspace<'a, O, S>,
    args: &[String],
    before_effect: &'a EffectGuard<'a>,
) -> crate::Result<Prepared<'a, O, S>>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
    before_effect().await?;
    workspace.access.require(workspace.view.workspace()).await?;
    // A replayed request returns its saved result before reading any
    // command input (a patch file it consumed may be gone by now).
    if let Some(request) = &workspace.request
        && let Some(output) = (RepositoryAccess {
            access: workspace.access,
            filesystem: workspace.filesystem,
            store: workspace.store,
            current: workspace.view.workspace(),
            repository: workspace.repository,
        })
        .request_result(request.operation)
        .await?
    {
        before_effect().await?;
        return Ok(Prepared::Complete(Output {
            command: Some(output),
            switched: None,
        }));
    }
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
        before_effect,
        access: workspace.access,
        fs: workspace.filesystem,
        store: workspace.store,
        current: workspace.view.workspace().clone(),
        lazy_current: workspace.view,
        repository_id: workspace.repository,
        ignore,
        repository,
        permit,
        lease: workspace.lease.clone(),
        merge_drivers,
        merge_cache: AsyncMutex::new(MemoryMergeResolutionCache::default()),
        switched: Mutex::new(None),
    };
    executor.authorize_repository().await?;
    if let Some(branch) = crate::tools::fs_command::agent_diff(args) {
        before_effect().await?;
        let output = executor.diff_agent(branch, tree).await?;
        before_effect().await?;
        return Ok(Prepared::Complete(output));
    }
    Ok(Prepared::Ready {
        executor,
        tree,
        patch,
    })
}

fn finish<O, S>(
    executor: WorkspaceGitExecutor<'_, O, S>,
    command: Option<GitCommandOutput>,
) -> crate::Result<Output<O>>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
{
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

async fn execute<E, O, S>(
    workspace: &Workspace<'_, O, S>,
    args: &[String],
    tree: GitTreeRef,
    patch: Option<&[u8]>,
    executor: &E,
) -> crate::Result<Option<GitCommandOutput>>
where
    O: acyclic_fs::AsyncObjectStore + 'static,
    S: CommandStore,
    E: GitFilesystemExecutor<Error = Error>,
{
    let repository = GitCompatRepository::new(workspace.repository, workspace.store.clone());
    run_git_command(
        &match workspace.request.clone() {
            Some(request) => repository.for_request(request),
            None => repository,
        },
        args,
        tree,
        patch,
        workspace.author,
        workspace.authored_at_seconds,
        executor,
    )
    .await
}
