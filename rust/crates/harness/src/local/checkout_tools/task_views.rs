//! Both model and direct calls select the admitted task's sparse fork.
use super::*;
use acyclic_fs::{
    CancellationToken, IdempotencyKey, LazyWorkspaceStore as _, MountOptions, WorkBudget,
    kernel::KernelAccounting,
};

impl CheckoutTools {
    pub(super) async fn select(&self, context: &ToolContext) -> Result<Option<Arc<Self>>> {
        let (task, _) = context.task().owned_task_binding(None)?;
        let owner = context
            .task()
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("tool task owner missing".into()))?;
        owner.verify_owner(None)?.await?;
        let admission = owner.admission().0;
        let task = if admission.task.name == "acyclic.direct_tool" {
            admission.parent.unwrap_or(task)
        } else {
            task
        };
        if admission.parent.is_none() && task == owner.task_binding().0 {
            return Ok(None);
        }
        let (ancestor, binding) = serialized(&self.state, async |mut state| {
            if let Some(tools) = state.children.get(&task) {
                return Ok((task, Some(Arc::clone(tools))));
            }
            let cancellation = CancellationToken::new();
            let work =
                acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut accounting = KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &cancellation,
            };
            let ancestors = context.task().ancestors(task)?;
            futures::pin_mut!(ancestors);
            let mut root = task;
            while let Some(candidate) = futures::TryStreamExt::try_next(&mut ancestors).await? {
                root = candidate;
                if let Some(tools) = state.children.get(&candidate) {
                    return Ok((candidate, Some(Arc::clone(tools))));
                }
                let id = self
                    .workspace
                    .filesystem
                    .workspace_id(&format!("task-{candidate}"))
                    .map_err(storage)?;
                if self
                    .workspace
                    .store
                    .load_lazy_workspace(id, &mut accounting)
                    .await
                    .map_err(storage)?
                    .is_none()
                {
                    continue;
                }
                let view = self
                    .fork_task(candidate, self.workspace.view(), owner)
                    .await?;
                let tools = Arc::new(self.mount_task_view(view, owner).await?);
                state.children.insert(candidate, Arc::clone(&tools));
                return Ok((candidate, Some(tools)));
            }
            Ok::<_, Error>((root, None))
        })
        .await?;
        if ancestor == task {
            return Ok(binding);
        }
        // Release the owner index before acquiring the ancestor's state. Its
        // guard pins the active branch through the durable, idempotent fork.
        let view = if let Some(parent) = binding {
            serialized(&parent.state, async |state| {
                from_selected(
                    state.active.as_ref(),
                    parent.workspace.view(),
                    async |view| self.fork_task(task, view, owner).await,
                )
                .await
            })
            .await?
        } else {
            serialized(&self.state, async |state| {
                from_selected(state.active.as_ref(), self.workspace.view(), async |view| {
                    self.fork_task(task, view, owner).await
                })
                .await
            })
            .await?
        };
        // Recheck under the owner index: concurrent selectors may have opened
        // this durable fork already. Only the winner constructs a native mount.
        serialized(&self.state, async |mut state| {
            cached_task(&mut state.children, task, None, &(), async |_| {
                self.mount_task_view(view, owner).await
            })
            .await
            .map(Some)
        })
        .await
    }
    async fn fork_task(
        &self,
        task: TaskId,
        view: &workspace::View,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
    ) -> Result<workspace::View> {
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        owner.verify_owner(None)?.await?;
        let name = format!("task-{task}");
        // Reopening a task retains its original fork even if the
        // parent's selected branch changed after child admission.
        let id = self
            .workspace
            .filesystem
            .workspace_id(&name)
            .map_err(storage)?;
        let saved = self
            .workspace
            .store
            .load_lazy_workspace(id, &mut accounting)
            .await
            .map_err(storage)?;
        let original = if let Some(saved) = saved {
            let source = saved
                .parent_workspace_id
                .ok_or_else(|| Error::Conflict("task fork has no parent".into()))?;
            if source == view.workspace().id() {
                None
            } else {
                let source = acyclic_fs::WorkspaceResolver::resolve(
                    &(
                        self.workspace.filesystem.clone(),
                        acyclic_fs::WorkspaceGraph::new(self.workspace.store.clone()),
                    ),
                    source,
                )
                .await
                .map_err(storage)?;
                Some(
                    view.open_related(source, &mut accounting)
                        .await
                        .map_err(storage)?,
                )
            }
        } else {
            None
        };
        let view = original.as_ref().unwrap_or(view);
        let (view, _) = view
            .fork(
                &name,
                IdempotencyKey::from_bytes(task.into_bytes()),
                // This lazy fork has no mount-operation lease. The
                // authority still rejects an active reservation.
                PublicationPermit::Unrestricted,
                &mut accounting,
            )
            .await
            .map_err(storage)?;
        owner.verify_owner(None)?.await?;
        Ok(view)
    }

    async fn mount_task_view(
        &self,
        view: workspace::View,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
    ) -> Result<Self> {
        // Mount retirement may outlive the previous owner. Each
        // live mount needs its own namespace so retiring it cannot
        // detach a replacement owner's mount at the same path.
        std::fs::create_dir_all(&self.workspace.mount_directory).map_err(storage)?;
        let path = self
            .workspace
            .mount_directory
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&path).map_err(storage)?;
        let mut workspace = LocalWorkspace {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(acyclic_harness_loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            state: StateArc::new(AsyncMutex::new(State::new(
                view.workspace().id(),
                Arc::clone(&self.workspace.mounts),
            ))),
            mounts: Arc::clone(&self.workspace.mounts),
            context_line: self.workspace.context_line().into(),
            path: path.clone(),
            fresh: false,
            mount_directory: self.workspace.mount_directory.clone(),
            view,
            mounted: None,
            identity: acyclic_fs::capture_root_identity(&path).map_err(storage)?,
            filesystem: self.workspace.filesystem.clone(),
            store: self.workspace.store.clone(),
        };
        let (active, pending) = workspace.selected_view().await?;
        let selected = active.as_ref().unwrap_or_else(|| workspace.view());
        let initial_view = selected.workspace().id();
        let mounted = Arc::new(
            selected
                .mount(
                    &path,
                    MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual),
                )
                .await
                .map_err(storage)?,
        );
        workspace.path = path;
        workspace.identity = acyclic_fs::capture_root_identity(&workspace.path).map_err(storage)?;
        workspace.mounted = Some(mounted);
        workspace.mounts = self.workspace.mounts_at(&workspace.path).await?;
        workspace.context_line = workspace.mounts.context_line();
        reconciliation::recover_pending(
            &workspace,
            active.as_ref().unwrap_or_else(|| workspace.view()),
            pending,
            &|| async { owner.verify_owner(None)?.await },
        )
        .await?;
        workspace.state = StateArc::new(AsyncMutex::new(State {
            active,
            initial_view,
            workspace: None,
            watcher: None,
            children: Default::default(),
            mounts: Arc::clone(&workspace.mounts),
            initialized: true,
        }));
        let workspace = Arc::new(workspace);
        let (capture_options, capture_policy) = capture::configuration(&workspace)?;
        Ok(Self {
            workspace: Arc::clone(&workspace),
            read_grant: self.read_grant.clone(),
            write_grant: self.write_grant.clone(),
            capture_options,
            capture_policy,
            clock: Arc::clone(&self.clock),
            task_files: Arc::clone(&self.task_files),
            state: StateArc::clone(&workspace.state),
            selected: true,
        })
    }
    // The caller owns this checkout's state guard. A sparse mount follows one
    // workspace; switching selects a separate mount instead of staging across
    // the mount's filesystem boundary. Failed mounts can be retried from state.
    pub(super) async fn selected_workspace(
        &self,
        state: &mut State,
        owner: &dyn crate::runtime::OwnedTaskMailbox,
    ) -> Result<Arc<LocalWorkspace>> {
        if self.workspace.mounted.is_none() {
            return Ok(self.workspace.clone());
        }
        let view = state
            .active
            .as_ref()
            .unwrap_or_else(|| self.workspace.view());
        let id = view.workspace().id();
        if id == state.initial_view {
            state.workspace = None;
            state.mounts = Arc::clone(&self.workspace.mounts);
            return Ok(self.workspace.clone());
        }
        if let Some((selected, workspace)) = &state.workspace
            && *selected == id
        {
            return Ok(workspace.clone());
        }
        owner.verify_owner(None)?.await?;
        std::fs::create_dir_all(&self.workspace.mount_directory).map_err(storage)?;
        let path = self
            .workspace
            .mount_directory
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir(&path).map_err(storage)?;
        let mounted = Arc::new(
            view.mount(
                &path,
                MountOptions::read_write().publication(acyclic_fs::MountPublication::Manual),
            )
            .await
            .map_err(storage)?,
        );
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let view = self
            .workspace
            .view()
            .open_related(
                self.workspace.view().workspace().clone(),
                &mut KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                },
            )
            .await
            .map_err(storage)?;
        let mounts = self.workspace.mounts_at(&path).await?;
        let workspace = Arc::new(LocalWorkspace {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(acyclic_harness_loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            context_line: mounts.context_line(),
            mounts: Arc::clone(&mounts),
            // This cached physical handle must not retain its owning cache.
            state: StateArc::new(AsyncMutex::new(State::new(
                view.workspace().id(),
                Arc::clone(&mounts),
            ))),
            identity: acyclic_fs::capture_root_identity(&path).map_err(storage)?,
            path,
            fresh: false,
            mount_directory: self.workspace.mount_directory.clone(),
            view,
            mounted: Some(mounted),
            filesystem: self.workspace.filesystem.clone(),
            store: self.workspace.store.clone(),
        });
        owner.verify_owner(None)?.await?;
        state.mounts = mounts;
        state.workspace = Some((id, workspace.clone()));
        Ok(workspace)
    }
}

// The caller's owned guard keeps lookup, construction and installation atomic.
pub(super) async fn cached_task<T, V>(
    cache: &mut std::collections::BTreeMap<TaskId, Arc<T>>,
    task: TaskId,
    active: Option<&V>,
    original: &V,
    create: impl AsyncFnOnce(&V) -> Result<T>,
) -> Result<Arc<T>> {
    if let Some(value) = cache.get(&task) {
        return Ok(Arc::clone(value));
    }
    let value = Arc::new(from_selected(active, original, create).await?);
    cache.insert(task, Arc::clone(&value));
    Ok(value)
}

async fn from_selected<T, V>(
    active: Option<&V>,
    original: &V,
    create: impl AsyncFnOnce(&V) -> Result<T>,
) -> Result<T> {
    create(active.unwrap_or(original)).await
}
