//! Task forks and mounts shared by `GraphCoder` and native-host coordination.
use super::super::checkout_tools::{State, reconciliation};
use super::*;
use acyclic_fs::PublicationPermit;
use acyclic_fs::{CancellationToken, IdempotencyKey, MountOptions, WorkBudget};

impl LocalWorkspace {
    pub(in crate::local) async fn fork_task(
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
        let id = self.filesystem.workspace_id(&name).map_err(storage)?;
        let saved = self
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
                        self.filesystem.clone(),
                        acyclic_fs::WorkspaceGraph::new(self.store.clone()),
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

    pub(in crate::local) async fn mount_task_view(
        &self,
        view: workspace::View,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
        mounts: Option<Arc<workspace::Mounts>>,
        mount_directory: &Path,
    ) -> Result<Arc<LocalWorkspace>> {
        // Mount retirement may outlive the previous owner. Each
        // live mount needs its own namespace so retiring it cannot
        // detach a replacement owner's mount at the same path.
        owner.verify_owner(None)?.await?;
        if mount_directory != self.mount_directory {
            return Err(Error::Invalid(
                "task mount root differs from the shared checkout".into(),
            ));
        }
        let path = self
            .held_task_mount_root()?
            .create_directories(Path::new(&crate::identity::fresh_uuid().to_string()))
            .map_err(storage)?
            .path()
            .to_path_buf();
        let mut workspace = LocalWorkspace {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(acyclic_harness_loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            state: StateArc::new(AsyncMutex::new(State::new(
                view.workspace().id(),
                Arc::clone(&self.mounts),
            ))),
            mounts: mounts.unwrap_or_else(|| Arc::clone(&self.mounts)),
            context_line: self.context_line().into(),
            processes: self.processes.clone(),
            path: path.clone(),
            fresh: false,
            mount_directory: mount_directory.to_path_buf(),
            view,
            mounted: None,
            identity: acyclic_fs::capture_root_identity(&path).map_err(storage)?,
            filesystem: self.filesystem.clone(),
            store: self.store.clone(),
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
        workspace.mounts = workspace
            .mounts_at_from(&workspace.path, Some(&self.mounts))
            .await?;
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
            allocated_mounts: Default::default(),
            mounts: Arc::clone(&workspace.mounts),
            initialized: true,
        }));
        let workspace = Arc::new(workspace);
        Ok(workspace)
    }
    // The caller owns this checkout's state guard. A sparse mount follows one
    // workspace; switching selects a separate mount instead of staging across
    // the mount's filesystem boundary. Failed mounts can be retried from state.
    pub(in crate::local) async fn selected_workspace(
        self: &Arc<Self>,
        state: &mut State,
        owner: &dyn crate::runtime::OwnedTaskMailbox,
    ) -> Result<Arc<LocalWorkspace>> {
        if self.mounted.is_none() {
            return Ok(self.clone());
        }
        let view = state.active.as_ref().unwrap_or_else(|| self.view());
        let id = view.workspace().id();
        if id == state.initial_view {
            state.workspace = None;
            state.mounts = Arc::clone(&self.mounts);
            return Ok(self.clone());
        }
        if let Some((selected, workspace)) = &state.workspace
            && *selected == id
        {
            return Ok(workspace.clone());
        }
        owner.verify_owner(None)?.await?;
        let path = self
            .held_task_mount_root()?
            .create_directories(Path::new(&crate::identity::fresh_uuid().to_string()))
            .map_err(storage)?
            .path()
            .to_path_buf();
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
            .view()
            .open_related(
                self.view().workspace().clone(),
                &mut KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                },
            )
            .await
            .map_err(storage)?;
        let mounts = self.mounts_at(&path).await?;
        let workspace = Arc::new(LocalWorkspace {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(acyclic_harness_loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            context_line: mounts.context_line(),
            processes: self.processes.clone(),
            mounts: Arc::clone(&mounts),
            // This cached physical handle must not retain its owning cache.
            state: StateArc::new(AsyncMutex::new(State::new(
                view.workspace().id(),
                Arc::clone(&mounts),
            ))),
            identity: acyclic_fs::capture_root_identity(&path).map_err(storage)?,
            path,
            fresh: false,
            mount_directory: self.mount_directory.clone(),
            view,
            mounted: Some(mounted),
            filesystem: self.filesystem.clone(),
            store: self.store.clone(),
        });
        owner.verify_owner(None)?.await?;
        state.mounts = mounts;
        state.workspace = Some((id, workspace.clone()));
        Ok(workspace)
    }
}
