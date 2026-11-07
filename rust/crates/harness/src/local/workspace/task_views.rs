//! Task view bindings shared by `GraphCoder` and native-host coordination.
use super::super::checkout_tools::{State, reconciliation};
use super::*;
use acyclic_fs::{CancellationToken, MountOptions, WorkBudget};
use acyclic_stream::BoxProviderFuture;

pub(super) fn view_options(
    admission: &std::sync::OnceLock<Arc<acyclic_fs::HeldRoot>>,
    options: MountOptions,
) -> MountOptions {
    match admission.get() {
        Some(root) => options.view_root(Arc::clone(root)),
        None => options,
    }
}

impl LocalWorkspace {
    /// Binds a spawned agent to its retained project fork and this filesystem's
    /// shared native instance. Model tools and native hosts use the same view.
    pub(in crate::local) async fn bound_task_view(
        &self,
        view: workspace::View,
        project: &VolumeRef,
        scratch: VolumeRef,
        owner: &dyn super::owner::ViewOwner,
        authority: &crate::filesystem::NativeMountAuthority,
    ) -> Result<(Arc<LocalWorkspace>, Arc<Mounts>)> {
        let mut state = crate::sync::owned::lock_owned(StateArc::clone(&self.state)).await;
        let key = project.storage_name(&mut None)?;
        let mounts = if let Some(mounts) = state
            .allocated_mounts
            .get(&key)
            .and_then(std::sync::Weak::upgrade)
        {
            mounts
        } else {
            state
                .allocated_mounts
                .retain(|_, mounts| mounts.strong_count() != 0);
            let mounts = self.bound_mounts(project, scratch, authority).await?;
            state.allocated_mounts.insert(key, Arc::downgrade(&mounts));
            mounts
        };
        let workspace = self
            .mount_task_view(view, owner, Some(Arc::clone(&mounts)), authority)
            .await?;
        Ok((workspace, mounts))
    }

    /// All task views enter the filesystem's shared instance. The host may
    /// select its already admitted workspace namespace; ordinary harnesses
    /// share the process root. No host adapter mounts a task view itself.
    async fn mount_task_source(&self, view: &workspace::View) -> Result<Arc<TaskMount>> {
        let options = self.mount_options(view.workspace()).await?;
        view.mount_view(view_options(&self.task_view_admission, options))
            .await
            .map(Arc::new)
            .map_err(storage)
    }

    /// The physical counterpart of [`Self::mount_task_source`]: `view`'s
    /// complete visible tree as a plain directory below the checkout's
    /// reserved view root, needing no native provider. Its first binding
    /// materializes the view through the canonical exact-tree and
    /// materialization engines; every later binding reuses the directory,
    /// whose effects capture reconciles like the checkout's own.
    async fn physical_task_source(&self, view: &workspace::View) -> Result<PathBuf> {
        let parent = self
            .held_task_mount_root()?
            .create_directories(
                Path::new(super::PHYSICAL_VIEW_ROOT),
                acyclic_fs::NativeDurability::Durable,
            )
            .map_err(storage)?;
        // The binding owns this directory. Selecting another Git workspace
        // must neither share another agent's directory nor orphan this one.
        let name = hex::encode(self.view().workspace().id().into_bytes());
        let path = parent.path().join(name);
        if !path.try_exists().map_err(storage)? {
            let cancellation = CancellationToken::new();
            // Promote every lazily inherited entry first, so the
            // materialized bytes are the view's own exact generation.
            let generation = view
                .exactify(
                    "/",
                    acyclic_fs::PublicationPermit::Unrestricted,
                    &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
                )
                .await
                .map_err(storage)?;
            let staging = super::bootstrap::staging(parent.path())?;
            super::bootstrap::initialize(
                async {
                    generation
                        .materialize(
                            ["/"].into_iter(),
                            &acyclic_fs::MaterializeOptions::native(&staging),
                            WorkBudget::UNBOUNDED,
                            &cancellation,
                        )
                        .await
                        .map_err(storage)?;
                    Ok(())
                },
                || {
                    acyclic_native_runtime::durable_rename(
                        &staging,
                        &path,
                        acyclic_native_runtime::RenameMode::NoReplace,
                        None,
                    )
                },
                || {
                    acyclic_native_runtime::remove_child_directory(parent.path(), &staging)
                        .map_err(storage)
                },
            )
            .await?;
        }
        std::fs::canonicalize(&path).map_err(storage)
    }

    /// Archive releases only the closed agent's original physical project
    /// binding. Root checkout and shared volumes remain available to owners
    /// that still hold them; retained logical history is independent.
    pub(in crate::local) fn release_physical_bindings(
        &self,
        bindings: &[crate::agent::VolumeBinding],
    ) -> Result<()> {
        if self.mounting != WorkspaceMounting::Physical {
            return Ok(());
        }
        let Some(project) = bindings.iter().find(|binding| binding.name == "project") else {
            return Ok(());
        };
        if project.volume.class() == crate::conversation::VolumeClass::SessionShared {
            return Ok(());
        }
        let id = self
            .filesystem
            .workspace_id(&project.volume.storage_name(&mut None)?)
            .map_err(storage)?;
        if id == self.view().workspace().id() {
            return Ok(());
        }
        let root = self.held_task_mount_root()?;
        let parent = match root.open_directory(Path::new(super::PHYSICAL_VIEW_ROOT)) {
            Ok(parent) => parent,
            Err(acyclic_fs::DemandError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(error) => return Err(storage(error)),
        };
        parent
            .remove_tree(Path::new(&hex::encode(id.into_bytes())))
            .map_err(storage)
    }

    // Child admission and tools share this boundary over native mount setup.
    pub(in crate::local) fn mount_task_view<'a>(
        &'a self,
        view: workspace::View,
        owner: &'a dyn super::owner::ViewOwner,
        mounts: Option<Arc<workspace::Mounts>>,
        authority: &'a crate::filesystem::NativeMountAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<LocalWorkspace>>> {
        Box::pin(async move {
            // Mount retirement may outlive the previous owner. Each
            // live mount needs its own namespace so retiring it cannot
            // detach a replacement owner's mount at the same path.
            owner.verify().await?;
            let mut workspace = LocalWorkspace {
                #[cfg(all(test, not(loom)))]
                selection_reads: std::sync::atomic::AtomicUsize::new(0),
                #[cfg(all(test, not(loom)))]
                mount_advances: std::sync::atomic::AtomicUsize::new(0),
                state: StateArc::new(AsyncMutex::new(State::new(
                    view.workspace().id(),
                    Arc::clone(&self.mounts),
                ))),
                mounts: mounts.unwrap_or_else(|| Arc::clone(&self.mounts)),
                context_line: self.context_line().into(),
                processes: self.processes.clone(),
                path: self.path.clone(),
                fresh: false,
                import_source: false,
                mount_directory: self.mount_directory.clone(),
                task_view_admission: Arc::clone(&self.task_view_admission),
                view,
                mounted: None,
                mounting: self.mounting,
                identity: acyclic_fs::NativeRootIdentity::from_bytes([0; 16]),
                filesystem: self.filesystem.clone(),
                store: self.store.clone(),
                mount_authority: Some(authority.clone()),
            };
            let (active, pending) = workspace.selected_view().await?;
            let selected = active.as_ref().unwrap_or_else(|| workspace.view());
            let initial_view = selected.workspace().id();
            // A physical view stays unmounted: its commands, file tools and
            // capture take the checkout's plain-directory path.
            match self.mounting {
                WorkspaceMounting::Native => {
                    let mounted = workspace.mount_task_source(selected).await?;
                    workspace.path = mounted.path().to_path_buf();
                    workspace.mounted = Some(mounted);
                }
                WorkspaceMounting::Physical => {
                    workspace.path = workspace.physical_task_source(selected).await?;
                }
            }
            workspace.identity =
                acyclic_fs::capture_root_identity(&workspace.path).map_err(storage)?;
            workspace.mounts = workspace
                .mounts_at_from(&workspace.path, Some(&self.mounts))
                .await?;
            workspace.context_line = workspace.mounts.context_line();
            reconciliation::recover_pending(
                &workspace,
                active.as_ref().unwrap_or_else(|| workspace.view()),
                pending,
                &|| owner.verify(),
            )
            .await?;
            workspace.state = StateArc::new(AsyncMutex::new(State {
                active,
                initialized: true,
                ..State::new(initial_view, Arc::clone(&workspace.mounts))
            }));
            let workspace = Arc::new(workspace);
            Ok(workspace)
        })
    }
    /// Returns the physical binding already admitted by checkout selection.
    /// Publication shares the state guard and cannot create another mount owner.
    pub(in crate::local) fn cached_selected_workspace(
        self: &Arc<Self>,
        state: &State,
    ) -> Option<Arc<LocalWorkspace>> {
        let view = state.active.as_ref().unwrap_or_else(|| self.view());
        if self.mounted.is_none() || view.workspace().id() == state.initial_view {
            return Some(Arc::clone(self));
        }
        state.workspace.as_ref().and_then(|(selected, workspace)| {
            (*selected == view.workspace().id()).then(|| Arc::clone(workspace))
        })
    }

    // The caller owns this checkout's state guard. A sparse mount follows one
    // workspace; switching selects a separate mount instead of staging across
    // the mount's filesystem boundary. Failed mounts can be retried from state.
    pub(in crate::local) async fn selected_workspace(
        self: &Arc<Self>,
        state: &mut State,
        owner: &dyn super::owner::ViewOwner,
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
        if let Some(workspace) = self.cached_selected_workspace(state) {
            return Ok(workspace);
        }
        owner.verify().await?;
        let mounted = self.mount_task_source(view).await?;
        let path = mounted.path().to_path_buf();
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
            #[cfg(all(test, not(loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(loom)))]
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
            import_source: false,
            mount_directory: self.mount_directory.clone(),
            task_view_admission: Arc::clone(&self.task_view_admission),
            view,
            mounted: Some(mounted),
            mounting: self.mounting,
            filesystem: self.filesystem.clone(),
            store: self.store.clone(),
            mount_authority: self.mount_authority.clone(),
        });
        owner.verify().await?;
        state.mounts = mounts;
        state.workspace = Some((id, workspace.clone()));
        Ok(workspace)
    }
}
