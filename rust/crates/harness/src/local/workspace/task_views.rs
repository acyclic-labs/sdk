//! Task view bindings shared by `GraphCoder` and native-host coordination.
use super::super::checkout_tools::{State, reconciliation};
use super::*;
use acyclic_fs::{CancellationToken, MountOptions, WorkBudget};
use acyclic_stream::BoxProviderFuture;

#[cfg(all(test, not(loom)))]
mod concurrency_tests;
#[cfg(all(test, not(loom)))]
mod tests;

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
        bindings: &[crate::agent::WorkspaceEntry],
        owner: &dyn super::owner::ViewOwner,
        authority: &crate::filesystem::NativeMountAuthority,
    ) -> Result<Arc<LocalWorkspace>> {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let key = bindings::volume_key(project)?;
        let auxiliary_key = bindings::auxiliary_key(bindings)?;
        let (selection, producer) =
            bindings::Bindings::select(&self.bindings, key, auxiliary_key, &work, &cancellation)?;
        let binding_owner = selection.binding.clone();
        let mounts = if let Some(producer) = producer {
            let mounts = self
                .bound_mounts(project, bindings, authority, &selection.binding)
                .await?;
            selection.publish(&mounts)?;
            producer.release();
            mounts
        } else {
            let mounts = selection.mounts().await?;
            // A retained result is physical custody, never fresh authority.
            // Recheck the actual caller against each retained auxiliary source.
            for (_, mount) in mounts.mounts() {
                if let Some(source) = &mount.view.source {
                    authority
                        .mount_options(
                            source.workspace.workspace(),
                            Some(&mount.view.volume),
                            !mount.mode.read_only(),
                            &work,
                        )
                        .await?;
                }
            }
            mounts
        };
        {
            let cached = binding_owner.workspace.lock().await;
            if let Some(workspace) = cached.upgrade() {
                drop(cached);
                owner.verify().await?;
                return Ok(workspace);
            }
        }
        // Verify without the per-binding guard: another verified caller may
        // finish the primary view while this owner's verification is pending.
        owner.verify().await?;
        let mut cached = binding_owner.workspace.lock().await;
        if let Some(workspace) = cached.upgrade() {
            drop(cached);
            owner.verify().await?;
            return Ok(workspace);
        }
        let workspace = self
            .mount_task_view(
                view,
                owner,
                Some((mounts, binding_owner.clone())),
                authority,
            )
            .await?;
        *cached = Arc::downgrade(&workspace);
        Ok(workspace)
    }

    /// All task views enter the filesystem's shared instance. The host may
    /// select its already admitted workspace namespace; ordinary harnesses
    /// share the process root. No host adapter mounts a task view itself.
    async fn mount_task_source(&self, view: &workspace::View) -> Result<Arc<TaskMount>> {
        volumes::require_available(self.native_mount_probe.probe())?;
        let options = self.mount_options(view.workspace()).await?;
        view.mount_view(view_options(&self.task_view_admission, options))
            .await
            .map(Arc::new)
            .map_err(storage)
    }

    /// A logical project binding needs no OS mount or physical task directory.
    pub(in crate::local) fn context_task_view(
        &self,
        view: workspace::View,
        project: &VolumeRef,
        bindings: &[crate::agent::WorkspaceEntry],
        authority: &crate::filesystem::NativeMountAuthority,
    ) -> Result<Arc<LocalWorkspace>> {
        let mounts = Arc::new(mounts::owner(
            &self.path,
            project,
            None,
            mounts::auxiliary(bindings).map(|(name, volume)| (name.clone(), volume.clone(), None)),
        )?);
        Ok(Arc::new(self.unmounted_task_view(view, mounts, authority)))
    }

    fn unmounted_task_view(
        &self,
        view: workspace::View,
        mounts: Arc<Mounts>,
        authority: &crate::filesystem::NativeMountAuthority,
    ) -> LocalWorkspace {
        LocalWorkspace {
            shutdown: self.shutdown.clone(),
            native_mount_probe: self.native_mount_probe.clone(),
            #[cfg(all(test, not(loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            state: StateArc::new(AsyncMutex::new(State::new(view.workspace().id()))),
            publication_debt: std::sync::atomic::AtomicBool::new(true),
            mounts: Arc::clone(&mounts),
            binding: None,
            binding_owner: None,
            bindings: self.bindings.clone(),
            context_line: mounts.context_line(),
            processes: self.processes.clone(),
            path: self.path.clone(),
            fresh: false,
            import_source: false,
            source_policy: self.source_policy.clone(),
            source_root: self.source_root.clone(),
            mount_directory: self.mount_directory.clone(),
            host_state: self.host_state.clone(),
            task_view_admission: Arc::clone(&self.task_view_admission),
            view,
            mounted: None,
            mounting: self.mounting,
            identity: acyclic_fs::NativeRootIdentity::from_bytes([0; 16]),
            filesystem: self.filesystem.clone(),
            store: self.store.clone(),
            mount_authority: Some(authority.clone()),
        }
    }

    // Child admission and tools share this boundary over native mount setup.
    pub(in crate::local) fn mount_task_view<'a>(
        &'a self,
        view: workspace::View,
        owner: &'a dyn super::owner::ViewOwner,
        mounts: Option<(
            Arc<workspace::Mounts>,
            acyclic_fs::kernel::allocation::RetainedArc<bindings::Binding>,
        )>,
        authority: &'a crate::filesystem::NativeMountAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<LocalWorkspace>>> {
        Box::pin(async move {
            // Mount retirement may outlive the previous owner. Each
            // live mount needs its own namespace so retiring it cannot
            // detach a replacement owner's mount at the same path.
            owner.verify().await?;
            let (mounts, binding_owner) = mounts.unzip();
            let mut workspace = self.unmounted_task_view(
                view,
                mounts.unwrap_or_else(|| Arc::clone(&self.mounts)),
                authority,
            );
            workspace.binding = binding_owner
                .as_ref()
                .map(|_| Arc::clone(&workspace.mounts));
            workspace.binding_owner = binding_owner.map(|binding| binding.erase());
            let reuse_primary = self.mounting == WorkspaceMounting::Physical
                && workspace.view().workspace().id() == self.view().workspace().id();
            let (active, pending) = if reuse_primary {
                (None, None)
            } else {
                workspace.selected_view().await?
            };
            let selected = active.as_ref().unwrap_or_else(|| workspace.view());
            let initial_view = selected.workspace().id();
            // Only the admitted primary checkout is a plain directory. Forks
            // retain their volume cut and demand-project through the shared
            // native instance; binding never exactifies or copies their tree.
            if reuse_primary {
                workspace.path = self.path.clone();
                workspace.identity = self.identity;
            } else {
                let mounted = workspace.mount_task_source(selected).await?;
                workspace.path = mounted.path().to_path_buf();
                workspace.identity = served_identity(&mounted)?;
                workspace.mounted = Some(mounted);
                workspace.mounting = WorkspaceMounting::Native;
            }
            workspace.mounts = workspace.mounts_at(&workspace.path).await?;
            workspace.context_line = workspace.mounts.context_line();
            if reuse_primary {
                // CheckoutTools owns recovery and initialization under this
                // original guard, including its branch and capture cursor.
                workspace.state = StateArc::clone(&self.state);
            } else {
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
                    ..State::new(initial_view)
                }));
            }
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
            shutdown: self.shutdown.clone(),
            native_mount_probe: self.native_mount_probe.clone(),
            #[cfg(all(test, not(loom)))]
            selection_reads: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(all(test, not(loom)))]
            mount_advances: std::sync::atomic::AtomicUsize::new(0),
            context_line: mounts.context_line(),
            processes: self.processes.clone(),
            mounts: Arc::clone(&mounts),
            binding: self.binding.clone(),
            binding_owner: self.binding_owner.clone(),
            bindings: self.bindings.clone(),
            // This cached physical handle must not retain its owning cache.
            state: StateArc::new(AsyncMutex::new(State::new(view.workspace().id()))),
            publication_debt: std::sync::atomic::AtomicBool::new(true),
            identity: served_identity(&mounted)?,
            path,
            fresh: false,
            import_source: false,
            source_policy: self.source_policy.clone(),
            source_root: self.source_root.clone(),
            mount_directory: self.mount_directory.clone(),
            host_state: self.host_state.clone(),
            task_view_admission: Arc::clone(&self.task_view_admission),
            view,
            mounted: Some(mounted),
            mounting: self.mounting,
            filesystem: self.filesystem.clone(),
            store: self.store.clone(),
            mount_authority: self.mount_authority.clone(),
        });
        owner.verify().await?;
        state.workspace = Some((id, workspace.clone()));
        Ok(workspace)
    }
}

/// A served view's root identity, from the mount that serves it. Its owner
/// never asks the kernel: the kernel would forward that request to callbacks
/// that can wait on the owner, so a coordination thread that asks deadlocks.
fn served_identity(mounted: &TaskMount) -> Result<acyclic_fs::NativeRootIdentity> {
    mounted
        .native_root_identity()
        .ok_or_else(|| Error::Unauthorized("task view is no longer mounted".into()))
}

impl LocalWorkspace {
    /// The root's current identity: a served view's from its mount, any
    /// other root's from the directory itself.
    pub(in crate::local) fn root_identity(&self) -> Result<acyclic_fs::NativeRootIdentity> {
        match &self.mounted {
            Some(mounted) => served_identity(mounted),
            None => acyclic_fs::capture_root_identity(&self.path).map_err(storage),
        }
    }
}
