use super::*;
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    LazyWorkspace, LazyWorkspaceStore, LocalCoreStateStore, NativeMaterializationWork,
    demand::native::NativeDemandSource, kernel::KernelAccounting,
};
mod bootstrap;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod performance_tests;

mod mounts;
pub(super) mod projection;
pub(super) mod volumes;
#[cfg(all(test, feature = "native-execution", unix))]
pub(super) use crate::sync::owned::lock_owned;
pub(super) use crate::sync::owned::{Arc as StateArc, Mutex as AsyncMutex};

pub(super) struct MountedVolume {
    pub(super) workspace: crate::tools::git::OwnedView,
    pub(super) destination: PathBuf,
    pub(super) native: StateArc<AsyncMutex<Option<projection::Native>>>,
    /// Native execution scopes currently using `native`.
    pub(super) users: StateArc<crate::sync::SyncMutex<usize>>,
    /// One writable projection cannot isolate effects from concurrent commands.
    pub(super) execution: StateArc<AsyncMutex<()>>,
}
pub(super) type Mounts = crate::workspace::MountTable<Option<Arc<MountedVolume>>>;

pub(super) type View = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;

/// Lazy view over a held checkout root, with persistent source identity.
pub struct LocalWorkspace {
    pub(super) path: PathBuf,
    pub(super) fresh: bool,
    pub(super) view: View,
    pub(super) mount_directory: PathBuf,
    pub(super) mounted: Option<
        Arc<
            acyclic_fs::native_mount::LazyMount<
                LocalAuthorityBackend,
                LocalObjectBackend,
                NativeDemandSource,
                LocalCoreStateStore,
            >,
        >,
    >,
    pub(super) filesystem: acyclic_fs::LocalFs,
    pub(super) store: LocalCoreStateStore,
    pub(super) identity: acyclic_fs::NativeRootIdentity,
    // Lock order: view state -> volume projection -> coordinator -> command-local
    // cache or native completion, then task-file index -> provider visibility -> provider state.
    // Callbacks never reacquire view state. All executor bindings share this
    // owner; the real cfg shim and its negative controls model it.
    pub(super) state: StateArc<AsyncMutex<super::checkout_tools::State>>,
    pub(super) mounts: Arc<Mounts>,
    pub(super) context_line: String,
    #[cfg(all(test, not(acyclic_harness_loom)))]
    pub(super) selection_reads: std::sync::atomic::AtomicUsize,
    #[cfg(all(test, not(acyclic_harness_loom)))]
    pub(super) mount_advances: std::sync::atomic::AtomicUsize,
}

pub(super) fn attach_operation(task: TaskId) -> acyclic_fs::OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.checkout-attach/v1\0");
    hash.update(&task.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(bytes)
}

impl LocalWorkspace {
    pub(super) async fn mounts_at(&self, path: &Path) -> Result<Arc<Mounts>> {
        let primary = self.mounts.named(None)?.1;
        let mut mounts = mounts::owner(
            path,
            &primary.view.volume,
            None,
            self.mounts
                .mounts()
                .filter(|(relative, _)| !relative.is_empty())
                .map(|(_, mount)| {
                    (
                        mount.view.name.clone(),
                        mount.view.volume.clone(),
                        mount.view.source.clone(),
                    )
                }),
        )?;
        #[cfg(target_os = "linux")]
        volumes::link(path, &mut mounts)?;
        #[cfg(not(target_os = "linux"))]
        volumes::attach(&self.filesystem, &self.store, path, &mut mounts).await?;
        Ok(Arc::new(mounts))
    }
    pub(super) async fn created_mounts(
        &self,
        project: &VolumeRef,
        context: &crate::runtime::ToolContext,
    ) -> Result<Arc<Mounts>> {
        let access = context
            .task()
            .scope()
            .created_access
            .as_ref()
            .ok_or_else(|| {
                Error::Unauthorized("child mounts require authenticated allocations".into())
            })?;
        let mut private = None;
        for binding in &access.content {
            if binding.volume.class() != VolumeClass::AgentPrivate {
                continue;
            }
            let granted = [VolumeOperation::Read, VolumeOperation::Write]
                .into_iter()
                .map(|operation| binding.volume.capability(operation, &mut None))
                .collect::<Result<Vec<_>>>()?
                .iter()
                .all(|grant| context.effective_scope().grants().contains(grant));
            if granted && private.replace(binding.volume.clone()).is_some() {
                return Err(Error::Unauthorized(
                    "child private mount is ambiguous".into(),
                ));
            }
        }
        // Bind auxiliary volumes outside the sparse projection. Linux's
        // elevated mount helper cannot traverse an owner-only FUSE parent.
        let root = self.mount_directory.join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&root).map_err(storage)?;
        let mut mounts = mounts::owner(
            &root,
            project,
            None,
            private.map(|volume| ("scratch".into(), volume, None)),
        )?;
        volumes::attach(&self.filesystem, &self.store, &root, &mut mounts).await?;
        Ok(Arc::new(mounts))
    }

    #[cfg(all(test, feature = "native-execution", unix))]
    pub(super) async fn current(
        &self,
    ) -> acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend> {
        let state = lock_owned(StateArc::clone(&self.state)).await;
        state
            .active
            .as_ref()
            .unwrap_or(&self.view)
            .workspace()
            .clone()
    }
    pub(super) fn context_line(&self) -> &str {
        &self.context_line
    }

    /// Binds commands to this workspace's existing serialized view owner.
    pub async fn executor(
        self: &Arc<Self>,
        project: &VolumeRef,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
        task_files: Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
    ) -> Result<Arc<dyn crate::tool::ToolExecutor>> {
        Ok(Arc::new(
            super::checkout_tools::CheckoutTools::new(Arc::clone(self), project, clock, task_files)
                .await?,
        ))
    }
    pub(super) async fn checkout_selection(
        &self,
    ) -> Result<(
        acyclic_fs::WorkspaceId,
        Option<checkout_tools::reconciliation::Pending>,
    )> {
        #[cfg(all(test, not(acyclic_harness_loom)))]
        self.selection_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = self.view.workspace().id();
        acyclic_fs::GitCompatRepository::new(root, self.store.clone())
            .checkout_selection()
            .await
            .map_err(storage)
    }

    pub(super) async fn selected_view(
        &self,
    ) -> Result<(
        Option<View>,
        Option<checkout_tools::reconciliation::Pending>,
    )> {
        let (selected, pending) = self.checkout_selection().await?;
        let root = self.view.workspace().id();
        if selected == root {
            return Ok((None, pending));
        }
        let selected = acyclic_fs::WorkspaceResolver::resolve(
            &(
                self.filesystem.clone(),
                acyclic_fs::WorkspaceGraph::new(self.store.clone()),
            ),
            selected,
        )
        .await
        .map_err(storage)?;
        self.view
            .open_related(
                selected,
                &mut acyclic_fs::WorkBudget::UNBOUNDED
                    .accounting(&acyclic_fs::CancellationToken::new()),
            )
            .await
            .map(|view| (Some(view), pending))
            .map_err(storage)
    }

    /// Source-backed filesystem view. No checkout scan occurs during open.
    #[must_use]
    pub(crate) const fn view(
        &self,
    ) -> &LazyWorkspace<
        LocalAuthorityBackend,
        LocalObjectBackend,
        NativeDemandSource,
        LocalCoreStateStore,
    > {
        &self.view
    }
}

pub(super) async fn open(
    filesystem: &acyclic_fs::Fs<LocalAuthorityBackend, LocalObjectBackend>,
    volume: &VolumeRef,
    scratch: &VolumeRef,
    path: &Path,
    directory: &Path,
    identity: Option<[u8; 16]>,
    operation: acyclic_fs::OperationId,
    existing: bool,
    native_mounts: bool,
    store: LocalCoreStateStore,
) -> Result<LocalWorkspace> {
    let path = std::fs::canonicalize(path).map_err(storage)?;
    #[cfg(target_os = "linux")]
    if native_mounts {
        volumes::recover_after_crash(&path)?;
    }
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let name = volume.storage_name(&mut None)?;
    let id = filesystem.workspace_id(&name).map_err(storage)?;
    let original = store
        .load_lazy_workspace(id, &mut accounting)
        .await
        .map_err(storage)?;
    let selected = if original.is_some() || existing {
        Some(
            filesystem
                .open_workspace(&name, &work, &cancellation)
                .await
                .map_err(storage)?,
        )
    } else {
        None
    };
    let config = selected.as_ref().map_or_else(
        || acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
        |workspace| workspace.volume_config(),
    );
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(&path),
        config.profile,
        config.limits,
        original.as_ref().map(|state| state.source),
        None,
        &work,
        &cancellation,
    )
    .await
    .map_err(storage)?;
    if identity != Some(source.root_identity().to_bytes()) {
        return Err(Error::Unauthorized(
            "local checkout root was replaced during open".into(),
        ));
    }
    let exclusions = if native_mounts {
        volumes::excluded(&path, config, volumes::SCRATCH_PATH)?
    } else {
        Vec::new()
    };
    let source = source
        .exclude(exclusions, &&work, &cancellation)
        .map_err(|error| Error::from(acyclic_fs::LazyWorkspaceError::from(error)))?;
    let source = Arc::new(source);
    let fresh = original.is_none();
    let view = match selected {
        Some(workspace) if original.is_some() => {
            LazyWorkspace::open(workspace, source, store.clone(), &mut accounting)
                .await
                .map_err(storage)?
        }
        Some(workspace) => LazyWorkspace::bind(workspace, source, store.clone(), &mut accounting)
            .await
            .map_err(storage)?,
        None => {
            LazyWorkspace::attach(
                filesystem,
                &name,
                source,
                store.clone(),
                acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
                operation,
                &mut accounting,
            )
            .await
            .map_err(storage)?
            .0
        }
    };
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(view.workspace())
        .await
        .map_err(storage)?;
    let mut mounts = mounts::owner(
        &path,
        volume,
        None,
        native_mounts.then(|| ("scratch".into(), scratch.clone(), None)),
    )?;
    if native_mounts {
        volumes::attach(filesystem, &store, &path, &mut mounts).await?;
    }
    let context_line = mounts.context_line();
    let mounts = Arc::new(mounts);
    let state = StateArc::new(AsyncMutex::new(super::checkout_tools::State::new(
        view.workspace().id(),
        Arc::clone(&mounts),
    )));
    Ok(LocalWorkspace {
        #[cfg(all(test, not(acyclic_harness_loom)))]
        selection_reads: std::sync::atomic::AtomicUsize::new(0),
        #[cfg(all(test, not(acyclic_harness_loom)))]
        mount_advances: std::sync::atomic::AtomicUsize::new(0),
        path,
        fresh,
        view,
        mount_directory: directory.join("task-views"),
        mounted: None,
        filesystem: filesystem.clone(),
        store,
        identity: acyclic_fs::NativeRootIdentity::from_bytes(
            identity.ok_or_else(|| Error::Invalid("checkout identity is absent".into()))?,
        ),
        state,
        context_line,
        mounts,
    })
}

// A configured SDK volume gets one persistent private native view. Bootstrap
// into a sibling and rename only the complete generation, before binding its
// native source. Subsequent commands and reopen reuse the ordinary lazy path.
pub(super) async fn open_volume(
    filesystem: &acyclic_fs::LocalFs,
    volume: &VolumeRef,
    scratch: &VolumeRef,
    directory: &Path,
    operation: acyclic_fs::OperationId,
    native_mounts: bool,
    store: LocalCoreStateStore,
) -> Result<LocalWorkspace> {
    let path = directory.join("project");
    if !path.try_exists().map_err(storage)? {
        let staging = bootstrap::staging(directory)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let name = volume.storage_name(&mut None)?;
        let workspace = filesystem
            .open_workspace(&name, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let generation = workspace
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        bootstrap::initialize(
            async {
                generation
                    .materialize(
                        ["/"].into_iter(),
                        &acyclic_fs::MaterializeOptions::native(&staging),
                        acyclic_fs::WorkBudget::UNBOUNDED,
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
            || acyclic_native_runtime::remove_child_directory(directory, &staging).map_err(storage),
        )
        .await?;
    }
    let identity = acyclic_fs::capture_root_identity(&path)
        .map_err(storage)?
        .to_bytes();
    open(
        filesystem,
        volume,
        scratch,
        &path,
        directory,
        Some(identity),
        operation,
        true,
        native_mounts,
        store,
    )
    .await
}
