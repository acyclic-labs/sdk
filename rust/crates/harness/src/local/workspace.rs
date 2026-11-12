use super::*;
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    LazyWorkspace, LazyWorkspaceStore, LocalCoreStateStore, NativeMaterializationWork,
    demand::native::NativeDemandSource, kernel::KernelAccounting,
};

/// Auxiliary task bindings below the owner's external host state.
pub const HOST_VIEW_ROOT: &str = ".volumes/agents";
/// The one namespace below [`HOST_VIEW_ROOT`] holding every task binding of
/// a checkout's tree, one directory per bound project.
const BINDINGS: &str = ".bindings";
/// Physical task-view directories, below [`HOST_VIEW_ROOT`] so that no
/// checkout capture or Git facade ever reads them as authored content.
const PHYSICAL_VIEW_ROOT: &str = ".physical";

mod bindings;
mod bootstrap;
#[cfg(test)]
#[cfg(not(loom))]
mod performance_tests;
#[cfg(all(test, not(loom)))]
mod source_tests;

mod children;
pub(in crate::local) mod git;
mod mounts;
pub(in crate::local) mod owner;
pub(in crate::local) mod publications;
pub(in crate::local) use children::LocalVolumeResources;
pub(super) mod projection;
mod task_views;
pub(super) mod volumes;
pub(super) use crate::sync::owned::{Arc as StateArc, Mutex as AsyncMutex};

pub(super) struct MountedVolume {
    pub(super) workspace: crate::tools::git::OwnedView,
    pub(super) binding_root: Arc<acyclic_fs::HeldRoot>,
    pub(super) destination: PathBuf,
    pub(super) options: Option<acyclic_fs::MountOptions>,
    pub(super) task_view_admission: Arc<std::sync::OnceLock<Arc<acyclic_fs::HeldRoot>>>,
    pub(super) native: StateArc<AsyncMutex<Option<projection::Native>>>,
    /// Host sessions currently using `native`.
    pub(super) users: StateArc<projection::Users>,
    /// Signs each native command's own view of this volume.
    pub(super) authority: Option<crate::filesystem::NativeMountAuthority>,
    // Retires the exact index entry after this physical source's fields.
    _binding: Option<acyclic_fs::kernel::allocation::RetainedArc<bindings::Binding>>,
}
pub(super) type Mounts = crate::workspace::MountTable<Option<Arc<MountedVolume>>>;

pub(super) type View = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;

pub(super) type TaskMount = acyclic_fs::native_mount::LazyMount<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;

/// Lazy view over a held checkout root, with persistent source identity.
pub struct LocalWorkspace {
    pub(super) shutdown: acyclic_fs::CancellationToken,
    pub(super) native_mount_probe: acyclic_fs::NativeMountProbe,
    pub(super) path: PathBuf,
    pub(super) fresh: bool,
    /// A new native checkout still owes its first authored source import.
    /// A materialized SDK volume already has an authored generation.
    pub(super) import_source: bool,
    pub(super) view: View,
    pub(super) mount_directory: PathBuf,
    pub(super) host_state: PathBuf,
    /// Optional sandbox admission shared by the root and all descendants.
    pub(super) task_view_admission: Arc<std::sync::OnceLock<Arc<acyclic_fs::HeldRoot>>>,
    pub(super) mounted: Option<Arc<TaskMount>>,
    /// How this owner's task views reach the host filesystem.
    pub(super) mounting: WorkspaceMounting,
    pub(super) filesystem: acyclic_fs::LocalFs,
    pub(super) store: LocalCoreStateStore,
    pub(super) mount_authority: Option<crate::filesystem::NativeMountAuthority>,
    pub(super) identity: acyclic_fs::NativeRootIdentity,
    // Lock order: view state -> volume projection -> coordinator -> command-local
    // cache or native completion, then task-file index -> provider visibility -> provider state.
    // Callbacks never reacquire view state. All executor bindings share this
    // owner; the real cfg shim and its negative controls model it.
    pub(super) state: StateArc<AsyncMutex<super::checkout_tools::State>>,
    /// A handback may be queued that no command has published yet. Starts
    /// raised, so the first command also settles debt a restart left queued.
    pub(super) publication_debt: std::sync::atomic::AtomicBool,
    pub(super) mounts: Arc<Mounts>,
    /// Original auxiliary binding, retained while this view or its selections
    /// can still execute against its shared physical projections.
    pub(super) binding: Option<Arc<Mounts>>,
    // Keyed custody outlives preparation, including views with no auxiliary mounts.
    binding_owner: Option<acyclic_fs::kernel::allocation::RetainedArc<bindings::Binding>>,
    bindings: acyclic_fs::kernel::allocation::RetainedArc<bindings::Bindings>,
    pub(super) context_line: String,
    /// Custody of this owner's tool process trees; absent in an embedded
    /// binding, whose state directory has no exclusive owner to fence it.
    pub(super) processes: Option<Arc<acyclic_native_runtime::ProcessCustody>>,
    /// Diff baselines of the workspaces this owner selected, shared with the
    /// views it derives.
    #[cfg(all(test, not(loom)))]
    pub(super) selection_reads: std::sync::atomic::AtomicUsize,
    #[cfg(all(test, not(loom)))]
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
    pub(super) async fn mount_options(
        &self,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<acyclic_fs::MountOptions> {
        let authority = self
            .mount_authority
            .as_ref()
            .ok_or(Error::FilesystemAccessConfiguration("native mount owner"))?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        authority
            .mount_options(
                workspace,
                Some(&self.mounts.named(None)?.1.view.volume),
                true,
                &work,
            )
            .await
            .map(|options| options.publication(acyclic_fs::MountPublication::Manual))
    }
    pub(in crate::local) fn held_task_mount_root(&self) -> Result<acyclic_fs::HeldRoot> {
        acyclic_fs::hold_root(&self.mount_directory).map_err(storage)
    }

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
        mounts.scratch_root = self.mounts.scratch_root.clone();
        volumes::bind_roots(&mut mounts)?;
        Ok(Arc::new(mounts))
    }

    /// Bind exactly the selected agent's auxiliary volumes. Reuse an admitted
    /// projection only when it names that same volume.
    pub(super) async fn bound_mounts(
        &self,
        project: &VolumeRef,
        bindings: &[crate::agent::WorkspaceEntry],
        authority: &crate::filesystem::NativeMountAuthority,
        binding: &acyclic_fs::kernel::allocation::RetainedArc<bindings::Binding>,
    ) -> Result<Arc<Mounts>> {
        // Bind auxiliary volumes outside the sparse projection. Linux's
        // elevated mount helper cannot traverse an owner-only FUSE parent.
        let directory = blake3::Hash::from_bytes(binding.key).to_hex();
        let root = self
            .held_task_mount_root()?
            .create_directories(
                &Path::new(BINDINGS).join(directory.as_str()),
                acyclic_fs::NativeDurability::Durable,
            )
            .map_err(storage)?;
        // Namespace bindings are independent of OS projection. Physical views
        // retain the same logical volumes without creating native mounts.
        let native = self.mounting == WorkspaceMounting::Native;
        let mut mounts = mounts::owner(
            root.path(),
            project,
            None,
            mounts::auxiliary(bindings).map(|(name, volume)| {
                let source = self
                    .mounts
                    .mounts()
                    .find(|(_, mount)| &mount.view.volume == volume)
                    .and_then(|(_, mount)| mount.view.source.clone());
                (name.clone(), volume.clone(), source)
            }),
        )?;
        mounts.scratch_root = self.mounts.scratch_root.clone();
        volumes::attach(
            &self.filesystem,
            &self.store,
            &root,
            &mut mounts,
            Some(authority),
            &self.task_view_admission,
            native,
            Some(binding),
        )
        .await?;
        Ok(Arc::new(mounts))
    }

    pub(super) fn context_line(&self) -> &str {
        &self.context_line
    }

    /// Binds commands to this workspace's existing serialized view owner.
    ///
    /// Its file publisher writes through the same checkout selection and
    /// command lock, so a tool's file lands where its commands run.
    /// `execution_grant` confines shell commands and lifecycle hooks. Supply it
    /// only after host authentication and approval; it does not authorize file
    /// publication, change workspace grants or make other tools read-only.
    /// A different selected workspace must fit the same grant or launch refuses.
    /// Its native policy is compiled once here on a shared worker; missing
    /// enforcement refuses construction. Launch still verifies the original
    /// roots and working directory before creating a child.
    pub async fn executor(
        self: &Arc<Self>,
        (project, files, maximum): (&VolumeRef, &Arc<super::LocalFilesystem>, Option<u64>),
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
        signed: &Scope,
        verifier: &crate::core::AuthorityVerifier,
        execution_grant: Option<acyclic_native_runtime::execution::ExecutionGrant>,
    ) -> Result<(
        Arc<dyn crate::tool::ToolExecutor>,
        Arc<dyn crate::conversation::ContentPublisher>,
    )> {
        let tools = Arc::new(
            super::checkout_tools::CheckoutTools::new(
                Arc::clone(self),
                project,
                clock,
                signed,
                verifier,
                execution_grant,
            )
            .await?,
        );
        tools.publish_baseline().await?;
        let publisher = super::checkout_tools::publisher::CheckoutPublisher::new(
            Arc::clone(&tools),
            Arc::clone(files),
            project,
            verifier,
            maximum,
        );
        Ok((tools, Arc::new(publisher)))
    }
    pub(super) fn checkout_selection(
        &self,
    ) -> impl std::future::Future<
        Output = Result<(
            acyclic_fs::WorkspaceId,
            Option<checkout_tools::reconciliation::Pending>,
        )>,
    > + Send
    + '_ {
        #[cfg(all(test, not(loom)))]
        let selection_reads = &self.selection_reads;
        let root = self.view.workspace().id();
        let repository = acyclic_fs::GitCompatRepository::new(root, self.store.clone());
        // Selection needs only the repository handle, not the workspace's
        // recursive binding graph, while its store read is suspended.
        async move {
            #[cfg(all(test, not(loom)))]
            selection_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            repository.checkout_selection().await.map_err(storage)
        }
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

/// The durable state an owner opens a workspace with: its core state
/// store and the custody of its tool process trees, absent in an embedded
/// binding, whose state directory has no exclusive owner to fence it.
pub(super) struct OwnerState {
    pub(super) store: LocalCoreStateStore,
    pub(super) processes: Option<Arc<acyclic_native_runtime::ProcessCustody>>,
}

/// What a checkout or volume view attaches as: the volume it serves, its
/// auxiliary volumes, its control directory and the operation that attaches it.
pub(super) struct Attach<'a> {
    pub(super) authority: Option<(&'a AuthorityRoot, AgentId)>,
    pub(super) volume: &'a VolumeRef,
    pub(super) scratch: &'a VolumeRef,
    pub(super) shared: Option<&'a VolumeRef>,
    pub(super) directory: &'a Path,
    pub(super) operation: acyclic_fs::OperationId,
    pub(super) native_mounts: bool,
    pub(super) mounting: WorkspaceMounting,
    pub(super) native_mount_probe: &'a acyclic_fs::NativeMountProbe,
}

impl Attach<'_> {
    fn auxiliary(&self) -> impl Iterator<Item = (&'static str, &VolumeRef)> {
        std::iter::once((crate::filesystem::HOME_VOLUME, self.scratch))
            .chain(self.shared.map(|volume| (super::SHARED_VOLUME, volume)))
    }
}

pub(super) async fn open(
    filesystem: &acyclic_fs::Fs<LocalAuthorityBackend, LocalObjectBackend>,
    attach: &Attach<'_>,
    root: acyclic_fs::HeldRoot,
    existing: bool,
    state: OwnerState,
) -> Result<LocalWorkspace> {
    let OwnerState {
        mut store,
        processes,
    } = state;
    let &Attach {
        volume,
        native_mounts,
        mounting,
        ..
    } = attach;
    // The held root is the root its holder validated; binding and every setup
    // side effect work in it and never reopen the path, so a replacement
    // there can neither attach this state nor receive its scratch setup.
    let path = root.path().to_path_buf();
    let identity = root.identity();
    let host = acyclic_fs::hold_root(attach.directory).map_err(storage)?;
    let binding_root = volumes::held_directories(&host, Path::new("bindings/root"))?;
    let mount_directory = volumes::held_directories(&host, Path::new(HOST_VIEW_ROOT))?
        .path()
        .to_path_buf();
    #[cfg(target_os = "linux")]
    if native_mounts {
        for (_, volume) in attach.auxiliary() {
            let binding = blake3::hash(volume.storage_name(&mut None)?.as_bytes())
                .to_hex()
                .to_string();
            volumes::recover_after_crash(&binding_root, Path::new(&binding))?;
        }
    }
    let (view, fresh, durability) =
        lazy_view(filesystem, attach, root, existing, &mut store).await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    acyclic_fs::WorkspaceGraph::new(store.clone())
        .register_root(
            view.workspace(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)
        .map_err(storage)?;
    let mut mounts = mounts::owner(
        &path,
        volume,
        None,
        attach
            .auxiliary()
            .map(|(name, volume)| (name.into(), volume.clone(), None)),
    )?;
    mounts.scratch_root = Some(
        volumes::held_directories(&host, Path::new("scratch"))?
            .path()
            .to_path_buf(),
    );
    let task_view_admission = Arc::new(std::sync::OnceLock::new());
    let mount_authority = attach
        .authority
        .map(|(issuer, agent)| crate::filesystem::NativeMountAuthority::new(issuer, agent))
        .transpose()?;
    volumes::attach(
        filesystem,
        &store,
        &binding_root,
        &mut mounts,
        mount_authority.as_ref(),
        &task_view_admission,
        native_mounts,
        None,
    )
    .await?;
    // No view escapes setup before its binding and all root registrations are
    // durable. Native crash cleanup above only retires old projections; it
    // neither consumes this new lineage nor publishes new workspace effects.
    if let Some(durability) = durability {
        durability.commit().await.map_err(storage)?;
    }
    let context_line = mounts.context_line();
    let mounts = Arc::new(mounts);
    let state = StateArc::new(AsyncMutex::new(super::checkout_tools::State::new(
        view.workspace().id(),
    )));
    Ok(LocalWorkspace {
        shutdown: acyclic_fs::CancellationToken::new(),
        native_mount_probe: attach.native_mount_probe.clone(),
        #[cfg(all(test, not(loom)))]
        selection_reads: std::sync::atomic::AtomicUsize::new(0),
        #[cfg(all(test, not(loom)))]
        mount_advances: std::sync::atomic::AtomicUsize::new(0),
        path,
        fresh,
        import_source: fresh && !existing,
        view,
        mount_directory,
        host_state: attach.directory.to_path_buf(),
        task_view_admission,
        mounted: None,
        mounting,
        filesystem: filesystem.clone(),
        store,
        mount_authority,
        identity,
        state,
        publication_debt: std::sync::atomic::AtomicBool::new(true),
        context_line,
        mounts,
        binding: None,
        binding_owner: None,
        bindings: bindings::Bindings::new(&cancellation)?,
        processes,
    })
}

/// The checkout's lazy view: reopened from its retained state, bound to an
/// existing volume, or attached fresh. Returns whether it is fresh.
async fn lazy_view(
    filesystem: &acyclic_fs::LocalFs,
    attach: &Attach<'_>,
    root: acyclic_fs::HeldRoot,
    existing: bool,
    store: &mut LocalCoreStateStore,
) -> Result<(View, bool, Option<acyclic_fs::DeferredDurability>)> {
    let &Attach {
        volume, operation, ..
    } = attach;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    crate::filesystem::CheckoutPublication::validate_state(
        root.path(),
        root.identity(),
        store,
        &work,
    )?;
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
    let source = held_source(
        root,
        config,
        original.as_ref().map(|state| state.source),
        &work,
        &cancellation,
    )
    .await?;
    let fresh = original.is_none();
    // A new binding has no pending native effects to recover. Its metadata
    // shares one durability boundary with the root registrations in `open`.
    // Existing bindings retain ordinary durability during pending-remove recovery.
    let durability = fresh.then(|| {
        let (deferred, durability) = store.defer_durability();
        *store = deferred;
        durability
    });
    let view = match selected {
        Some(workspace) if !fresh => {
            LazyWorkspace::open(workspace, source, store.clone(), &mut accounting).await
        }
        Some(workspace) => {
            LazyWorkspace::bind(workspace, source, store.clone(), &mut accounting).await
        }
        None => LazyWorkspace::attach(
            filesystem,
            &name,
            source,
            store.clone(),
            acyclic_fs::VolumeInitialization::new(
                acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
                Some(acyclic_fs::access::INITIAL_POLICY),
            ),
            operation,
            &mut accounting,
        )
        .await
        .map(|(view, ..)| view),
    }
    .map_err(storage)?;
    Ok((view, fresh, durability))
}

/// Every authored path is source data. Recovery lives in external host state.
async fn held_source(
    root: acyclic_fs::HeldRoot,
    config: acyclic_fs::VolumeConfig,
    reference: Option<acyclic_fs::demand::SourceReference>,
    work: &NativeMaterializationWork,
    cancellation: &acyclic_fs::CancellationToken,
) -> Result<Arc<NativeDemandSource>> {
    let identity = root.identity();
    let (path, _custody) = work.path(root.path(), None)?;
    let source = NativeDemandSource::open_held(
        root,
        config.profile,
        config.limits,
        reference,
        None,
        work,
        cancellation,
    )
    .await
    .map_err(storage)?;
    let actual = work.root_identity(&path)?;
    if actual != identity {
        return Err(
            acyclic_fs::NativeTreeMaterializationError::AllocationChanged {
                expected: identity,
                actual,
            }
            .into(),
        );
    }
    Ok(Arc::new(source))
}

// A configured SDK volume gets one persistent private native view. Bootstrap
// into a sibling and rename only the complete generation, before binding its
// native source. Subsequent commands and reopen reuse the ordinary lazy path.
pub(super) async fn open_volume(
    filesystem: &acyclic_fs::LocalFs,
    attach: &Attach<'_>,
    state: OwnerState,
) -> Result<LocalWorkspace> {
    let &Attach {
        volume, directory, ..
    } = attach;
    let path = directory.join("project");
    if !path.try_exists().map_err(storage)? {
        let staging = bootstrap::staging(directory)?;
        let mut materialization = acyclic_fs::MaterializeOptions::native(&staging);
        materialization.materialized_view =
            Some(acyclic_fs::NativeViewBaseline::recording(&state.store));
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
                        &materialization,
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
    let root =
        acyclic_fs::hold_root(&std::fs::canonicalize(&path).map_err(storage)?).map_err(storage)?;
    open(filesystem, attach, root, true, state).await
}
