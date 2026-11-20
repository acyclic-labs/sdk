use super::*;
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    LazyWorkspace, LazyWorkspaceStore, LocalCoreStateStore, NativeMaterializationWork,
    demand::native::NativeDemandSource, kernel::KernelAccounting,
};

/// How a checkout's task views reach the host filesystem.
///
/// `Native` projects every task view, the home volume and bound volumes
/// through the platform's native provider (Linux FUSE, Windows ProjFS, macOS
/// NFS) and refuses to open without it. `Physical` needs no device, capability
/// or mount helper: the checkout and every task view are plain directories,
/// and their changes are captured from userspace at each tool boundary. See
/// `NATIVE_MOUNT_QUALIFICATION.md` for the guarantees that differ.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WorkspaceMounting {
    /// Native projection; opening refuses when the host lacks it.
    #[default]
    Native,
    /// Plain directories with userspace change capture.
    Physical,
}

impl WorkspaceMounting {
    /// `Native` when this host admits a native projection; otherwise
    /// `Physical`, with the probe's reason for the caller to report.
    #[cfg(not(target_arch = "wasm32"))]
    #[must_use]
    pub fn detect() -> (Self, Option<&'static str>) {
        match acyclic_fs::probe_native_mount().unavailable_reason {
            None => (Self::Native, None),
            Some(reason) => (Self::Physical, Some(reason)),
        }
    }
}

/// Auxiliary task bindings below the owner's external host state.
pub const HOST_VIEW_ROOT: &str = ".volumes/agents";
/// The one namespace below [`HOST_VIEW_ROOT`] holding every task binding of
/// a checkout's tree, one directory per bound project.
const BINDINGS: &str = ".bindings";
/// Stable discovery name in a host-owned binding directory.
const PROJECT_ALIAS: &str = ".project";
/// Physical task-view directories, below [`HOST_VIEW_ROOT`] so that no
/// checkout capture or Git facade ever reads them as authored content.
const PHYSICAL_VIEW_ROOT: &str = ".physical";

#[doc(hidden)]
pub mod bindings;
mod bootstrap;
#[cfg(all(test, not(loom)))]
mod source_tests;

mod children;
pub mod git;
#[doc(hidden)]
pub mod mounts;
pub mod owner;
pub mod publications;
pub use children::LocalVolumeResources;
pub mod projection;
#[doc(hidden)]
pub mod task_views;
/// Volume binding, retirement and mount-table ordering.
pub mod volumes;
pub use crate::sync::owned::{Arc as StateArc, Mutex as AsyncMutex};

/// One volume bound into a checkout, with the projection it is served from.
pub struct MountedVolume {
    pub workspace: crate::tools::git::OwnedView,
    pub binding_root: Arc<acyclic_fs::HeldRoot>,
    pub destination: PathBuf,
    pub options: Option<acyclic_fs::MountOptions>,
    pub task_view_admission: Arc<std::sync::OnceLock<Arc<acyclic_fs::HeldRoot>>>,
    pub native: StateArc<AsyncMutex<Option<projection::Native>>>,
    /// Host sessions currently using `native`.
    pub users: StateArc<projection::Users>,
    /// Keeps the shared native instance serving command views running, so a
    /// command's view is a namespace entry, never an OS mount and unmount.
    pub shared_views: tokio::sync::OnceCell<acyclic_fs::SharedViewInstance>,
    // Retires the exact index entry after this physical source's fields.
    pub _binding: Option<acyclic_fs::kernel::allocation::ErasedRetainedArc>,
}
/// The mount table of one checkout: every bound volume by its relative path.
pub type Mounts = acyclic_harness_core::workspace::MountTable<Option<Arc<MountedVolume>>>;

/// A lazily materialised view of the checkout's project tree.
pub type View = LazyWorkspace<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;

pub(crate) type TaskMount = acyclic_fs::native_mount::LazyMount<
    LocalAuthorityBackend,
    LocalObjectBackend,
    NativeDemandSource,
    LocalCoreStateStore,
>;

/// Lazy view over a held checkout root, with persistent source identity.
pub struct LocalWorkspace {
    /// Cancels every mount and process this checkout owns.
    pub shutdown: acyclic_fs::CancellationToken,
    pub(crate) native_mount_probe: acyclic_fs::NativeMountProbe,
    /// The checkout's host directory.
    pub path: PathBuf,
    pub(crate) fresh: bool,
    /// A new native checkout still owes its first authored source import.
    /// A materialized SDK volume already has an authored generation.
    pub import_source: bool,
    /// The checkout's original host root.
    pub source_root: acyclic_native_runtime::execution::ExecutionGrant,
    pub(crate) view: View,
    /// The directory this checkout's native mounts are created under.
    pub mount_directory: PathBuf,
    /// The host state directory shared with this checkout's children.
    pub host_state: PathBuf,
    /// Optional sandbox admission shared by the root and all descendants.
    pub task_view_admission: Arc<std::sync::OnceLock<Arc<acyclic_fs::HeldRoot>>>,
    /// The physical task mount, when this view has one.
    pub mounted: Option<Arc<TaskMount>>,
    /// How this owner's task views reach the host filesystem.
    pub mounting: WorkspaceMounting,
    /// The filesystem this checkout's volumes live in.
    pub filesystem: acyclic_fs::LocalFs,
    /// The core state store backing `filesystem`.
    pub store: LocalCoreStateStore,
    /// The authority native mounts are bound under, when they are bound.
    pub mount_authority: Option<crate::filesystem::NativeMountAuthority>,
    pub identity: acyclic_fs_native_contract::NativeRootIdentity,
    // Lock order: view state -> volume projection -> coordinator -> command-local
    // cache or native completion, then task-file index -> provider visibility -> provider state.
    // Callbacks never reacquire view state. All executor bindings share this
    // owner; the real cfg shim and its negative controls model it.
    /// The view state every executor binding of this owner shares.
    pub state: StateArc<AsyncMutex<super::checkout_tools::State>>,
    /// A handback may be queued that no command has published yet. Starts
    /// raised, so the first command also settles debt a restart left queued.
    pub publication_debt: std::sync::atomic::AtomicBool,
    /// The mount table this checkout's views are resolved through.
    pub mounts: Arc<Mounts>,
    /// Original auxiliary binding, retained while this view or its selections
    /// can still execute against its shared physical projections.
    pub binding: Option<Arc<Mounts>>,
    // Keyed custody outlives preparation, including views with no auxiliary mounts.
    pub binding_owner: Option<acyclic_fs::kernel::allocation::ErasedRetainedArc>,
    pub bindings: acyclic_fs::kernel::allocation::RetainedArc<bindings::Bindings>,
    pub(crate) context_line: String,
    /// Custody of this owner's tool process trees; absent in an embedded
    /// binding, whose state directory has no exclusive owner to fence it.
    pub processes: Option<Arc<acyclic_native_runtime::ProcessCustody>>,
    /// Diff baselines of the workspaces this owner selected, shared with the
    /// views it derives.
    #[cfg(all(test, not(loom)))]
    pub selection_reads: std::sync::atomic::AtomicUsize,
    #[cfg(all(test, not(loom)))]
    pub mount_advances: std::sync::atomic::AtomicUsize,
}

/// The durable operation identity of one task's attach.
pub fn attach_operation(task: TaskId) -> acyclic_fs_format::identity::OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.local.checkout-attach/v1\0");
    hash.update(&task.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs_format::identity::OperationId::from_bytes(bytes)
}

impl LocalWorkspace {
    /// The selected native cwd, independent of the whole-volume capture path.
    pub fn native_cwd(&self) -> Result<Option<&Path>> {
        Ok(self.mounts.native_root_for(self.mounts.named(None)?.1))
    }

    fn project_alias(&self) -> Option<PathBuf> {
        self.mounted.as_ref()?;
        Some(self.binding.as_ref()?.root.join(PROJECT_ALIAS))
    }

    /// The path a task's view is announced at, when it is the primary mount.
    pub fn announced_path(&self, entry: crate::agent::EntryId) -> Option<PathBuf> {
        let mount = self.mounts.by_entry(entry)?;
        let path = self.mounts.native_root_for(mount)?;
        if self
            .mounts
            .named(None)
            .is_ok_and(|(_, primary)| std::ptr::eq(primary, mount))
            && let Some(alias) = self.project_alias()
        {
            return Some(alias);
        }
        Some(path.to_path_buf())
    }

    pub async fn mount_options(
        &self,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<acyclic_fs::MountOptions> {
        let authority = self
            .mount_authority
            .as_ref()
            .ok_or(Error::FilesystemAccessConfiguration("native mount owner"))?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(
            acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let primary = self.mounts.named(None)?.1;
        authority
            .mount_options(
                workspace,
                Some(&primary.view.volume),
                &primary.view.root,
                !primary.mode.read_only(),
                &work,
            )
            .await
            .map(|options| options.publication(acyclic_fs::MountPublication::Manual))
    }
    /// The held root of this checkout's physical task mount.
    pub fn held_task_mount_root(&self) -> Result<acyclic_fs::HeldRoot> {
        acyclic_fs::hold_root(&self.mount_directory).map_err(storage)
    }

    pub(crate) async fn mounts_at(&self, path: &Path) -> Result<Arc<Mounts>> {
        let primary = self.mounts.named(None)?.1;
        let mut mounts = mounts::owner(
            path,
            (
                primary.view.entry,
                &primary.view.name,
                &primary.view.volume,
                &primary.view.root,
                mounts::mode(primary),
                None,
            ),
            self.mounts
                .mounts()
                .filter(|(relative, _)| !relative.is_empty())
                .map(|(_, mount)| {
                    (
                        mount.view.entry,
                        mount.view.name.clone(),
                        mount.view.volume.clone(),
                        mount.view.root.clone(),
                        mounts::mode(mount),
                        mount.view.source.clone(),
                    )
                }),
        )?;
        mounts.scratch_root = self.mounts.scratch_root.clone();
        if !primary.view.root.is_empty() {
            // Native mounts already project the subtree. Physical capture
            // retains the whole-volume root; only the process root is scoped.
            let exposed = if self.mounted.is_some() {
                path.to_path_buf()
            } else {
                path.join(&primary.view.root)
            };
            for (relative, mount) in mounts.mounts_mut() {
                if relative.is_empty() {
                    mount.native_root =
                        acyclic_harness_core::workspace::NativeRoot::Path(exposed.clone());
                }
            }
        }
        volumes::bind_roots(&mut mounts)?;
        Ok(Arc::new(mounts))
    }

    /// Bind exactly the selected agent's auxiliary volumes. Reuse an admitted
    /// projection only when it names that same volume.
    pub async fn bound_mounts(
        &self,
        bindings: &[crate::agent::WorkspaceEntry],
        authority: &crate::filesystem::NativeMountAuthority,
        binding: &acyclic_fs::kernel::allocation::RetainedArc<bindings::Binding>,
    ) -> Result<Arc<Mounts>> {
        let (primary, project) = mounts::primary(bindings)?;
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
            (
                primary.id,
                &primary.name,
                project,
                &primary.root,
                primary.mode,
                None,
            ),
            mounts::auxiliary(bindings).map(|(entry, volume)| {
                let source = self
                    .mounts
                    .mounts()
                    .find(|(_, mount)| {
                        &mount.view.volume == volume
                            && mounts::mode(mount) == entry.mode
                            && mount.view.root == entry.root
                    })
                    .and_then(|(_, mount)| mount.view.source.clone());
                (
                    entry.id,
                    entry.name.clone(),
                    volume.clone(),
                    entry.root.clone(),
                    entry.mode,
                    source,
                )
            }),
        )?;
        mounts.scratch_root = self.mounts.scratch_root.clone();
        volumes::attach(volumes::Attach {
            filesystem: &self.filesystem,
            store: &self.store,
            root: &root,
            mounts: &mut mounts,
            authority: Some(authority),
            admission: &self.task_view_admission,
            native,
            binding: Some(binding),
        })
        .await?;
        Ok(Arc::new(mounts))
    }

    /// The one-line summary of this checkout shown to the agent.
    pub fn context_line(&self) -> &str {
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
        clock: Arc<dyn acyclic_actors::local::Clock>,
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
    pub(crate) fn checkout_selection(
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

    /// The view this checkout currently has selected.
    pub async fn selected_view(
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
                &mut acyclic_fs_work::performance::WorkBudget::UNBOUNDED
                    .accounting(&acyclic_fs::CancellationToken::new()),
            )
            .await
            .map(|view| (Some(view), pending))
            .map_err(storage)
    }

    /// Source-backed filesystem view. No checkout scan occurs during open.
    #[must_use]
    pub const fn view(
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
pub struct OwnerState {
    /// The core state store the owner's views are opened against.
    pub store: LocalCoreStateStore,
    /// Custody of the owner's tool process trees, when it has an exclusive
    /// state directory to fence them with.
    pub processes: Option<Arc<acyclic_native_runtime::ProcessCustody>>,
}

/// What a checkout or volume view attaches as: the volume it serves, its
/// auxiliary volumes, its control directory and the operation that attaches it.
pub struct Attach<'a> {
    /// The authority the attach is signed under, when it is signed.
    pub authority: Option<(&'a AuthorityRoot, AgentId)>,
    /// The volume the view serves.
    pub volume: &'a VolumeRef,
    /// The auxiliary home volume.
    pub scratch: &'a VolumeRef,
    /// The shared volume, when the view has one.
    pub shared: Option<&'a VolumeRef>,
    /// The control directory the view is announced in.
    pub directory: &'a Path,
    /// The operation this attach is journaled under.
    pub operation: acyclic_fs_format::identity::OperationId,
    /// Whether the view's volumes are mounted natively.
    pub native_mounts: bool,
    /// How this owner's task views reach the host filesystem.
    pub mounting: WorkspaceMounting,
    /// The probe that decides whether native mounts are available.
    pub native_mount_probe: &'a acyclic_fs::NativeMountProbe,
    /// The entropy a new native checkout's source import is seeded from.
    pub source_entropy: acyclic_fs::demand::native::SourceEntropy,
}

impl Attach<'_> {
    fn auxiliary(&self) -> impl Iterator<Item = (&'static str, &VolumeRef)> {
        std::iter::once((crate::filesystem::HOME_VOLUME, self.scratch))
            .chain(self.shared.map(|volume| (super::SHARED_VOLUME, volume)))
    }
}

/// Opens the selected checkout, binding its volumes and views.
pub async fn open(
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
    // side effect work in it; additional native authority compares that held
    // witness before use, so a replacement
    // there can neither attach this state nor receive its scratch setup.
    let path = root.path().to_path_buf();
    let identity = root.identity();
    let source_root = acyclic_native_runtime::execution::ExecutionGrant::new(
        &path,
        acyclic_native_runtime::execution::ExecutionAccess::ReadOnly,
        [],
        acyclic_native_runtime::execution::ExecutionNetwork::Off,
    )
    .map_err(Error::process_io)?;
    let actual = acyclic_fs::file_identity(source_root.workspace_file()).map_err(storage)?;
    if actual != identity {
        return Err(
            acyclic_fs::NativeTreeMaterializationError::AllocationChanged {
                expected: identity,
                actual,
            }
            .into(),
        );
    }
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
            acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)
        .map_err(storage)?;
    let mut mounts = mounts::owner(
        &path,
        (
            crate::agent::WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::User,
                "project".into(),
                volume.clone(),
            )
            .id,
            "project",
            volume,
            "",
            crate::agent::WorkspaceMode::ReadWrite,
            None,
        ),
        attach.auxiliary().map(|(name, volume)| {
            (
                crate::agent::WorkspaceEntry::initial_volume(
                    if name == crate::filesystem::HOME_VOLUME {
                        crate::agent::WorkspaceRole::Home
                    } else {
                        crate::agent::WorkspaceRole::Shared
                    },
                    name.into(),
                    volume.clone(),
                )
                .id,
                name.into(),
                volume.clone(),
                String::new(),
                crate::agent::WorkspaceMode::ReadWrite,
                None,
            )
        }),
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
    volumes::attach(volumes::Attach {
        filesystem,
        store: &store,
        root: &binding_root,
        mounts: &mut mounts,
        authority: mount_authority.as_ref(),
        admission: &task_view_admission,
        native: native_mounts,
        binding: None,
    })
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
        source_root,
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
    let work = NativeMaterializationWork::new(
        acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    crate::filesystem::CheckoutPublication::validate_state(
        root.path(),
        root.identity(),
        store,
        &work,
    )?;
    let mut accounting = KernelAccounting::native(&work, &cancellation);
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
        original.as_ref().map(|state| state.source.clone()),
        &work,
        &cancellation,
        attach.source_entropy.clone(),
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

/// Every path below the held root is authored source data.
async fn held_source(
    root: acyclic_fs::HeldRoot,
    config: acyclic_fs::VolumeConfig,
    reference: Option<acyclic_fs::demand::SourceReference>,
    work: &NativeMaterializationWork,
    cancellation: &acyclic_fs::CancellationToken,
    source_entropy: acyclic_fs::demand::native::SourceEntropy,
) -> Result<Arc<NativeDemandSource>> {
    let identity = root.identity();
    let (path, _custody) = work.path(root.path(), None)?;
    let source = NativeDemandSource::open_held_with_entropy(
        root,
        config.profile,
        config.limits,
        reference,
        None,
        work,
        cancellation,
        source_entropy,
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
/// Opens one volume of the selected checkout by its reference.
pub async fn open_volume(
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
            .open_workspace(
                &name,
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        let generation = workspace
            .head(
                acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        bootstrap::initialize(
            async {
                generation
                    .materialize(
                        ["/"].into_iter(),
                        &materialization,
                        acyclic_fs_work::performance::WorkBudget::UNBOUNDED,
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
