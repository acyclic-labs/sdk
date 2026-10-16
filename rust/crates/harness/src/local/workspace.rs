use super::*;
use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    LazyWorkspace, LazyWorkspaceStore, LocalCoreStateStore, NativeMaterializationWork,
    demand::native::NativeDemandSource, kernel::KernelAccounting,
};

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
    view: View,
    pub(super) filesystem: acyclic_fs::LocalFs,
    pub(super) store: LocalCoreStateStore,
    pub(super) identity: acyclic_fs::NativeRootIdentity,
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
    pub(super) async fn selected_view(&self) -> Result<Option<View>> {
        let root = self.view.workspace().id();
        let selected = acyclic_fs::GitCompatRepository::new(root, self.store.clone())
            .selected_workspace()
            .await
            .map_err(storage)?;
        if selected == root {
            return Ok(None);
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
            .map(Some)
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
    path: &Path,
    directory: &Path,
    identity: Option<[u8; 16]>,
    operation: acyclic_fs::OperationId,
    existing: bool,
) -> Result<LocalWorkspace> {
    let path = std::fs::canonicalize(path).map_err(storage)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let name = volume.storage_name(&mut None)?;
    let id = filesystem.workspace_id(&name).map_err(storage)?;
    let store = LocalCoreStateStore::new(directory.join("workspace"));
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
    Ok(LocalWorkspace {
        path,
        fresh,
        view,
        filesystem: filesystem.clone(),
        store,
        identity: acyclic_fs::NativeRootIdentity::from_bytes(
            identity.ok_or_else(|| Error::Invalid("checkout identity is absent".into()))?,
        ),
    })
}

// A configured SDK volume gets one persistent private native view. Bootstrap
// into a sibling and rename only the complete generation, before binding its
// native source. Subsequent commands and reopen reuse the ordinary lazy path.
pub(super) async fn open_volume(
    filesystem: &acyclic_fs::LocalFs,
    volume: &VolumeRef,
    directory: &Path,
    operation: acyclic_fs::OperationId,
) -> Result<LocalWorkspace> {
    let path = directory.join("project");
    if !path.try_exists().map_err(storage)? {
        let staging = directory.join("project-initializing");
        if staging.try_exists().map_err(storage)? {
            crate::filesystem::NativeDirectory(directory)
                .remove(&staging)
                .map_err(Error::Storage)?;
        }
        owner::directory(&staging)?;
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
        generation
            .materialize(
                ["/"].into_iter(),
                &acyclic_fs::MaterializeOptions::native(&staging),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?;
        std::fs::rename(&staging, &path).map_err(storage)?;
    }
    let identity = acyclic_fs::capture_root_identity(&path)
        .map_err(storage)?
        .to_bytes();
    open(
        filesystem,
        volume,
        &path,
        directory,
        Some(identity),
        operation,
        true,
    )
    .await
}
