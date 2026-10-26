use std::{path::Path, sync::Arc};

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::kernel::KernelAccounting;
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::{
    CancellationToken, LazyWorkspace, Lifecycle, LocalAuthorityBackend, LocalFs,
    LocalObjectBackend, MemoryLazyWorkspaceStore, NativeMaterializationWork, OperationId,
    VolumeConfig, WorkBudget,
};
pub(super) fn work(
    cancel: &CancellationToken,
) -> Result<NativeMaterializationWork, acyclic_fs::NativeTreeMaterializationError> {
    NativeMaterializationWork::new(WorkBudget::UNBOUNDED, cancel).map_err(|(error, _)| error)
}

pub(super) async fn open_source(
    path: &Path,
) -> Result<Arc<NativeDemandSource>, Box<dyn std::error::Error>> {
    Ok(Arc::new(
        NativeDemandSource::open(
            std::borrow::Cow::Borrowed(path),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value,
    ))
}

pub(super) async fn attach(
    fs: &LocalFs,
    name: &str,
    source: Arc<NativeDemandSource>,
) -> Result<
    LazyWorkspace<
        LocalAuthorityBackend,
        LocalObjectBackend,
        NativeDemandSource,
        MemoryLazyWorkspaceStore,
    >,
    Box<dyn std::error::Error>,
> {
    let cancel = CancellationToken::new();
    let work = work(&cancel)?;
    Ok(LazyWorkspace::attach(
        fs,
        name,
        source,
        MemoryLazyWorkspaceStore::default(),
        acyclic_fs::VolumeInitialization {
            config: VolumeConfig::portable(if fs.capabilities().durable {
                Lifecycle::Durable
            } else {
                Lifecycle::Ephemeral
            }),
            access_policy: None,
        },
        OperationId::new(),
        &mut KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancel,
        },
    )
    .await?
    .0)
}
