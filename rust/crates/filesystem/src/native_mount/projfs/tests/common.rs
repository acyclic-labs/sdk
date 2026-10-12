use super::{
    CallbackGate, PostOperationFailures, flush_callback_gate, record_post_operation_failure,
    reject_stale_projection, source_hresult,
};
use crate::kernel::{FileMetadata, KernelAccounting};
use crate::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    VolumeConfig,
};
use crate::native_mount::adapter::{CheckoutMountSource, SharedCheckout};
use crate::native_mount::{MountFilesystem, MountPath, MountSourceError};
use crate::{
    CancellationToken, Fs, IdempotencyKey, LocalOptions, MountOptions, MountPublication, WorkBudget,
};
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::{Arc, Barrier, Mutex};
use windows::Win32::Storage::ProjectedFileSystem::{
    PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED, PRJ_NOTIFICATION_PRE_DELETE,
    PrjMarkDirectoryAsPlaceholder,
};
use windows::core::{GUID, HSTRING, PCWSTR};

pub(super) type MemorySource =
    CheckoutMountSource<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;

pub(super) fn windows_name(name: &str) -> Vec<u8> {
    name.encode_utf16().flat_map(u16::to_le_bytes).collect()
}

pub(super) fn windows_path(name: &str) -> MountPath {
    MountPath::root().child(windows_name(name))
}

pub(super) async fn windows_checkout_source()
-> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = crate::model::FilesystemProfile::Windows;
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = fs
        .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    Ok(Arc::new(CheckoutMountSource::new(
        Arc::new(SharedCheckout::new(checkout)),
        config,
    )?))
}

pub(super) fn mount_source(
    source: &Arc<MemorySource>,
) -> Result<
    (
        tempfile::TempDir,
        std::path::PathBuf,
        crate::NativeMountSession,
    ),
    Box<dyn std::error::Error>,
> {
    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    let session = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: destination.clone(),
            writable: true,
        },
        Arc::clone(source) as Arc<dyn MountFilesystem>,
    )?;
    Ok((root, destination, session))
}

pub(super) fn sorted_names(directory: &std::path::Path) -> std::io::Result<Vec<String>> {
    let mut names = std::fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

pub(super) fn powershell(script: &str) -> std::io::Result<acyclic_native_runtime::ProcessTree> {
    acyclic_native_runtime::ProcessTree::spawn(std::process::Command::new("powershell.exe").args([
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        script,
    ]))
}

pub(super) fn succeeded(
    mut child: acyclic_native_runtime::ProcessTree,
) -> Result<(), Box<dyn std::error::Error>> {
    let result = child.wait(std::time::Duration::from_secs(120));
    child.terminate(None)?;
    let status = result?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("external process failed: {status}").into())
    }
}

pub(super) async fn writes_only_promised_content(
    listed_first: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    use std::io::Write as _;

    let root = tempfile::tempdir()?;
    let source_root = root.path().join("source");
    std::fs::create_dir(&source_root)?;
    std::fs::write(source_root.join("changed.txt"), b"before")?;
    std::fs::write(source_root.join("stable.txt"), b"stable")?;
    let demand = windows_demand_source(&source_root, work.cancellation()).await?;
    let (lazy, _) = crate::LazyWorkspace::attach(
        &Fs::memory(),
        "pinned-projfs",
        Arc::new(demand),
        crate::MemoryLazyWorkspaceStore::default(),
        VolumeConfig::native(Lifecycle::Ephemeral),
        crate::OperationId::new(),
        &mut KernelAccounting::native(&work, work.cancellation()),
    )
    .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = lazy.mount(&destination, manual_mount_options()).await?;
    if listed_first {
        assert_eq!(std::fs::read_dir(&destination)?.count(), 2);
    }
    assert_eq!(std::fs::metadata(destination.join("changed.txt"))?.len(), 6);
    assert_eq!(std::fs::metadata(destination.join("stable.txt"))?.len(), 6);

    let mut changed = std::fs::OpenOptions::new()
        .write(true)
        .open(source_root.join("changed.txt"))?;
    changed.write_all(b"after!")?;
    changed.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1 << 30))?;
    drop(changed);

    let read = std::fs::read(destination.join("changed.txt"))?;
    assert!(
        read == b"before" || read == b"after!",
        "a file holds content no listing named: {read:?}"
    );
    assert_eq!(std::fs::read(destination.join("stable.txt"))?, b"stable");
    mount.revalidate()?;
    assert_eq!(std::fs::read(destination.join("changed.txt"))?, b"after!");
    mount.unmount().await?;
    Ok(())
}

pub(super) fn setup_work() -> Result<crate::NativeMaterializationWork, Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    crate::initialize_native_mount_services()?;
    Ok(
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(cause, _)| cause)?,
    )
}

pub(super) fn setup_chunk(
    bytes: &[u8],
    work: &crate::NativeMaterializationWork,
) -> Result<crate::kernel::OwnedSourceChunk, Box<dyn std::error::Error>> {
    let (bytes, allocation) = work.copy_bytes(bytes)?;
    let bytes =
        acyclic_stream::OwnedBytes::from_vec_with_guard(bytes, allocation, work.runtime_meter())
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    work.runtime_error(error)
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    crate::NativeTreeMaterializationError::InvalidLayout
                }
            })?;
    Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, 0))
}

pub(super) async fn windows_demand_source(
    root: &std::path::Path,
    cancellation: &CancellationToken,
) -> Result<crate::demand::native::NativeDemandSource, crate::DemandError> {
    crate::demand::native::NativeDemandSource::open(
        std::borrow::Cow::Owned(root.to_path_buf()),
        crate::model::FilesystemProfile::Windows,
        crate::model::VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        cancellation,
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)
}

pub(super) async fn local_workspace(
    root: &std::path::Path,
    name: &str,
    work: &crate::NativeMaterializationWork,
) -> Result<
    crate::Workspace<crate::facade::LocalAuthorityBackend, crate::facade::LocalObjectBackend>,
    Box<dyn std::error::Error>,
> {
    Ok(Fs::local(LocalOptions::new(root.join("state")))
        .await?
        .create_workspace(name, WorkBudget::UNBOUNDED, work.cancellation())
        .await
        .map_err(|failure| failure.error)?
        .value)
}

pub(super) fn manual_mount_options() -> MountOptions {
    MountOptions::read_write().publication(MountPublication::Manual)
}
