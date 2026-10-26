use super::*;
use crate::kernel::KernelAccounting;
use std::sync::Arc;

// Unit and integration ProjFS adapters use one authorized fixture source.
#[path = "../../../../tests/projfs_metadata_support.rs"]
mod shared;
pub(super) use shared::*;

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
        crate::VolumeInitialization {
            config: VolumeConfig::native(Lifecycle::Ephemeral),
            access_policy: None,
        },
        crate::OperationId::new(),
        &mut KernelAccounting::native(&work, work.cancellation()),
    )
    .await?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    let mount = lazy
        .mount(&destination, manual_mount_options(lazy.workspace()).await?)
        .await?;
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

pub(super) fn callback_runtime(
    source: Arc<MemorySource>,
    root: &std::path::Path,
    work: &crate::NativeMaterializationWork,
) -> Result<Runtime, Box<dyn std::error::Error>> {
    Ok(Runtime {
        _policy_lease: Box::new(()),
        source,
        work: Mutex::new(work.clone()),
        active_work: Mutex::new(Default::default()),
        original_work: Mutex::new(None),
        callback_guard: None,
        root: root.to_path_buf(),
        metadata_root: HostRoot::open(root, &mut &*work)?,
        staging: Mutex::new(None),
        creating_name: RwLock::new((MountPath::root(), None)),
        writable: true,
        metadata_baselines: Mutex::new(HashMap::new()),
        projection: Mutex::new(HashMap::new()),
        projector: Arc::new(Projector::new()),
        metadata_probes: Mutex::new(HashMap::new()),
        metadata_edits: FileLocks::default(),
        post_operation_failure: Arc::new(Mutex::new(PostOperationFailures::default())),
        callbacks: CallbackGate::start()?,
        _allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
    })
}
