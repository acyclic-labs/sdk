use super::*;

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
impl ViewObserver for Recorded {
    fn view_changed(&self, position: ViewStamp, _origin: crate::native_mount::ViewOrigin) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(position);
    }
}
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub(super) async fn recorded_view(
    source_root: &Path,
) -> Result<
    (
        Arc<crate::demand::native::NativeDemandSource>,
        LazyMountSource<
            crate::facade::MemoryAuthorityBackend,
            crate::facade::MemoryObjectBackend,
            crate::demand::native::NativeDemandSource,
            crate::MemoryLazyWorkspaceStore,
        >,
        Arc<Recorded>,
    ),
    Box<dyn std::error::Error>,
> {
    std::fs::write(source_root.join("source-only"), b"source")?;
    let demand =
        Arc::new(native_source(source_root, crate::model::FilesystemProfile::Portable).await?);
    let view = mounted_view_of(
        Arc::clone(&demand),
        "recorded",
        crate::MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let recorded = Arc::new(Recorded::default());
    let observer: Weak<Recorded> = Arc::downgrade(&recorded);
    view.observe_view(observer);
    Ok((demand, view, recorded))
}
pub(super) async fn mounted_view(
    source: &Path,
    name: &str,
) -> Result<
    LazyMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
        crate::demand::native::NativeDemandSource,
        crate::MemoryLazyWorkspaceStore,
    >,
    Box<dyn std::error::Error>,
> {
    mounted_view_of(
        Arc::new(native_source(source, crate::model::FilesystemProfile::Portable).await?),
        name,
        crate::MemoryLazyWorkspaceStore::default(),
    )
    .await
}

pub(super) async fn native_source(
    source: &Path,
    profile: crate::model::FilesystemProfile,
) -> Result<crate::demand::native::NativeDemandSource, Box<dyn std::error::Error>> {
    Ok(crate::demand::native::NativeDemandSource::open(
        std::borrow::Cow::Borrowed(source),
        profile,
        crate::model::VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .value)
}
async fn mounted_view_of<D: DemandSource + 'static, S: LazyWorkspaceStore>(
    demand: Arc<D>,
    name: &str,
    store: S,
) -> Result<
    LazyMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
        D,
        S,
    >,
    Box<dyn std::error::Error>,
> {
    use crate::Fs;
    use crate::model::{CheckoutMode, GenerationSelector};
    use crate::native_mount::{MountPublication, SharedCheckout};

    let lazy = Arc::new(
        LazyWorkspace::attach(
            &Fs::memory(),
            name,
            demand,
            store,
            crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            crate::OperationId::new(),
            &mut crate::kernel::KernelAccounting::standalone(
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            ),
        )
        .await?
        .0,
    );
    let checkout = lazy
        .workspace()
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let config = checkout.volume_config();
    let authored = Arc::new(CheckoutMountSource::new(
        Arc::new(SharedCheckout::with_publication(
            checkout,
            MountPublication::Manual,
        )),
        config,
    )?);
    let cancellation = CancellationToken::new();
    CallbackRuntime::create()?;
    let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
    Ok(LazyMountSource::new(
        lazy,
        authored,
        "/".to_owned(),
        adapter::NativeDemandCarrier::Borrowed(&mut work),
        None,
    )?)
}

pub(super) fn mounted(names: &[&str]) -> MountPath {
    names.iter().fold(MountPath::root(), |path, name| {
        path.child(if cfg!(windows) {
            name.encode_utf16().flat_map(u16::to_le_bytes).collect()
        } else {
            name.as_bytes().to_vec()
        })
    })
}

pub(super) async fn live_lazy_mount_of(
    demand: crate::demand::native::NativeDemandSource,
    destination: &Path,
    name: &str,
) -> Result<
    crate::native_mount::LazyMount<
        impl AsyncAuthorityStore + Send + Sync + 'static,
        impl AsyncObjectStore + Send + Sync + 'static,
        crate::demand::native::NativeDemandSource,
        crate::MemoryLazyWorkspaceStore,
    >,
    Box<dyn std::error::Error>,
> {
    use crate::model::{Lifecycle, VolumeConfig};
    use crate::native_mount::{MountOptions, MountPublication};
    use crate::{Fs, MemoryLazyWorkspaceStore};

    let lazy = LazyWorkspace::attach(
        &Fs::memory(),
        name,
        Arc::new(demand),
        MemoryLazyWorkspaceStore::default(),
        VolumeConfig::native(Lifecycle::Ephemeral),
        crate::OperationId::new(),
        &mut crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ),
    )
    .await?
    .0;
    std::fs::create_dir_all(destination)?;
    Ok(lazy
        .mount(
            destination,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?)
}

pub(super) fn sorted_names(directory: &Path) -> std::io::Result<Vec<std::ffi::OsString>> {
    let mut names = std::fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(names)
}

pub(super) fn is_absent(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

#[cfg(target_os = "linux")]
async fn keeps_a_reused_identity_apart(watched: bool) -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

    let root = tempfile::tempdir()?;
    let (source, mount) = (root.path().join("source"), root.path().join("mount"));
    std::fs::create_dir_all(&source)?;
    std::fs::write(source.join("removed"), b"old")?;
    let demand = native_source(&source, crate::model::FilesystemProfile::Posix).await?;
    let demand = if watched { demand } else { demand.unwatched() };
    let live = live_lazy_mount_of(demand, &mount, "live-reused-identity").await?;
    assert_eq!(std::fs::metadata(mount.join("removed"))?.len(), 3);
    // Holds the removed name's inode without a file handle.
    let held = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH)
        .open(mount.join("removed"))?;
    let identity = std::fs::metadata(source.join("removed"))?.ino();
    std::fs::remove_file(source.join("removed"))?;
    let mut reused = false;
    for index in 0..256 {
        let candidate = source.join(format!("created-{index}"));
        std::fs::write(&candidate, b"new file")?;
        if std::fs::metadata(&candidate)?.ino() == identity {
            std::fs::rename(&candidate, source.join("created"))?;
            reused = true;
            break;
        }
        std::fs::remove_file(&candidate)?;
    }
    if !reused {
        eprintln!("the host did not reuse the identity ({watched:?}); nothing to check");
        live.unmount().await?;
        return Ok(());
    }
    live.revalidate()?;
    let created = std::fs::metadata(mount.join("created"))?;
    assert_eq!(created.len(), 8);
    let mode = std::fs::metadata(source.join("created"))?
        .permissions()
        .mode();
    // A mode change through the held inode must not reach the new file.
    let _ = std::fs::set_permissions(
        format!("/proc/self/fd/{}", std::os::fd::AsRawFd::as_raw_fd(&held)),
        std::fs::Permissions::from_mode(0o600),
    );
    assert_eq!(
        std::fs::metadata(source.join("created"))?
            .permissions()
            .mode(),
        mode
    );
    assert_eq!(
        std::fs::metadata(mount.join("created"))?
            .permissions()
            .mode()
            & 0o777,
        mode & 0o777
    );
    drop(held);
    live.unmount().await?;
    Ok(())
}

#[cfg(unix)]
async fn keeps_hard_links_one_file(watched: bool) -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt as _;

    let root = tempfile::tempdir()?;
    let (source, mount) = (root.path().join("source"), root.path().join("mount"));
    let pairs = [("a", "d/a"), ("b", "d/b"), ("c", "l/c"), ("s/x", "s/y")];
    for directory in ["d", "l", "s"] {
        std::fs::create_dir_all(source.join(directory))?;
    }
    for (first, second) in pairs {
        std::fs::write(source.join(first), first.as_bytes())?;
        std::fs::hard_link(source.join(first), source.join(second))?;
    }
    let demand = native_source(&source, crate::model::FilesystemProfile::Posix).await?;
    let demand = if watched { demand } else { demand.unwatched() };
    let live = live_lazy_mount_of(demand, &mount, "live-hard-links").await?;
    let same_file = |first: &str, second: &str| -> Result<(), Box<dyn std::error::Error>> {
        // Keep the inode live while comparing names: an unwatched mount
        // may receive FORGET between two otherwise independent stat calls.
        let _held = std::fs::File::open(mount.join(first))?;
        let (one, other) = (
            std::fs::metadata(mount.join(first))?,
            std::fs::metadata(mount.join(second))?,
        );
        assert_eq!(
            one.ino(),
            other.ino(),
            "{first} and {second} are one inode ({watched:?})"
        );
        assert_eq!(
            (one.nlink(), other.nlink()),
            (2, 2),
            "{first} and {second} ({watched:?})"
        );
        std::fs::write(mount.join(second), format!("through {second}"))?;
        assert_eq!(
            std::fs::read_to_string(mount.join(first))?,
            format!("through {second}")
        );
        std::fs::write(mount.join(first), format!("through {first}"))?;
        assert_eq!(
            std::fs::read_to_string(mount.join(second))?,
            format!("through {first}")
        );
        Ok(())
    };
    // One listing names both.
    assert_eq!(sorted_names(&mount.join("s"))?, ["x", "y"]);
    same_file("s/x", "s/y")?;
    // A listing names the inner one first.
    let listed = std::fs::read_dir(mount.join("l"))?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(listed, ["c"]);
    same_file("c", "l/c")?;
    // The outer name first, then the inner one.
    same_file("a", "d/a")?;
    // The inner name first, then the outer one.
    same_file("d/b", "b")?;
    // And every name still agrees after the source is revalidated.
    live.revalidate()?;
    for (first, second) in pairs {
        let _held = std::fs::File::open(mount.join(first))?;
        assert_eq!(
            std::fs::metadata(mount.join(first))?.ino(),
            std::fs::metadata(mount.join(second))?.ino(),
            "{first} and {second} after revalidation ({watched:?})"
        );
    }
    live.unmount().await?;
    Ok(())
}
