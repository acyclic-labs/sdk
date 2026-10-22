use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, kernel::NamespacePath};
use acyclic_native_runtime::spawn;

pub(super) const SCRATCH_PATH: &str = ".volumes/scratch";

mod exclude;
pub(super) mod parents;
#[cfg(all(test, feature = "native-execution", not(acyclic_harness_loom)))]
mod publication_tests;
#[cfg(all(test, target_os = "linux", not(acyclic_harness_loom)))]
mod recovery_tests;

#[cfg(target_os = "linux")]
pub(super) fn recover_after_crash(root: &Path) -> Result<()> {
    let relative = Path::new(SCRATCH_PATH);
    let parent = relative
        .parent()
        .ok_or_else(|| Error::Invalid("scratch mount has no parent".into()))?;
    create_mount_directory(root, parent)?;
    acyclic_fs::recover_disconnected_native_mount_destination(&root.join(relative))?;
    Ok(())
}

pub(in crate::local) fn require_available(
    capabilities: acyclic_fs::NativeMountCapabilities,
) -> Result<()> {
    if capabilities.available() {
        return Ok(());
    }
    use acyclic_fs::NativeMountKind;
    Err(Error::MountUnavailable {
        device: match capabilities.kind {
            Some(NativeMountKind::LinuxFuse) => "/dev/fuse",
            Some(NativeMountKind::WindowsProjFs) => "ProjectedFSLib.dll",
            Some(NativeMountKind::MacOsNfs) => "/sbin/mount_nfs",
            None => "native mount provider",
        },
    })
}

/// Root-relative paths a bound volume owns inside a task view: its alias or
/// mount destination and the mount control files beside it. Capture excludes
/// them and the Git facade reserves them; neither is ever authored content.
pub(in crate::local) fn owned(root: &Path, relative: &str) -> Result<Vec<String>> {
    let destination = root.join(relative);
    std::iter::once(destination.clone())
        .chain(acyclic_fs::native_mount_control_paths(&destination).map_err(Error::from)?)
        .filter(|path| path.starts_with(root))
        .map(|path| {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| Error::Invalid("mount metadata lies outside the task view".into()))?;
            let relative = relative
                .to_str()
                .ok_or_else(|| Error::Invalid("mount path is not UTF-8".into()))?;
            Ok(relative.replace('\\', "/"))
        })
        .collect()
}

pub(in crate::local) fn excluded(
    root: &Path,
    config: acyclic_fs::VolumeConfig,
    relative: &str,
) -> Result<Vec<NamespacePath>> {
    owned(root, relative)?
        .into_iter()
        .map(|relative| {
            let path =
                acyclic_fs::path::PortablePath::parse(&format!("/{relative}"), config.limits)
                    .map_err(storage)?;
            NamespacePath::from_portable_in_profile(&path, config.profile, config.limits)
                .map_err(storage)
        })
        .collect()
}

pub(super) async fn attach(
    filesystem: &acyclic_fs::LocalFs,
    store: &acyclic_fs::LocalCoreStateStore,
    root: &Path,
    mounts: &mut Mounts,
) -> Result<()> {
    let cancellation = CancellationToken::new();
    for (relative, mount) in mounts.mounts_mut() {
        if relative.is_empty() {
            continue;
        }
        let source = filesystem
            .open_workspace(
                &mount.view.volume.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| Error::from(failure.error))?
            .value;
        let destination = root.join(relative);
        // Bind the namespace before the initial source/watch cut. Creating its
        // parent during the first shell command would author a project change
        // even when that command only reads. The projection itself stays absent.
        if let Some(parent) = Path::new(relative).parent() {
            create_mount_directory(root, parent)?;
        }
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(&source)
            .await?;
        mount.view.source = Some(Arc::new(MountedVolume {
            workspace: crate::tools::git::OwnedView::Volume(source),
            destination,
            native: StateArc::new(AsyncMutex::new(None)),
            users: StateArc::new(crate::sync::SyncMutex::new(0)),
            execution: StateArc::new(AsyncMutex::new(())),
        }));
    }
    Ok(())
}

pub(in crate::local) async fn sync(mounts: &Mounts) -> Result<()> {
    for (_, mount) in ordered_mounts(mounts)? {
        if let Some(source) = &mount.view.source {
            // This call precedes the command scope. Share its execution guard
            // so preflight cannot publish another worker's unverified effects.
            let _execution =
                crate::sync::owned::lock_owned(StateArc::clone(&source.execution)).await;
            source.prepare_command().await?;
            let native = source.native().await;
            if let Some(native) = native.as_ref() {
                native.sync().await?;
                native.advance_to_head().await?;
            }
        }
    }
    Ok(())
}

type BoundMount = crate::workspace::Mount<Option<Arc<MountedVolume>>>;

fn ordered_mounts(mounts: &Mounts) -> Result<Vec<(&str, &BoundMount)>> {
    let mut ordered = std::collections::BTreeMap::new();
    for (relative, mount) in mounts.mounts() {
        if let Some(source) = &mount.view.source {
            // The binding is validated even when another name already selects
            // this projection. Lock order belongs to the volume and physical
            // projection, never to aliases in a particular task's mount table.
            projection_root(&source.destination, relative)?;
            let key = (
                source.workspace.workspace().id(),
                source.destination.clone(),
            );
            ordered.entry(key).or_insert((relative, mount));
        }
    }
    Ok(ordered.into_values().collect())
}

/// The single native execution scope shared by the shell and hooks. Entry projects
/// every bound volume, so a command sees persisted volume data. A successful
/// exit publishes and detaches each projection and removes its SDK-owned
/// residue, so the checkout holds no mount between executions. The body owns
/// every check that must precede publication.
struct Claim {
    source: Arc<MountedVolume>,
    entered: projection::Entered,
    // Released only after publication or abandonment finishes, including when
    // the caller disappears while the physical process still owns this claim.
    _execution: crate::sync::owned::OwnedMutexGuard<()>,
}

pub(in crate::local) struct Claims {
    entries: Vec<Claim>,
    runtime: tokio::runtime::Handle,
}

impl Drop for Claims {
    fn drop(&mut self) {
        let entries = std::mem::take(&mut self.entries);
        if entries.is_empty() {
            return;
        }
        // The worker drops custody only after process-tree cleanup. Its owned
        // task now retains the execution guards through unpublished retirement;
        // dropping the awaiting caller cannot cancel this cleanup task.
        self.runtime.spawn(async move {
            for claim in entries.into_iter().rev() {
                if let Err(error) = claim.source.abandon(claim.entered).await {
                    tracing::error!(?error, "cancelled native scope retirement failed");
                }
            }
        });
    }
}

pub(in crate::local) async fn native_scope<T>(
    mounts: &Mounts,
    body: impl AsyncFnOnce(Claims) -> Result<(T, Claims)>,
) -> Result<T> {
    let mut claims = Claims {
        entries: Vec::new(),
        runtime: tokio::runtime::Handle::current(),
    };
    let mut projected = Ok(());
    for (relative, mount) in ordered_mounts(mounts)? {
        if let Some(source) = &mount.view.source {
            // Stable projection identity orders acquisition. Hold every selected volume's
            // execution guard across worker cleanup, verification and retirement.
            // A Linux task view aliases the original binding; mount at that
            // binding's root, never below another owner-only FUSE mount.
            let root = match projection_root(&source.destination, relative) {
                Ok(root) => root,
                Err(error) => {
                    projected = Err(error);
                    break;
                }
            };
            let execution =
                crate::sync::owned::lock_owned(StateArc::clone(&source.execution)).await;
            let entry = async {
                source.prepare_command().await?;
                Box::pin(source.enter(root, relative, mount.mode.read_only())).await
            }
            .await;
            match entry {
                Ok(entry) => claims.entries.push(Claim {
                    source: Arc::clone(source),
                    entered: entry,
                    _execution: execution,
                }),
                Err(error) => {
                    projected = Err(error);
                    break;
                }
            }
        }
    }
    if let Err(error) = projected {
        // No command ran, so the volumes already entered leave through the
        // same exit. A failed leave keeps its projection, and the next exit
        // or retirement retries it and reports that cause again.
        let _ = leave_all(std::mem::take(&mut claims.entries)).await;
        return Err(error);
    }
    // Boxed, as are projection entry and exit: these deep futures stay out of
    // every caller's layout. The body returns claims only after physical
    // cleanup and verification. Cancellation leaves them in the worker,
    // rather than this caller future.
    let (value, mut claims) = Box::pin(body(claims)).await?;
    let entered = std::mem::take(&mut claims.entries);
    leave_all(entered).await?;
    Ok(value)
}

async fn leave_all(entered: Vec<Claim>) -> Result<()> {
    // Publication is an accepted operation. Losing its caller after verification
    // must not release the execution guard before physical retirement finishes.
    spawn(async move {
        let mut retired = Ok(());
        for claim in entered.into_iter().rev() {
            keep_first(
                &mut retired,
                Box::pin(claim.source.leave(claim.entered)).await,
            );
        }
        retired
    })
    .await
    .map_err(|error| Error::Storage(format!("native scope retirement interrupted: {error}")))?
}

// Every multi-volume exit attempts each volume and reports the first cause; one
// failure never strands the volumes after it.
fn keep_first(first: &mut Result<()>, next: Result<()>) {
    if first.is_ok() {
        *first = next;
    }
}

/// Negative controls only: projects outside any execution scope, so nothing
/// retires the projection until `retire`.
#[cfg(test)]
pub(in crate::local) async fn project_unscoped(mounts: &Mounts) -> Result<()> {
    for (relative, mount) in mounts.mounts() {
        if let Some(source) = &mount.view.source {
            let root = projection_root(&source.destination, relative)?;
            drop(source.enter(root, relative, mount.mode.read_only()).await?);
        }
    }
    Ok(())
}

/// Retires every projection when the host disconnects. A volume a worker still
/// owns reports a conflict, but every other volume is still published and
/// retired rather than abandoned unpublished when the harness drops.
pub(in crate::local) async fn retire(mounts: &Mounts) -> Result<()> {
    let mut retired = Ok(());
    for (_, mount) in mounts.mounts() {
        if let Some(source) = &mount.view.source {
            keep_first(&mut retired, Box::pin(source.retire()).await);
        }
    }
    retired
}

fn projection_root<'a>(destination: &'a Path, relative: &str) -> Result<&'a Path> {
    destination
        .ancestors()
        .nth(Path::new(relative).components().count())
        .filter(|_| destination.ends_with(relative))
        .ok_or_else(|| Error::Invalid("volume projection lies outside its binding".into()))
}

#[cfg(test)]
mod binding_paths {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // Native aliases retain their source's composed root, independent of
        // prefix spelling or a nested task's different physical root.
        #[test]
        fn projection_keeps_the_source_binding_root(
            directories in prop::collection::vec("[a-zA-ZΩα]{1,8}", 0..4),
            volume in "[a-zA-ZΩα]{1,8}",
        ) {
            let mut root=std::env::current_dir().map_err(|error| TestCaseError::fail(error.to_string()))?;
            for directory in directories {root.push(directory);}
            let relative=format!(".volumes/{volume}");
            let destination=root.join(&relative);
            let projected=projection_root(&destination,&relative)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert_eq!(projected,root.as_path());
            let other=format!("{relative}x");
            prop_assert!(projection_root(&destination,&other).is_err());
            // Choosing the alias task's root loses the source binding.
            let task_root=root.join("task-view");
            prop_assert_ne!(task_root.as_path(),projected);
        }
    }
}

// FUSE's elevated helper cannot traverse an owner-only mount, and ProjFS cannot
// start a virtualization root at a placeholder in another projection. Retain
// the already bound volume instead of mounting again below the task view.
#[cfg(any(target_os = "linux", windows))]
pub(super) fn link(root: &Path, mounts: &mut Mounts) -> Result<()> {
    for (relative, mount) in mounts.mounts_mut() {
        if relative.is_empty() {
            continue;
        }
        let source = mount
            .view
            .source
            .as_ref()
            .ok_or_else(|| Error::NotFound("volume mount is not bound".into()))?;
        let target = source.destination.as_path();
        let relative = Path::new(relative);
        if let Some(parent) = relative.parent() {
            create_mount_directory(root, parent)?;
        }
        let path = root.join(relative);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let linked = std::fs::read_link(&path).map_err(mount_io)?;
                let linked = path
                    .parent()
                    .ok_or_else(|| Error::Invalid("volume alias has no parent".into()))?
                    .join(linked);
                if canonical_destination(&linked)? != canonical_destination(target)? {
                    return Err(Error::Conflict("volume alias names another binding".into()));
                }
            }
            Ok(metadata) if metadata.is_dir() => {
                // remove_dir atomically refuses a nonempty directory; user
                // content is never replaced to install a volume binding.
                std::fs::remove_dir(&path).map_err(mount_io)?;
                link_directory(target, &path).map_err(mount_io)?;
            }
            Ok(_) => {
                return Err(Error::Conflict(
                    "volume alias replaces a project file".into(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                link_directory(target, &path).map_err(mount_io)?;
            }
            Err(error) => return Err(mount_io(error)),
        }
        mount.native_root = Some(target.to_path_buf());
    }
    Ok(())
}

// A command retires its physical projection. Resolve its held parent rather
// than requiring that transient destination to exist when validating an alias.
#[cfg(any(target_os = "linux", windows))]
fn canonical_destination(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::Invalid("volume destination has no parent".into()))?;
    let name = path
        .file_name()
        .ok_or_else(|| Error::Invalid("volume destination has no name".into()))?;
    Ok(std::fs::canonicalize(parent).map_err(mount_io)?.join(name))
}

#[cfg(target_os = "linux")]
fn link_directory(target: &Path, path: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, path)
}

#[cfg(windows)]
fn link_directory(target: &Path, path: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, path)
}

// Recover only SDK-owned mount destinations. The existing recovery API takes
// the exclusive destination fence before probing or detaching the driver, so
// a live owner is rejected and authored host residue is retained.
pub(super) fn prepare_mount_directory(root: &Path, relative: &Path) -> Result<()> {
    let destination = root.join(relative);
    let controls = acyclic_fs::native_mount_control_paths(&destination)?;
    let mut excluded = vec![destination.clone()];
    excluded.extend(
        controls
            .iter()
            .filter(|path| path.starts_with(root))
            .cloned(),
    );
    exclude::exclude_mount_paths(root, &excluded).map_err(mount_io)?;
    if let Some(parent) = relative.parent() {
        create_mount_directory(root, parent)?;
    }
    let [fence, ..] = controls;
    match std::fs::symlink_metadata(fence) {
        Ok(_) => acyclic_fs::detach_native_mount_destination_after_crash(&destination)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(mount_io(error)),
    }
    create_mount_directory(root, relative)
}

pub(super) fn create_mount_directory(root: &Path, relative: &Path) -> Result<()> {
    let mut path = root.to_path_buf();
    for part in relative.components() {
        let std::path::Component::Normal(name) = part else {
            return Err(Error::Invalid("invalid mount path".into()));
        };
        path.push(name);
        match std::fs::create_dir(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let entry = std::fs::symlink_metadata(&path).map_err(mount_io)?;
                if !entry.is_dir() || entry.file_type().is_symlink() {
                    return Err(Error::Invalid(
                        "mount parent is not an ordinary directory".into(),
                    ));
                }
            }
            Err(error) => return Err(mount_io(error)),
        }
    }
    Ok(())
}

fn mount_io(cause: std::io::Error) -> Error {
    acyclic_fs::NativeTreeMaterializationError::Io(cause).into()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn absent_fuse_device_has_a_typed_tool_admission_error() {
        let error = require_available(acyclic_fs::NativeMountCapabilities {
            kind: Some(acyclic_fs::NativeMountKind::LinuxFuse),
            unavailable_reason: Some("missing device"),
        })
        .unwrap_err();
        assert!(matches!(
            error,
            Error::MountUnavailable {
                device: "/dev/fuse"
            }
        ));
        assert_eq!(error.code(), crate::wire::ErrorCode::Unsupported);
    }

    #[test]
    fn mount_directory_preserves_the_native_io_cause() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let error =
            prepare_mount_directory(&directory.path().join("missing"), Path::new(SCRATCH_PATH))
                .expect_err("the mount parent is absent");
        assert!(matches!(error, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Io(error), ..
        } if error.kind() == std::io::ErrorKind::NotFound));
        Ok(())
    }

    #[test]
    fn mount_parent_cannot_redirect_creation_outside_the_view() -> Result<()> {
        let root = tempfile::tempdir().map_err(storage)?;
        let outside = tempfile::tempdir().map_err(storage)?;
        std::os::unix::fs::symlink(outside.path(), root.path().join(".volumes"))
            .map_err(storage)?;
        assert!(matches!(
            prepare_mount_directory(root.path(), Path::new(SCRATCH_PATH)),
            Err(Error::Invalid(_))
        ));
        assert!(!outside.path().join("scratch").exists());
        Ok(())
    }
}
