use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, kernel::NamespacePath};

pub(super) const SCRATCH_PATH: &str = ".volumes/scratch";

#[cfg(target_os = "linux")]
pub(super) fn recover_after_crash(root: &Path) -> Result<()> {
    let relative = Path::new(SCRATCH_PATH);
    let parent = relative
        .parent()
        .ok_or_else(|| Error::Invalid("scratch mount has no parent".into()))?;
    create_mount_directory(root, parent)?;
    let destination = root.join(relative);
    match std::fs::metadata(&destination) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {
            // A killed FUSE owner leaves a disconnected namespace. Reuse the
            // platform's exclusive recovery fence before inspecting the source.
            acyclic_fs::detach_native_mount_destination_after_crash(&destination)?;
            Ok(())
        }
        Err(error) => Err(storage(error)),
    }
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

pub(in crate::local) fn excluded(
    root: &Path,
    config: acyclic_fs::VolumeConfig,
    relative: &str,
) -> Result<Vec<NamespacePath>> {
    let destination = root.join(relative);
    std::iter::once(destination.clone())
        .chain(acyclic_fs::native_mount_control_paths(&destination).map_err(Error::from)?)
        .map(|path| {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| Error::Invalid("mount metadata lies outside the task view".into()))?;
            let relative = relative
                .to_str()
                .ok_or_else(|| Error::Invalid("mount path is not UTF-8".into()))?;
            let path = acyclic_fs::path::PortablePath::parse(
                &format!("/{}", relative.replace('\\', "/")),
                config.limits,
            )
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
        }));
    }
    Ok(())
}

pub(in crate::local) async fn sync(mounts: &Mounts) -> Result<()> {
    for (_, mount) in mounts.mounts() {
        if let Some(source) = &mount.view.source {
            let native = source.native().await;
            if let Some(native) = native.as_ref() {
                native.sync().await?;
                native.advance_to_head().await?;
            }
        }
    }
    Ok(())
}

pub(in crate::local) async fn project(mounts: &Mounts) -> Result<()> {
    for (relative, mount) in mounts.mounts() {
        if let Some(source) = &mount.view.source {
            // A Linux task view aliases the original binding; mount at that
            // binding's root, never below another owner-only FUSE mount.
            let root = projection_root(&source.destination, relative)?;
            source
                .project(root, relative, mount.mode.read_only())
                .await?;
        }
    }
    Ok(())
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

// fusermount's elevated helper cannot traverse an owner-only FUSE mount.
// Retain the already bound volume and expose its authorized path instead of
// starting a second kernel mount below the task's sparse projection.
#[cfg(target_os = "linux")]
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
                if std::fs::read_link(&path).map_err(mount_io)? != target {
                    return Err(Error::Conflict("volume alias names another binding".into()));
                }
            }
            Ok(metadata) if metadata.is_dir() => {
                // remove_dir atomically refuses a nonempty directory; user
                // content is never replaced to install a volume binding.
                std::fs::remove_dir(&path).map_err(mount_io)?;
                std::os::unix::fs::symlink(target, &path).map_err(mount_io)?;
            }
            Ok(_) => {
                return Err(Error::Conflict(
                    "volume alias replaces a project file".into(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::os::unix::fs::symlink(target, &path).map_err(mount_io)?;
            }
            Err(error) => return Err(mount_io(error)),
        }
        mount.native_root = Some(target.to_path_buf());
    }
    Ok(())
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
            create_mount_directory(&directory.path().join("missing"), Path::new(SCRATCH_PATH))
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
            create_mount_directory(root.path(), Path::new(SCRATCH_PATH)),
            Err(Error::Invalid(_))
        ));
        assert!(!outside.path().join("scratch").exists());
        Ok(())
    }
}
