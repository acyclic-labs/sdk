use super::*;
use acyclic_fs::{
    CancellationToken, MountOptions, MountPublication, WorkBudget, kernel::NamespacePath,
};

pub(super) const SCRATCH_PATH: &str = ".volumes/scratch";

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
        create_mount_directory(root, Path::new(relative))?;
        let options = if mount.mode.read_only() {
            MountOptions::read_only()
        } else {
            MountOptions::read_write().publication(MountPublication::Manual)
        };
        acyclic_fs::WorkspaceGraph::new(store.clone())
            .register_root(&source)
            .await?;
        let native = source.mount(&destination, options).await?;
        mount.view.source = Some(Arc::new(MountedVolume {
            workspace: crate::tools::git::OwnedView::Volume(source),
            native: Some(native),
        }));
    }
    Ok(())
}

pub(in crate::local) async fn sync(mounts: &Mounts) -> Result<()> {
    for (_, mount) in mounts.mounts() {
        if let Some(native) = mount
            .view
            .source
            .as_ref()
            .and_then(|source| source.native.as_ref())
        {
            native.sync().await?;
            native.advance_to_head().await?;
        }
    }
    Ok(())
}

fn create_mount_directory(root: &Path, relative: &Path) -> Result<()> {
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
