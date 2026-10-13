use super::*;

#[cfg(test)]
mod host_observation_tests {
    use super::*;

    #[test]
    fn only_absent_paths_may_become_deletions() -> Result<(), Box<dyn std::error::Error>> {
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::NotADirectory,
        ] {
            assert!(classify_host_observation(Err(DemandError::Io(kind.into())))?.is_none());
        }
        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::InvalidInput,
        ] {
            assert!(matches!(
                classify_host_observation(Err(DemandError::Io(kind.into()))),
                Err(CaptureError::Kernel(crate::FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::Io(_)
                )))
            ));
        }
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), b"body")?;
        let source_root =
            HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
                .map_err(crate::native_host::ordinary_error)?;
        let native =
            crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|(error, _)| error)?;
        let scope = CaptureWorkScope::new(&native)?;
        assert!(
            observe_host_path(
                &source_root,
                &test_host_path(Path::new("file/child")),
                &scope
            )?
            .is_none()
        );
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn subtree_walk_rejects_intermediate_symlinks() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        std::fs::write(outside.path().join("private"), b"outside")?;
        symlink(outside.path(), directory.path().join("alias"))?;
        let source_root =
            HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
                .map_err(crate::native_host::ordinary_error)?;
        assert!(
            source_root
                .stat(
                    Path::new("alias/private"),
                    &mut crate::native_host::ordinary_work()
                )
                .map_err(crate::native_host::ordinary_error)
                .is_err()
        );
        Ok(())
    }
}
