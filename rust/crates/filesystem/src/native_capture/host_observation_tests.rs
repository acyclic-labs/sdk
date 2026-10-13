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
            Err(CaptureError::Kernel(error)) if matches!(*error, crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::Io(_)
            ))
        ));
    }
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), b"body")?;
    let source_root = HostRoot::open(directory.path(), &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    let native =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let scope = CaptureWorkScope::new(&native)?;
    assert!(
        observe_host_path(
            &source_root,
            &test_host_path(&Path::new("file").join("child")),
            &scope.native
        )?
        .is_none()
    );
    Ok(())
}
