use super::*;

#[test]
fn root_identity_keeps_the_real_native_open_failure() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("file");
    std::fs::write(&file, b"not a directory")?;
    let paths = [file, directory.path().join("missing")];
    #[cfg(unix)]
    let paths = {
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(directory.path(), &link)?;
        paths.into_iter().chain([link])
    };
    for path in paths {
        // Observe the host's actual failure, rather than inventing errno or
        // assuming that different kernels reject a no-follow open identically.
        let Err(DemandError::Io(expected)) =
            HostRoot::open(&path, &mut crate::native_host::ordinary_work())
        else {
            panic!("invalid root did not return a native open error: {path:?}");
        };
        match capture_root_identity(&path) {
            Err(CaptureError::Kernel(error)) => match *error {
                crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::Io(actual)) => {
                    assert_eq!(actual.kind(), expected.kind());
                    assert_eq!(actual.raw_os_error(), expected.raw_os_error());
                }
                error => panic!("native root open lost its cause: {error:?}"),
            },
            result => panic!("native root open lost its cause: {result:?}"),
        }
    }
    Ok(())
}

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
