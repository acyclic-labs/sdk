//! Direct Windows images and the process identity checked before execution.
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use windows_sys::Win32::Foundation::{
    APPMODEL_ERROR_NO_PACKAGE, ERROR_INSUFFICIENT_BUFFER, HANDLE,
};

/// Resolves an accessible native PE image rather than an activation alias.
///
/// Packaged Windows applications may launch descendants outside an assigned
/// Job. Shell discovery therefore considers every PATH candidate and selects
/// a directly accessible image. The suspended process's actual package
/// identity is also checked at the common process admission boundary.
#[allow(
    unsafe_code,
    reason = "classify a native executable with GetBinaryTypeW"
)]
pub(crate) fn resolve_native_executable(path: &Path) -> io::Result<PathBuf> {
    let path = path.canonicalize()?;
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut kind = 0;
    // SAFETY: canonicalize accepted the path, and wide has a terminating NUL.
    if unsafe {
        windows_sys::Win32::Storage::FileSystem::GetBinaryTypeW(wide.as_ptr(), &raw mut kind)
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if !matches!(kind, 0 | 6) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "process containment requires a native Windows executable",
        ));
    }
    Ok(path)
}

#[allow(
    unsafe_code,
    reason = "query the suspended process's OS package identity"
)]
pub(crate) fn require_unpackaged(process: HANDLE) -> io::Result<()> {
    let mut length = 0;
    // SAFETY: the caller owns the suspended child handle, which grants query
    // access. A null buffer with zero length queries only its identity class.
    let status = unsafe {
        windows_sys::Win32::Storage::Packaging::Appx::GetPackageFullName(
            process,
            &raw mut length,
            std::ptr::null_mut(),
        )
    };
    admit_package_status(status)
}

fn admit_package_status(status: u32) -> io::Result<()> {
    match status {
        APPMODEL_ERROR_NO_PACKAGE => Ok(()),
        ERROR_INSUFFICIENT_BUFFER => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "packaged Windows applications cannot preserve process-tree custody",
        )),
        other => Err(io::Error::other(format!(
            "cannot establish unpackaged process identity: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn only_an_os_proven_unpackaged_process_is_admitted(status: u32) {
            prop_assert_eq!(admit_package_status(status).is_ok(), status == APPMODEL_ERROR_NO_PACKAGE);
        }
    }

    #[test]
    fn package_identity_and_query_errors_fail_closed() {
        assert!(admit_package_status(APPMODEL_ERROR_NO_PACKAGE).is_ok());
        for status in [ERROR_INSUFFICIENT_BUFFER, 0, 5, u32::MAX] {
            assert!(admit_package_status(status).is_err());
        }
    }

    #[test]
    fn image_resolution_rejects_non_executables_and_preserves_literal_paths() {
        let root = tempfile::tempdir().expect("image fixture");
        let image = root.path().join("native & % image.exe");
        std::fs::copy(std::env::current_exe().expect("test image"), &image).expect("copy image");
        assert_eq!(
            resolve_native_executable(&image).expect("native image"),
            image.canonicalize().unwrap()
        );
        let invalid = root.path().join("activation.exe");
        std::fs::write(&invalid, []).expect("non-image fixture");
        assert!(resolve_native_executable(&invalid).is_err());
        assert!(resolve_native_executable(&root.path().join("absent.exe")).is_err());
    }
}
