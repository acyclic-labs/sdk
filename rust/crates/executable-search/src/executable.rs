//! Direct Windows executable images.

use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};

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

#[cfg(test)]
mod tests {
    use super::*;

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
