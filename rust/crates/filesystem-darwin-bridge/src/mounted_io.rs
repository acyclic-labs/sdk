use super::MOUNT_OWNER_IMAGE;
use std::ffi::{CString, c_char, c_int};
use std::os::unix::ffi::OsStrExt as _;
use std::sync::OnceLock;

unsafe extern "C" {
    fn nfs4_test_mounted_io(root: *const c_char, image: *const u8, len: usize) -> c_int;
}

/// Mounts a fixture view at a fresh directory under the temporary root, then
/// looks up and creates an entry through the kernel; returns that errno.
pub(crate) fn mounted_io() -> std::io::Result<c_int> {
    // mount_nfs records the resolved mount point, so the path must be canonical.
    let root = std::env::temp_dir()
        .canonicalize()?
        .join(format!("darwin-nfs-{}", std::process::id()));
    let mount_path = CString::new(root.as_os_str().as_bytes())?;
    std::fs::create_dir(&root)?;
    // SAFETY: the path stays terminated through the call and the image is static.
    let result = unsafe {
        nfs4_test_mounted_io(
            mount_path.as_ptr(),
            MOUNT_OWNER_IMAGE.as_ptr(),
            MOUNT_OWNER_IMAGE.len(),
        )
    };
    std::fs::remove_dir(&root)?;
    Ok(result)
}

/// The permission error a mounted view returned to this process, if any.
pub(super) fn denied() -> Option<&'static std::io::Error> {
    static PROBE: OnceLock<Option<std::io::Error>> = OnceLock::new();
    PROBE
        .get_or_init(|| {
            let errno = mounted_io().ok().filter(|errno| *errno != 0)?;
            let error = std::io::Error::from_raw_os_error(errno);
            (error.kind() == std::io::ErrorKind::PermissionDenied).then_some(error)
        })
        .as_ref()
}
