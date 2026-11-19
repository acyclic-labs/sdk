use super::MOUNT_OWNER_IMAGE;
use std::ffi::{CString, c_char, c_int};
use std::os::unix::ffi::OsStrExt as _;

unsafe extern "C" {
    fn nfs4_test_mounted_io(root: *const c_char, image: *const u8, len: usize) -> c_int;
}

#[test]
fn readiness_callback_can_issue_synchronous_nfs_io() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir()
        .canonicalize()?
        .join(format!("darwin-nfs-{}", std::process::id()));
    let mount_path = CString::new(root.as_os_str().as_bytes())?;
    std::fs::create_dir(&root)?;
    // SAFETY: the fixture retains its terminated path through the call and the image is static.
    let result = unsafe {
        nfs4_test_mounted_io(
            mount_path.as_ptr(),
            MOUNT_OWNER_IMAGE.as_ptr(),
            MOUNT_OWNER_IMAGE.len(),
        )
    };
    let removed = std::fs::remove_dir(&root);
    assert_eq!(result, 0);
    removed?;
    Ok(())
}
