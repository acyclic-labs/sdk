use super::MOUNT_OWNER_IMAGE;
use std::ffi::{CString, c_char, c_int};
use std::os::unix::ffi::OsStrExt as _;

unsafe extern "C" {
    fn nfs4_test_mounted_io(root: *const c_char, image: *const u8, len: usize) -> c_int;
}

#[test]
fn readiness_callback_can_issue_synchronous_nfs_io() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join(format!("darwin-nfs-{}", std::process::id()));
    let mount_path = CString::new(root.as_os_str().as_bytes())?;
    std::fs::create_dir(&root)?;
    let log = format!("/tmp/dfuse-diag-{}.log", std::process::id());
    // SAFETY: the test sets the diagnostic path before the transport reads it.
    unsafe { std::env::set_var("DARWINFUSE_LOG", &log) };
    eprintln!("DIAG log={log}");
    // SAFETY: the fixture retains its terminated path through the call and the image is static.
    let result = unsafe {
        nfs4_test_mounted_io(
            mount_path.as_ptr(),
            MOUNT_OWNER_IMAGE.as_ptr(),
            MOUNT_OWNER_IMAGE.len(),
        )
    };
    if let Ok(log) = std::fs::read_to_string(&log) {
        for line in log.lines() {
            if line.contains("ACCESS")
                || line.contains("op[")
                || line.contains("unsupported op")
                || line.contains("failed with status")
                || line.contains("DIAG")
                || line.contains("CREATE")
                || line.contains("LOOKUP")
                || line.contains("GETATTR")
                || line.contains("SETATTR")
                || line.contains("OPEN")
                || line.contains("CLOSE")
                || line.contains("REMOVE")
                || line.contains("RENAME")
                || line.contains("Mounted")
                || line.contains("mount_nfs")
            {
                eprintln!("DIAGLOG {line}");
            }
        }
    }
    let removed = std::fs::remove_dir(&root);
    assert_eq!(result, 0);
    removed?;
    Ok(())
}
