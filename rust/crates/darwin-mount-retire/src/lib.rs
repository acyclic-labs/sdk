//! Retires a macOS kernel mount through Disk Arbitration: a forced unmount request awaited to its
//! terminal callback. Every Darwin mount owner retires through this one implementation: the SDK's
//! mount drivers and mount owner process, and the test runner reclaiming what a case left. macOS
//! refuses a non-root `unmount(2)` even of the user's own NFS mount, and diskutil's path discovery
//! opens the mount's root, which blocks when its server is dead.

#[cfg(target_os = "macos")]
#[allow(
    unsafe_code,
    reason = "FFI: the Disk Arbitration retirement compiled from retirement.c"
)]
mod native {
    use std::ffi::{CStr, c_char, c_int};
    use std::io;

    unsafe extern "C" {
        fn acyclic_fs_darwin_retire_mount(destination: *const c_char, dissent: *mut c_int)
        -> c_int;
    }

    /// Retires the mount at `destination`, its exact mount point as the kernel's mount table
    /// names it, and returns once Disk Arbitration has answered. The caller owns the destination
    /// for the whole call.
    ///
    /// # Errors
    ///
    /// The errno class of a refused or failed retirement (`ENOENT`: nothing is mounted there),
    /// with Disk Arbitration's own status in the message.
    pub fn retire(destination: &CStr) -> io::Result<()> {
        let mut dissent = 0;
        // SAFETY: `destination` is terminated and outlives the call, which returns only after
        // Disk Arbitration's terminal callback, so no callback storage outlives it.
        let error =
            unsafe { acyclic_fs_darwin_retire_mount(destination.as_ptr(), &raw mut dissent) };
        if error == 0 {
            return Ok(());
        }
        let class = io::Error::from_raw_os_error(error);
        Err(io::Error::new(
            class.kind(),
            format!(
                "retire {}: {class} (Disk Arbitration status {dissent:#x})",
                destination.to_string_lossy()
            ),
        ))
    }
}

#[cfg(target_os = "macos")]
pub use native::retire;
