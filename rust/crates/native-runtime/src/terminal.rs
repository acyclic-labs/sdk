//! Controlling-terminal lookup for Darwin, where kqueue rejects `/dev/tty`.
use std::io;

/// The device the kernel records as this process's controlling terminal,
/// such as `/dev/ttys002`. Unlike the `/dev/tty` alias, kqueue accepts it.
pub fn controlling_device() -> io::Result<std::path::PathBuf> {
    use std::os::unix::ffi::OsStrExt;
    unsafe extern "C" {
        // <stdlib.h>: the reentrant devname(3).
        fn devname_r(
            device: libc::dev_t,
            kind: libc::mode_t,
            buffer: *mut libc::c_char,
            length: libc::c_int,
        ) -> *mut libc::c_char;
    }
    let missing = || io::Error::new(io::ErrorKind::NotFound, "no controlling terminal");
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(|_| io::ErrorKind::InvalidData)?;
    // SAFETY: the output is writable for exactly `size` bytes.
    let filled = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if filled != size {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: proc_pidinfo filled the whole zero-initialized record.
    let device = unsafe { info.assume_init() }.e_tdev;
    // NODEV: the process has no controlling terminal.
    if device == u32::MAX {
        return Err(missing());
    }
    let mut name = [0 as libc::c_char; 64];
    // SAFETY: the buffer is writable for its stated length; the result is
    // null or a NUL-terminated name inside it.
    let found = unsafe {
        devname_r(
            device as libc::dev_t,
            libc::S_IFCHR,
            name.as_mut_ptr(),
            name.len() as libc::c_int,
        )
    };
    if found.is_null() {
        return Err(missing());
    }
    // SAFETY: devname_r returned a NUL-terminated string in `name`.
    let name = unsafe { std::ffi::CStr::from_ptr(found) };
    Ok(std::path::Path::new("/dev").join(std::ffi::OsStr::from_bytes(name.to_bytes())))
}
