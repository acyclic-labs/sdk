use super::{File, io, unsupported};
use std::os::{fd::AsRawFd as _, unix::fs::FileTypeExt as _};

pub(super) fn admit(file: File) -> io::Result<File> {
    let descriptor = file.as_raw_fd();
    // SAFETY: F_GETFL reads flags on the caller's still-owned descriptor.
    #[expect(unsafe_code, reason = "reads the original descriptor's status flags")]
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if flags & libc::O_ACCMODE == libc::O_RDONLY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "inherited output descriptor is read-only",
        ));
    }
    let kind = file.metadata()?.file_type();
    // SAFETY: isatty inspects only this live descriptor.
    #[expect(unsafe_code, reason = "classifies the original terminal descriptor")]
    let terminal = kind.is_char_device() && unsafe { libc::isatty(descriptor) } == 1;
    if !kind.is_fifo() && !kind.is_socket() && !terminal {
        return Err(unsupported());
    }
    if flags & libc::O_NONBLOCK != 0 {
        return Ok(file);
    }
    #[cfg(target_os = "linux")]
    if kind.is_fifo() || terminal {
        use std::os::unix::fs::OpenOptionsExt as _;
        // Opening the original procfs descriptor creates a different open file
        // description. A dup followed by F_SETFL would change the launcher's
        // flags too. Keep the original alive until its replacement is open.
        return std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOCTTY)
            .open(format!("/proc/self/fd/{descriptor}"));
    }
    Err(unsupported())
}
