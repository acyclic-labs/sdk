use crate::Durability;
use std::fs::File;
use std::io;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd};

// The held-descriptor and file APIs share this physical attempt.
// The original caller owns admission and observes its first result.
pub(crate) fn sync_descriptor_once(descriptor: BorrowedFd<'_>) -> io::Result<()> {
    syscall_once(descriptor, libc::F_FULLFSYNC)
}

pub(crate) fn sync_once(
    file: &File,
    durability: Durability,
    mut admit: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    if durability == Durability::Full {
        return syscall(file, libc::F_FULLFSYNC, &mut admit);
    }
    match syscall(file, libc::F_BARRIERFSYNC, &mut admit) {
        Err(error) if error.raw_os_error() == Some(libc::ENOTSUP) => syscall(file, 0, &mut admit),
        result => result,
    }
}

fn syscall(
    file: &File,
    command: i32,
    admit: &mut impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    admit()?;
    syscall_once(file.as_fd(), command)
}

fn syscall_once(descriptor: BorrowedFd<'_>, command: i32) -> io::Result<()> {
    #[cfg(test)]
    let command = tests::intercept(command)?;
    // SAFETY: each flush operation acts only on the held live descriptor.
    let result = unsafe {
        if command == 0 {
            libc::fsync(descriptor.as_raw_fd())
        } else {
            libc::fcntl(descriptor.as_raw_fd(), command)
        }
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
#[path = "apple_flush_tests.rs"]
mod tests;
