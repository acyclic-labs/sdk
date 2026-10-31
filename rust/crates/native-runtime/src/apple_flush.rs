use crate::Durability;
use std::fs::File;
use std::io;
use std::os::fd::{AsFd as _, AsRawFd as _, BorrowedFd};

// The held-descriptor API and the retrying file API share this physical
// attempt: stage the descriptor with `fsync`, then share one `F_FULLFSYNC`
// of its device with every concurrent request (see `device_flush`). The
// caller alone owns admission, cancellation and EINTR retry.
pub(crate) fn sync_descriptor_once(descriptor: BorrowedFd<'_>) -> io::Result<()> {
    sync_descriptor_requested_once(descriptor, Durability::Full)
}

fn sync_descriptor_requested_once(
    descriptor: BorrowedFd<'_>,
    requested: Durability,
) -> io::Result<()> {
    #[cfg(not(loom))]
    return crate::device_flush::full(
        descriptor,
        |descriptor| syscall_once(descriptor, 0, requested),
        |descriptor| syscall_once(descriptor, libc::F_FULLFSYNC, requested),
    );
    #[cfg(loom)]
    syscall_once(descriptor, libc::F_FULLFSYNC, requested)
}

pub(crate) fn order_descriptor_once(descriptor: BorrowedFd<'_>) -> io::Result<()> {
    syscall_once(descriptor, libc::F_BARRIERFSYNC, Durability::Barrier)
}

pub(crate) fn sync_once(
    file: &File,
    durability: Durability,
    mut admit: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    match durability {
        Durability::Full => full(file, durability, &mut admit),
        Durability::Barrier => match syscall(file, libc::F_BARRIERFSYNC, durability, &mut admit) {
            Err(error) if error.raw_os_error() == Some(libc::ENOTSUP) => {
                full(file, durability, &mut admit)
            }
            result => result,
        },
    }
}

fn full(
    file: &File,
    requested: Durability,
    admit: &mut impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    loop {
        admit()?;
        match sync_descriptor_requested_once(file.as_fd(), requested) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

fn syscall(
    file: &File,
    command: i32,
    requested: Durability,
    admit: &mut impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    loop {
        admit()?;
        match syscall_once(file.as_fd(), command, requested) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

fn syscall_once(descriptor: BorrowedFd<'_>, command: i32, requested: Durability) -> io::Result<()> {
    #[cfg(test)]
    let command = tests::intercept(command)?;
    let syscall = match command {
        libc::F_FULLFSYNC => "F_FULLFSYNC",
        libc::F_BARRIERFSYNC => "F_BARRIERFSYNC",
        _ => "fsync",
    };
    crate::held_sync::observe_sync(requested, syscall, descriptor.as_raw_fd() as u64, || {
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
    })
}

#[cfg(test)]
#[path = "apple_flush_tests.rs"]
mod tests;
