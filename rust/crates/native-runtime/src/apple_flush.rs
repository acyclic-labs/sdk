use crate::Durability;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;

pub(crate) fn sync_once(
    file: &File,
    durability: Durability,
    mut admit: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    if durability == Durability::Full {
        match syscall(file, libc::F_FULLFSYNC, &mut admit) {
            Ok(()) => return Ok(()),
            Err(error) if error.raw_os_error() == Some(libc::ENOTSUP) => {}
            Err(error) => return Err(error),
        }
    }
    match syscall(file, libc::F_BARRIERFSYNC, &mut admit) {
        Err(error) if error.raw_os_error() == Some(libc::ENOTSUP) => syscall(file, 0, &mut admit)?,
        result => result?,
    }
    match durability {
        Durability::Barrier => Ok(()),
        Durability::Full => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "filesystem does not support F_FULLFSYNC; only a write barrier was completed",
        )),
    }
}

fn syscall(
    file: &File,
    command: i32,
    admit: &mut impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    loop {
        admit()?;
        let result = (|| {
            #[cfg(test)]
            let command = tests::intercept(command)?;
            // SAFETY: each flush operation acts only on the held live descriptor.
            let result = unsafe {
                if command == 0 {
                    libc::fsync(file.as_raw_fd())
                } else {
                    libc::fcntl(file.as_raw_fd(), command)
                }
            };
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        })();
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(test)]
#[path = "apple_flush_tests.rs"]
mod tests;
