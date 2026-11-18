use super::{FileLock, FileLockMode};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub(super) fn open(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

pub(super) fn is_free(path: &Path) -> io::Result<bool> {
    match FileLock::try_acquire(open(path)?, FileLockMode::Exclusive) {
        Ok(_probe) => Ok(true),
        Err(error) if super::is_exclusive_lock_contention(&error) => Ok(false),
        Err(error) => Err(error),
    }
}
