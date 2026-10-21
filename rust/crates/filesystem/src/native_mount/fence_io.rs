use std::{fs::File, io};

#[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
use acyclic_native_runtime::{FileLock, FileLockMode};

/// An open mount lock. Its [`FileLock`] releases the lock on its open file
/// description before closing: a descriptor duplicated into a concurrently
/// spawned child (between fork and exec) must not extend ownership.
#[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
#[derive(Debug)]
pub(super) struct MountLockFile(FileLock);

#[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
impl From<File> for MountLockFile {
    fn from(file: File) -> Self {
        Self(FileLock::new(file))
    }
}

#[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
impl std::ops::Deref for MountLockFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.0
    }
}

#[cfg(test)]
#[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
pub(super) struct MountLockFile(
    File,
    std::sync::Arc<super::disconnected::kernel::Kernel>,
    usize,
);

#[cfg(test)]
#[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
impl std::fmt::Debug for MountLockFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, formatter)
    }
}

#[cfg(test)]
#[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
impl From<File> for MountLockFile {
    fn from(file: File) -> Self {
        let domain = super::disconnected::kernel::Kernel::lock_domain(&file);
        Self(
            file,
            super::disconnected::kernel::current().unwrap(),
            domain,
        )
    }
}
#[cfg(test)]
#[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
impl std::ops::Deref for MountLockFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.0
    }
}
#[cfg(test)]
#[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
impl Drop for MountLockFile {
    fn drop(&mut self) {
        // The real retirement worker may outlive the model's main closure.
        // Keep syscall state alive until this actual descriptor is closed.
        self.1.unlock(&self.0, self.2);
    }
}

pub(super) fn try_lock(file: &MountLockFile) -> io::Result<()> {
    #[cfg(test)]
    #[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
    return file.1.try_lock(file, file.2);
    #[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
    file.0.try_lock(FileLockMode::Exclusive)
}

pub(super) fn unlock(file: &MountLockFile) -> io::Result<()> {
    #[cfg(test)]
    #[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
    {
        file.1.unlock(file, file.2);
        return Ok(());
    }
    #[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
    file.0.unlock()
}

// A blocking OS lock is one admitted syscall, independent of contention.
// The model replaces that syscall with the same kernel-owned wait queue.
pub(super) fn lock(file: &MountLockFile) -> io::Result<()> {
    #[cfg(test)]
    #[cfg(all(acyclic_fs_native_loom, target_os = "linux"))]
    loop {
        if file.2 == 0 && file.1.registry_wait_disabled() {
            return file.1.try_lock(file, file.2);
        }
        match file.1.try_lock(file, file.2) {
            Ok(()) => return Ok(()),
            Err(error) if super::lock_is_contended(&error) => {
                file.1.wait_for_unlock(file, file.2);
            }
            Err(error) => return Err(error),
        }
    }
    #[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
    file.0.lock(FileLockMode::Exclusive)
}
