use std::{fs::File, io};

#[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
pub(super) type MountLockFile = File;

#[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
pub(super) struct MountLockFile(
    File,
    std::sync::Arc<super::disconnected::kernel::Kernel>,
    usize,
);

#[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
impl std::fmt::Debug for MountLockFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, formatter)
    }
}

#[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
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
#[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
impl std::ops::Deref for MountLockFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.0
    }
}
#[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
impl Drop for MountLockFile {
    fn drop(&mut self) {
        // The real retirement worker may outlive the model's main closure.
        // Keep syscall state alive until this actual descriptor is closed.
        self.1.unlock(&self.0, self.2);
    }
}

pub(super) fn try_lock(file: &MountLockFile) -> io::Result<()> {
    #[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
    return file.1.try_lock(file, file.2);
    #[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
    fs2::FileExt::try_lock_exclusive(file)
}

pub(super) fn unlock(file: &MountLockFile) -> io::Result<()> {
    #[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
    {
        file.1.unlock(file, file.2);
        return Ok(());
    }
    #[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
    fs2::FileExt::unlock(file)
}

pub(super) fn wait(file: &MountLockFile) {
    #[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
    return file.1.wait_for_unlock(file, file.2);
    #[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
    {
        let _ = file;
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
