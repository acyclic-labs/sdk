use std::{fs::File, io};

#[cfg(not(all(test, acyclic_fs_native_loom, target_os = "linux")))]
pub(super) type MountLockFile = File;

#[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
#[derive(Debug)]
pub(super) struct MountLockFile(File);

#[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
impl From<File> for MountLockFile {
    fn from(file: File) -> Self {
        Self(file)
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
        if let Some(kernel) = super::disconnected::kernel::current() {
            kernel.unlock(&self.0);
        }
    }
}

pub(super) fn try_lock(file: &File) -> io::Result<()> {
    #[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
    if let Some(kernel) = super::disconnected::kernel::current() {
        return kernel.try_lock(file);
    }
    fs2::FileExt::try_lock_exclusive(file)
}

pub(super) fn unlock(file: &File) -> io::Result<()> {
    #[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
    if let Some(kernel) = super::disconnected::kernel::current() {
        kernel.unlock(file);
        return Ok(());
    }
    fs2::FileExt::unlock(file)
}

pub(super) fn wait(file: &File) {
    #[cfg(all(test, acyclic_fs_native_loom, target_os = "linux"))]
    if let Some(kernel) = super::disconnected::kernel::current() {
        kernel.wait_for_unlock(file);
        return;
    }
    let _ = file;
    std::thread::sleep(std::time::Duration::from_millis(1));
}
