use crate::*;
pub(crate) use scope::{with_file_scope, with_file_admission};
#[cfg(all(test, any(windows, target_os = "linux", target_vendor = "apple")))]
pub(crate) use scope::{in_file_scope};
pub(crate) use operations::{write_bounded_in_place, validate_write_batch};
pub(crate) use range::{read_bounded_in_place, validate_prepared_read_range, read_prepared_in_place};

mod scope;
mod capability;
mod operations;
mod range;
pub use capability::{NativeFile, NativeFileMeter};
pub use operations::{Durability, sync_file, sync_data, sync_parent, read_at, write_all_at, OwnedWrite, OwnedRead, read_batch_async, set_len};
#[cfg(windows)]
pub use operations::device_control_in_place;
#[cfg(unix)]
pub use operations::read_declared_source_bytes;
pub use range::{NativeReadResult, AsyncRangeReader};

#[derive(Debug)]
pub(crate) struct NativeFilePayload {
    pub(crate) file: File,
    // Fields retire in declaration order: physical File before its claim.
    pub(crate) _allocation: Option<ReadAllocation>,
}

impl NativeFilePayload {
    pub(crate) fn unmetered(file: File) -> Self { Self { file, _allocation: None } }
}

impl std::ops::Deref for NativeFilePayload {
    type Target = File;
    fn deref(&self) -> &File { &self.file }
}

#[cfg(unix)]
impl std::os::fd::AsFd for NativeFilePayload {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.file)
    }
}

#[cfg(windows)]
pub(crate) type FileIdentity = (u64, [u8; 16]);

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(crate) type FileIdentity = (u64, u64);

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    #[cfg(windows)]
    return windows::file_identity(file);
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = file.metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }
}

pub(crate) fn is_uncertain_io_error(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux::is_uncertain_completion(error)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        false
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) fn write_all_batch_owned(file: &Arc<NativeFilePayload>, writes: Vec<OwnedWrite>) -> io::Result<()> {
    validate_write_batch(&writes)?;
    write_all_batch_impl(file, writes)
}
