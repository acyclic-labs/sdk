use crate::*;
pub(crate) use operations::write_bounded_in_place;
#[cfg(test)]
pub(crate) use range::read_bounded_spare;
pub(crate) use range::validate_prepared_read_range;
pub(crate) use scope::{with_file_admission, with_file_scope};
pub(crate) use write_validation::validate_write_batch;

mod capability;
mod durable;
pub use durable::{
    flush_file_all, flush_file_data, rename_file, truncate_file, write_file, write_file_all,
    write_file_with,
};
mod destination;
pub(crate) use destination::ReadDestination;
mod operations;
mod range;
mod scope;
mod write_validation;
pub use capability::{NativeFile, NativeFileMeter};
#[cfg(windows)]
pub use operations::device_control_in_place;
#[cfg(unix)]
pub use operations::read_declared_source_bytes;
pub use operations::{
    Durability, OwnedRead, OwnedWrite, read_at, read_batch_async, set_len, sync_data, sync_file,
    sync_parent, write_all_at,
};
pub use range::AsyncRangeReader;

#[derive(Debug)]
pub(crate) struct NativeFilePayload {
    pub(crate) file: File,
    // Fields retire in declaration order: physical File before its claim.
    pub(crate) _allocation: Option<ReadAllocation>,
}

impl std::ops::Deref for NativeFilePayload {
    type Target = File;
    fn deref(&self) -> &File {
        &self.file
    }
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
pub(crate) fn write_all_batch_owned(
    file: &Arc<NativeFilePayload>,
    writes: Vec<OwnedWrite>,
) -> io::Result<()> {
    validate_write_batch(&writes)?;
    write_all_batch_impl(file, writes)
}
