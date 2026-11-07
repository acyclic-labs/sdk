use crate::*;
#[cfg(any(windows, target_os = "linux"))]
pub(crate) use operations::write_all_in_place;
pub(crate) use operations::write_bounded_in_place;
#[cfg(all(test, target_os = "linux", target_pointer_width = "64"))]
pub(crate) use range::read_bounded_slice;
#[cfg(test)]
pub(crate) use range::read_bounded_spare;
pub(crate) use range::validate_prepared_read_range;
pub(crate) use scope::{with_file_admission, with_file_scope};
pub(crate) use write_validation::validate_write_batch;

mod capability;
#[cfg(target_os = "linux")]
mod descriptor;
#[cfg(target_os = "linux")]
pub(crate) use descriptor::outside_standard_streams;
mod sync_batch;
pub use sync_batch::NativeSyncBatch;
mod durable;
pub use durable::{
    flush_file_all, flush_file_data, rename_file, truncate_file, write_file, write_file_all,
    write_file_with,
};
mod destination;
pub(crate) use destination::ReadDestination;
#[cfg(all(test, target_vendor = "apple", not(loom)))]
mod device_tests;
mod operations;
pub(crate) use durable::flush_cached_data;
pub(crate) use operations::{sync_cached_data, sync_cached_file};
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
    sync_filesystem, sync_parent, write_all_at,
};
pub use range::AsyncRangeReader;

/// Evidence retained by one physical cached descriptor, never by its path.
#[cfg(target_vendor = "apple")]
pub(crate) type FlushState = crate::device_flush::HeldDevice;
#[cfg(not(target_vendor = "apple"))]
pub(crate) type FlushState = ();

/// Samples only previously resolved device evidence, after the native barrier.
pub(crate) fn cached_ordered_cut(state: &FlushState) -> Option<u64> {
    #[cfg(all(target_vendor = "apple", not(loom)))]
    return state.ordered_cut();
    #[cfg(not(all(target_vendor = "apple", not(loom))))]
    {
        let _ = state;
        None
    }
}

/// Coverage belongs to the retained physical descriptor's original device.
pub(crate) fn cached_flush_covers(state: &FlushState, cut: u64) -> bool {
    #[cfg(all(target_vendor = "apple", not(loom)))]
    return state.covers(cut);
    #[cfg(not(all(target_vendor = "apple", not(loom))))]
    {
        let _ = (state, cut);
        false
    }
}

/// One original file descriptor and its owner-bound durability evidence.
///
/// Importing moves the file without I/O, allocation or descriptor duplication.
/// Evidence resolves at the first successful full-flush stage and survives only
/// while this owner retains that file; descriptor-number reuse cannot inherit it.
#[derive(Debug)]
pub struct HeldFile {
    pub(crate) file: File,
    #[cfg(target_vendor = "apple")]
    pub(crate) device: crate::device_flush::HeldDevice,
    // Fields retire in declaration order: physical File before its claim.
    pub(crate) _allocation: Option<ReadAllocation>,
}

impl HeldFile {
    /// Retains the original file, without querying it or changing its flags.
    pub fn new(file: File) -> Self {
        Self {
            file,
            #[cfg(target_vendor = "apple")]
            device: Default::default(),
            _allocation: None,
        }
    }

    /// Borrows this owner's original file.
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Returns the original file, releasing its retained durability evidence.
    pub fn into_file(self) -> File {
        self.file
    }

    /// One full-flush attempt after the caller's original durability admission.
    /// Uses the same physical flush as [`crate::sync_descriptor_once`], retaining
    /// successful device classification across requests on this owner. Native
    /// failures, including interruption, return without retry or weaker fallback.
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    pub fn sync_once(&self) -> io::Result<()> {
        #[cfg(unix)]
        use std::os::fd::AsFd as _;
        #[cfg(windows)]
        use std::os::windows::io::AsHandle as _;
        #[cfg(target_vendor = "apple")]
        return crate::apple::sync_descriptor_once(self.file.as_fd(), Some(&self.device));
        #[cfg(target_os = "linux")]
        return crate::sync_descriptor_once(self.file.as_fd());
        #[cfg(windows)]
        crate::sync_descriptor_once(self.file.as_handle())
    }
}

impl std::ops::Deref for HeldFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.file
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for HeldFile {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.file)
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsHandle for HeldFile {
    fn as_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        std::os::windows::io::AsHandle::as_handle(&self.file)
    }
}

#[cfg(windows)]
pub(crate) type FileIdentity = (u64, [u8; 16]);

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(crate) type FileIdentity = (u64, u64);

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
pub(crate) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    #[cfg(test)]
    FILE_IDENTITY_QUERIES.with(|count| count.set(count.get() + 1));
    #[cfg(windows)]
    return windows::file_identity(file);
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        Ok(file_identity_from_metadata(&file.metadata()?))
    }
}

#[cfg(all(test, any(windows, target_os = "linux", target_vendor = "apple")))]
thread_local! {
    static FILE_IDENTITY_QUERIES: Cell<usize> = const { Cell::new(0) };
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(crate) fn file_identity_from_metadata(metadata: &std::fs::Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt as _;
    (metadata.dev(), metadata.ino())
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
    file: &Arc<HeldFile>,
    writes: Vec<OwnedWrite>,
) -> io::Result<()> {
    validate_write_batch(&writes)?;
    write_all_batch_impl(file, writes)
}
