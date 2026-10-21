//! Explicit NTFS fixtures for Windows features whose contract requires NTFS.
use std::io;
use std::os::windows::fs::OpenOptionsExt as _;
use std::os::windows::io::AsRawHandle as _;
use windows::Win32::{Foundation::HANDLE, Storage::FileSystem::GetVolumeInformationByHandleW};

/// Retains compression and exact-notification coverage when general TEMP is ReFS.
pub(crate) fn ntfs_tempdir() -> io::Result<tempfile::TempDir> {
    let directory = match std::env::var_os("ACYCLIC_TEST_NTFS_ROOT").filter(|root| !root.is_empty())
    {
        Some(root) => tempfile::Builder::new()
            .prefix("acyclic-ntfs-")
            .tempdir_in(root)?,
        None => tempfile::tempdir()?,
    };
    let handle = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS.0)
        .open(directory.path())?;
    let mut name = [0_u16; 64];
    query_filesystem(&handle, &mut name)?;
    if !name.starts_with(&[78, 84, 70, 83, 0]) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NTFS fixture required: set ACYCLIC_TEST_NTFS_ROOT to an NTFS directory",
        ));
    }
    Ok(directory)
}

#[allow(
    unsafe_code,
    reason = "queries the filesystem type through an owned directory handle"
)]
fn query_filesystem(handle: &std::fs::File, name: &mut [u16; 64]) -> io::Result<()> {
    // SAFETY: the owned directory handle and output buffer live through the query.
    unsafe {
        GetVolumeInformationByHandleW(
            HANDLE(handle.as_raw_handle()),
            None,
            None,
            None,
            None,
            Some(name),
        )
    }
    .map_err(io::Error::from)
}
