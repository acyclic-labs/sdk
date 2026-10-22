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

/// Creates an actual directory junction without a command interpreter or child.
pub(crate) fn junction(alias: &std::path::Path, target: &std::path::Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    crate::initialize_test_native_io();
    let target = std::fs::canonicalize(target)?;
    let print: Vec<u16> = target.as_os_str().encode_wide().collect();
    let suffix = print.strip_prefix(&[92, 92, 63, 92]).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "junction requires a canonical Windows path",
        )
    })?;
    let substitute: Vec<u16> = [92, 63, 63, 92]
        .into_iter()
        .chain(suffix.iter().copied())
        .collect();
    let length = |units: usize| -> io::Result<u16> {
        units
            .checked_mul(2)
            .and_then(|bytes| u16::try_from(bytes).ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "junction path is too long"))
    };
    let substitute_bytes = length(substitute.len())?;
    let print_offset = length(substitute.len() + 1)?;
    let print_bytes = length(print.len())?;
    let data_bytes = length(4 + substitute.len() + 1 + print.len() + 1)?;
    // REPARSE_DATA_BUFFER's mount-point variant: four byte offsets/lengths,
    // followed by the two independently NUL-terminated UTF-16 names.
    let mut data = Vec::with_capacity(8 + usize::from(data_bytes));
    data.extend_from_slice(
        &windows::Win32::System::SystemServices::IO_REPARSE_TAG_MOUNT_POINT.to_le_bytes(),
    );
    for value in [
        data_bytes,
        0,
        0,
        substitute_bytes,
        print_offset,
        print_bytes,
    ] {
        data.extend_from_slice(&value.to_le_bytes());
    }
    for unit in substitute.into_iter().chain([0]).chain(print).chain([0]) {
        data.extend_from_slice(&unit.to_le_bytes());
    }
    std::fs::create_dir(alias)?;
    let directory = std::fs::OpenOptions::new()
        .access_mode(windows::Win32::Foundation::GENERIC_WRITE.0)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(alias)?;
    acyclic_native_runtime::device_control_in_place(
        &directory,
        windows::Win32::System::Ioctl::FSCTL_SET_REPARSE_POINT,
        &data,
        &mut [],
        None,
    )?;
    Ok(())
}

#[test]
fn junction_fixture_preserves_literal_shell_characters() -> io::Result<()> {
    let root = tempfile::Builder::new()
        .prefix("junction-&%PATH%-")
        .tempdir()?;
    let target = root.path().join("target");
    std::fs::create_dir(&target)?;
    std::fs::write(target.join("leaf"), b"held outside the alias")?;
    let alias = root.path().join("alias&%PATH%");
    junction(&alias, &target)?;
    assert_eq!(
        std::fs::read(alias.join("leaf"))?,
        b"held outside the alias"
    );
    std::fs::remove_dir(&alias)?;
    assert_eq!(
        std::fs::read(target.join("leaf"))?,
        b"held outside the alias"
    );
    Ok(())
}
