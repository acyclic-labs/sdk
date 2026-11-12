use super::*;
use std::os::windows::fs::OpenOptionsExt as _;
use windows_sys::Win32::{
    Storage::FileSystem::FILE_FLAG_OVERLAPPED,
    System::Ioctl::{FSCTL_QUERY_ALLOCATED_RANGES, FSCTL_SET_SPARSE},
};

#[test]
fn sparse_device_queries_retain_partial_bytes_and_resume_at_the_original_range() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(directory.path().join("sparse"))?;
    device_control_in_place(
        &file,
        FSCTL_SET_SPARSE,
        &[],
        &mut [],
        NativeOperationKind::Write,
        None,
    )?
    .into_complete()?;
    let end = 32 * 1024 * 1024_u64;
    file.set_len(end)?;
    crate::file::write_all_in_place(&file, 0, &[1; 4096], None)?;
    crate::file::write_all_in_place(&file, end / 2, &[2; 4096], None)?;
    file.sync_all()?;
    let mut query = [0; 16];
    query[8..].copy_from_slice(&end.to_le_bytes());
    let mut page = [0; 16];
    let first = device_control_in_place(
        &file,
        FSCTL_QUERY_ALLOCATED_RANGES,
        &query,
        &mut page,
        NativeOperationKind::Read,
        None,
    )?;
    assert_eq!(first, NativeControlCompletion::MoreData(page.len()));
    assert_eq!(first.into_complete().unwrap_err().raw_os_error(), Some(234));
    let offset = u64::from_le_bytes(page[..8].try_into().unwrap());
    let length = u64::from_le_bytes(page[8..].try_into().unwrap());
    assert_eq!(offset, 0);
    assert!(length > 0 && length < end / 2);
    let next = offset + length;
    query[..8].copy_from_slice(&next.to_le_bytes());
    query[8..].copy_from_slice(&(end - next).to_le_bytes());
    let second = device_control_in_place(
        &file,
        FSCTL_QUERY_ALLOCATED_RANGES,
        &query,
        &mut page,
        NativeOperationKind::Read,
        None,
    )?;
    assert_eq!(second, NativeControlCompletion::Complete(page.len()));
    assert_eq!(second.into_complete()?, page.len());
    assert_eq!(u64::from_le_bytes(page[..8].try_into().unwrap()), end / 2);
    assert!(u64::from_le_bytes(page[8..].try_into().unwrap()) > 0);
    Ok(())
}
