//! Compare complete fixture bytes while Windows retains their mandatory locks.
#![allow(
    unsafe_code,
    reason = "read-only Windows file mappings require native FFI"
)]
use std::{
    fs::File,
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    path::Path,
};
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, FILE_MAP_READ, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile, PAGE_READONLY,
    UnmapViewOfFile,
};

struct View(MEMORY_MAPPED_VIEW_ADDRESS);

impl Drop for View {
    fn drop(&mut self) {
        // SAFETY: this owner has the one successful mapping and releases it once.
        let _ = unsafe { UnmapViewOfFile(self.0) };
    }
}

pub(super) fn read(path: &Path) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    let length = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
    if length == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: the live fixture file is opened read-only, with no named mapping
    // or change to its length. Fixture commands are stopped during snapshots.
    let mapping = unsafe {
        CreateFileMappingW(
            file.as_raw_handle(),
            std::ptr::null(),
            PAGE_READONLY,
            0,
            0,
            std::ptr::null(),
        )
    };
    if mapping.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileMappingW transferred a fresh owned handle.
    let mapping = unsafe { OwnedHandle::from_raw_handle(mapping) };
    // SAFETY: the owned read-only mapping covers exactly the current file.
    let view = unsafe { MapViewOfFile(mapping.as_raw_handle(), FILE_MAP_READ, 0, 0, length) };
    if view.Value.is_null() {
        return Err(io::Error::last_os_error());
    }
    let view = View(view);
    // SAFETY: this sequential fixture never truncates or modifies its receipt
    // during a snapshot; both the mapping and view outlive the complete copy.
    let bytes = unsafe { std::slice::from_raw_parts(view.0.Value.cast::<u8>(), length) }.to_vec();
    Ok(bytes)
}

#[test]
fn snapshots_read_locked_bytes_and_detect_same_length_changes() -> io::Result<()> {
    use acyclic_native_runtime::{FileLock, FileLockMode};
    use std::io::{Seek as _, Write as _};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("locked-receipt");
    let file = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    let mut writer = &file;
    writer.write_all(b"before")?;
    writer.flush()?;
    let held = FileLock::acquire(file, FileLockMode::Exclusive)?;
    let Err(error) = std::fs::read(&path) else {
        return Err(io::Error::other(
            "ordinary reads must reproduce the Windows lock violation",
        ));
    };
    assert_eq!(error.raw_os_error(), Some(33));
    assert_eq!(read(&path)?, b"before");
    let mut writer: &File = &held;
    writer.rewind()?;
    writer.write_all(b"after!")?;
    writer.flush()?;
    assert_eq!(read(&path)?, b"after!");
    let other = File::options().read(true).write(true).open(&path)?;
    assert!(FileLock::try_acquire(other, FileLockMode::Exclusive).is_err());
    Ok(())
}
