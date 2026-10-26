use super::*;

// Actual held entries, authenticated root handle, backing capacity and birth
// credit. These resources move into the existing physical retirement worker.
pub(super) type ProjectionCleanup = (
    Vec<std::fs::File>,
    std::fs::File,
    crate::NativeAllocation,
    crate::NativeWorkReservation,
);

fn cleanup_work(files: usize) -> Result<crate::WorkCounters, crate::WorkError> {
    Ok(crate::WorkCounters {
        // POSIX disposition, then legacy disposition if necessary; root tag last.
        backend_write_operations: (files as u64)
            .checked_mul(2)
            .and_then(|writes| writes.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    })
}

#[cfg(test)]
pub(in crate::native_mount) fn detach_projection(
    destination: &std::path::Path,
    expected: crate::NativeRootIdentity,
    source: &dyn MountFilesystem,
    work: &crate::NativeMaterializationWork,
    stop: impl FnOnce() -> Result<(), NativeMountError>,
) -> Result<(), NativeMountError> {
    let mut cleanup = prepare_projection_cleanup(destination, expected, source, work)?;
    stop()?;
    finish_projection_cleanup(&mut cleanup)
}

pub(super) fn prepare_projection_cleanup(
    destination: &std::path::Path,
    expected: crate::NativeRootIdentity,
    source: &dyn MountFilesystem,
    work: &crate::NativeMaterializationWork,
) -> Result<ProjectionCleanup, NativeMountError> {
    use windows::Wdk::Storage::FileSystem::{FILE_OPEN, FILE_SYNCHRONOUS_IO_NONALERT};
    use windows::Win32::Storage::FileSystem::{
        FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES, SYNCHRONIZE,
    };
    let root = HostRoot::open(destination, &mut &*work)?;
    if root.identity() != expected
        || reparse_tag_held(&root.directory, &mut &*work)?
            != Some(windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
    {
        return Err(NativeMountError::InvalidDestination);
    }
    let root_handle = crate::native_host::open_windows_at(
        &root.directory,
        &[46],
        FILE_GENERIC_WRITE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN,
        FILE_SYNCHRONOUS_IO_NONALERT,
        &mut &*work,
    )?;
    let mut files = Vec::new();
    let mut allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    collect_projection(
        &root,
        &MountPath::root(),
        source,
        work,
        &mut files,
        &mut allocation,
    )?;
    let credit = work.reserve_work(cleanup_work(files.len())?)?;
    Ok((files, root_handle, allocation, credit))
}

pub(super) fn finish_projection_cleanup(
    (files, root_handle, _, credit): &mut ProjectionCleanup,
) -> Result<(), NativeMountError> {
    // Each attempt funds only the still-held entries from the original finite
    // ledger. Failed syscalls consume credit; cancellation cannot revoke cleanup.
    credit.refill_cleanup(cleanup_work(files.len())?)?;
    // Held no-follow handles name only source-backed entries sampled before detach.
    while let Some(file) = files.last() {
        match delete_through(file, || credit.started_io(WriteOperations.delta(1), None)) {
            // Unknown children keep their directory; cleanup never traverses them.
            Err(crate::NativeTreeMaterializationError::Io(error))
                if error
                    .raw_os_error()
                    .is_some_and(|code| code == 145 || code == HR_DIRECTORY_NOT_EMPTY.0) => {}
            result => result.map_err(NativeMountError::from)?,
        }
        drop(files.pop());
    }
    credit.started_io(WriteOperations.delta(1), None)?;
    delete_root_tag(root_handle).map_err(NativeMountError::from)
}

/// Removes the ProjFS reparse tag from a virtualization root opened for
/// writing, which detaches the root from every provider.
pub(super) fn delete_root_tag(root_handle: &std::fs::File) -> std::io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    let tag = windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS.to_le_bytes();
    let mut packet = [0_u8; 8];
    packet[..4].copy_from_slice(&tag);
    let mut returned = 0;
    // SAFETY: the root handle and fixed reparse header remain live.
    unsafe {
        windows::Win32::System::IO::DeviceIoControl(
            windows::Win32::Foundation::HANDLE(root_handle.as_raw_handle()),
            windows::Win32::System::Ioctl::FSCTL_DELETE_REPARSE_POINT,
            Some(packet.as_ptr().cast()),
            u32::try_from(packet.len())
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?,
            None,
            0,
            Some(&raw mut returned),
            None,
        )
    }
    .map_err(std::io::Error::from)
}

/// Detaches a virtualization root by path, if it is one: the provider that
/// served it is gone (stopped, or its process died).
pub(in crate::native_mount) fn detach_root_by_path(root: &std::path::Path) -> std::io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt as _;
    // Backup semantics open the directory itself; no reparse follows.
    let handle = std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(
            windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS.0
                | windows::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT.0,
        )
        .open(root)?;
    detach_root_handle(&handle)
}

fn detach_root_handle(handle: &std::fs::File) -> std::io::Result<()> {
    const ERROR_NOT_A_REPARSE_POINT: u32 = 4390;
    match delete_root_tag(handle) {
        // Already detached; the control reports the Win32 code as an HRESULT.
        Err(error)
            if error.raw_os_error().is_some_and(|code| {
                code as u32 == ERROR_NOT_A_REPARSE_POINT
                    || code as u32 == 0x8007_0000 | ERROR_NOT_A_REPARSE_POINT
            }) =>
        {
            Ok(())
        }
        result => result,
    }
}

fn collect_projection(
    root: &HostRoot,
    path: &MountPath,
    source: &dyn MountFilesystem,
    work: &crate::NativeMaterializationWork,
    files: &mut Vec<std::fs::File>,
    allocation: &mut crate::NativeAllocation,
) -> Result<(), NativeMountError> {
    use windows::Wdk::Storage::FileSystem::{FILE_OPEN, FILE_SYNCHRONOUS_IO_NONALERT};
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_READ_ATTRIBUTES, SYNCHRONIZE};
    let (mut reader, _reader_allocation) = root.read_dir_stats(&[], &mut &*work)?;
    while let Some((entry, _entry_allocation)) = reader.next(&mut &*work)? {
        // Name-only probes omit reparse entries; inspect their held no-follow metadata.
        let stat = match entry.stat {
            Some(stat) => stat,
            None => root.stat(&entry.name, &mut &*work)?,
        };
        let bytes = path.components().iter().map(Vec::len).sum::<usize>()
            + (path.components().len() * 2).max(4) * size_of::<Vec<u8>>();
        let copied = crate::WorkCounters {
            bytes_copied: (path.components().iter().map(Vec::len).sum::<usize>()
                + entry.name.len() * 2) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(copied)?;
        let _path_allocation = work.claim(bytes as u64, 1, crate::WorkCounters::UNCHARGED)?;
        let (mut component, _component_allocation) =
            crate::native_host::buffer(entry.name.len() * 2, &mut &*work)?;
        component.extend(entry.name.iter().flat_map(|unit| unit.to_le_bytes()));
        let child = path.child(component);
        work.record_admitted(copied)?;
        let Some(lookup) = source.lookup_with_meter(&child, work)? else {
            continue;
        };
        let directory = stat.file_type() == crate::demand::SourceNodeKind::Directory;
        if directory != (lookup.node.kind == MountNodeKind::Directory) {
            continue;
        }
        let file = crate::native_host::open_windows_at(
            &root.directory,
            &entry.name,
            DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
            FILE_OPEN,
            FILE_SYNCHRONOUS_IO_NONALERT,
            &mut &*work,
        )?;
        if root.stat_file(&file, &mut &*work)?.identity() != stat.identity() {
            return Err(NativeMountError::InvalidDestination);
        }
        if files.len() == files.capacity() {
            let capacity = files
                .capacity()
                .max(4)
                .checked_mul(2)
                .ok_or(crate::WorkError::Overflow)?;
            (&mut &*work)
                .reconcile_allocation(allocation, (capacity * size_of::<std::fs::File>()) as u64)?;
            files.try_reserve_exact(capacity - files.len()).map_err(
                <&crate::NativeMaterializationWork as NativeHostWork>::allocation_failed,
            )?;
            (&mut &*work).reconcile_allocation(
                allocation,
                (files.capacity() * size_of::<std::fs::File>()) as u64,
            )?;
        }
        files.push(file);
        if directory
            && (stat.raw.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 == 0
                || stat.raw.ReparseTag
                    == windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS)
        {
            let directory = root.open_dir_held(&entry.name, &mut &*work)?;
            let identity = crate::native_host::directory_identity(&directory, &mut &*work)?;
            if identity != stat.identity() {
                return Err(NativeMountError::InvalidDestination);
            }
            let held = HostRoot::from_parts(directory, identity);
            collect_projection(&held, &child, source, work, files, allocation)?;
        }
    }
    Ok(())
}

/// ProjFS detachment is the only platform-specific part of dead-root
/// recovery. Enumeration and deletion use the existing held-root core.
pub(in crate::native_mount) fn detach_dead_shared_root(
    roots: &crate::HeldRoot,
    name: &std::path::Path,
) -> Result<(), NativeMountError> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows::Wdk::Storage::FileSystem::{
        FILE_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
    };
    use windows::Win32::Storage::FileSystem::{
        FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES, SYNCHRONIZE,
    };

    crate::native_host::require_leaf(name)?;
    let leaf: Vec<u16> = name.as_os_str().encode_wide().collect();
    let mut work = crate::native_host::ordinary_work();
    match crate::native_host::open_windows_at(
        &roots.root.directory,
        &leaf,
        FILE_GENERIC_WRITE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
        FILE_OPEN,
        FILE_DIRECTORY_FILE | FILE_OPEN_REPARSE_POINT | FILE_SYNCHRONOUS_IO_NONALERT,
        &mut work,
    ) {
        Ok(handle) => {
            let identity = roots.root.stat_file(&handle, &mut work)?.identity();
            detach_root_handle(&handle)?;
            roots.root.remove(&leaf, Some(identity), &mut work)?;
        }
        Err(crate::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let staging = super::staging_path(name);
    let staging: Vec<u16> = staging.as_os_str().encode_wide().collect();
    roots.root.remove(&staging, None, &mut work)?;
    Ok(())
}
