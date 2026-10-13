use super::*;

pub(in crate::native_mount) fn detach_projection(
    destination: &std::path::Path,
    expected: crate::NativeRootIdentity,
    source: &dyn MountFilesystem,
    work: &crate::NativeMaterializationWork,
    stop: impl FnOnce() -> Result<(), NativeMountError>,
) -> Result<(), NativeMountError> {
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
    stop()?;
    // Held no-follow handles name only source-backed entries sampled before detach.
    for file in files.into_iter().rev() {
        match delete_through(&file, || (&mut &*work).started_io(WriteOperations.delta(1))) {
            // Unknown children keep their directory; cleanup never traverses them.
            Err(crate::NativeTreeMaterializationError::Io(error))
                if error
                    .raw_os_error()
                    .is_some_and(|code| code == 145 || code == HR_DIRECTORY_NOT_EMPTY.0) => {}
            result => result.map_err(|error| {
                NativeMountError::Driver(format!("removing a held projected entry: {error}"))
            })?,
        }
    }
    let tag = windows::Win32::System::SystemServices::IO_REPARSE_TAG_PROJFS.to_le_bytes();
    let mut packet = [0_u8; 8];
    packet[..4].copy_from_slice(&tag);
    let mut returned = 0;
    (&mut &*work)
        .attempt_io(WriteOperations.delta(1), || {
            use std::os::windows::io::AsRawHandle;
            // SAFETY: the authenticated root handle and fixed reparse header remain live.
            unsafe {
                windows::Win32::System::IO::DeviceIoControl(
                    windows::Win32::Foundation::HANDLE(root_handle.as_raw_handle()),
                    windows::Win32::System::Ioctl::FSCTL_DELETE_REPARSE_POINT,
                    Some(packet.as_ptr().cast()),
                    packet.len() as u32,
                    None,
                    0,
                    Some(&raw mut returned),
                    None,
                )
            }
            .map_err(std::io::Error::from)
        })
        .map_err(|error| {
            NativeMountError::Driver(format!("clearing the detached ProjFS root tag: {error}"))
        })?;
    drop(allocation);
    Ok(())
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
            let held = HostRoot {
                directory,
                identity,
            };
            collect_projection(&held, &child, source, work, files, allocation)?;
        }
    }
    Ok(())
}
