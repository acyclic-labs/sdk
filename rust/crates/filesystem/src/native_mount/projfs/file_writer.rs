use super::*;

pub(super) fn ordinary_basic_information(
    entry: &ProjectedEntry,
) -> windows::Win32::Storage::FileSystem::FILE_BASIC_INFO {
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_RECALL_ON_OPEN, FILE_ATTRIBUTE_REPARSE_POINT, FILE_BASIC_INFO,
    };

    let attributes = entry.info.FileAttributes
        & !(FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS.0
            | FILE_ATTRIBUTE_RECALL_ON_OPEN.0
            | FILE_ATTRIBUTE_REPARSE_POINT.0);
    FILE_BASIC_INFO {
        CreationTime: entry.info.CreationTime,
        LastAccessTime: entry.info.LastAccessTime,
        LastWriteTime: entry.info.LastWriteTime,
        ChangeTime: entry.info.ChangeTime,
        FileAttributes: if attributes == 0 {
            FILE_ATTRIBUTE_NORMAL.0
        } else {
            attributes
        },
    }
}

pub(super) fn staging_directory(
    root: &std::path::Path,
    work: &crate::NativeMaterializationWork,
) -> Result<(cap_std::fs::Dir, crate::NativeWorkReservation), NativeMountError> {
    let allocation = work.claim(
        (root.as_os_str().len() as u64)
            .checked_mul(4)
            .and_then(|n| n.checked_add(128))
            .ok_or(crate::WorkError::Overflow)?,
        2,
        crate::WorkCounters::UNCHARGED,
    )?;
    let staging = staging_path(root);
    let parent = crate::native_host::open_root_directory_with_work(
        staging
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?,
        &mut &*work,
    )?;
    let credit = work.allocation_work().reserve_work(crate::WorkCounters {
        backend_write_operations: 3,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let file = crate::native_host::with_wide_path_with_work(
        std::path::Path::new(
            staging
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?,
        ),
        &mut &*work,
        |name, work| {
            crate::native_host::create_windows_new_directory_with_work(&parent, name, work)
        },
    )?;
    drop(parent);
    drop(staging);
    drop(allocation);
    Ok((cap_std::fs::Dir::from_std_file(file), credit))
}
