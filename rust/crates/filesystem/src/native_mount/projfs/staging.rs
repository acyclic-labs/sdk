use super::*;

pub(super) fn staging_path(root: &std::path::Path) -> PathBuf {
    let name = root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    root.with_file_name(format!(".{name}.acyclic-staging"))
}

pub(super) fn write_listed_content(
    projection: Projection<'_>,
    path: &MountPath,
    entry: &ProjectedEntry,
    file: &mut std::fs::File,
) -> Result<bool, String> {
    let size = u64::try_from(entry.info.FileSize).unwrap_or(0);
    let mut failure = None;
    let mut delivered = 0_u64;
    let mut sink = |_offset: u64, packet: crate::kernel::OwnedSourceChunk| {
        let mut remaining = packet.bytes().as_ref();
        while !remaining.is_empty() {
            let count =
                match crate::native_host::write_builtin_file(file, remaining, projection.work) {
                    Ok(0) => {
                        return Err(MountSourceError::NativeWork(
                            std::io::Error::from(std::io::ErrorKind::WriteZero).into(),
                        ));
                    }
                    Ok(count) => count,
                    Err(error) => {
                        failure = Some(error);
                        return Err(MountSourceError::Stale);
                    }
                };
            delivered = delivered
                .checked_add(count as u64)
                .ok_or(crate::WorkError::Overflow)?;
            remaining = &remaining[count..];
        }
        Ok(())
    };
    let read = match entry.facts.pin {
        Some(pin) => projection.runtime.source.as_ref().read_pinned_with_meter(
            path,
            pin,
            0,
            size,
            WRITE_CHUNK_BYTES,
            &mut sink,
            projection.work,
        ),
        None => (|| {
            let mut offset = 0;
            while offset < size {
                let count = u32::try_from(size - offset)
                    .unwrap_or(u32::MAX)
                    .min(WRITE_CHUNK_BYTES);
                let packet = projection.runtime.source.as_ref().read_range_with_meter(
                    path,
                    offset,
                    count,
                    projection.work,
                )?;
                if packet.bytes().is_empty() {
                    return Err(MountSourceError::Stale);
                }
                offset += u64::try_from(packet.bytes().len()).unwrap_or(u64::MAX);
                sink(offset, packet)?;
            }
            Ok(())
        })(),
    };
    match (read, failure) {
        (_, Some(error)) => Err(format!("writing {path:?}: {error}")),
        (Ok(()), None) => Ok(delivered == size),
        (Err(MountSourceError::Stale | MountSourceError::NotFound), None) => Ok(false),
        (Err(error), None) => Err(format!("reading {path:?}: {error}")),
    }
}

pub(super) fn held_while(
    path: &std::path::Path,
    state: HostWindowsMetadata,
) -> std::io::Result<Option<std::fs::File>> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ,
    };

    let file = match std::fs::OpenOptions::new()
        .access_mode(DELETE.0 | FILE_READ_ATTRIBUTES.0)
        .share_mode((FILE_SHARE_READ | FILE_SHARE_DELETE).0)
        .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok((handle_state(&file)? == state).then_some(file))
}

pub(super) fn delete_link(path: &std::path::Path, digest: u64) -> std::io::Result<bool> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let link = std::fs::OpenOptions::new()
        .access_mode(DELETE.0 | FILE_READ_ATTRIBUTES.0)
        .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
        .custom_flags((FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS).0)
        .open(path)?;
    let target = reparse_data(&link, symbolic_link_target)?.flatten();
    if target.is_none_or(|target| link_digest(target) != digest) {
        return Ok(false);
    }
    delete_through(&link, || Ok::<_, std::io::Error>(()))?;
    Ok(true)
}

pub(super) fn delete_file_while(
    path: &std::path::Path,
    state: HostWindowsMetadata,
) -> std::io::Result<bool> {
    let Some(file) = held_while(path, state)? else {
        return Ok(false);
    };
    delete_through(&file, || Ok::<_, std::io::Error>(()))?;
    Ok(true)
}

pub(super) fn delete_through<E: From<std::io::Error>>(
    file: &impl std::os::windows::io::AsRawHandle,
    mut start: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
        FILE_DISPOSITION_FLAG_POSIX_SEMANTICS, FILE_DISPOSITION_INFO, FILE_DISPOSITION_INFO_EX,
        FILE_DISPOSITION_INFO_EX_FLAGS, FileDispositionInfo, FileDispositionInfoEx,
        SetFileInformationByHandle,
    };

    let handle = HANDLE(file.as_raw_handle());
    let posix = FILE_DISPOSITION_INFO_EX {
        Flags: FILE_DISPOSITION_INFO_EX_FLAGS(
            FILE_DISPOSITION_FLAG_DELETE.0
                | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS.0
                | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE.0,
        ),
    };
    let on_close = FILE_DISPOSITION_INFO { DeleteFile: true };
    start()?;
    // SAFETY: the held DELETE handle and fixed-sized ABI input remain live.
    let result = unsafe {
        SetFileInformationByHandle(
            handle,
            FileDispositionInfoEx,
            (&raw const posix).cast(),
            size_of::<FILE_DISPOSITION_INFO_EX>() as u32,
        )
    };
    if result.is_err() {
        start()?;
        // SAFETY: the same original handle receives the fallback disposition.
        unsafe {
            SetFileInformationByHandle(
                handle,
                FileDispositionInfo,
                (&raw const on_close).cast(),
                size_of::<FILE_DISPOSITION_INFO>() as u32,
            )
        }
        .map_err(std::io::Error::from)
        .map_err(E::from)?;
    }
    Ok(())
}

pub(super) fn symbolic_link_target(data: &[u8]) -> Option<Vec<u16>> {
    const SYMLINK: u32 = 0xA000_000C;
    const PATH_BUFFER: usize = 20;
    let field = |at: usize| {
        data.get(at..)
            .and_then(<[u8]>::first_chunk)
            .map(|bytes| usize::from(u16::from_le_bytes(*bytes)))
    };
    if u32::from_le_bytes(*data.first_chunk()?) != SYMLINK {
        return None;
    }
    let (offset, length) = (field(8)?, field(10)?);
    let name = data.get(PATH_BUFFER + offset..PATH_BUFFER + offset + length)?;
    let mut units = decode_utf16_name(name)?;
    let unc = r"\??\UNC\".encode_utf16().collect::<Vec<_>>();
    let nt = r"\??\".encode_utf16().collect::<Vec<_>>();
    if units.starts_with(&unc) {
        units.splice(..unc.len(), r"\\".encode_utf16());
    } else if units.starts_with(&nt) {
        units.drain(..nt.len());
    }
    Some(units)
}

pub(super) fn delete_unowned(
    projection: Projection<'_>,
    path: &MountPath,
) -> Result<Release, String> {
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    let relative = HSTRING::from(host.as_os_str());
    match prj_delete(projection, &relative, PRJ_UPDATE_ALLOW_READ_ONLY) {
        Ok(()) => Ok(Release::Done),
        Err(error) if error.code() == HR_SHARING_VIOLATION => Ok(Release::Busy),
        Err(error)
            if [
                HR_FILE_NOT_FOUND,
                HR_PATH_NOT_FOUND,
                HR_VIRTUALIZATION_INVALID_OPERATION,
                HR_DIRECTORY_NOT_EMPTY,
            ]
            .contains(&error.code()) =>
        {
            Ok(Release::Done)
        }
        Err(error) => Err(format!(
            "deleting placeholder {path:?}: {}",
            driver_error(&error)
        )),
    }
}
