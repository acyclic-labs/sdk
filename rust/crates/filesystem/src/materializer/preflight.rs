use super::*;
use crate::kernel::FileKind;
use crate::native_host::{HostRoot, HostStat};
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt as _;

pub(super) async fn preflight<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    root: &HostRoot,
    path: &str,
    generation: &crate::Generation<A, O>,
    children: bool,
    access_time: bool,
    work: &NativeMaterializationWork,
) -> Result<bool, NativeWorkspacePublicationError> {
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut pending = Vec::new();
    let mut allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let path = copy_string(path, &mut allocation)?;
    native_vec_push(&mut pending, (path, children, allocation), &mut storage)?;
    while let Some((path, children, _allocation)) = pending.pop() {
        let (absolute, _absolute_allocation) = native_absolute_string(&path, work)?;
        let source = match generation.stat(&absolute, work, work.cancellation()).await {
            Ok(stat) => Some(stat),
            Err(crate::WorkspaceError::NotFound) => None,
            Err(error) => return Err(error.into()),
        };
        let relative = Path::new(&path);
        let leaf = Path::new(
            relative
                .file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        );
        let parent = match { #[cfg(unix)] { root.open_dir_held(relative.parent().unwrap_or_else(|| Path::new("")), &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(relative.parent().unwrap_or_else(|| Path::new("")), &mut &*work, |path, work| root.open_dir_held(path, work)) } } {
            Ok(parent) => Some(parent),
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                None
            }
            Err(error) => return Err(error.into()),
        };
        let observed = match &parent {
            Some(parent) => match { #[cfg(unix)] { root.stat(relative, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(relative, &mut &*work, |path, work| root.stat(path, work)) } } {
                Ok(stat) => Some(stat),
                Err(NativeTreeMaterializationError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    None
                }
                Err(error) => return Err(error.into()),
            },
            None => None,
        };
        let (source, observed, parent) = match (source, observed, parent) {
            (None, None, _) => continue,
            (Some(source), Some(observed), Some(parent)) => (source, observed, parent),
            _ => return Ok(false),
        };
        if !source_metadata_matches(&observed, &source.metadata, access_time, work)? {
            return Ok(false);
        }
        match source.kind {
            FileKind::Regular if observed.file_type().is_file() => {
                if source.logical_bytes != Some(observed.len()) {
                    return Ok(false);
                }
                let mut file = crate::native_host::open_leaf(
                    &parent,
                    leaf,
                    &crate::native_host::NativeOpenOptions::Read,
                    &mut &*work,
                )?;
                if crate::native_host::stat_file(&file, &mut &*work)?.identity()
                    != observed.identity()
                {
                    return Ok(false);
                }
                let mut buffer = [0; 64 * 1024];
                let mut offset = 0;
                use std::io::Read as _;
                while offset < observed.len() {
                    let length = (observed.len() - offset)
                        .min(buffer.len() as u64)
                        .min(work.remaining()?.source_bytes_read / 2);
                    if length == 0 {
                        work.charge(NativeResourceCredit::ReadBytes.delta(2))?;
                    }
                    let expected = generation
                        .read_range(&absolute, offset, length, work, work.cancellation())
                        .await?;
                    if expected.bytes.len() as u64 != length {
                        return Ok(false);
                    }
                    let mut count = 0;
                    while count < length as usize {
                        work.charge(NativeResourceCredit::ReadOperations.delta(1))?;
                        let read = file.read(&mut buffer[count..length as usize])?;
                        work.spent(NativeResourceCredit::ReadBytes.delta(read as u64))?;
                        if read == 0 {
                            return Ok(false);
                        }
                        count += read;
                    }
                    if native_compare(
                        expected.bytes.iter().copied(),
                        buffer[..count].iter().copied(),
                        work,
                    )? != std::cmp::Ordering::Equal
                    {
                        return Ok(false);
                    }
                    offset += length;
                }
                let finished = crate::native_host::stat_file(&file, &mut &*work)?;
                if finished.identity() != observed.identity()
                    || finished.modified()? != observed.modified()?
                    || finished.len() != observed.len()
                    || !source_metadata_matches(&finished, &source.metadata, false, work)?
                {
                    return Ok(false);
                }
            }
            FileKind::SymbolicLink if observed.file_type().is_symlink() => {
                let expected = generation
                    .read_symbolic_link(&absolute, work, work.cancellation())
                    .await?;
                if !{
                    #[cfg(unix)]
                    let name = leaf.as_os_str();
                    #[cfg(windows)]
                    let (name, _name_allocation) = wide_name(leaf, work)?;
                    #[cfg(unix)]
                    let (target, _allocation) =
                        root.read_link(relative, work.remaining()?.source_bytes_read, &mut &*work)?;
                    #[cfg(windows)]
                    let (target, _allocation) = crate::native_host::read_link_held_wide(
                        &parent,
                        &name,
                        work.remaining()?.source_bytes_read,
                        &mut &*work,
                    )?;
                    #[cfg(unix)]
                    let bytes = target.iter().copied();
                    #[cfg(windows)]
                    let bytes = native_utf16_name_bytes(target.iter().copied());
                    native_compare(bytes, expected.bytes.iter().copied(), work)?
                        == std::cmp::Ordering::Equal
                } {
                    return Ok(false);
                }
            }
            FileKind::Directory if observed.is_dir() => {
                if children
                    && !generation_children(
                        root,
                        &path,
                        generation,
                        Some((&mut pending, &mut storage)),
                        work,
                    )
                    .await?
                {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
        let selected = { #[cfg(unix)] { root.stat(relative, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(relative, &mut &*work, |path, work| root.stat(path, work)) } }?;
        if selected.identity() != observed.identity()
            || children && observed.is_dir() && selected.modified()? != observed.modified()?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

// Without a queue, only host entries unknown to the generation are rejected.
pub(super) async fn generation_children<
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
>(
    root: &HostRoot,
    path: &str,
    generation: &crate::Generation<A, O>,
    mut pending: Option<(
        &mut Vec<(String, bool, NativeAllocation)>,
        &mut NativeAllocation,
    )>,
    work: &NativeMaterializationWork,
) -> Result<bool, NativeWorkspacePublicationError> {
    let directory = { #[cfg(unix)] { root.open_dir_held(Path::new(path), &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new(path), &mut &*work, |path, work| root.open_dir_held(path, work)) } }?;
    let identity = crate::native_host::directory_identity(&directory, &mut &*work)?;
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut names = Vec::new();
    let (mut reader, cursor) = crate::native_host::read_dir_held(&directory, &mut &*work)?;
    while let Some(name) = next_name(&mut reader, work)? {
        native_vec_push(&mut names, name, &mut storage)?;
    }
    drop(reader);
    drop(cursor);
    let (absolute, _absolute_allocation) = native_absolute_string(path, work)?;
    let mut after = None;
    let mut after_allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    loop {
        let (mut page, allocation) = generation
            .list_directory(&absolute, after.as_ref(), 1024, work, work.cancellation())
            .await?;
        let mut allocation = allocation.ok_or(NativeTreeMaterializationError::MissingCustody)?;
        for entry in &page.entries {
            let mut found = None;
            for (index, (name, _)) in names.iter().enumerate() {
                if host_name_matches(name, &entry.name, work)? {
                    found = Some(index);
                    break;
                }
            }
            if let Some(index) = found {
                names.swap_remove(index);
            } else if pending.is_some() {
                return Ok(false);
            }
            if let Some((pending, storage)) = &mut pending {
                let (child, child_allocation) = work.display(&format_args!(
                    "{path}/{}",
                    entry
                        .name
                        .unicode_text()
                        .ok_or(NativeTreeMaterializationError::InvalidLayout)?
                ))?;
                native_vec_push(pending, (child, true, child_allocation), storage)?;
            }
        }
        let more = page.has_more;
        let next = if more {
            page.entries.pop().map(|entry| entry.name)
        } else {
            None
        };
        let bytes = next.as_ref().map_or(0, |name| name.retained_bytes() as u64);
        drop(page);
        allocation.resize(bytes)?;
        drop(after.take());
        drop(after_allocation);
        after = next;
        after_allocation = allocation;
        if !more || after.is_none() {
            break;
        }
    }
    Ok(names.is_empty()
        && crate::native_host::directory_identity(&directory, &mut &*work)? == identity)
}

fn host_name_matches(
    host: &std::ffi::OsStr,
    logical: &crate::kernel::LogicalName,
    work: &NativeMaterializationWork,
) -> Result<bool, NativeTreeMaterializationError> {
    match logical.encoding() {
        crate::kernel::NameEncoding::WindowsUtf16Le => {
            if logical.as_bytes().len() % 2 != 0 {
                return Ok(false);
            }
            let units = logical
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]]));
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt as _;
                Ok(native_compare(
                    host.as_bytes().iter().copied(),
                    native_utf16_name_bytes(units),
                    work,
                )? == std::cmp::Ordering::Equal)
            }
            #[cfg(windows)]
            {
                Ok(native_compare(host.encode_wide(), units, work)? == std::cmp::Ordering::Equal)
            }
        }
        crate::kernel::NameEncoding::Utf8 | crate::kernel::NameEncoding::PosixBytes => {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt as _;
                Ok(native_compare(
                    host.as_bytes().iter().copied(),
                    logical.as_bytes().iter().copied(),
                    work,
                )? == std::cmp::Ordering::Equal)
            }
            #[cfg(windows)]
            {
                work.charge(crate::WorkCounters {
                    items_examined: logical.as_bytes().len() as u64,
                    ..crate::WorkCounters::default()
                })?;
                let Ok(text) = std::str::from_utf8(logical.as_bytes()) else {
                    return Ok(false);
                };
                Ok(
                    native_compare(host.encode_wide(), text.encode_utf16(), work)?
                        == std::cmp::Ordering::Equal,
                )
            }
        }
    }
}
fn source_metadata_matches(
    observed: &HostStat,
    authored: &crate::WorkspaceMetadata,
    access_time: bool,
    work: &NativeMaterializationWork,
) -> Result<bool, NativeTreeMaterializationError> {
    work.charge(NativeResourceCredit::Items.delta(1))?;
    if authored.has_named_attributes
        || authored.has_acl
        || authored.has_security_descriptor
        || authored.posix_flags.is_some()
    {
        return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata);
    }
    #[cfg(windows)]
    if authored.posix_mode.is_some()
        || authored.posix_uid.is_some()
        || authored.posix_gid.is_some()
        || authored.changed_ns.is_some()
    {
        return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata);
    }
    #[cfg(unix)]
    if authored.windows_attributes.is_some() {
        return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata);
    }
    let times: [(
        Option<i64>,
        fn(&HostStat) -> std::io::Result<cap_std::time::SystemTime>,
    ); 3] = [
        (authored.created_ns, HostStat::created),
        (authored.modified_ns, HostStat::modified),
        (
            if access_time {
                authored.accessed_ns
            } else {
                None
            },
            HostStat::accessed,
        ),
    ];
    for (expected, read) in times {
        if let Some(expected) = expected {
            work.charge(NativeResourceCredit::Items.delta(1))?;
            match crate::native_capture::system_time(read(observed)) {
                crate::kernel::MetadataField::Value(actual) if actual == expected => {}
                crate::kernel::MetadataField::Value(_) => return Ok(false),
                _ => return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata),
            }
        }
    }
    #[cfg(unix)]
    {
        if authored.changed_ns.is_some_and(|expected| {
            i128::from(expected)
                != i128::from(observed.ctime()) * 1_000_000_000 + i128::from(observed.ctime_nsec())
        }) {
            return Ok(false);
        }
        if authored
            .posix_mode
            .is_some_and(|value| value & 0o7777 != observed.mode() & 0o7777)
            || authored
                .posix_uid
                .is_some_and(|value| value != observed.uid())
            || authored
                .posix_gid
                .is_some_and(|value| value != observed.gid())
        {
            return Ok(false);
        }
    }
    #[cfg(windows)]
    if authored
        .windows_attributes
        .is_some_and(|value| value & 0x2137 != observed.file_attributes() & 0x2137)
    {
        return Ok(false);
    }
    Ok(true)
}
