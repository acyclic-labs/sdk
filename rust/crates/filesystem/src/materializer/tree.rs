use super::*;
use crate::native_host::{HostStat, HostStatReader};

#[cfg(unix)]
pub(super) type NativeWalkName = std::ffi::OsString;
#[cfg(windows)]
pub(super) type NativeWalkName = Vec<u16>;
#[cfg(unix)]
pub(super) type NativeWalkNameRef = std::ffi::OsStr;
#[cfg(windows)]
pub(super) type NativeWalkNameRef = [u16];
pub(super) fn with_native_walk_leaf<T>(
    leaf: &Path,
    work: &NativeMaterializationWork,
    action: impl FnOnce(&NativeWalkNameRef) -> Result<T, NativeTreeMaterializationError>,
) -> Result<T, NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        let _ = work;
        action(leaf.as_os_str())
    }
    #[cfg(windows)]
    {
        crate::native_host::with_wide_path_with_work(leaf, &mut &*work, |name, _| action(name))
    }
}
pub(super) fn next_native_walk_name(
    reader: &mut HostStatReader,
    work: &NativeMaterializationWork,
) -> Result<Option<(NativeWalkName, NativeAllocation)>, NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        reader
            .next_with_work(&mut &*work)
            .map(|entry| entry.map(|(entry, allocation)| (entry.name, allocation)))
    }
    #[cfg(windows)]
    {
        reader
            .next_wide_with_work(&mut &*work)
            .map(|entry| entry.map(|(entry, allocation)| (entry.name, allocation)))
    }
}
pub(super) fn stat_native_walk_name(
    parent: &cap_std::fs::Dir,
    name: &NativeWalkNameRef,
    work: &NativeMaterializationWork,
) -> Result<HostStat, NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        crate::native_host::stat_leaf_with_work(parent, name, &mut &*work)
    }
    #[cfg(windows)]
    {
        crate::native_host::stat_windows_leaf_with_work(parent, name, &mut &*work)
    }
}
pub(crate) fn native_stat_held(
    parent: &cap_std::fs::Dir,
    leaf: &Path,
    work: &NativeMaterializationWork,
) -> Result<HostStat, NativeTreeMaterializationError> {
    with_native_walk_leaf(leaf, work, |name| stat_native_walk_name(parent, name, work))
}
pub(super) fn open_native_walk_directory(
    parent: &cap_std::fs::Dir,
    name: &NativeWalkNameRef,
    work: &NativeMaterializationWork,
) -> Result<cap_std::fs::Dir, NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        crate::native_host::open_unix_directory_with_work(parent, name, false, &mut &*work)
    }
    #[cfg(windows)]
    {
        crate::native_host::open_windows_leaf_handle_with_work(
            parent,
            name,
            windows::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            Some(true),
            &mut &*work,
        )
        .map(cap_std::fs::Dir::from_std_file)
    }
}
fn open_native_walk_file(
    parent: &cap_std::fs::Dir,
    name: &NativeWalkNameRef,
    work: &NativeMaterializationWork,
) -> Result<std::fs::File, NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        crate::native_host::open_leaf_with_work(
            parent,
            Path::new(name),
            &crate::native_host::NativeOpenOptions::Read,
            &mut &*work,
        )
    }
    #[cfg(windows)]
    {
        crate::native_host::open_windows_leaf_handle_with_work(
            parent,
            name,
            windows::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            Some(false),
            &mut &*work,
        )
    }
}
pub(super) fn native_compare<T: Ord>(
    mut a: impl Iterator<Item = T>,
    mut b: impl Iterator<Item = T>,
    work: &NativeMaterializationWork,
) -> Result<std::cmp::Ordering, NativeTreeMaterializationError> {
    loop {
        work.charge(NativeResourceCredit::Items.delta(1))?;
        match (a.next(), b.next()) {
            (Some(a), Some(b)) if a == b => {}
            (Some(a), Some(b)) => return Ok(a.cmp(&b)),
            (None, Some(_)) => return Ok(std::cmp::Ordering::Less),
            (Some(_), None) => return Ok(std::cmp::Ordering::Greater),
            (None, None) => return Ok(std::cmp::Ordering::Equal),
        }
    }
}
pub(super) fn native_sort_by<T>(
    values: &mut [T],
    work: &NativeMaterializationWork,
    mut less: impl FnMut(&T, &T) -> Result<bool, NativeTreeMaterializationError>,
) -> Result<(), NativeTreeMaterializationError> {
    fn sift<T>(
        values: &mut [T],
        mut root: usize,
        end: usize,
        work: &NativeMaterializationWork,
        less: &mut impl FnMut(&T, &T) -> Result<bool, NativeTreeMaterializationError>,
    ) -> Result<(), NativeTreeMaterializationError> {
        while let Some(child) = root
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .filter(|n| *n < end)
        {
            let child = if child + 1 < end && less(&values[child + 1], &values[child])? {
                child + 1
            } else {
                child
            };
            if !less(&values[child], &values[root])? {
                break;
            }
            work.charge(crate::WorkCounters {
                bytes_copied: owned_size(2, std::mem::size_of::<T>())?,
                ..crate::WorkCounters::UNCHARGED
            })?;
            values.swap(root, child);
            root = child;
        }
        Ok(())
    }
    for root in (0..values.len() / 2).rev() {
        sift(values, root, values.len(), work, &mut less)?;
    }
    for end in (1..values.len()).rev() {
        work.charge(crate::WorkCounters {
            bytes_copied: owned_size(2, std::mem::size_of::<T>())?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        values.swap(0, end);
        sift(values, 0, end, work, &mut less)?;
    }
    Ok(())
}
struct HashFrame {
    directory: cap_std::fs::Dir,
    names: Vec<(NativeWalkName, NativeAllocation)>,
    allocation: NativeAllocation,
}
fn hash_frame(
    directory: cap_std::fs::Dir,
    work: &NativeMaterializationWork,
) -> Result<HashFrame, NativeTreeMaterializationError> {
    let (mut reader, cursor) =
        crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
    let mut allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut names = Vec::new();
    while let Some(name) = next_native_walk_name(&mut reader, work)? {
        native_vec_push(&mut names, name, &mut allocation)?;
    }
    drop(reader);
    drop(cursor);
    native_sort_by(&mut names, work, |a, b| {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt as _;
            Ok(native_compare(
                a.0.as_bytes().iter().copied(),
                b.0.as_bytes().iter().copied(),
                work,
            )? == std::cmp::Ordering::Less)
        }
        #[cfg(windows)]
        {
            Ok(native_compare(
                native_utf16_name_bytes(a.0.iter().copied()),
                native_utf16_name_bytes(b.0.iter().copied()),
                work,
            )? == std::cmp::Ordering::Less)
        }
    })?;
    Ok(HashFrame {
        directory,
        names,
        allocation,
    })
}
pub(super) fn hash_native_entry(
    backend: &NativeTreeMaterializationBackend,
    parent: &cap_std::fs::Dir,
    leaf: &Path,
    metadata: &HostStat,
    hasher: &mut blake3::Hasher,
) -> Result<(), NativeTreeMaterializationError> {
    let work = backend.work();
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut frames = Vec::new();
    if let Some(frame) = with_native_walk_leaf(leaf, &work, |name| {
        hash_leaf(parent, name, metadata, hasher, &work)
    })? {
        native_vec_push(&mut frames, frame, &mut storage)?;
    }
    while let Some(frame) = frames.last_mut() {
        let Some((name, allocation)) = frame.names.pop() else {
            frames.pop();
            continue;
        };
        hash_name(&name, hasher, &work)?;
        let metadata = stat_native_walk_name(&frame.directory, &name, &work)?;
        let child = hash_leaf(&frame.directory, &name, &metadata, hasher, &work)?;
        drop(name);
        drop(allocation);
        if let Some(frame) = child {
            native_vec_push(&mut frames, frame, &mut storage)?;
        }
    }
    Ok(())
}
fn hash_leaf(
    parent: &cap_std::fs::Dir,
    name: &NativeWalkNameRef,
    metadata: &HostStat,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<Option<HashFrame>, NativeTreeMaterializationError> {
    use std::io::Read as _;
    let kind: &[u8] = if metadata.file_type().is_symlink() {
        b"link"
    } else if metadata.is_dir() {
        b"directory"
    } else if metadata.file_type().is_file() {
        b"file"
    } else {
        b"special"
    };
    #[cfg(unix)]
    let length = 81;
    #[cfg(windows)]
    let length = 33;
    work.charge(crate::WorkCounters {
        source_entries_visited: 1,
        bytes_hashed: kind.len() as u64 + length,
        ..crate::WorkCounters::UNCHARGED
    })?;
    hasher.update(kind);
    hasher.update(&metadata.len().to_le_bytes());
    hasher.update(&[u8::from(native_metadata(metadata).readonly)]);
    #[cfg(unix)]
    {
        for value in [
            metadata.dev(),
            metadata.ino(),
            u64::from(metadata.mode()),
            metadata.nlink(),
            u64::from(metadata.uid()),
            u64::from(metadata.gid()),
            metadata.rdev(),
        ] {
            hasher.update(&value.to_le_bytes());
        }
        hasher.update(&metadata.mtime().to_le_bytes());
        hasher.update(&metadata.mtime_nsec().to_le_bytes());
    }
    #[cfg(windows)]
    for value in [
        u64::from(metadata.file_attributes()),
        metadata.last_write_time(),
        metadata.len(),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    if metadata.file_type().is_symlink() {
        #[cfg(unix)]
        let (target, allocation) = crate::native_host::read_link_held_with_work(
            parent,
            Path::new(name),
            work.remaining()?.bytes_hashed,
            &mut &*work,
        )?;
        #[cfg(windows)]
        let (target, allocation) = crate::native_host::read_link_held_wide_with_work(
            parent,
            name,
            work.remaining()?.bytes_hashed,
            &mut &*work,
        )?;
        #[cfg(unix)]
        {
            work.charge(crate::WorkCounters {
                bytes_hashed: target.len() as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            hasher.update(&target);
        }
        #[cfg(windows)]
        {
            hash_name(&target, hasher, work)?;
        }
        drop(target);
        drop(allocation);
    } else if metadata.is_dir() {
        let directory = open_native_walk_directory(parent, name, work)?;
        if crate::native_host::directory_identity(&directory, &mut &*work)?
            != metadata
                .identity()
                .map_err(native_materialization_work_error)?
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        return hash_frame(directory, work).map(Some);
    } else if metadata.file_type().is_file() {
        let mut file = open_native_walk_file(parent, name, work)?;
        let opened = crate::native_host::stat_file_with_work(&file, &mut &*work)?;
        if opened
            .identity()
            .map_err(native_materialization_work_error)?
            != metadata
                .identity()
                .map_err(native_materialization_work_error)?
            || opened.len() != metadata.len()
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let mut buffer = [0; 64 * 1024];
        let mut offset = 0;
        while offset < metadata.len() {
            let remaining = work.remaining()?;
            let length = (metadata.len() - offset)
                .min(buffer.len() as u64)
                .min(remaining.source_bytes_read)
                .min(remaining.bytes_hashed) as usize;
            if length == 0 {
                work.charge(crate::WorkCounters {
                    source_bytes_read: 1,
                    bytes_hashed: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
            }
            work.charge(NativeResourceCredit::ReadOperations.delta(1))?;
            let count = file.read(&mut buffer[..length])?;
            if count == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
            }
            work.spent(crate::WorkCounters {
                source_bytes_read: count as u64,
                bytes_hashed: count as u64,
                ..crate::WorkCounters::UNCHARGED
            })?;
            hasher.update(&buffer[..count]);
            offset += count as u64;
        }
        let current = crate::native_host::stat_file_with_work(&file, &mut &*work)?;
        if current
            .identity()
            .map_err(native_materialization_work_error)?
            != metadata
                .identity()
                .map_err(native_materialization_work_error)?
            || current.len() != metadata.len()
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
    }
    Ok(None)
}
fn hash_name(
    name: &NativeWalkNameRef,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        work.charge(crate::WorkCounters {
            bytes_hashed: name.as_bytes().len() as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        hasher.update(name.as_bytes());
    }
    #[cfg(windows)]
    {
        work.charge(crate::WorkCounters {
            bytes_hashed: owned_size(name.len(), 2)?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        for value in name {
            hasher.update(&value.to_le_bytes());
        }
    }
    Ok(())
}
pub(super) fn hash_native_metadata_edit(
    metadata: &HostStat,
    desired: Option<&NativeMetadataImage>,
    hasher: &mut blake3::Hasher,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let kind: &[u8] = if metadata.file_type().is_symlink() {
        b"link"
    } else if metadata.is_dir() {
        b"directory"
    } else if metadata.file_type().is_file() {
        b"file"
    } else {
        b"special"
    };
    #[cfg(unix)]
    let length = 24;
    #[cfg(windows)]
    let length = if metadata.is_dir() || metadata.file_type().is_file() {
        20
    } else {
        4
    };
    work.charge(crate::WorkCounters {
        source_entries_visited: 1,
        bytes_hashed: kind.len() as u64 + length,
        ..crate::WorkCounters::UNCHARGED
    })?;
    hasher.update(kind);
    #[cfg(unix)]
    for value in [
        metadata.dev(),
        metadata.ino(),
        u64::from(
            desired
                .and_then(|m| m.posix_mode)
                .unwrap_or_else(|| metadata.mode()),
        ),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    #[cfg(windows)]
    {
        let attributes =
            desired
                .and_then(|m| m.windows_attributes)
                .unwrap_or_else(|| match desired {
                    Some(m) if m.readonly => metadata.file_attributes() | 1,
                    Some(_) => metadata.file_attributes() & !1,
                    None => metadata.file_attributes(),
                });
        hasher.update(&attributes.to_le_bytes());
        if metadata.is_dir() || metadata.file_type().is_file() {
            hasher.update(
                &metadata
                    .identity()
                    .map_err(native_materialization_work_error)?
                    .to_bytes(),
            );
        }
    }
    Ok(())
}
pub(super) fn native_metadata(metadata: &HostStat) -> NativeMetadataImage {
    NativeMetadataImage {
        #[cfg(unix)]
        readonly: metadata.mode() & 0o222 == 0,
        #[cfg(windows)]
        readonly: metadata.file_attributes() & 1 != 0,
        #[cfg(unix)]
        posix_mode: Some(metadata.mode()),
        #[cfg(not(unix))]
        posix_mode: None,
        #[cfg(windows)]
        windows_attributes: Some(metadata.file_attributes()),
        #[cfg(not(windows))]
        windows_attributes: None,
    }
}
struct RemoveFrame {
    directory: cap_std::fs::Dir,
    reader: HostStatReader,
    name: Option<(NativeWalkName, NativeAllocation)>,
    cursor: NativeAllocation,
}
fn remove_frame(
    parent: &cap_std::fs::Dir,
    name: &NativeWalkNameRef,
    metadata: &HostStat,
    work: &NativeMaterializationWork,
) -> Result<RemoveFrame, NativeTreeMaterializationError> {
    let directory = open_native_walk_directory(parent, name, work)?;
    if crate::native_host::directory_identity(&directory, &mut &*work)?
        != metadata
            .identity()
            .map_err(native_materialization_work_error)?
    {
        return Err(NativeTreeMaterializationError::ExternalMutation);
    }
    let (reader, cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
    Ok(RemoveFrame {
        directory,
        reader,
        name: None,
        cursor,
    })
}
fn remove_leaf(
    parent: &cap_std::fs::Dir,
    name: &NativeWalkNameRef,
    directory: bool,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        if directory {
            crate::native_host::remove_empty_directory_held_with_work(
                parent,
                Path::new(name),
                &mut &*work,
            )
        } else {
            crate::native_host::remove_leaf_with_work(parent, Path::new(name), &mut &*work)
        }
    }
    #[cfg(windows)]
    {
        crate::native_host::remove_windows_leaf_with_work(parent, name, directory, &mut &*work)
    }
}
pub(super) fn remove_native_held(
    parent: &cap_std::fs::Dir,
    leaf: &Path,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    with_native_walk_leaf(leaf, work, |name| remove_native_walk(parent, name, work))
}
pub(super) fn remove_native_walk(
    parent: &cap_std::fs::Dir,
    name: &NativeWalkNameRef,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let metadata = match stat_native_walk_name(parent, name, work) {
        Ok(value) => value,
        Err(NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return remove_leaf(parent, name, false, work);
    }
    let root = remove_frame(parent, name, &metadata, work)?;
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut frames = Vec::new();
    native_vec_push(&mut frames, root, &mut storage)?;
    while let Some(frame) = frames.last_mut() {
        if let Some((child, allocation)) = next_native_walk_name(&mut frame.reader, work)? {
            let metadata = stat_native_walk_name(&frame.directory, &child, work)?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                let mut frame = remove_frame(&frame.directory, &child, &metadata, work)?;
                frame.name = Some((child, allocation));
                native_vec_push(&mut frames, frame, &mut storage)?;
            } else {
                remove_leaf(&frame.directory, &child, false, work)?;
                drop(child);
                drop(allocation);
            }
        } else {
            let frame = frames.pop().expect("current removal frame");
            crate::native_host::sync_directory_held(&frame.directory, &mut &*work)?;
            let RemoveFrame {
                directory,
                reader,
                name: child,
                cursor,
            } = frame;
            drop(reader);
            drop(cursor);
            drop(directory);
            let parent = frames.last().map_or(parent, |frame| &frame.directory);
            if let Some((child, allocation)) = child {
                remove_leaf(parent, &child, true, work)?;
                drop(child);
                drop(allocation);
            } else {
                remove_leaf(parent, name, true, work)?;
            }
        }
    }
    Ok(())
}

pub(super) fn native_utf16_name_bytes(
    units: impl Iterator<Item = u16>,
) -> impl Iterator<Item = u8> {
    std::char::decode_utf16(units).flat_map(|unit| {
        let value = unit
            .map(u32::from)
            .unwrap_or_else(|error| u32::from(error.unpaired_surrogate()));
        let mut bytes = [0; 4];
        let length = if value < 0x80 {
            bytes[0] = value as u8;
            1
        } else if value < 0x800 {
            bytes[0] = 0xc0 | (value >> 6) as u8;
            bytes[1] = 0x80 | (value & 63) as u8;
            2
        } else if value < 0x10000 {
            bytes[0] = 0xe0 | (value >> 12) as u8;
            bytes[1] = 0x80 | ((value >> 6) & 63) as u8;
            bytes[2] = 0x80 | (value & 63) as u8;
            3
        } else {
            bytes[0] = 0xf0 | (value >> 18) as u8;
            bytes[1] = 0x80 | ((value >> 12) & 63) as u8;
            bytes[2] = 0x80 | ((value >> 6) & 63) as u8;
            bytes[3] = 0x80 | (value & 63) as u8;
            4
        };
        bytes.into_iter().take(length)
    })
}
