//! Native file observations and observation-checked caches for documentation inputs.

use std::fs;
use std::io;
use std::path::Path;
use std::time::SystemTime;

/// Whether no-follow metadata identifies a symlink or a Windows reparse point.
pub fn is_linked(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

/// Inspects the path and its ancestors without following their final component.
/// Missing components are permitted; other failures preserve the caller's error.
/// This is a path inspection, not a retained guarantee against later replacement.
pub fn reject_linked_ancestors<E>(
    path: &Path,
    linked: impl FnOnce(&Path) -> E,
    failed: impl FnOnce(&Path, io::Error) -> E,
) -> Result<(), E> {
    for ancestor in path.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if is_linked(&metadata) => return Err(linked(ancestor)),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(failed(ancestor, error)),
        }
    }
    Ok(())
}

/// Returns a cached value when its saved observation matches the current one.
/// Otherwise loads the value and caches it only if a second observation agrees.
/// Observation or load failures leave the cache unchanged; differing observations
/// return the caller's `changed` error. This does not lock the observed resource
/// against changes after an observation.
pub fn cached<K: Ord, S: Clone + Eq, V: Clone, E>(
    values: &std::cell::RefCell<std::collections::BTreeMap<K, (S, V)>>,
    key: K,
    mut observe: impl FnMut() -> Result<S, E>,
    load: impl FnOnce() -> Result<V, E>,
    changed: impl FnOnce() -> E,
) -> Result<V, E> {
    let before = observe()?;
    if let Some(value) = values
        .borrow()
        .get(&key)
        .filter(|(cached, _)| *cached == before)
        .map(|(_, value)| value.clone())
    {
        return Ok(value);
    }
    let value = load()?;
    if observe()? != before {
        return Err(changed());
    }
    values.borrow_mut().insert(key, (before, value.clone()));
    Ok(value)
}

/// File identity and metadata used to detect replacement or modification.
/// Equality compares observations; it is not a digest of the file's contents.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Stamp {
    length: u64,
    modified: SystemTime,
    created: Option<SystemTime>,
    #[cfg(unix)]
    native: (u64, u64, i64, i64),
    #[cfg(windows)]
    native: (u64, [u8; 16], i64),
}

/// Observes a regular file's identity, length and timestamps, following links.
/// Returns an error if the path cannot be observed or is not a regular file.
pub fn stamp(path: &Path) -> io::Result<Stamp> {
    #[cfg(windows)]
    let file = {
        use std::os::windows::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .access_mode(windows_sys::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES)
            .open(path)?
    };
    #[cfg(windows)]
    let metadata = file.metadata()?;
    #[cfg(not(windows))]
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(io::Error::other(format!(
            "expected a regular file: {}",
            path.display()
        )));
    }
    Ok(Stamp {
        length: metadata.len(),
        modified: metadata.modified()?,
        created: metadata.created().ok(),
        #[cfg(unix)]
        native: {
            use std::os::unix::fs::MetadataExt;
            (
                metadata.dev(),
                metadata.ino(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            )
        },
        #[cfg(windows)]
        native: windows_identity(&file)?,
    })
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Windows file identity requires querying the owned handle through Win32"
)]
fn windows_identity(file: &fs::File) -> std::io::Result<(u64, [u8; 16], i64)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem as winfs;
    let mut basic = std::mem::MaybeUninit::<winfs::FILE_BASIC_INFO>::uninit();
    let mut id = std::mem::MaybeUninit::<winfs::FILE_ID_INFO>::uninit();
    // SAFETY: both buffers have the API's exact type/size and outlive the calls;
    // the file owns the live handle. Read the buffers only after both succeed.
    unsafe {
        if winfs::GetFileInformationByHandleEx(
            file.as_raw_handle(),
            winfs::FileBasicInfo,
            basic.as_mut_ptr().cast(),
            std::mem::size_of::<winfs::FILE_BASIC_INFO>() as u32,
        ) == 0
            || winfs::GetFileInformationByHandleEx(
                file.as_raw_handle(),
                winfs::FileIdInfo,
                id.as_mut_ptr().cast(),
                std::mem::size_of::<winfs::FILE_ID_INFO>() as u32,
            ) == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        let id = id.assume_init();
        Ok((
            id.VolumeSerialNumber,
            id.FileId.Identifier,
            basic.assume_init().ChangeTime,
        ))
    }
}
